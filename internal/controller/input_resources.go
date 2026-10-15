package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

var errInputResourceAccessDenied = errors.New("input resource access denied")

type inputFileOpenerFactory func(context.Context) func(projectsv1.PreparedFile) (io.ReadCloser, error)

type storageResourceMetadata interface {
	GetResource(context.Context, string) (storagev1.Resource, error)
}

// inputFileOpener checks every frozen descriptor and current resource grant
// before any file bytes are requested. The returned callback repeats those
// checks immediately before each privileged Storage content read.
func (c *Controller) inputFileOpener(ctx context.Context, job generationv1.Job, input store.ExecutionInput) (inputFileOpenerFactory, error) {
	if input.Execution == nil || len(input.Execution.Prepared.Files) == 0 {
		return nil, nil
	}
	metadata, ok := c.storage.(storageResourceMetadata)
	if !ok {
		return nil, errors.New("Storage resource metadata is unavailable")
	}
	principal := input.Execution.Actor
	byPointer := make(map[string]store.PinnedInputResource, len(input.InputResources))
	for _, pinned := range input.InputResources {
		if pinned.State != "active" || pinned.PinID == "" || pinned.Resource.Scope != job.Scope || pinned.Resource.Class != storagev1.ClassResource {
			return nil, errors.New("input resource pin binding is invalid")
		}
		if _, exists := byPointer[pinned.Resource.File.Pointer]; exists {
			return nil, errors.New("duplicate durable input resource pointer")
		}
		byPointer[pinned.Resource.File.Pointer] = pinned
	}
	seenPointers := make(map[string]bool, len(input.Execution.Prepared.Files))
	for _, file := range input.Execution.Prepared.Files {
		pinned, exists := byPointer[file.Pointer]
		if !exists || seenPointers[file.Pointer] || pinned.Resource.File != file {
			return nil, errors.New("input file differs from its durable pinned descriptor")
		}
		if err := c.validateInputResource(ctx, principal, metadata, pinned); err != nil {
			return nil, err
		}
		seenPointers[file.Pointer] = true
	}
	if len(seenPointers) != len(byPointer) {
		return nil, errors.New("durable input resource list contains unexpected files")
	}
	plan, err := renderwire.PlanInputFiles(input.Execution.Prepared.Files)
	if err != nil {
		return nil, err
	}
	byFile := make(map[string]store.PinnedInputResource, len(plan))
	for _, file := range plan {
		pinned, exists := byPointer[file.Pointer]
		if !exists || pinned.Resource.File != file {
			return nil, errors.New("input file stream plan differs from its durable pin descriptor")
		}
		byFile[file.Path] = pinned
	}
	return func(attemptCtx context.Context) func(projectsv1.PreparedFile) (io.ReadCloser, error) {
		return func(file projectsv1.PreparedFile) (io.ReadCloser, error) {
			pinned, exists := byFile[file.Path]
			if !exists || pinned.Resource.File != file {
				return nil, errors.New("renderer requested an unbound input file")
			}
			if err := attemptCtx.Err(); err != nil {
				return nil, err
			}
			if err := c.validateInputResource(attemptCtx, principal, metadata, pinned); err != nil {
				return nil, err
			}
			if err := attemptCtx.Err(); err != nil {
				return nil, err
			}
			response, err := c.storage.ResourceContent(attemptCtx, file.ResourceID)
			if err != nil {
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				if attemptErr := attemptCtx.Err(); attemptErr != nil {
					return nil, attemptErr
				}
				return nil, errors.New("Storage input resource could not be opened")
			}
			if response == nil || response.Body == nil {
				return nil, errors.New("Storage returned an empty input resource response")
			}
			if err = attemptCtx.Err(); err != nil {
				_ = response.Body.Close()
				return nil, err
			}
			if response.StatusCode != http.StatusOK || response.ContentLength >= 0 && response.ContentLength != file.SizeBytes {
				_ = response.Body.Close()
				return nil, errors.New("Storage input response differs from the frozen file descriptor")
			}
			return response.Body, nil
		}
	}, nil
}

func (c *Controller) validateInputResource(ctx context.Context, principal storagev1.Principal, metadata storageResourceMetadata, pinned store.PinnedInputResource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file := pinned.Resource.File
	allowed, err := c.access.Check(ctx, storagev1.AccessCheck{Principal: principal, Scope: pinned.Resource.Scope, Operation: "resource.read", ObjectID: file.ResourceID})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("check current input resource rights: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if !allowed {
		return errInputResourceAccessDenied
	}
	resource, err := metadata.GetResource(ctx, file.ResourceID)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("read frozen input resource metadata: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if resource.ResourceID != file.ResourceID || resource.Scope != pinned.Resource.Scope || resource.Class != pinned.Resource.Class || resource.Class != storagev1.ClassResource || resource.SHA256 != file.SHA256 || resource.SizeBytes != file.SizeBytes {
		return errors.New("Storage input resource differs from its frozen metadata")
	}
	return nil
}
