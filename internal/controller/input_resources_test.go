package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type inputResourceStorageFixture struct {
	*storageFixture
	resource storagev1.Resource
	content  []byte
	reads    []string
	getCalls []context.Context
	readCtxs []context.Context
	getFn    func(context.Context, string) (storagev1.Resource, error)
	readFn   func(context.Context, string) (*http.Response, error)
}

func (s *inputResourceStorageFixture) GetResource(ctx context.Context, id string) (storagev1.Resource, error) {
	s.getCalls = append(s.getCalls, ctx)
	if s.getFn != nil {
		return s.getFn(ctx, id)
	}
	if id != s.resource.ResourceID {
		return storagev1.Resource{}, errors.New("unknown resource")
	}
	return s.resource, nil
}

func (s *inputResourceStorageFixture) ResourceContent(ctx context.Context, id string) (*http.Response, error) {
	s.readCtxs = append(s.readCtxs, ctx)
	if s.readFn != nil {
		return s.readFn(ctx, id)
	}
	if id != s.resource.ResourceID {
		return nil, errors.New("unknown resource")
	}
	s.reads = append(s.reads, id)
	return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(s.content)), Body: io.NopCloser(bytes.NewReader(s.content))}, nil
}

type inputResourceAccessFixture struct {
	checks  []storagev1.AccessCheck
	ctxs    []context.Context
	checkFn func(context.Context, storagev1.AccessCheck) (bool, error)
}

func (a *inputResourceAccessFixture) Check(ctx context.Context, check storagev1.AccessCheck) (bool, error) {
	a.checks = append(a.checks, check)
	a.ctxs = append(a.ctxs, ctx)
	if a.checkFn != nil {
		return a.checkFn(ctx, check)
	}
	return true, nil
}

func TestInputFileOpenerFollowsDeduplicatedWirePlan(t *testing.T) {
	content := []byte("same resource bytes")
	digest := sha256.Sum256(content)
	hash := hex.EncodeToString(digest[:])
	scope := storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}
	makeFile := func(pointer, filename string) projectsv1.PreparedFile {
		return projectsv1.PreparedFile{Pointer: pointer, ResourceID: "resource-1", Filename: filename, SHA256: hash, SizeBytes: int64(len(content)), Path: generationv1.GeneratedInputPath(hash, filename)}
	}
	files := []projectsv1.PreparedFile{
		makeFile("/assetA", "paper.pdf"),
		makeFile("/assetC", "paper.pdf"),
		makeFile("/assetB", "source.txt"),
	}
	principal := storagev1.Principal{Subject: "actor", Domain: "user"}
	inputResources := make([]store.PinnedInputResource, 0, len(files))
	for _, file := range files {
		inputResources = append(inputResources, store.PinnedInputResource{Resource: projectsv1.InputResource{File: file, Scope: scope, Class: storagev1.ClassResource}, AcquisitionKey: "pin-key", PinID: "pin-1", State: "active"})
	}
	input := store.ExecutionInput{Execution: &projectsv1.ResolvedExecution{Actor: principal, Scope: scope, Prepared: projectsv1.PreparedInput{Files: files}}, InputResources: inputResources}
	storage := &inputResourceStorageFixture{storageFixture: newStorageFixture(), resource: storagev1.Resource{ResourceID: "resource-1", Scope: scope, Class: storagev1.ClassResource, SizeBytes: int64(len(content)), SHA256: hash}, content: content}
	access := &inputResourceAccessFixture{}
	controller := &Controller{storage: storage, access: access}
	openerFactory, err := controller.inputFileOpener(context.Background(), generationv1.Job{JobID: "gen_job", Scope: scope, SourceSnapshotID: "snapshot-1"}, input)
	if err != nil {
		t.Fatal(err)
	}
	opener := openerFactory(context.Background())
	plan, err := renderwire.PlanInputFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 2 {
		t.Fatalf("deduplicated stream plan contains %d files, want 2", len(plan))
	}
	for _, file := range plan {
		reader, openErr := opener(file)
		if openErr != nil {
			t.Fatalf("open planned %s: %v", file.Pointer, openErr)
		}
		got, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(got, content) {
			t.Fatalf("planned file %s bytes: %q read=%v close=%v", file.Pointer, got, readErr, closeErr)
		}
	}
	if len(storage.reads) != len(plan) {
		t.Fatalf("Storage reads=%v, want one read per unique generated path", storage.reads)
	}
	if len(access.checks) != len(files)+len(plan) {
		t.Fatalf("current resource checks=%d, want preflight and each content read", len(access.checks))
	}
}

func TestInputFileOpenerUsesAttemptContextAndStopsAfterCancellation(t *testing.T) {
	for _, stage := range []string{"access", "metadata", "content"} {
		t.Run(stage, func(t *testing.T) {
			content := []byte("frozen resource")
			digest := sha256.Sum256(content)
			hash := hex.EncodeToString(digest[:])
			scope := storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}
			file := projectsv1.PreparedFile{Pointer: "/asset", ResourceID: "resource-1", Filename: "asset.pdf", SHA256: hash, SizeBytes: int64(len(content)), Path: generationv1.GeneratedInputPath(hash, "asset.pdf")}
			pinned := store.PinnedInputResource{Resource: projectsv1.InputResource{File: file, Scope: scope, Class: storagev1.ClassResource}, AcquisitionKey: "pin-key", PinID: "pin-1", State: "active"}
			input := store.ExecutionInput{Execution: &projectsv1.ResolvedExecution{Actor: storagev1.Principal{Subject: "actor", Domain: "user"}, Scope: scope, Prepared: projectsv1.PreparedInput{Files: []projectsv1.PreparedFile{file}}}, InputResources: []store.PinnedInputResource{pinned}}
			storage := &inputResourceStorageFixture{storageFixture: newStorageFixture(), resource: storagev1.Resource{ResourceID: file.ResourceID, Scope: scope, Class: storagev1.ClassResource, SizeBytes: file.SizeBytes, SHA256: hash}, content: content}
			access := &inputResourceAccessFixture{}
			controller := &Controller{storage: storage, access: access}
			factory, err := controller.inputFileOpener(context.Background(), generationv1.Job{JobID: "gen_job", Scope: scope}, input)
			if err != nil {
				t.Fatal(err)
			}

			started, release := make(chan struct{}), make(chan struct{})
			var closeOnce sync.Once
			block := func() { closeOnce.Do(func() { close(started) }); <-release }
			if stage == "access" {
				access.checkFn = func(ctx context.Context, _ storagev1.AccessCheck) (bool, error) {
					if len(access.ctxs) == 2 { // The first check was the pre-quota preflight.
						block()
					}
					return true, nil // Simulate a dependency that returns success after cancellation.
				}
			}
			if stage == "metadata" {
				storage.getFn = func(_ context.Context, id string) (storagev1.Resource, error) {
					if len(storage.getCalls) == 2 { // The first read was the pre-quota preflight.
						block()
					}
					return storage.resource, nil
				}
			}
			var responseBody *trackedInputBody
			if stage == "content" {
				storage.readFn = func(_ context.Context, _ string) (*http.Response, error) {
					responseBody = &trackedInputBody{Reader: bytes.NewReader(content)}
					block()
					return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(content)), Body: responseBody}, nil
				}
			}

			attemptCtx, cancel := boundedExecutionContext(context.Background(), time.Now().Add(time.Minute))
			defer cancel()
			open := factory(attemptCtx)
			result := make(chan error, 1)
			go func() {
				reader, openErr := open(file)
				if reader != nil {
					_ = reader.Close()
				}
				result <- openErr
			}()
			<-started
			cancel()
			close(release)
			if err = <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("open after canceled %s check returned %v, want context.Canceled", stage, err)
			}
			if len(access.ctxs) < 2 || access.ctxs[1] != attemptCtx {
				t.Fatalf("authorization used outer context instead of bounded attempt context: contexts=%d", len(access.ctxs))
			}
			switch stage {
			case "access":
				if len(storage.getCalls) != 1 || len(storage.readCtxs) != 0 {
					t.Fatalf("canceled authorization proceeded to metadata/content: metadata=%d content=%d", len(storage.getCalls), len(storage.readCtxs))
				}
			case "metadata":
				if len(storage.getCalls) != 2 || len(storage.readCtxs) != 0 {
					t.Fatalf("canceled metadata check proceeded to content: metadata=%d content=%d", len(storage.getCalls), len(storage.readCtxs))
				}
				if storage.getCalls[1] != attemptCtx {
					t.Fatal("metadata lookup did not receive the bounded attempt context")
				}
			case "content":
				if len(storage.readCtxs) != 1 || storage.readCtxs[0] != attemptCtx || responseBody == nil || !responseBody.closed {
					t.Fatalf("content call did not use the attempt context and close late response: contexts=%d body=%v", len(storage.readCtxs), responseBody)
				}
			}
		})
	}
}

type trackedInputBody struct {
	*bytes.Reader
	closed bool
}

func (b *trackedInputBody) Close() error {
	b.closed = true
	return nil
}
