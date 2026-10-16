package store

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

// ExecutionInput is read from durable admission data, never from a workflow's
// sole copy of a request. Parameters retain their original normalized bytes.
type ExecutionInput struct {
	Identity             *identityv1.Identity
	InitiatorID          string
	NormalizedRequest    []byte
	Request              generationv1.CreateJobRequest
	Selection            *generationv1.RenderSelectionRequest
	SelectionFingerprint string
	Execution            *projectsv1.ResolvedExecution
	InputResources       []PinnedInputResource
}

func (s *Store) GetExecutionInput(ctx context.Context, jobID string) (ExecutionInput, error) {
	var out ExecutionInput
	authority, authorityErr := s.LoadAuthority(ctx, jobID)
	if authorityErr != nil {
		return out, authorityErr
	}
	out.Identity = authority.Identity
	var fingerprint, selectionFingerprint, actorSubject, actorDomain string
	var selectionRequest []byte
	err := s.pool.QueryRow(ctx, `SELECT initiator_id,normalized_request,fingerprint,selection_request,COALESCE(selection_fingerprint,''),actor_subject,actor_domain FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&out.InitiatorID, &out.NormalizedRequest, &fingerprint, &selectionRequest, &selectionFingerprint, &actorSubject, &actorDomain)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if len(selectionRequest) == 0 {
		request, normalized, actual, normalizeErr := generationv1.NormalizeRequest(out.NormalizedRequest)
		if normalizeErr != nil || actual != fingerprint || !bytes.Equal(normalized, out.NormalizedRequest) || selectionFingerprint != "" || actorSubject != "" || actorDomain != "" {
			return ExecutionInput{}, errors.New("durable execution input unavailable or invalid")
		}
		out.Request = request
		return out, nil
	}
	selection, normalizedSelection, actualSelection, err := generationv1.NormalizeSelection(selectionRequest)
	if err != nil || !bytes.Equal(normalizedSelection, selectionRequest) || actualSelection != selectionFingerprint {
		return ExecutionInput{}, errors.New("durable selection input unavailable or invalid")
	}
	var execution projectsv1.ResolvedExecution
	if err = jsonvalue.Unmarshal(out.NormalizedRequest, &execution, projectsv1.ResponseLimit); err != nil {
		return ExecutionInput{}, errors.New("durable resolved execution is invalid")
	}
	normalizedExecution, actualExecution, err := NormalizeResolvedExecution(execution)
	if err != nil || !bytes.Equal(normalizedExecution, out.NormalizedRequest) || actualExecution != fingerprint || execution.Actor != (storagev1.Principal{Subject: actorSubject, Domain: actorDomain}) {
		return ExecutionInput{}, errors.New("durable resolved execution binding is invalid")
	}
	if err = projectsv1.ValidateResolved(projectsv1.ResolveRequest{Actor: execution.Actor, Selection: selection, SelectionFingerprint: selectionFingerprint}, execution); err != nil {
		return ExecutionInput{}, errors.New("durable Projects execution is invalid")
	}
	inputResources, err := s.loadInputResources(ctx, jobID)
	if err != nil || len(inputResources) != len(execution.InputResources) {
		return ExecutionInput{}, errors.New("durable input resource bindings are unavailable or invalid")
	}
	byPointer := make(map[string]PinnedInputResource, len(inputResources))
	for _, resource := range inputResources {
		if resource.State != "active" || resource.PinID == "" || resource.Resource.Class != storagev1.ClassResource {
			return ExecutionInput{}, errors.New("durable input resource is not protected by an active pin")
		}
		byPointer[resource.Resource.File.Pointer] = resource
	}
	for _, resource := range execution.InputResources {
		pinned, exists := byPointer[resource.File.Pointer]
		if !exists || pinned.Resource != resource {
			return ExecutionInput{}, errors.New("durable input resource differs from the resolved allowlist")
		}
		delete(byPointer, resource.File.Pointer)
	}
	if len(byPointer) != 0 {
		return ExecutionInput{}, errors.New("durable input resource allowlist contains an unexpected field")
	}
	out.Selection = &selection
	out.SelectionFingerprint = selectionFingerprint
	out.Execution = &execution
	out.InputResources = inputResources
	out.Request = generationv1.CreateJobRequest{Scope: execution.Scope, SnapshotID: execution.SnapshotID, TargetID: execution.Descriptor.TargetID, Entrypoint: execution.Descriptor.Entrypoint, Parameters: append([]byte(nil), execution.Prepared.JSON...), ParametersSchemaPath: execution.Descriptor.InputSchema}
	return out, nil
}

func (s *Store) loadInputResources(ctx context.Context, jobID string) ([]PinnedInputResource, error) {
	rows, err := s.pool.Query(ctx, `SELECT field_pointer,resource_id,workspace_id,project_id,object_class,filename,generated_path,size_bytes,sha256,acquisition_key,pin_id,pin_state FROM generation_input_resources WHERE job_id=$1 ORDER BY field_pointer`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PinnedInputResource
	for rows.Next() {
		var item PinnedInputResource
		var file projectsv1.PreparedFile
		if err = rows.Scan(&file.Pointer, &file.ResourceID, &item.Resource.Scope.WorkspaceID, &item.Resource.Scope.ProjectID, &item.Resource.Class, &file.Filename, &file.Path, &file.SizeBytes, &file.SHA256, &item.AcquisitionKey, &item.PinID, &item.State); err != nil {
			return nil, err
		}
		item.Resource.File = file
		out = append(out, item)
	}
	return out, rows.Err()
}
