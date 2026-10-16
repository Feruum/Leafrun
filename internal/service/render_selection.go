package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

func (s *Server) createRender(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(r)
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authentication is required")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(r.Header.Values("Idempotency-Key")) != 1 || !generationv1.ValidIdempotencyKey(key) {
		writeAPIError(w, http.StatusBadRequest, "invalid_idempotency_key", "A valid Idempotency-Key is required")
		return
	}
	selection, selectionBytes, selectionFingerprint, err := readRenderSelection(w, r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "The render selection is invalid")
		return
	}
	if allowed, accessErr := s.checkAccess(r.Context(), principal, selection.Scope, "generation.create", ""); accessErr != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	} else if !allowed {
		writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
		return
	}
	if selection.ReleaseID == "" {
		if allowed, accessErr := s.checkAccess(r.Context(), principal, selection.Scope, "source.read", selection.SnapshotID); accessErr != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
			return
		} else if !allowed {
			writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
			return
		}
	}
	initiatorID, err := EncodeInitiator(principal)
	if err != nil {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authentication is required")
		return
	}
	workspace, project := selection.Scope.WorkspaceID, selection.Scope.ProjectID
	if admission, replay, lookupErr := s.store.LookupAdmission(r.Context(), workspace, initiatorID, key, selectionFingerprint); lookupErr != nil {
		s.writeStoreError(w, lookupErr)
		return
	} else if replay {
		s.finishRenderAdmission(w, r, principal, admission)
		return
	}
	if s.cfg.Projects == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "projects_unavailable", "Project resolution is temporarily unavailable")
		return
	}
	resolveRequest := projectsv1.ResolveRequest{Authority: authorityRef(r.Context()), Actor: principal, Selection: selection, SelectionFingerprint: selectionFingerprint}
	resolved, err := s.cfg.Projects.Resolve(r.Context(), resolveRequest)
	if err != nil {
		visible, checkErr := s.checkAccess(r.Context(), principal, selection.Scope, "source.read", selection.SnapshotID)
		if checkErr != nil {
			writeAPIError(w, 503, "access_unavailable", "Access checks are temporarily unavailable")
			return
		}
		if visible {
			for _, ref := range selection.FileRefs {
				allowed, err := s.checkAccess(r.Context(), principal, selection.Scope, "resource.read", ref.ResourceID)
				if err != nil {
					writeAPIError(w, 503, "access_unavailable", "Access checks are temporarily unavailable")
					return
				}
				if !allowed {
					visible = false
				}
			}
		}
		s.writeProjectsError(w, err, visible)
		return
	}
	if resolved.Descriptor.Kind == "bundle" {
		writeAPIError(w, http.StatusUnprocessableEntity, "unsupported_target", "Bundle targets are not supported by this render endpoint")
		return
	}
	if err = projectsv1.ValidateResolved(resolveRequest, resolved); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "projects_unavailable", "Project resolution returned invalid execution data")
		return
	}
	if err = s.preflightInputResources(r.Context(), principal, resolved); err != nil {
		var accessDenied *selectionAccessDenied
		if errors.As(err, &accessDenied) {
			writeAPIError(w, http.StatusForbidden, "forbidden", "Access to an input resource is denied")
			return
		}
		var invalidResource *selectionResourceInvalid
		if errors.As(err, &invalidResource) {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_file_reference", "An input resource does not match the resolved file binding")
			return
		}
		writeAPIError(w, http.StatusServiceUnavailable, "storage_unavailable", "Input resource metadata is temporarily unavailable")
		return
	}
	resolvedBytes, executionFingerprint, err := store.NormalizeResolvedExecution(resolved)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "projects_unavailable", "Project resolution returned invalid execution data")
		return
	}
	resources, err := inputResourceIntents(resolved.InputResources)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "The input resource pins could not be prepared")
		return
	}
	input := store.AdmitInput{
		Identity:    requestIdentity(r.Context()),
		WorkspaceID: workspace, ProjectID: project, InitiatorID: initiatorID, Actor: principal,
		IdempotencyKey: key, NormalizedRequest: resolvedBytes, Parameters: append([]byte(nil), resolved.Prepared.JSON...),
		Fingerprint: executionFingerprint, SelectionRequest: selectionBytes, SelectionFingerprint: selectionFingerprint,
		ResolvedExecution: &resolved, InputResources: resources, EngineVersion: resolved.Descriptor.TypstVersion,
		SnapshotID: resolved.SnapshotID, SourceContentID: resolved.ContentID,
		TargetID: resolved.Descriptor.TargetID, Entrypoint: resolved.Descriptor.Entrypoint,
	}
	if selection.ReleaseID != "" {
		decision, checkErr := s.cfg.Projects.CurrentRelease(r.Context(), projectsv1.CurrentReleaseCheck{
			Authority: authorityRef(r.Context()),
			Actor:     principal, Scope: resolved.Scope, ReleaseID: resolved.ReleaseID, DescriptorDigest: resolved.Descriptor.Digest,
		})
		if checkErr != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "projects_unavailable", "Release checks are temporarily unavailable")
			return
		}
		if !decision.Allowed {
			writeAPIError(w, http.StatusConflict, "selection_conflict", "The selected project state changed")
			return
		}
	}
	admission, _, err := s.store.Admit(r.Context(), input)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.finishRenderAdmission(w, r, principal, admission)
}

func readRenderSelection(w http.ResponseWriter, r *http.Request) (generationv1.RenderSelectionRequest, []byte, string, error) {
	if err := authn.RequireMediaType(r, "application/json"); err != nil || r.ContentLength > jsonvalue.MaxBytes {
		return generationv1.RenderSelectionRequest{}, nil, "", errors.New("application/json selection body is required")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, jsonvalue.MaxBytes))
	if err != nil {
		return generationv1.RenderSelectionRequest{}, nil, "", err
	}
	request, normalized, fingerprint, err := generationv1.NormalizeSelection(body)
	if err != nil {
		return generationv1.RenderSelectionRequest{}, nil, "", err
	}
	return request, normalized, fingerprint, nil
}

func (s *Server) finishRenderAdmission(w http.ResponseWriter, r *http.Request, principal storagev1.Principal, admission generationv1.Admission) {
	if admission.Job.Status.Terminal() {
		if err := s.redactSourceFields(r.Context(), principal, &admission.Job); err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
			return
		}
		writeJSON(w, http.StatusAccepted, admission)
		return
	}
	if err := s.EnsureSourcePin(r.Context(), admission.Job); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "admission_pending", "The job is saved and can be retried with the same idempotency key")
		return
	}
	active, err := s.store.AdmissionActivated(r.Context(), admission.Job.JobID)
	if err != nil || !active {
		writeAPIError(w, http.StatusServiceUnavailable, "admission_pending", "The job is saved and can be retried with the same idempotency key")
		return
	}
	if err = s.redactSourceFields(r.Context(), principal, &admission.Job); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, admission)
}

type selectionAccessDenied struct{}

func (*selectionAccessDenied) Error() string { return "input resource access denied" }

type selectionResourceInvalid struct{}

func (*selectionResourceInvalid) Error() string { return "input resource metadata mismatch" }

func (s *Server) preflightInputResources(ctx context.Context, principal storagev1.Principal, resolved projectsv1.ResolvedExecution) error {
	if len(resolved.InputResources) == 0 {
		return nil
	}
	if s.cfg.Resources == nil {
		return errors.New("Storage resource metadata reader is unavailable")
	}
	for _, input := range resolved.InputResources {
		file := input.File
		allowed, err := s.checkAccess(ctx, principal, input.Scope, "resource.read", file.ResourceID)
		if err != nil {
			return fmt.Errorf("check input resource rights: %w", err)
		}
		if !allowed {
			return &selectionAccessDenied{}
		}
		resource, err := s.cfg.Resources.GetResource(ctx, file.ResourceID)
		if err != nil {
			var status *HTTPStatusError
			if errors.As(err, &status) && (status.StatusCode == http.StatusNotFound || status.StatusCode == http.StatusGone) {
				return &selectionResourceInvalid{}
			}
			return fmt.Errorf("read input resource metadata: %w", err)
		}
		if resource.ResourceID != file.ResourceID || resource.Scope != input.Scope || resource.Class != input.Class || resource.Class != storagev1.ClassResource || resource.SizeBytes != file.SizeBytes || resource.SHA256 != file.SHA256 {
			return &selectionResourceInvalid{}
		}
	}
	return nil
}

func inputResourceIntents(resources []projectsv1.InputResource) ([]store.InputResourceIntent, error) {
	byResource := make(map[string]string, len(resources))
	byKey := make(map[string]string, len(resources))
	out := make([]store.InputResourceIntent, 0, len(resources))
	for _, resource := range resources {
		resourceID := resource.File.ResourceID
		key := byResource[resourceID]
		if key == "" {
			var err error
			key, err = newPinKey()
			if err != nil {
				return nil, err
			}
			byResource[resourceID] = key
			byKey[key] = resourceID
		} else if byKey[key] != resourceID {
			return nil, errors.New("input resources share a pin key")
		}
		out = append(out, store.InputResourceIntent{Resource: resource, AcquisitionKey: key})
	}
	return out, nil
}

func (s *Server) writeProjectsError(w http.ResponseWriter, err error, sourceVisible ...bool) {
	var response *projectsv1.HTTPError
	if !errors.As(err, &response) {
		writeAPIError(w, http.StatusServiceUnavailable, "projects_unavailable", "Project resolution is temporarily unavailable")
		return
	}
	status, code, message := http.StatusServiceUnavailable, "projects_unavailable", "Project resolution is temporarily unavailable"
	switch response.Status {
	case http.StatusBadRequest:
		status, code, message = http.StatusBadRequest, "invalid_selection", "The render selection is invalid"
	case http.StatusForbidden:
		status, code, message = http.StatusForbidden, "forbidden", "Access is denied"
	case http.StatusNotFound:
		status, code, message = http.StatusNotFound, "not_found", "The selected project or release was not found"
	case http.StatusConflict:
		status, code, message = http.StatusConflict, "selection_conflict", "The selected project state changed"
	case http.StatusRequestEntityTooLarge:
		status, code, message = http.StatusRequestEntityTooLarge, "selection_too_large", "The render selection exceeds the supported limit"
	case http.StatusUnprocessableEntity:
		status, code, message = http.StatusUnprocessableEntity, "invalid_selection", "The render selection could not be resolved"
		if response.Code == "unsupported_target" {
			code, message = "unsupported_target", "Bundle targets are not supported by this render endpoint"
		}
	}
	fields := make([]generationv1.FieldError, 0, len(response.Fields))
	if len(sourceVisible) == 1 && sourceVisible[0] {
		for _, field := range response.Fields {
			fields = append(fields, generationv1.FieldError{File: field.File, Pointer: field.Pointer, Code: field.Code, Message: field.Message})
		}
	}
	writeAPIErrorFields(w, status, code, message, fields)
}
