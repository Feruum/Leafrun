package service

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

type apiError struct {
	Error struct {
		Code    string                    `json:"code"`
		Message string                    `json:"message"`
		Fields  []generationv1.FieldError `json:"fields,omitempty"`
	} `json:"error"`
}

func (s *Server) authenticate(r *http.Request) (storagev1.Principal, bool) {
	if s.cfg.AuthMode == "access" {
		id, ok := authn.IdentityFromContext(r.Context())
		return id.Principal, ok
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return storagev1.Principal{}, false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n,") {
		return storagev1.Principal{}, false
	}
	return s.cfg.Credentials.ResolveToken(token)
}

func (s *Server) authenticateOwner(r *http.Request) bool {
	in, err := authn.CredentialsWithCookieNames(r, authn.ConfiguredCookieNames(s.cfg.CookieName, s.cfg.GuestCookieName))
	if err != nil || in.SessionCookie != "" {
		return false
	}
	token := in.Bearer
	return len(token) == len(s.cfg.OwnerToken) && subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.OwnerToken)) == 1
}

func (s *Server) checkAccess(ctx context.Context, principal storagev1.Principal, scope storagev1.Scope, operation, objectID string) (bool, error) {
	if operation == "" {
		return false, errors.New("Access operation is required")
	}
	in := storagev1.AccessCheck{Principal: principal, Scope: scope, Operation: operation, ObjectID: objectID}
	if id, ok := authn.IdentityFromContext(ctx); ok {
		if id.Principal != principal {
			return false, errors.New("identity binding mismatch")
		}
		ref := id.Authority
		in.Authority = &ref
	} else if s.cfg.AuthMode == "access" {
		return false, errors.New("current authority required")
	}
	return s.cfg.Access.Check(ctx, in)
}

func readCreateRequest(w http.ResponseWriter, r *http.Request) (generationv1.CreateJobRequest, []byte, string, error) {
	var request generationv1.CreateJobRequest
	if err := authn.RequireMediaType(r, "application/json"); err != nil {
		return request, nil, "", errors.New("application/json content type is required")
	}
	if r.ContentLength > jsonvalue.MaxBytes {
		return request, nil, "", errors.New("request exceeds 1 MiB")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, jsonvalue.MaxBytes))
	if err != nil {
		return request, nil, "", err
	}
	if err = jsonvalue.Unmarshal(body, &request, jsonvalue.MaxBytes); err != nil {
		return request, nil, "", err
	}
	request, normalized, fingerprint, err := generationv1.NormalizeRequest(body)
	if err != nil {
		return generationv1.CreateJobRequest{}, nil, "", err
	}
	return request, normalized, fingerprint, nil
}

func (s *Server) serveJobRoute(w http.ResponseWriter, r *http.Request) {
	escaped := r.URL.EscapedPath()
	const prefix = "/v1/jobs/"
	if !strings.HasPrefix(escaped, prefix) {
		writeAPIError(w, http.StatusNotFound, "not_found", "The requested resource was not found")
		return
	}
	parts := strings.Split(strings.TrimPrefix(escaped, prefix), "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		writeAPIError(w, http.StatusNotFound, "not_found", "The requested resource was not found")
		return
	}
	jobID, err := url.PathUnescape(parts[0])
	if err != nil || !validJobID(jobID) {
		writeAPIError(w, http.StatusNotFound, "not_found", "The requested resource was not found")
		return
	}
	if len(parts) == 2 {
		if parts[1] != "cancel" || r.Method != http.MethodPost {
			writeAPIError(w, http.StatusNotFound, "not_found", "The requested resource was not found")
			return
		}
		s.cancelJob(w, r, jobID)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "The HTTP method is not supported")
		return
	}
	s.getJob(w, r, jobID)
}

func validJobID(jobID string) bool {
	return validIdentifier(jobID) && !strings.ContainsAny(jobID, "/\\")
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, jobID string) {
	principal, ok := s.authenticate(r)
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authentication is required")
		return
	}
	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	allowed, err := s.checkAccess(r.Context(), principal, job.Scope, "generation.read", job.JobID)
	if err == nil && allowed {
		allowed, err = s.jobCallerBinding(r.Context(), principal, job)
	}
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	}
	if !allowed {
		writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
		return
	}
	if err := s.redactSourceFields(r.Context(), principal, &job); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request, jobID string) {
	if r.Body != nil {
		b, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(b) != 0 {
			writeAPIError(w, 400, "invalid_request", "Cancellation requires an empty body")
			return
		}
	}
	principal, ok := s.authenticate(r)
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authentication is required")
		return
	}
	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	allowed, err := s.checkAccess(r.Context(), principal, job.Scope, "generation.cancel", job.JobID)
	if err == nil && allowed {
		allowed, err = s.jobCallerBinding(r.Context(), principal, job)
	}
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	}
	if !allowed {
		writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
		return
	}
	if _, err = s.store.RequestCancel(r.Context(), jobID); err != nil {
		s.writeStoreError(w, err)
		return
	}
	job, err = s.store.GetJob(r.Context(), jobID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err = s.redactSourceFields(r.Context(), principal, &job); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) redactSourceFields(ctx context.Context, principal storagev1.Principal, job *generationv1.Job) error {
	allowed, err := s.checkAccess(ctx, principal, job.Scope, "source.read", job.SourceSnapshotID)
	if err != nil {
		return err
	}
	// Native diagnostics/prepared JSON can depend on independently selected
	// file resources. Historical visibility does not require their pins active.
	if allowed {
		if state, ok := s.store.(authorityLoader); ok {
			binding, loadErr := state.LoadAuthority(ctx, job.JobID)
			if loadErr != nil {
				return loadErr
			}
			for _, ref := range binding.Resources {
				var checkErr error
				allowed, checkErr = s.checkAccess(ctx, principal, ref.Scope, "resource.read", ref.ResourceID)
				if checkErr != nil {
					return checkErr
				}
				if !allowed {
					break
				}
			}
		} else if s.cfg.AuthMode == "access" {
			allowed = false
		}
	}
	if !allowed {
		job.Diagnostics = nil
		job.RawDiagnostics = ""
		job.Parameters = nil
		job.Entrypoint = ""
		job.DiagnosticsVisibility = "restricted"
		job.Phase = safePhase(job.Phase)
		job.FailureCode = safeFailureCode(job.FailureCode)
	}
	return nil
}

func safePhase(phase string) string {
	switch phase {
	case "pin_pending", "workflow_pending", "queued", "preparing", "rendering", "publishing", "completed", "complete", "compile", "export", "storage", "infrastructure", "access", "publication", "render", "resource_limit", "timeout", "canceling", "cancelling":
		return phase
	}
	return "completed"
}
func safeFailureCode(code string) string {
	switch code {
	case "", "typst_compile_failed", "compile_failed", "typst_export_failed", "export_failed", "access_denied", "resource_limit", "timeout", "renderer_failure", "artifact_expired", "artifact_upload_failed", "staging_unavailable", "render_failed", "canceled", "expired":
		return code
	}
	return "render_failed"
}

func (s *Server) publishable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "The HTTP method is not supported")
		return
	}
	if !s.authenticateOwner(r) {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Owner authentication is required")
		return
	}
	var request storagev1.PublishabilityCheck
	if err := readStrictJSON(w, r, &request); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "The owner check request is invalid")
		return
	}
	allowed, err := s.owner.Publishable(r.Context(), request)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "owner_unavailable", "Generation owner checks are temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, storagev1.PublishabilityDecision{Publishable: allowed})
}

func (s *Server) pinOwner(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "The HTTP method is not supported")
		return
	}
	if !s.authenticateOwner(r) {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Owner authentication is required")
		return
	}
	var request storagev1.PinOwnerCheck
	if err := readStrictJSON(w, r, &request); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "The owner check request is invalid")
		return
	}
	state, err := s.owner.Pin(r.Context(), request)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, storagev1.PinOwnerDecision{State: storagev1.OwnerUnknown})
		return
	}
	if state != storagev1.OwnerReferenced && state != storagev1.OwnerAbsent && state != storagev1.OwnerUnknown {
		writeJSON(w, http.StatusServiceUnavailable, storagev1.PinOwnerDecision{State: storagev1.OwnerUnknown})
		return
	}
	writeJSON(w, http.StatusOK, storagev1.PinOwnerDecision{State: state})
}

func readStrictJSON(w http.ResponseWriter, r *http.Request, target any) error {
	if err := authn.RequireMediaType(r, "application/json"); err != nil {
		return errors.New("application/json content type required")
	}
	if r.ContentLength > jsonvalue.MaxBytes {
		return errors.New("request too large")
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, jsonvalue.MaxBytes))
	if err != nil {
		return err
	}
	return jsonvalue.Unmarshal(data, target, jsonvalue.MaxBytes)
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrAuthorityDenied):
		writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
	case errors.Is(err, store.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "not_found", "The requested resource was not found")
	case errors.Is(err, store.ErrConflict):
		writeAPIError(w, http.StatusConflict, "idempotency_conflict", "The idempotency key is bound to a different request")
	case errors.Is(err, store.ErrCapacity):
		writeAPIError(w, http.StatusTooManyRequests, "capacity_exceeded", "Generation capacity is full")
	case errors.Is(err, store.ErrInvalidTransition), errors.Is(err, store.ErrReceiptExpired):
		writeAPIError(w, http.StatusConflict, "invalid_state", "The requested state transition is not available")
	default:
		writeAPIError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "Generation is temporarily unavailable")
	}
}

func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	writeAPIErrorFields(w, status, code, message, nil)
}

func writeAPIErrorFields(w http.ResponseWriter, status int, code, message string, fields []generationv1.FieldError) {
	var response apiError
	response.Error.Code = code
	response.Error.Message = message
	response.Error.Fields = fields
	writeJSON(w, status, response)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
