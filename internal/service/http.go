package service

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type apiError struct {
	Error struct {
		Code    string                    `json:"code"`
		Message string                    `json:"message"`
		Fields  []generationv1.FieldError `json:"fields,omitempty"`
	} `json:"error"`
}

func (s *Server) authenticate(r *http.Request) (storagev1.Principal, bool) {
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
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	return len(token) == len(s.cfg.OwnerToken) && subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.OwnerToken)) == 1
}

func (s *Server) checkAccess(ctx context.Context, principal storagev1.Principal, scope storagev1.Scope, operation, objectID string) (bool, error) {
	if operation == "" {
		return false, errors.New("Access operation is required")
	}
	return s.cfg.Access.Check(ctx, storagev1.AccessCheck{Principal: principal, Scope: scope, Operation: operation, ObjectID: objectID})
}

func readCreateRequest(w http.ResponseWriter, r *http.Request) (generationv1.CreateJobRequest, []byte, string, error) {
	var request generationv1.CreateJobRequest
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
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
	if !allowed {
		job.Diagnostics = nil
		job.RawDiagnostics = ""
		job.Parameters = nil
	}
	return nil
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
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
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
