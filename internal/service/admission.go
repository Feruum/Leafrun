package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const (
	ControllerTaskQueue = "generation-controller"
	maxPinRotations     = 3
)

var errEntrypointMissing = errors.New("entrypoint is not present in the source snapshot")

func validateSourceSnapshot(snapshot storagev1.Snapshot, request generationv1.CreateJobRequest) error {
	if snapshot.SnapshotID != request.SnapshotID || snapshot.Scope != request.Scope || storagev1.ValidateManifest(snapshot.Manifest) != nil {
		return errors.New("Storage snapshot metadata failed identity validation")
	}
	for _, file := range snapshot.Manifest.Files {
		if file.Path == request.Entrypoint {
			return nil
		}
	}
	return errEntrypointMissing
}

type Server struct {
	store AdmissionStore
	owner OwnerStore
	cfg   Config
}

func New(state AdmissionStore, owner OwnerStore, config Config) (*Server, error) {
	if state == nil || owner == nil {
		return nil, errors.New("Generation store and owner checks are required")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Server{store: state, owner: owner, cfg: config}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/healthz" && r.Method == http.MethodGet:
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/readyz" && r.Method == http.MethodGet:
		s.ready(w, r)
	case r.URL.Path == "/v1/jobs" && r.Method == http.MethodPost:
		s.createJob(w, r)
	case r.URL.Path == "/internal/v1/checks/publishable" && r.Method == http.MethodPost:
		s.publishable(w, r)
	case r.URL.Path == "/internal/v1/checks/pin-owner" && r.Method == http.MethodPost:
		s.pinOwner(w, r)
	default:
		s.serveJobRoute(w, r)
	}
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if s.cfg.Ready == nil || s.cfg.Ready(ctx) != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "Generation is not ready")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
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
	request, normalized, fingerprint, err := readCreateRequest(w, r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "The request body is invalid")
		return
	}
	if allowed, err := s.checkAccess(r.Context(), principal, request.Scope, "generation.create", ""); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	} else if !allowed {
		writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
		return
	}
	if allowed, err := s.checkAccess(r.Context(), principal, request.Scope, "source.read", request.SnapshotID); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "access_unavailable", "Access checks are temporarily unavailable")
		return
	} else if !allowed {
		writeAPIError(w, http.StatusForbidden, "forbidden", "Access is denied")
		return
	}
	initiatorID, err := EncodeInitiator(principal)
	if err != nil {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authentication is required")
		return
	}
	input := store.AdmitInput{
		WorkspaceID: request.Scope.WorkspaceID, ProjectID: request.Scope.ProjectID,
		InitiatorID: initiatorID, IdempotencyKey: key, NormalizedRequest: normalized,
		Parameters: request.Parameters, Fingerprint: fingerprint, EngineVersion: s.cfg.EngineVersion,
		SnapshotID: request.SnapshotID, TargetID: request.TargetID, Entrypoint: request.Entrypoint,
	}
	// A live, exact receipt is authoritative for replay. Its source snapshot can
	// have expired since the initial admission, so only fresh admissions inspect
	// source metadata before asking Store to reserve queue capacity.
	_, replay, err := s.store.LookupAdmission(r.Context(), input.WorkspaceID, input.InitiatorID, input.IdempotencyKey, input.Fingerprint)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !replay {
		snapshot, snapshotErr := s.cfg.Pins.GetSnapshot(r.Context(), request.SnapshotID)
		if snapshotErr != nil {
			if isHTTPStatus(snapshotErr, http.StatusNotFound) || isHTTPStatus(snapshotErr, http.StatusGone) {
				// Close the race where an identical request became durable after
				// the first receipt lookup but before Storage answered.
				_, replay, err = s.store.LookupAdmission(r.Context(), input.WorkspaceID, input.InitiatorID, input.IdempotencyKey, input.Fingerprint)
				if err != nil {
					s.writeStoreError(w, err)
					return
				}
				if !replay {
					writeAPIError(w, http.StatusNotFound, "snapshot_not_found", "The source snapshot was not found")
					return
				}
			} else {
				writeAPIError(w, http.StatusServiceUnavailable, "storage_unavailable", "Storage is temporarily unavailable")
				return
			}
		} else if err = validateSourceSnapshot(snapshot, request); err != nil {
			if errors.Is(err, errEntrypointMissing) {
				writeAPIError(w, http.StatusUnprocessableEntity, "entrypoint_not_found", "The entrypoint is not present in the source snapshot")
				return
			}
			writeAPIError(w, http.StatusServiceUnavailable, "storage_unavailable", "Storage returned invalid snapshot metadata")
			return
		}
	}
	admission, _, err := s.store.Admit(r.Context(), input)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err = s.EnsureSourcePin(r.Context(), admission.Job); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "admission_pending", "The job is saved and can be retried with the same idempotency key")
		return
	}
	active, err := s.store.AdmissionActivated(r.Context(), admission.Job.JobID)
	if err != nil || !active {
		writeAPIError(w, http.StatusServiceUnavailable, "admission_pending", "The job is saved and can be retried with the same idempotency key")
		return
	}
	writeJSON(w, http.StatusAccepted, admission)
}

// EnsureSourcePin recovers one admission from its persisted pin intent. It is
// shared by HTTP retries and the admission reconciler.
func (s *Server) EnsureSourcePin(ctx context.Context, job generationv1.Job) error {
	return s.ensureSnapshotPin(ctx, job, job.SourceSnapshotID, true)
}

// EnsureDerivedPin acquires and activates the ordinary pin for a derived
// snapshot already recorded on the job. Store validates the job/epoch/state
// binding before accepting the pin receipt.
func (s *Server) EnsureDerivedPin(ctx context.Context, job generationv1.Job, snapshotID string) error {
	if snapshotID == "" || snapshotID == job.SourceSnapshotID {
		return errors.New("a distinct derived snapshot ID is required")
	}
	return s.ensureSnapshotPin(ctx, job, snapshotID, false)
}

func (s *Server) ensureSnapshotPin(ctx context.Context, job generationv1.Job, snapshotID string, source bool) error {
	if !validIdentifier(job.JobID) || !validIdentifier(snapshotID) {
		return errors.New("job and snapshot IDs are required")
	}
	if source {
		active, err := s.store.AdmissionActivated(ctx, job.JobID)
		if err != nil || active {
			return err
		}
	}
	for rotations := 0; rotations <= maxPinRotations; rotations++ {
		if source {
			active, err := s.store.AdmissionActivated(ctx, job.JobID)
			if err != nil || active {
				return err
			}
		}
		acquire, err := s.store.LatestPinAcquire(ctx, job.JobID, snapshotID)
		if errors.Is(err, store.ErrNotFound) || err == nil && acquire.State == "failed" {
			acquire, err = s.beginPinIntent(ctx, job, snapshotID)
			if errors.Is(err, store.ErrInvalidTransition) {
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("load or save source pin intent: %w", err)
		}
		request, err := decodePinRequest(acquire.Request)
		if err != nil || request.Scope != job.Scope || request.ObjectID != snapshotID || request.IdempotencyKey != acquire.Key {
			return errors.New("persisted source pin intent does not match the job")
		}

		var pin storagev1.Pin
		if acquire.Response != nil {
			pin, err = s.cfg.Pins.GetPin(ctx, acquire.Response.PinID)
			if isExpiredPinError(err) {
				if e := s.store.RecordPinAcquireExpired(ctx, job.JobID, acquire.Key); e != nil {
					return fmt.Errorf("record expired source pin: %w", e)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("read source pin: %w", err)
			}
		} else {
			pin, err = s.cfg.Pins.AcquirePin(ctx, request)
			if isExpiredPinError(err) {
				if e := s.store.RecordPinAcquireExpired(ctx, job.JobID, acquire.Key); e != nil {
					return fmt.Errorf("record expired source pin: %w", e)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("acquire source pin: %w", err)
			}
		}
		if err = validatePin(request, pin); err != nil {
			return err
		}
		if err = s.store.RecordPinAcquireResponse(ctx, job.JobID, acquire.Key, pin); err != nil {
			return fmt.Errorf("persist snapshot pin receipt: %w", err)
		}
		// Storage checks expiry as well as state when activation is repeated for
		// an already-active pin, so its response remains authoritative here.
		activated, activateErr := s.cfg.Pins.ActivatePin(ctx, pin.PinID)
		if activateErr != nil {
			if isExpiredPinError(activateErr) {
				if e := s.store.RecordPinAcquireExpired(ctx, job.JobID, acquire.Key); e != nil {
					return fmt.Errorf("record expired snapshot pin: %w", e)
				}
				continue
			}
			var rejection *HTTPStatusError
			if errors.As(activateErr, &rejection) {
				return fmt.Errorf("Storage rejected snapshot pin activation: %w", activateErr)
			}
			// Only an ambiguous transport failure can hide a successful response.
			activated, err = s.cfg.Pins.GetPin(ctx, pin.PinID)
			if isExpiredPinError(err) {
				if e := s.store.RecordPinAcquireExpired(ctx, job.JobID, acquire.Key); e != nil {
					return fmt.Errorf("record expired source pin: %w", e)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("recover source pin activation: %w", err)
			}
		}
		pin = activated
		if err = validatePin(request, pin); err != nil || pin.State != "active" {
			return errors.New("Storage did not confirm an active snapshot pin")
		}
		if err = s.store.RecordPinAcquireResponse(ctx, job.JobID, acquire.Key, pin); err != nil {
			return fmt.Errorf("persist active source pin: %w", err)
		}
		if source {
			workflowRequest, requestErr := workflowRequestJSON(job.JobID)
			if requestErr != nil {
				return requestErr
			}
			err = s.store.MarkPinActiveAndRecordWorkflowStart(ctx, job.JobID, acquire.Key, "render/"+job.JobID, pin, workflowRequest)
		} else {
			err = s.store.MarkPinActive(ctx, job.JobID, acquire.Key, pin)
		}
		if err != nil {
			if source {
				active, checkErr := s.store.AdmissionActivated(ctx, job.JobID)
				if checkErr == nil && active {
					return nil
				}
			}
			return fmt.Errorf("record active snapshot pin: %w", err)
		}
		return nil
	}
	return errors.New("source pin expired repeatedly before activation")
}

func (s *Server) beginPinIntent(ctx context.Context, job generationv1.Job, snapshotID string) (store.PinAcquire, error) {
	key, err := newPinKey()
	if err != nil {
		return store.PinAcquire{}, err
	}
	request := storagev1.AcquirePinRequest{
		Scope: job.Scope, ObjectID: snapshotID, HolderID: "leafrun-generation",
		HolderDomain: "service", RetentionPolicy: "ordinary", IdempotencyKey: key,
	}
	// Use the exact default Store holder. The Store rejects configuration drift
	// instead of allowing a request to impersonate another service principal.
	return s.store.BeginPinAcquireIntent(ctx, store.PinAcquireIntent{JobID: job.JobID, IdempotencyKey: key, Request: request})
}

func newPinKey() (string, error) {
	var random [20]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "genpin_" + hex.EncodeToString(random[:]), nil
}

func workflowRequestJSON(jobID string) ([]byte, error) {
	if !validIdentifier(jobID) {
		return nil, errors.New("valid job ID required for workflow request")
	}
	return json.Marshal(struct {
		JobID string `json:"jobId"`
	}{JobID: jobID})
}

func decodePinRequest(data []byte) (storagev1.AcquirePinRequest, error) {
	var request storagev1.AcquirePinRequest
	err := jsonvalue.Unmarshal(data, &request, 1<<20)
	return request, err
}

func validatePin(request storagev1.AcquirePinRequest, pin storagev1.Pin) error {
	if pin.PinID == "" || pin.Scope != request.Scope || pin.ObjectID != request.ObjectID || pin.HolderID != request.HolderID || pin.HolderDomain != request.HolderDomain || pin.RetentionPolicy != request.RetentionPolicy || pin.ExpiresAt.IsZero() || (pin.State != "pending" && pin.State != "active" && pin.State != "released") {
		return errors.New("Storage returned a source pin with mismatched bindings")
	}
	if pin.State == "released" {
		return errors.New("Storage returned a released snapshot pin")
	}
	return nil
}

func isExpiredPinError(err error) bool {
	var response *HTTPStatusError
	return errors.As(err, &response) && response.StatusCode == http.StatusConflict && response.Code == "pin_expired"
}

func isHTTPStatus(err error, status int) bool {
	var response *HTTPStatusError
	return errors.As(err, &response) && response.StatusCode == status
}

func validIdentifier(value string) bool {
	return len(value) > 0 && len(value) <= 256 && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}
