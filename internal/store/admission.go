package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

// Admit creates the job, seven-day receipt and queue reservation in one
// transaction. It returns replay=true when an unexpired matching receipt exists.
func (s *Store) Admit(ctx context.Context, in AdmitInput) (generationv1.Admission, bool, error) {
	var out generationv1.Admission
	if err := validateAdmission(in); err != nil {
		return out, false, err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, false, err
	}
	defer rollback(ctx, tx)
	if err = lockQueue(ctx, tx); err != nil {
		return out, false, err
	}
	var oldFingerprint, oldJob string
	var expiry time.Time
	err = tx.QueryRow(ctx, `SELECT fingerprint,job_id,expires_at FROM generation_admission_receipts WHERE workspace_id=$1 AND initiator_id=$2 AND idempotency_key=$3 FOR UPDATE`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey).Scan(&oldFingerprint, &oldJob, &expiry)
	if err == nil {
		var live bool
		if e := tx.QueryRow(ctx, `SELECT expires_at > clock_timestamp() FROM generation_admission_receipts WHERE workspace_id=$1 AND initiator_id=$2 AND idempotency_key=$3`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey).Scan(&live); e != nil {
			return out, false, e
		}
		if live {
			if oldFingerprint != in.Fingerprint {
				return out, false, ErrConflict
			}
			j, e := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE job_id=$1`, oldJob))
			if e != nil {
				return out, false, fmt.Errorf("load admission replay: %w", e)
			}
			if e = tx.QueryRow(ctx, `SELECT expires_at FROM generation_admission_receipts WHERE job_id=$1`, oldJob).Scan(&out.IdempotencyExpiresAt); e != nil {
				return out, false, e
			}
			if e = commit(ctx, tx); e != nil {
				return out, false, e
			}
			out.Job = j.dto()
			return out, true, nil
		}
		if _, e := tx.Exec(ctx, `DELETE FROM generation_admission_receipts WHERE workspace_id=$1 AND initiator_id=$2 AND idempotency_key=$3`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey); e != nil {
			return out, false, e
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return out, false, fmt.Errorf("read admission receipt: %w", err)
	}
	var globalQueued, workspaceQueued int
	if err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE reservation_type='queue'),count(*) FILTER(WHERE reservation_type='queue' AND workspace_id=$1) FROM generation_reservations`, in.WorkspaceID).Scan(&globalQueued, &workspaceQueued); err != nil {
		return out, false, fmt.Errorf("count queued generation jobs: %w", err)
	}
	if globalQueued >= s.cfg.QueueGlobal || workspaceQueued >= s.cfg.QueueWorkspace {
		return out, false, ErrCapacity
	}
	jobID, e := newID("gen_")
	if e != nil {
		return out, false, e
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_jobs(job_id,workspace_id,project_id,initiator_id,idempotency_key,normalized_request,parameters,fingerprint,source_snapshot_id,target_id,entrypoint,engine_version,status,phase,deadline)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'queued','pin_pending',clock_timestamp()+interval '15 minutes')`, jobID, in.WorkspaceID, in.ProjectID, in.InitiatorID, in.IdempotencyKey, in.NormalizedRequest, in.Parameters, in.Fingerprint, in.SnapshotID, in.TargetID, in.Entrypoint, in.EngineVersion)
	if err != nil {
		return out, false, fmt.Errorf("insert generation job: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO generation_reservations(job_id,workspace_id,reservation_type) VALUES($1,$2,'queue')`, jobID, in.WorkspaceID); err != nil {
		return out, false, fmt.Errorf("reserve generation queue slot: %w", err)
	}
	if err = tx.QueryRow(ctx, `INSERT INTO generation_admission_receipts(workspace_id,initiator_id,idempotency_key,fingerprint,job_id,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6*interval '1 microsecond') RETURNING expires_at`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey, in.Fingerprint, jobID, s.cfg.ReceiptTTL.Microseconds()).Scan(&out.IdempotencyExpiresAt); err != nil {
		return out, false, fmt.Errorf("persist admission receipt: %w", err)
	}
	j, e := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE job_id=$1`, jobID))
	if e != nil {
		return out, false, e
	}
	if err = commit(ctx, tx); err != nil {
		return out, false, err
	}
	out.Job = j.dto()
	return out, false, nil
}

func validateAdmission(in AdmitInput) error {
	if len(in.NormalizedRequest) == 0 || len(in.NormalizedRequest) > 1<<20 || len(in.Parameters) > 1<<20 {
		return errors.New("normalized request and parameters must be bounded nonempty request data")
	}
	if in.WorkspaceID == "" || in.ProjectID == "" || in.InitiatorID == "" || !generationv1.ValidIdempotencyKey(in.IdempotencyKey) {
		return errors.New("workspace, project, initiator and valid idempotency key are required")
	}
	if strings.TrimSpace(in.EngineVersion) == "" || len(in.EngineVersion) > 128 {
		return errors.New("bounded engine version is required")
	}
	request, normalized, fingerprint, err := generationv1.NormalizeRequest(in.NormalizedRequest)
	if err != nil {
		return fmt.Errorf("validate normalized generation request: %w", err)
	}
	if !bytes.Equal(normalized, in.NormalizedRequest) || fingerprint != in.Fingerprint {
		return errors.New("request bytes or fingerprint are not the Contracts canonical form")
	}
	if request.Scope.WorkspaceID != in.WorkspaceID || request.Scope.ProjectID != in.ProjectID {
		return errors.New("request scope does not match admission scope")
	}
	if request.SnapshotID != in.SnapshotID || request.TargetID != in.TargetID || request.Entrypoint != in.Entrypoint {
		return errors.New("admission identity does not match the normalized request")
	}
	if !bytes.Equal(request.Parameters, in.Parameters) {
		return errors.New("parameter bytes do not match the normalized request")
	}
	return nil
}

// GetJob returns the durable public job fields. Authorization and response
// redaction remain the HTTP/service adapter's responsibility.
func (s *Store) GetJob(ctx context.Context, jobID string) (generationv1.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, jobSelect+` WHERE job_id=$1`, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return generationv1.Job{}, ErrNotFound
	}
	if err != nil {
		return generationv1.Job{}, fmt.Errorf("get generation job: %w", err)
	}
	return j.dto(), nil
}

type PinAcquire struct {
	Number    int
	Key       string
	Request   []byte
	Response  *storagev1.Pin
	State     string
	ExpiresAt *time.Time
}

// LatestPinAcquire recovers the exact persisted request after a lost response.
func (s *Store) LatestPinAcquire(ctx context.Context, jobID, objectID string) (PinAcquire, error) {
	var out PinAcquire
	var response []byte
	err := s.pool.QueryRow(ctx, `SELECT acquire_number,idempotency_key,request,response,state,expires_at FROM generation_pin_acquires WHERE job_id=$1 AND object_id=$2 ORDER BY acquire_number DESC LIMIT 1`, jobID, objectID).Scan(&out.Number, &out.Key, &out.Request, &response, &out.State, &out.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if len(response) > 0 {
		var pin storagev1.Pin
		if err = jsonvalue.Unmarshal(response, &pin, 1<<20); err != nil {
			return out, err
		}
		out.Response = &pin
	}
	return out, nil
}

// RecordPinAcquireExpired records an authoritative Storage 410 for this exact
// acquisition. It also resolves an unknown expiry after a lost acquire reply.
// Transient errors must never be reported through this method.
func (s *Store) RecordPinAcquireExpired(ctx context.Context, jobID, key string) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var locked string
	if err = tx.QueryRow(ctx, `SELECT job_id FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&locked); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, key).Scan(&state); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if state != "pending" && state != "failed" {
		return ErrInvalidTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET state='failed',expires_at=clock_timestamp(),updated_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key=$2`, jobID, key); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) AdmissionActivated(ctx context.Context, jobID string) (bool, error) {
	var yes bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_workflow_starts WHERE job_id=$1)`, jobID).Scan(&yes)
	return yes, err
}

func (s *Store) PendingAdmissions(ctx context.Context, limit int) ([]string, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("admission batch must be between 1 and 100")
	}
	rows, err := s.pool.Query(ctx, `SELECT j.job_id FROM generation_jobs j WHERE j.status='queued' AND j.phase='pin_pending' AND j.deadline>clock_timestamp() ORDER BY j.created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// BeginPinAcquireIntent persists the exact Storage request before calling
// AcquirePin. A pending unexpired acquisition cannot be rotated to a new key.
func (s *Store) BeginPinAcquireIntent(ctx context.Context, in PinAcquireIntent) (PinAcquire, error) {
	var out PinAcquire
	if in.JobID == "" || !generationv1.ValidIdempotencyKey(in.IdempotencyKey) || in.Request.IdempotencyKey != in.IdempotencyKey {
		return out, errors.New("job, pin idempotency key and matching request are required")
	}
	if in.Request.HolderID != s.cfg.PinHolderID || in.Request.HolderDomain != s.cfg.PinHolderDomain {
		return out, errors.New("pin request must use the fixed Generation service principal")
	}
	requestBytes := append([]byte(nil), in.RequestBytes...)
	if len(requestBytes) == 0 {
		var err error
		requestBytes, err = json.Marshal(in.Request)
		if err != nil {
			return out, err
		}
	}
	var decoded storagev1.AcquirePinRequest
	if err := jsonvalue.Unmarshal(requestBytes, &decoded, 1<<20); err != nil || decoded != in.Request {
		return out, errors.New("pin request bytes must match the typed Storage request")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	var workspace, project, sourceSnapshot, derivedSnapshot, status string
	var deadlineOK bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,source_snapshot_id,derived_snapshot_id,status,deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&workspace, &project, &sourceSnapshot, &derivedSnapshot, &status, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if status != "queued" && status != "preparing" && status != "publishing" {
		return out, ErrInvalidTransition
	}
	if !deadlineOK {
		return out, ErrInvalidTransition
	}
	if in.Request.Scope != (storagev1.Scope{WorkspaceID: workspace, ProjectID: project}) {
		return out, errors.New("pin request does not match the admitted scope")
	}
	if in.Request.ObjectID == sourceSnapshot {
		if status != "queued" && status != "preparing" {
			return out, ErrInvalidTransition
		}
	} else if derivedSnapshot != "" && in.Request.ObjectID == derivedSnapshot {
		if status != "preparing" && status != "publishing" {
			return out, ErrInvalidTransition
		}
	} else {
		return out, errors.New("pin request must target the admitted source or recorded derived snapshot")
	}
	var response []byte
	err = tx.QueryRow(ctx, `SELECT acquire_number,idempotency_key,request,response,state,expires_at FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2`, in.JobID, in.IdempotencyKey).Scan(&out.Number, &out.Key, &out.Request, &response, &out.State, &out.ExpiresAt)
	if err == nil {
		if !bytes.Equal(out.Request, requestBytes) {
			return PinAcquire{}, ErrConflict
		}
		if len(response) > 0 {
			var x storagev1.Pin
			if json.Unmarshal(response, &x) != nil {
				return PinAcquire{}, ErrConflict
			}
			out.Response = &x
		}
		if err = commit(ctx, tx); err != nil {
			return PinAcquire{}, err
		}
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return PinAcquire{}, err
	}
	var lastNumber int
	var state string
	var expires *time.Time
	var existingResponse []byte
	err = tx.QueryRow(ctx, `SELECT acquire_number,state,expires_at,response FROM generation_pin_acquires WHERE job_id=$1 AND object_id=$2 ORDER BY acquire_number DESC LIMIT 1 FOR UPDATE`, in.JobID, in.Request.ObjectID).Scan(&lastNumber, &state, &expires, &existingResponse)
	if err == nil {
		if state == "pending" {
			var alive bool
			if e := tx.QueryRow(ctx, `SELECT expires_at IS NULL OR expires_at > clock_timestamp() FROM generation_pin_acquires WHERE job_id=$1 AND acquire_number=$2`, in.JobID, lastNumber).Scan(&alive); e != nil {
				return out, e
			}
			if alive {
				return out, ErrInvalidTransition
			}
		}
		if state == "active" {
			return out, ErrInvalidTransition
		}
		if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET state='failed',updated_at=clock_timestamp() WHERE job_id=$1 AND acquire_number=$2 AND state='pending' AND expires_at<=clock_timestamp()`, in.JobID, lastNumber); err != nil {
			return out, err
		}
		if len(existingResponse) > 0 {
			var oldPin storagev1.Pin
			if err = json.Unmarshal(existingResponse, &oldPin); err != nil {
				return out, err
			}
			if err = schedulePinReleaseForObject(ctx, tx, in.JobID, in.Request.ObjectID, time.Time{}); err != nil {
				return out, err
			}
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	var n int
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(acquire_number),0)+1 FROM generation_pin_acquires WHERE job_id=$1`, in.JobID).Scan(&n); err != nil {
		return out, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO generation_pin_acquires(job_id,acquire_number,idempotency_key,object_id,request,state) VALUES($1,$2,$3,$4,$5,'pending')`, in.JobID, n, in.IdempotencyKey, in.Request.ObjectID, requestBytes); err != nil {
		return out, fmt.Errorf("save pin acquire intent: %w", err)
	}
	if err = commit(ctx, tx); err != nil {
		return out, err
	}
	return PinAcquire{Number: n, Key: in.IdempotencyKey, Request: requestBytes, State: "pending"}, nil
}

// RecordPinAcquireResponse persists the immutable AcquirePin receipt. A retry
// after losing the Storage response can call AcquirePin again with the same key.
func (s *Store) RecordPinAcquireResponse(ctx context.Context, jobID, key string, pin storagev1.Pin) error {
	if pin.Scope.WorkspaceID == "" || pin.PinID == "" || (pin.State != "pending" && pin.State != "active") {
		return errors.New("Storage pin receipt is incomplete")
	}
	encoded, err := json.Marshal(pin)
	if err != nil {
		return err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var terminalAt *time.Time
	if err = tx.QueryRow(ctx, `SELECT terminal_at FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&terminalAt); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var req []byte
	var state string
	var existing []byte
	err = tx.QueryRow(ctx, `SELECT request,state,response FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, key).Scan(&req, &state, &existing)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var request storagev1.AcquirePinRequest
	if err = json.Unmarshal(req, &request); err != nil {
		return err
	}
	if pin.Scope != request.Scope || pin.ObjectID != request.ObjectID || pin.HolderID != request.HolderID || pin.HolderDomain != request.HolderDomain || pin.RetentionPolicy != request.RetentionPolicy {
		return ErrConflict
	}
	if len(existing) > 0 {
		var prior storagev1.Pin
		if json.Unmarshal(existing, &prior) != nil {
			return ErrConflict
		}
		if prior.PinID != pin.PinID || prior.Scope != pin.Scope || prior.ObjectID != pin.ObjectID || prior.HolderID != pin.HolderID || prior.HolderDomain != pin.HolderDomain || prior.RetentionPolicy != pin.RetentionPolicy {
			return ErrConflict
		}
		// Storage may change the state from pending to active between retries;
		// identity is immutable and activation is committed by the next method.
		if state == "active" && pin.State != "active" {
			return ErrConflict
		}
	} else if state != "pending" && state != "failed" {
		return ErrInvalidTransition
	}
	_, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET response=$3::jsonb,storage_pin_id=$4,expires_at=$5,updated_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key=$2`, jobID, key, encoded, pin.PinID, pin.ExpiresAt)
	if err != nil {
		return err
	}
	if state == "failed" {
		if err = enqueuePinRelease(ctx, tx, jobID, pin, nil); err != nil {
			return err
		}
	} else if terminalAt != nil {
		if err = schedulePinRelease(ctx, tx, jobID, terminalAt.Add(s.cfg.DiagnosticTTL)); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

// MarkPinActiveAndRecordWorkflowStart commits the active pin and stable
// workflow-start intent together. The workflow ID is always render/{jobID}.
func (s *Store) MarkPinActiveAndRecordWorkflowStart(ctx context.Context, jobID, key, workflowID string, pin storagev1.Pin, workflowRequest []byte) error {
	if workflowID != "render/"+jobID {
		return errors.New("workflow ID must be the stable render/{jobID} identity")
	}
	if pin.State != "active" || pin.PinID == "" {
		return errors.New("an active Storage pin is required")
	}
	if len(workflowRequest) == 0 || len(workflowRequest) > 1<<20 {
		return errors.New("bounded workflow request is required")
	}
	encoded, err := json.Marshal(pin)
	if err != nil {
		return err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspace, project, snapshot, status, phase string
	var deadlineOK bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,source_snapshot_id,status,phase,deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&workspace, &project, &snapshot, &status, &phase, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "queued" {
		return ErrInvalidTransition
	}
	if !deadlineOK {
		return ErrInvalidTransition
	}
	if pin.Scope != (storagev1.Scope{WorkspaceID: workspace, ProjectID: project}) || pin.ObjectID != snapshot || pin.HolderID != s.cfg.PinHolderID || pin.HolderDomain != s.cfg.PinHolderDomain {
		return ErrConflict
	}
	var pinNotExpired bool
	if err = tx.QueryRow(ctx, `SELECT $1::timestamptz>clock_timestamp()`, pin.ExpiresAt).Scan(&pinNotExpired); err != nil {
		return err
	}
	if !pinNotExpired {
		return ErrInvalidTransition
	}
	var requestBytes, stored []byte
	var pinState string
	var pinID string
	err = tx.QueryRow(ctx, `SELECT request,response,state,storage_pin_id FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, key).Scan(&requestBytes, &stored, &pinState, &pinID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if pinState != "pending" && pinState != "active" {
		return ErrInvalidTransition
	}
	var acquireRequest storagev1.AcquirePinRequest
	if err = jsonvalue.Unmarshal(requestBytes, &acquireRequest, 1<<20); err != nil {
		return err
	}
	if acquireRequest.IdempotencyKey != key || acquireRequest.Scope != pin.Scope || acquireRequest.ObjectID != pin.ObjectID || acquireRequest.HolderID != pin.HolderID || acquireRequest.HolderDomain != pin.HolderDomain || acquireRequest.RetentionPolicy != pin.RetentionPolicy {
		return ErrConflict
	}
	if pinID == "" || len(stored) == 0 {
		return ErrInvalidTransition
	}
	if pinID != pin.PinID {
		return ErrConflict
	}
	if len(stored) > 0 {
		var old storagev1.Pin
		if json.Unmarshal(stored, &old) != nil || old.PinID != pin.PinID || old.Scope != pin.Scope || old.ObjectID != pin.ObjectID || old.HolderID != pin.HolderID || old.HolderDomain != pin.HolderDomain || old.RetentionPolicy != pin.RetentionPolicy {
			return ErrConflict
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET response=$3::jsonb,storage_pin_id=$4,expires_at=$5,state='active',updated_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key=$2`, jobID, key, encoded, pin.PinID, pin.ExpiresAt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_workflow_starts(job_id,workflow_id,request,state) VALUES($1,$2,$3,'pending') ON CONFLICT(job_id) DO NOTHING`, jobID, workflowID, workflowRequest)
	if err != nil {
		return err
	}
	var existingID string
	var existingRequest []byte
	if err = tx.QueryRow(ctx, `SELECT workflow_id,request FROM generation_workflow_starts WHERE job_id=$1`, jobID).Scan(&existingID, &existingRequest); err != nil {
		return err
	}
	if existingID != workflowID || !bytes.Equal(existingRequest, workflowRequest) {
		return ErrConflict
	}
	if phase != "workflow_pending" {
		if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET phase='workflow_pending',updated_at=clock_timestamp(),version=version+1 WHERE job_id=$1`, jobID); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

// MarkPinActive records a secondary pin, such as the pin for a derived
// snapshot. The source pin must use MarkPinActiveAndRecordWorkflowStart so the
// first workflow intent cannot be omitted.
func (s *Store) MarkPinActive(ctx context.Context, jobID, key string, pin storagev1.Pin) error {
	if jobID == "" || key == "" || pin.PinID == "" || pin.State != "active" {
		return errors.New("job, acquisition key and active Storage pin are required")
	}
	encoded, err := json.Marshal(pin)
	if err != nil {
		return err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspace, project, sourceSnapshot, derivedSnapshot, status string
	var deadlineOK bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,source_snapshot_id,derived_snapshot_id,status,deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).
		Scan(&workspace, &project, &sourceSnapshot, &derivedSnapshot, &status, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if (status != "preparing" && status != "publishing") || !deadlineOK || derivedSnapshot == "" || pin.ObjectID != derivedSnapshot {
		return ErrInvalidTransition
	}
	if pin.Scope != (storagev1.Scope{WorkspaceID: workspace, ProjectID: project}) || pin.HolderID != s.cfg.PinHolderID || pin.HolderDomain != s.cfg.PinHolderDomain {
		return ErrConflict
	}
	var unexpired bool
	if err = tx.QueryRow(ctx, `SELECT $1::timestamptz>clock_timestamp()`, pin.ExpiresAt).Scan(&unexpired); err != nil {
		return err
	}
	if !unexpired {
		return ErrInvalidTransition
	}
	var requestBytes, storedBytes []byte
	var state, priorID string
	err = tx.QueryRow(ctx, `SELECT request,response,state,storage_pin_id FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, key).
		Scan(&requestBytes, &storedBytes, &state, &priorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if state != "pending" && state != "active" {
		return ErrInvalidTransition
	}
	var request storagev1.AcquirePinRequest
	if err = jsonvalue.Unmarshal(requestBytes, &request, 1<<20); err != nil {
		return err
	}
	if priorID == "" || len(storedBytes) == 0 {
		return ErrInvalidTransition
	}
	if request.IdempotencyKey != key || request.Scope != pin.Scope || request.ObjectID != pin.ObjectID || request.HolderID != pin.HolderID || request.HolderDomain != pin.HolderDomain || request.RetentionPolicy != pin.RetentionPolicy || priorID != pin.PinID {
		return ErrConflict
	}
	if len(storedBytes) > 0 {
		var prior storagev1.Pin
		if err = json.Unmarshal(storedBytes, &prior); err != nil {
			return ErrConflict
		}
		if prior.PinID != pin.PinID || prior.Scope != pin.Scope || prior.ObjectID != pin.ObjectID || prior.HolderID != pin.HolderID || prior.HolderDomain != pin.HolderDomain || prior.RetentionPolicy != pin.RetentionPolicy {
			return ErrConflict
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET response=$3::jsonb,storage_pin_id=$4,expires_at=$5,state='active',updated_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key=$2`, jobID, key, encoded, pin.PinID, pin.ExpiresAt); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) ListPendingWorkflowStarts(ctx context.Context, limit int) ([]WorkflowStartIntent, error) {
	if limit < 1 || limit > 500 {
		return nil, errors.New("workflow batch limit must be between 1 and 500")
	}
	rows, err := s.pool.Query(ctx, `SELECT job_id,workflow_id,request,state,lease_owner,lease_epoch,lease_until,created_at FROM generation_workflow_starts WHERE state='pending' ORDER BY created_at,job_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkflowStartIntent
	for rows.Next() {
		var x WorkflowStartIntent
		if err = rows.Scan(&x.JobID, &x.WorkflowID, &x.Request, &x.State, &x.LeaseOwner, &x.LeaseEpoch, &x.LeaseUntil, &x.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// ClaimWorkflowStarts leases a batch of persisted Temporal start intents to one
// dispatcher. An expired claim can be retried with the original workflow ID.
func (s *Store) ClaimWorkflowStarts(ctx context.Context, worker string, limit int, lease time.Duration) ([]WorkflowStartIntent, error) {
	if worker == "" || len(worker) > 128 || limit < 1 || limit > 100 || lease < time.Second || lease > 30*time.Second {
		return nil, errors.New("workflow claim requires bounded worker, batch and lease")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	if _, err = tx.Exec(ctx, `UPDATE generation_workflow_starts w SET state='discarded',lease_owner='',lease_until=NULL FROM generation_jobs j WHERE w.job_id=j.job_id AND w.state='pending' AND (j.deadline<=clock_timestamp() OR j.cancellation_requested OR j.terminal_at IS NOT NULL)`); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT w.job_id,w.workflow_id,w.request,w.state,w.lease_owner,w.lease_epoch,w.lease_until,w.created_at FROM generation_workflow_starts w JOIN generation_jobs j ON j.job_id=w.job_id
		WHERE w.state='pending' AND (w.lease_until IS NULL OR w.lease_until<=clock_timestamp()) AND j.deadline>clock_timestamp() AND NOT j.cancellation_requested AND j.terminal_at IS NULL
		ORDER BY w.created_at,w.job_id FOR UPDATE OF w SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	var out []WorkflowStartIntent
	for rows.Next() {
		var x WorkflowStartIntent
		if err = rows.Scan(&x.JobID, &x.WorkflowID, &x.Request, &x.State, &x.LeaseOwner, &x.LeaseEpoch, &x.LeaseUntil, &x.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, x)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if err = tx.QueryRow(ctx, `UPDATE generation_workflow_starts SET lease_owner=$2,lease_epoch=lease_epoch+1,lease_until=clock_timestamp()+$3*interval '1 microsecond' WHERE job_id=$1 RETURNING lease_owner,lease_epoch,lease_until`, out[i].JobID, worker, lease.Microseconds()).Scan(&out[i].LeaseOwner, &out[i].LeaseEpoch, &out[i].LeaseUntil); err != nil {
			return nil, err
		}
	}
	if err = commit(ctx, tx); err != nil {
		return nil, err
	}
	return out, nil
}

// MarkWorkflowStarted acknowledges Temporal's start response. If the response
// was lost, callers leave the intent pending and retry the same workflow ID.
func (s *Store) MarkWorkflowStarted(ctx context.Context, jobID, workflowID, owner string, epoch int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE generation_workflow_starts SET state='acknowledged',acknowledged_at=clock_timestamp(),lease_owner='',lease_until=NULL WHERE job_id=$1 AND workflow_id=$2 AND state='pending' AND lease_owner=$3 AND lease_epoch=$4 AND lease_until>clock_timestamp()`, jobID, workflowID, owner, epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}
