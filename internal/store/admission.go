package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
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
	receiptFingerprint := admissionReceiptFingerprint(in)
	var expiry time.Time
	err = tx.QueryRow(ctx, `SELECT fingerprint,job_id,expires_at FROM generation_admission_receipts WHERE workspace_id=$1 AND initiator_id=$2 AND idempotency_key=$3 FOR UPDATE`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey).Scan(&oldFingerprint, &oldJob, &expiry)
	if err == nil {
		var live bool
		if e := tx.QueryRow(ctx, `SELECT expires_at > clock_timestamp() FROM generation_admission_receipts WHERE workspace_id=$1 AND initiator_id=$2 AND idempotency_key=$3`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey).Scan(&live); e != nil {
			return out, false, e
		}
		if live {
			if oldFingerprint != receiptFingerprint {
				return out, false, ErrConflict
			}
			j, e := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE job_id=$1`, oldJob))
			if e != nil {
				return out, false, fmt.Errorf("load admission replay: %w", e)
			}
			if e = tx.QueryRow(ctx, `SELECT expires_at FROM generation_admission_receipts WHERE job_id=$1`, oldJob).Scan(&out.IdempotencyExpiresAt); e != nil {
				return out, false, e
			}
			if e = s.gateRequestAuthority(ctx, tx, oldJob, true); e != nil {
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
	var selectionRequest, selectionFingerprint any
	var originalIdentity any
	authorityRelease, authorityDigest := "", ""
	if in.ResolvedExecution != nil {
		authorityRelease = in.ResolvedExecution.ReleaseID
		authorityDigest = in.ResolvedExecution.Descriptor.Digest
	}
	if in.Identity != nil {
		originalIdentity, err = json.Marshal(in.Identity)
		if err != nil {
			return out, false, err
		}
	}
	var actorSubject, actorDomain any = "", ""
	if len(in.SelectionRequest) > 0 {
		selectionRequest, selectionFingerprint = in.SelectionRequest, in.SelectionFingerprint
		actorSubject, actorDomain = in.Actor.Subject, in.Actor.Domain
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_jobs(job_id,workspace_id,project_id,initiator_id,idempotency_key,normalized_request,parameters,fingerprint,source_snapshot_id,source_content_id,target_id,entrypoint,engine_version,status,phase,deadline,selection_request,selection_fingerprint,actor_subject,actor_domain,original_identity,authority_release_id,authority_descriptor_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'queued','pin_pending',clock_timestamp()+interval '15 minutes',$14,$15,$16,$17,$18::jsonb,$19,$20)`, jobID, in.WorkspaceID, in.ProjectID, in.InitiatorID, in.IdempotencyKey, in.NormalizedRequest, in.Parameters, in.Fingerprint, in.SnapshotID, in.SourceContentID, in.TargetID, in.Entrypoint, in.EngineVersion, selectionRequest, selectionFingerprint, actorSubject, actorDomain, originalIdentity, authorityRelease, authorityDigest)
	if err != nil {
		return out, false, fmt.Errorf("insert generation job: %w", err)
	}
	if err = insertInputResources(ctx, tx, jobID, in.InputResources); err != nil {
		return out, false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO generation_reservations(job_id,workspace_id,reservation_type) VALUES($1,$2,'queue')`, jobID, in.WorkspaceID); err != nil {
		return out, false, fmt.Errorf("reserve generation queue slot: %w", err)
	}
	if err = tx.QueryRow(ctx, `INSERT INTO generation_admission_receipts(workspace_id,initiator_id,idempotency_key,fingerprint,job_id,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6*interval '1 microsecond') RETURNING expires_at`, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey, receiptFingerprint, jobID, s.cfg.ReceiptTTL.Microseconds()).Scan(&out.IdempotencyExpiresAt); err != nil {
		return out, false, fmt.Errorf("persist admission receipt: %w", err)
	}
	j, e := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE job_id=$1`, jobID))
	if e != nil {
		return out, false, e
	}
	if err = s.captureAuditContext(ctx, tx, jobID); err != nil {
		return out, false, err
	}
	if err = s.enqueueJobAudit(ctx, tx, jobID, uint64(j.version), j.createdAt, auditv1.ActionGenerationAdmission, auditv1.OutcomeAdmitted); err != nil {
		return out, false, err
	}
	if err = s.gateRequestAuthority(ctx, tx, jobID, false); err != nil {
		return out, false, err
	}
	if err = commit(ctx, tx); err != nil {
		return out, false, err
	}
	out.Job = j.dto()
	return out, false, nil
}

const resolvedExecutionVersion = "leafrun-resolved-execution-v1"

// NormalizeResolvedExecution produces the canonical durable execution bytes
// and their versioned content fingerprint. Actor and selection receipt data
// remain part of the authority, while renderer profile and file metadata are
// frozen alongside the prepared JSON.
func NormalizeResolvedExecution(execution projectsv1.ResolvedExecution) ([]byte, string, error) {
	encoded, err := json.Marshal(execution)
	if err != nil || len(encoded) == 0 || int64(len(encoded)) > projectsv1.ResponseLimit {
		return nil, "", errors.New("resolved execution exceeds the canonical 12 MiB limit")
	}
	var decoded projectsv1.ResolvedExecution
	if err = jsonvalue.Unmarshal(encoded, &decoded, projectsv1.ResponseLimit); err != nil {
		return nil, "", err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, "", errors.New("resolved execution is not canonical")
	}
	sum := sha256.Sum256(append([]byte(resolvedExecutionVersion+"\n"), encoded...))
	return encoded, hex.EncodeToString(sum[:]), nil
}

func admissionReceiptFingerprint(in AdmitInput) string {
	if len(in.SelectionRequest) > 0 {
		return in.SelectionFingerprint
	}
	return in.Fingerprint
}

func validateAdmission(in AdmitInput) error {
	if in.Identity != nil && (in.Identity.Validate() != nil || in.Identity.Authority.Kind == "service" || in.Identity.Authority.Kind == "dev" || in.Actor != (storagev1.Principal{}) && in.Identity.Principal != in.Actor) {
		return ErrAuthorityDenied
	}
	if in.WorkspaceID == "" || in.ProjectID == "" || in.InitiatorID == "" || !generationv1.ValidIdempotencyKey(in.IdempotencyKey) {
		return errors.New("workspace, project, initiator and valid idempotency key are required")
	}
	if strings.TrimSpace(in.EngineVersion) == "" || len(in.EngineVersion) > 128 {
		return errors.New("bounded engine version is required")
	}
	if len(in.SelectionRequest) > 0 || in.SelectionFingerprint != "" || in.ResolvedExecution != nil || len(in.InputResources) > 0 {
		return validateSelectionAdmission(in)
	}
	if len(in.NormalizedRequest) == 0 || len(in.NormalizedRequest) > 1<<20 || len(in.Parameters) > 1<<20 {
		return errors.New("normalized request and parameters must be bounded nonempty request data")
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
	if !bytes.Equal(request.Parameters, in.Parameters) || in.SourceContentID != "" || in.Actor != (storagev1.Principal{}) {
		return errors.New("legacy request parameters or actor fields do not match the normalized request")
	}
	return nil
}

func validateSelectionAdmission(in AdmitInput) error {
	if len(in.SelectionRequest) == 0 || len(in.SelectionRequest) > 1<<20 || len(in.NormalizedRequest) == 0 || int64(len(in.NormalizedRequest)) > projectsv1.ResponseLimit || len(in.Parameters) == 0 || len(in.Parameters) > 1<<20 || in.ResolvedExecution == nil {
		return errors.New("selection and resolved execution must be bounded nonempty data")
	}
	selection, normalizedSelection, fingerprint, err := generationv1.NormalizeSelection(in.SelectionRequest)
	if err != nil || !bytes.Equal(normalizedSelection, in.SelectionRequest) || fingerprint != in.SelectionFingerprint {
		return errors.New("selection bytes or fingerprint are not the Contracts canonical form")
	}
	canonicalExecution, executionFingerprint, err := NormalizeResolvedExecution(*in.ResolvedExecution)
	if err != nil || !bytes.Equal(canonicalExecution, in.NormalizedRequest) || executionFingerprint != in.Fingerprint {
		return errors.New("resolved execution bytes or fingerprint are not canonical")
	}
	execution := *in.ResolvedExecution
	resolveRequest := projectsv1.ResolveRequest{Actor: in.Actor, Selection: selection, SelectionFingerprint: fingerprint}
	if err = projectsv1.ValidateResolved(resolveRequest, execution); err != nil {
		return fmt.Errorf("validate resolved Projects execution: %w", err)
	}
	if !projectsv1.ValidIdentifier(in.Actor.Subject) || !projectsv1.ValidIdentifier(in.Actor.Domain) || execution.Actor != in.Actor || selection.Scope.WorkspaceID != in.WorkspaceID || selection.Scope.ProjectID != in.ProjectID || execution.Scope != selection.Scope {
		return errors.New("resolved execution actor or scope does not match the admission")
	}
	if execution.SnapshotID != in.SnapshotID || execution.ContentID != in.SourceContentID || execution.Descriptor.TargetID != in.TargetID || execution.Descriptor.Entrypoint != in.Entrypoint || execution.Descriptor.TypstVersion != in.EngineVersion || !bytes.Equal(execution.Prepared.JSON, in.Parameters) {
		return errors.New("resolved execution identity or parameters do not match the admission")
	}
	if len(in.InputResources) != len(execution.InputResources) {
		return errors.New("input resource intents do not match the resolved execution")
	}
	byPointer := make(map[string]projectsv1.InputResource, len(execution.InputResources))
	for _, resource := range execution.InputResources {
		byPointer[resource.File.Pointer] = resource
	}
	keyByResource := make(map[string]string, len(in.InputResources))
	resourceByKey := make(map[string]string, len(in.InputResources))
	for _, intent := range in.InputResources {
		file := intent.Resource.File
		resource, exists := byPointer[file.Pointer]
		if !exists || resource != intent.Resource || intent.Resource.Scope != execution.Scope || intent.Resource.Class != storagev1.ClassResource || !generationv1.ValidIdempotencyKey(intent.AcquisitionKey) {
			return errors.New("input resource pin intent differs from the resolved allowlist")
		}
		if prior, exists := keyByResource[file.ResourceID]; exists && prior != intent.AcquisitionKey {
			return errors.New("one input resource must share one pin acquisition")
		}
		if prior, exists := resourceByKey[intent.AcquisitionKey]; exists && prior != file.ResourceID {
			return errors.New("input resource pin acquisition key is shared across resources")
		}
		keyByResource[file.ResourceID] = intent.AcquisitionKey
		resourceByKey[intent.AcquisitionKey] = file.ResourceID
		delete(byPointer, file.Pointer)
	}
	if len(byPointer) != 0 {
		return errors.New("input resource pin intent is missing a resolved file")
	}
	return nil
}

func insertInputResources(ctx context.Context, tx pgx.Tx, jobID string, resources []InputResourceIntent) error {
	for _, input := range resources {
		file := input.Resource.File
		_, err := tx.Exec(ctx, `INSERT INTO generation_input_resources(job_id,field_pointer,resource_id,workspace_id,project_id,object_class,filename,generated_path,size_bytes,sha256,acquisition_key)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, jobID, file.Pointer, file.ResourceID, input.Resource.Scope.WorkspaceID, input.Resource.Scope.ProjectID, input.Resource.Class, file.Filename, file.Path, file.SizeBytes, file.SHA256, input.AcquisitionKey)
		if err != nil {
			return fmt.Errorf("persist input resource allowlist: %w", err)
		}
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
	var state, objectID string
	if err = tx.QueryRow(ctx, `SELECT state,object_id FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, key).Scan(&state, &objectID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var inputResource, workflowStarted bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_input_resources WHERE job_id=$1 AND resource_id=$2 AND acquisition_key=$3),EXISTS(SELECT 1 FROM generation_workflow_starts WHERE job_id=$1)`, jobID, objectID, key).Scan(&inputResource, &workflowStarted); err != nil {
		return err
	}
	if state != "pending" && state != "failed" && !(state == "active" && inputResource && !workflowStarted) {
		return ErrInvalidTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET state='failed',expires_at=clock_timestamp(),updated_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key=$2`, jobID, key); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_input_resources SET pin_state='failed',pin_id='' WHERE job_id=$1 AND acquisition_key=$2 AND pin_state IN ('pending','active')`, jobID, key); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// InputPinAcquires returns one durable acquisition row per unique resource.
// Duplicate field pointers share the same Storage pin and key.
func (s *Store) InputPinAcquires(ctx context.Context, jobID string) ([]InputPinAcquire, error) {
	if jobID == "" {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT ON (r.acquisition_key) r.resource_id,r.workspace_id,r.project_id,r.object_class,r.field_pointer,r.filename,r.generated_path,r.size_bytes,r.sha256,r.acquisition_key,r.pin_id,r.pin_state
		FROM generation_input_resources r WHERE r.job_id=$1 ORDER BY r.acquisition_key,r.field_pointer`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InputPinAcquire
	for rows.Next() {
		var item InputPinAcquire
		var file projectsv1.PreparedFile
		if err = rows.Scan(&file.ResourceID, &item.Resource.Scope.WorkspaceID, &item.Resource.Scope.ProjectID, &item.Resource.Class, &file.Pointer, &file.Filename, &file.Path, &file.SizeBytes, &file.SHA256, &item.AcquisitionKey, &item.PinID, &item.State); err != nil {
			return nil, err
		}
		item.Resource.File = file
		out = append(out, item)
	}
	return out, rows.Err()
}

// RotateInputPinAcquireKey changes the durable allowlist only after Storage
// authoritatively expired the prior key.
func (s *Store) RotateInputPinAcquireKey(ctx context.Context, jobID, resourceID, oldKey, newKey string) error {
	if jobID == "" || resourceID == "" || !generationv1.ValidIdempotencyKey(oldKey) || !generationv1.ValidIdempotencyKey(newKey) || oldKey == newKey {
		return errors.New("job, resource and distinct bounded pin keys are required")
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
	if terminalAt != nil {
		return ErrInvalidTransition
	}
	var expired string
	if err = tx.QueryRow(ctx, `SELECT state FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, oldKey).Scan(&expired); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if expired != "failed" {
		return ErrInvalidTransition
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_input_resources WHERE job_id=$1 AND resource_id=$2 AND acquisition_key=$3 AND pin_state='failed')`, jobID, resourceID, oldKey).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_input_resources SET acquisition_key=$4,pin_state='pending',pin_id='' WHERE job_id=$1 AND resource_id=$2 AND acquisition_key=$3 AND pin_state='failed'`, jobID, resourceID, oldKey, newKey); err != nil {
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
		var allowed bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_input_resources WHERE job_id=$1 AND resource_id=$2 AND acquisition_key=$3 AND workspace_id=$4 AND project_id=$5 AND object_class='resource' AND pin_state IN ('pending','failed'))`, in.JobID, in.Request.ObjectID, in.IdempotencyKey, workspace, project).Scan(&allowed); err != nil {
			return out, err
		}
		if !allowed || status != "queued" || in.Request.RetentionPolicy != "ordinary" {
			return out, errors.New("pin request must match an admitted input resource or snapshot")
		}
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
	var unpinnedInputs bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_input_resources WHERE job_id=$1 AND (pin_state<>'active' OR pin_id=''))`, jobID).Scan(&unpinnedInputs); err != nil {
		return err
	}
	if unpinnedInputs {
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
	if err = s.gatePinAuthority(ctx, tx, jobID, pin.ExpiresAt); err != nil {
		return err
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
	if err = s.gatePinAuthority(ctx, tx, jobID, pin.ExpiresAt); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// MarkInputPinActive records one resource pin after Storage confirmed its
// exact active receipt. Input rows become renderable only with that receipt.
func (s *Store) MarkInputPinActive(ctx context.Context, jobID, key string, pin storagev1.Pin) error {
	if jobID == "" || key == "" || pin.PinID == "" || pin.State != "active" {
		return errors.New("job, input acquisition key and active Storage pin are required")
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
	var workspace, project, status string
	var deadlineOK bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,status,deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&workspace, &project, &status, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "queued" || !deadlineOK || pin.Scope != (storagev1.Scope{WorkspaceID: workspace, ProjectID: project}) || pin.HolderID != s.cfg.PinHolderID || pin.HolderDomain != s.cfg.PinHolderDomain || pin.RetentionPolicy != "ordinary" {
		return ErrInvalidTransition
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
	err = tx.QueryRow(ctx, `SELECT request,response,state,storage_pin_id FROM generation_pin_acquires WHERE job_id=$1 AND idempotency_key=$2 FOR UPDATE`, jobID, key).Scan(&requestBytes, &storedBytes, &state, &priorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	var acquire storagev1.AcquirePinRequest
	if err = jsonvalue.Unmarshal(requestBytes, &acquire, 1<<20); err != nil {
		return err
	}
	if acquire.IdempotencyKey != key || acquire.Scope != pin.Scope || acquire.ObjectID != pin.ObjectID || acquire.HolderID != pin.HolderID || acquire.HolderDomain != pin.HolderDomain || acquire.RetentionPolicy != pin.RetentionPolicy || state != "pending" && state != "active" || priorID != pin.PinID || len(storedBytes) == 0 {
		return ErrConflict
	}
	var prior storagev1.Pin
	if err = jsonvalue.Unmarshal(storedBytes, &prior, 1<<20); err != nil || prior.PinID != pin.PinID || prior.Scope != pin.Scope || prior.ObjectID != pin.ObjectID || prior.HolderID != pin.HolderID || prior.HolderDomain != pin.HolderDomain || prior.RetentionPolicy != pin.RetentionPolicy {
		return ErrConflict
	}
	var allowed bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_input_resources WHERE job_id=$1 AND resource_id=$2 AND acquisition_key=$3 AND workspace_id=$4 AND project_id=$5 AND object_class='resource' AND pin_state IN ('pending','active'))`, jobID, pin.ObjectID, key, workspace, project).Scan(&allowed); err != nil {
		return err
	}
	if !allowed {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET response=$3::jsonb,storage_pin_id=$4,expires_at=$5,state='active',updated_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key=$2`, jobID, key, encoded, pin.PinID, pin.ExpiresAt); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_input_resources SET pin_id=$3,pin_state='active' WHERE job_id=$1 AND acquisition_key=$2 AND resource_id=$4`, jobID, key, pin.PinID, pin.ObjectID); err != nil {
		return err
	}
	if err = s.gatePinAuthority(ctx, tx, jobID, pin.ExpiresAt); err != nil {
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
		ORDER BY w.created_at,w.job_id FOR UPDATE OF j,w SKIP LOCKED LIMIT $1`, limit)
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
	for _, item := range out {
		if err = s.gateAuthority(ctx, tx, item.JobID, "generation.execute"); err != nil {
			if errors.Is(err, ErrAuthorityDenied) {
				_ = tx.Rollback(ctx)
				return nil, errors.Join(err, s.FailJob(ctx, TerminalInput{JobID: item.JobID, Phase: "access", FailureCode: "access_denied"}))
			}
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
