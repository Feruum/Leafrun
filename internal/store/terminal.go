package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/events"
)

func safeDiagnostics(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return []byte("[]"), nil
	}
	if len(raw) > 1<<20 || !json.Valid(raw) || raw[0] != '[' {
		return nil, errors.New("diagnostics must be a valid bounded JSON array")
	}
	return append([]byte(nil), raw...), nil
}

func (s *Store) TerminalSuccess(ctx context.Context, in SuccessInput) error {
	diagnostics, err := safeDiagnostics(in.Diagnostics)
	if err != nil {
		return err
	}
	if len(in.RawDiagnostics) > 1<<20 {
		return errors.New("raw diagnostics exceed the bounded limit")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspace, project, status, owner string
	var version, epoch int64
	var canceled, leaseOK, deadlineOK bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,status,publication_owner,publication_epoch,version,cancellation_requested,COALESCE(publication_lease_until>clock_timestamp(),false),deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&workspace, &project, &status, &owner, &epoch, &version, &canceled, &leaseOK, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "publishing" || canceled || owner != in.Publisher || epoch != in.PublicationEpoch || !leaseOK || !deadlineOK {
		return ErrLeaseLost
	}
	candidate, scanErr := scanCandidate(tx.QueryRow(ctx, candidateSelect+` WHERE job_id=$1 AND candidate_id=$2 FOR UPDATE`, in.JobID, in.CandidateID))
	err = scanErr
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if !artifactEqual(candidate.Artifact, in.Artifact) || candidate.Artifact.OriginJobID != in.JobID || candidate.Scope != (storagev1.Scope{WorkspaceID: workspace, ProjectID: project}) {
		return ErrConflict
	}
	var candidateValid bool
	if err = tx.QueryRow(ctx, `SELECT expires_at>clock_timestamp() FROM generation_publication_candidates WHERE candidate_id=$1`, candidate.CandidateID).Scan(&candidateValid); err != nil {
		return err
	}
	if !candidateValid {
		return ErrCandidateExpired
	}
	var receiptValid bool
	if err = tx.QueryRow(ctx, `SELECT expires_at>clock_timestamp() FROM generation_admission_receipts WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&receiptValid); errors.Is(err, pgx.ErrNoRows) {
		return ErrReceiptExpired
	} else if err != nil {
		return err
	}
	if !receiptValid {
		return ErrReceiptExpired
	}
	var binding *generationv1.ArtifactBinding
	binding = &generationv1.ArtifactBinding{ArtifactID: candidate.Artifact.ArtifactID, SnapshotID: candidate.Artifact.SnapshotID, TargetID: candidate.Artifact.Metadata.TargetID, RenderAttemptID: candidate.Artifact.Metadata.AttemptID, InputFingerprint: candidate.Artifact.Metadata.InputFingerprint, SHA256: candidate.Artifact.Hash, SizeBytes: candidate.Artifact.SizeBytes, ExpiresAt: candidate.Artifact.ExpiresAt}
	bindingJSON, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	var terminalAt time.Time
	var newVersion int64
	err = tx.QueryRow(ctx, `UPDATE generation_jobs SET status='succeeded',phase='completed',artifact=$2::jsonb,artifact_id=$3,diagnostics=$4::jsonb,raw_diagnostics=$5,failure_code='',terminal_at=clock_timestamp(),updated_at=clock_timestamp(),version=version+1 WHERE job_id=$1 AND deadline>clock_timestamp() AND publication_lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM generation_publication_candidates c WHERE c.candidate_id=$6 AND c.job_id=$1 AND c.expires_at>clock_timestamp()) AND EXISTS(SELECT 1 FROM generation_admission_receipts r WHERE r.job_id=$1 AND r.expires_at>clock_timestamp()) RETURNING terminal_at,version`, in.JobID, bindingJSON, binding.ArtifactID, diagnostics, in.RawDiagnostics, candidate.CandidateID).Scan(&terminalAt, &newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("commit generation success: %w", err)
	}
	if err = schedulePinRelease(ctx, tx, in.JobID, terminalAt.Add(s.cfg.DiagnosticTTL)); err != nil {
		return err
	}
	if err = deleteQueueReservation(ctx, tx, in.JobID); err != nil {
		return err
	}
	if err = s.enqueueTerminal(ctx, tx, in.JobID, workspace, project, uint64(newVersion), terminalAt, generationv1.Succeeded); err != nil {
		return err
	}
	// Check after job/candidate/receipt locks and stored binding/fence checks.
	// A lock wait cannot reuse an earlier allow decision.
	if err = s.gateAuthority(ctx, tx, in.JobID, "generation.publish"); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT COALESCE(publication_lease_until>clock_timestamp(),false),deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1`, in.JobID).Scan(&leaseOK, &deadlineOK); err != nil {
		return err
	}
	if !leaseOK || !deadlineOK {
		return ErrLeaseLost
	}
	if err = tx.QueryRow(ctx, `SELECT expires_at>clock_timestamp() FROM generation_publication_candidates WHERE candidate_id=$1`, candidate.CandidateID).Scan(&candidateValid); err != nil {
		return err
	}
	if !candidateValid {
		return ErrCandidateExpired
	}
	if err = tx.QueryRow(ctx, `SELECT expires_at>clock_timestamp() FROM generation_admission_receipts WHERE job_id=$1`, in.JobID).Scan(&receiptValid); err != nil {
		return err
	}
	if !receiptValid {
		return ErrReceiptExpired
	}
	var cleared bool
	if err = tx.QueryRow(ctx, `UPDATE generation_jobs SET publication_owner='',publication_lease_until=NULL WHERE job_id=$1 AND deadline>clock_timestamp() AND publication_lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM generation_publication_candidates c WHERE c.candidate_id=$2 AND c.job_id=$1 AND c.expires_at>clock_timestamp()) AND EXISTS(SELECT 1 FROM generation_admission_receipts r WHERE r.job_id=$1 AND r.expires_at>clock_timestamp()) RETURNING true`, in.JobID, candidate.CandidateID).Scan(&cleared); errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	} else if err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) FailJob(ctx context.Context, in TerminalInput) error {
	return s.terminalFailure(ctx, in, generationv1.Failed)
}
func (s *Store) FailAttempt(ctx context.Context, attemptID string, epoch int64, in TerminalInput) error {
	if attemptID == "" || epoch <= 0 {
		return ErrInvalidTransition
	}
	in.attemptID, in.attemptEpoch = attemptID, epoch
	return s.terminalFailure(ctx, in, generationv1.Failed)
}
func (s *Store) FailPublication(ctx context.Context, owner string, epoch int64, in TerminalInput) error {
	if owner == "" || epoch <= 0 {
		return ErrInvalidTransition
	}
	in.publisher, in.publicationEpoch = owner, epoch
	return s.terminalFailure(ctx, in, generationv1.Failed)
}
func (s *Store) ExpireJob(ctx context.Context, in TerminalInput) error {
	return s.terminalFailure(ctx, in, generationv1.Expired)
}

func (s *Store) terminalFailure(ctx context.Context, in TerminalInput, status generationv1.Status) error {
	if status != generationv1.Failed && status != generationv1.Expired {
		return ErrInvalidTransition
	}
	diagnostics, err := safeDiagnostics(in.Diagnostics)
	if err != nil {
		return err
	}
	if len(in.RawDiagnostics) > 1<<20 || len(in.FailureCode) > 128 || strings.ContainsAny(in.FailureCode, "\x00\r\n") || len(in.Phase) > 64 || strings.ContainsAny(in.Phase, "\x00\r\n") {
		return errors.New("failure details exceed bounded limits")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspace, project, oldStatus string
	var version int64
	var deadlineReached bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,status,version,deadline<=clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&workspace, &project, &oldStatus, &version, &deadlineReached)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if oldStatus == "succeeded" || oldStatus == "failed" || oldStatus == "canceled" || oldStatus == "expired" || oldStatus == "cancelling" {
		return ErrInvalidTransition
	}
	if status == generationv1.Expired && !deadlineReached {
		return ErrInvalidTransition
	}
	if in.attemptID != "" {
		var owned bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.job_id=$2 AND a.fencing_epoch=$3 AND j.attempt_epoch=$3 AND a.owner_lease_until>clock_timestamp() AND a.state IN ('active','stopped') AND j.deadline>clock_timestamp())`, in.attemptID, in.JobID, in.attemptEpoch).Scan(&owned); err != nil {
			return err
		}
		if !owned {
			return ErrLeaseLost
		}
	}
	if in.publicationEpoch > 0 {
		var owned bool
		if err = tx.QueryRow(ctx, `SELECT publication_owner=$2 AND publication_epoch=$3 AND publication_lease_until>clock_timestamp() AND status='publishing' AND deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1`, in.JobID, in.publisher, in.publicationEpoch).Scan(&owned); err != nil {
			return err
		}
		if !owned {
			return ErrLeaseLost
		}
	}
	phase := in.Phase
	if phase == "" {
		phase = "completed"
	}
	var terminalAt time.Time
	var newVersion int64
	err = tx.QueryRow(ctx, `UPDATE generation_jobs SET status=$2,phase=$6,diagnostics=$3::jsonb,raw_diagnostics=$4,failure_code=$5,terminal_at=clock_timestamp(),updated_at=clock_timestamp(),version=version+1,publication_owner='',publication_lease_until=NULL WHERE job_id=$1 RETURNING terminal_at,version`, in.JobID, string(status), diagnostics, in.RawDiagnostics, in.FailureCode, phase).Scan(&terminalAt, &newVersion)
	if err != nil {
		return fmt.Errorf("commit generation terminal state: %w", err)
	}
	if err = schedulePinRelease(ctx, tx, in.JobID, terminalAt.Add(s.cfg.DiagnosticTTL)); err != nil {
		return err
	}
	if err = deleteQueueReservation(ctx, tx, in.JobID); err != nil {
		return err
	}
	if err = s.enqueueTerminal(ctx, tx, in.JobID, workspace, project, uint64(newVersion), terminalAt, status); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// RequestCancel serializes against success using the job row. It is idempotent;
// a running attempt keeps its execution slot until the exact container is
// confirmed stopped, then MarkAttemptStopped writes the canceled terminal event.
func (s *Store) RequestCancel(ctx context.Context, jobID string) (bool, error) {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return false, err
	}
	defer rollback(ctx, tx)
	commitCurrent := func() error {
		if err := s.gateCurrentOperation(ctx, tx, jobID, "generation.cancel"); err != nil {
			return err
		}
		return commit(ctx, tx)
	}
	var workspace, project, status string
	var version int64
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,status,version FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&workspace, &project, &status, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	if status == "succeeded" || status == "failed" || status == "canceled" || status == "expired" {
		if err = commitCurrent(); err != nil {
			return false, err
		}
		return false, nil
	}
	if status == "cancelling" {
		if err = commitCurrent(); err != nil {
			return false, err
		}
		return true, nil
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET status='cancelling',phase='cancelling',cancellation_requested=true,updated_at=clock_timestamp(),version=version+1,publication_owner='',publication_lease_until=NULL WHERE job_id=$1`, jobID); err != nil {
		return false, err
	}
	var unstopped bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_attempts WHERE job_id=$1 AND stop_confirmed=false)`, jobID).Scan(&unstopped); err != nil {
		return false, err
	}
	if unstopped {
		if err = deleteQueueReservation(ctx, tx, jobID); err != nil {
			return false, err
		}
		if err = commitCurrent(); err != nil {
			return false, err
		}
		return true, nil
	}
	if err = finalizeCancelTx(ctx, tx, s, jobID, workspace, project); err != nil {
		return false, err
	}
	if err = commitCurrent(); err != nil {
		return false, err
	}
	return true, nil
}

func finalizeCancelTx(ctx context.Context, tx pgx.Tx, s *Store, jobID, workspace, project string) error {
	var terminalAt time.Time
	var version int64
	err := tx.QueryRow(ctx, `UPDATE generation_jobs SET status='canceled',phase='completed',terminal_at=clock_timestamp(),updated_at=clock_timestamp(),version=version+1,publication_owner='',publication_lease_until=NULL WHERE job_id=$1 AND status='cancelling' RETURNING terminal_at,version`, jobID).Scan(&terminalAt, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidTransition
	}
	if err != nil {
		return err
	}
	if err = schedulePinRelease(ctx, tx, jobID, terminalAt.Add(s.cfg.DiagnosticTTL)); err != nil {
		return err
	}
	if err = deleteQueueReservation(ctx, tx, jobID); err != nil {
		return err
	}
	return s.enqueueTerminal(ctx, tx, jobID, workspace, project, uint64(version), terminalAt, generationv1.Canceled)
}

func deleteQueueReservation(ctx context.Context, tx pgx.Tx, jobID string) error {
	_, err := tx.Exec(ctx, `DELETE FROM generation_reservations WHERE job_id=$1 AND (reservation_type='queue' OR NOT EXISTS(SELECT 1 FROM generation_attempts WHERE job_id=$1 AND stop_confirmed=false))`, jobID)
	return err
}

func schedulePinRelease(ctx context.Context, tx pgx.Tx, jobID string, notBefore time.Time) error {
	return schedulePinReleaseForObject(ctx, tx, jobID, "", notBefore)
}

func schedulePinReleaseForObject(ctx context.Context, tx pgx.Tx, jobID, objectID string, notBefore time.Time) error {
	query := `SELECT storage_pin_id,state,response FROM generation_pin_acquires WHERE job_id=$1 AND response IS NOT NULL AND state<>'released'`
	args := []any{jobID}
	if objectID != "" {
		query += ` AND object_id=$2`
		args = append(args, objectID)
	}
	query += ` ORDER BY acquire_number FOR UPDATE`
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	type pinRow struct {
		id, state string
		response  []byte
	}
	var pins []pinRow
	for rows.Next() {
		var p pinRow
		if err = rows.Scan(&p.id, &p.state, &p.response); err != nil {
			rows.Close()
			return err
		}
		pins = append(pins, p)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, item := range pins {
		var pin storagev1.Pin
		if err = json.Unmarshal(item.response, &pin); err != nil {
			return err
		}
		if item.id == "" || pin.State == "released" {
			continue
		}
		var scheduled *time.Time
		if item.state == "active" || pin.State == "active" {
			scheduled = &notBefore
		}
		if err = enqueuePinRelease(ctx, tx, jobID, pin, scheduled); err != nil {
			return err
		}
	}
	return nil
}

func enqueuePinRelease(ctx context.Context, tx pgx.Tx, jobID string, pin storagev1.Pin, notBefore *time.Time) error {
	if jobID == "" || pin.PinID == "" || pin.State == "released" {
		return nil
	}
	req, err := json.Marshal(struct {
		Scope           storagev1.Scope `json:"scope"`
		PinID           string          `json:"pinId"`
		ObjectID        string          `json:"objectId"`
		HolderID        string          `json:"holderId"`
		HolderDomain    string          `json:"holderDomain"`
		RetentionPolicy string          `json:"retentionPolicy"`
	}{pin.Scope, pin.PinID, pin.ObjectID, pin.HolderID, pin.HolderDomain, pin.RetentionPolicy})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_cleanup_intents(job_id,kind,idempotency_key,request,state,not_before) VALUES($1,'pin.release',$2,$3,'pending',COALESCE($4::timestamptz,clock_timestamp())) ON CONFLICT(job_id,kind,idempotency_key) DO NOTHING`, jobID, "release/"+pin.PinID, req, notBefore)
	return err
}

func (s *Store) enqueueTerminal(ctx context.Context, tx pgx.Tx, jobID, workspace, project string, version uint64, at time.Time, status generationv1.Status) error {
	eventID := fmt.Sprintf("generation:%s:v%d", jobID, version)
	e, subject, err := events.NewGenerationCompleted(events.GenerationCompletedInput{EventID: eventID, Producer: "leafrun-generation", JobID: jobID, WorkspaceID: workspace, ProjectID: project, AggregateVersion: version, OccurredAt: at})
	if err != nil {
		return err
	}
	if err = e.Validate(); err != nil {
		return err
	}
	if err = events.Enqueue(ctx, tx, e, subject); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_terminal_outbox(job_id,event_id) VALUES($1,$2) ON CONFLICT(job_id,event_id) DO NOTHING`, jobID, eventID)
	if err != nil {
		return err
	}
	return s.enqueueJobAudit(ctx, tx, jobID, version, at, auditv1.ActionGenerationTerminal, terminalAuditOutcome(status))
}
