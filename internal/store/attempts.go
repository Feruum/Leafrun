package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ClaimExecution moves a durable queue reservation into an execution slot and
// creates a fresh attempt ID and fencing epoch in the same transaction.
func (s *Store) ClaimExecution(ctx context.Context, in ClaimAttemptInput) (Attempt, error) {
	var out Attempt
	if in.JobID == "" || in.CreateContainerIntentKey == "" || in.StagingToken == "" || in.PrivateStagingPath == "" || strings.ContainsAny(in.StagingToken+in.PrivateStagingPath, "\x00\r\n") {
		return out, errors.New("job, create-container intent key and private staging identifiers are required")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	if err = lockQueue(ctx, tx); err != nil {
		return out, err
	}
	var workspace, status string
	var attemptNo, epoch int64
	var deadlineOK, canceled bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,status,current_attempt_number,attempt_epoch,deadline>clock_timestamp(),cancellation_requested FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&workspace, &status, &attemptNo, &epoch, &deadlineOK, &canceled)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if status != "queued" || canceled {
		return out, ErrInvalidTransition
	}
	if !deadlineOK {
		return out, ErrInvalidTransition
	}
	var activePin bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_pin_acquires WHERE job_id=$1 AND state='active')`, in.JobID).Scan(&activePin); err != nil {
		return out, err
	}
	if !activePin {
		return out, ErrInvalidTransition
	}
	var reservationType string
	if err = tx.QueryRow(ctx, `SELECT reservation_type FROM generation_reservations WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&reservationType); errors.Is(err, pgx.ErrNoRows) {
		return out, ErrInvalidTransition
	} else if err != nil {
		return out, err
	}
	if reservationType != "queue" {
		return out, ErrInvalidTransition
	}
	var executingGlobal, executingWorkspace int
	if err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE reservation_type='execution'),count(*) FILTER(WHERE reservation_type='execution' AND workspace_id=$1) FROM generation_reservations`, workspace).Scan(&executingGlobal, &executingWorkspace); err != nil {
		return out, err
	}
	if executingGlobal >= s.cfg.ExecutionGlobal || executingWorkspace >= s.cfg.ExecutionWorkspace {
		return out, ErrCapacity
	}
	if attemptNo >= defaultAttempts {
		return out, ErrInvalidTransition
	}
	var priorUnstopped bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_attempts WHERE job_id=$1 AND stop_confirmed=false)`, in.JobID).Scan(&priorUnstopped); err != nil {
		return out, err
	}
	if priorUnstopped {
		return out, ErrInvalidTransition
	}
	out.AttemptID, err = newID("att_")
	if err != nil {
		return Attempt{}, err
	}
	out.JobID = in.JobID
	out.AttemptNumber = int(attemptNo + 1)
	out.FencingEpoch = epoch + 1
	out.State = "active"
	out.CreateContainerIntentKey = in.CreateContainerIntentKey
	out.StagingToken = in.StagingToken
	out.PrivateStagingPath = in.PrivateStagingPath
	err = tx.QueryRow(ctx, `INSERT INTO generation_attempts(attempt_id,job_id,attempt_number,fencing_epoch,state,create_container_intent_key,staging_token,private_staging_path,owner_lease_until)
		VALUES($1,$2,$3,$4,'active',$5,$6,$7,clock_timestamp()+$8*interval '1 microsecond') RETURNING owner_lease_until,heartbeat_at,created_at`, out.AttemptID, in.JobID, out.AttemptNumber, out.FencingEpoch, in.CreateContainerIntentKey, in.StagingToken, in.PrivateStagingPath, s.cfg.AttemptLease.Microseconds()).Scan(&out.OwnerLeaseUntil, &out.HeartbeatAt, &out.CreatedAt)
	if err != nil {
		return Attempt{}, fmt.Errorf("create render attempt: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET status='preparing',phase='preparing',current_attempt_number=$2,attempt_epoch=$3,updated_at=clock_timestamp(),version=version+1 WHERE job_id=$1`, in.JobID, out.AttemptNumber, out.FencingEpoch); err != nil {
		return Attempt{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_reservations SET reservation_type='execution',created_at=clock_timestamp() WHERE job_id=$1`, in.JobID); err != nil {
		return Attempt{}, err
	}
	if err = s.gateAuthority(ctx, tx, in.JobID, "generation.execute"); err != nil {
		return out, err
	}
	var active bool
	if err = tx.QueryRow(ctx, `SELECT owner_lease_until>clock_timestamp() FROM generation_attempts WHERE attempt_id=$1`, out.AttemptID).Scan(&active); err != nil {
		return Attempt{}, err
	}
	if !active {
		return Attempt{}, ErrLeaseLost
	}
	if err = commit(ctx, tx); err != nil {
		return Attempt{}, err
	}
	return out, nil
}

func (s *Store) HeartbeatAttempt(ctx context.Context, attemptID string, epoch int64) (time.Time, error) {
	var until time.Time
	err := s.pool.QueryRow(ctx, `UPDATE generation_attempts a SET owner_lease_until=clock_timestamp()+$3*interval '1 microsecond',heartbeat_at=clock_timestamp()
		FROM generation_jobs j WHERE a.attempt_id=$1 AND a.job_id=j.job_id AND a.fencing_epoch=$2 AND j.attempt_epoch=$2 AND a.state='active'
		AND a.owner_lease_until>clock_timestamp() AND j.status IN ('preparing','rendering','publishing') AND NOT j.cancellation_requested RETURNING a.owner_lease_until`, attemptID, epoch, s.cfg.AttemptLease.Microseconds()).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("renew render attempt lease: %w", err)
	}
	return until, nil
}

// TakeOverExpiredAttempt gives a new controller a fresh fencing epoch while
// preserving the same immutable render attempt, container ID and staging path.
func (s *Store) TakeOverExpiredAttempt(ctx context.Context, jobID string) (Attempt, error) {
	var out Attempt
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	var status string
	var currentEpoch int64
	var cancellation, deadlineOK bool
	err = tx.QueryRow(ctx, `SELECT a.attempt_id,a.job_id,a.attempt_number,a.fencing_epoch,a.state,a.create_container_intent_key,a.staging_token,a.private_staging_path,a.container_id,a.stop_confirmed,a.owner_lease_until,a.heartbeat_at,a.created_at,j.status,j.attempt_epoch,j.cancellation_requested,j.deadline>clock_timestamp()
		FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.job_id=$1 AND a.state='active' AND a.owner_lease_until<=clock_timestamp() ORDER BY a.attempt_number DESC LIMIT 1 FOR UPDATE OF a,j`, jobID).
		Scan(&out.AttemptID, &out.JobID, &out.AttemptNumber, &out.FencingEpoch, &out.State, &out.CreateContainerIntentKey, &out.StagingToken, &out.PrivateStagingPath, &out.ContainerID, &out.StopConfirmed, &out.OwnerLeaseUntil, &out.HeartbeatAt, &out.CreatedAt, &status, &currentEpoch, &cancellation, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrLeaseLost
	}
	if err != nil {
		return out, err
	}
	if out.FencingEpoch != currentEpoch {
		return out, ErrLeaseLost
	}
	if (status != "preparing" && status != "rendering") || cancellation || !deadlineOK {
		return out, ErrInvalidTransition
	}
	out.FencingEpoch++
	err = tx.QueryRow(ctx, `UPDATE generation_jobs SET attempt_epoch=$2,updated_at=clock_timestamp() WHERE job_id=$1 RETURNING attempt_epoch`, jobID, out.FencingEpoch).Scan(&out.FencingEpoch)
	if err != nil {
		return Attempt{}, err
	}
	err = tx.QueryRow(ctx, `UPDATE generation_attempts SET fencing_epoch=$2,owner_lease_until=clock_timestamp()+$3*interval '1 microsecond',heartbeat_at=clock_timestamp() WHERE attempt_id=$1 RETURNING owner_lease_until,heartbeat_at`, out.AttemptID, out.FencingEpoch, s.cfg.AttemptLease.Microseconds()).Scan(&out.OwnerLeaseUntil, &out.HeartbeatAt)
	if err != nil {
		return Attempt{}, err
	}
	if err = commit(ctx, tx); err != nil {
		return Attempt{}, err
	}
	return out, nil
}

// CheckExecutionStart repeats the configured saved-purpose authority gate after
// slow container creation/attachment. It uses the locked job's original binding
// and rechecks current attempt/time fences after the remote authority wait.
func (s *Store) CheckExecutionStart(ctx context.Context, jobID, attemptID string, epoch int64) error {
	if jobID == "" || attemptID == "" || epoch <= 0 {
		return ErrLeaseLost
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var lockedJob string
	if err = tx.QueryRow(ctx, `SELECT job_id FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&lockedJob); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	owner := IntentInput{JobID: lockedJob, RenderAttemptID: attemptID, AttemptEpoch: epoch, Kind: IntentContainerStart}
	if err = checkIntentOwner(ctx, tx, owner); err != nil {
		return err
	}
	if err = s.gateAuthority(ctx, tx, lockedJob, "generation.execute"); err != nil {
		return err
	}
	if err = checkIntentOwner(ctx, tx, owner); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// MarkRendering advances a current leased attempt after the workflow has
// completed preparation and validation.
func (s *Store) MarkRendering(ctx context.Context, attemptID string, epoch int64) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var jobID string
	err = tx.QueryRow(ctx, `SELECT a.job_id FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.fencing_epoch=$2 AND j.attempt_epoch=$2 AND a.state='active' AND a.owner_lease_until>clock_timestamp() AND j.status='preparing' AND NOT j.cancellation_requested FOR UPDATE OF j,a`, attemptID, epoch).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET status='rendering',phase='rendering',updated_at=clock_timestamp(),version=version+1 WHERE job_id=$1`, jobID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) RecordSourceIdentity(ctx context.Context, jobID, contentID string) error {
	if contentID == "" || len(contentID) > 256 {
		return errors.New("bounded source content ID is required")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var old string
	var terminal bool
	err = tx.QueryRow(ctx, `SELECT source_content_id,terminal_at IS NOT NULL FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&old, &terminal)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if terminal {
		return ErrInvalidTransition
	}
	if old != "" && old != contentID {
		return ErrConflict
	}
	if old == contentID {
		return commit(ctx, tx)
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET source_content_id=$2,updated_at=clock_timestamp() WHERE job_id=$1`, jobID, contentID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) RecordDerivedIdentity(ctx context.Context, jobID, attemptID string, epoch int64, snapshotID, contentID string) error {
	if snapshotID == "" || contentID == "" || len(snapshotID) > 256 || len(contentID) > 256 {
		return errors.New("bounded derived snapshot and content IDs are required")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var status string
	var oldSnapshot, oldContent string
	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT j.status,j.derived_snapshot_id,j.derived_content_id,j.attempt_epoch FROM generation_jobs j JOIN generation_attempts a ON a.job_id=j.job_id WHERE j.job_id=$1 AND a.attempt_id=$2 AND a.fencing_epoch=$3 AND a.state='active' AND a.owner_lease_until>clock_timestamp() FOR UPDATE OF j,a`, jobID, attemptID, epoch).Scan(&status, &oldSnapshot, &oldContent, &currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if currentEpoch != epoch {
		return ErrLeaseLost
	}
	if status != "preparing" {
		return ErrInvalidTransition
	}
	if (oldSnapshot != "" && oldSnapshot != snapshotID) || (oldContent != "" && oldContent != contentID) {
		return ErrConflict
	}
	if oldSnapshot == snapshotID && oldContent == contentID {
		return commit(ctx, tx)
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET derived_snapshot_id=$2,derived_content_id=$3,updated_at=clock_timestamp() WHERE job_id=$1`, jobID, snapshotID, contentID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// RecordContainer durably records the exact Docker container before Start or
// Wait is called. Replays may only repeat the same immutable container ID.
func (s *Store) RecordContainer(ctx context.Context, attemptID string, epoch int64, containerID string) error {
	if strings.TrimSpace(containerID) == "" || len(containerID) > 256 || strings.ContainsAny(containerID, "\x00\r\n") {
		return errors.New("bounded Docker container ID is required")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var prior string
	err = tx.QueryRow(ctx, `SELECT a.container_id FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.fencing_epoch=$2 AND j.attempt_epoch=$2 AND a.state='active' AND a.owner_lease_until>clock_timestamp() AND j.status IN ('preparing','rendering') AND NOT j.cancellation_requested FOR UPDATE OF a,j`, attemptID, epoch).Scan(&prior)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if prior != "" && prior != containerID {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_attempts SET container_id=$3 WHERE attempt_id=$1 AND fencing_epoch=$2`, attemptID, epoch, containerID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// RecordRecoveredContainer lets the janitor persist an ID discovered from the
// immutable Docker create key after the controller lost the create response.
// It is deliberately usable after job termination, because an unknown
// container keeps its execution reservation until the exact ID is stopped.
func (s *Store) RecordRecoveredContainer(ctx context.Context, attemptID string, epoch int64, createIntentKey, containerID string) error {
	if createIntentKey == "" || strings.TrimSpace(containerID) == "" || len(containerID) > 256 || strings.ContainsAny(containerID, "\x00\r\n") {
		return errors.New("recovered container requires its durable create key and bounded ID")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var storedKey, storedID, state string
	var stopped bool
	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT a.create_container_intent_key,a.container_id,a.state,a.stop_confirmed,j.attempt_epoch
		FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id
		WHERE a.attempt_id=$1 AND a.fencing_epoch=$2 FOR UPDATE OF a,j`, attemptID, epoch).
		Scan(&storedKey, &storedID, &state, &stopped, &currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if currentEpoch != epoch || storedKey != createIntentKey {
		return ErrLeaseLost
	}
	if state != "active" || stopped {
		return ErrInvalidTransition
	}
	if storedID != "" && storedID != containerID {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_attempts SET container_id=$2 WHERE attempt_id=$1`, attemptID, containerID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// MarkAttemptStopped is a monotonic janitor/controller fact. It verifies the
// exact container recorded for the current epoch and is the only operation that
// releases an execution reservation after an external container was created.
// An unknown container identity cannot establish that execution has stopped.
func (s *Store) MarkAttemptStopped(ctx context.Context, attemptID string, epoch int64, containerID string) error {
	if strings.TrimSpace(containerID) == "" {
		return errors.New("an exact nonempty container ID is required to confirm stop")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var jobID, storedID, state, jobStatus, workspace, project string
	var confirmed bool
	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT a.job_id,a.container_id,a.state,a.stop_confirmed,j.attempt_epoch,j.status,j.workspace_id,j.project_id FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.fencing_epoch=$2 FOR UPDATE OF a,j`, attemptID, epoch).Scan(&jobID, &storedID, &state, &confirmed, &currentEpoch, &jobStatus, &workspace, &project)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if currentEpoch != epoch {
		return ErrLeaseLost
	}
	if storedID != containerID {
		return ErrConflict
	}
	if confirmed {
		return commit(ctx, tx)
	}
	if state != "active" && state != "retryable" && state != "finished" {
		return ErrInvalidTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_attempts SET stop_confirmed=true,stopped_at=clock_timestamp(),state=CASE WHEN state='active' THEN 'stopped' ELSE state END WHERE attempt_id=$1 AND fencing_epoch=$2`, attemptID, epoch); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, jobID); err != nil {
		return err
	}
	if jobStatus == "cancelling" {
		var anyUnstopped bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_attempts WHERE job_id=$1 AND stop_confirmed=false)`, jobID).Scan(&anyUnstopped); err != nil {
			return err
		}
		if !anyUnstopped {
			if err = finalizeCancelTx(ctx, tx, s, jobID, workspace, project); err != nil {
				return err
			}
		}
	}
	return commit(ctx, tx)
}

// AbandonUncreatedAttempt is the only empty-container cleanup path. It locks
// the job against SaveIntent and proves no durable container.create request
// exists. Once such an intent exists, only exact-ID stop confirmation suffices.
func (s *Store) AbandonUncreatedAttempt(ctx context.Context, attemptID string, epoch int64) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var jobID string
	if err = tx.QueryRow(ctx, `SELECT job_id FROM generation_attempts WHERE attempt_id=$1`, attemptID).Scan(&jobID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var currentEpoch int64
	var status, workspace, project string
	if err = tx.QueryRow(ctx, `SELECT attempt_epoch,status,workspace_id,project_id FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&currentEpoch, &status, &workspace, &project); err != nil {
		return err
	}
	if currentEpoch != epoch {
		return ErrLeaseLost
	}
	var containerID, state string
	var stopped bool
	if err = tx.QueryRow(ctx, `SELECT container_id,state,stop_confirmed FROM generation_attempts WHERE attempt_id=$1 AND fencing_epoch=$2 FOR UPDATE`, attemptID, epoch).Scan(&containerID, &state, &stopped); errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	} else if err != nil {
		return err
	}
	var createMayHaveRun bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND kind=$3)`, jobID, attemptID, IntentContainerCreate).Scan(&createMayHaveRun); err != nil {
		return err
	}
	if createMayHaveRun || containerID != "" {
		return ErrInvalidTransition
	}
	if stopped {
		return commit(ctx, tx)
	}
	if state != "active" {
		return ErrInvalidTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_attempts SET stop_confirmed=true,stopped_at=clock_timestamp(),state='stopped' WHERE attempt_id=$1`, attemptID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, jobID); err != nil {
		return err
	}
	if status == "cancelling" {
		if err = finalizeCancelTx(ctx, tx, s, jobID, workspace, project); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

// MarkAttemptRendered requires a confirmed container stop before a result can
// enter the independent publication state.
func (s *Store) MarkAttemptRendered(ctx context.Context, attemptID string, epoch int64) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var jobID, status string
	var currentEpoch int64
	var stopConfirmed bool
	var leaseOK bool
	err = tx.QueryRow(ctx, `SELECT a.job_id,j.status,j.attempt_epoch,a.stop_confirmed,a.owner_lease_until>clock_timestamp() FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.fencing_epoch=$2 FOR UPDATE OF a,j`, attemptID, epoch).Scan(&jobID, &status, &currentEpoch, &stopConfirmed, &leaseOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if currentEpoch != epoch || !leaseOK {
		return ErrLeaseLost
	}
	if !stopConfirmed {
		return ErrInvalidTransition
	}
	if status != "rendering" || status == "cancelling" {
		return ErrInvalidTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_attempts SET state='finished' WHERE attempt_id=$1 AND fencing_epoch=$2`, attemptID, epoch); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET status='publishing',phase='publication_pending',updated_at=clock_timestamp(),version=version+1 WHERE job_id=$1`, jobID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// RetryStoppedAttempt requeues only an explicitly retryable attempt after its
// exact container has stopped. The store enforces the three-attempt and deadline
// bounds; the service decides which failures are retryable.
func (s *Store) RetryStoppedAttempt(ctx context.Context, attemptID string, epoch int64) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if err = lockQueue(ctx, tx); err != nil {
		return err
	}
	var jobID, workspace, status string
	var attemptNo int
	var currentEpoch int64
	var stopped, deadlineOK, canceled bool
	err = tx.QueryRow(ctx, `SELECT a.job_id,j.workspace_id,j.status,a.attempt_number,j.attempt_epoch,a.stop_confirmed,j.deadline>clock_timestamp(),j.cancellation_requested FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.fencing_epoch=$2 FOR UPDATE OF a,j`, attemptID, epoch).Scan(&jobID, &workspace, &status, &attemptNo, &currentEpoch, &stopped, &deadlineOK, &canceled)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if currentEpoch != epoch {
		return ErrLeaseLost
	}
	if !stopped {
		return ErrInvalidTransition
	}
	if status != "rendering" && status != "preparing" {
		return ErrInvalidTransition
	}
	if canceled {
		return ErrInvalidTransition
	}
	if !deadlineOK || attemptNo >= defaultAttempts {
		return ErrInvalidTransition
	}
	var globalQueued, workspaceQueued int
	if err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE reservation_type='queue'),count(*) FILTER(WHERE reservation_type='queue' AND workspace_id=$1) FROM generation_reservations`, workspace).Scan(&globalQueued, &workspaceQueued); err != nil {
		return err
	}
	if globalQueued >= s.cfg.QueueGlobal || workspaceQueued >= s.cfg.QueueWorkspace {
		return ErrCapacity
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_attempts SET state='retryable' WHERE attempt_id=$1 AND fencing_epoch=$2`, attemptID, epoch); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO generation_reservations(job_id,workspace_id,reservation_type) VALUES($1,$2,'queue') ON CONFLICT(job_id) DO UPDATE SET workspace_id=EXCLUDED.workspace_id,reservation_type='queue',created_at=clock_timestamp()`, jobID, workspace); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET status='queued',phase='retry_queued',updated_at=clock_timestamp(),version=version+1 WHERE job_id=$1`, jobID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, scheduleAttemptCleanupSQL, attemptID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) ListUnstoppedAttempts(ctx context.Context, limit int) ([]Attempt, error) {
	if limit < 1 || limit > 500 {
		return nil, errors.New("attempt batch limit must be between 1 and 500")
	}
	rows, err := s.pool.Query(ctx, `SELECT attempt_id,job_id,attempt_number,fencing_epoch,state,create_container_intent_key,staging_token,private_staging_path,container_id,stop_confirmed,owner_lease_until,heartbeat_at,created_at FROM generation_attempts WHERE stop_confirmed=false ORDER BY created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err = rows.Scan(&a.AttemptID, &a.JobID, &a.AttemptNumber, &a.FencingEpoch, &a.State, &a.CreateContainerIntentKey, &a.StagingToken, &a.PrivateStagingPath, &a.ContainerID, &a.StopConfirmed, &a.OwnerLeaseUntil, &a.HeartbeatAt, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) CurrentAttempt(ctx context.Context, jobID string) (Attempt, error) {
	var a Attempt
	err := s.pool.QueryRow(ctx, `SELECT attempt_id,job_id,attempt_number,fencing_epoch,state,create_container_intent_key,staging_token,private_staging_path,container_id,stop_confirmed,owner_lease_until,heartbeat_at,created_at FROM generation_attempts WHERE job_id=$1 ORDER BY attempt_number DESC LIMIT 1`, jobID).Scan(&a.AttemptID, &a.JobID, &a.AttemptNumber, &a.FencingEpoch, &a.State, &a.CreateContainerIntentKey, &a.StagingToken, &a.PrivateStagingPath, &a.ContainerID, &a.StopConfirmed, &a.OwnerLeaseUntil, &a.HeartbeatAt, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// RecordPublishedDerivedIdentity binds the exported immutable bundle only after
// the renderer has stopped and a current publication owner may perform uploads.
func (s *Store) RecordPublishedDerivedIdentity(ctx context.Context, lease PublicationLease, attemptID, snapshotID, contentID string) error {
	if snapshotID == "" || contentID == "" || len(snapshotID) > 256 || len(contentID) > 256 {
		return ErrInvalidTransition
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var oldSnapshot, oldContent string
	err = tx.QueryRow(ctx, `SELECT derived_snapshot_id,derived_content_id FROM generation_jobs WHERE job_id=$1 AND status='publishing' AND publication_owner=$2 AND publication_epoch=$3 AND publication_lease_until>clock_timestamp() AND NOT cancellation_requested AND deadline>clock_timestamp() FOR UPDATE`, lease.JobID, lease.Owner, lease.Epoch).Scan(&oldSnapshot, &oldContent)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	var stopped bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_attempts WHERE job_id=$1 AND attempt_id=$2 AND state='finished' AND stop_confirmed)`, lease.JobID, attemptID).Scan(&stopped); err != nil {
		return err
	}
	if !stopped {
		return ErrInvalidTransition
	}
	if oldSnapshot != "" && oldSnapshot != snapshotID || oldContent != "" && oldContent != contentID {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET derived_snapshot_id=$2,derived_content_id=$3,updated_at=clock_timestamp(),version=version+CASE WHEN derived_snapshot_id='' THEN 1 ELSE 0 END WHERE job_id=$1`, lease.JobID, snapshotID, contentID); err != nil {
		return err
	}
	return commit(ctx, tx)
}
