package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// FindIntent reads an immutable request/receipt across publisher takeovers.
// The current owner must still SaveIntent before replaying any external effect.
func (s *Store) FindIntent(ctx context.Context, jobID, attemptID, kind, key string) (OperationIntent, error) {
	var out OperationIntent
	err := scanIntent(s.pool.QueryRow(ctx, `SELECT intent_id,job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at,abandoned_at,created_at FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND kind=$3 AND idempotency_key=$4 ORDER BY intent_id DESC LIMIT 1`, jobID, attemptID, kind, key), &out)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

// RecordAttemptFailureClassification is cleanup state, not a new external
// effect. It may outlive the job deadline while its exact fenced attempt still
// has a live owner/recovery lease. Cancellation and other terminal outcomes win.
func (s *Store) RecordAttemptFailureClassification(ctx context.Context, attemptID string, epoch int64, phase, code string) error {
	if attemptID == "" || epoch <= 0 || phase != "resource_limit" || code != "resource_limit" {
		return ErrInvalidTransition
	}
	request, _ := json.Marshal(struct {
		Phase       string `json:"phase"`
		FailureCode string `json:"failureCode"`
	}{phase, code})
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
	var status string
	var current int64
	if err = tx.QueryRow(ctx, `SELECT status,attempt_epoch FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&status, &current); err != nil {
		return err
	}
	var aEpoch int64
	var state string
	var live bool
	if err = tx.QueryRow(ctx, `SELECT fencing_epoch,state,owner_lease_until>clock_timestamp() FROM generation_attempts WHERE attempt_id=$1 FOR UPDATE`, attemptID).Scan(&aEpoch, &state, &live); err != nil {
		return err
	}
	if current != epoch || aEpoch != epoch || !live {
		return ErrLeaseLost
	}
	if (status != "preparing" && status != "rendering" && status != "expired") || (state != "active" && state != "stopped") {
		return ErrInvalidTransition
	}
	var prior []byte
	err = tx.QueryRow(ctx, `SELECT request FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND kind='render.failure' AND idempotency_key='failure' ORDER BY intent_id DESC LIMIT 1`, jobID, attemptID).Scan(&prior)
	if err == nil && !bytes.Equal(prior, request) {
		return ErrConflict
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO generation_operation_intents(job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at) VALUES($1,$2,$3,0,'render.failure','failure',$4,false,'{"recorded":true}'::bytea,clock_timestamp()) ON CONFLICT(job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key) DO NOTHING`, jobID, attemptID, epoch, request); err != nil {
		return err
	}
	return commit(ctx, tx)
}

const attemptColumns = `a.attempt_id,a.job_id,a.attempt_number,a.fencing_epoch,a.state,a.create_container_intent_key,a.staging_token,a.private_staging_path,a.container_id,a.stop_confirmed,a.owner_lease_until,a.heartbeat_at,a.created_at`

func scanAttempt(row interface{ Scan(...any) error }) (Attempt, error) {
	var a Attempt
	err := row.Scan(&a.AttemptID, &a.JobID, &a.AttemptNumber, &a.FencingEpoch, &a.State, &a.CreateContainerIntentKey, &a.StagingToken, &a.PrivateStagingPath, &a.ContainerID, &a.StopConfirmed, &a.OwnerLeaseUntil, &a.HeartbeatAt, &a.CreatedAt)
	return a, err
}

func (s *Store) GetAttempt(ctx context.Context, jobID, attemptID string) (Attempt, error) {
	a, err := scanAttempt(s.pool.QueryRow(ctx, `SELECT `+attemptColumns+` FROM generation_attempts a WHERE a.job_id=$1 AND a.attempt_id=$2`, jobID, attemptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

func (s *Store) Ready(ctx context.Context) error {
	n, err := generationLedger(ctx, s.pool)
	if err != nil {
		return err
	}
	if n != len(generationMigrationFiles) {
		return errors.New("Generation schema upgrade required")
	}
	rows, err := s.pool.Query(ctx, `SELECT j.job_id,j.deadline,j.normalized_request,j.original_identity,j.authority_release_id,j.authority_descriptor_digest,a.recovery_claimed,w.state,o.sent_at FROM generation_jobs j LEFT JOIN generation_attempts a ON a.job_id=j.job_id LEFT JOIN generation_workflow_starts w ON w.job_id=j.job_id LEFT JOIN generation_terminal_outbox t ON t.job_id=j.job_id LEFT JOIN platform_event_outbox o ON o.event_id=t.event_id LIMIT 0`)
	if err != nil {
		return err
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	rows, err = s.pool.Query(ctx, `SELECT job_id,operation_id,canonical_scope,initiating_identity FROM generation_audit_context LIMIT 0`)
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}

// AbandonAttemptIntents resolves pending work only after exact stop and a
// committed retry/terminal decision. Ambiguous live external effects stay open.
func (s *Store) AbandonAttemptIntents(ctx context.Context, attemptID string, epoch int64) error {
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
	var terminal bool
	if err = tx.QueryRow(ctx, `SELECT terminal_at IS NOT NULL FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&terminal); err != nil {
		return err
	}
	var state string
	var stopped bool
	var current int64
	if err = tx.QueryRow(ctx, `SELECT state,stop_confirmed,fencing_epoch FROM generation_attempts WHERE attempt_id=$1 FOR UPDATE`, attemptID).Scan(&state, &stopped, &current); err != nil {
		return err
	}
	if current != epoch {
		return ErrLeaseLost
	}
	if !stopped || (!terminal && state != "retryable") {
		return ErrInvalidTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_operation_intents SET abandoned_at=clock_timestamp() WHERE job_id=$1 AND render_attempt_id=$2 AND completed_at IS NULL AND abandoned_at IS NULL`, jobID, attemptID); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// ReapableAttempts uses the database clock. A cancelled or terminal job is
// immediately eligible for stopping; publishing attempts stay with publishers.
func (s *Store) ReapableAttempts(ctx context.Context, limit int) ([]Attempt, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("recovery batch must be between 1 and 100")
	}
	rows, err := s.pool.Query(ctx, `SELECT `+attemptColumns+` FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.state<>'finished' AND ((NOT a.stop_confirmed AND (a.owner_lease_until<=clock_timestamp() OR j.status IN ('cancelling','canceled','failed','expired'))) OR (a.stop_confirmed AND a.state='stopped' AND a.owner_lease_until<=clock_timestamp() AND j.status IN ('preparing','rendering'))) ORDER BY a.owner_lease_until,a.attempt_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		a, e := scanAttempt(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ClaimStaleAttempt fences the old controller before the janitor acts. Taking
// over a stopped attempt also repairs a crash between stop and render commit.
func (s *Store) ClaimStaleAttempt(ctx context.Context, attemptID string) (Attempt, error) {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return Attempt{}, err
	}
	defer rollback(ctx, tx)
	var jobID string
	if err = tx.QueryRow(ctx, `SELECT job_id FROM generation_attempts WHERE attempt_id=$1`, attemptID).Scan(&jobID); errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, ErrNotFound
	} else if err != nil {
		return Attempt{}, err
	}
	var status string
	var currentEpoch int64
	if err = tx.QueryRow(ctx, `SELECT status,attempt_epoch FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&status, &currentEpoch); err != nil {
		return Attempt{}, err
	}
	a, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM generation_attempts a WHERE a.attempt_id=$1 FOR UPDATE`, attemptID))
	if err != nil {
		return a, err
	}
	var expired, recoveryClaimed bool
	if err = tx.QueryRow(ctx, `SELECT owner_lease_until<=clock_timestamp(),recovery_claimed FROM generation_attempts WHERE attempt_id=$1`, attemptID).Scan(&expired, &recoveryClaimed); err != nil {
		return a, err
	}
	terminal := status == "cancelling" || status == "canceled" || status == "failed" || status == "expired"
	if currentEpoch != a.FencingEpoch || a.State == "finished" || (!expired && !terminal) || status == "publishing" || status == "succeeded" {
		return Attempt{}, ErrLeaseLost
	}
	// A terminal job's live janitor claim must not be repeatedly stolen. Its
	// first claim changes state, after which only lease expiry permits takeover.
	if terminal && recoveryClaimed && !expired {
		return Attempt{}, ErrLeaseLost
	}
	a.FencingEpoch++
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET attempt_epoch=$2 WHERE job_id=$1`, jobID, a.FencingEpoch); err != nil {
		return a, err
	}
	// Keep state='active' so stop confirmation and guarded failure/retry use the
	// same transitions as a controller. The lease fences other recovery workers.
	if err = tx.QueryRow(ctx, `UPDATE generation_attempts SET fencing_epoch=$2,recovery_claimed=true,owner_lease_until=clock_timestamp()+$3*interval '1 microsecond',heartbeat_at=clock_timestamp() WHERE attempt_id=$1 RETURNING owner_lease_until`, attemptID, a.FencingEpoch, s.cfg.AttemptLease.Microseconds()).Scan(&a.OwnerLeaseUntil); err != nil {
		return a, err
	}
	if err = commit(ctx, tx); err != nil {
		return Attempt{}, err
	}
	return a, nil
}

const scheduleAttemptCleanupSQL = `INSERT INTO generation_cleanup_intents(job_id,kind,idempotency_key,request,state,not_before) SELECT a.job_id,'attempt.files',$1,convert_to(json_build_object('stagingToken',a.staging_token,'path',a.private_staging_path)::text,'UTF8'),'pending',clock_timestamp() FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.attempt_id=$1 AND a.stop_confirmed AND (j.terminal_at IS NOT NULL OR a.state='retryable') ON CONFLICT(job_id,kind,idempotency_key) DO NOTHING`

// ScheduleAttemptCleanup accepts stopped terminal or superseded attempts.
func (s *Store) ScheduleAttemptCleanup(ctx context.Context, attemptID string) error {
	_, err := s.pool.Exec(ctx, scheduleAttemptCleanupSQL, attemptID)
	return err
}

// TerminalAttempts provides durable cleanup work after cancellation or after a
// controller loses the terminal commit acknowledgement.
func (s *Store) TerminalAttempts(ctx context.Context, limit int) ([]Attempt, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("cleanup batch must be between 1 and 100")
	}
	rows, err := s.pool.Query(ctx, `SELECT `+attemptColumns+` FROM generation_attempts a JOIN generation_jobs j ON j.job_id=a.job_id WHERE a.stop_confirmed AND (j.terminal_at IS NOT NULL OR a.state='retryable') AND NOT EXISTS(SELECT 1 FROM generation_cleanup_intents c WHERE c.job_id=a.job_id AND c.kind='attempt.files' AND c.idempotency_key=a.attempt_id) ORDER BY COALESCE(j.terminal_at,a.stopped_at),a.attempt_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		a, e := scanAttempt(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// PendingExecutionJobs lets recovery repair work independently of any one
// activity response. It carries IDs only, never request or source bytes.
func (s *Store) PendingExecutionJobs(ctx context.Context, limit int) ([]string, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("execution batch must be between 1 and 100")
	}
	rows, err := s.pool.Query(ctx, `SELECT job_id FROM generation_jobs WHERE status IN ('queued','publishing') AND deadline>clock_timestamp() AND NOT cancellation_requested ORDER BY updated_at LIMIT $1`, limit)
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
