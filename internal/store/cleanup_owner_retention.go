package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/events"
)

type CleanupIntent struct {
	ID             int64
	JobID          string
	Kind           string
	IdempotencyKey string
	Request        []byte
	State          string
	NotBefore      time.Time
	LeaseOwner     string
	LeaseEpoch     int64
	LeaseUntil     *time.Time
	Output         []byte
	CreatedAt      time.Time
}

func (s *Store) ClaimCleanup(ctx context.Context, worker string, limit int, lease time.Duration) ([]CleanupIntent, error) {
	if worker == "" || len(worker) > 128 || limit < 1 || limit > 100 || lease < time.Second || lease > 30*time.Second {
		return nil, errors.New("cleanup claim requires bounded worker, batch and lease")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	if err = lockCleanup(ctx, tx); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT cleanup_id,job_id,kind,idempotency_key,request,state,not_before,lease_owner,lease_epoch,lease_until,output,created_at FROM generation_cleanup_intents
		WHERE state<>'complete' AND not_before<=clock_timestamp() AND (state='pending' OR lease_until<=clock_timestamp())
		ORDER BY not_before,cleanup_id FOR UPDATE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	var out []CleanupIntent
	for rows.Next() {
		var x CleanupIntent
		if err = rows.Scan(&x.ID, &x.JobID, &x.Kind, &x.IdempotencyKey, &x.Request, &x.State, &x.NotBefore, &x.LeaseOwner, &x.LeaseEpoch, &x.LeaseUntil, &x.Output, &x.CreatedAt); err != nil {
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
		if err = tx.QueryRow(ctx, `UPDATE generation_cleanup_intents SET state='claimed',lease_owner=$2,lease_epoch=lease_epoch+1,lease_until=clock_timestamp()+$3*interval '1 microsecond' WHERE cleanup_id=$1 RETURNING state,lease_epoch,lease_until`, out[i].ID, worker, lease.Microseconds()).Scan(&out[i].State, &out[i].LeaseEpoch, &out[i].LeaseUntil); err != nil {
			return nil, err
		}
		out[i].LeaseOwner = worker
	}
	if err = commit(ctx, tx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) CompleteCleanup(ctx context.Context, id int64, worker string, epoch int64, output []byte) error {
	if id <= 0 || worker == "" || len(output) == 0 || len(output) > 1<<20 {
		return errors.New("cleanup completion requires a bounded receipt")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var jobID, kind, key, state, owner string
	var prior []byte
	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT job_id,kind,idempotency_key,state,lease_owner,lease_epoch,output FROM generation_cleanup_intents WHERE cleanup_id=$1 FOR UPDATE`, id).Scan(&jobID, &kind, &key, &state, &owner, &currentEpoch, &prior)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if state == "complete" {
		if currentEpoch != epoch {
			return ErrLeaseLost
		}
		if string(prior) != string(output) {
			return ErrConflict
		}
		if err = commit(ctx, tx); err != nil {
			return err
		}
		return nil
	}
	var validLease bool
	if err = tx.QueryRow(ctx, `SELECT lease_until>clock_timestamp() FROM generation_cleanup_intents WHERE cleanup_id=$1`, id).Scan(&validLease); err != nil {
		return err
	}
	if state != "claimed" || owner != worker || currentEpoch != epoch || !validLease {
		return ErrLeaseLost
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_cleanup_intents SET state='complete',output=$2,completed_at=clock_timestamp(),lease_owner='',lease_until=NULL WHERE cleanup_id=$1`, id, output); err != nil {
		return err
	}
	if kind == "pin.release" {
		var pinID string
		prefix, pid, ok := strings.Cut(key, "/")
		if !ok || prefix != "release" || pid == "" {
			return ErrConflict
		}
		pinID = pid
		if _, err = tx.Exec(ctx, `UPDATE generation_pin_acquires SET state='released',updated_at=clock_timestamp() WHERE job_id=$1 AND storage_pin_id=$2`, jobID, pinID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE generation_input_resources SET pin_state='released' WHERE job_id=$1 AND pin_id=$2`, jobID, pinID); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

func (s *Store) PendingCleanup(ctx context.Context, jobID string, limit int) ([]CleanupIntent, error) {
	if limit < 1 || limit > 500 {
		return nil, errors.New("cleanup batch limit must be between 1 and 500")
	}
	rows, err := s.pool.Query(ctx, `SELECT cleanup_id,job_id,kind,idempotency_key,request,state,not_before,lease_owner,lease_epoch,lease_until,output,created_at FROM generation_cleanup_intents WHERE job_id=$1 AND state<>'complete' ORDER BY cleanup_id LIMIT $2`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CleanupIntent
	for rows.Next() {
		var x CleanupIntent
		if err = rows.Scan(&x.ID, &x.JobID, &x.Kind, &x.IdempotencyKey, &x.Request, &x.State, &x.NotBefore, &x.LeaseOwner, &x.LeaseEpoch, &x.LeaseUntil, &x.Output, &x.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// Publishable implements Storage's strict owner callback. Expiry is omitted:
// once committed, historical success remains publishable metadata until the
// Storage object itself expires and is garbage collected.
func (o *Owner) Publishable(ctx context.Context, in storagev1.PublishabilityCheck) (bool, error) {
	if o == nil || o.store == nil {
		return false, errors.New("generation owner is unavailable")
	}
	var yes bool
	err := o.store.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_jobs WHERE job_id=$1 AND workspace_id=$2 AND project_id=$3 AND status='succeeded' AND artifact_id=$4)`, in.OriginJobID, in.Scope.WorkspaceID, in.Scope.ProjectID, in.ArtifactID).Scan(&yes)
	if err != nil {
		return false, fmt.Errorf("query generation artifact owner: %w", err)
	}
	return yes, nil
}

// Pin recognizes a fully saved pending binding before Storage activates it.
// Active bindings protect execution and the bounded diagnostic history.
func (o *Owner) Pin(ctx context.Context, in storagev1.PinOwnerCheck) (storagev1.OwnerState, error) {
	if o == nil || o.store == nil {
		return storagev1.OwnerUnknown, errors.New("generation owner is unavailable")
	}
	if in.HolderID != o.store.cfg.PinHolderID || in.HolderDomain != o.store.cfg.PinHolderDomain {
		return storagev1.OwnerAbsent, nil
	}
	var yes bool
	err := o.store.pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM generation_pin_acquires p JOIN generation_jobs j ON j.job_id=p.job_id
		WHERE j.workspace_id=$1 AND j.project_id=$2 AND p.storage_pin_id=$3
		AND (p.state='active' OR (p.state='pending' AND j.terminal_at IS NULL AND NOT j.cancellation_requested AND j.deadline>clock_timestamp() AND p.expires_at>clock_timestamp()))
		AND p.object_id=$4 AND convert_from(p.request,'UTF8')::jsonb->>'objectId'=$4 AND p.response->>'objectId'=$4
		AND convert_from(p.request,'UTF8')::jsonb->>'idempotencyKey'=p.idempotency_key
		AND p.response->>'pinId'=p.storage_pin_id
		AND convert_from(p.request,'UTF8')::jsonb->'scope'->>'workspaceId'=$1
		AND convert_from(p.request,'UTF8')::jsonb->'scope'->>'projectId'=$2
		AND p.response->'scope'=convert_from(p.request,'UTF8')::jsonb->'scope'
		AND convert_from(p.request,'UTF8')::jsonb->>'holderId'=$5 AND p.response->>'holderId'=$5
		AND convert_from(p.request,'UTF8')::jsonb->>'holderDomain'=$6 AND p.response->>'holderDomain'=$6
		AND convert_from(p.request,'UTF8')::jsonb->>'retentionPolicy'=p.response->>'retentionPolicy'
		AND (p.object_id=j.source_snapshot_id OR p.object_id=j.derived_snapshot_id OR EXISTS(
			SELECT 1 FROM generation_input_resources r WHERE r.job_id=j.job_id AND r.resource_id=p.object_id
			AND r.acquisition_key=p.idempotency_key AND r.workspace_id=j.workspace_id AND r.project_id=j.project_id
			AND r.object_class='resource' AND r.pin_state IN ('pending','active')
			AND (r.pin_id='' OR r.pin_id=p.storage_pin_id)
			AND convert_from(p.request,'UTF8')::jsonb->>'retentionPolicy'='ordinary' AND p.response->>'retentionPolicy'='ordinary'
		))
		AND (j.terminal_at IS NULL OR j.terminal_at+($7::bigint*interval '1 microsecond')>clock_timestamp())
		AND NOT EXISTS(SELECT 1 FROM generation_cleanup_intents c WHERE c.job_id=j.job_id AND c.kind='pin.release' AND c.state='complete' AND c.idempotency_key='release/'||p.storage_pin_id)
	)`, in.Scope.WorkspaceID, in.Scope.ProjectID, in.PinID, in.ObjectID, in.HolderID, in.HolderDomain, o.store.cfg.DiagnosticTTL.Microseconds()).Scan(&yes)
	if err != nil {
		return storagev1.OwnerUnknown, fmt.Errorf("query Generation pin owner: %w", err)
	}
	if yes {
		return storagev1.OwnerReferenced, nil
	}
	var uncertain bool
	err = o.store.pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM generation_pin_acquires p JOIN generation_jobs j ON j.job_id=p.job_id
		WHERE j.workspace_id=$1 AND j.project_id=$2 AND p.object_id=$3
		AND p.state='pending' AND p.response IS NULL AND p.storage_pin_id=''
		AND convert_from(p.request,'UTF8')::jsonb->>'objectId'=$3
		AND convert_from(p.request,'UTF8')::jsonb->>'idempotencyKey'=p.idempotency_key
		AND convert_from(p.request,'UTF8')::jsonb->'scope'->>'workspaceId'=$1
		AND convert_from(p.request,'UTF8')::jsonb->'scope'->>'projectId'=$2
		AND convert_from(p.request,'UTF8')::jsonb->>'retentionPolicy'='ordinary'
		AND convert_from(p.request,'UTF8')::jsonb->>'holderId'=$4
		AND convert_from(p.request,'UTF8')::jsonb->>'holderDomain'=$5
		AND j.terminal_at IS NULL AND NOT j.cancellation_requested AND j.deadline>clock_timestamp()
		AND (
			(p.object_id=j.source_snapshot_id AND j.status IN ('queued','preparing'))
			OR (j.derived_snapshot_id<>'' AND p.object_id=j.derived_snapshot_id AND j.status IN ('preparing','publishing'))
			OR EXISTS(
				SELECT 1 FROM generation_input_resources r
				WHERE r.job_id=j.job_id AND r.resource_id=p.object_id AND r.acquisition_key=p.idempotency_key
				AND r.workspace_id=j.workspace_id AND r.project_id=j.project_id AND r.object_class='resource'
				AND r.pin_state='pending' AND r.pin_id=''
			)
		)
	)`, in.Scope.WorkspaceID, in.Scope.ProjectID, in.ObjectID, in.HolderID, in.HolderDomain).Scan(&uncertain)
	if err != nil {
		return storagev1.OwnerUnknown, fmt.Errorf("query Generation pending pin owner: %w", err)
	}
	if uncertain {
		return storagev1.OwnerUnknown, nil
	}
	return storagev1.OwnerAbsent, nil
}

func (s *Store) ExpireDue(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 500 {
		return 0, errors.New("reconciliation batch limit must be between 1 and 500")
	}
	rows, err := s.pool.Query(ctx, `SELECT job_id FROM generation_jobs WHERE terminal_at IS NULL AND status<>'cancelling' AND deadline<=clock_timestamp() ORDER BY deadline,job_id LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		if err = s.expireDueJob(ctx, id); err != nil && !errors.Is(err, ErrInvalidTransition) {
			return count, err
		}
		if err == nil {
			count++
		}
	}
	return count, nil
}

func (s *Store) expireDueJob(ctx context.Context, jobID string) error {
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspace, project, status string
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,status FROM generation_jobs WHERE job_id=$1 AND deadline<=clock_timestamp() FOR UPDATE`, jobID).Scan(&workspace, &project, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidTransition
	}
	if err != nil {
		return err
	}
	if status == "cancelling" || status == "succeeded" || status == "failed" || status == "canceled" || status == "expired" {
		return ErrInvalidTransition
	}
	var terminalAt time.Time
	var newVersion int64
	if err = tx.QueryRow(ctx, `UPDATE generation_jobs SET status='expired',phase='completed',failure_code='deadline_exceeded',terminal_at=clock_timestamp(),updated_at=clock_timestamp(),version=version+1,publication_owner='',publication_lease_until=NULL WHERE job_id=$1 RETURNING terminal_at,version`, jobID).Scan(&terminalAt, &newVersion); err != nil {
		return err
	}
	if err = schedulePinRelease(ctx, tx, jobID, terminalAt.Add(s.cfg.DiagnosticTTL)); err != nil {
		return err
	}
	if err = deleteQueueReservation(ctx, tx, jobID); err != nil {
		return err
	}
	if err = s.enqueueTerminal(ctx, tx, jobID, workspace, project, uint64(newVersion), terminalAt, "expired"); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (s *Store) Retain(ctx context.Context) (RetentionResult, error) {
	var out RetentionResult
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	var tag pgconn.CommandTag
	if tag, err = tx.Exec(ctx, `DELETE FROM generation_admission_receipts WHERE expires_at<=clock_timestamp()`); err != nil {
		return out, err
	}
	out.ReceiptsDeleted = tag.RowsAffected()
	if _, err = tx.Exec(ctx, `INSERT INTO generation_cleanup_intents(job_id,kind,idempotency_key,request,state,not_before)
		SELECT i.job_id,'intent.abandoned','intent/'||i.intent_id,convert_to(json_build_object('intentId',i.intent_id,'kind',i.kind)::text,'UTF8'),'pending',clock_timestamp()
		FROM generation_operation_intents i JOIN generation_jobs j ON j.job_id=i.job_id
		WHERE i.sensitive AND i.completed_at IS NULL AND i.abandoned_at IS NULL AND j.terminal_at IS NOT NULL AND j.terminal_at+$1*interval '1 microsecond'<=clock_timestamp()
		ON CONFLICT(job_id,kind,idempotency_key) DO NOTHING`, s.cfg.DiagnosticTTL.Microseconds()); err != nil {
		return out, err
	}
	if tag, err = tx.Exec(ctx, `UPDATE generation_jobs SET parameters=NULL,normalized_request=''::bytea,selection_request=NULL,selection_fingerprint=NULL,actor_subject='',actor_domain='',raw_diagnostics='',diagnostics='[]'::jsonb,updated_at=clock_timestamp() WHERE terminal_at IS NOT NULL AND terminal_at+$1*interval '1 microsecond'<=clock_timestamp() AND (parameters IS NOT NULL OR octet_length(normalized_request)>0 OR selection_request IS NOT NULL OR raw_diagnostics<>'' OR diagnostics<>'[]'::jsonb)`, s.cfg.DiagnosticTTL.Microseconds()); err != nil {
		return out, err
	}
	out.SensitiveFieldsCleared = tag.RowsAffected()
	if tag, err = tx.Exec(ctx, `UPDATE generation_operation_intents i SET request=''::bytea,output=CASE WHEN i.completed_at IS NOT NULL THEN ''::bytea ELSE NULL END,abandoned_at=CASE WHEN i.completed_at IS NULL THEN clock_timestamp() ELSE i.abandoned_at END
		FROM generation_jobs j WHERE i.job_id=j.job_id AND i.sensitive AND i.abandoned_at IS NULL AND j.terminal_at IS NOT NULL AND j.terminal_at+$1*interval '1 microsecond'<=clock_timestamp()`, s.cfg.DiagnosticTTL.Microseconds()); err != nil {
		return out, err
	}
	out.SensitiveFieldsCleared += tag.RowsAffected()
	if tag, err = tx.Exec(ctx, `DELETE FROM generation_jobs j WHERE j.terminal_at IS NOT NULL AND j.terminal_at+$1*interval '1 microsecond'<=clock_timestamp()
		AND NOT EXISTS(SELECT 1 FROM generation_admission_receipts r WHERE r.job_id=j.job_id AND r.expires_at>clock_timestamp())
		AND NOT EXISTS(SELECT 1 FROM generation_reservations r WHERE r.job_id=j.job_id)
		AND NOT EXISTS(SELECT 1 FROM generation_cleanup_intents c WHERE c.job_id=j.job_id AND c.state<>'complete')
		AND NOT EXISTS(SELECT 1 FROM generation_operation_intents i WHERE i.job_id=j.job_id AND i.completed_at IS NULL AND i.abandoned_at IS NULL)
		AND NOT EXISTS(SELECT 1 FROM generation_workflow_starts w WHERE w.job_id=j.job_id AND w.state NOT IN ('acknowledged','discarded'))
		AND NOT EXISTS(SELECT 1 FROM generation_pin_acquires p WHERE p.job_id=j.job_id AND p.state IN ('active','pending'))
		AND NOT EXISTS(SELECT 1 FROM generation_terminal_outbox t JOIN platform_event_outbox o ON o.event_id=t.event_id WHERE t.job_id=j.job_id AND o.sent_at IS NULL)
		AND NOT EXISTS(SELECT 1 FROM generation_terminal_outbox t JOIN platform_event_inbox i ON i.event_id=t.event_id WHERE t.job_id=j.job_id AND i.status='quarantined')`, s.cfg.HistoryTTL.Microseconds()); err != nil {
		return out, err
	}
	out.JobsDeleted = tag.RowsAffected()
	var dbNow time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return out, err
	}
	if _, err = events.PurgeExpired(ctx, tx, dbNow); err != nil {
		return out, err
	}
	if err = commit(ctx, tx); err != nil {
		return out, err
	}
	return out, nil
}
