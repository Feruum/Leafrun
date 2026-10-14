package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/leafrun/leafrun-contracts/generation/v1"
)

var kindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){0,3}$`)

func sensitiveIntent(kind string) bool {
	switch kind {
	case IntentDeriveBundle, IntentRenderInput, "render.bundle", "render.input_bundle":
		return true
	default:
		return false
	}
}

func validateIntent(in IntentInput) error {
	if in.JobID == "" || !generationv1.ValidIdempotencyKey(in.IdempotencyKey) || len(in.Request) == 0 || len(in.Request) > 16<<20 {
		return errors.New("intent requires a job, bounded key and exact request bytes")
	}
	if len(in.Kind) > 64 || !kindPattern.MatchString(in.Kind) {
		return errors.New("intent kind must be a bounded dotted lowercase identifier")
	}
	if in.RenderAttemptID == "" && in.AttemptEpoch != 0 {
		return errors.New("attempt epoch requires a render attempt")
	}
	if in.RenderAttemptID != "" && in.AttemptEpoch <= 0 {
		return errors.New("render attempt requires its positive fencing epoch")
	}
	if in.PublicationEpoch < 0 {
		return errors.New("publication epoch cannot be negative")
	}
	if in.PublicationEpoch > 0 && in.Owner == "" {
		return errors.New("publication intents require the current publisher identity")
	}
	return nil
}

// checkIntentOwner is called while the job row is locked. It prevents stale
// attempts and expired publishers from changing even completed intent receipts.
func checkIntentOwner(ctx context.Context, tx pgx.Tx, in IntentInput) error {
	var state string
	var canceled bool
	var currentAttemptEpoch, currentPublicationEpoch int64
	var publisher string
	var publicationLeaseOK, deadlineOK bool
	err := tx.QueryRow(ctx, `SELECT status,cancellation_requested,attempt_epoch,publication_epoch,publication_owner,COALESCE(publication_lease_until>clock_timestamp(),false),deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1`, in.JobID).Scan(&state, &canceled, &currentAttemptEpoch, &currentPublicationEpoch, &publisher, &publicationLeaseOK, &deadlineOK)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if canceled || !deadlineOK || state == "cancelling" || state == "succeeded" || state == "failed" || state == "canceled" || state == "expired" {
		return ErrInvalidTransition
	}
	if in.RenderAttemptID != "" {
		var attemptState string
		var stopped, leaseOK bool
		err = tx.QueryRow(ctx, `SELECT state,stop_confirmed,owner_lease_until>clock_timestamp() FROM generation_attempts WHERE attempt_id=$1 AND job_id=$2 AND fencing_epoch=$3`, in.RenderAttemptID, in.JobID, in.AttemptEpoch).Scan(&attemptState, &stopped, &leaseOK)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if currentAttemptEpoch != in.AttemptEpoch {
			return ErrLeaseLost
		}
		if in.Kind == IntentContainerCreate {
			var createKey string
			if err = tx.QueryRow(ctx, `SELECT create_container_intent_key FROM generation_attempts WHERE attempt_id=$1`, in.RenderAttemptID).Scan(&createKey); err != nil {
				return err
			}
			if in.IdempotencyKey != createKey {
				return ErrConflict
			}
		}
		if in.PublicationEpoch > 0 {
			if attemptState != "finished" || !stopped {
				return ErrInvalidTransition
			}
		} else if attemptState != "active" || !leaseOK {
			return ErrLeaseLost
		}
	}
	if in.Kind == IntentContainerCreate && in.RenderAttemptID == "" {
		return ErrInvalidTransition
	}
	if in.PublicationEpoch > 0 && (currentPublicationEpoch != in.PublicationEpoch || publisher != in.Owner || !publicationLeaseOK) {
		return ErrLeaseLost
	}
	return nil
}

// SaveIntent writes an exact external-operation request before the call. A
// repeated key is replayable only for identical bytes and the same owner epoch.
func (s *Store) SaveIntent(ctx context.Context, in IntentInput) (OperationIntent, error) {
	var out OperationIntent
	if err := validateIntent(in); err != nil {
		return out, err
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	var lockedJob string
	err = tx.QueryRow(ctx, `SELECT job_id FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&lockedJob)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	} else if err != nil {
		return out, err
	}
	if err = checkIntentOwner(ctx, tx, in); err != nil {
		return out, err
	}
	var previous []byte
	err = tx.QueryRow(ctx, `SELECT request FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND kind=$3 AND idempotency_key=$4 ORDER BY intent_id LIMIT 1`, in.JobID, in.RenderAttemptID, in.Kind, in.IdempotencyKey).Scan(&previous)
	if err == nil && !bytes.Equal(previous, in.Request) {
		return OperationIntent{}, ErrConflict
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return OperationIntent{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_operation_intents(job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key) DO NOTHING`, in.JobID, in.RenderAttemptID, in.AttemptEpoch, in.PublicationEpoch, in.Kind, in.IdempotencyKey, in.Request, in.Sensitive || sensitiveIntent(in.Kind))
	if err != nil {
		return out, fmt.Errorf("save generation operation intent: %w", err)
	}
	if err = scanIntent(tx.QueryRow(ctx, `SELECT intent_id,job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at,abandoned_at,created_at FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND attempt_epoch=$3 AND publication_epoch=$4 AND kind=$5 AND idempotency_key=$6`, in.JobID, in.RenderAttemptID, in.AttemptEpoch, in.PublicationEpoch, in.Kind, in.IdempotencyKey), &out); err != nil {
		return OperationIntent{}, err
	}
	if !bytes.Equal(out.Request, in.Request) {
		return OperationIntent{}, ErrConflict
	}
	if err = commit(ctx, tx); err != nil {
		return OperationIntent{}, err
	}
	return out, nil
}

// CompleteIntent persists the exact external response without replacing the
// immutable request bytes. A repeated completion must have byte-identical data.
func (s *Store) CompleteIntent(ctx context.Context, in IntentInput, output []byte) (OperationIntent, error) {
	var out OperationIntent
	if err := validateIntent(in); err != nil {
		return out, err
	}
	if len(output) == 0 || len(output) > 16<<20 {
		return out, errors.New("intent output must be bounded nonempty bytes")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	var lockedJob string
	err = tx.QueryRow(ctx, `SELECT job_id FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, in.JobID).Scan(&lockedJob)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	} else if err != nil {
		return out, err
	}
	if err = checkIntentOwner(ctx, tx, in); err != nil {
		return out, err
	}
	if err = scanIntent(tx.QueryRow(ctx, `SELECT intent_id,job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at,abandoned_at,created_at FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND attempt_epoch=$3 AND publication_epoch=$4 AND kind=$5 AND idempotency_key=$6 FOR UPDATE`, in.JobID, in.RenderAttemptID, in.AttemptEpoch, in.PublicationEpoch, in.Kind, in.IdempotencyKey), &out); errors.Is(err, pgx.ErrNoRows) {
		return OperationIntent{}, ErrNotFound
	} else if err != nil {
		return OperationIntent{}, err
	}
	if !bytes.Equal(out.Request, in.Request) {
		return OperationIntent{}, ErrConflict
	}
	if out.AbandonedAt != nil {
		return OperationIntent{}, ErrInvalidTransition
	}
	if out.CompletedAt != nil {
		if !bytes.Equal(out.Output, output) {
			return OperationIntent{}, ErrConflict
		}
		if err = commit(ctx, tx); err != nil {
			return OperationIntent{}, err
		}
		return out, nil
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_operation_intents SET output=$2,completed_at=clock_timestamp() WHERE intent_id=$1`, out.ID, output); err != nil {
		return OperationIntent{}, err
	}
	if err = scanIntent(tx.QueryRow(ctx, `SELECT intent_id,job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at,abandoned_at,created_at FROM generation_operation_intents WHERE intent_id=$1`, out.ID), &out); err != nil {
		return OperationIntent{}, err
	}
	if err = commit(ctx, tx); err != nil {
		return OperationIntent{}, err
	}
	return out, nil
}

func scanIntent(row interface{ Scan(...any) error }, out *OperationIntent) error {
	var completed *time.Time
	var abandoned *time.Time
	err := row.Scan(&out.ID, &out.JobID, &out.RenderAttemptID, &out.AttemptEpoch, &out.PublicationEpoch, &out.Kind, &out.IdempotencyKey, &out.Request, &out.Sensitive, &out.Output, &completed, &abandoned, &out.CreatedAt)
	if err != nil {
		return err
	}
	out.CompletedAt = completed
	out.AbandonedAt = abandoned
	return nil
}

func (s *Store) GetIntent(ctx context.Context, in IntentInput) (OperationIntent, error) {
	var out OperationIntent
	if err := validateIntent(in); err != nil {
		return out, err
	}
	err := scanIntent(s.pool.QueryRow(ctx, `SELECT intent_id,job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at,abandoned_at,created_at FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND attempt_epoch=$3 AND publication_epoch=$4 AND kind=$5 AND idempotency_key=$6`, in.JobID, in.RenderAttemptID, in.AttemptEpoch, in.PublicationEpoch, in.Kind, in.IdempotencyKey), &out)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationIntent{}, ErrNotFound
	}
	return out, err
}

func (s *Store) ListPendingIntents(ctx context.Context, jobID string, limit int) ([]OperationIntent, error) {
	if limit < 1 || limit > 500 {
		return nil, errors.New("intent batch limit must be between 1 and 500")
	}
	rows, err := s.pool.Query(ctx, `SELECT intent_id,job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key,request,sensitive,output,completed_at,abandoned_at,created_at FROM generation_operation_intents WHERE job_id=$1 AND completed_at IS NULL AND abandoned_at IS NULL ORDER BY intent_id LIMIT $2`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OperationIntent
	for rows.Next() {
		var x OperationIntent
		if err = scanIntent(rows, &x); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
