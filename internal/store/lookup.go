package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
)

// LookupAdmission is a read-only replay preflight. Admit remains the atomic
// authority when another request creates or expires the receipt concurrently.
func (s *Store) LookupAdmission(ctx context.Context, workspaceID, initiatorID, key, fingerprint string) (generationv1.Admission, bool, error) {
	var out generationv1.Admission
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, false, err
	}
	defer rollback(ctx, tx)
	var jobID, saved string
	err = tx.QueryRow(ctx, `SELECT job_id,fingerprint,expires_at FROM generation_admission_receipts WHERE workspace_id=$1 AND initiator_id=$2 AND idempotency_key=$3 AND expires_at>clock_timestamp() FOR SHARE`, workspaceID, initiatorID, key).Scan(&jobID, &saved, &out.IdempotencyExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if saved != fingerprint {
		return generationv1.Admission{}, false, ErrConflict
	}
	row, err := scanJob(tx.QueryRow(ctx, jobSelect+` WHERE job_id=$1`, jobID))
	if err != nil {
		return generationv1.Admission{}, false, err
	}
	if err = s.gateRequestAuthority(ctx, tx, jobID, true); err != nil {
		return generationv1.Admission{}, false, err
	}
	if err = commit(ctx, tx); err != nil {
		return generationv1.Admission{}, false, err
	}
	out.Job = row.dto()
	return out, true, nil
}
