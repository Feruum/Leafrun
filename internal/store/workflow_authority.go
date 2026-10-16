package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (s *Store) AuthorizeWorkflowStart(ctx context.Context, jobID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var valid bool
	if err = tx.QueryRow(ctx, `SELECT status='queued' AND NOT cancellation_requested AND deadline>clock_timestamp() FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&valid); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if !valid {
		return ErrInvalidTransition
	}
	if err = s.gateAuthority(ctx, tx, jobID, "generation.execute"); err != nil {
		if errors.Is(err, ErrAuthorityDenied) {
			_ = tx.Rollback(ctx)
			return errors.Join(err, s.FailJob(ctx, TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"}))
		}
		return err
	}
	return tx.Commit(ctx)
}
