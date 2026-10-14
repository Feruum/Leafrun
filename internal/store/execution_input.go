package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
)

// ExecutionInput is read from durable admission data, never from a workflow's
// sole copy of a request. Parameters retain their original normalized bytes.
type ExecutionInput struct {
	InitiatorID       string
	NormalizedRequest []byte
	Request           generationv1.CreateJobRequest
}

func (s *Store) GetExecutionInput(ctx context.Context, jobID string) (ExecutionInput, error) {
	var out ExecutionInput
	var fingerprint string
	err := s.pool.QueryRow(ctx, `SELECT initiator_id,normalized_request,fingerprint FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&out.InitiatorID, &out.NormalizedRequest, &fingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	request, _, actual, err := generationv1.NormalizeRequest(out.NormalizedRequest)
	if err != nil || actual != fingerprint {
		return ExecutionInput{}, fmt.Errorf("durable execution input unavailable or invalid")
	}
	out.Request = request
	return out, nil
}
