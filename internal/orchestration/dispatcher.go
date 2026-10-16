package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

const (
	// Serial Temporal calls must fit wholly inside each claimed start lease.
	workflowStartBatch = 1
	workflowStartLease = 30 * time.Second
	workflowStartCall  = 20 * time.Second
)

type WorkflowStartStore interface {
	ClaimWorkflowStarts(context.Context, string, int, time.Duration) ([]store.WorkflowStartIntent, error)
	MarkWorkflowStarted(context.Context, string, string, string, int64) error
}

type TemporalClient interface {
	ExecuteWorkflow(context.Context, client.StartWorkflowOptions, any, ...any) (client.WorkflowRun, error)
}

type workflowAuthorityStore interface {
	AuthorizeWorkflowStart(context.Context, string) error
}

type Dispatcher struct {
	store    WorkflowStartStore
	temporal TemporalClient
	workerID string
}

func NewDispatcher(state WorkflowStartStore, temporalClient TemporalClient, workerID string) (*Dispatcher, error) {
	if state == nil || temporalClient == nil || workerID == "" || len(workerID) > 128 || strings.TrimSpace(workerID) != workerID {
		return nil, errors.New("workflow-start Store, Temporal client and bounded worker ID are required")
	}
	return &Dispatcher{store: state, temporal: temporalClient, workerID: workerID}, nil
}

func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	intents, err := d.store.ClaimWorkflowStarts(ctx, d.workerID, workflowStartBatch, workflowStartLease)
	if err != nil {
		return 0, fmt.Errorf("claim Temporal workflow starts: %w", err)
	}
	started := 0
	var failures []error
	for _, intent := range intents {
		if ctx.Err() != nil {
			return started, errors.Join(append(failures, ctx.Err())...)
		}
		if err = validateWorkflowIntent(intent); err != nil {
			failures = append(failures, fmt.Errorf("invalid durable workflow intent for %s: %w", intent.JobID, err))
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, workflowStartCall)
		if authority, ok := d.store.(workflowAuthorityStore); ok {
			if err = authority.AuthorizeWorkflowStart(callCtx, intent.JobID); err != nil {
				cancel()
				failures = append(failures, err)
				continue
			}
		}
		run, startErr := d.temporal.ExecuteWorkflow(callCtx, client.StartWorkflowOptions{
			ID: intent.WorkflowID, TaskQueue: TaskQueue,
			WorkflowExecutionTimeout:                 15 * time.Minute,
			WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowIDConflictPolicy:                 enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			WorkflowExecutionErrorWhenAlreadyStarted: true,
		}, WorkflowName, intent.JobID)
		cancel()
		if startErr != nil && !temporal.IsWorkflowExecutionAlreadyStartedError(startErr) {
			failures = append(failures, fmt.Errorf("start Temporal workflow %s: %w", intent.WorkflowID, startErr))
			continue
		}
		if startErr == nil && (run == nil || run.GetID() != intent.WorkflowID) {
			failures = append(failures, fmt.Errorf("Temporal returned an unexpected workflow identity for %s", intent.WorkflowID))
			continue
		}
		if err = d.store.MarkWorkflowStarted(ctx, intent.JobID, intent.WorkflowID, intent.LeaseOwner, intent.LeaseEpoch); err != nil {
			failures = append(failures, fmt.Errorf("acknowledge Temporal workflow %s: %w", intent.WorkflowID, err))
			continue
		}
		started++
	}
	return started, errors.Join(failures...)
}

func validateWorkflowIntent(intent store.WorkflowStartIntent) error {
	if intent.JobID == "" || len(intent.JobID) > 256 || intent.WorkflowID != "render/"+intent.JobID || intent.State != "pending" || intent.LeaseOwner == "" || intent.LeaseEpoch < 1 {
		return errors.New("workflow identity or claim lease is invalid")
	}
	var request struct {
		JobID string `json:"jobId"`
	}
	if err := jsonvalue.Unmarshal(intent.Request, &request, 1<<20); err != nil {
		return errors.New("workflow request is invalid")
	}
	if request.JobID != intent.JobID {
		return errors.New("workflow request does not match its durable job identity")
	}
	canonical, err := json.Marshal(struct {
		JobID string `json:"jobId"`
	}{JobID: intent.JobID})
	if err != nil || !bytes.Equal(canonical, intent.Request) {
		return errors.New("workflow request contains fields beyond the job identity")
	}
	return nil
}

func (d *Dispatcher) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	if interval > time.Minute {
		return errors.New("workflow dispatcher interval cannot exceed one minute")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		count, err := d.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("Generation workflow dispatch iteration failed")
		}
		if count == workflowStartBatch {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
