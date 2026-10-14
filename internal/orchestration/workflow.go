package orchestration

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName        = "generation.render"
	ExecuteActivityName = "generation.execute"
	TaskQueue           = "generation-controller"
)

// RenderWorkflow carries only the durable job ID. Request data and parameters
// are recovered from Generation's Store by the controller activity.
func RenderWorkflow(ctx workflow.Context, jobID string) error {
	if jobID == "" || len(jobID) > 256 {
		return errors.New("valid Generation job ID required")
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              TaskQueue,
		ScheduleToCloseTimeout: 15 * time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    20 * time.Second,
			// Control/lease/transport retries are bounded by this workflow's
			// deadline. Store separately caps actual renderer attempts at three.
			MaximumAttempts: 0,
		},
	})
	return workflow.ExecuteActivity(ctx, ExecuteActivityName, jobID).Get(ctx, nil)
}
