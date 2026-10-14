package orchestration

import (
	"context"
	"errors"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// RegisterWorkflow installs the versioned workflow name used by the durable
// dispatcher and historical Temporal executions.
func RegisterWorkflow(registry worker.WorkflowRegistry) error {
	if registry == nil {
		return errors.New("Temporal workflow registry is required")
	}
	registry.RegisterWorkflowWithOptions(RenderWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	return nil
}

// RegisterExecuteActivity lets the controller process own the named activity
// implementation while the workflow worker remains a separate process.
func RegisterExecuteActivity(registry worker.ActivityRegistry, execute func(context.Context, string) error) error {
	if registry == nil || execute == nil {
		return errors.New("Temporal activity registry and executor are required")
	}
	registry.RegisterActivityWithOptions(execute, activity.RegisterOptions{Name: ExecuteActivityName})
	return nil
}

func NewWorkflowWorker(temporalClient client.Client, options worker.Options) (worker.Worker, error) {
	if temporalClient == nil {
		return nil, errors.New("Temporal client is required")
	}
	instance := worker.New(temporalClient, TaskQueue, options)
	if err := RegisterWorkflow(instance); err != nil {
		instance.Stop()
		return nil, err
	}
	return instance, nil
}

func NewActivityWorker(temporalClient client.Client, options worker.Options, execute func(context.Context, string) error) (worker.Worker, error) {
	if temporalClient == nil {
		return nil, errors.New("Temporal client is required")
	}
	instance := worker.New(temporalClient, TaskQueue, options)
	if err := RegisterExecuteActivity(instance, execute); err != nil {
		instance.Stop()
		return nil, err
	}
	return instance, nil
}
