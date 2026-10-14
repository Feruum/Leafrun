package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

type testStartStore struct {
	intents []store.WorkflowStartIntent
	marked  int
	markErr error
}

type budgetStartStore struct{ testStartStore }

func (s *budgetStartStore) ClaimWorkflowStarts(_ context.Context, _ string, limit int, lease time.Duration) ([]store.WorkflowStartIntent, error) {
	if time.Duration(limit)*workflowStartCall >= lease {
		return nil, errors.New("serial Temporal calls outlive claimed lease")
	}
	return s.intents, nil
}
func TestDispatcherClaimsOnlyStartsThatFitLeaseBudget(t *testing.T) {
	state := &budgetStartStore{testStartStore{intents: []store.WorkflowStartIntent{validIntent(t)}}}
	dispatcher, err := NewDispatcher(state, &testTemporalClient{}, "dispatcher")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := dispatcher.RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("lease budget violated: %d %v", n, err)
	}
}

func (s *testStartStore) ClaimWorkflowStarts(context.Context, string, int, time.Duration) ([]store.WorkflowStartIntent, error) {
	return s.intents, nil
}
func (s *testStartStore) MarkWorkflowStarted(context.Context, string, string, string, int64) error {
	s.marked++
	return s.markErr
}

type testWorkflowRun struct {
	client.WorkflowRun
	id string
}

func (r testWorkflowRun) GetID() string { return r.id }

type testTemporalClient struct {
	options client.StartWorkflowOptions
	name    any
	args    []any
	err     error
	started int
}

func (c *testTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	c.started++
	c.options, c.name, c.args = options, workflow, args
	if c.err != nil {
		return nil, c.err
	}
	return testWorkflowRun{id: options.ID}, nil
}

func validIntent(t *testing.T) store.WorkflowStartIntent {
	t.Helper()
	request, err := json.Marshal(struct {
		JobID string `json:"jobId"`
	}{JobID: "gen_job1"})
	if err != nil {
		t.Fatal(err)
	}
	return store.WorkflowStartIntent{JobID: "gen_job1", WorkflowID: "render/gen_job1", Request: request, State: "pending", LeaseOwner: "dispatcher-a", LeaseEpoch: 1}
}

func TestDispatcherStartsDurableJobOnlyAndRejectsWorkflowReuse(t *testing.T) {
	state := &testStartStore{intents: []store.WorkflowStartIntent{validIntent(t)}}
	temporalClient := &testTemporalClient{}
	dispatcher, err := NewDispatcher(state, temporalClient, "dispatcher-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := dispatcher.RunOnce(context.Background())
	if err != nil || started != 1 {
		t.Fatalf("RunOnce() = %d, %v", started, err)
	}
	if temporalClient.options.ID != "render/gen_job1" || temporalClient.options.TaskQueue != TaskQueue || temporalClient.options.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE || !temporalClient.options.WorkflowExecutionErrorWhenAlreadyStarted {
		t.Fatalf("unsafe Temporal start options: %+v", temporalClient.options)
	}
	if temporalClient.name != WorkflowName || len(temporalClient.args) != 1 || temporalClient.args[0] != "gen_job1" {
		t.Fatalf("workflow received unexpected request: name=%v args=%v", temporalClient.name, temporalClient.args)
	}
	if state.marked != 1 {
		t.Fatalf("durable start acknowledgement count = %d", state.marked)
	}
}

func TestDispatcherTreatsExistingStableWorkflowAsOriginalStartAck(t *testing.T) {
	state := &testStartStore{intents: []store.WorkflowStartIntent{validIntent(t)}}
	temporalClient := &testTemporalClient{err: serviceerror.NewWorkflowExecutionAlreadyStarted("already exists", "req-1", "run-1")}
	dispatcher, err := NewDispatcher(state, temporalClient, "dispatcher-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := dispatcher.RunOnce(context.Background())
	if err != nil || started != 1 || state.marked != 1 {
		t.Fatalf("existing workflow was not acknowledged: started=%d marked=%d err=%v", started, state.marked, err)
	}
}

func TestDispatcherLeavesTransientStartFailurePending(t *testing.T) {
	state := &testStartStore{intents: []store.WorkflowStartIntent{validIntent(t)}}
	temporalClient := &testTemporalClient{err: errors.New("Temporal unavailable")}
	dispatcher, err := NewDispatcher(state, temporalClient, "dispatcher-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := dispatcher.RunOnce(context.Background())
	if err == nil || started != 0 || state.marked != 0 {
		t.Fatalf("transient start failure was acknowledged: started=%d marked=%d err=%v", started, state.marked, err)
	}
}

func TestDispatcherRejectsRequestBeyondJobID(t *testing.T) {
	intent := validIntent(t)
	intent.Request = []byte(`{"jobId":"gen_job1","parameters":{"secret":"private"}}`)
	state := &testStartStore{intents: []store.WorkflowStartIntent{intent}}
	temporalClient := &testTemporalClient{}
	dispatcher, err := NewDispatcher(state, temporalClient, "dispatcher-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := dispatcher.RunOnce(context.Background())
	if err == nil || started != 0 || temporalClient.started != 0 || state.marked != 0 {
		t.Fatalf("dispatcher trusted extra workflow data: started=%d temporal=%d marked=%d err=%v", started, temporalClient.started, state.marked, err)
	}
}

func TestRenderWorkflowCallsNamedExecuteActivityWithOnlyJobID(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	var got string
	environment.RegisterActivityWithOptions(func(_ context.Context, jobID string) error {
		got = jobID
		return nil
	}, activity.RegisterOptions{Name: ExecuteActivityName})
	environment.ExecuteWorkflow(RenderWorkflow, "gen_job1")
	if !environment.IsWorkflowCompleted() || environment.GetWorkflowError() != nil || got != "gen_job1" {
		t.Fatalf("named execution activity did not receive durable ID: completed=%v id=%q err=%v", environment.IsWorkflowCompleted(), got, environment.GetWorkflowError())
	}
}

func TestRenderWorkflowRetriesControlFailuresUntilDurableJobCanResume(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, activity.RegisterOptions{Name: ExecuteActivityName})
	environment.OnActivity(ExecuteActivityName, mock.Anything, "gen_job1").Return(errors.New("owner lease is still held")).Times(4)
	environment.OnActivity(ExecuteActivityName, mock.Anything, "gen_job1").Return(nil).Once()
	environment.ExecuteWorkflow(RenderWorkflow, "gen_job1")
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal("control retries exhausted before fenced render attempt could resume:", err)
	}
	environment.AssertExpectations(t)
}
