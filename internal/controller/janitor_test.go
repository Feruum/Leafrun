package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/store"
)

func TestJanitorReopensConfirmedOutputAfterStopCommitGap(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job := queuedRuntimeJob(t, s)
	stageToken := "0123456789abcdef0123456789abcdef"
	stagePath := filepath.Join(root, stageToken)
	if err := os.Mkdir(stagePath, 0o700); err != nil {
		t.Fatal(err)
	}
	createKey := "create-janitor-test"
	attempt, err := s.ClaimExecution(context.Background(), store.ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: createKey, StagingToken: stageToken, PrivateStagingPath: stagePath})
	if err != nil {
		t.Fatal(err)
	}
	containerID := strings.Repeat("a", 64)
	image := runtimeImageID
	spec := sandbox.Spec{JobID: job.JobID, AttemptID: attempt.AttemptID, CreateKey: createKey, Image: image}
	createRequest, _ := json.Marshal(spec)
	createIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: store.IntentContainerCreate, IdempotencyKey: createKey, Request: createRequest}
	if _, err = s.SaveIntent(context.Background(), createIntent); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordContainer(context.Background(), attempt.AttemptID, attempt.FencingEpoch, containerID); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordSourceIdentity(context.Background(), job.JobID, runtimeSource); err != nil {
		t.Fatal(err)
	}
	job.SourceContentID = runtimeSource
	if err = s.MarkRendering(context.Background(), attempt.AttemptID, attempt.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	input, err := s.GetExecutionInput(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	request := renderRequest(job, attempt, input)
	requestBytes, _ := json.Marshal(request)
	inputIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: store.IntentRenderInput, IdempotencyKey: stableKey(attempt, "render-input"), Request: requestBytes, Sensitive: true}
	if _, err = s.SaveIntent(context.Background(), inputIntent); err != nil {
		t.Fatal(err)
	}
	outputIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: "render.output", IdempotencyKey: "output", Request: requestBytes, Sensitive: true}
	if _, err = s.SaveIntent(context.Background(), outputIntent); err != nil {
		t.Fatal(err)
	}
	output, err := sandbox.ReadOutput(context.Background(), bytes.NewReader(renderOutputFor(request)[8:]), stagePath, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = output.Close() })
	if err = output.Confirm(spec, sandbox.State{ID: containerID, Status: "exited", ExitCode: 0}); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(outputRecord{Token: output.Token(), Spec: spec, Terminal: output.Terminal})
	if _, err = s.CompleteIntent(context.Background(), outputIntent, receipt); err != nil {
		t.Fatal(err)
	}
	createReceipt, _ := json.Marshal(containerReceipt{ContainerID: containerID})
	if _, err = s.CompleteIntent(context.Background(), createIntent, createReceipt); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptStopped(context.Background(), attempt.AttemptID, attempt.FencingEpoch, containerID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, attempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	engine := newRuntimeEngine(job, sandbox.State{ID: containerID, Status: "exited", ExitCode: 0})
	engine.created = true
	engine.state = sandbox.State{ID: containerID, Status: "exited", ExitCode: 0}
	storage := newRuntimeStorage(job)
	janitor, err := NewJanitor(s, storage, engine, Config{StagingRoot: root, OwnerID: "janitor-owner"})
	if err != nil {
		t.Fatal(err)
	}
	if err = janitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := s.GetAttempt(context.Background(), job.JobID, attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != "publishing" || recovered.State != "finished" || !recovered.StopConfirmed || !engine.removed || storage.createUploadCalls != 0 {
		t.Fatalf("janitor did not recover only the confirmed output: status=%s attempt=%+v removed=%v uploads=%d", current.Status, recovered, engine.removed, storage.createUploadCalls)
	}
}

func TestJanitorClassifiesResourceLimitedContainerWithoutOutputProof(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job := queuedRuntimeJob(t, s)
	token := "abcdef0123456789abcdef0123456789"
	stage := filepath.Join(root, token)
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	key := "create-resource-janitor"
	attempt, err := s.ClaimExecution(context.Background(), store.ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: key, StagingToken: token, PrivateStagingPath: stage})
	if err != nil {
		t.Fatal(err)
	}
	containerID := strings.Repeat("a", 64)
	spec := sandbox.Spec{JobID: job.JobID, AttemptID: attempt.AttemptID, CreateKey: key, Image: runtimeImageID}
	request, _ := json.Marshal(spec)
	createIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: store.IntentContainerCreate, IdempotencyKey: key, Request: request}
	if _, err = s.SaveIntent(context.Background(), createIntent); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordContainer(context.Background(), attempt.AttemptID, attempt.FencingEpoch, containerID); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkRendering(context.Background(), attempt.AttemptID, attempt.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, attempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	engine := newRuntimeEngine(job, sandbox.State{ID: containerID, Status: "exited", ExitCode: rendererTimeoutExit})
	engine.created = true
	engine.state = sandbox.State{ID: containerID, Status: "exited", ExitCode: rendererTimeoutExit}
	janitor, err := NewJanitor(s, newRuntimeStorage(job), engine, Config{StagingRoot: root, OwnerID: "resource-janitor"})
	if err != nil {
		t.Fatal(err)
	}
	if err = janitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := s.GetAttempt(context.Background(), job.JobID, attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != "failed" || current.Phase != "resource_limit" || current.FailureCode != "resource_limit" || !recovered.StopConfirmed || !engine.removed {
		t.Fatalf("janitor retried an unproven timeout container: job=%s/%s attempt=%+v removed=%v", current.Status, current.FailureCode, recovered, engine.removed)
	}
	if _, err = s.FindIntent(context.Background(), job.JobID, attempt.AttemptID, "render.output", "output"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("test unexpectedly recorded an output proof: %v", err)
	}
}
