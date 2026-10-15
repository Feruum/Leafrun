package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/service"
	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/moby/moby/client"
	"go.temporal.io/sdk/activity"
)

const (
	controllerHeartbeatMax = 8 * time.Second
	controllerCallTimeout  = 5 * time.Second
	containerStopTimeout   = 20 * time.Second
	renderWatchdogTimeout  = 65 * time.Second
	rendererTimeoutExit    = 124
	rendererLimitExit      = 72
)

var immutableImageID = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var exactContainerID = regexp.MustCompile(`^[0-9a-f]{64}$`)
var privateToken = regexp.MustCompile(`^[a-f0-9]{32}$`)
var errAttemptRetryScheduled = errors.New("render attempt requeued")

// Engine is the narrow trusted Docker surface used by the controller and
// janitor. Inspect must verify the complete sandbox policy for the supplied
// immutable spec before returning an identity discovered by deterministic name.
type Engine interface {
	Ready(context.Context) error
	Create(context.Context, sandbox.Spec) (string, error)
	Inspect(context.Context, string, sandbox.Spec) (sandbox.State, error)
	Attach(context.Context, string) (client.ContainerAttachResult, error)
	Start(context.Context, string) error
	Kill(context.Context, string) error
	Remove(context.Context, string) error
}

type Controller struct {
	state     *store.Store
	storage   Storage
	engine    Engine
	access    service.AccessChecker
	cfg       Config
	publisher *Publisher
}

// New constructs the trusted execution controller. The staging root is shared
// only by this service and its janitor; renderer containers receive no mounts.
func New(state *store.Store, storage Storage, engine Engine, pins DerivedPinner, access service.AccessChecker, cfg Config) (*Controller, error) {
	if state == nil || storage == nil || engine == nil || pins == nil || access == nil {
		return nil, errors.New("Store, Storage, Engine, pin coordinator and Access are required")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if !immutableImageID.MatchString(cfg.RendererImage) {
		return nil, errors.New("renderer image must be an immutable local sha256 image ID")
	}
	if err := ensurePrivateRoot(cfg.StagingRoot); err != nil {
		return nil, err
	}
	publisher, err := NewPublisher(state, storage, pins, access, cfg)
	if err != nil {
		return nil, err
	}
	return &Controller{state: state, storage: storage, engine: engine, access: access, cfg: cfg, publisher: publisher}, nil
}

// Execute is the activity entry point. It reads the durable request and checks
// the initiating actor against current Access before claiming quota. A Docker
// readiness probe may precede the claim; container creation and source-content
// IO happen only after quota is reserved. Active attempts are left to their
// lease owner/janitor.
func (c *Controller) Execute(ctx context.Context, jobID string) error {
	if ctx == nil || jobID == "" {
		return errors.New("execution context and job ID are required")
	}
	job, err := c.state.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job.Status.Terminal() {
		return nil
	}
	if job.Status == generationv1.Publishing {
		return c.publisher.Publish(ctx, jobID)
	}
	if job.Status != generationv1.Queued {
		return store.ErrLeaseLost
	}
	input, err := c.state.GetExecutionInput(ctx, jobID)
	if err != nil {
		return err
	}
	principal, err := service.DecodeInitiator(input.InitiatorID)
	if err != nil {
		return err
	}
	allowed, err := c.access.Check(ctx, storagev1.AccessCheck{Principal: principal, Scope: job.Scope, Operation: "generation.execute", ObjectID: job.JobID})
	if err != nil {
		return err
	}
	if !allowed {
		return c.state.FailJob(ctx, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
	}
	var openInputFiles inputFileOpenerFactory
	if input.Selection != nil {
		allowed, err = c.access.Check(ctx, storagev1.AccessCheck{Principal: input.Execution.Actor, Scope: job.Scope, Operation: "generation.create"})
		if err != nil {
			return err
		}
		if !allowed {
			return c.state.FailJob(ctx, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
		}
		if input.Selection.ReleaseID == "" {
			allowed, err = c.access.Check(ctx, storagev1.AccessCheck{Principal: input.Execution.Actor, Scope: job.Scope, Operation: "source.read", ObjectID: job.SourceSnapshotID})
			if err != nil {
				return err
			}
			if !allowed {
				return c.state.FailJob(ctx, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
			}
		}
		openInputFiles, err = c.inputFileOpener(ctx, job, input)
		if errors.Is(err, errInputResourceAccessDenied) {
			return c.state.FailJob(ctx, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
		}
		if err != nil {
			return err
		}
	}
	if err = c.engine.Ready(ctx); err != nil {
		return err
	}
	token, createKey, err := newAttemptIdentity()
	if err != nil {
		return err
	}
	stagePath := filepath.Join(c.cfg.StagingRoot, token)
	attempt, err := c.state.ClaimExecution(ctx, store.ClaimAttemptInput{JobID: jobID, CreateContainerIntentKey: createKey, StagingToken: token, PrivateStagingPath: stagePath})
	if err != nil {
		return err
	}
	if err = os.Mkdir(stagePath, 0o700); err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "staging_unavailable", "preparing", err, true)
	}
	if err = checkStagePath(c.cfg.StagingRoot, attempt.StagingToken, attempt.PrivateStagingPath); err != nil {
		return err
	}

	runCtx, cancelRun := boundedExecutionContext(ctx, job.Deadline)
	defer cancelRun()
	stopHeartbeat := c.heartbeatAttempt(runCtx, cancelRun, attempt)
	defer stopHeartbeat()
	if err = c.executeAttempt(runCtx, cancelRun, job, input, attempt, openInputFiles); err != nil {
		stopHeartbeat()
		cancelRun()
		return err
	}
	// The render lease ends before publication begins. Publication uses the
	// activity context and its own lease/heartbeat, without inheriting the
	// renderer watchdog or a five-second DB transition timeout.
	stopHeartbeat()
	cancelRun()
	current, err := c.state.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if current.Status == generationv1.Publishing {
		return c.publisher.Publish(ctx, jobID)
	}
	return nil
}

func (c *Controller) executeAttempt(ctx context.Context, cancel context.CancelFunc, job generationv1.Job, input store.ExecutionInput, attempt store.Attempt, openInputFileFactory inputFileOpenerFactory) error {
	var openInputFiles func(projectsv1.PreparedFile) (io.ReadCloser, error)
	if openInputFileFactory != nil {
		openInputFiles = openInputFileFactory(ctx)
	}
	stagePath := attempt.PrivateStagingPath
	snapshot, err := c.storage.GetSnapshot(ctx, job.SourceSnapshotID)
	if err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "source_unavailable", "preparing", err, false)
	}
	remote, bundle, err := newRemoteSnapshotBundle(ctx, c.storage, snapshot, generationJobIdentity{SnapshotID: job.SourceSnapshotID, ContentID: job.SourceContentID, Scope: job.Scope})
	if err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "source_invalid", "preparing", err, true)
	}
	if err = c.state.RecordSourceIdentity(ctx, job.JobID, snapshot.Manifest.ID); err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "source_unavailable", "preparing", err, false)
	}
	job.SourceContentID = snapshot.Manifest.ID

	request := renderRequest(job, attempt, input)
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "input_invalid", "preparing", err, true)
	}
	inputIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: store.IntentRenderInput, IdempotencyKey: stableKey(attempt, "render-input"), Request: requestBytes, Sensitive: true}
	if _, err = c.state.SaveIntent(ctx, inputIntent); err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "intent_unavailable", "preparing", err, false)
	}
	outputIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: "render.output", IdempotencyKey: "output", Request: requestBytes, Sensitive: true}
	if _, err = c.state.SaveIntent(ctx, outputIntent); err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "intent_unavailable", "preparing", err, false)
	}
	if err = c.state.MarkRendering(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
		return c.finishWithoutContainer(ctx, job, attempt, "execution_unavailable", "preparing", err, false)
	}

	spec := sandbox.Spec{JobID: job.JobID, AttemptID: attempt.AttemptID, CreateKey: attempt.CreateContainerIntentKey, Image: c.cfg.RendererImage}
	containerID, err := c.ensureContainer(ctx, attempt, spec)
	if err != nil {
		// A create intent makes absence ambiguous. The exact slot and private
		// stage remain reserved until the janitor fences and resolves the ID.
		return err
	}
	attach, err := c.engine.Attach(ctx, containerID)
	if err != nil {
		return errors.Join(err, c.stopAndFinalize(ctx, job, attempt, spec, containerID, falseWithCause(err)))
	}
	attachClosed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = attach.CloseWrite()
			attach.Close()
		case <-attachClosed:
		}
	}()
	defer func() {
		close(attachClosed)
		attach.Close()
	}()

	startRequest, err := json.Marshal(struct {
		ContainerID string `json:"containerId"`
	}{ContainerID: containerID})
	if err != nil {
		return errors.Join(err, c.stopAndFinalize(ctx, job, attempt, spec, containerID, falseWithCause(err)))
	}
	startIntent := store.IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: store.IntentContainerStart, IdempotencyKey: stableKey(attempt, "container-start"), Request: startRequest}
	if _, err = c.state.SaveIntent(ctx, startIntent); err != nil {
		return errors.Join(err, c.stopAndFinalize(ctx, job, attempt, spec, containerID, falseWithCause(err)))
	}

	startErr := c.engine.Start(ctx, containerID)
	if startErr != nil {
		state, inspectErr := c.engine.Inspect(ctx, containerID, spec)
		if inspectErr != nil || !state.Running && !state.Stopped() {
			return errors.Join(startErr, inspectErr, c.stopAndFinalize(ctx, job, attempt, spec, containerID, failureCause{err: errors.Join(startErr, inspectErr), retryable: true}))
		}
		// A successful exit after a failed Start acknowledgement is not the
		// renderer result for this invocation: the stdin stream was never sent.
		if !state.Running {
			return errors.Join(startErr, c.stopAndFinalize(ctx, job, attempt, spec, containerID, failureCause{err: startErr, retryable: true}))
		}
	}

	inputErr := make(chan error, 1)
	go func() {
		writeErr := sandbox.WriteInput(attach.Conn, request, bundle, openInputFiles)
		if writeErr == nil {
			writeErr = remote.Verify()
		}
		if closeErr := attach.CloseWrite(); writeErr == nil {
			writeErr = closeErr
		}
		inputErr <- writeErr
	}()

	outputCh := make(chan outputResult, 1)
	go func() {
		out, readErr := sandbox.ReadOutput(ctx, sandbox.NewDockerStdout(attach.Reader), stagePath, request)
		outputCh <- outputResult{output: out, err: readErr}
	}()
	var out *sandbox.Output
	var streamErr error
	var inputDone bool
	for out == nil && streamErr == nil {
		select {
		case result := <-outputCh:
			out, streamErr = result.output, result.err
		case inputFailure := <-inputErr:
			inputDone = true
			if inputFailure != nil {
				streamErr = inputFailure
				cancel()
			}
		case <-ctx.Done():
			streamErr = ctx.Err()
			cancel()
		}
	}
	if out == nil {
		// Ensure blocked IO has been interrupted before stopping/removing the
		// exact container. The stage remains private until stop is confirmed.
		attach.Close()
		if !inputDone {
			select {
			case <-inputErr:
			case <-time.After(time.Second):
			}
		}
		cause := falseWithCause(streamErr)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cause.retryable, cause.resourceLimit = false, true
		}
		return c.stopAndFinalize(ctx, job, attempt, spec, containerID, cause)
	}
	if !inputDone {
		select {
		case writeErr := <-inputErr:
			inputDone = true
			if writeErr != nil {
				streamErr = writeErr
			}
		case <-time.After(2 * time.Second):
			streamErr = errors.New("renderer input stream did not complete")
		}
	}
	if streamErr != nil {
		return c.stopAndFinalize(ctx, job, attempt, spec, containerID, falseWithCause(streamErr))
	}

	state, inspectErr := c.engine.Inspect(ctx, containerID, spec)
	if inspectErr != nil {
		// A full terminal and EOF are insufficient without an exact-ID inspect.
		cause := falseWithCause(inspectErr)
		if errors.Is(inspectErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cause = resourceLimitCause(inspectErr)
		}
		return errors.Join(inspectErr, c.stopAndFinalize(ctx, job, attempt, spec, containerID, cause))
	}
	if state.ID != containerID || !state.Successful() {
		cause := classifyRendererState(falseWithCause(errors.New("renderer did not exit successfully")), state)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cause = resourceLimitCause(cause.err)
		}
		return c.stopAndFinalize(ctx, job, attempt, spec, containerID, cause)
	}
	if err = out.Confirm(spec, state); err != nil {
		return c.stopAndFinalize(ctx, job, attempt, spec, containerID, falseWithCause(err))
	}
	// Renderer-declared validation, compilation, and resource failures are
	// confirmed too; the terminal proof has no PDF and can only drive failure.
	record, _ := json.Marshal(outputRecord{Token: out.Token(), Spec: spec, Terminal: out.Terminal})
	if _, err = c.state.CompleteIntent(ctx, outputIntent, record); err != nil {
		return c.stopAndFinalize(ctx, job, attempt, spec, containerID, falseWithCause(err))
	}

	if err = c.stopAndRemove(ctx, spec, containerID); err != nil {
		// Keep the durable output intent and stage for the janitor. Publication
		// remains impossible until the stop fact is committed.
		return err
	}
	transitionCtx, cancelTransition := context.WithTimeout(context.WithoutCancel(ctx), controllerCallTimeout)
	defer cancelTransition()
	if err = c.state.MarkAttemptStopped(transitionCtx, attempt.AttemptID, attempt.FencingEpoch, containerID); err != nil {
		return err
	}
	if !out.Terminal.Success {
		failure := store.TerminalInput{JobID: job.JobID, Phase: out.Terminal.Phase, FailureCode: out.Terminal.FailureCode, RawDiagnostics: out.Terminal.RawDiagnostics}
		failure.Diagnostics, _ = json.Marshal(out.Terminal.Diagnostics)
		if len(out.Terminal.Diagnostics) == 0 {
			failure.Diagnostics = []byte("[]")
		}
		if err = c.state.FailAttempt(transitionCtx, attempt.AttemptID, attempt.FencingEpoch, failure); err != nil {
			return err
		}
		if err = c.state.AbandonAttemptIntents(transitionCtx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
			return err
		}
		return c.state.ScheduleAttemptCleanup(transitionCtx, attempt.AttemptID)
	}
	if err = c.state.MarkAttemptRendered(transitionCtx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
		return err
	}
	return nil
}

type outputResult struct {
	output *sandbox.Output
	err    error
}

type failureCause struct {
	err           error
	retryable     bool
	resourceLimit bool
	phase         string
	code          string
}

func falseWithCause(err error) failureCause {
	return failureCause{err: err, retryable: true, phase: "render", code: "renderer_failure"}
}

type rendererFailureRecord struct {
	Phase       string `json:"phase"`
	FailureCode string `json:"failureCode"`
}

func resourceLimitCause(err error) failureCause {
	return failureCause{err: err, phase: "resource_limit", code: "resource_limit", resourceLimit: true}
}

func classifyRendererState(cause failureCause, state sandbox.State) failureCause {
	if state.OOMKilled || state.Stopped() && state.Status == "exited" && (state.ExitCode == rendererTimeoutExit || state.ExitCode == rendererLimitExit) {
		return resourceLimitCause(cause.err)
	}
	return cause
}

// Persist only fixed, non-sensitive classification fields. The key stays
// stable across owner epochs, while the database retains each fenced request
// so a janitor can recover a failure after the container is removed.
func persistResourceLimit(ctx context.Context, state *store.Store, attempt store.Attempt) error {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controllerCallTimeout)
	defer cancel()
	err := state.RecordAttemptFailureClassification(writeCtx, attempt.AttemptID, attempt.FencingEpoch, "resource_limit", "resource_limit")
	if errors.Is(err, store.ErrInvalidTransition) {
		// Cancellation or another terminal transition wins. The exact container
		// still needs stop confirmation, but this renderer must not replace the
		// job's established outcome.
		return nil
	}
	return err
}

func findResourceLimit(ctx context.Context, state *store.Store, attempt store.Attempt) (bool, error) {
	intent, err := state.FindIntent(ctx, attempt.JobID, attempt.AttemptID, "render.failure", "failure")
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if intent.AbandonedAt != nil || intent.CompletedAt == nil || string(intent.Output) != `{"recorded":true}` || intent.Sensitive || intent.JobID != attempt.JobID || intent.RenderAttemptID != attempt.AttemptID || intent.AttemptEpoch <= 0 || intent.Kind != "render.failure" || intent.IdempotencyKey != "failure" {
		return false, errors.New("renderer failure classification is not bound to this attempt")
	}
	var record rendererFailureRecord
	if jsonvalue.Unmarshal(intent.Request, &record, 4096) != nil || record.Phase != "resource_limit" || record.FailureCode != "resource_limit" {
		return false, errors.New("renderer failure classification is invalid")
	}
	return true, nil
}

func (c *Controller) ensureContainer(ctx context.Context, attempt store.Attempt, spec sandbox.Spec) (string, error) {
	request, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	intent := store.IntentInput{JobID: attempt.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: store.IntentContainerCreate, IdempotencyKey: attempt.CreateContainerIntentKey, Request: request}
	previous, findErr := c.state.FindIntent(ctx, attempt.JobID, attempt.AttemptID, store.IntentContainerCreate, attempt.CreateContainerIntentKey)
	if findErr != nil && !errors.Is(findErr, store.ErrNotFound) {
		return "", findErr
	}
	if findErr == nil {
		request = previous.Request
		if err = jsonvalue.Unmarshal(request, &spec, 16<<20); err != nil {
			return "", errors.New("persisted container create spec is invalid")
		}
	}
	if spec.JobID != attempt.JobID || spec.AttemptID != attempt.AttemptID || spec.CreateKey != attempt.CreateContainerIntentKey || !immutableImageID.MatchString(spec.Image) {
		return "", errors.New("container create spec differs from the durable attempt")
	}
	intent.Request = request
	saved, err := c.state.SaveIntent(ctx, intent)
	if err != nil {
		return "", err
	}
	if attempt.ContainerID != "" {
		return c.verifyContainerIdentity(ctx, attempt.ContainerID, spec)
	}
	if saved.CompletedAt != nil && len(saved.Output) > 0 {
		var receipt containerReceipt
		if jsonvalue.Unmarshal(saved.Output, &receipt, 16<<20) != nil || !exactContainerID.MatchString(receipt.ContainerID) {
			return "", errors.New("persisted container create receipt is invalid")
		}
		if err = c.state.RecordContainer(ctx, attempt.AttemptID, attempt.FencingEpoch, receipt.ContainerID); err != nil {
			return "", err
		}
		return c.verifyContainerIdentity(ctx, receipt.ContainerID, spec)
	}
	id, createErr := c.engine.Create(ctx, spec)
	if createErr != nil || !exactContainerID.MatchString(id) {
		state, inspectErr := c.engine.Inspect(ctx, sandbox.ContainerName(spec), spec)
		if inspectErr != nil {
			return "", errors.Join(createErr, inspectErr)
		}
		id = state.ID
		if !exactContainerID.MatchString(id) {
			return "", errors.New("Docker inspection returned an invalid container ID")
		}
		if err = c.state.RecordRecoveredContainer(ctx, attempt.AttemptID, attempt.FencingEpoch, spec.CreateKey, id); err != nil {
			return "", err
		}
	} else if err = c.state.RecordContainer(ctx, attempt.AttemptID, attempt.FencingEpoch, id); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(containerReceipt{ContainerID: id})
	if err != nil {
		return "", err
	}
	if _, err = c.state.CompleteIntent(ctx, intent, encoded); err != nil {
		return "", err
	}
	return c.verifyContainerIdentity(ctx, id, spec)
}

func (c *Controller) verifyContainerIdentity(ctx context.Context, id string, spec sandbox.Spec) (string, error) {
	if !exactContainerID.MatchString(id) {
		return "", errors.New("container identity is not an exact Docker ID")
	}
	state, err := c.engine.Inspect(ctx, id, spec)
	if err != nil {
		return "", err
	}
	if state.ID != id {
		return "", errors.New("Docker inspection returned a different container identity")
	}
	return id, nil
}

type containerReceipt struct {
	ContainerID string `json:"containerId"`
}

func (c *Controller) stopAndFinalize(ctx context.Context, job generationv1.Job, attempt store.Attempt, spec sandbox.Spec, containerID string, cause failureCause) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(cause.err, context.DeadlineExceeded) {
		cause = resourceLimitCause(cause.err)
	}
	if cause.resourceLimit {
		if err := persistResourceLimit(ctx, c.state, attempt); err != nil {
			return errors.Join(cause.err, err)
		}
	}
	observe := func(state sandbox.State) error {
		cause = classifyRendererState(cause, state)
		if cause.resourceLimit {
			return persistResourceLimit(ctx, c.state, attempt)
		}
		return nil
	}
	if err := c.stopAndRemoveObserved(ctx, spec, containerID, observe); err != nil {
		return errors.Join(cause.err, err)
	}
	transitionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controllerCallTimeout)
	defer cancel()
	if err := c.state.MarkAttemptStopped(transitionCtx, attempt.AttemptID, attempt.FencingEpoch, containerID); err != nil {
		return errors.Join(cause.err, err)
	}
	return c.finishStoppedFailure(transitionCtx, job, attempt, cause)
}

func (c *Controller) finishWithoutContainer(ctx context.Context, job generationv1.Job, attempt store.Attempt, code, phase string, cause error, permanent bool) error {
	transitionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controllerCallTimeout)
	defer cancel()
	if err := c.state.AbandonUncreatedAttempt(transitionCtx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
		return errors.Join(cause, err)
	}
	return c.finishStoppedFailure(transitionCtx, job, attempt, failureCause{err: cause, retryable: !permanent, resourceLimit: code == "resource_limit", phase: phase, code: code})
}

func (c *Controller) finishStoppedFailure(ctx context.Context, job generationv1.Job, attempt store.Attempt, cause failureCause) error {
	current, err := c.state.GetJob(ctx, job.JobID)
	if err != nil {
		return err
	}
	if current.Status.Terminal() || current.Status == generationv1.Cancelling {
		return nil
	}
	if cause.retryable && !cause.resourceLimit {
		if err = c.state.RetryStoppedAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch); err == nil {
			if err = c.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
				return err
			}
			return errAttemptRetryScheduled
		}
		if !errors.Is(err, store.ErrInvalidTransition) {
			return errors.Join(cause.err, err)
		}
	}
	phase, code := cause.phase, cause.code
	if phase == "" {
		phase = "render"
	}
	if code == "" {
		code = "renderer_failure"
	}
	if cause.resourceLimit {
		phase, code = "resource_limit", "resource_limit"
	}
	diagnostics, _ := json.Marshal([]generationv1.Diagnostic{})
	terminal := store.TerminalInput{JobID: job.JobID, Phase: phase, FailureCode: code, Diagnostics: diagnostics}
	if err = c.state.FailAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch, terminal); err != nil {
		if errors.Is(err, store.ErrInvalidTransition) {
			return nil
		}
		return errors.Join(cause.err, err)
	}
	if err = c.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
		return err
	}
	return c.state.ScheduleAttemptCleanup(ctx, attempt.AttemptID)
}

func (c *Controller) stopAndRemove(ctx context.Context, spec sandbox.Spec, containerID string) error {
	return c.stopAndRemoveObserved(ctx, spec, containerID, nil)
}

func (c *Controller) stopAndRemoveObserved(ctx context.Context, spec sandbox.Spec, containerID string, observe func(sandbox.State) error) error {
	if !exactContainerID.MatchString(containerID) {
		return errors.New("cannot stop a container without its exact durable ID")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), containerStopTimeout)
	defer cancel()
	state, err := c.engine.Inspect(stopCtx, containerID, spec)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("inspect exact renderer container: %w", err)
	}
	if err == nil && state.ID != containerID {
		return errors.New("Docker inspection returned a different container identity")
	}
	if err == nil && observe != nil {
		if err = observe(state); err != nil {
			return err
		}
	}
	if err == nil && (state.Running || state.Restarting) {
		if killErr := c.engine.Kill(stopCtx, containerID); killErr != nil {
			state, err = c.engine.Inspect(stopCtx, containerID, spec)
			if err != nil || state.ID != containerID || state.Running || state.Restarting {
				return errors.Join(killErr, err, errors.New("renderer container stop is unconfirmed"))
			}
			if observe != nil {
				if err = observe(state); err != nil {
					return err
				}
			}
		}
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		for state.Running || state.Restarting {
			select {
			case <-stopCtx.Done():
				return stopCtx.Err()
			case <-deadline.C:
				return errors.New("renderer container stop was not confirmed")
			case <-time.After(100 * time.Millisecond):
			}
			state, err = c.engine.Inspect(stopCtx, containerID, spec)
			if err != nil {
				return err
			}
			if state.ID != containerID {
				return errors.New("Docker inspection returned a different container identity")
			}
			if observe != nil {
				if err = observe(state); err != nil {
					return err
				}
			}
		}
	}
	if err == nil && !state.Stopped() && !state.Dead {
		return errors.New("renderer container is not confirmed stopped")
	}
	if err == nil && observe != nil {
		if err = observe(state); err != nil {
			return err
		}
	}
	removeErr := c.engine.Remove(stopCtx, containerID)
	if removeErr != nil && !isNotFound(removeErr) {
		return fmt.Errorf("remove exact renderer container: %w", removeErr)
	}
	return nil
}

func (c *Controller) heartbeatAttempt(ctx context.Context, cancel context.CancelFunc, attempt store.Attempt) func() {
	interval := controllerHeartbeatMax
	if c.cfg.Heartbeat > 0 && c.cfg.Heartbeat < interval {
		interval = c.cfg.Heartbeat
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				call, stop := context.WithTimeout(ctx, controllerCallTimeout)
				_, err := c.state.HeartbeatAttempt(call, attempt.AttemptID, attempt.FencingEpoch)
				stop()
				if err != nil {
					slog.Error("Generation attempt heartbeat failed")
					cancel()
					return
				}
				safeRecordActivityHeartbeat(ctx, attempt.AttemptID)
			}
		}
	}()
	return func() { stopOnce.Do(func() { close(done); <-stopped }) }
}

func safeRecordActivityHeartbeat(ctx context.Context, attemptID string) {
	defer func() { _ = recover() }()
	activity.RecordHeartbeat(ctx, struct {
		AttemptID string `json:"attemptId"`
	}{AttemptID: attemptID})
}

func boundedExecutionContext(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	var ctx context.Context = parent
	var cancel context.CancelFunc = func() {}
	if !deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}
	limited, limitCancel := context.WithTimeout(ctx, renderWatchdogTimeout)
	return limited, func() { limitCancel(); cancel() }
}

func newAttemptIdentity() (token, createKey string, err error) {
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return "", "", err
	}
	token = hex.EncodeToString(random[:])
	return token, "create-" + token, nil
}

func ensurePrivateRoot(root string) error {
	if !filepath.IsAbs(root) {
		return errors.New("staging root must be absolute")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create private staging root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("staging root must be an ordinary directory")
	}
	if err = os.Chmod(root, 0o700); err != nil {
		return fmt.Errorf("secure staging root: %w", err)
	}
	return nil
}

func checkStagePath(root, token, stagePath string) error {
	if !privateToken.MatchString(token) || filepath.Clean(stagePath) != filepath.Join(filepath.Clean(root), token) {
		return errors.New("private staging path differs from its durable opaque token")
	}
	info, err := os.Lstat(stagePath)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("attempt staging path is not an ordinary directory")
	}
	return nil
}

func removePrivateAttemptDir(root, token, stagePath string) error {
	if !privateToken.MatchString(token) || filepath.Clean(stagePath) != filepath.Join(filepath.Clean(root), token) {
		return errors.New("refuse to remove an unbound attempt staging path")
	}
	rootInfo, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("private staging root is unavailable")
	}
	info, err := os.Lstat(stagePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refuse to remove an unsafe attempt staging path")
	}
	return os.RemoveAll(stagePath)
}

func isNotFound(err error) bool {
	var marker interface{ NotFound() }
	return errors.As(err, &marker)
}
