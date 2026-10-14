package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
)

const (
	janitorBatch = 100
	janitorLease = 30 * time.Second
)

type Janitor struct {
	state   *store.Store
	storage Storage
	engine  Engine
	cfg     Config
}

func NewJanitor(state *store.Store, storage Storage, engine Engine, cfg Config) (*Janitor, error) {
	if state == nil || storage == nil || engine == nil {
		return nil, errors.New("Store, Storage and Engine are required")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := ensurePrivateRoot(cfg.StagingRoot); err != nil {
		return nil, err
	}
	return &Janitor{state: state, storage: storage, engine: engine, cfg: cfg}, nil
}

// RunOnce fences stale render owners, confirms and removes their exact
// containers before releasing execution slots, and replays durable cleanup.
// Unknown Docker state is returned as an error while its reservation and stage
// remain untouched for a later pass.
func (j *Janitor) RunOnce(ctx context.Context) error {
	if ctx == nil {
		return errors.New("janitor context is required")
	}
	var failures []error
	attempts, err := j.state.ReapableAttempts(ctx, janitorBatch)
	if err != nil {
		return errors.New("list stale render attempts")
	}
	for _, candidate := range attempts {
		if err = ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		claimed, claimErr := j.state.ClaimStaleAttempt(ctx, candidate.AttemptID)
		if errors.Is(claimErr, store.ErrLeaseLost) {
			continue
		}
		if claimErr != nil {
			failures = append(failures, errors.New("claim stale render attempt"))
			continue
		}
		if err = j.recoverAttempt(ctx, claimed); err != nil {
			failures = append(failures, errors.New("recover stale render attempt"))
		}
	}
	if err = j.scheduleTerminalFiles(ctx); err != nil {
		failures = append(failures, errors.New("schedule terminal attempt cleanup"))
	}
	if err = j.runCleanup(ctx); err != nil {
		failures = append(failures, errors.New("process durable cleanup"))
	}
	return errors.Join(failures...)
}

func (j *Janitor) recoverAttempt(ctx context.Context, attempt store.Attempt) error {
	if err := checkStagePath(j.cfg.StagingRoot, attempt.StagingToken, attempt.PrivateStagingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	create, err := j.state.FindIntent(ctx, attempt.JobID, attempt.AttemptID, store.IntentContainerCreate, attempt.CreateContainerIntentKey)
	if errors.Is(err, store.ErrNotFound) {
		if attempt.ContainerID != "" {
			return errors.New("container ID exists without its durable create spec")
		}
		if err = j.state.AbandonUncreatedAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
			return err
		}
		attempt.StopConfirmed = true
		return j.reconcileStopped(ctx, attempt)
	}
	if err != nil {
		return err
	}
	spec, err := persistedCreateSpec(create, attempt)
	if err != nil {
		return err
	}
	containerID, err := j.recoverContainerIdentity(ctx, attempt, create, spec)
	if err != nil {
		return err
	}
	if err = j.stopAndRemove(ctx, containerID, spec, attempt); err != nil {
		return err
	}
	if err = j.state.MarkAttemptStopped(ctx, attempt.AttemptID, attempt.FencingEpoch, containerID); err != nil {
		return err
	}
	attempt.ContainerID = containerID
	attempt.StopConfirmed = true
	return j.reconcileStopped(ctx, attempt)
}

func persistedCreateSpec(intent store.OperationIntent, attempt store.Attempt) (sandbox.Spec, error) {
	var spec sandbox.Spec
	if len(intent.Request) == 0 || jsonvalue.Unmarshal(intent.Request, &spec, 16<<20) != nil {
		return spec, errors.New("persisted container spec is invalid")
	}
	if intent.JobID != attempt.JobID || intent.RenderAttemptID != attempt.AttemptID || intent.Kind != store.IntentContainerCreate || intent.IdempotencyKey != attempt.CreateContainerIntentKey || spec.JobID != attempt.JobID || spec.AttemptID != attempt.AttemptID || spec.CreateKey != attempt.CreateContainerIntentKey || !immutableImageID.MatchString(spec.Image) {
		return sandbox.Spec{}, errors.New("persisted container spec differs from the fenced attempt")
	}
	return spec, nil
}

func (j *Janitor) recoverContainerIdentity(ctx context.Context, attempt store.Attempt, intent store.OperationIntent, spec sandbox.Spec) (string, error) {
	if attempt.ContainerID != "" {
		if !exactContainerID.MatchString(attempt.ContainerID) {
			return "", errors.New("durable container identity is invalid")
		}
		return attempt.ContainerID, nil
	}
	if intent.CompletedAt != nil && len(intent.Output) > 0 {
		var receipt containerReceipt
		if jsonvalue.Unmarshal(intent.Output, &receipt, 16<<20) != nil || !exactContainerID.MatchString(receipt.ContainerID) {
			return "", errors.New("persisted container create receipt is invalid")
		}
		if err := j.recordRecoveredContainer(ctx, attempt, spec, receipt.ContainerID); err != nil {
			return "", err
		}
		return receipt.ContainerID, nil
	}

	name := sandbox.ContainerName(spec)
	state, err := j.engine.Inspect(ctx, name, spec)
	if err != nil && !isNotFound(err) {
		return "", err
	}
	if isNotFound(err) {
		// The durable create request may have reached Docker even when the old
		// controller saw a timeout. Repeating this deterministic create closes
		// that gap; the name prevents a second distinct container from winning.
		createdID, createErr := j.engine.Create(ctx, spec)
		if createErr != nil || !exactContainerID.MatchString(createdID) {
			state, err = j.engine.Inspect(ctx, name, spec)
			if err != nil {
				return "", errors.Join(createErr, err)
			}
		} else {
			state.ID = createdID
		}
	}
	if !exactContainerID.MatchString(state.ID) {
		return "", errors.New("Docker recovery returned an invalid container ID")
	}
	if err = j.recordRecoveredContainer(ctx, attempt, spec, state.ID); err != nil {
		return "", err
	}
	return state.ID, nil
}

func (j *Janitor) recordRecoveredContainer(ctx context.Context, attempt store.Attempt, spec sandbox.Spec, id string) error {
	state, err := j.engine.Inspect(ctx, id, spec)
	if err != nil {
		return err
	}
	if state.ID != id {
		return errors.New("Docker recovery returned a different exact container ID")
	}
	return j.state.RecordRecoveredContainer(ctx, attempt.AttemptID, attempt.FencingEpoch, spec.CreateKey, id)
}

func (j *Janitor) stopAndRemove(ctx context.Context, id string, spec sandbox.Spec, attempt store.Attempt) error {
	if !exactContainerID.MatchString(id) {
		return errors.New("exact container ID required for janitor cleanup")
	}
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), containerStopTimeout)
	defer cancel()
	state, err := j.engine.Inspect(stopCtx, id, spec)
	if err != nil && !isNotFound(err) {
		return err
	}
	if err == nil && state.ID != id {
		return errors.New("Docker inspection returned a different exact container ID")
	}
	if err == nil {
		if err = j.recordResourceLimit(ctx, attempt, state); err != nil {
			return err
		}
	}
	if err == nil && (state.Running || state.Restarting) {
		if killErr := j.engine.Kill(stopCtx, id); killErr != nil {
			state, err = j.engine.Inspect(stopCtx, id, spec)
			if err != nil || state.Running || state.Restarting || state.ID != id {
				return errors.Join(killErr, err, errors.New("container stop remains unknown"))
			}
			if err = j.recordResourceLimit(ctx, attempt, state); err != nil {
				return err
			}
		}
		stopTimer := time.NewTimer(10 * time.Second)
		defer stopTimer.Stop()
		for state.Running || state.Restarting {
			select {
			case <-stopCtx.Done():
				return stopCtx.Err()
			case <-stopTimer.C:
				return errors.New("container stop was not confirmed")
			case <-time.After(100 * time.Millisecond):
			}
			state, err = j.engine.Inspect(stopCtx, id, spec)
			if err != nil || state.ID != id {
				return errors.Join(err, errors.New("container stop remains unknown"))
			}
			if err = j.recordResourceLimit(ctx, attempt, state); err != nil {
				return err
			}
		}
	}
	if err == nil && !state.Stopped() && !state.Dead {
		return errors.New("container is not confirmed stopped")
	}
	if err == nil {
		if err = j.recordResourceLimit(ctx, attempt, state); err != nil {
			return err
		}
	}
	if err = j.engine.Remove(stopCtx, id); err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

func (j *Janitor) reconcileStopped(ctx context.Context, attempt store.Attempt) error {
	job, err := j.state.GetJob(ctx, attempt.JobID)
	if err != nil {
		return err
	}
	if job.Status.Terminal() {
		if err = j.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil && !errors.Is(err, store.ErrInvalidTransition) {
			return err
		}
		return nil
	}
	if job.Status == generationv1.Cancelling {
		return nil
	}
	if job.Status != generationv1.Preparing && job.Status != generationv1.Rendering {
		return nil
	}
	resourceLimit, err := findResourceLimit(ctx, j.state, attempt)
	if err != nil {
		return err
	}
	if resourceLimit {
		failed := store.TerminalInput{JobID: attempt.JobID, Phase: "resource_limit", FailureCode: "resource_limit", Diagnostics: []byte("[]")}
		if err = j.state.FailAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch, failed); err != nil {
			return err
		}
		if err = j.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
			return err
		}
		return j.state.ScheduleAttemptCleanup(ctx, attempt.AttemptID)
	}
	verified, terminalFailure, verifyErr := j.confirmedOutput(ctx, job, attempt)
	if verifyErr == nil && verified != nil {
		if terminalFailure {
			failure := store.TerminalInput{JobID: job.JobID, Phase: verified.Terminal.Phase, FailureCode: verified.Terminal.FailureCode, RawDiagnostics: verified.Terminal.RawDiagnostics}
			failure.Diagnostics, _ = json.Marshal(verified.Terminal.Diagnostics)
			if len(verified.Terminal.Diagnostics) == 0 {
				failure.Diagnostics = []byte("[]")
			}
			if err = j.state.FailAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch, failure); err != nil {
				return err
			}
			if err = j.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
				return err
			}
			return j.state.ScheduleAttemptCleanup(ctx, attempt.AttemptID)
		}
		return j.state.MarkAttemptRendered(ctx, attempt.AttemptID, attempt.FencingEpoch)
	}
	// An invalid or absent confirmation never becomes publishable. Retry it as
	// a technical failure only after exact stop and removal above.
	if err = j.state.RetryStoppedAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch); err == nil {
		if err = j.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
			return err
		}
		return nil
	}
	if !errors.Is(err, store.ErrInvalidTransition) {
		return err
	}
	failed := store.TerminalInput{JobID: attempt.JobID, Phase: "render", FailureCode: "renderer_failure", Diagnostics: []byte("[]")}
	if err = j.state.FailAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch, failed); err != nil {
		if errors.Is(err, store.ErrInvalidTransition) {
			return nil
		}
		return err
	}
	if err = j.state.AbandonAttemptIntents(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
		return err
	}
	return j.state.ScheduleAttemptCleanup(ctx, attempt.AttemptID)
}

func (j *Janitor) recordResourceLimit(ctx context.Context, attempt store.Attempt, state sandbox.State) error {
	if !state.OOMKilled && !(state.Stopped() && state.Status == "exited" && (state.ExitCode == rendererTimeoutExit || state.ExitCode == rendererLimitExit)) {
		return nil
	}
	return persistResourceLimit(ctx, j.state, attempt)
}

func (j *Janitor) confirmedOutput(ctx context.Context, job generationv1.Job, attempt store.Attempt) (*sandbox.Output, bool, error) {
	intent, err := j.state.FindIntent(ctx, job.JobID, attempt.AttemptID, "render.output", "output")
	if err != nil {
		return nil, false, err
	}
	if intent.CompletedAt == nil || intent.AbandonedAt != nil || len(intent.Output) == 0 {
		return nil, false, errors.New("confirmed renderer receipt is missing")
	}
	var record outputRecord
	if err = jsonvalue.Unmarshal(intent.Output, &record, 2<<20); err != nil {
		return nil, false, errors.New("confirmed renderer receipt is invalid")
	}
	if !privateToken.MatchString(attempt.StagingToken) || filepath.Clean(attempt.PrivateStagingPath) != filepath.Join(filepath.Clean(j.cfg.StagingRoot), attempt.StagingToken) || record.Spec.JobID != job.JobID || record.Spec.AttemptID != attempt.AttemptID || record.Spec.CreateKey != attempt.CreateContainerIntentKey || record.Spec.Image == "" || !immutableImageID.MatchString(record.Spec.Image) || !exactContainerID.MatchString(attempt.ContainerID) {
		return nil, false, errors.New("confirmed renderer receipt bindings differ")
	}
	input, err := j.state.GetExecutionInput(ctx, job.JobID)
	if err != nil {
		return nil, false, err
	}
	expected := renderRequest(job, attempt, input)
	if !sameRequest(record, expected) {
		return nil, false, errors.New("confirmed renderer request differs from the durable source")
	}
	if record.Terminal.Scope != expected.Scope || record.Terminal.SourceContentID != expected.SourceContentID || !record.Terminal.Complete || record.Terminal.JobID != job.JobID || record.Terminal.AttemptID != attempt.AttemptID || record.Terminal.TargetID != expected.TargetID || record.Terminal.InputFingerprint != expected.InputFingerprint || record.Terminal.SourceSnapshotID != expected.SourceSnapshotID {
		return nil, false, errors.New("renderer terminal source binding differs")
	}
	create, err := j.state.FindIntent(ctx, job.JobID, attempt.AttemptID, store.IntentContainerCreate, attempt.CreateContainerIntentKey)
	if err != nil {
		return nil, false, err
	}
	createSpec, err := persistedCreateSpec(create, attempt)
	if err != nil || record.Spec != createSpec {
		return nil, false, errors.New("renderer receipt differs from the persisted sandbox spec")
	}
	requestBytes, err := json.Marshal(expected)
	if err != nil || string(intent.Request) != string(requestBytes) {
		return nil, false, errors.New("renderer input intent differs from the durable request")
	}
	out, err := sandbox.ReopenOutput(ctx, attempt.PrivateStagingPath, record.Token, expected, record.Spec, attempt.ContainerID)
	if err != nil {
		return nil, false, err
	}
	if !record.Terminal.Success {
		return out, true, nil
	}
	return out, false, nil
}

func sameRequest(record outputRecord, expected renderwire.RenderRequest) bool {
	// The terminal record independently binds the source identity. The exact
	// request bytes are checked against the durable render.input intent below.
	return record.Terminal.Scope == expected.Scope && record.Terminal.JobID == expected.JobID && record.Terminal.AttemptID == expected.AttemptID && record.Terminal.TargetID == expected.TargetID && record.Terminal.InputFingerprint == expected.InputFingerprint && record.Terminal.SourceSnapshotID == expected.SourceSnapshotID && record.Terminal.SourceContentID == expected.SourceContentID
}

func (j *Janitor) scheduleTerminalFiles(ctx context.Context) error {
	attempts, err := j.state.TerminalAttempts(ctx, janitorBatch)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		if err = j.state.ScheduleAttemptCleanup(ctx, attempt.AttemptID); err != nil {
			return err
		}
	}
	return nil
}

func (j *Janitor) runCleanup(ctx context.Context) error {
	for {
		intents, err := j.state.ClaimCleanup(ctx, j.cfg.OwnerID, 1, janitorLease)
		if err != nil {
			return err
		}
		if len(intents) == 0 {
			return nil
		}
		intent := intents[0]
		var receipt []byte
		switch intent.Kind {
		case "pin.release":
			var request struct {
				PinID        string `json:"pinId"`
				ObjectID     string `json:"objectId"`
				HolderID     string `json:"holderId"`
				HolderDomain string `json:"holderDomain"`
			}
			prefix, pinID, ok := strings.Cut(intent.IdempotencyKey, "/")
			if !ok || prefix != "release" || pinID == "" || strings.Contains(pinID, "/") || jsonvalue.Unmarshal(intent.Request, &request, 1<<20) != nil || request.PinID != pinID || request.ObjectID == "" || request.HolderID == "" || request.HolderDomain == "" {
				return errors.New("pin release intent is invalid")
			}
			callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err = j.storage.ReleasePin(callCtx, pinID)
			cancel()
			if err != nil {
				return errors.New("Storage pin release is pending")
			}
			receipt = []byte(`{"released":true}`)
		case "attempt.files":
			var request struct {
				StagingToken string `json:"stagingToken"`
				Path         string `json:"path"`
			}
			if jsonvalue.Unmarshal(intent.Request, &request, 1<<20) != nil || request.StagingToken == "" || filepath.Clean(request.Path) != filepath.Join(filepath.Clean(j.cfg.StagingRoot), request.StagingToken) || intent.IdempotencyKey == "" {
				return errors.New("attempt cleanup intent is invalid")
			}
			attempt, getErr := j.state.GetAttempt(ctx, intent.JobID, intent.IdempotencyKey)
			if getErr != nil || !attempt.StopConfirmed || attempt.StagingToken != request.StagingToken || filepath.Clean(attempt.PrivateStagingPath) != filepath.Clean(request.Path) {
				return errors.New("attempt cleanup lacks a stop-confirmed matching attempt")
			}
			job, getErr := j.state.GetJob(ctx, intent.JobID)
			if getErr != nil || attempt.State != "retryable" && !job.Status.Terminal() {
				return errors.New("attempt cleanup is not bound to a retryable or terminal attempt")
			}
			current, getErr := j.state.CurrentAttempt(ctx, intent.JobID)
			if getErr != nil {
				return errors.New("cannot verify the current attempt before cleanup")
			}
			if current.AttemptID != attempt.AttemptID && (current.StagingToken == request.StagingToken || filepath.Clean(current.PrivateStagingPath) == filepath.Clean(request.Path)) {
				return errors.New("attempt cleanup path is shared with the current renderer attempt")
			}
			if err = removePrivateAttemptDir(j.cfg.StagingRoot, request.StagingToken, request.Path); err != nil {
				return errors.New("attempt staging cleanup is pending")
			}
			receipt, _ = json.Marshal(struct {
				Removed bool `json:"removed"`
			}{Removed: true})
		case "intent.abandoned":
			var request struct {
				IntentID int64  `json:"intentId"`
				Kind     string `json:"kind"`
			}
			if jsonvalue.Unmarshal(intent.Request, &request, 1<<20) != nil || request.IntentID <= 0 || request.Kind == "" {
				return errors.New("abandoned intent cleanup is invalid")
			}
			receipt = []byte(`{"abandoned":true}`)
		default:
			return fmt.Errorf("unsupported cleanup intent kind %q", intent.Kind)
		}
		if err = j.state.CompleteCleanup(ctx, intent.ID, j.cfg.OwnerID, intent.LeaseEpoch, receipt); err != nil {
			return errors.New("complete durable cleanup")
		}
	}
}
