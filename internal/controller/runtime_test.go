package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/service"
	"github.com/Feruum/Leafrun/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/moby/moby/client"
)

const (
	runtimeImageID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

var runtimeSource = func() string {
	content := []byte("# test project")
	hash := sha256.Sum256(content)
	manifest := storagev1.SnapshotManifest{FormatVersion: 1, Files: []storagev1.SnapshotFile{{Path: "main.typ", Size: int64(len(content)), SHA256: hex.EncodeToString(hash[:])}}, Directories: []string{}, Fonts: []storagev1.SnapshotFont{}}
	id, err := storagev1.ManifestContentID(manifest)
	if err != nil {
		panic(err)
	}
	return id
}()

type runtimeStorage struct {
	*storageFixture
	snapshot           storagev1.Snapshot
	resources          map[string][]byte
	createUploadCalls  int
	beforeCreateUpload func(context.Context) error
}

func newRuntimeStorage(job generationv1.Job) *runtimeStorage {
	content := []byte("# test project")
	hash := sha256.Sum256(content)
	manifest := storagev1.SnapshotManifest{
		FormatVersion: 1,
		ID:            runtimeSource,
		Files:         []storagev1.SnapshotFile{{Path: job.Entrypoint, Size: int64(len(content)), SHA256: hex.EncodeToString(hash[:])}},
		Directories:   []string{},
		Fonts:         []storagev1.SnapshotFont{},
	}
	return &runtimeStorage{
		storageFixture: newStorageFixture(),
		snapshot:       storagev1.Snapshot{SnapshotID: job.SourceSnapshotID, Scope: job.Scope, Manifest: manifest, Resources: []storagev1.ResourceRef{{Tree: "project", Path: job.Entrypoint, ResourceID: "resource-main"}}},
		resources:      map[string][]byte{"resource-main": content},
	}
}

func (s *runtimeStorage) GetSnapshot(_ context.Context, id string) (storagev1.Snapshot, error) {
	if id != s.snapshot.SnapshotID {
		return storagev1.Snapshot{}, errors.New("unexpected snapshot ID")
	}
	return s.snapshot, nil
}

func (s *runtimeStorage) ResourceContent(_ context.Context, id string) (*http.Response, error) {
	content, ok := s.resources[id]
	if !ok {
		return nil, errors.New("unknown resource")
	}
	return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(content)), Body: io.NopCloser(bytes.NewReader(content))}, nil
}

func (s *runtimeStorage) CreateUpload(ctx context.Context, in storagev1.CreateUploadRequest) (storagev1.Upload, error) {
	s.createUploadCalls++
	if s.beforeCreateUpload != nil {
		if err := s.beforeCreateUpload(ctx); err != nil {
			return storagev1.Upload{}, err
		}
	}
	return s.storageFixture.CreateUpload(ctx, in)
}

type runtimeNotFound struct{}

func (runtimeNotFound) Error() string { return "not found" }
func (runtimeNotFound) NotFound()     {}

type runtimeEngine struct {
	mu                        sync.Mutex
	id                        string
	state                     sandbox.State
	created                   bool
	createCalls               int
	nameInspections           int
	inspectCalls              int
	loseCreateAck             bool
	createUnknown             bool
	nameInspectErr            error
	attachErr                 error
	blockOutput               bool
	rawOutput                 []byte
	failurePhase              string
	inspectFailure            error
	inspectFailuresAfterStart int
	inspectFailuresRemaining  int
	startGate                 <-chan struct{}
	started                   chan struct{}
	startOnce                 sync.Once
	request                   renderwire.RenderRequest
	job                       generationv1.Job
	stateAfterStart           sandbox.State
	killCalls                 int
	removeCalls               int
	removed                   bool
}

func newRuntimeEngine(job generationv1.Job, outputState sandbox.State) *runtimeEngine {
	return &runtimeEngine{id: strings.Repeat("a", 64), state: sandbox.State{ID: strings.Repeat("a", 64), Status: "created"}, stateAfterStart: outputState, job: job}
}

func (e *runtimeEngine) Ready(context.Context) error { return nil }

func (e *runtimeEngine) Create(_ context.Context, spec sandbox.Spec) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.createCalls++
	e.request = renderwire.RenderRequest{Scope: e.job.Scope, JobID: e.job.JobID, AttemptID: spec.AttemptID, TargetID: e.job.TargetID, Entrypoint: e.job.Entrypoint, SourceSnapshotID: e.job.SourceSnapshotID, SourceContentID: runtimeSource, InputFingerprint: e.job.InputFingerprint}
	if !e.createUnknown || e.loseCreateAck {
		e.created = true
		e.state = sandbox.State{ID: e.id, Status: "created"}
	}
	if e.loseCreateAck {
		return "", errors.New("create acknowledgement lost")
	}
	if e.createUnknown {
		return "", errors.New("Docker create response timed out")
	}
	if spec.Image != runtimeImageID {
		return "", errors.New("unexpected renderer image")
	}
	return e.id, nil
}

func (e *runtimeEngine) Inspect(_ context.Context, selector string, spec sandbox.Spec) (sandbox.State, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.inspectCalls++
	if selector == sandbox.ContainerName(spec) {
		e.nameInspections++
		if e.nameInspectErr != nil {
			return sandbox.State{}, e.nameInspectErr
		}
		if !e.created {
			return sandbox.State{}, runtimeNotFound{}
		}
		return e.state, nil
	}
	if selector != e.id || !e.created {
		return sandbox.State{}, runtimeNotFound{}
	}
	if e.inspectFailuresRemaining > 0 {
		e.inspectFailuresRemaining--
		if e.inspectFailure != nil {
			return sandbox.State{}, e.inspectFailure
		}
		return sandbox.State{}, errors.New("injected Docker inspect failure")
	}
	return e.state, nil
}

func (e *runtimeEngine) Attach(context.Context, string) (client.ContainerAttachResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.attachErr != nil {
		return client.ContainerAttachResult{}, e.attachErr
	}
	controllerConn, rendererConn := net.Pipe()
	go func() {
		_, _ = io.Copy(io.Discard, rendererConn)
		_ = rendererConn.Close()
	}()
	if !e.blockOutput {
		output := e.rawOutput
		if output == nil {
			output = renderOutputFor(e.request)
		}
		if e.failurePhase != "" {
			output = renderFailureOutputFor(e.request, e.failurePhase, "compile_error")
		}
		return client.ContainerAttachResult{HijackedResponse: client.HijackedResponse{Conn: controllerConn, Reader: bufio.NewReader(bytes.NewReader(output))}}, nil
	}
	pipeReader, _ := io.Pipe()
	return client.ContainerAttachResult{HijackedResponse: client.HijackedResponse{Conn: closingConn{Conn: controllerConn, extra: pipeReader}, Reader: bufio.NewReader(pipeReader)}}, nil
}

type closingConn struct {
	net.Conn
	extra io.Closer
}

func (c closingConn) CloseWrite() error { return nil }
func (c closingConn) Close() error      { return errors.Join(c.Conn.Close(), c.extra.Close()) }

func (e *runtimeEngine) Start(_ context.Context, _ string) error {
	e.mu.Lock()
	gate, started := e.startGate, e.started
	e.mu.Unlock()
	if gate != nil {
		e.startOnce.Do(func() { close(started) })
		<-gate // Model Docker accepting Start after the request context is canceled.
	}
	e.mu.Lock()
	e.state = e.stateAfterStart
	e.state.ID = e.id
	e.inspectFailuresRemaining = e.inspectFailuresAfterStart
	e.mu.Unlock()
	return nil
}

func (e *runtimeEngine) Kill(context.Context, string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.killCalls++
	e.state = sandbox.State{ID: e.id, Status: "exited", ExitCode: 137}
	return nil
}

func (e *runtimeEngine) Remove(context.Context, string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removeCalls++
	e.removed = true
	return nil
}

func dockerStdout(protocol []byte) []byte {
	frame := make([]byte, 8+len(protocol))
	frame[0] = 1
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(protocol)))
	copy(frame[8:], protocol)
	return frame
}

func renderOutputFor(request renderwire.RenderRequest) []byte {
	content := []byte("%PDF-1.7\nrendered")
	hash := sha256.Sum256(content)
	desc := renderwire.PDFDescriptor{Size: int64(len(content)), SHA256: hex.EncodeToString(hash[:])}
	terminal := renderwire.TerminalEnvelope{Complete: true, Success: true, Scope: request.Scope, JobID: request.JobID, AttemptID: request.AttemptID, TargetID: request.TargetID, InputFingerprint: request.InputFingerprint, SourceSnapshotID: request.SourceSnapshotID, SourceContentID: request.SourceContentID, EngineVersion: "0.15.1", Phase: "complete", PDF: &desc}
	var protocol bytes.Buffer
	encoder := renderwire.NewEncoder(&protocol, renderwire.OutputLimit)
	header, _ := json.Marshal(desc)
	_ = encoder.WriteBytes(renderwire.PDFHeader, header)
	_ = encoder.WriteBytes(renderwire.PDF, content)
	terminalBytes, _ := json.Marshal(terminal)
	_ = encoder.WriteBytes(renderwire.Terminal, terminalBytes)
	return dockerStdout(protocol.Bytes())
}

func renderFailureOutputFor(request renderwire.RenderRequest, phase, code string) []byte {
	terminal := renderwire.TerminalEnvelope{Complete: true, Scope: request.Scope, JobID: request.JobID, AttemptID: request.AttemptID, TargetID: request.TargetID, InputFingerprint: request.InputFingerprint, SourceSnapshotID: request.SourceSnapshotID, SourceContentID: request.SourceContentID, EngineVersion: "0.15.1", Phase: phase, FailureCode: code}
	var protocol bytes.Buffer
	encoder := renderwire.NewEncoder(&protocol, renderwire.OutputLimit)
	terminalBytes, _ := json.Marshal(terminal)
	_ = encoder.WriteBytes(renderwire.Terminal, terminalBytes)
	return dockerStdout(protocol.Bytes())
}

func queuedRuntimeJob(t *testing.T, s *store.Store) generationv1.Job {
	t.Helper()
	ctx := context.Background()
	principal := storagev1.Principal{Subject: "runtime-actor", Domain: "user"}
	actor, err := service.EncodeInitiator(principal)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"scope":{"workspaceId":"workspace","projectId":"project"},"snapshotId":"snapshot-base","targetId":"pdf","entrypoint":"main.typ"}`)
	req, normalized, fingerprint, err := generationv1.NormalizeRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	admission, _, err := s.Admit(ctx, store.AdmitInput{WorkspaceID: req.Scope.WorkspaceID, ProjectID: req.Scope.ProjectID, InitiatorID: actor, IdempotencyKey: "runtime-test", NormalizedRequest: normalized, Fingerprint: fingerprint, EngineVersion: "0.15.1", SnapshotID: req.SnapshotID, TargetID: req.TargetID, Entrypoint: req.Entrypoint})
	if err != nil {
		t.Fatal(err)
	}
	acquire := storagev1.AcquirePinRequest{Scope: req.Scope, ObjectID: req.SnapshotID, HolderID: "leafrun-generation", HolderDomain: "service", RetentionPolicy: "ordinary", IdempotencyKey: "runtime-pin"}
	if _, err = s.BeginPinAcquireIntent(ctx, store.PinAcquireIntent{JobID: admission.Job.JobID, IdempotencyKey: acquire.IdempotencyKey, Request: acquire}); err != nil {
		t.Fatal(err)
	}
	pin := storagev1.Pin{PinID: "runtime-pin", Scope: req.Scope, ObjectID: req.SnapshotID, HolderID: acquire.HolderID, HolderDomain: acquire.HolderDomain, RetentionPolicy: "ordinary", State: "active", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour)}
	if err = s.RecordPinAcquireResponse(ctx, admission.Job.JobID, acquire.IdempotencyKey, pin); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkPinActiveAndRecordWorkflowStart(ctx, admission.Job.JobID, acquire.IdempotencyKey, "render/"+admission.Job.JobID, pin, []byte(fmt.Sprintf(`{"jobId":%q}`, admission.Job.JobID))); err != nil {
		t.Fatal(err)
	}
	return admission.Job
}

func newRuntimeController(t *testing.T, s *store.Store, storage *runtimeStorage, engine *runtimeEngine, root string) *Controller {
	t.Helper()
	controller, err := New(s, storage, engine, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "runtime-owner", RendererImage: runtimeImageID, Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func attemptForJob(t *testing.T, pool *pgxpool.Pool, jobID string) (string, string, bool) {
	t.Helper()
	var attemptID, stage string
	var stopped bool
	if err := pool.QueryRow(context.Background(), `SELECT attempt_id,private_staging_path,stop_confirmed FROM generation_attempts WHERE job_id=$1 ORDER BY attempt_number DESC LIMIT 1`, jobID).Scan(&attemptID, &stage, &stopped); err != nil {
		t.Fatal(err)
	}
	return attemptID, stage, stopped
}

func TestExecuteUnknownDockerStateRetainsStageAndExecutionReservation(t *testing.T) {
	s, pool := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Running: true, Status: "running"})
	engine.createUnknown = true
	engine.nameInspectErr = errors.New("Docker inspection timed out")
	root := t.TempDir()
	c := newRuntimeController(t, s, storage, engine, root)
	if err := c.Execute(context.Background(), job.JobID); err == nil {
		t.Fatal("unknown create state must remain unresolved")
	}
	attemptID, stage, stopped := attemptForJob(t, pool, job.JobID)
	if stopped || os.IsNotExist(func() error { _, err := os.Stat(stage); return err }()) {
		t.Fatalf("uncertain attempt lost its stop fact or private stage: stopped=%v path=%s", stopped, stage)
	}
	var executions int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, job.JobID).Scan(&executions); err != nil {
		t.Fatal(err)
	}
	if executions != 1 || engine.removeCalls != 0 {
		t.Fatalf("uncertain Docker state released its slot: reservations=%d removes=%d attempt=%s", executions, engine.removeCalls, attemptID)
	}
}

func TestExecuteRecoversLostCreateAcknowledgementByDeterministicName(t *testing.T) {
	s, pool := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Status: "created"})
	engine.loseCreateAck = true
	engine.attachErr = errors.New("attach unavailable")
	root := t.TempDir()
	c := newRuntimeController(t, s, storage, engine, root)
	if err := c.Execute(context.Background(), job.JobID); err == nil {
		t.Fatal("injected attach failure must be reported")
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, stage, stopped := attemptForJob(t, pool, job.JobID)
	attempt, err := s.GetAttempt(context.Background(), job.JobID, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if engine.createCalls != 1 || engine.nameInspections != 1 || !engine.removed || !stopped || current.Status != generationv1.Queued || attempt.State != "retryable" {
		t.Fatalf("lost create acknowledgement was not recovered safely: job=%s attempt=%+v creates=%d nameInspects=%d removed=%v", current.Status, attempt, engine.createCalls, engine.nameInspections, engine.removed)
	}
	if _, err = os.Stat(stage); err != nil {
		t.Fatalf("durable retry cleanup ran before the janitor: %v", err)
	}
	janitor, err := NewJanitor(s, storage, engine, Config{StagingRoot: root, OwnerID: "lost-create-cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	if err = janitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("janitor did not consume durable retry cleanup: %v", err)
	}
}

func TestExecuteDoesNotUploadUntilExactContainerExitZero(t *testing.T) {
	s, pool := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Running: true, Status: "running"})
	root := t.TempDir()
	c := newRuntimeController(t, s, storage, engine, root)
	if err := c.Execute(context.Background(), job.JobID); !errors.Is(err, errAttemptRetryScheduled) {
		t.Fatalf("technical retry was requeued without a Temporal retry signal: %v", err)
	}
	if storage.createUploadCalls != 0 || storage.putCalls != 0 || storage.finalizeCalls != 0 {
		t.Fatalf("Storage received a result before exit-zero inspection: create=%d put=%d finalize=%d", storage.createUploadCalls, storage.putCalls, storage.finalizeCalls)
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != generationv1.Queued || !engine.removed {
		t.Fatalf("running renderer was not safely stopped and retried: status=%s removed=%v", current.Status, engine.removed)
	}
	_, stage, _ := attemptForJob(t, pool, job.JobID)
	if _, err = os.Stat(stage); err != nil {
		t.Fatalf("retry stage was not retained for durable cleanup: %v", err)
	}
	janitor, err := NewJanitor(s, storage, engine, Config{StagingRoot: root, OwnerID: "retry-stage-cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	if err = janitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("janitor did not remove retryable attempt stage: %v", err)
	}
}

func TestExecuteBeginsPublicationOnlyAfterContainerStopIsDurable(t *testing.T) {
	s, _ := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Status: "exited", ExitCode: 0})
	storage.beforeCreateUpload = func(ctx context.Context) error {
		attempt, err := s.CurrentAttempt(ctx, job.JobID)
		if err != nil {
			return err
		}
		if !attempt.StopConfirmed || attempt.State != "finished" {
			return errors.New("publication began before the durable exact-container stop")
		}
		return nil
	}
	c := newRuntimeController(t, s, storage, engine, t.TempDir())
	if err := c.Execute(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != generationv1.Succeeded || storage.createUploadCalls != 1 || storage.putCalls != 1 || !engine.removed {
		t.Fatalf("confirmed render was not published after stop: status=%s creates=%d puts=%d removed=%v", current.Status, storage.createUploadCalls, storage.putCalls, engine.removed)
	}
}

func TestExecuteCompileFailureIsTerminalAndNeverPublishes(t *testing.T) {
	s, pool := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Status: "exited", ExitCode: 0})
	engine.failurePhase = "compile"
	c := newRuntimeController(t, s, storage, engine, t.TempDir())
	if err := c.Execute(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, stopped := attemptForJob(t, pool, job.JobID)
	if current.Status != generationv1.Failed || current.Phase != "compile" || current.FailureCode != "compile_error" || !stopped || storage.createUploadCalls != 0 {
		t.Fatalf("renderer compile failure was retried or published: status=%s phase=%s code=%s stopped=%v uploads=%d", current.Status, current.Phase, current.FailureCode, stopped, storage.createUploadCalls)
	}
}

func TestExecuteResourceLimitExitWithPartialEOFIsTerminal(t *testing.T) {
	tests := []struct {
		name  string
		state sandbox.State
	}{
		{name: "supervisor timeout", state: sandbox.State{Status: "exited", ExitCode: rendererTimeoutExit}},
		{name: "tmpfs limit", state: sandbox.State{Status: "exited", ExitCode: rendererLimitExit}},
		{name: "out of memory", state: sandbox.State{Status: "exited", ExitCode: 137, OOMKilled: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, pool := privateStore(t)
			job := queuedRuntimeJob(t, s)
			storage := newRuntimeStorage(job)
			state := test.state
			state.ID = strings.Repeat("a", 64)
			engine := newRuntimeEngine(job, state)
			engine.rawOutput = dockerStdout([]byte("partial renderer stream"))
			controller := newRuntimeController(t, s, storage, engine, t.TempDir())
			if err := controller.Execute(context.Background(), job.JobID); err != nil {
				t.Fatal(err)
			}
			current, err := s.GetJob(context.Background(), job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			var attempts int
			if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_attempts WHERE job_id=$1`, job.JobID).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if current.Status != generationv1.Failed || current.Phase != "resource_limit" || current.FailureCode != "resource_limit" || attempts != 1 || storage.createUploadCalls != 0 || !engine.removed {
				t.Fatalf("partial resource-limited renderer was retried/published: status=%s phase=%s code=%s attempts=%d uploads=%d removed=%v", current.Status, current.Phase, current.FailureCode, attempts, storage.createUploadCalls, engine.removed)
			}
		})
	}
}

func TestJanitorPreservesFinalInspectDeadlineAsResourceLimit(t *testing.T) {
	s, pool := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Status: "exited", ExitCode: 0})
	engine.inspectFailure = context.DeadlineExceeded
	engine.inspectFailuresAfterStart = 2 // final proof inspect, then pre-remove inspect
	root := t.TempDir()
	controller := newRuntimeController(t, s, storage, engine, root)
	if err := controller.Execute(context.Background(), job.JobID); err == nil {
		t.Fatal("injected inspect deadline and unknown stop must be surfaced")
	}
	attemptID, stage, stopped := attemptForJob(t, pool, job.JobID)
	if stopped {
		t.Fatal("unknown stop was incorrectly committed")
	}
	if _, err := os.Stat(stage); err != nil {
		t.Fatalf("unknown stop lost its private stage: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, attemptID); err != nil {
		t.Fatal(err)
	}
	janitor, err := NewJanitor(s, storage, engine, Config{StagingRoot: root, OwnerID: "deadline-janitor"})
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
	var attempts int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_attempts WHERE job_id=$1`, job.JobID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if current.Status != generationv1.Failed || current.Phase != "resource_limit" || current.FailureCode != "resource_limit" || attempts != 1 || !engine.removed {
		t.Fatalf("janitor forgot the final-inspect deadline classification: status=%s phase=%s code=%s attempts=%d removed=%v", current.Status, current.Phase, current.FailureCode, attempts, engine.removed)
	}
}

func TestExecuteCancellationDuringStartRemovesBeforeReleasingSlot(t *testing.T) {
	s, pool := privateStore(t)
	job := queuedRuntimeJob(t, s)
	storage := newRuntimeStorage(job)
	gate := make(chan struct{})
	engine := newRuntimeEngine(job, sandbox.State{ID: strings.Repeat("a", 64), Running: true, Status: "running"})
	engine.startGate = gate
	engine.started = make(chan struct{})
	engine.blockOutput = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newRuntimeController(t, s, storage, engine, t.TempDir())
	done := make(chan error, 1)
	go func() { done <- c.Execute(ctx, job.JobID) }()
	select {
	case <-engine.started:
	case <-time.After(5 * time.Second):
		t.Fatal("controller never reached Docker Start")
	}
	if canceled, err := s.RequestCancel(context.Background(), job.JobID); err != nil || !canceled {
		t.Fatalf("request cancellation: accepted=%v err=%v", canceled, err)
	}
	cancel()
	close(gate)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not finish after cancellation")
	}
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, stopped := attemptForJob(t, pool, job.JobID)
	var executions int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, job.JobID).Scan(&executions); err != nil {
		t.Fatal(err)
	}
	if current.Status != generationv1.Canceled || !stopped || !engine.removed || engine.killCalls == 0 || executions != 0 {
		t.Fatalf("late Start escaped cancellation fencing: status=%s stopped=%v removed=%v kills=%d executionSlots=%d", current.Status, stopped, engine.removed, engine.killCalls, executions)
	}
}

var _ Engine = (*runtimeEngine)(nil)
var _ Storage = (*runtimeStorage)(nil)
