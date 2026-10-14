package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func openStoreTest(t *testing.T, cfg Config) (*Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GENERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("GENERATION_TEST_DATABASE_URL is not set")
	}
	parsed, err := pgxpool.ParseConfig(dsn)
	if err != nil || parsed.ConnConfig.Database != "generation_test" {
		t.Fatal("Generation tests require the dedicated generation_test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to isolated Generation test database: %v", err)
	}
	if err = admin.Ping(ctx); err != nil {
		admin.Close()
		t.Fatalf("ping isolated Generation test database: %v", err)
	}
	schema, err := newID("generation_test_")
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatalf("create private Generation test schema: %v", err)
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatalf("configure private Generation test schema: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		t.Fatalf("connect to private Generation test schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		_, _ = admin.Exec(dropCtx, `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	})
	s, err := New(pool, cfg)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		pool.Close()
		t.Fatalf("apply Generation migrations: %v", err)
	}
	return s, pool
}

func testAdmissionInput(t *testing.T, raw []byte, workspace, project, initiator, key string) AdmitInput {
	t.Helper()
	r, normalized, fingerprint, err := generationv1.NormalizeRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	return AdmitInput{WorkspaceID: workspace, ProjectID: project, InitiatorID: initiator, IdempotencyKey: key, NormalizedRequest: normalized, Parameters: r.Parameters, Fingerprint: fingerprint, EngineVersion: "typst-0.15.1", SnapshotID: r.SnapshotID, TargetID: r.TargetID, Entrypoint: r.Entrypoint}
}

func testRequest(workspace, project string, parameterLexeme string) []byte {
	return []byte(fmt.Sprintf(`{"scope":{"workspaceId":%q,"projectId":%q},"snapshotId":"snap-test","targetId":"pdf","entrypoint":"main.typ","parameters":{"n":%s},"parametersSchemaPath":"params.schema.json"}`, workspace, project, parameterLexeme))
}

func cleanupTestJobs(t *testing.T, pool *pgxpool.Pool, ids ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, id := range ids {
			_, _ = pool.Exec(ctx, `DELETE FROM platform_event_inbox WHERE event_id LIKE 'generation:'||$1||':v%'`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM platform_event_outbox WHERE event_id LIKE 'generation:'||$1||':v%'`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM generation_jobs WHERE job_id=$1`, id)
		}
	})
}

func admitTestJob(t *testing.T, s *Store, workspace, project, initiator, key string) generationv1.Admission {
	t.Helper()
	a, _, err := s.Admit(context.Background(), testAdmissionInput(t, testRequest(workspace, project, "1.0"), workspace, project, initiator, key))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func activateTestPin(t *testing.T, s *Store, pool *pgxpool.Pool, job generationv1.Job) storagev1.Pin {
	t.Helper()
	key := "pin-intent-" + job.JobID
	req := storagev1.AcquirePinRequest{Scope: job.Scope, ObjectID: job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: "generation-job", IdempotencyKey: key}
	requestBytes, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginPinAcquireIntent(context.Background(), PinAcquireIntent{JobID: job.JobID, IdempotencyKey: key, Request: req, RequestBytes: requestBytes}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	pin := storagev1.Pin{PinID: "pin-" + job.JobID, Scope: job.Scope, ObjectID: job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: req.RetentionPolicy, State: "pending", CreatedAt: base, ExpiresAt: base.Add(time.Hour)}
	if err = s.RecordPinAcquireResponse(context.Background(), job.JobID, key, pin); err != nil {
		t.Fatal(err)
	}
	pin.State = "active"
	workflowRequest, _ := json.Marshal(struct {
		JobID string `json:"jobId"`
	}{job.JobID})
	if err = s.MarkPinActiveAndRecordWorkflowStart(context.Background(), job.JobID, key, "render/"+job.JobID, pin, workflowRequest); err != nil {
		t.Fatal(err)
	}
	claims, err := s.ClaimWorkflowStarts(context.Background(), "test-dispatcher", 100, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var epoch int64
	for _, claim := range claims {
		if claim.JobID == job.JobID {
			epoch = claim.LeaseEpoch
		}
	}
	if err = s.MarkWorkflowStarted(context.Background(), job.JobID, "render/"+job.JobID, "test-dispatcher", epoch); err != nil {
		t.Fatal(err)
	}
	_ = pool
	return pin
}

func TestPostgresAdmissionReplayExactBytesAndConcurrentQuota(t *testing.T) {
	s, pool := openStoreTest(t, Config{QueueGlobal: 1, QueueWorkspace: 1})
	firstInput := testAdmissionInput(t, testRequest("ws-replay", "p1", "1.0"), "ws-replay", "p1", "user-1", "key-1")
	first, replayed, err := s.Admit(context.Background(), firstInput)
	if err != nil || replayed {
		t.Fatalf("first admission: replay=%v err=%v", replayed, err)
	}
	cleanupTestJobs(t, pool, first.Job.JobID)
	var savedRequest, savedParameters []byte
	if err = pool.QueryRow(context.Background(), `SELECT normalized_request,parameters FROM generation_jobs WHERE job_id=$1`, first.Job.JobID).Scan(&savedRequest, &savedParameters); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(savedRequest, firstInput.NormalizedRequest) || !bytes.Equal(savedParameters, firstInput.Parameters) || !bytes.Contains(savedParameters, []byte("1.0")) {
		t.Fatalf("normalized numeric lexeme changed: request=%s params=%s", savedRequest, savedParameters)
	}
	second, replayed, err := s.Admit(context.Background(), firstInput)
	if err != nil || !replayed || second.Job.JobID != first.Job.JobID {
		t.Fatalf("same-key replay: job=%s replay=%v err=%v", second.Job.JobID, replayed, err)
	}
	changed := testAdmissionInput(t, testRequest("ws-replay", "p1", "1.00"), "ws-replay", "p1", "user-1", "key-1")
	if _, _, err = s.Admit(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed numeric lexeme should conflict, got %v", err)
	}
	if _, _, err = s.Admit(context.Background(), testAdmissionInput(t, testRequest("ws-replay", "p1", "2"), "ws-replay", "p1", "user-2", "key-2")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("workspace queue limit should reject the second reservation, got %v", err)
	}
	if _, err = pool.Exec(context.Background(), `DELETE FROM generation_jobs WHERE job_id=$1`, first.Job.JobID); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var ids []string
	successes, capacity := 0, 0
	var failures []error
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ws := fmt.Sprintf("ws-parallel-%d", i)
			a, _, e := s.Admit(context.Background(), testAdmissionInput(t, testRequest(ws, "project", "1"), ws, "project", fmt.Sprintf("user-%d", i), fmt.Sprintf("key-%d", i)))
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				successes++
				ids = append(ids, a.Job.JobID)
			} else if errors.Is(e, ErrCapacity) {
				capacity++
			} else {
				failures = append(failures, e)
			}
		}(i)
	}
	wg.Wait()
	cleanupTestJobs(t, pool, ids...)
	if len(failures) > 0 || successes != 1 || capacity != 7 {
		t.Fatalf("global quota was not serialized: success=%d capacity=%d failures=%v", successes, capacity, failures)
	}
}

func TestPostgresPinRotationKeepsJobAndWorkflowIntentDurable(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-pin", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	makeRequest := func(key string) (storagev1.AcquirePinRequest, []byte) {
		r := storagev1.AcquirePinRequest{Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: "generation-job", IdempotencyKey: key}
		b, _ := json.Marshal(r)
		return r, b
	}
	r1, b1 := makeRequest("pin-key-1")
	if _, err := s.BeginPinAcquireIntent(context.Background(), PinAcquireIntent{JobID: a.Job.JobID, IdempotencyKey: r1.IdempotencyKey, Request: r1, RequestBytes: b1}); err != nil {
		t.Fatal(err)
	}
	p1 := storagev1.Pin{PinID: "pin-pending-1", Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: r1.RetentionPolicy, State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour).UTC()}
	if err := s.RecordPinAcquireResponse(context.Background(), a.Job.JobID, r1.IdempotencyKey, p1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE generation_pin_acquires SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1 AND idempotency_key=$2`, a.Job.JobID, r1.IdempotencyKey); err != nil {
		t.Fatal(err)
	}
	r2, b2 := makeRequest("pin-key-2")
	rotated, err := s.BeginPinAcquireIntent(context.Background(), PinAcquireIntent{JobID: a.Job.JobID, IdempotencyKey: r2.IdempotencyKey, Request: r2, RequestBytes: b2})
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Number != 2 {
		t.Fatalf("expected a new pin acquire attempt, got %d", rotated.Number)
	}
	var oldPinRelease int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_cleanup_intents WHERE job_id=$1 AND kind='pin.release' AND idempotency_key='release/pin-pending-1' AND not_before<=clock_timestamp()`, a.Job.JobID).Scan(&oldPinRelease); err != nil || oldPinRelease != 1 {
		t.Fatalf("expired pin acquire was not queued for release: count=%d err=%v", oldPinRelease, err)
	}
	p2 := storagev1.Pin{PinID: "pin-active-2", Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: r2.RetentionPolicy, State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour).UTC()}
	if err = s.RecordPinAcquireResponse(context.Background(), a.Job.JobID, r2.IdempotencyKey, p2); err != nil {
		t.Fatal(err)
	}
	p2.State = "active"
	workflowRequest := []byte(`{"jobId":"` + a.Job.JobID + `"}`)
	if err = s.MarkPinActiveAndRecordWorkflowStart(context.Background(), a.Job.JobID, r2.IdempotencyKey, "render/"+a.Job.JobID, p2, workflowRequest); err != nil {
		t.Fatal(err)
	}
	jobAfter, err := s.GetJob(context.Background(), a.Job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkPinActiveAndRecordWorkflowStart(context.Background(), a.Job.JobID, r2.IdempotencyKey, "render/"+a.Job.JobID, p2, workflowRequest); err != nil {
		t.Fatalf("lost workflow-start response replay failed: %v", err)
	}
	jobReplay, err := s.GetJob(context.Background(), a.Job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if jobReplay.Version != jobAfter.Version {
		t.Fatalf("pin/workflow replay advanced version: %d -> %d", jobAfter.Version, jobReplay.Version)
	}
	intents, err := s.ListPendingWorkflowStarts(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range intents {
		if w.JobID == a.Job.JobID && w.WorkflowID == "render/"+a.Job.JobID && bytes.Equal(w.Request, workflowRequest) {
			found = true
		}
	}
	if !found {
		t.Fatal("durable workflow-start intent was not listed")
	}
	claimed, err := s.ClaimWorkflowStarts(context.Background(), "dispatcher-a", 10, 5*time.Second)
	if err != nil || len(claimed) != 1 || claimed[0].WorkflowID != "render/"+a.Job.JobID {
		t.Fatalf("workflow start was not durably claimed: %#v err=%v", claimed, err)
	}
	other, err := s.ClaimWorkflowStarts(context.Background(), "dispatcher-b", 10, 5*time.Second)
	if err != nil || len(other) != 0 {
		t.Fatalf("active workflow claim was not exclusive: %#v err=%v", other, err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_workflow_starts SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := s.ClaimWorkflowStarts(context.Background(), "dispatcher-b", 10, 5*time.Second)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].WorkflowID != claimed[0].WorkflowID {
		t.Fatalf("expired workflow claim did not replay stable ID: %#v err=%v", reclaimed, err)
	}
	if err = s.MarkWorkflowStarted(context.Background(), a.Job.JobID, "render/"+a.Job.JobID, claimed[0].LeaseOwner, claimed[0].LeaseEpoch); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale start ACK accepted: %v", err)
	}
	if err = s.MarkWorkflowStarted(context.Background(), a.Job.JobID, "render/"+a.Job.JobID, reclaimed[0].LeaseOwner, reclaimed[0].LeaseEpoch); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresLatePinResponseAfterCancellationSchedulesRelease(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-pin-late", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	key := "pin-lost-response"
	req := storagev1.AcquirePinRequest{Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: "generation-job", IdempotencyKey: key}
	encoded, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginPinAcquireIntent(context.Background(), PinAcquireIntent{JobID: a.Job.JobID, IdempotencyKey: key, Request: req, RequestBytes: encoded}); err != nil {
		t.Fatal(err)
	}
	if canceled, err := s.RequestCancel(context.Background(), a.Job.JobID); err != nil || !canceled {
		t.Fatalf("cancel pending admission: canceled=%v err=%v", canceled, err)
	}
	latePin := storagev1.Pin{PinID: "pin-late", Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: req.HolderID, HolderDomain: req.HolderDomain, RetentionPolicy: req.RetentionPolicy, State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour).UTC()}
	if err = s.RecordPinAcquireResponse(context.Background(), a.Job.JobID, key, latePin); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_cleanup_intents WHERE job_id=$1 AND kind='pin.release' AND idempotency_key='release/pin-late' AND not_before<=clock_timestamp()`, a.Job.JobID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("late pin receipt was not converted into immediate cleanup: count=%d err=%v", count, err)
	}
}

func TestPostgresDerivedSnapshotPinPersistsAndIsOwned(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-derived-pin", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	activateTestPin(t, s, pool, a.Job)
	attempt, err := s.ClaimExecution(context.Background(), ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "create-derived-pin", StagingToken: "opaque-stage", PrivateStagingPath: "C:\\private\\derived-pin"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordDerivedIdentity(context.Background(), a.Job.JobID, attempt.AttemptID, attempt.FencingEpoch, "derived-snapshot", "derived-content"); err != nil {
		t.Fatal(err)
	}
	key := "derived-pin-key"
	req := storagev1.AcquirePinRequest{Scope: a.Job.Scope, ObjectID: "derived-snapshot", HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: "generation-job", IdempotencyKey: key}
	requestBytes, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginPinAcquireIntent(context.Background(), PinAcquireIntent{JobID: a.Job.JobID, IdempotencyKey: key, Request: req, RequestBytes: requestBytes}); err != nil {
		t.Fatalf("derived snapshot pin intent was rejected after the source pin became active: %v", err)
	}
	pin := storagev1.Pin{PinID: "derived-pin-id", Scope: req.Scope, ObjectID: req.ObjectID, HolderID: req.HolderID, HolderDomain: req.HolderDomain, RetentionPolicy: req.RetentionPolicy, State: "pending", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour).UTC()}
	if err = s.RecordPinAcquireResponse(context.Background(), a.Job.JobID, key, pin); err != nil {
		t.Fatal(err)
	}
	pin.State = "active"
	if err = s.MarkPinActive(context.Background(), a.Job.JobID, key, pin); err != nil {
		t.Fatal(err)
	}
	ownerState, err := s.Owner().Pin(context.Background(), storagev1.PinOwnerCheck{Scope: pin.Scope, PinID: pin.PinID, ObjectID: pin.ObjectID, HolderID: pin.HolderID, HolderDomain: pin.HolderDomain})
	if err != nil || ownerState != storagev1.OwnerReferenced {
		t.Fatalf("active derived snapshot pin was not recognized by the owner callback: state=%s err=%v", ownerState, err)
	}
}

func TestPostgresAttemptFenceAndUnknownContainerKeepsExecutionSlot(t *testing.T) {
	s, pool := openStoreTest(t, Config{QueueGlobal: 2, QueueWorkspace: 2, ExecutionGlobal: 1})
	a := admitTestJob(t, s, "ws-attempt", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	activateTestPin(t, s, pool, a.Job)
	first, err := s.ClaimExecution(context.Background(), ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "create-1", StagingToken: "opaque-stage", PrivateStagingPath: "C:\\private\\job\\attempt-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkRendering(context.Background(), first.AttemptID, first.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	containerRequest := []byte(`{"image":"pinned-image","network":"none"}`)
	intentInput := IntentInput{JobID: a.Job.JobID, RenderAttemptID: first.AttemptID, AttemptEpoch: first.FencingEpoch, Kind: IntentContainerCreate, IdempotencyKey: "create-1", Request: containerRequest}
	if _, err = s.SaveIntent(context.Background(), intentInput); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteIntent(context.Background(), intentInput, []byte(`{"containerId":"container-exact-1"}`)); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordContainer(context.Background(), first.AttemptID, first.FencingEpoch, "container-exact-1"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.HeartbeatAttempt(context.Background(), first.AttemptID, first.FencingEpoch); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale heartbeat was accepted: %v", err)
	}
	if err = s.MarkAttemptStopped(context.Background(), first.AttemptID, first.FencingEpoch, "wrong-container"); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong Docker ID was accepted: %v", err)
	}
	var execCount int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, a.Job.JobID).Scan(&execCount); err != nil {
		t.Fatal(err)
	}
	if execCount != 1 {
		t.Fatal("unknown Docker state released its execution slot")
	}
	if err = s.MarkAttemptStopped(context.Background(), first.AttemptID, first.FencingEpoch, "container-exact-1"); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryStoppedAttempt(context.Background(), first.AttemptID, first.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	second, err := s.ClaimExecution(context.Background(), ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "create-2", StagingToken: "opaque-stage-2", PrivateStagingPath: "C:\\private\\job\\attempt-2"})
	if err != nil {
		t.Fatal(err)
	}
	if second.AttemptNumber != 2 || second.AttemptID == first.AttemptID || second.FencingEpoch <= first.FencingEpoch {
		t.Fatalf("retry reused attempt identity or fence: first=%+v second=%+v", first, second)
	}
	if err = s.RecordContainer(context.Background(), first.AttemptID, first.FencingEpoch, "late-container"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old attempt mutated after retry: %v", err)
	}
}

func TestPostgresJanitorRecoversContainerAfterTerminalFailure(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-janitor-recovery", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	activateTestPin(t, s, pool, a.Job)
	attempt, err := s.ClaimExecution(context.Background(), ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "docker-create-key", StagingToken: "opaque-stage", PrivateStagingPath: "C:\\private\\job\\recover"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FailJob(context.Background(), TerminalInput{JobID: a.Job.JobID, Diagnostics: json.RawMessage(`[]`), FailureCode: "worker_lost"}); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordContainer(context.Background(), attempt.AttemptID, attempt.FencingEpoch, "late-container"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("terminal controller registered a late container: %v", err)
	}
	if err = s.RecordRecoveredContainer(context.Background(), attempt.AttemptID, attempt.FencingEpoch, "wrong-create-key", "container-recovered"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("janitor recovered a container from the wrong create key: %v", err)
	}
	var slots int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, a.Job.JobID).Scan(&slots); err != nil || slots != 1 {
		t.Fatalf("terminal unknown container released its execution slot: slots=%d err=%v", slots, err)
	}
	if err = s.RecordRecoveredContainer(context.Background(), attempt.AttemptID, attempt.FencingEpoch, "docker-create-key", "container-recovered"); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptStopped(context.Background(), attempt.AttemptID, attempt.FencingEpoch, "container-recovered"); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution'`, a.Job.JobID).Scan(&slots); err != nil || slots != 0 {
		t.Fatalf("confirmed recovered container did not release execution slot: slots=%d err=%v", slots, err)
	}
}

func finishTestAttempt(t *testing.T, s *Store, pool *pgxpool.Pool, job generationv1.Job, suffix string) Attempt {
	t.Helper()
	activateTestPin(t, s, pool, job)
	attempt, err := s.ClaimExecution(context.Background(), ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: "create-" + suffix, StagingToken: "opaque-" + suffix, PrivateStagingPath: "C:\\private\\" + job.JobID + "\\" + suffix})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkRendering(context.Background(), attempt.AttemptID, attempt.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordContainer(context.Background(), attempt.AttemptID, attempt.FencingEpoch, "container-"+suffix); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptStopped(context.Background(), attempt.AttemptID, attempt.FencingEpoch, "container-"+suffix); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptRendered(context.Background(), attempt.AttemptID, attempt.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	return attempt
}

func createTestCandidate(t *testing.T, s *Store, job generationv1.Job, attempt Attempt, publisher, artifactID string) (PublicationLease, PublicationCandidate) {
	t.Helper()
	lease, err := s.BeginPublication(context.Background(), job.JobID, publisher)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	artifact := storagev1.Artifact{
		ArtifactID: artifactID, Scope: storagev1.Scope{WorkspaceID: job.Scope.WorkspaceID, ProjectID: job.Scope.ProjectID},
		OriginJobID: job.JobID, SnapshotID: job.SourceSnapshotID, Hash: "sha256-test-" + artifactID, SizeBytes: 42,
		Metadata:  storagev1.ArtifactMetadata{Filename: "main.pdf", MediaType: "application/pdf", TargetID: job.TargetID, AttemptID: attempt.AttemptID, InputFingerprint: job.InputFingerprint},
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	artifactBytes, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(storagev1.FinalizeArtifactRequest{
		WriterToken: "private-writer-token", OriginJobID: job.JobID, SnapshotID: job.SourceSnapshotID,
		Metadata: artifact.Metadata, IdempotencyKey: "finalize-" + artifactID,
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := IntentInput{JobID: job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, PublicationEpoch: lease.Epoch, Owner: publisher, Kind: IntentArtifactFinalize, IdempotencyKey: "finalize-" + artifactID, Request: request}
	if _, err = s.SaveIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteIntent(context.Background(), intent, artifactBytes); err != nil {
		t.Fatal(err)
	}
	candidate := PublicationCandidate{
		JobID: job.JobID, RenderAttemptID: attempt.AttemptID, Scope: artifact.Scope, SnapshotID: artifact.SnapshotID,
		TargetID: artifact.Metadata.TargetID, InputFingerprint: artifact.Metadata.InputFingerprint, Artifact: artifact,
		ArtifactBytes: artifactBytes, FinalizeReceipt: artifactBytes,
	}
	if err = s.RegisterCandidate(context.Background(), job.JobID, publisher, lease.Epoch, candidate); err != nil {
		t.Fatal(err)
	}
	candidate, err = s.GetCandidate(context.Background(), job.JobID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	return lease, candidate
}

func successInput(jobID, publisher string, lease PublicationLease, candidate PublicationCandidate) SuccessInput {
	return SuccessInput{JobID: jobID, Publisher: publisher, PublicationEpoch: lease.Epoch, CandidateID: candidate.CandidateID, Artifact: candidate.Artifact, Diagnostics: json.RawMessage(`[]`), RawDiagnostics: "raw compiler output"}
}

func TestPostgresPublicationAdoptionFencesOldPublisherAndExpiry(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-publication-adopt", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	attempt := finishTestAttempt(t, s, pool, a.Job, "adopt")
	first, candidate := createTestCandidate(t, s, a.Job, attempt, "publisher-old", "artifact-adopt")
	if _, err := pool.Exec(context.Background(), `UPDATE generation_jobs SET publication_lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	second, err := s.BeginPublication(context.Background(), a.Job.JobID, "publisher-new")
	if err != nil {
		t.Fatal(err)
	}
	if second.Candidate == nil || second.Candidate.CandidateID != candidate.CandidateID {
		t.Fatal("publication takeover did not adopt the unexpired durable candidate")
	}
	// Fetch the adopted candidate again to assert durability independently of
	// the takeover response copy.
	adoptedCandidate, err := s.GetCandidate(context.Background(), a.Job.JobID, candidate.Artifact.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if adoptedCandidate.CandidateID != candidate.CandidateID || adoptedCandidate.RenderAttemptID != attempt.AttemptID {
		t.Fatal("publication takeover lost the prior immutable candidate")
	}
	if err = s.TerminalSuccess(context.Background(), successInput(a.Job.JobID, "publisher-old", first, candidate)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old publisher committed after takeover: %v", err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_admission_receipts SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if err = s.TerminalSuccess(context.Background(), successInput(a.Job.JobID, "publisher-new", second, adoptedCandidate)); !errors.Is(err, ErrReceiptExpired) {
		t.Fatalf("expired admission receipt permitted historical success: %v", err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_admission_receipts SET expires_at=clock_timestamp()+interval '1 hour' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_publication_candidates SET expires_at=clock_timestamp()-interval '1 second' WHERE candidate_id=$1`, candidate.CandidateID); err != nil {
		t.Fatal(err)
	}
	if err = s.TerminalSuccess(context.Background(), successInput(a.Job.JobID, "publisher-new", second, adoptedCandidate)); !errors.Is(err, ErrCandidateExpired) {
		t.Fatalf("expired candidate created a new success: %v", err)
	}
	var attemptCount int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_attempts WHERE job_id=$1`, a.Job.JobID).Scan(&attemptCount); err != nil || attemptCount != 1 {
		t.Fatalf("publication adoption started a new render attempt: count=%d err=%v", attemptCount, err)
	}
}

func TestPostgresPublicationCommitAtomicAndHistoricalSuccess(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-publication-success", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	attempt := finishTestAttempt(t, s, pool, a.Job, "success")
	lease, candidate := createTestCandidate(t, s, a.Job, attempt, "publisher", "artifact-success")
	var version int64
	if err := pool.QueryRow(context.Background(), `SELECT version FROM generation_jobs WHERE job_id=$1`, a.Job.JobID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	conflictEventID := fmt.Sprintf("generation:%s:v%d", a.Job.JobID, version+1)
	if _, err := pool.Exec(context.Background(), `INSERT INTO platform_event_outbox(event_id,subject,encoded_envelope,payload_hash) VALUES($1,'wrong.subject','{}'::bytea,'wrong'::bytea)`, conflictEventID); err != nil {
		t.Fatal(err)
	}
	if err := s.TerminalSuccess(context.Background(), successInput(a.Job.JobID, "publisher", lease, candidate)); err == nil {
		t.Fatal("outbox event identity conflict should abort terminal success")
	}
	job, err := s.GetJob(context.Background(), a.Job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != generationv1.Publishing {
		t.Fatalf("job state committed without its terminal event: %s", job.Status)
	}
	if _, err = pool.Exec(context.Background(), `DELETE FROM platform_event_outbox WHERE event_id=$1`, conflictEventID); err != nil {
		t.Fatal(err)
	}
	if err = s.TerminalSuccess(context.Background(), successInput(a.Job.JobID, "publisher", lease, candidate)); err != nil {
		t.Fatal(err)
	}
	var encodedEvent []byte
	if err = pool.QueryRow(context.Background(), `SELECT encoded_envelope FROM platform_event_outbox WHERE event_id=$1`, conflictEventID).Scan(&encodedEvent); err != nil {
		t.Fatal(err)
	}
	var decodedEvent struct {
		Producer string `json:"producer"`
		Data     struct {
			JobID string `json:"jobId"`
		} `json:"data"`
	}
	if err = json.Unmarshal(encodedEvent, &decodedEvent); err != nil || decodedEvent.Data.JobID != a.Job.JobID || decodedEvent.Producer != "leafrun-generation" {
		t.Fatalf("success event missing or has wrong identity: %+v err=%v", decodedEvent, err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_publication_candidates SET expires_at=clock_timestamp()-interval '1 second' WHERE candidate_id=$1`, candidate.CandidateID); err != nil {
		t.Fatal(err)
	}
	var publishable bool
	publishable, err = s.Owner().Publishable(context.Background(), storagev1.PublishabilityCheck{Scope: candidate.Scope, OriginJobID: a.Job.JobID, ArtifactID: candidate.Artifact.ArtifactID})
	if err != nil || !publishable {
		t.Fatalf("committed historical success disappeared after artifact expiry: publishable=%v err=%v", publishable, err)
	}
}

func TestPostgresCancellationAndPublicationCommitSerialize(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "ws-cancel-race", "project", "user", "admission")
	cleanupTestJobs(t, pool, a.Job.JobID)
	attempt := finishTestAttempt(t, s, pool, a.Job, "race")
	lease, candidate := createTestCandidate(t, s, a.Job, attempt, "publisher", "artifact-race")
	start := make(chan struct{})
	var wg sync.WaitGroup
	var successErr error
	var cancelAccepted bool
	var cancelErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		successErr = s.TerminalSuccess(context.Background(), successInput(a.Job.JobID, "publisher", lease, candidate))
	}()
	go func() {
		defer wg.Done()
		<-start
		cancelAccepted, cancelErr = s.RequestCancel(context.Background(), a.Job.JobID)
	}()
	close(start)
	wg.Wait()
	if cancelErr != nil {
		t.Fatal(cancelErr)
	}
	job, err := s.GetJob(context.Background(), a.Job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != generationv1.Succeeded && job.Status != generationv1.Canceled {
		t.Fatalf("race left job in nonterminal state %q (success err=%v, cancel accepted=%v)", job.Status, successErr, cancelAccepted)
	}
	if job.Status == generationv1.Succeeded && successErr != nil {
		t.Fatalf("success won but reported an error: %v", successErr)
	}
	if job.Status == generationv1.Canceled && !errors.Is(successErr, ErrLeaseLost) && !errors.Is(successErr, ErrInvalidTransition) {
		t.Fatalf("cancellation won but publisher was not fenced: %v", successErr)
	}
	var n int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_terminal_outbox t JOIN platform_event_outbox o ON o.event_id=t.event_id WHERE t.job_id=$1`, a.Job.JobID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("state and terminal event were not committed exactly once: events=%d err=%v", n, err)
	}
}

func TestPostgresRetentionScrubsParameterCopiesAndWaitsForDurableWork(t *testing.T) {
	s, pool := openStoreTest(t, Config{ReceiptTTL: time.Second, DiagnosticTTL: time.Second, HistoryTTL: time.Second})
	const sentinel = "parameter-secret-retention-sentinel"
	raw := []byte(`{"scope":{"workspaceId":"ws-retention","projectId":"project"},"snapshotId":"snap-test","targetId":"pdf","entrypoint":"main.typ","parameters":{"token":"` + sentinel + `"},"parametersSchemaPath":"params.schema.json"}`)
	a, _, err := s.Admit(context.Background(), testAdmissionInput(t, raw, "ws-retention", "project", "user", "admission"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupTestJobs(t, pool, a.Job.JobID)
	completed := IntentInput{JobID: a.Job.JobID, Kind: IntentDeriveBundle, IdempotencyKey: "derive-completed", Request: []byte(`{"source":"` + sentinel + `"}`)}
	if _, err = s.SaveIntent(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteIntent(context.Background(), completed, []byte(`{"derived":"`+sentinel+`"}`)); err != nil {
		t.Fatal(err)
	}
	pendingSensitive := IntentInput{JobID: a.Job.JobID, Kind: IntentRenderInput, IdempotencyKey: "render-input-pending", Request: []byte(`{"parameters":"` + sentinel + `"}`)}
	if _, err = s.SaveIntent(context.Background(), pendingSensitive); err != nil {
		t.Fatal(err)
	}
	pendingWork := IntentInput{JobID: a.Job.JobID, Kind: IntentContainerStart, IdempotencyKey: "start-pending", Request: []byte(`{"containerId":"unknown"}`)}
	if _, err = s.SaveIntent(context.Background(), pendingWork); err != nil {
		t.Fatal(err)
	}
	if err = s.FailJob(context.Background(), TerminalInput{JobID: a.Job.JobID, Diagnostics: json.RawMessage(`[]`), RawDiagnostics: "diagnostic-secret " + sentinel, FailureCode: "compile_failed"}); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_jobs SET terminal_at=clock_timestamp()-interval '2 seconds' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_admission_receipts SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Retain(context.Background()); err != nil {
		t.Fatal(err)
	}
	var normalized, parameters []byte
	var rawDiagnostics string
	if err = pool.QueryRow(context.Background(), `SELECT normalized_request,parameters,raw_diagnostics FROM generation_jobs WHERE job_id=$1`, a.Job.JobID).Scan(&normalized, &parameters, &rawDiagnostics); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(normalized, []byte(sentinel)) || bytes.Contains(parameters, []byte(sentinel)) || bytes.Contains([]byte(rawDiagnostics), []byte(sentinel)) {
		t.Fatal("terminal job retained parameter or raw-diagnostic copies after the diagnostic TTL")
	}
	var completedRequest, completedOutput, pendingRequest, pendingOutput []byte
	var abandonedAt *time.Time
	if err = pool.QueryRow(context.Background(), `SELECT request,output FROM generation_operation_intents WHERE job_id=$1 AND idempotency_key='derive-completed'`, a.Job.JobID).Scan(&completedRequest, &completedOutput); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(context.Background(), `SELECT request,output,abandoned_at FROM generation_operation_intents WHERE job_id=$1 AND idempotency_key='render-input-pending'`, a.Job.JobID).Scan(&pendingRequest, &pendingOutput, &abandonedAt); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(completedRequest, []byte(sentinel)) || bytes.Contains(completedOutput, []byte(sentinel)) || bytes.Contains(pendingRequest, []byte(sentinel)) || len(pendingOutput) != 0 || abandonedAt == nil {
		t.Fatal("sensitive intent copies were not scrubbed and durably abandoned")
	}
	var cleanupCount int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_cleanup_intents WHERE job_id=$1 AND kind='intent.abandoned' AND state='pending'`, a.Job.JobID).Scan(&cleanupCount); err != nil || cleanupCount != 1 {
		t.Fatalf("sensitive intent cleanup was not durable: count=%d err=%v", cleanupCount, err)
	}
	var secretCleanup bool
	if err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM generation_cleanup_intents WHERE job_id=$1 AND position($2 in convert_from(request,'UTF8'))>0)`, a.Job.JobID, sentinel).Scan(&secretCleanup); err != nil || secretCleanup {
		t.Fatalf("cleanup intent copied parameter data: found=%v err=%v", secretCleanup, err)
	}
	var kept bool
	if err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM generation_jobs WHERE job_id=$1)`, a.Job.JobID).Scan(&kept); err != nil || !kept {
		t.Fatalf("pending cleanup or operation intent did not protect history: kept=%v err=%v", kept, err)
	}
	claimed, err := s.ClaimCleanup(context.Background(), "retention-test", 10, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim abandoned-intent cleanup: items=%d err=%v", len(claimed), err)
	}
	if err = s.CompleteCleanup(context.Background(), claimed[0].ID, "retention-test", claimed[0].LeaseEpoch, []byte(`{"abandoned":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE platform_event_outbox o SET sent_at=clock_timestamp() FROM generation_terminal_outbox t WHERE t.job_id=$1 AND o.event_id=t.event_id`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Retain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM generation_jobs WHERE job_id=$1)`, a.Job.JobID).Scan(&kept); err != nil || !kept {
		t.Fatalf("unresolved operation intent did not protect history after cleanup: kept=%v err=%v", kept, err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_operation_intents SET output='{}'::bytea,completed_at=clock_timestamp() WHERE job_id=$1 AND idempotency_key='start-pending'`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Retain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM generation_jobs WHERE job_id=$1)`, a.Job.JobID).Scan(&kept); err != nil || kept {
		t.Fatalf("resolved old history was not collected: kept=%v err=%v", kept, err)
	}
}
