package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestPostgresPendingPinOwnerAndActivationOrdering(t *testing.T) {
	s, _ := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "pending-owner", "project", "user", "admit")
	ctx := context.Background()
	req := storagev1.AcquirePinRequest{Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: "ordinary", IdempotencyKey: "acquire"}
	if _, err := s.BeginPinAcquireIntent(ctx, PinAcquireIntent{JobID: a.Job.JobID, IdempotencyKey: req.IdempotencyKey, Request: req}); err != nil {
		t.Fatal(err)
	}
	pin := storagev1.Pin{PinID: "pending-pin", Scope: req.Scope, ObjectID: req.ObjectID, HolderID: req.HolderID, HolderDomain: req.HolderDomain, RetentionPolicy: req.RetentionPolicy, State: "active", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}
	workflow, _ := json.Marshal(map[string]string{"jobId": a.Job.JobID})
	if err := s.MarkPinActiveAndRecordWorkflowStart(ctx, a.Job.JobID, req.IdempotencyKey, "render/"+a.Job.JobID, pin, workflow); err == nil {
		t.Fatal("activation accepted before durable pending binding")
	}
	pin.State = "pending"
	if err := s.RecordPinAcquireResponse(ctx, a.Job.JobID, req.IdempotencyKey, pin); err != nil {
		t.Fatal(err)
	}
	state, err := s.Owner().Pin(ctx, storagev1.PinOwnerCheck{Scope: pin.Scope, PinID: pin.PinID, ObjectID: pin.ObjectID, HolderID: pin.HolderID, HolderDomain: pin.HolderDomain})
	if err != nil || state != storagev1.OwnerReferenced {
		t.Fatalf("Storage activation callback: state=%s error=%v", state, err)
	}
	pin.State = "active"
	if err := s.MarkPinActiveAndRecordWorkflowStart(ctx, a.Job.JobID, req.IdempotencyKey, "render/"+a.Job.JobID, pin, workflow); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresPreCreateAbandonCannotHideLostCreateResponse(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	a := admitTestJob(t, s, "create-intent", "project", "user", "admit")
	activateTestPin(t, s, pool, a.Job)
	attempt, err := s.ClaimExecution(ctx, ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "create", StagingToken: "stage", PrivateStagingPath: "/private/stage"})
	if err != nil {
		t.Fatal(err)
	}
	intent := IntentInput{JobID: a.Job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: IntentContainerCreate, IdempotencyKey: "create", Request: []byte(`{"name":"stable-container"}`)}
	if _, err = s.SaveIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err = s.AbandonUncreatedAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("lost create response abandoned: %v", err)
	}
	if _, err = pool.Exec(ctx, `DELETE FROM generation_operation_intents WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if err = s.AbandonUncreatedAttempt(ctx, attempt.AttemptID, attempt.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SaveIntent(ctx, intent); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("abandoned owner created intent: %v", err)
	}
}

func TestPostgresCandidateCannotRewriteSavedFinalizeReceipt(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "receipt-proof", "project", "user", "admit")
	attempt := finishTestAttempt(t, s, pool, a.Job, "receipt")
	lease, candidate := createTestCandidate(t, s, a.Job, attempt, "publisher", "original")
	candidate.CandidateID = ""
	candidate.Artifact.ArtifactID = "invented"
	candidate.Artifact.Hash = "invented-hash"
	candidate.ArtifactBytes, _ = json.Marshal(candidate.Artifact)
	if err := s.RegisterCandidate(context.Background(), a.Job.JobID, "publisher", lease.Epoch, candidate); !errors.Is(err, ErrConflict) {
		t.Fatalf("candidate changed saved receipt: %v", err)
	}
}

func TestPostgresExpiryRequiresDatabaseDeadline(t *testing.T) {
	s, _ := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "early-expiry", "project", "user", "admit")
	if err := s.ExpireJob(context.Background(), TerminalInput{JobID: a.Job.JobID, FailureCode: "deadline"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expired before DB deadline: %v", err)
	}
}

func TestPostgresPendingAcquireBlocksHistoryDeletion(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	a := admitTestJob(t, s, "pending-retention", "project", "user", "admit")
	req := storagev1.AcquirePinRequest{Scope: a.Job.Scope, ObjectID: a.Job.SourceSnapshotID, HolderID: s.cfg.PinHolderID, HolderDomain: s.cfg.PinHolderDomain, RetentionPolicy: "ordinary", IdempotencyKey: "lost-response"}
	if _, err := s.BeginPinAcquireIntent(ctx, PinAcquireIntent{JobID: a.Job.JobID, IdempotencyKey: req.IdempotencyKey, Request: req}); err != nil {
		t.Fatal(err)
	}
	if err := s.FailJob(ctx, TerminalInput{JobID: a.Job.JobID, FailureCode: "infrastructure"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE generation_jobs SET terminal_at=clock_timestamp()-interval '31 days' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE generation_admission_receipts SET expires_at=clock_timestamp()-interval '1 day' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform_event_outbox SET sent_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	result, err := s.Retain(ctx)
	if err != nil || result.JobsDeleted != 0 {
		t.Fatalf("lost pin acquire was deleted: %+v %v", result, err)
	}
}

func TestPostgresCreateIntentMustMatchClaimedKey(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	a := admitTestJob(t, s, "create-key", "project", "user", "admit")
	activateTestPin(t, s, pool, a.Job)
	attempt, err := s.ClaimExecution(ctx, ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "recorded-key", StagingToken: "token", PrivateStagingPath: "/private/stage"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SaveIntent(ctx, IntentInput{JobID: a.Job.JobID, RenderAttemptID: attempt.AttemptID, AttemptEpoch: attempt.FencingEpoch, Kind: IntentContainerCreate, IdempotencyKey: "different-key", Request: []byte(`{"name":"container"}`)})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("create recovery key diverged: %v", err)
	}
}

func TestPostgresCleanupClaimEpochAndDiagnosticsRetention(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	a := admitTestJob(t, s, "cleanup-epoch", "project", "user", "admit")
	if err := s.FailJob(ctx, TerminalInput{JobID: a.Job.JobID, FailureCode: "compile_failed", Diagnostics: json.RawMessage(`[{"severity":"error","message":"source-secret"}]`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE generation_jobs SET terminal_at=clock_timestamp()-interval '25 hours' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO generation_cleanup_intents(job_id,kind,idempotency_key,request,state) VALUES($1,'staging.remove','stage',$2,'pending')`, a.Job.JobID, []byte(`{"token":"stage"}`)); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimCleanup(ctx, "same-worker", 10, 5*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim: %v %v", first, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE generation_cleanup_intents SET lease_until=clock_timestamp()-interval '1 second' WHERE cleanup_id=$1`, first[0].ID); err != nil {
		t.Fatal(err)
	}
	second, err := s.ClaimCleanup(ctx, "same-worker", 10, 5*time.Second)
	if err != nil || len(second) != 1 {
		t.Fatalf("reclaim: %v %v", second, err)
	}
	if err = s.CompleteCleanup(ctx, first[0].ID, "same-worker", first[0].LeaseEpoch, []byte(`{"removed":true}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale same-worker cleanup ACK: %v", err)
	}
	if err = s.CompleteCleanup(ctx, second[0].ID, "same-worker", second[0].LeaseEpoch, []byte(`{"removed":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Retain(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := s.GetJob(ctx, a.Job.JobID)
	if err != nil || len(job.Diagnostics) != 0 {
		t.Fatalf("structured diagnostics retained: %+v %v", job.Diagnostics, err)
	}
}

func TestPostgresEmptyContainerCannotConfirmStopped(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	a := admitTestJob(t, s, "unknown-container", "project", "user", "admit")
	activateTestPin(t, s, pool, a.Job)
	attempt, err := s.ClaimExecution(context.Background(), ClaimAttemptInput{JobID: a.Job.JobID, CreateContainerIntentKey: "create", StagingToken: "stage", PrivateStagingPath: "/private/stage"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptStopped(context.Background(), attempt.AttemptID, attempt.FencingEpoch, ""); err == nil {
		t.Fatal("unknown container released execution slot")
	}
	var reserved bool
	if err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM generation_reservations WHERE job_id=$1 AND reservation_type='execution')`, a.Job.JobID).Scan(&reserved); err != nil || !reserved {
		t.Fatalf("execution slot lost: %v", err)
	}
}
