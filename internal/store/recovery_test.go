package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgresRecoveryClaimsOnlyStaleAttemptsAndFindsExactIntents(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	job := admitTestJob(t, s, "recover", "project", "actor", "recover-key").Job
	activateTestPin(t, s, pool, job)
	a, err := s.ClaimExecution(ctx, ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: "create-key", StagingToken: "stage-test", PrivateStagingPath: "/private/stage-test"})
	if err != nil {
		t.Fatal(err)
	}
	in := IntentInput{JobID: job.JobID, RenderAttemptID: a.AttemptID, AttemptEpoch: a.FencingEpoch, Kind: IntentContainerCreate, IdempotencyKey: a.CreateContainerIntentKey, Request: []byte(`{"create":true}`)}
	if _, err = s.SaveIntent(ctx, in); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ClaimStaleAttempt(ctx, a.AttemptID); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stole live attempt: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, a.AttemptID); err != nil {
		t.Fatal(err)
	}
	stale, err := s.ReapableAttempts(ctx, 100)
	if err != nil || len(stale) != 1 {
		t.Fatalf("stale list: %+v %v", stale, err)
	}
	claimed, err := s.ClaimStaleAttempt(ctx, a.AttemptID)
	if err != nil || claimed.FencingEpoch != a.FencingEpoch+1 {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	found, err := s.FindIntent(ctx, job.JobID, a.AttemptID, IntentContainerCreate, a.CreateContainerIntentKey)
	if err != nil || string(found.Request) != string(in.Request) {
		t.Fatalf("lost prior intent: %+v %v", found, err)
	}
	if _, err = s.CompleteIntent(ctx, in, []byte(`{"id":"late"}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	if err = s.AbandonAttemptIntents(ctx, a.AttemptID, claimed.FencingEpoch); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("abandoned unknown live effect: %v", err)
	}
	if err = s.RecordRecoveredContainer(ctx, a.AttemptID, claimed.FencingEpoch, a.CreateContainerIntentKey, "recovered-container"); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptStopped(ctx, a.AttemptID, claimed.FencingEpoch, "recovered-container"); err != nil {
		t.Fatal(err)
	}
	if err = s.RetryStoppedAttempt(ctx, a.AttemptID, claimed.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	cleanup, err := s.PendingCleanup(ctx, job.JobID, 100)
	if err != nil {
		t.Fatal(err)
	}
	foundCleanup := false
	for _, item := range cleanup {
		if item.Kind == "attempt.files" && item.IdempotencyKey == a.AttemptID {
			foundCleanup = true
		}
	}
	if !foundCleanup {
		t.Fatal("retry committed without durable file cleanup")
	}
	if err = s.AbandonAttemptIntents(ctx, a.AttemptID, claimed.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	found, err = s.FindIntent(ctx, job.JobID, a.AttemptID, IntentContainerCreate, a.CreateContainerIntentKey)
	if err != nil || found.AbandonedAt == nil {
		t.Fatalf("abandonment missing: %+v %v", found, err)
	}
}

func TestPostgresWorkflowClaimsDiscardClosedAndExpiredJobs(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	for _, kind := range []string{"canceled", "expired"} {
		job := admitTestJob(t, s, "claims-"+kind, "project", "actor", kind).Job
		activateTestPin(t, s, pool, job)
		if _, err := pool.Exec(ctx, `UPDATE generation_workflow_starts SET state='pending',acknowledged_at=NULL,lease_until=NULL,lease_owner='' WHERE job_id=$1`, job.JobID); err != nil {
			t.Fatal(err)
		}
		if kind == "canceled" {
			if _, err := s.RequestCancel(ctx, job.JobID); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := pool.Exec(ctx, `UPDATE generation_jobs SET deadline=clock_timestamp()-interval '1 second' WHERE job_id=$1`, job.JobID); err != nil {
				t.Fatal(err)
			}
		}
	}
	claims, err := s.ClaimWorkflowStarts(ctx, "dispatcher", 100, 30*time.Second)
	if err != nil || len(claims) > 0 {
		t.Fatalf("started closed/expired jobs: %+v %v", claims, err)
	}
	pending, err := s.ListPendingWorkflowStarts(ctx, 100)
	if err != nil || len(pending) > 0 {
		t.Fatalf("stale start intents block history: %+v %v", pending, err)
	}
}

func TestPostgresRetentionPurgesAcknowledgedEventHistoryOnly(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO platform_event_outbox(event_id,subject,encoded_envelope,payload_hash,created_at,sent_at) VALUES('resolved','leafrun.generation.test','{}'::bytea,'x'::bytea,clock_timestamp()-interval '32 days',clock_timestamp()-interval '31 days'),('pending','leafrun.generation.test','{}'::bytea,'x'::bytea,clock_timestamp()-interval '32 days',NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retain(ctx); err != nil {
		t.Fatal(err)
	}
	var resolved, pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE event_id='resolved'),count(*) FILTER(WHERE event_id='pending') FROM platform_event_outbox`).Scan(&resolved, &pending); err != nil {
		t.Fatal(err)
	}
	if resolved != 0 || pending != 1 {
		t.Fatalf("event retention resolved=%d pending=%d", resolved, pending)
	}
}

func TestPostgresFailureClassificationSurvivesDeadlineButFencesStaleOwners(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	job := admitTestJob(t, s, "classify", "project", "actor", "classify").Job
	activateTestPin(t, s, pool, job)
	a, err := s.ClaimExecution(ctx, ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: "create", StagingToken: "private", PrivateStagingPath: "/private"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE generation_jobs SET deadline=clock_timestamp()-interval '1 second' WHERE job_id=$1`, job.JobID); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordAttemptFailureClassification(ctx, a.AttemptID, a.FencingEpoch, "resource_limit", "resource_limit"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE generation_attempts SET owner_lease_until=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, a.AttemptID); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimStaleAttempt(ctx, a.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordAttemptFailureClassification(ctx, a.AttemptID, a.FencingEpoch, "resource_limit", "resource_limit"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale marker accepted: %v", err)
	}
	if _, err = s.ExpireDue(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordAttemptFailureClassification(ctx, a.AttemptID, claimed.FencingEpoch, "resource_limit", "resource_limit"); err != nil {
		t.Fatal("expired renderer cannot finish cleanup classification:", err)
	}
}
