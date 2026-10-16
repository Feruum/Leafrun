package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPostgresAdmissionAuthorityAfterQueueWait(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	var revoked atomic.Bool
	var calls atomic.Int32
	state.SetRequestAuthorityGate(func(ctx context.Context, b AuthorityBinding, replay bool) error {
		calls.Add(1)
		if replay {
			t.Error("new admission treated as replay")
		}
		if revoked.Load() {
			return ErrAuthorityDenied
		}
		return nil
	})
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err = lockQueue(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		_, _, e := state.Admit(context.Background(), testAdmissionInput(t, testRequest("admission-wait", "project", "1"), "admission-wait", "project", "alice", "key"))
		finished <- e
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("admission did not wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 0 {
		t.Fatal("authority checked before wait")
	}
	revoked.Store(true)
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-finished; !errors.Is(err, ErrAuthorityDenied) {
		t.Fatalf("admission: %v", err)
	}
	for _, table := range []string{"generation_jobs", "generation_reservations", "generation_admission_receipts"} {
		var n int
		if err = pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s escaped rollback: %d %v", table, n, err)
		}
	}
}

func TestPostgresAdmissionReplayUsesRequestGate(t *testing.T) {
	state, _ := openStoreTest(t, Config{})
	in := testAdmissionInput(t, testRequest("replay-current", "project", "1"), "replay-current", "project", "alice", "key")
	first, _, err := state.Admit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	state.SetAuthorityGate(func(context.Context, AuthorityBinding, string) error {
		t.Error("original execution gate used for receipt")
		return ErrAuthorityDenied
	})
	var calls int
	state.SetRequestAuthorityGate(func(ctx context.Context, b AuthorityBinding, replay bool) error {
		calls++
		if !replay || b.JobID != first.Job.JobID {
			t.Error("wrong replay binding")
		}
		return nil
	})
	if _, replay, e := state.Admit(context.Background(), in); e != nil || !replay {
		t.Fatalf("replay %v %v", replay, e)
	}
	if _, replay, e := state.LookupAdmission(context.Background(), in.WorkspaceID, in.InitiatorID, in.IdempotencyKey, in.Fingerprint); e != nil || !replay {
		t.Fatalf("lookup %v %v", replay, e)
	}
	if calls != 2 {
		t.Fatalf("request checks %d", calls)
	}
}

func TestPostgresCancelFinalCurrentGateRollsBackAllEffects(t *testing.T) {
	s, p := openStoreTest(t, Config{})
	admitted := admitTestJob(t, s, "cancel-current", "project", "alice", "key")
	s.SetRequestAuthorityGate(func(ctx context.Context, b AuthorityBinding, replay bool) error {
		if b.CurrentOperation != "generation.cancel" || b.JobID != admitted.Job.JobID {
			t.Error("wrong current cancellation binding")
		}
		return ErrAuthorityDenied
	})
	if _, err := s.RequestCancel(context.Background(), admitted.Job.JobID); !errors.Is(err, ErrAuthorityDenied) {
		t.Fatalf("cancel gate %v", err)
	}
	job, err := s.GetJob(context.Background(), admitted.Job.JobID)
	if err != nil || job.Status != "queued" {
		t.Fatalf("cancellation escaped rollback %+v %v", job, err)
	}
	var events, reservations int
	if err = p.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM generation_terminal_outbox),(SELECT count(*) FROM generation_reservations)`).Scan(&events, &reservations); err != nil || events != 0 || reservations != 1 {
		t.Fatalf("cancel effects %d %d %v", events, reservations, err)
	}
}

func TestPostgresDeferredWorkflowUsesOriginalAuthority(t *testing.T) {
	for _, phase := range []string{"claim", "start"} {
		t.Run(phase, func(t *testing.T) {
			s, p := openStoreTest(t, Config{})
			admitted := admitTestJob(t, s, "workflow-current", "project", "alice", "key")
			activateTestPin(t, s, p, admitted.Job)
			if _, err := p.Exec(context.Background(), `UPDATE generation_workflow_starts SET state='pending',lease_owner='',lease_until=NULL WHERE job_id=$1`, admitted.Job.JobID); err != nil {
				t.Fatal(err)
			}
			s.SetAuthorityGate(func(ctx context.Context, b AuthorityBinding, op string) error {
				if b.JobID != admitted.Job.JobID || op != "generation.execute" {
					t.Error("wrong original workflow binding")
				}
				return ErrAuthorityDenied
			})
			var err error
			if phase == "claim" {
				_, err = s.ClaimWorkflowStarts(context.Background(), "dispatcher", 1, 5*time.Second)
			} else {
				err = s.AuthorizeWorkflowStart(context.Background(), admitted.Job.JobID)
			}
			if !errors.Is(err, ErrAuthorityDenied) {
				t.Fatalf("workflow %s: %v", phase, err)
			}
			job, e := s.GetJob(context.Background(), admitted.Job.JobID)
			if e != nil || job.Status != "failed" || job.FailureCode != "access_denied" {
				t.Fatalf("denied workflow retained active work %+v %v", job, e)
			}
		})
	}
}
