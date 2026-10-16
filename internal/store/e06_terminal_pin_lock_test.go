package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestE06TerminalRechecksAuthorityAfterPinLock(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	admission := admitTestJob(t, state, "e06-late-lock", "project", "user", "key")
	attempt := finishTestAttempt(t, state, pool, admission.Job, "e06-late-lock")
	lease, candidate := createTestCandidate(t, state, admission.Job, attempt, "publisher", "artifact-late-lock")
	var revoked atomic.Bool
	var calls atomic.Int32
	state.SetAuthorityGate(func(context.Context, AuthorityBinding, string) error {
		calls.Add(1)
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
	var number int64
	if err := tx.QueryRow(context.Background(), `SELECT acquire_number FROM generation_pin_acquires WHERE job_id=$1 AND response IS NOT NULL AND state<>'released' LIMIT 1 FOR UPDATE`, admission.Job.JobID).Scan(&number); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- state.TerminalSuccess(ctx, successInput(admission.Job.JobID, "publisher", lease, candidate))
	}()
	stop := time.Now().Add(6 * time.Second)
	for {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.datname=current_database() AND a.wait_event_type='Lock' AND a.query LIKE '%generation_pin_acquires%FOR UPDATE%' AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.relation='generation_pin_acquires'::regclass))`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(stop) {
			t.Fatal("terminal did not reach an observed lock wait on its exact owned pin table")
		}
		time.Sleep(10 * time.Millisecond)
	}
	callsBeforeUnlock := calls.Load()
	revoked.Store(true)
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = <-done
	if !errors.Is(err, ErrAuthorityDenied) {
		t.Fatalf("revocation after late pin lock was ignored: err=%v gate_calls_before_unlock=%d gate_calls_total=%d", err, callsBeforeUnlock, calls.Load())
	}
	job, err := state.GetJob(context.Background(), admission.Job.JobID)
	if err != nil || job.Artifact != nil || string(job.Status) == "succeeded" {
		t.Fatalf("revoked publication escaped rollback: status=%s err=%v", job.Status, err)
	}
}
