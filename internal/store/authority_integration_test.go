package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
)

func TestPostgresOriginalAuthoritySurvivesSameActorReplay(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	original := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "original-session", Version: 1}}
	in := testAdmissionInput(t, testRequest("authority-workspace", "project", "1"), "authority-workspace", "project", "initiator", "same-key")
	in.Identity = &original
	first, _, err := state.Admit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := original
	refreshed.Authority.ID = "refreshed-session"
	in.Identity = &refreshed
	replay, reused, err := state.Admit(context.Background(), in)
	if err != nil || !reused || replay.Job.JobID != first.Job.JobID {
		t.Fatalf("replay %v %v", reused, err)
	}
	saved, err := state.LoadAuthority(context.Background(), first.Job.JobID)
	if err != nil || saved.Identity == nil || *saved.Identity != original {
		t.Fatalf("original rebound: %+v %v", saved, err)
	}
	if err = state.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = state.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE generation_schema_migrations SET checksum='tampered' WHERE version=3`); err != nil {
		t.Fatal(err)
	}
	if err = state.Ready(context.Background()); err == nil {
		t.Fatal("tampered migration ready")
	}
	if err = state.Migrate(context.Background()); err == nil {
		t.Fatal("tampered migration accepted")
	}
}

func TestPostgresAuthorityReadinessRequiresAllColumns(t *testing.T) {
	for _, column := range []string{"original_identity", "authority_release_id", "authority_descriptor_digest"} {
		t.Run(column, func(t *testing.T) {
			s, p := openStoreTest(t, Config{})
			if _, err := p.Exec(context.Background(), "ALTER TABLE generation_jobs DROP COLUMN "+column); err != nil {
				t.Fatal(err)
			}
			if err := s.Ready(context.Background()); err == nil {
				t.Fatal("missing authority column reported ready")
			}
		})
	}
}

func TestPostgresTerminalAuthorityGateAfterRowLockWait(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	admission := admitTestJob(t, state, "authority-lock", "project", "user", "key")
	attempt := finishTestAttempt(t, state, pool, admission.Job, "authority")
	lease, candidate := createTestCandidate(t, state, admission.Job, attempt, "publisher", "artifact-authority")
	var revoked atomic.Bool
	var calls atomic.Int32
	state.SetAuthorityGate(func(ctx context.Context, binding AuthorityBinding, operation string) error {
		calls.Add(1)
		if binding.JobID != admission.Job.JobID || binding.Scope != admission.Job.Scope || operation != "generation.publish" {
			return errors.New("wrong locked binding")
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
	if _, err = tx.Exec(context.Background(), `SELECT job_id FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, admission.Job.JobID); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		finished <- state.TerminalSuccess(context.Background(), successInput(admission.Job.JobID, "publisher", lease, candidate))
	}()
	// Use pg_stat_activity to prove the call reached a lock wait, not a timer.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		err = pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%publication_owner%FOR UPDATE%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal never waited on row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 0 {
		t.Fatal("gate ran before row lock")
	}
	revoked.Store(true)
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-finished; !errors.Is(err, ErrAuthorityDenied) {
		t.Fatalf("revoked terminal: %v", err)
	}
	job, err := state.GetJob(context.Background(), admission.Job.JobID)
	if err != nil || job.Status != generationv1.Publishing || job.Artifact != nil {
		t.Fatalf("publication escaped rollback: %+v %v", job, err)
	}
	var events int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_terminal_outbox WHERE job_id=$1`, job.JobID).Scan(&events); err != nil || events != 0 {
		t.Fatalf("terminal event escaped: %d %v", events, err)
	}
}
