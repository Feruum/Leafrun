package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The new remote authority gate is a real bounded wait after the earlier clock
// checks. An allow must not commit a publication whose fence expired meanwhile.
func TestE06TerminalRechecksClockAfterAuthorityWait(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"publication-lease", `UPDATE generation_jobs SET publication_lease_until=clock_timestamp()+interval '1200 milliseconds' WHERE job_id=$1 RETURNING publication_lease_until`},
		{"job-deadline", `UPDATE generation_jobs SET deadline=clock_timestamp()+interval '1200 milliseconds' WHERE job_id=$1 RETURNING deadline`},
		{"candidate", `UPDATE generation_publication_candidates SET expires_at=clock_timestamp()+interval '1200 milliseconds' WHERE candidate_id=$1 RETURNING expires_at`},
		{"receipt", `UPDATE generation_admission_receipts SET expires_at=clock_timestamp()+interval '1200 milliseconds' WHERE job_id=$1 RETURNING expires_at`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, pool := openStoreTest(t, Config{})
			admission := admitTestJob(t, state, "e06-clock", "project", "user", "key")
			attempt := finishTestAttempt(t, state, pool, admission.Job, "e06-clock")
			lease, candidate := createTestCandidate(t, state, admission.Job, attempt, "publisher", "artifact-clock")
			key := admission.Job.JobID
			if tc.name == "candidate" {
				key = candidate.CandidateID
			}
			var expires time.Time
			if err := pool.QueryRow(context.Background(), tc.sql, key).Scan(&expires); err != nil {
				t.Fatal(err)
			}
			entered := false
			state.SetAuthorityGate(func(ctx context.Context, binding AuthorityBinding, operation string) error {
				entered = true
				var now time.Time
				if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
					return err
				}
				if !now.Before(expires) {
					return errors.New("probe inconclusive: boundary expired before authority wait")
				}
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for now.Before(expires.Add(20 * time.Millisecond)) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
					}
					if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
						return err
					}
				}
				return nil
			})
			err := state.TerminalSuccess(context.Background(), successInput(admission.Job.JobID, "publisher", lease, candidate))
			if !entered {
				t.Fatal("probe did not reach the final authority wait")
			}
			if err == nil {
				t.Fatalf("expired %s committed terminal success after authority wait", tc.name)
			}
			job, getErr := state.GetJob(context.Background(), admission.Job.JobID)
			if getErr != nil || job.Artifact != nil || string(job.Status) == "succeeded" {
				t.Fatalf("expired result was published: status=%s error=%v", job.Status, getErr)
			}
			var events int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_terminal_outbox WHERE job_id=$1`, admission.Job.JobID).Scan(&events); err != nil || events != 0 {
				t.Fatalf("expired terminal event escaped: events=%d error=%v", events, err)
			}
		})
	}
	t.Run("fresh-positive-control", func(t *testing.T) {
		state, pool := openStoreTest(t, Config{})
		admission := admitTestJob(t, state, "e06-clock-control", "project", "user", "key")
		attempt := finishTestAttempt(t, state, pool, admission.Job, "e06-clock-control")
		lease, candidate := createTestCandidate(t, state, admission.Job, attempt, "publisher", "artifact-clock-control")
		calls := 0
		state.SetAuthorityGate(func(context.Context, AuthorityBinding, string) error { calls++; return nil })
		if err := state.TerminalSuccess(context.Background(), successInput(admission.Job.JobID, "publisher", lease, candidate)); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("positive-control gate calls=%d", calls)
		}
	})
}
