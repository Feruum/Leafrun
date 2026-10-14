package store

import (
	"context"
	"errors"
	"testing"
)

func TestPostgresLookupAdmissionOnlyReturnsLiveExactReceipts(t *testing.T) {
	s, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	in := testAdmissionInput(t, testRequest("lookup", "project", "1"), "lookup", "project", "actor", "lookup-key")
	a, _, err := s.Admit(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := s.LookupAdmission(ctx, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey, in.Fingerprint)
	if err != nil || !found || got.Job.JobID != a.Job.JobID {
		t.Fatalf("lookup: %+v %v %v", got, found, err)
	}
	if _, _, err = s.LookupAdmission(ctx, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey, "changed"); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed fingerprint: %v", err)
	}
	if _, found, err = s.LookupAdmission(ctx, in.WorkspaceID, "another-actor", in.IdempotencyKey, in.Fingerprint); err != nil || found {
		t.Fatalf("other actor: %v %v", found, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE generation_admission_receipts SET expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1`, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, found, err = s.LookupAdmission(ctx, in.WorkspaceID, in.InitiatorID, in.IdempotencyKey, "changed"); err != nil || found {
		t.Fatalf("expired: %v %v", found, err)
	}
}
