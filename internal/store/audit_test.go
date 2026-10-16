package store

import (
	"context"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"github.com/leafrun/leafrun-platform/events"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuditContextRequiresCompleteExactCanonicalHierarchy(t *testing.T) {
	id := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	for _, scope := range []accessv1.PermissionScope{{WorkspaceID: "w", ProjectID: "p"}, {OrganizationID: "o", WorkspaceID: "foreign", ProjectID: "p"}, {OrganizationID: "o", WorkspaceID: "w", ProjectID: "foreign"}} {
		if validateAuditContext(id, scope, "w", "p") == nil {
			t.Fatal("accepted incomplete or foreign canonical scope")
		}
	}
	if err := validateAuditContext(id, accessv1.PermissionScope{OrganizationID: "o", WorkspaceID: "w", ProjectID: "p"}, "w", "p"); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		v, err := newAuditID()
		if err != nil || !events.ValidAuditID(v) {
			t.Fatal("audit ID is not server UUIDv4")
		}
	}
}

func TestPostgresAuditAdmissionReplayAndTerminalOriginalContext(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	original := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "original", Version: 1}}
	full := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: "workspace", ProjectID: "project"}
	state.SetAuditScopeGate(func(context.Context, AuthorityBinding, string) (accessv1.PermissionScope, error) { return full, nil })
	in := testAdmissionInput(t, testRequest("workspace", "project", "1"), "workspace", "project", "initiator", "secret-caller-key")
	in.Identity = &original
	a, _, err := state.Admit(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := original
	refreshed.Authority.ID = "replacement"
	in.Identity = &refreshed
	if _, replay, err := state.Admit(context.Background(), in); err != nil || !replay {
		t.Fatalf("replay %v %v", replay, err)
	}
	if err = state.FailJob(context.Background(), TerminalInput{JobID: a.Job.JobID, Phase: "render", FailureCode: "render_failed"}); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(context.Background(), `SELECT subject,encoded_envelope FROM platform_event_outbox WHERE subject='leafrun.audit.generation.action.v1' ORDER BY created_at,event_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	operation := ""
	for rows.Next() {
		var subject string
		var raw []byte
		if err = rows.Scan(&subject, &raw); err != nil {
			t.Fatal(err)
		}
		env, action, e := events.DecodeAuditAction(subject, raw)
		if e != nil {
			t.Fatal(e)
		}
		if action.Actor != original.Principal || action.Authority != original.Authority || env.OrganizationID != full.OrganizationID || action.Revision == "" {
			t.Fatal("lost original context")
		}
		if count == 0 {
			operation = action.OperationID
		}
		if action.OperationID != operation || action.OperationID == in.IdempotencyKey {
			t.Fatal("operation rebound")
		}
		count++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("want admission and terminal once, got %d", count)
	}
}

func TestPostgresAuditLegacyExpiryUsesTruthfulServiceContext(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	old := identityv1.Identity{Principal: identityv1.Principal{Subject: "revoked-human", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "revoked-session", Version: 1}}
	input := testAdmissionInput(t, testRequest("legacy-workspace", "project", "1"), "legacy-workspace", "project", "initiator", "key")
	input.Identity = &old
	admission, _, err := state.Admit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	full := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: "legacy-workspace", ProjectID: "project"}
	state.SetAuditScopeGate(func(context.Context, AuthorityBinding, string) (accessv1.PermissionScope, error) { return full, nil })
	state.SetAuditCleanupGate(func(_ context.Context, b AuthorityBinding) (identityv1.Identity, accessv1.PermissionScope, error) {
		if b.Scope.WorkspaceID != full.WorkspaceID || b.Scope.ProjectID != full.ProjectID {
			t.Fatal("lookup widened target")
		}
		return identityv1.Identity{Principal: identityv1.Principal{Subject: "leafrun-generation", Domain: "service"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindService, ID: "generation-cleanup", Version: 1}}, full, nil
	})
	if _, err = pool.Exec(ctx, `UPDATE generation_jobs SET deadline=clock_timestamp()-interval '1 second' WHERE job_id=$1`, admission.Job.JobID); err != nil {
		t.Fatal(err)
	}
	if count, err := state.ExpireDue(ctx, 10); err != nil || count != 1 {
		t.Fatalf("legacy cleanup stranded: count %d error %v", count, err)
	}
	var raw []byte
	if err = pool.QueryRow(ctx, `SELECT encoded_envelope FROM platform_event_outbox WHERE subject='leafrun.audit.generation.action.v1'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	_, action, err := events.DecodeAuditAction("leafrun.audit.generation.action.v1", raw)
	if err != nil {
		t.Fatal(err)
	}
	if action.Actor.Domain != "service" || action.Actor.Subject != "leafrun-generation" || action.Authority.Kind != identityv1.KindService || action.Actor == old.Principal {
		t.Fatalf("cleanup impersonated original human: %+v", action)
	}
	var reservations int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM generation_reservations WHERE job_id=$1`, admission.Job.JobID).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("legacy queue reservation retained %d %v", reservations, err)
	}
}

func TestPostgresAuditEnqueueFailureRollsBackAdmissionAndTerminal(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	ctx := context.Background()
	full := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: "workspace", ProjectID: "project"}
	state.SetAuditScopeGate(func(context.Context, AuthorityBinding, string) (accessv1.PermissionScope, error) { return full, nil })
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	in := testAdmissionInput(t, testRequest("workspace", "project", "1"), "workspace", "project", "initiator", "key")
	in.Identity = &identity
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_audit()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.subject='leafrun.audit.generation.action.v1' THEN RAISE EXCEPTION 'audit blocked';END IF;RETURN NEW;END$$;CREATE TRIGGER reject_audit BEFORE INSERT ON platform_event_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.Admit(ctx, in); err == nil {
		t.Fatal("admitted without audit outbox")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM generation_jobs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("job committed without audit %d %v", count, err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE platform_event_outbox DISABLE TRIGGER reject_audit`); err != nil {
		t.Fatal(err)
	}
	a, _, err := state.Admit(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `ALTER TABLE platform_event_outbox ENABLE TRIGGER reject_audit`); err != nil {
		t.Fatal(err)
	}
	if err = state.FailJob(ctx, TerminalInput{JobID: a.Job.JobID, Phase: "render", FailureCode: "render_failed"}); err == nil {
		t.Fatal("terminal committed without audit")
	}
	job, err := state.GetJob(ctx, a.Job.JobID)
	if err != nil || job.Status != "queued" {
		t.Fatalf("terminal did not rollback %s %v", job.Status, err)
	}
}

func TestPostgresAuditOutboxWaitPrecedesFinalAdmissionAuthority(t *testing.T) {
	state, pool := openStoreTest(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	full := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: "workspace", ProjectID: "project"}
	state.SetAuditScopeGate(func(context.Context, AuthorityBinding, string) (accessv1.PermissionScope, error) { return full, nil })
	var revoked atomic.Bool
	state.SetRequestAuthorityGate(func(context.Context, AuthorityBinding, bool) error {
		if revoked.Load() {
			return ErrAuthorityDenied
		}
		return nil
	})
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	in := testAdmissionInput(t, testRequest("workspace", "project", "1"), "workspace", "project", "initiator", "key")
	in.Identity = &identity
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(ctx, `LOCK TABLE platform_event_outbox IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, _, e := state.Admit(ctx, in); done <- e }()
	until := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND relation='platform_event_outbox'::regclass)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(until) {
			t.Fatal("admission never reached outbox wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
	revoked.Store(true)
	if err = lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != ErrAuthorityDenied {
		t.Fatalf("stale admission authorization escaped %v", err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM generation_jobs)+(SELECT count(*) FROM platform_event_outbox)`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("admission escaped audit wait: %d %v", count, err)
	}
}
