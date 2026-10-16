package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

func TestPendingOriginalAuthorityRevocationAndLostAcquireCleanup(t *testing.T) {
	state, pool := openHTTPStoreTestWithPool(t)
	principal := storagev1.Principal{Subject: "alice", Domain: "user"}
	initiator, _ := EncodeInitiator(principal)
	original := identityv1.Identity{Principal: principal, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "original", Version: 1}}
	selection := generationv1.RenderSelectionRequest{Scope: storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}, ReleaseID: "release-1"}
	_, selectionBytes, selectionFingerprint, err := generationv1.NormalizeSelection(mustJSON(t, selection))
	if err != nil {
		t.Fatal(err)
	}
	resolved := validResolvedSelection(t, selection, selectionFingerprint, principal)
	executionBytes, fingerprint, err := store.NormalizeResolvedExecution(resolved)
	if err != nil {
		t.Fatal(err)
	}
	admitted, _, err := state.Admit(context.Background(), store.AdmitInput{Identity: &original, WorkspaceID: selection.Scope.WorkspaceID, ProjectID: selection.Scope.ProjectID, InitiatorID: initiator, Actor: principal, IdempotencyKey: "pending-original", NormalizedRequest: executionBytes, Parameters: resolved.Prepared.JSON, Fingerprint: fingerprint, SelectionRequest: selectionBytes, SelectionFingerprint: selectionFingerprint, ResolvedExecution: &resolved, EngineVersion: resolved.Descriptor.TypstVersion, SnapshotID: resolved.SnapshotID, SourceContentID: resolved.ContentID, TargetID: resolved.Descriptor.TargetID, Entrypoint: resolved.Descriptor.Entrypoint})
	if err != nil {
		t.Fatal(err)
	}
	access := &testAccess{allowed: true, denied: map[string]bool{"source.read": true}}
	pins := &testPins{loseFirstAck: true}
	guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
	server, err := New(state, state.Owner(), Config{AuditCleanupAuthority: identityv1.AuthorityRef{Kind: identityv1.KindService, ID: "fixture-cleanup", Version: 1}, AuthMode: "access", Authenticator: publicAuthenticator{}, BrowserGuard: guard, Access: access, Pins: pins, Projects: &selectionProjects{current: projectsv1.CurrentReleaseDecision{Allowed: true}}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err != nil {
		t.Fatal(err)
	}
	state.SetAuthorityGate(server.durableAuthorityGate)
	if err = server.EnsureSourcePin(context.Background(), admitted.Job); err == nil {
		t.Fatal("lost acquire response did not remain pending")
	}
	if len(pins.acquireKeys) != 1 {
		t.Fatal("pin acquire missing")
	}
	originalKey := pins.acquireKeys[0]
	access.denied["generation.execute"] = true
	refreshed := original
	refreshed.Authority.ID = "refreshed"
	ctx, _ := authn.WithIdentity(context.Background(), refreshed)
	if err = server.EnsureSourcePin(ctx, admitted.Job); !errors.Is(err, store.ErrAuthorityDenied) {
		t.Fatalf("revoked original revived: %v", err)
	}
	job, err := state.GetJob(context.Background(), admitted.Job.JobID)
	if err != nil || job.Status != generationv1.Failed {
		t.Fatalf("pending denial did not fail durably: %s %v", job.Status, err)
	}
	if err = server.recoverUnknownPins(ctx); err != nil {
		t.Fatal(err)
	}
	if len(pins.acquireKeys) != 2 || pins.acquireKeys[1] != originalKey || pins.activateCalls != 0 {
		t.Fatalf("cleanup rebound/activated: keys=%v activations=%d", pins.acquireKeys, pins.activateCalls)
	}
	var releases int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_cleanup_intents WHERE job_id=$1 AND kind='pin.release'`, job.JobID).Scan(&releases); err != nil || releases != 1 {
		t.Fatalf("lost pin not durably scheduled for release: %d %v", releases, err)
	}
}
