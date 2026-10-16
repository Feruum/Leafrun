package service

import (
	"context"
	"github.com/Feruum/Leafrun/internal/store"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"testing"
)

type cleanupScopeProbe struct {
	AccessChecker
	request accessv1.CheckRequest
	scope   accessv1.PermissionScope
	err     error
}

func (p *cleanupScopeProbe) CheckScope(_ context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	p.request = in
	return accessv1.CheckResponse{Allowed: true, CanonicalScope: &p.scope}, p.err
}
func TestAuditCleanupScopeUsesOnlyFixedServiceAndExactOwnedTarget(t *testing.T) {
	ref := identityv1.AuthorityRef{Kind: identityv1.KindService, ID: "configured-generation-cleanup", Version: 4}
	binding := store.AuthorityBinding{JobID: "owned-job", Scope: storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}, Identity: &identityv1.Identity{Principal: identityv1.Principal{Subject: "revoked", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "expired", Version: 1}}}
	probe := &cleanupScopeProbe{scope: accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: "workspace", ProjectID: "project"}}
	server := &Server{cfg: Config{Access: probe, AuditCleanupAuthority: ref}}
	identity, scope, err := server.auditCleanupGate(context.Background(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Principal != (identityv1.Principal{Subject: "leafrun-generation", Domain: "service"}) || identity.Authority != ref || scope != probe.scope || probe.request.Principal != identity.Principal || probe.request.Authority != nil || probe.request.Operation != "generation.cleanup" || probe.request.ObjectID != binding.JobID || probe.request.Scope.WorkspaceID != binding.Scope.WorkspaceID || probe.request.Scope.ProjectID != binding.Scope.ProjectID {
		t.Fatal("cleanup identity or scope rebound")
	}
	probe.scope.ProjectID = "foreign"
	if _, _, err = server.auditCleanupGate(context.Background(), binding); err == nil {
		t.Fatal("foreign registry scope accepted")
	}
	probe.scope = accessv1.PermissionScope{WorkspaceID: "workspace", ProjectID: "project"}
	if _, _, err = server.auditCleanupGate(context.Background(), binding); err == nil {
		t.Fatal("fabricated missing organization")
	}
}
