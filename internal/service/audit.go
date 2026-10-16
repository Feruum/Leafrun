package service

import (
	"context"
	"errors"
	"github.com/Feruum/Leafrun/internal/store"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
)

type canonicalScopeChecker interface {
	CheckScope(context.Context, accessv1.CheckRequest) (accessv1.CheckResponse, error)
}

func (c *AccessClient) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	if c.current == nil {
		return accessv1.CheckResponse{}, accessv1.ErrDependency
	}
	return c.current.CheckScope(ctx, in)
}

func (s *Server) auditScopeGate(ctx context.Context, b store.AuthorityBinding, operation string) (accessv1.PermissionScope, error) {
	if b.Identity == nil {
		return accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	checker, ok := s.cfg.Access.(canonicalScopeChecker)
	if !ok {
		return accessv1.PermissionScope{}, errors.New("canonical Access scope unavailable")
	}
	scope := accessv1.PermissionScope{WorkspaceID: b.Scope.WorkspaceID, ProjectID: b.Scope.ProjectID}
	if b.AuditScope != nil {
		scope = *b.AuditScope
	}
	out, err := checker.CheckScope(ctx, accessv1.CheckRequest{Principal: b.Identity.Principal, Authority: &b.Identity.Authority, Scope: scope, Operation: operation, ObjectID: b.JobID})
	if err != nil {
		return accessv1.PermissionScope{}, err
	}
	if !out.Allowed || out.CanonicalScope == nil {
		return accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	canonical := *out.CanonicalScope
	if canonical.Validate() != nil || canonical.OrganizationID == "" || canonical.WorkspaceID != b.Scope.WorkspaceID || canonical.ProjectID != b.Scope.ProjectID || (b.AuditScope != nil && canonical != *b.AuditScope) {
		return accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	return canonical, nil
}

func (s *Server) auditCleanupGate(ctx context.Context, b store.AuthorityBinding) (identityv1.Identity, accessv1.PermissionScope, error) {
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "leafrun-generation", Domain: "service"}, Authority: s.cfg.AuditCleanupAuthority}
	if identity.Validate() != nil || identity.Authority.Kind != identityv1.KindService {
		return identityv1.Identity{}, accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	checker, ok := s.cfg.Access.(canonicalScopeChecker)
	if !ok {
		return identityv1.Identity{}, accessv1.PermissionScope{}, accessv1.ErrDependency
	}
	// No initiating-user authority is passed to this fixed technical service
	// request. Access authenticates the service credential and closed ACL.
	scope := accessv1.PermissionScope{WorkspaceID: b.Scope.WorkspaceID, ProjectID: b.Scope.ProjectID}
	decision, err := checker.CheckScope(ctx, accessv1.CheckRequest{Principal: identity.Principal, Scope: scope, Operation: "generation.cleanup", ObjectID: b.JobID})
	if err != nil {
		return identityv1.Identity{}, accessv1.PermissionScope{}, err
	}
	if !decision.Allowed || decision.CanonicalScope == nil {
		return identityv1.Identity{}, accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	canonical := *decision.CanonicalScope
	if canonical.Validate() != nil || canonical.OrganizationID == "" || canonical.WorkspaceID != b.Scope.WorkspaceID || canonical.ProjectID != b.Scope.ProjectID {
		return identityv1.Identity{}, accessv1.PermissionScope{}, store.ErrAuthorityDenied
	}
	return identity, canonical, nil
}
