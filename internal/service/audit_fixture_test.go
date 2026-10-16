package service

import (
	"context"
	"github.com/Feruum/Leafrun/internal/store"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

// These adapters keep boundary-only controlled fixtures explicit. Durable
// action/outbox proof uses the actual PostgreSQL Store tests, not this store.
func (s *testStore) SetAuditScopeGate(store.AuditScopeGate)     {}
func (s *testStore) SetAuditCleanupGate(store.AuditCleanupGate) {}
func fixtureCanonical(in accessv1.CheckRequest, allowed bool, err error) (accessv1.CheckResponse, error) {
	if !allowed || err != nil {
		return accessv1.CheckResponse{}, err
	}
	scope := accessv1.PermissionScope{OrganizationID: "fixture-organization", WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID}
	return accessv1.CheckResponse{Allowed: true, CanonicalScope: &scope}, nil
}
func fixtureStorageCheck(in accessv1.CheckRequest) storagev1.AccessCheck {
	return storagev1.AccessCheck{Principal: in.Principal, Authority: in.Authority, Scope: storagev1.Scope{WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID}, Operation: in.Operation, ObjectID: in.ObjectID}
}
func (a *testAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := a.Check(ctx, fixtureStorageCheck(in))
	return fixtureCanonical(in, allowed, err)
}
func (a *e06DisclosureAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := a.Check(ctx, fixtureStorageCheck(in))
	return fixtureCanonical(in, allowed, err)
}
