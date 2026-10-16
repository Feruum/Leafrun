package service

import (
	"context"
	"errors"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

func requestIdentity(ctx context.Context) *identityv1.Identity {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil
	}
	return &id
}
func authorityRef(ctx context.Context) *identityv1.AuthorityRef {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return nil
	}
	ref := id.Authority
	return &ref
}

// RestoreExecutionAuthority uses only original durable provenance. A refreshed
// request identity is never a replacement for an admitted execution identity.
func RestoreExecutionAuthority(ctx context.Context, input store.ExecutionInput) (context.Context, error) {
	if input.Identity == nil {
		return ctx, nil
	}
	principal, err := DecodeInitiator(input.InitiatorID)
	if err != nil || principal != input.Identity.Principal {
		return ctx, store.ErrAuthorityDenied
	}
	return authn.WithIdentity(ctx, *input.Identity)
}

type authorityLoader interface {
	LoadAuthority(context.Context, string) (store.AuthorityBinding, error)
}
type failedAdmissionStore interface {
	FailJob(context.Context, store.TerminalInput) error
}

func (s *Server) requestAuthorityGate(ctx context.Context, binding store.AuthorityBinding, replay bool) error {
	current, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return store.ErrAuthorityDenied
	}
	principal, err := DecodeInitiator(binding.InitiatorID)
	if err != nil {
		return store.ErrAuthorityDenied
	}
	if binding.CurrentOperation != "" {
		if current.Principal != principal {
			allowed, e := s.checkAccess(ctx, current.Principal, binding.Scope, "project.read", binding.JobID)
			if e != nil {
				return e
			}
			if !allowed {
				return store.ErrAuthorityDenied
			}
		}
		allowed, e := s.checkAccess(ctx, current.Principal, binding.Scope, binding.CurrentOperation, binding.JobID)
		if e != nil {
			return e
		}
		if !allowed {
			return store.ErrAuthorityDenied
		}
		return nil
	}
	if current.Principal != principal {
		return store.ErrAuthorityDenied
	}
	if !replay {
		binding.Identity = &current
		return s.durableAuthorityGate(ctx, binding, "generation.create")
	}
	// Replay exposes a historical receipt, not permission to resume its work.
	allowed, err := s.checkAccess(ctx, principal, binding.Scope, "generation.create", binding.JobID)
	if err != nil {
		return err
	}
	if !allowed {
		return store.ErrAuthorityDenied
	}
	if binding.ReleaseID == "" {
		allowed, err = s.checkAccess(ctx, principal, binding.Scope, "source.read", binding.SourceSnapshotID)
		if err != nil {
			return err
		}
		if !allowed {
			return store.ErrAuthorityDenied
		}
	}
	return nil
}

func (s *Server) durableAuthorityGate(ctx context.Context, binding store.AuthorityBinding, operation string) error {
	if binding.Identity == nil {
		return store.ErrAuthorityDenied
	}
	principal, err := DecodeInitiator(binding.InitiatorID)
	if err != nil || principal != binding.Identity.Principal {
		return store.ErrAuthorityDenied
	}
	ctx, err = authn.WithIdentity(ctx, *binding.Identity)
	if err != nil {
		return store.ErrAuthorityDenied
	}
	check := func(action, object string) error {
		allowed, checkErr := s.checkAccess(ctx, principal, binding.Scope, action, object)
		if checkErr != nil {
			return checkErr
		}
		if !allowed {
			return store.ErrAuthorityDenied
		}
		return nil
	}
	if err = check(operation, binding.JobID); err != nil {
		return err
	}
	if binding.ReleaseID == "" {
		if err = check("source.read", binding.SourceSnapshotID); err != nil {
			return err
		}
	} else {
		if s.cfg.Projects == nil || binding.DescriptorDigest == "" {
			return store.ErrAuthorityDenied
		}
		decision, checkErr := s.cfg.Projects.CurrentRelease(ctx, projectsv1.CurrentReleaseCheck{Authority: &binding.Identity.Authority, Actor: principal, Scope: binding.Scope, ReleaseID: binding.ReleaseID, DescriptorDigest: binding.DescriptorDigest})
		if checkErr != nil {
			return checkErr
		}
		if !decision.Allowed {
			return store.ErrAuthorityDenied
		}
	}
	for _, ref := range binding.Resources {
		if ref.Scope != binding.Scope {
			return store.ErrConflict
		}
		if err = check("resource.read", ref.ResourceID); err != nil {
			return err
		}
	}
	// Projects may wait on Storage. Finish with current generate authority after
	// that owner call, immediately before the durable owner transition.
	if err = check(operation, binding.JobID); err != nil {
		return err
	}
	if binding.AuditScope != nil {
		_, err = s.auditScopeGate(ctx, binding, operation)
		return err
	}
	return nil
}
func (s *Server) restoreAuthority(ctx context.Context, jobID string) (context.Context, error) {
	state, ok := s.store.(authorityLoader)
	if !ok {
		if s.cfg.AuthMode == "dev" {
			return ctx, nil
		}
		return ctx, errors.New("durable original authority unavailable")
	}
	binding, err := state.LoadAuthority(ctx, jobID)
	if err != nil {
		return ctx, err
	}
	if binding.Identity == nil {
		if s.cfg.AuthMode == "dev" {
			return ctx, nil
		}
		return ctx, store.ErrAuthorityDenied
	}
	principal, err := DecodeInitiator(binding.InitiatorID)
	if err != nil || principal != binding.Identity.Principal {
		return ctx, store.ErrAuthorityDenied
	}
	return authn.WithIdentity(ctx, *binding.Identity)
}
func (s *Server) checkOriginal(ctx context.Context, jobID string, scopeWorkspace, scopeProject, operation, objectID string) error {
	if s.cfg.AuthMode == "dev" {
		return nil
	}
	state, ok := s.store.(authorityLoader)
	if !ok {
		return errors.New("durable original authority unavailable")
	}
	binding, err := state.LoadAuthority(ctx, jobID)
	if err != nil {
		return err
	}
	if binding.Scope.WorkspaceID != scopeWorkspace || binding.Scope.ProjectID != scopeProject {
		return store.ErrConflict
	}
	return s.durableAuthorityGate(ctx, binding, operation)
}

func (s *Server) jobCallerBinding(ctx context.Context, principal storagev1.Principal, job generationv1.Job) (bool, error) {
	if s.cfg.AuthMode == "dev" {
		return true, nil
	}
	loader, ok := s.store.(authorityLoader)
	if !ok {
		return false, errors.New("durable initiator binding unavailable")
	}
	binding, err := loader.LoadAuthority(ctx, job.JobID)
	if err != nil {
		return false, err
	}
	initiator, err := DecodeInitiator(binding.InitiatorID)
	if err != nil {
		return false, err
	}
	if initiator == principal {
		return true, nil
	}
	return s.checkAccess(ctx, principal, job.Scope, "project.read", job.JobID)
}
