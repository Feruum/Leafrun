package store

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"github.com/leafrun/leafrun-platform/events"
	"strconv"
	"time"
)

type AuditScopeGate func(context.Context, AuthorityBinding, string) (accessv1.PermissionScope, error)
type AuditCleanupGate func(context.Context, AuthorityBinding) (identityv1.Identity, accessv1.PermissionScope, error)

func (s *Store) SetAuditCleanupGate(g AuditCleanupGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.auditCleanupGate = g
}

// SetAuditScopeGate enables mandatory transactional audit production. Runtime
// installs it in Access mode; only explicit development fixture stores omit it.
func (s *Store) SetAuditScopeGate(g AuditScopeGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.auditScopeGate = g
}

func newAuditID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", errors.New("audit identity unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func validateAuditContext(id identityv1.Identity, scope accessv1.PermissionScope, w, p string) error {
	if id.Validate() != nil || id.Authority.Kind == identityv1.KindService || id.Authority.Kind == identityv1.KindDev || scope.Validate() != nil || scope.OrganizationID == "" || scope.WorkspaceID != w || scope.ProjectID != p {
		return ErrAuthorityDenied
	}
	return nil
}

func (s *Store) captureAuditContext(ctx context.Context, tx pgx.Tx, job string) error {
	s.gateMu.RLock()
	gate := s.auditScopeGate
	s.gateMu.RUnlock()
	if gate == nil {
		return nil
	}
	binding, err := loadAuthority(ctx, tx, job)
	if err != nil {
		return err
	}
	if binding.Identity == nil {
		return ErrAuthorityDenied
	}
	final, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	scope, err := gate(final, binding, "generation.create")
	if err != nil {
		return err
	}
	if err = validateAuditContext(*binding.Identity, scope, binding.Scope.WorkspaceID, binding.Scope.ProjectID); err != nil {
		return err
	}
	operation, err := newAuditID()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return err
	}
	identityRaw, err := json.Marshal(binding.Identity)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_audit_context(job_id,operation_id,canonical_scope,initiating_identity)VALUES($1,$2,$3::jsonb,$4::jsonb)`, job, operation, raw, identityRaw)
	return err
}

func (s *Store) captureLegacyCleanup(ctx context.Context, tx pgx.Tx, job string) error {
	s.gateMu.RLock()
	gate := s.auditCleanupGate
	s.gateMu.RUnlock()
	if gate == nil {
		return ErrAuthorityDenied
	}
	binding, err := loadAuthority(ctx, tx, job)
	if err != nil {
		return err
	}
	// This is called only for a failed/cancelled terminal cleanup, never a
	// success/publication. The locked owner row fixes the exact target scope.
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	id, scope, err := gate(bounded, binding)
	if err != nil {
		return err
	}
	if id.Validate() != nil || id.Principal.Domain != "service" || id.Principal.Subject != "leafrun-generation" || id.Authority.Kind != identityv1.KindService || scope.Validate() != nil || scope.OrganizationID == "" || scope.WorkspaceID != binding.Scope.WorkspaceID || scope.ProjectID != binding.Scope.ProjectID {
		return ErrAuthorityDenied
	}
	operation, err := newAuditID()
	if err != nil {
		return err
	}
	scopeRaw, err := json.Marshal(scope)
	if err != nil {
		return err
	}
	identityRaw, err := json.Marshal(id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_audit_context(job_id,operation_id,canonical_scope,initiating_identity)VALUES($1,$2,$3::jsonb,$4::jsonb)`, job, operation, scopeRaw, identityRaw)
	return err
}

func (s *Store) enqueueJobAudit(ctx context.Context, tx pgx.Tx, job string, version uint64, at time.Time, name auditv1.ActionName, outcome auditv1.Outcome) error {
	var operation string
	var scope accessv1.PermissionScope
	var raw, identityRaw []byte
	err := tx.QueryRow(ctx, `SELECT operation_id,canonical_scope,initiating_identity FROM generation_audit_context WHERE job_id=$1`, job).Scan(&operation, &raw, &identityRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		s.gateMu.RLock()
		required := s.auditScopeGate != nil
		s.gateMu.RUnlock()
		if !required {
			return nil
		}
		if name != auditv1.ActionGenerationTerminal || outcome == auditv1.OutcomeCompleted {
			return ErrAuthorityDenied
		}
		if err = s.captureLegacyCleanup(ctx, tx, job); err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `SELECT operation_id,canonical_scope,initiating_identity FROM generation_audit_context WHERE job_id=$1`, job).Scan(&operation, &raw, &identityRaw)
	}
	if err != nil {
		return err
	}
	var identity identityv1.Identity
	if json.Unmarshal(raw, &scope) != nil || json.Unmarshal(identityRaw, &identity) != nil || identity.Validate() != nil {
		return ErrAuthorityDenied
	}
	binding, err := loadAuthority(ctx, tx, job)
	if err != nil {
		return err
	}
	if identity.Authority.Kind == identityv1.KindService {
		if identity.Principal.Domain != "service" || identity.Principal.Subject != "leafrun-generation" || name != auditv1.ActionGenerationTerminal || outcome == auditv1.OutcomeCompleted {
			return ErrAuthorityDenied
		}
	} else if binding.Identity == nil || identity != *binding.Identity || validateAuditContext(identity, scope, binding.Scope.WorkspaceID, binding.Scope.ProjectID) != nil {
		return ErrAuthorityDenied
	}
	event, err := newAuditID()
	if err != nil {
		return err
	}
	if err = events.EnqueueAuditAction(ctx, tx, events.AuditActionInput{EventID: event, Scope: scope, OccurredAt: at, Action: auditv1.Action{SchemaVersion: 1, Producer: auditv1.ProducerGeneration, Actor: identity.Principal, Authority: identity.Authority, OperationID: operation, Action: name, Resource: auditv1.ResourceRef{Kind: auditv1.ResourceJob, ID: job}, Outcome: outcome, Revision: strconv.FormatUint(version, 10)}}); err != nil {
		return err
	}
	// Retention already protects all pending events linked to a job here.
	_, err = tx.Exec(ctx, `INSERT INTO generation_terminal_outbox(job_id,event_id)VALUES($1,$2)`, job, event)
	return err
}
func terminalAuditOutcome(status generationv1.Status) auditv1.Outcome {
	switch status {
	case generationv1.Succeeded:
		return auditv1.OutcomeCompleted
	case generationv1.Canceled:
		return auditv1.OutcomeCancelled
	default:
		return auditv1.OutcomeFailed
	}
}
