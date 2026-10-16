package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

var ErrAuthorityDenied = errors.New("original generation authority denied")

// AuthorityBinding is provenance, separate from immutable execution bytes and
// receipt fingerprints. Its values come from the actual durable owner row.
type AuthorityBinding struct {
	CurrentOperation string
	JobID            string
	Scope            storagev1.Scope
	InitiatorID      string
	Identity         *identityv1.Identity
	AuditScope       *accessv1.PermissionScope
	SourceSnapshotID string
	SourceContentID  string
	ReleaseID        string
	DescriptorDigest string
	Resources        []AuthorityResource
}

func (s *Store) gateCurrentOperation(ctx context.Context, tx pgx.Tx, jobID, operation string) error {
	s.gateMu.RLock()
	gate := s.requestAuthorityGate
	s.gateMu.RUnlock()
	if gate == nil {
		return nil
	}
	binding, err := loadAuthority(ctx, tx, jobID)
	if err != nil {
		return err
	}
	binding.CurrentOperation = operation
	final, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = gate(final, binding, true); err != nil {
		return err
	}
	return final.Err()
}

type AuthorityResource struct {
	Scope      storagev1.Scope
	ResourceID string
}
type AuthorityGate func(context.Context, AuthorityBinding, string) error
type RequestAuthorityGate func(context.Context, AuthorityBinding, bool) error

// Request checks use the current authenticated caller. Historical receipt
// replay must not restore the original credential or execution time fences.
func (s *Store) SetRequestAuthorityGate(gate RequestAuthorityGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.requestAuthorityGate = gate
}
func (s *Store) gateRequestAuthority(ctx context.Context, tx pgx.Tx, jobID string, replay bool) error {
	s.gateMu.RLock()
	gate := s.requestAuthorityGate
	s.gateMu.RUnlock()
	if gate == nil {
		return nil
	}
	binding, err := loadAuthority(ctx, tx, jobID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = gate(ctx, binding, replay); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	var live bool
	if err = tx.QueryRow(ctx, `SELECT expires_at>clock_timestamp() FROM generation_admission_receipts WHERE job_id=$1`, jobID).Scan(&live); err != nil {
		return err
	}
	if !live {
		return ErrReceiptExpired
	}
	if !replay {
		if err = tx.QueryRow(ctx, `SELECT deadline>clock_timestamp() AND NOT cancellation_requested FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&live); err != nil {
			return err
		}
		if !live {
			return ErrLeaseLost
		}
	}
	return nil
}

// SetAuthorityGate installs the live access-mode gate before serving work.
// Fixture stores deliberately do not install a gate.
func (s *Store) SetAuthorityGate(gate AuthorityGate) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	s.authorityGate = gate
}

type authorityQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func loadAuthority(ctx context.Context, q authorityQuerier, jobID string) (AuthorityBinding, error) {
	var out AuthorityBinding
	var raw []byte
	err := q.QueryRow(ctx, `SELECT job_id,workspace_id,project_id,initiator_id,original_identity,source_snapshot_id,source_content_id,authority_release_id,authority_descriptor_digest FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&out.JobID, &out.Scope.WorkspaceID, &out.Scope.ProjectID, &out.InitiatorID, &raw, &out.SourceSnapshotID, &out.SourceContentID, &out.ReleaseID, &out.DescriptorDigest)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if len(raw) > 0 {
		var id identityv1.Identity
		if jsonvalue.Unmarshal(raw, &id, 2048) != nil || id.Validate() != nil || id.Authority.Kind == identityv1.KindService || id.Authority.Kind == identityv1.KindDev {
			return AuthorityBinding{}, ErrAuthorityDenied
		}
		out.Identity = &id
	}
	var scopeRaw []byte
	err = q.QueryRow(ctx, `SELECT canonical_scope FROM generation_audit_context WHERE job_id=$1`, jobID).Scan(&scopeRaw)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	if len(scopeRaw) > 0 {
		var scope accessv1.PermissionScope
		if jsonvalue.Unmarshal(scopeRaw, &scope, 2048) != nil || scope.Validate() != nil || scope.OrganizationID == "" || scope.WorkspaceID != out.Scope.WorkspaceID || scope.ProjectID != out.Scope.ProjectID {
			return out, ErrAuthorityDenied
		}
		out.AuditScope = &scope
	}
	rows, err := q.Query(ctx, `SELECT workspace_id,project_id,resource_id FROM generation_input_resources WHERE job_id=$1 ORDER BY field_pointer`, jobID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref AuthorityResource
		if rows.Scan(&ref.Scope.WorkspaceID, &ref.Scope.ProjectID, &ref.ResourceID) != nil || len(out.Resources) >= 1024 {
			return AuthorityBinding{}, ErrAuthorityDenied
		}
		out.Resources = append(out.Resources, ref)
	}
	if err = rows.Err(); err != nil {
		return AuthorityBinding{}, err
	}
	return out, nil
}

// LoadAuthority works before pins activate and does not read normalized input.
func (s *Store) LoadAuthority(ctx context.Context, jobID string) (AuthorityBinding, error) {
	return loadAuthority(ctx, s.pool, jobID)
}
func (s *Store) gateAuthority(ctx context.Context, tx pgx.Tx, jobID, operation string) error {
	s.gateMu.RLock()
	gate := s.authorityGate
	s.gateMu.RUnlock()
	if gate == nil {
		return nil
	}
	binding, err := loadAuthority(ctx, tx, jobID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = gate(ctx, binding, operation); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// The dependency wait can consume owner-local time fences even while row
	// locks hold. Re-read their current database clock after the remote check.
	var live bool
	if err = tx.QueryRow(ctx, `SELECT deadline>clock_timestamp() AND NOT cancellation_requested FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&live); err != nil {
		return err
	}
	if !live {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) gatePinAuthority(ctx context.Context, tx pgx.Tx, jobID string, expiry time.Time) error {
	if err := s.gateAuthority(ctx, tx, jobID, "generation.execute"); err != nil {
		return err
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz>clock_timestamp()`, expiry).Scan(&live); err != nil {
		return err
	}
	if !live {
		return ErrInvalidTransition
	}
	return nil
}
