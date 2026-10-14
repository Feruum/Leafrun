package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func (s *Store) BeginPublication(ctx context.Context, jobID, owner string) (PublicationLease, error) {
	var out PublicationLease
	if jobID == "" || owner == "" || len(owner) > 128 {
		return out, errors.New("job and bounded publication owner are required")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return out, err
	}
	defer rollback(ctx, tx)
	var status string
	var canceled bool
	var epoch int64
	var oldOwner string
	var oldUntil *time.Time
	err = tx.QueryRow(ctx, `SELECT status,cancellation_requested,publication_epoch,publication_owner,publication_lease_until FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&status, &canceled, &epoch, &oldOwner, &oldUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if status != "publishing" || canceled {
		return out, ErrInvalidTransition
	}
	var leaseActive bool
	if oldOwner != "" {
		if err = tx.QueryRow(ctx, `SELECT publication_lease_until>clock_timestamp() FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&leaseActive); err != nil {
			return out, err
		}
	}
	if leaseActive && oldOwner != owner {
		return out, ErrPublicationHeld
	}
	if !leaseActive || oldOwner == "" {
		epoch++
	}
	if _, err = tx.Exec(ctx, `UPDATE generation_jobs SET publication_epoch=$2,publication_owner=$3,publication_lease_until=clock_timestamp()+$4*interval '1 microsecond',phase='publishing',updated_at=clock_timestamp() WHERE job_id=$1`, jobID, epoch, owner, s.cfg.PublicationLease.Microseconds()); err != nil {
		return out, err
	}
	if err = tx.QueryRow(ctx, `SELECT publication_lease_until FROM generation_jobs WHERE job_id=$1`, jobID).Scan(&out.LeaseUntil); err != nil {
		return out, err
	}
	out.JobID = jobID
	out.Owner = owner
	out.Epoch = epoch
	candidate, e := scanCandidate(tx.QueryRow(ctx, candidateSelect+` WHERE job_id=$1 AND expires_at>clock_timestamp() ORDER BY created_at DESC LIMIT 1`, jobID))
	if e == nil {
		out.Candidate = &candidate
	} else if !errors.Is(e, pgx.ErrNoRows) {
		return PublicationLease{}, e
	}
	if err = commit(ctx, tx); err != nil {
		return PublicationLease{}, err
	}
	return out, nil
}

func (s *Store) RenewPublication(ctx context.Context, jobID, owner string, epoch int64) (time.Time, error) {
	var until time.Time
	err := s.pool.QueryRow(ctx, `UPDATE generation_jobs SET publication_lease_until=clock_timestamp()+$4*interval '1 microsecond',updated_at=clock_timestamp()
		WHERE job_id=$1 AND publication_owner=$2 AND publication_epoch=$3 AND publication_lease_until>clock_timestamp() AND status='publishing' AND NOT cancellation_requested RETURNING publication_lease_until`, jobID, owner, epoch, s.cfg.PublicationLease.Microseconds()).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, err
	}
	return until, nil
}

const candidateSelect = `SELECT candidate_id,job_id,render_attempt_id,workspace_id,project_id,snapshot_id,target_id,input_fingerprint,artifact_id,sha256,size_bytes,expires_at,artifact_bytes,finalize_receipt FROM generation_publication_candidates`

func scanCandidate(row interface{ Scan(...any) error }) (PublicationCandidate, error) {
	var c PublicationCandidate
	var workspace, project string
	err := row.Scan(&c.CandidateID, &c.JobID, &c.RenderAttemptID, &workspace, &project, &c.SnapshotID, &c.TargetID, &c.InputFingerprint, &c.Artifact.ArtifactID, &c.Artifact.Hash, &c.Artifact.SizeBytes, &c.Artifact.ExpiresAt, &c.ArtifactBytes, &c.FinalizeReceipt)
	if err != nil {
		return PublicationCandidate{}, err
	}
	c.Scope = storagev1.Scope{WorkspaceID: workspace, ProjectID: project}
	if err = jsonvalue.Unmarshal(c.ArtifactBytes, &c.Artifact, 1<<20); err != nil {
		return PublicationCandidate{}, fmt.Errorf("decode stored Storage artifact: %w", err)
	}
	return c, nil
}

// RegisterCandidate commits the immutable Storage finalize receipt under the
// current publication epoch. A later publisher can adopt it after lease expiry.
func (s *Store) RegisterCandidate(ctx context.Context, jobID, owner string, publicationEpoch int64, c PublicationCandidate) error {
	if c.JobID != jobID || jobID == "" || owner == "" || publicationEpoch <= 0 || c.Artifact.ArtifactID == "" || c.RenderAttemptID == "" || c.SnapshotID == "" || c.TargetID == "" || c.InputFingerprint == "" || c.Artifact.SizeBytes <= 0 || len(c.FinalizeReceipt) == 0 || len(c.FinalizeReceipt) > 1<<20 {
		return errors.New("publication candidate is incomplete")
	}
	artifactBytes := append([]byte(nil), c.ArtifactBytes...)
	if len(artifactBytes) == 0 {
		var err error
		artifactBytes, err = json.Marshal(c.Artifact)
		if err != nil {
			return err
		}
	}
	var decoded storagev1.Artifact
	if err := jsonvalue.Unmarshal(artifactBytes, &decoded, 1<<20); err != nil {
		return errors.New("Storage artifact bytes are invalid")
	}
	if !artifactEqual(decoded, c.Artifact) {
		return ErrConflict
	}
	var receipt storagev1.Artifact
	if err := jsonvalue.Unmarshal(c.FinalizeReceipt, &receipt, 1<<20); err != nil || !artifactEqual(receipt, c.Artifact) {
		return ErrConflict
	}
	if c.Artifact.Scope != c.Scope || c.Artifact.OriginJobID != jobID || c.Artifact.SnapshotID != c.SnapshotID || c.Artifact.Metadata.TargetID != c.TargetID || c.Artifact.Metadata.AttemptID != c.RenderAttemptID || c.Artifact.Metadata.InputFingerprint != c.InputFingerprint || c.Artifact.Hash == "" {
		return errors.New("Storage artifact binding does not match the candidate")
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var workspace, project, sourceSnapshot, derivedSnapshot, target, fingerprint, status, publisher string
	var currentEpoch int64
	var leaseOK, canceled bool
	err = tx.QueryRow(ctx, `SELECT workspace_id,project_id,source_snapshot_id,derived_snapshot_id,target_id,fingerprint,status,publication_owner,publication_epoch,COALESCE(publication_lease_until>clock_timestamp(),false),cancellation_requested FROM generation_jobs WHERE job_id=$1 FOR UPDATE`, jobID).Scan(&workspace, &project, &sourceSnapshot, &derivedSnapshot, &target, &fingerprint, &status, &publisher, &currentEpoch, &leaseOK, &canceled)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "publishing" || canceled || publisher != owner || currentEpoch != publicationEpoch || !leaseOK {
		return ErrLeaseLost
	}
	expectedSnapshot := sourceSnapshot
	if derivedSnapshot != "" {
		expectedSnapshot = derivedSnapshot
	}
	if c.Scope != (storagev1.Scope{WorkspaceID: workspace, ProjectID: project}) || c.SnapshotID != expectedSnapshot || c.TargetID != target || c.InputFingerprint != fingerprint {
		return ErrConflict
	}
	var attemptJob, attemptState string
	var attemptStopped bool
	err = tx.QueryRow(ctx, `SELECT job_id,state,stop_confirmed FROM generation_attempts WHERE attempt_id=$1`, c.RenderAttemptID).Scan(&attemptJob, &attemptState, &attemptStopped)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if attemptJob != jobID || attemptState != "finished" || !attemptStopped {
		return ErrConflict
	}
	var hasFinalizeReceipt bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM generation_operation_intents WHERE job_id=$1 AND render_attempt_id=$2 AND kind=$3 AND output=$4 AND completed_at IS NOT NULL)`, jobID, c.RenderAttemptID, IntentArtifactFinalize, c.FinalizeReceipt).Scan(&hasFinalizeReceipt); err != nil {
		return err
	}
	if !hasFinalizeReceipt {
		return ErrConflict
	}
	var notExpired bool
	if err = tx.QueryRow(ctx, `SELECT $1::timestamptz>clock_timestamp()`, c.Artifact.ExpiresAt).Scan(&notExpired); err != nil {
		return err
	}
	if !notExpired {
		return ErrCandidateExpired
	}
	if c.CandidateID == "" {
		c.CandidateID, err = newID("cand_")
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO generation_publication_candidates(candidate_id,job_id,render_attempt_id,workspace_id,project_id,snapshot_id,target_id,input_fingerprint,artifact_id,sha256,size_bytes,expires_at,artifact,artifact_bytes,finalize_receipt)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15) ON CONFLICT(job_id,artifact_id) DO NOTHING`, c.CandidateID, jobID, c.RenderAttemptID, workspace, project, c.SnapshotID, c.TargetID, c.InputFingerprint, c.Artifact.ArtifactID, c.Artifact.Hash, c.Artifact.SizeBytes, c.Artifact.ExpiresAt, artifactBytes, artifactBytes, c.FinalizeReceipt)
	if err != nil {
		return fmt.Errorf("persist publication candidate: %w", err)
	}
	var prior PublicationCandidate
	prior, err = scanCandidate(tx.QueryRow(ctx, candidateSelect+` WHERE job_id=$1 AND artifact_id=$2 FOR UPDATE`, jobID, c.Artifact.ArtifactID))
	if err != nil {
		return err
	}
	if !candidateEqual(prior, c) {
		return ErrConflict
	}
	return commit(ctx, tx)
}

func candidateEqual(a, b PublicationCandidate) bool {
	return a.JobID == b.JobID && a.RenderAttemptID == b.RenderAttemptID && a.Scope == b.Scope && a.SnapshotID == b.SnapshotID && a.TargetID == b.TargetID && a.InputFingerprint == b.InputFingerprint && a.Artifact.ArtifactID == b.Artifact.ArtifactID && a.Artifact.Hash == b.Artifact.Hash && a.Artifact.SizeBytes == b.Artifact.SizeBytes && a.Artifact.ExpiresAt.Equal(b.Artifact.ExpiresAt) && bytes.Equal(a.ArtifactBytes, b.ArtifactBytes) && bytes.Equal(a.FinalizeReceipt, b.FinalizeReceipt)
}

func artifactEqual(a, b storagev1.Artifact) bool {
	return a.ArtifactID == b.ArtifactID && a.Scope == b.Scope && a.OriginJobID == b.OriginJobID && a.SnapshotID == b.SnapshotID && a.Hash == b.Hash && a.SizeBytes == b.SizeBytes && a.Metadata == b.Metadata && a.CreatedAt.Equal(b.CreatedAt) && a.ExpiresAt.Equal(b.ExpiresAt)
}

func (s *Store) GetCandidate(ctx context.Context, jobID, artifactID string) (PublicationCandidate, error) {
	c, err := scanCandidate(s.pool.QueryRow(ctx, candidateSelect+` WHERE job_id=$1 AND artifact_id=$2`, jobID, artifactID))
	if errors.Is(err, pgx.ErrNoRows) {
		return PublicationCandidate{}, ErrNotFound
	}
	return c, err
}
