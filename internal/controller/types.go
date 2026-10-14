// Package controller coordinates trusted external effects from durable intent.
// Source bytes and Typst run only inside the bounded renderer container.
package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/Feruum/Leafrun/internal/service"
	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

type Storage interface {
	GetSnapshot(context.Context, string) (storagev1.Snapshot, error)
	ResourceContent(context.Context, string) (*http.Response, error)
	CreateUpload(context.Context, storagev1.CreateUploadRequest) (storagev1.Upload, error)
	GetUpload(context.Context, string) (storagev1.Upload, error)
	RenewUpload(context.Context, string, storagev1.RenewUploadRequest) (storagev1.Upload, error)
	PutUpload(context.Context, string, string, io.Reader) (storagev1.Upload, error)
	CommitResource(context.Context, string, storagev1.CommitResourceRequest) (storagev1.Resource, error)
	CreateSnapshot(context.Context, storagev1.CreateSnapshotRequest) (storagev1.Snapshot, error)
	FinalizeArtifact(context.Context, string, storagev1.FinalizeArtifactRequest) (storagev1.Artifact, error)
	ReleasePin(context.Context, string) error
}
type DerivedPinner interface {
	EnsureDerivedPin(context.Context, generationv1.Job, string) error
}
type Config struct {
	StagingRoot   string
	OwnerID       string
	RendererImage string
	Heartbeat     time.Duration
}

func (c Config) validate() error {
	if !filepath.IsAbs(c.StagingRoot) || c.OwnerID == "" || len(c.OwnerID) > 128 {
		return errors.New("absolute private staging root and bounded owner ID required")
	}
	return nil
}

type Publisher struct {
	state   *store.Store
	storage Storage
	pins    DerivedPinner
	access  service.AccessChecker
	cfg     Config
}

func NewPublisher(s *store.Store, storage Storage, pins DerivedPinner, access service.AccessChecker, cfg Config) (*Publisher, error) {
	if s == nil || storage == nil || pins == nil || access == nil {
		return nil, errors.New("store, Storage, pin coordinator and Access are required")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Publisher{s, storage, pins, access, cfg}, nil
}
func renderRequest(job generationv1.Job, a store.Attempt, in store.ExecutionInput) renderwire.RenderRequest {
	return renderwire.RenderRequest{Scope: job.Scope, JobID: job.JobID, AttemptID: a.AttemptID, TargetID: job.TargetID, Entrypoint: job.Entrypoint, SourceSnapshotID: job.SourceSnapshotID, SourceContentID: job.SourceContentID, InputFingerprint: job.InputFingerprint, Parameters: in.Request.Parameters, ParametersSchemaPath: in.Request.ParametersSchemaPath}
}
func (p *Publisher) checkAccess(ctx context.Context, job generationv1.Job, in store.ExecutionInput, operation string) (bool, error) {
	principal, err := service.DecodeInitiator(in.InitiatorID)
	if err != nil {
		return false, err
	}
	return p.access.Check(ctx, storagev1.AccessCheck{Principal: principal, Scope: job.Scope, Operation: operation, ObjectID: job.JobID})
}
