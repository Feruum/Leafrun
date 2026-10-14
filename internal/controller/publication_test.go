package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/service"
	"github.com/Feruum/Leafrun/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/events"
)

func privateStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GENERATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("dedicated Generation test database is disabled")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || cfg.ConnConfig.Database != "generation_test" {
		t.Fatal("dedicated generation_test database required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("controller_test_%x", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	})
	s, err := store.New(pool, store.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s, pool
}

type accessFixture struct {
	allow bool
	err   error
}

func (a accessFixture) Check(context.Context, storagev1.AccessCheck) (bool, error) {
	return a.allow, a.err
}

type pinFixture struct{}

func (pinFixture) EnsureDerivedPin(context.Context, generationv1.Job, string) error { return nil }

type storageFixture struct {
	uploads                 map[string]storagev1.Upload
	creates                 map[string]string
	artifacts               map[string]storagev1.Artifact
	requests                []storagev1.FinalizeArtifactRequest
	failFinalize            bool
	failReadAfterPut        bool
	tamperSnapshotRefs      bool
	finalizeCalls, putCalls int
}

func newStorageFixture() *storageFixture {
	return &storageFixture{uploads: map[string]storagev1.Upload{}, creates: map[string]string{}, artifacts: map[string]storagev1.Artifact{}}
}
func (f *storageFixture) CreateUpload(_ context.Context, in storagev1.CreateUploadRequest) (storagev1.Upload, error) {
	if id := f.creates[in.IdempotencyKey]; id != "" {
		return f.uploads[id], nil
	}
	id := fmt.Sprintf("upload-%d", len(f.uploads)+1)
	u := storagev1.Upload{UploadID: id, Scope: in.Scope, Class: in.Class, State: "pending", SizeBytes: in.SizeBytes, SHA256: in.SHA256, WriterToken: "writer-" + id, LeaseUntil: time.Now().Add(time.Hour), CreatedAt: time.Now().UTC()}
	f.creates[in.IdempotencyKey] = id
	f.uploads[id] = u
	return u, nil
}
func (f *storageFixture) GetUpload(_ context.Context, id string) (storagev1.Upload, error) {
	if f.failReadAfterPut && f.uploads[id].Class == storagev1.ClassResult && f.uploads[id].State == "uploaded" {
		f.failReadAfterPut = false
		return storagev1.Upload{}, errors.New("crash before finalize intent")
	}
	u, ok := f.uploads[id]
	if !ok {
		return u, errors.New("missing upload")
	}
	return u, nil
}

func TestPublicationRecoversUploadedPDFWithoutStagingOrFinalizeIntent(t *testing.T) {
	s, _ := privateStore(t)
	root := t.TempDir()
	job, a := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.failReadAfterPut = true
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("injected crash was not observed")
	}
	if err = os.RemoveAll(a.PrivateStagingPath); err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	done, _ := s.GetJob(context.Background(), job.JobID)
	if done.Status != generationv1.Succeeded || storage.putCalls != 1 {
		t.Fatalf("no-file recovery: %+v puts=%d", done, storage.putCalls)
	}
}
func (f *storageFixture) RenewUpload(_ context.Context, id string, in storagev1.RenewUploadRequest) (storagev1.Upload, error) {
	u := f.uploads[id]
	if in.WriterToken != u.WriterToken {
		return u, errors.New("wrong writer")
	}
	u.LeaseUntil = time.Now().Add(time.Hour)
	f.uploads[id] = u
	return u, nil
}
func (f *storageFixture) PutUpload(_ context.Context, id, writer string, r io.Reader) (storagev1.Upload, error) {
	f.putCalls++
	u := f.uploads[id]
	data, err := io.ReadAll(r)
	h := sha256.Sum256(data)
	if err != nil || writer != u.WriterToken || int64(len(data)) != u.SizeBytes || hex.EncodeToString(h[:]) != u.SHA256 {
		return u, errors.New("invalid upload")
	}
	u.State = "uploaded"
	f.uploads[id] = u
	return u, nil
}
func (f *storageFixture) FinalizeArtifact(_ context.Context, id string, in storagev1.FinalizeArtifactRequest) (storagev1.Artifact, error) {
	f.finalizeCalls++
	f.requests = append(f.requests, in)
	if a, ok := f.artifacts[id]; ok {
		return a, nil
	}
	u := f.uploads[id]
	now := time.Now().UTC()
	a := storagev1.Artifact{ArtifactID: "artifact-" + id, Scope: u.Scope, OriginJobID: in.OriginJobID, SnapshotID: in.SnapshotID, Hash: u.SHA256, SizeBytes: u.SizeBytes, Metadata: in.Metadata, CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
	f.artifacts[id] = a
	u.State = "committed"
	u.ArtifactID = a.ArtifactID
	f.uploads[id] = u
	if f.failFinalize {
		f.failFinalize = false
		return storagev1.Artifact{}, errors.New("lost finalize acknowledgement")
	}
	return a, nil
}
func (*storageFixture) GetSnapshot(context.Context, string) (storagev1.Snapshot, error) {
	return storagev1.Snapshot{}, errors.New("unexpected snapshot read")
}
func (*storageFixture) ResourceContent(context.Context, string) (*http.Response, error) {
	return nil, errors.New("unexpected resource read")
}
func (f *storageFixture) CommitResource(_ context.Context, id string, in storagev1.CommitResourceRequest) (storagev1.Resource, error) {
	u := f.uploads[id]
	if u.WriterToken != in.WriterToken {
		return storagev1.Resource{}, errors.New("wrong resource writer")
	}
	u.State = "committed"
	u.ResourceID = "resource-" + id
	f.uploads[id] = u
	return storagev1.Resource{ResourceID: u.ResourceID, Scope: u.Scope, Class: u.Class, SizeBytes: u.SizeBytes, SHA256: u.SHA256, CreatedAt: u.CreatedAt}, nil
}
func (f *storageFixture) CreateSnapshot(_ context.Context, in storagev1.CreateSnapshotRequest) (storagev1.Snapshot, error) {
	if f.tamperSnapshotRefs {
		in.Resources = append([]storagev1.ResourceRef(nil), in.Resources...)
		in.Resources[0].ResourceID = "substituted"
	}
	return storagev1.Snapshot{SnapshotID: "snapshot-" + in.IdempotencyKey, Scope: in.Scope, Manifest: in.Manifest, Resources: in.Resources}, nil
}

func TestPublicationRejectsSnapshotReceiptWithSubstitutedResource(t *testing.T) {
	s, _ := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root, true)
	storage := newStorageFixture()
	storage.tamperSnapshotRefs = true
	p, _ := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err := p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("substituted snapshot resource receipt was published")
	}
	if storage.finalizeCalls != 0 {
		t.Fatal("invalid derived snapshot reached finalize")
	}
}

func TestPublicationWriterCredentialsAreScrubbedWithSensitiveHistory(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root, true)
	storage := newStorageFixture()
	p, _ := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err := p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE generation_jobs SET terminal_at=clock_timestamp()-interval '25 hours' WHERE job_id=$1`, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retain(context.Background()); err != nil {
		t.Fatal(err)
	}
	var credentials int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_operation_intents WHERE job_id=$1 AND (strpos(convert_from(request,'UTF8'),'writer-upload')>0 OR strpos(convert_from(COALESCE(output,''::bytea),'UTF8'),'writer-upload')>0)`, job.JobID).Scan(&credentials); err != nil {
		t.Fatal(err)
	}
	if credentials != 0 {
		t.Fatalf("writer credentials retained after sensitive TTL: %d records", credentials)
	}
}
func (*storageFixture) ReleasePin(context.Context, string) error { return nil }

func preparedOutput(t *testing.T, s *store.Store, root string, withParameters ...bool) (generationv1.Job, store.Attempt) {
	t.Helper()
	ctx := context.Background()
	principal := storagev1.Principal{Subject: "actor", Domain: "user"}
	actor, _ := service.EncodeInitiator(principal)
	raw := []byte(`{"scope":{"workspaceId":"workspace","projectId":"project"},"snapshotId":"snapshot-base","targetId":"pdf","entrypoint":"main.typ"}`)
	if len(withParameters) > 0 && withParameters[0] {
		raw = append(raw[:len(raw)-1], []byte(`,"parameters":{"n":1.0},"parametersSchemaPath":"schema.json"}`)...)
	}
	req, normalized, fp, err := generationv1.NormalizeRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	admission, _, err := s.Admit(ctx, store.AdmitInput{WorkspaceID: req.Scope.WorkspaceID, ProjectID: req.Scope.ProjectID, InitiatorID: actor, IdempotencyKey: "test-job", NormalizedRequest: normalized, Parameters: req.Parameters, Fingerprint: fp, EngineVersion: "0.15.1", SnapshotID: req.SnapshotID, TargetID: req.TargetID, Entrypoint: req.Entrypoint})
	if err != nil {
		t.Fatal(err)
	}
	job := admission.Job
	acquire := storagev1.AcquirePinRequest{Scope: req.Scope, ObjectID: req.SnapshotID, HolderID: "leafrun-generation", HolderDomain: "service", RetentionPolicy: "ordinary", IdempotencyKey: "pin-key"}
	if _, err = s.BeginPinAcquireIntent(ctx, store.PinAcquireIntent{JobID: job.JobID, IdempotencyKey: acquire.IdempotencyKey, Request: acquire}); err != nil {
		t.Fatal(err)
	}
	pin := storagev1.Pin{PinID: "pin", Scope: req.Scope, ObjectID: req.SnapshotID, HolderID: acquire.HolderID, HolderDomain: acquire.HolderDomain, RetentionPolicy: "ordinary", State: "active", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Hour)}
	if err = s.RecordPinAcquireResponse(ctx, job.JobID, acquire.IdempotencyKey, pin); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkPinActiveAndRecordWorkflowStart(ctx, job.JobID, acquire.IdempotencyKey, "render/"+job.JobID, pin, []byte(fmt.Sprintf(`{"jobId":%q}`, job.JobID))); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "stage-test")
	if err = os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	a, err := s.ClaimExecution(ctx, store.ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: "create", StagingToken: "stage-test", PrivateStagingPath: path})
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 64)
	if err = s.RecordContainer(ctx, a.AttemptID, a.FencingEpoch, id); err != nil {
		t.Fatal(err)
	}
	a.ContainerID = id
	if err = s.RecordSourceIdentity(ctx, job.JobID, "source-content"); err != nil {
		t.Fatal(err)
	}
	job.SourceContentID = "source-content"
	if err = s.MarkRendering(ctx, a.AttemptID, a.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	expected := renderRequest(job, a, store.ExecutionInput{Request: req})
	pdf := []byte("%PDF-1.7\nverified content")
	hash := sha256.Sum256(pdf)
	desc := renderwire.PDFDescriptor{Size: int64(len(pdf)), SHA256: hex.EncodeToString(hash[:])}
	terminal := renderwire.TerminalEnvelope{Complete: true, Success: true, Scope: job.Scope, JobID: job.JobID, AttemptID: a.AttemptID, TargetID: job.TargetID, SourceSnapshotID: job.SourceSnapshotID, SourceContentID: job.SourceContentID, InputFingerprint: job.InputFingerprint, EngineVersion: "0.15.1", Phase: "complete", PDF: &desc}
	var stream bytes.Buffer
	e := renderwire.NewEncoder(&stream, renderwire.OutputLimit)
	if len(req.Parameters) > 0 {
		payload := req.Parameters
		h := sha256.Sum256(payload)
		manifest := storagev1.SnapshotManifest{FormatVersion: 1, Files: []storagev1.SnapshotFile{{Path: "input.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(h[:])}}, Directories: []string{}, Fonts: []storagev1.SnapshotFont{}}
		manifest.ID, err = storagev1.ManifestContentID(manifest)
		if err != nil {
			t.Fatal(err)
		}
		terminal.DerivedContentID = manifest.ID
		if err = renderwire.WriteBundle(e, renderwire.Bundle{Manifest: manifest, ProjectFS: fstest.MapFS{"input.json": &fstest.MapFile{Data: payload}}}); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := json.Marshal(desc)
	_ = e.WriteBytes(renderwire.PDFHeader, b)
	_ = e.WriteBytes(renderwire.PDF, pdf)
	b, _ = json.Marshal(terminal)
	_ = e.WriteBytes(renderwire.Terminal, b)
	out, err := sandbox.ReadOutput(ctx, &stream, path, expected)
	if err != nil {
		t.Fatal(err)
	}
	spec := sandbox.Spec{JobID: job.JobID, AttemptID: a.AttemptID, CreateKey: a.CreateContainerIntentKey, Image: "sha256:" + strings.Repeat("b", 64)}
	if err = out.Confirm(spec, sandbox.State{ID: id, Status: "exited", ExitCode: 0}); err != nil {
		t.Fatal(err)
	}
	intent := store.IntentInput{JobID: job.JobID, RenderAttemptID: a.AttemptID, AttemptEpoch: a.FencingEpoch, Kind: "render.output", IdempotencyKey: "output", Request: []byte(`{"formatVersion":1}`), Sensitive: true}
	if _, err = s.SaveIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	record, _ := json.Marshal(outputRecord{Token: out.Token(), Spec: spec, Terminal: out.Terminal})
	if _, err = s.CompleteIntent(ctx, intent, record); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptStopped(ctx, a.AttemptID, a.FencingEpoch, id); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkAttemptRendered(ctx, a.AttemptID, a.FencingEpoch); err != nil {
		t.Fatal(err)
	}
	return job, a
}

func TestPublicationRecoversDerivedResourcesAndPDFWithoutStaging(t *testing.T) {
	s, _ := privateStore(t)
	root := t.TempDir()
	job, a := preparedOutput(t, s, root, true)
	storage := newStorageFixture()
	storage.failReadAfterPut = true
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("injected crash was not observed")
	}
	puts := storage.putCalls
	if puts != 2 {
		t.Fatalf("expected one resource and PDF upload before crash, got %d", puts)
	}
	if err = os.RemoveAll(a.PrivateStagingPath); err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	done, _ := s.GetJob(context.Background(), job.JobID)
	if done.Status != generationv1.Succeeded || done.DerivedSnapshotID == "" || storage.putCalls != puts {
		t.Fatalf("derived receipt recovery: %+v puts=%d", done, storage.putCalls)
	}
}

func TestPublicationRecoversLostFinalizeWithoutRerenderOrReupload(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.failFinalize = true
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("lost response must remain unresolved")
	}
	middle, _ := s.GetJob(context.Background(), job.JobID)
	if middle.Status != generationv1.Publishing || middle.Artifact != nil {
		t.Fatalf("published before receipt: %+v", middle)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	done, _ := s.GetJob(context.Background(), job.JobID)
	if done.Status != generationv1.Succeeded || done.Artifact == nil || storage.putCalls != 1 || storage.finalizeCalls != 2 {
		t.Fatalf("recovery: %+v puts=%d finalizes=%d", done, storage.putCalls, storage.finalizeCalls)
	}
	if storage.requests[0] != storage.requests[1] {
		t.Fatal("finalize request changed during recovery")
	}
	allowed, err := s.Owner().Publishable(context.Background(), storagev1.PublishabilityCheck{Scope: job.Scope, OriginJobID: job.JobID, ArtifactID: done.Artifact.ArtifactID})
	if err != nil || !allowed {
		t.Fatalf("owner: %v %v", allowed, err)
	}
	var wire []byte
	if err = pool.QueryRow(context.Background(), `SELECT o.encoded_envelope FROM platform_event_outbox o JOIN generation_terminal_outbox t ON t.event_id=o.event_id WHERE t.job_id=$1`, job.JobID).Scan(&wire); err != nil {
		t.Fatal(err)
	}
	event, err := events.Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	var data events.GenerationCompletedData
	if err = json.Unmarshal(event.Data, &data); err != nil {
		t.Fatal(err)
	}
	if event.Producer != "leafrun-generation" || string(event.Data) != fmt.Sprintf(`{"jobId":%q}`, job.JobID) {
		t.Fatalf("completion event incompatible with consumer: %+v %s", event, event.Data)
	}
}

func TestPublicationFailsClosedWhenAccessIsUnknown(t *testing.T) {
	s, _ := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{err: errors.New("offline")}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("uncertain access allowed publication")
	}
	if storage.putCalls != 0 || storage.finalizeCalls != 0 {
		t.Fatal("Storage called despite uncertain access")
	}
}
