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
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
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

type selectionPublicationAccess struct {
	allow  bool
	err    error
	checks []storagev1.AccessCheck
}

func (a *selectionPublicationAccess) Check(_ context.Context, check storagev1.AccessCheck) (bool, error) {
	a.checks = append(a.checks, check)
	return a.allow, a.err
}

type selectionPublicationProjects struct {
	decision projectsv1.CurrentReleaseDecision
	err      error
	check    projectsv1.CurrentReleaseCheck
	calls    int
}

func (*selectionPublicationProjects) Resolve(context.Context, projectsv1.ResolveRequest) (projectsv1.ResolvedExecution, error) {
	return projectsv1.ResolvedExecution{}, errors.New("unexpected render resolution")
}

func (p *selectionPublicationProjects) CurrentRelease(_ context.Context, check projectsv1.CurrentReleaseCheck) (projectsv1.CurrentReleaseDecision, error) {
	p.calls++
	p.check = check
	return p.decision, p.err
}

func TestSelectionPublicationAuthorizationTracksCurrentReleaseWithoutSourceRead(t *testing.T) {
	actor := storagev1.Principal{Subject: "actor", Domain: "user"}
	scope := storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}
	digest := strings.Repeat("a", 64)
	access := &selectionPublicationAccess{allow: true}
	projects := &selectionPublicationProjects{decision: projectsv1.CurrentReleaseDecision{Allowed: true}}
	publisher := &Publisher{access: access, cfg: Config{Projects: projects}}
	job := generationv1.Job{JobID: "gen_job", Scope: scope, SourceSnapshotID: "snapshot-1"}
	input := store.ExecutionInput{
		Selection: &generationv1.RenderSelectionRequest{Scope: scope, ReleaseID: "release-1"},
		Execution: &projectsv1.ResolvedExecution{Actor: actor, Scope: scope, ReleaseID: "release-1", Descriptor: projectsv1.TargetDescriptor{Digest: digest}},
	}
	allowed, err := publisher.selectionStillAuthorized(context.Background(), job, input)
	if err != nil || !allowed {
		t.Fatalf("published release authorization = %v, %v", allowed, err)
	}
	if len(access.checks) != 1 || access.checks[0].Operation != "generation.create" || access.checks[0].Principal != actor {
		t.Fatalf("release rights checks = %+v", access.checks)
	}
	if projects.calls != 1 || projects.check.Actor != actor || projects.check.Scope != scope || projects.check.ReleaseID != "release-1" || projects.check.DescriptorDigest != digest {
		t.Fatalf("current release check = %+v calls=%d", projects.check, projects.calls)
	}
	projects.decision.Allowed = false
	allowed, err = publisher.selectionStillAuthorized(context.Background(), job, input)
	if err != nil || allowed {
		t.Fatalf("withdrawn release authorization = %v, %v", allowed, err)
	}
	projects.err = errors.New("Projects unavailable")
	allowed, err = publisher.selectionStillAuthorized(context.Background(), job, input)
	if err == nil || allowed {
		t.Fatalf("uncertain release authorization = %v, %v", allowed, err)
	}
	if len(access.checks) != 3 || access.checks[2].Operation != "generation.create" {
		t.Fatalf("release publication checked an unexpected permission: %+v", access.checks)
	}
}

func TestWorkingSelectionPublicationRequiresCurrentSourceRead(t *testing.T) {
	actor := storagev1.Principal{Subject: "actor", Domain: "user"}
	scope := storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}
	access := &selectionPublicationAccess{allow: true}
	publisher := &Publisher{access: access}
	job := generationv1.Job{JobID: "gen_job", Scope: scope, SourceSnapshotID: "snapshot-1"}
	input := store.ExecutionInput{
		Selection: &generationv1.RenderSelectionRequest{Scope: scope, SnapshotID: job.SourceSnapshotID},
		Execution: &projectsv1.ResolvedExecution{Actor: actor, Scope: scope, SnapshotID: job.SourceSnapshotID},
	}
	allowed, err := publisher.selectionStillAuthorized(context.Background(), job, input)
	if err != nil || !allowed {
		t.Fatalf("working selection authorization = %v, %v", allowed, err)
	}
	if len(access.checks) != 2 || access.checks[0].Operation != "generation.create" || access.checks[1].Operation != "source.read" || access.checks[1].ObjectID != job.SourceSnapshotID {
		t.Fatalf("working selection rights checks = %+v", access.checks)
	}
}

type pinFixture struct{}

func (pinFixture) EnsureDerivedPin(context.Context, generationv1.Job, string) error { return nil }

type storageFixture struct {
	uploads                 map[string]storagev1.Upload
	creates                 map[string]string
	artifacts               map[string]storagev1.Artifact
	resources               map[string]storagev1.Resource
	createRequests          []storagev1.CreateUploadRequest
	requests                []storagev1.FinalizeArtifactRequest
	finalizeUploadIDs       []string
	commitErrors            []error
	finalizeErrors          []error
	releaseErrors           []error
	releasedPins            []string
	releaseStates           []storagev1.PinReleaseState
	releaseTamper           bool
	onExpiredRenew          func()
	expireFirstCreate       bool
	expireAfterPutAt        int
	failFinalize            bool
	failCommit              bool
	failReadAfterPut        bool
	tamperSnapshotRefs      bool
	finalizeCalls, putCalls int
	commitCalls             int
}

func newStorageFixture() *storageFixture {
	return &storageFixture{uploads: map[string]storagev1.Upload{}, creates: map[string]string{}, artifacts: map[string]storagev1.Artifact{}, resources: map[string]storagev1.Resource{}}
}
func (f *storageFixture) CreateUpload(_ context.Context, in storagev1.CreateUploadRequest) (storagev1.Upload, error) {
	f.createRequests = append(f.createRequests, in)
	if id := f.creates[in.IdempotencyKey]; id != "" {
		return f.uploads[id], nil
	}
	id := fmt.Sprintf("upload-%d", len(f.uploads)+1)
	u := storagev1.Upload{UploadID: id, Scope: in.Scope, Class: in.Class, State: "created", SizeBytes: in.SizeBytes, SHA256: in.SHA256, WriterToken: "writer-" + id, LeaseUntil: time.Now().Add(time.Hour), CreatedAt: time.Now().UTC()}
	if f.expireFirstCreate && len(f.uploads) == 0 {
		u.LeaseUntil = time.Now().Add(-time.Second)
	}
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
	if !time.Now().Before(u.LeaseUntil) {
		if f.onExpiredRenew != nil {
			f.onExpiredRenew()
		}
		return u, writerExpiredError()
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
	if f.expireAfterPutAt == f.putCalls {
		u.LeaseUntil = time.Now().Add(-time.Second)
	}
	f.uploads[id] = u
	return u, nil
}
func (f *storageFixture) FinalizeArtifact(_ context.Context, id string, in storagev1.FinalizeArtifactRequest) (storagev1.Artifact, error) {
	f.finalizeCalls++
	f.requests = append(f.requests, in)
	f.finalizeUploadIDs = append(f.finalizeUploadIDs, id)
	if a, ok := f.artifacts[id]; ok {
		return a, nil
	}
	u := f.uploads[id]
	if f.finalizeCalls <= len(f.finalizeErrors) && f.finalizeErrors[f.finalizeCalls-1] != nil {
		return storagev1.Artifact{}, f.finalizeErrors[f.finalizeCalls-1]
	}
	if !time.Now().Before(u.LeaseUntil) {
		return storagev1.Artifact{}, writerExpiredError()
	}
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
	f.commitCalls++
	if r, ok := f.resources[id]; ok {
		return r, nil
	}
	if f.commitCalls <= len(f.commitErrors) && f.commitErrors[f.commitCalls-1] != nil {
		return storagev1.Resource{}, f.commitErrors[f.commitCalls-1]
	}
	u := f.uploads[id]
	if u.WriterToken != in.WriterToken {
		return storagev1.Resource{}, errors.New("wrong resource writer")
	}
	if !time.Now().Before(u.LeaseUntil) {
		return storagev1.Resource{}, writerExpiredError()
	}
	u.State = "committed"
	u.ResourceID = "resource-" + id
	f.uploads[id] = u
	r := storagev1.Resource{ResourceID: u.ResourceID, Scope: u.Scope, Class: u.Class, SizeBytes: u.SizeBytes, SHA256: u.SHA256, CreatedAt: u.CreatedAt}
	f.resources[id] = r
	if f.failCommit {
		f.failCommit = false
		return storagev1.Resource{}, errors.New("lost resource commit acknowledgement")
	}
	return r, nil
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
func (f *storageFixture) ReleasePin(_ context.Context, id string) error {
	f.releasedPins = append(f.releasedPins, id)
	index := len(f.releasedPins) - 1
	if index < len(f.releaseErrors) {
		return f.releaseErrors[index]
	}
	return nil
}

func (f *storageFixture) ReleasePinBound(ctx context.Context, id string, binding storagev1.PinReleaseRequest) (storagev1.PinReleaseReceipt, error) {
	if err := f.ReleasePin(ctx, id); err != nil {
		return storagev1.PinReleaseReceipt{}, err
	}
	state := storagev1.PinReleaseReleased
	index := len(f.releasedPins) - 1
	if index < len(f.releaseStates) {
		state = f.releaseStates[index]
	}
	receipt := storagev1.PinReleaseReceipt{PinID: id, Binding: binding, State: state}
	if f.releaseTamper {
		receipt.Binding.ObjectID = "different-object"
	}
	return receipt, nil
}

func writerExpiredError() error {
	return &storagev1.HTTPError{Status: http.StatusConflict, Code: "writer_expired", Message: "upload writer lease expired"}
}

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
	stageToken := "0123456789abcdef0123456789abcdef"
	path := filepath.Join(root, stageToken)
	if err = os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	a, err := s.ClaimExecution(ctx, store.ClaimAttemptInput{JobID: job.JobID, CreateContainerIntentKey: "create", StagingToken: stageToken, PrivateStagingPath: path})
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

func TestPublicationRotatesExpiredPDFUploadBeforePutWithoutRerender(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.expireFirstCreate = true
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	assertOneRenderAttempt(t, pool, job.JobID)
	if len(storage.createRequests) != 2 || storage.createRequests[0].IdempotencyKey == storage.createRequests[1].IdempotencyKey || storage.putCalls != 1 || storage.finalizeCalls != 1 || storage.finalizeUploadIDs[0] != "upload-2" {
		t.Fatalf("expired pre-PUT writer was not replaced with a fresh upload: creates=%+v puts=%d finalizes=%d uploadIDs=%v", storage.createRequests, storage.putCalls, storage.finalizeCalls, storage.finalizeUploadIDs)
	}
}

func TestPublicationCannotRotateAfterPublisherLeaseTakeover(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.expireFirstCreate = true
	storage.onExpiredRenew = func() {
		storage.onExpiredRenew = nil
		if _, err := pool.Exec(context.Background(), `UPDATE generation_jobs SET publication_lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, job.JobID); err != nil {
			t.Fatalf("expire old publisher lease: %v", err)
		}
		if _, err := s.BeginPublication(context.Background(), job.JobID, "replacement-publisher"); err != nil {
			t.Fatalf("take over publication lease: %v", err)
		}
	}
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale publisher should fail before rotating upload: %v", err)
	}
	if len(storage.createRequests) != 1 || len(storage.uploads) != 1 || storage.putCalls != 0 {
		t.Fatalf("stale publisher created or wrote a replacement upload: creates=%d uploads=%d puts=%d", len(storage.createRequests), len(storage.uploads), storage.putCalls)
	}
}

func TestPublicationRotatesExpiredUploadedPDFBeforeFinalizeWithoutRerender(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.expireAfterPutAt = 1
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	assertOneRenderAttempt(t, pool, job.JobID)
	if len(storage.createRequests) != 2 || storage.createRequests[0].IdempotencyKey == storage.createRequests[1].IdempotencyKey || storage.putCalls != 2 || storage.finalizeCalls != 2 || storage.finalizeUploadIDs[0] == storage.finalizeUploadIDs[1] {
		t.Fatalf("expired uploaded writer was not safely replaced: creates=%+v puts=%d finalizes=%d uploadIDs=%v", storage.createRequests, storage.putCalls, storage.finalizeCalls, storage.finalizeUploadIDs)
	}
}

func TestPublicationRotatesPersistedFinalizeOnlyAfterExactWriterExpiredRejection(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.finalizeErrors = []error{errors.New("lost finalize response"), writerExpiredError()}
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("uncertain finalize response must remain unresolved")
	}
	if len(storage.createRequests) != 1 || storage.putCalls != 1 {
		t.Fatalf("transport uncertainty caused an unsafe rotation: creates=%d puts=%d", len(storage.createRequests), storage.putCalls)
	}
	for id, upload := range storage.uploads {
		upload.LeaseUntil = time.Now().Add(-time.Second)
		storage.uploads[id] = upload
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	assertOneRenderAttempt(t, pool, job.JobID)
	if len(storage.createRequests) != 2 || storage.putCalls != 2 || storage.finalizeCalls != 3 || storage.requests[0] != storage.requests[1] || storage.finalizeUploadIDs[0] != storage.finalizeUploadIDs[1] || storage.finalizeUploadIDs[2] == storage.finalizeUploadIDs[1] || storage.requests[2].IdempotencyKey == storage.requests[1].IdempotencyKey {
		t.Fatalf("expired finalize intent was not replayed then rotated: creates=%d puts=%d finalizes=%d requests=%+v uploadIDs=%v", len(storage.createRequests), storage.putCalls, storage.finalizeCalls, storage.requests, storage.finalizeUploadIDs)
	}
}

func TestPublicationDoesNotRotateAfterCommittedFinalizeLostACKAndExpiredLease(t *testing.T) {
	s, _ := privateStore(t)
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
	for id, upload := range storage.uploads {
		upload.LeaseUntil = time.Now().Add(-time.Second)
		storage.uploads[id] = upload
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	if len(storage.createRequests) != 1 || len(storage.uploads) != 1 || storage.putCalls != 1 || storage.finalizeCalls != 2 || storage.requests[0] != storage.requests[1] {
		t.Fatalf("committed finalize was rotated after lost ACK: creates=%d uploads=%d puts=%d finalizes=%d", len(storage.createRequests), len(storage.uploads), storage.putCalls, storage.finalizeCalls)
	}
}

func TestPublicationDoesNotRotateOnOtherConflict(t *testing.T) {
	s, _ := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root)
	storage := newStorageFixture()
	storage.finalizeErrors = []error{&storagev1.HTTPError{Status: http.StatusConflict, Code: "idempotency_conflict", Message: "request changed"}}
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("other Storage conflict must be returned")
	}
	if len(storage.createRequests) != 1 || len(storage.uploads) != 1 || storage.putCalls != 1 || storage.finalizeCalls != 1 {
		t.Fatalf("non-expiry conflict caused upload rotation: creates=%d uploads=%d puts=%d finalizes=%d", len(storage.createRequests), len(storage.uploads), storage.putCalls, storage.finalizeCalls)
	}
}

func TestPublicationRotatesExpiredDerivedResourceWriterWithoutRerender(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root, true)
	storage := newStorageFixture()
	storage.commitErrors = []error{writerExpiredError()}
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	assertOneRenderAttempt(t, pool, job.JobID)
	done, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != generationv1.Succeeded || done.DerivedSnapshotID == "" || len(storage.createRequests) != 3 || storage.putCalls != 3 || storage.commitCalls != 2 {
		t.Fatalf("expired derived resource writer was not rotated: job=%+v creates=%d puts=%d commits=%d", done, len(storage.createRequests), storage.putCalls, storage.commitCalls)
	}
}

func TestPublicationDoesNotRotateAfterCommittedResourceLostACKAndExpiredLease(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, _ := preparedOutput(t, s, root, true)
	storage := newStorageFixture()
	storage.failCommit = true
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err == nil {
		t.Fatal("lost resource commit acknowledgement must remain unresolved")
	}
	for id, upload := range storage.uploads {
		upload.LeaseUntil = time.Now().Add(-time.Second)
		storage.uploads[id] = upload
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	assertOneRenderAttempt(t, pool, job.JobID)
	done, err := s.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != generationv1.Succeeded || len(storage.uploads) != 2 || len(storage.createRequests) != 2 || storage.putCalls != 2 || storage.commitCalls != 2 {
		t.Fatalf("committed resource was rotated after lost ACK: job=%+v uploads=%d creates=%d puts=%d commits=%d", done, len(storage.uploads), len(storage.createRequests), storage.putCalls, storage.commitCalls)
	}
}

func TestPublicationRemovesStagingAfterDurableTerminalCleanupIsScheduled(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, attempt := preparedOutput(t, s, root)
	storage := newStorageFixture()
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(attempt.PrivateStagingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal attempt staging was not removed: stat err=%v", err)
	}
	var scheduled int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_cleanup_intents WHERE job_id=$1 AND kind='attempt.files' AND idempotency_key=$2 AND state<>'complete'`, job.JobID, attempt.AttemptID).Scan(&scheduled); err != nil {
		t.Fatal(err)
	}
	if scheduled != 1 {
		t.Fatalf("stage was removed without durable cleanup intent: intents=%d", scheduled)
	}
}

func TestPublicationKeepsStagingWhenDurableCleanupSchedulingFails(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, attempt := preparedOutput(t, s, root)
	if _, err := pool.Exec(context.Background(), `CREATE FUNCTION block_attempt_file_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='attempt.files' THEN RAISE EXCEPTION 'injected cleanup insert failure'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `CREATE TRIGGER block_attempt_file_cleanup BEFORE INSERT ON generation_cleanup_intents FOR EACH ROW EXECUTE FUNCTION block_attempt_file_cleanup()`); err != nil {
		t.Fatal(err)
	}
	storage := newStorageFixture()
	p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), job.JobID); err != nil {
		t.Fatalf("committed publication should not fail on best-effort cleanup scheduling: %v", err)
	}
	done, err := s.GetJob(context.Background(), job.JobID)
	if err != nil || done.Status != generationv1.Succeeded {
		t.Fatalf("cleanup failure changed terminal success: job=%+v err=%v", done, err)
	}
	if _, err = os.Stat(attempt.PrivateStagingPath); err != nil {
		t.Fatalf("staging was removed without durable cleanup intent: %v", err)
	}
	attempts, err := s.TerminalAttempts(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].AttemptID != attempt.AttemptID {
		t.Fatalf("janitor fallback cannot recover unscheduled terminal staging: %+v", attempts)
	}
}

func assertOneRenderAttempt(t *testing.T, pool *pgxpool.Pool, jobID string) {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM generation_attempts WHERE job_id=$1`, jobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("publication recovery rerendered: attempts=%d", count)
	}
}

func TestPublicationUploadRotationBudgetSurvivesPublisherRestart(t *testing.T) {
	s, pool := privateStore(t)
	root := t.TempDir()
	job, attempt := preparedOutput(t, s, root)
	storage := newStorageFixture()
	for range 16 {
		storage.finalizeErrors = append(storage.finalizeErrors, writerExpiredError())
	}
	for range 2 {
		p, err := NewPublisher(s, storage, pinFixture{}, accessFixture{allow: true}, Config{StagingRoot: root, OwnerID: "publisher"})
		if err != nil {
			t.Fatal(err)
		}
		if err = p.Publish(context.Background(), job.JobID); err == nil {
			t.Fatal("repeated writer expiry exceeded its recovery budget")
		}
		if len(storage.uploads) != 4 || len(storage.createRequests) != 4 {
			t.Fatalf("publisher restart reset the upload replacement budget: uploads=%d creates=%d", len(storage.uploads), len(storage.createRequests))
		}
	}
	assertOneRenderAttempt(t, pool, job.JobID)
	current, err := s.GetJob(context.Background(), job.JobID)
	if err != nil || current.Status != generationv1.Publishing || current.Artifact != nil || !current.Deadline.Equal(job.Deadline) {
		t.Fatalf("replacement budget changed the original job/expiry: %+v, %v", current, err)
	}
	if _, err = os.Stat(attempt.PrivateStagingPath); err != nil {
		t.Fatalf("unpublished output was discarded after bounded recovery: %v", err)
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
