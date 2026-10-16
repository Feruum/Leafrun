package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Feruum/Leafrun/internal/service"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"io"
	"path/filepath"
	"reflect"
	"time"

	"github.com/Feruum/Leafrun/internal/sandbox"
	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	renderwire "github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"go.temporal.io/sdk/activity"
)

type outputRecord struct {
	Token    string                      `json:"token"`
	Spec     sandbox.Spec                `json:"spec"`
	Terminal renderwire.TerminalEnvelope `json:"terminal"`
}
type finalizeIntent struct {
	UploadID string                            `json:"uploadId"`
	Request  storagev1.FinalizeArtifactRequest `json:"request"`
	SHA256   string                            `json:"sha256"`
	Size     int64                             `json:"size"`
	Scope    storagev1.Scope                   `json:"scope"`
}

const (
	intentUploadRotate = "upload.rotate"
	maxUploadRotations = 3
)

type uploadKeys struct {
	Create   string `json:"create"`
	Put      string `json:"put"`
	Commit   string `json:"commit"`
	Finalize string `json:"finalize"`
}

type uploadRotation struct {
	PreviousUploadID string     `json:"previousUploadId"`
	Keys             uploadKeys `json:"keys"`
}

// effect replays identical bytes across publication epochs. Each new owner
// saves its own fenced intent before using any earlier receipt or making IO.
func effect[Request, Response any](ctx context.Context, p *Publisher, a store.Attempt, l store.PublicationLease, kind, key string, request Request, call func(Request) (Response, error)) (Response, error) {
	var result Response
	data, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	old, err := p.state.FindIntent(ctx, a.JobID, a.AttemptID, kind, key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return result, err
	}
	if err == nil {
		data = old.Request
		if err = jsonvalue.Unmarshal(data, &request, 16<<20); err != nil {
			return result, err
		}
	}
	in := store.IntentInput{JobID: a.JobID, RenderAttemptID: a.AttemptID, AttemptEpoch: a.FencingEpoch, PublicationEpoch: l.Epoch, Owner: l.Owner, Kind: kind, IdempotencyKey: key, Request: data, Sensitive: true}
	saved, err := p.state.SaveIntent(ctx, in)
	if err != nil {
		return result, err
	}
	output := saved.Output
	if len(output) == 0 && old.CompletedAt != nil {
		output = old.Output
	}
	if len(output) > 0 {
		if err = jsonvalue.Unmarshal(output, &result, 16<<20); err != nil {
			return result, err
		}
	} else {
		result, err = call(request)
		if err != nil {
			return result, err
		}
		output, err = json.Marshal(result)
		if err != nil {
			return result, err
		}
	}
	_, err = p.state.CompleteIntent(ctx, in, output)
	return result, err
}

func stableKey(a store.Attempt, kind string) string {
	hash := sha256.Sum256([]byte(a.AttemptID + "\x00" + kind))
	return "gen_" + hex.EncodeToString(hash[:])
}

func (p *Publisher) output(ctx context.Context, job generationv1.Job, a store.Attempt, in store.ExecutionInput) (outputRecord, error) {
	intent, err := p.state.FindIntent(ctx, job.JobID, a.AttemptID, "render.output", "output")
	if err != nil {
		return outputRecord{}, err
	}
	var record outputRecord
	if intent.CompletedAt == nil || jsonvalue.Unmarshal(intent.Output, &record, 2<<20) != nil {
		return record, errors.New("confirmed renderer receipt is missing")
	}
	t := record.Terminal
	r := renderRequest(job, a, in)
	if !t.Complete || !t.Success || t.PDF == nil || t.JobID != r.JobID || t.AttemptID != r.AttemptID || t.Scope != r.Scope || t.SourceSnapshotID != r.SourceSnapshotID || t.SourceContentID != r.SourceContentID || t.TargetID != r.TargetID || t.InputFingerprint != r.InputFingerprint || t.EngineVersion != "0.15.1" || record.Spec.JobID != job.JobID || record.Spec.AttemptID != a.AttemptID || record.Spec.CreateKey != a.CreateContainerIntentKey {
		return record, errors.New("persisted renderer receipt bindings differ")
	}
	return record, nil
}

func (p *Publisher) Publish(ctx context.Context, jobID string) error {
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx)
	}
	job, err := p.state.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job.Status.Terminal() {
		return nil
	}
	input, err := p.state.GetExecutionInput(ctx, jobID)
	if err != nil {
		return err
	}
	ctx, err = service.RestoreExecutionAuthority(ctx, input)
	if err != nil {
		return err
	}
	allowed, err := p.checkAccess(ctx, job, input, "generation.publish")
	if err != nil {
		return err
	}
	lease, err := p.state.BeginPublication(ctx, jobID, p.cfg.OwnerID)
	if err != nil {
		return err
	}
	if !allowed {
		return p.state.FailPublication(ctx, lease.Owner, lease.Epoch, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
	}
	publishCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopHeartbeat := p.heartbeatPublication(publishCtx, cancel, lease)
	defer stopHeartbeat()
	a, err := p.state.CurrentAttempt(publishCtx, jobID)
	if err != nil {
		return err
	}
	record, err := p.output(publishCtx, job, a, input)
	if err != nil {
		return err
	}
	var candidate store.PublicationCandidate
	if lease.Candidate != nil {
		candidate = *lease.Candidate
	} else {
		candidate, err = p.finalize(publishCtx, job, a, input, lease, record)
		if errors.Is(err, store.ErrCandidateExpired) {
			return p.state.FailPublication(ctx, lease.Owner, lease.Epoch, store.TerminalInput{JobID: jobID, Phase: "publication", FailureCode: "artifact_expired"})
		}
		if err != nil {
			return err
		}
	}
	// Recheck after potentially slow Storage work and immediately before the
	// guarded terminal transaction. An uncertain decision keeps bytes private.
	allowed, err = p.checkAccess(publishCtx, job, input, "generation.publish")
	if err != nil {
		return err
	}
	if !allowed {
		return p.state.FailPublication(publishCtx, lease.Owner, lease.Epoch, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
	}
	allowed, err = p.selectionStillAuthorized(publishCtx, job, input)
	if err != nil {
		return err
	}
	if !allowed {
		return p.state.FailPublication(publishCtx, lease.Owner, lease.Epoch, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
	}
	diagnostics, _ := json.Marshal(record.Terminal.Diagnostics)
	if len(record.Terminal.Diagnostics) == 0 {
		diagnostics = []byte("[]")
	}
	err = p.state.TerminalSuccess(publishCtx, store.SuccessInput{JobID: jobID, Publisher: lease.Owner, PublicationEpoch: lease.Epoch, CandidateID: candidate.CandidateID, Artifact: candidate.Artifact, Diagnostics: diagnostics, RawDiagnostics: record.Terminal.RawDiagnostics})
	if errors.Is(err, store.ErrAuthorityDenied) {
		return p.state.FailPublication(publishCtx, lease.Owner, lease.Epoch, store.TerminalInput{JobID: jobID, Phase: "access", FailureCode: "access_denied"})
	}
	if err == nil && a.StopConfirmed && privateToken.MatchString(a.StagingToken) && filepath.Clean(a.PrivateStagingPath) == filepath.Join(filepath.Clean(p.cfg.StagingRoot), a.StagingToken) {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), controllerCallTimeout)
		scheduleErr := p.state.ScheduleAttemptCleanup(cleanupCtx, a.AttemptID)
		cancelCleanup()
		if scheduleErr == nil {
			_ = removePrivateAttemptDir(p.cfg.StagingRoot, a.StagingToken, a.PrivateStagingPath)
		}
	}
	return err
}

// selectionStillAuthorized rechecks the rights that admitted an E05 render at
// the publication boundary. Release renders are authorized by the live
// Projects release decision and generation.create; they do not require
// source.read. Working renders require current generation.create and
// source.read for the frozen source snapshot.
func (p *Publisher) selectionStillAuthorized(ctx context.Context, job generationv1.Job, input store.ExecutionInput) (bool, error) {
	if input.Execution == nil || input.Selection == nil {
		return true, nil
	}
	principal := input.Execution.Actor
	allowed, err := p.access.Check(ctx, storagev1.AccessCheck{Principal: principal, Scope: job.Scope, Operation: "generation.create"})
	if err != nil || !allowed {
		return allowed, err
	}
	if input.Selection.ReleaseID != "" {
		if p.cfg.Projects == nil {
			return false, errors.New("Projects release publication check is unavailable")
		}
		var authority *identityv1.AuthorityRef
		if input.Identity != nil {
			authority = &input.Identity.Authority
		}
		decision, checkErr := p.cfg.Projects.CurrentRelease(ctx, projectsv1.CurrentReleaseCheck{Authority: authority, Actor: principal, Scope: job.Scope, ReleaseID: input.Execution.ReleaseID, DescriptorDigest: input.Execution.Descriptor.Digest})
		if checkErr != nil {
			return false, checkErr
		}
		return decision.Allowed, nil
	}
	return p.access.Check(ctx, storagev1.AccessCheck{Principal: principal, Scope: job.Scope, Operation: "source.read", ObjectID: job.SourceSnapshotID})
}

func (p *Publisher) heartbeatPublication(ctx context.Context, cancel context.CancelFunc, l store.PublicationLease) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	interval := p.cfg.Heartbeat
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if activity.IsActivity(ctx) {
					activity.RecordHeartbeat(ctx)
				}
				call, stop := context.WithTimeout(ctx, 5*time.Second)
				_, err := p.state.RenewPublication(call, l.JobID, l.Owner, l.Epoch)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return func() { close(done); <-stopped }
}

func (p *Publisher) reopen(ctx context.Context, job generationv1.Job, a store.Attempt, in store.ExecutionInput, r outputRecord) (*sandbox.Output, error) {
	expectedPath := filepath.Join(p.cfg.StagingRoot, a.StagingToken)
	if filepath.Clean(a.PrivateStagingPath) != expectedPath || filepath.Base(a.StagingToken) != a.StagingToken || a.StagingToken == "." || a.StagingToken == "" {
		return nil, errors.New("staging path differs from durable token")
	}
	return sandbox.ReopenOutput(ctx, expectedPath, r.Token, renderRequest(job, a, in), r.Spec, a.ContainerID)
}

func (p *Publisher) finalize(ctx context.Context, job generationv1.Job, a store.Attempt, in store.ExecutionInput, l store.PublicationLease, r outputRecord) (store.PublicationCandidate, error) {
	var candidate store.PublicationCandidate
	finalKey := stableKey(a, "finalize")
	previous, findErr := p.state.FindIntent(ctx, job.JobID, a.AttemptID, store.IntentArtifactFinalize, finalKey)
	if findErr != nil && !errors.Is(findErr, store.ErrNotFound) {
		return candidate, findErr
	}
	keys := initialPDFUploadKeys(a)
	var baseFinal finalizeIntent
	var baseUpload storagev1.Upload
	var staged *sandbox.Output
	if findErr == nil {
		if err := jsonvalue.Unmarshal(previous.Request, &baseFinal, 16<<20); err != nil {
			return candidate, err
		}
		baseUpload = storagev1.Upload{UploadID: baseFinal.UploadID, Scope: baseFinal.Scope, Class: storagev1.ClassResult, SizeBytes: baseFinal.Size, SHA256: baseFinal.SHA256, WriterToken: baseFinal.Request.WriterToken}
	} else {
		var err error
		staged, _ = p.reopen(ctx, job, a, in, r)
		snapshotID := job.SourceSnapshotID
		if len(in.Request.Parameters) > 0 {
			snapshotID, err = p.saveDerived(ctx, job, a, l, staged, r)
			if err != nil {
				return candidate, err
			}
		}
		create := storagev1.CreateUploadRequest{Scope: job.Scope, Class: storagev1.ClassResult, SizeBytes: r.Terminal.PDF.Size, SHA256: r.Terminal.PDF.SHA256, IdempotencyKey: keys.Create}
		baseUpload, err = effect(ctx, p, a, l, store.IntentUploadCreate, keys.Create, create, func(q storagev1.CreateUploadRequest) (storagev1.Upload, error) { return p.storage.CreateUpload(ctx, q) })
		if err != nil {
			return candidate, err
		}
		if err = validateUpload(baseUpload, job.Scope, storagev1.ClassResult, r.Terminal.PDF.Size, r.Terminal.PDF.SHA256); err != nil {
			return candidate, err
		}
		baseFinal = finalizeIntent{UploadID: baseUpload.UploadID, Scope: job.Scope, SHA256: r.Terminal.PDF.SHA256, Size: r.Terminal.PDF.Size, Request: storagev1.FinalizeArtifactRequest{WriterToken: baseUpload.WriterToken, OriginJobID: job.JobID, SnapshotID: snapshotID, Metadata: storagev1.ArtifactMetadata{Filename: "document.pdf", MediaType: "application/pdf", TargetID: job.TargetID, AttemptID: a.AttemptID, InputFingerprint: job.InputFingerprint}, IdempotencyKey: keys.Finalize}}
	}
	openPDF := func() (io.ReadCloser, error) {
		if staged == nil {
			var err error
			staged, err = p.reopen(ctx, job, a, in, r)
			if err != nil {
				return nil, err
			}
		}
		return staged.OpenPDF()
	}
	current, currentKeys, rotations, err := p.putWithRotations(ctx, job, a, l, baseUpload, keys, 0, "pdf", openPDF)
	if err != nil {
		return candidate, err
	}
	var final finalizeIntent
	var artifact storagev1.Artifact
	for {
		final = baseFinal
		final.UploadID = current.UploadID
		final.Request.WriterToken = current.WriterToken
		final.Request.IdempotencyKey = currentKeys.Finalize
		// GetUpload is the recovery surface before publication. GetArtifact
		// requires committed success and cannot close the finalize/commit gap.
		latest, getErr := p.storage.GetUpload(ctx, current.UploadID)
		if getErr != nil {
			return candidate, getErr
		}
		if getErr = validateUpload(latest, job.Scope, storagev1.ClassResult, final.Size, final.SHA256); getErr != nil || final.Scope != job.Scope || final.Request.OriginJobID != job.JobID || final.Request.Metadata.AttemptID != a.AttemptID || final.Request.Metadata.TargetID != job.TargetID || final.Request.Metadata.InputFingerprint != job.InputFingerprint || final.SHA256 != r.Terminal.PDF.SHA256 || final.Size != r.Terminal.PDF.Size {
			return candidate, errors.New("finalize intent/upload bindings differ")
		}
		if latest.State != "committed" && latest.State != "uploaded" {
			return candidate, errors.New("finalize upload is not ready")
		}
		artifact, err = effect(ctx, p, a, l, store.IntentArtifactFinalize, currentKeys.Finalize, final, func(q finalizeIntent) (storagev1.Artifact, error) {
			allowed, checkErr := p.checkAccess(ctx, job, in, "generation.publish")
			if checkErr != nil {
				return storagev1.Artifact{}, checkErr
			}
			if !allowed {
				return storagev1.Artifact{}, store.ErrAuthorityDenied
			}
			allowed, checkErr = p.selectionStillAuthorized(ctx, job, in)
			if checkErr != nil {
				return storagev1.Artifact{}, checkErr
			}
			if !allowed {
				return storagev1.Artifact{}, store.ErrAuthorityDenied
			}
			return p.storage.FinalizeArtifact(ctx, q.UploadID, q.Request)
		})
		if err == nil {
			current = latest
			break
		}
		if !writerExpired(err) || rotations >= maxUploadRotations {
			return candidate, err
		}
		current, err = p.latestRotatableUpload(ctx, job.Scope, storagev1.ClassResult, final.Size, final.SHA256, latest)
		if err != nil {
			return candidate, err
		}
		current, currentKeys, err = p.rotateUpload(ctx, job, a, l, "pdf", current)
		if err != nil {
			return candidate, err
		}
		rotations++
		current, currentKeys, rotations, err = p.putWithRotations(ctx, job, a, l, current, currentKeys, rotations, "pdf", openPDF)
		if err != nil {
			return candidate, err
		}
	}
	if artifact.Hash != final.SHA256 || artifact.SizeBytes != final.Size || artifact.Scope != job.Scope || artifact.Metadata != final.Request.Metadata || artifact.SnapshotID != final.Request.SnapshotID || artifact.OriginJobID != job.JobID || artifact.ExpiresAt.Sub(artifact.CreatedAt) != 24*time.Hour {
		return candidate, errors.New("Storage finalize receipt differs from expected result")
	}
	receipt, _ := json.Marshal(artifact)
	candidate = store.PublicationCandidate{JobID: job.JobID, RenderAttemptID: a.AttemptID, Scope: job.Scope, SnapshotID: artifact.SnapshotID, TargetID: job.TargetID, InputFingerprint: job.InputFingerprint, Artifact: artifact, ArtifactBytes: receipt, FinalizeReceipt: receipt}
	if err = p.state.RegisterCandidate(ctx, job.JobID, l.Owner, l.Epoch, candidate); err != nil {
		return candidate, err
	}
	return p.state.GetCandidate(ctx, job.JobID, artifact.ArtifactID)
}

type putIntent struct {
	Upload storagev1.Upload `json:"upload"`
}

func (p *Publisher) put(ctx context.Context, job generationv1.Job, a store.Attempt, l store.PublicationLease, upload storagev1.Upload, putKey, label string, open func() (io.ReadCloser, error)) error {
	if upload.Scope != job.Scope || upload.UploadID == "" || upload.WriterToken == "" {
		return errors.New("invalid upload receipt")
	}
	_, err := effect(ctx, p, a, l, store.IntentUploadPut, putKey, putIntent{upload}, func(q putIntent) (storagev1.Upload, error) {
		current, err := p.storage.GetUpload(ctx, q.Upload.UploadID)
		if err != nil {
			return current, err
		}
		if current.Scope != q.Upload.Scope || current.Class != q.Upload.Class || current.SizeBytes != q.Upload.SizeBytes || current.SHA256 != q.Upload.SHA256 {
			return current, errors.New("upload bindings changed")
		}
		if current.State == "uploaded" || current.State == "committed" {
			return current, nil
		}
		if _, err = p.storage.RenewUpload(ctx, current.UploadID, storagev1.RenewUploadRequest{WriterToken: q.Upload.WriterToken}); err != nil {
			return current, err
		}
		file, err := open()
		if err != nil {
			return current, err
		}
		defer file.Close()
		return p.storage.PutUpload(ctx, current.UploadID, q.Upload.WriterToken, file)
	})
	return err
}

func initialPDFUploadKeys(a store.Attempt) uploadKeys {
	return uploadKeys{Create: stableKey(a, "pdf-upload"), Put: stableKey(a, "put/pdf"), Finalize: stableKey(a, "finalize")}
}

func initialResourceUploadKeys(a store.Attempt, label string) uploadKeys {
	return uploadKeys{Create: stableKey(a, "resource/"+label), Put: stableKey(a, "put/"+label), Commit: stableKey(a, "commit/"+label)}
}

func validateUpload(u storagev1.Upload, scope storagev1.Scope, class storagev1.ObjectClass, size int64, hash string) error {
	if u.UploadID == "" || u.WriterToken == "" || u.Scope != scope || u.Class != class || u.SizeBytes != size || u.SHA256 != hash {
		return errors.New("upload receipt bindings differ")
	}
	return nil
}

func writerExpired(err error) bool {
	var api *storagev1.HTTPError
	return errors.As(err, &api) && api.Status == 409 && api.Code == "writer_expired"
}

func uploadRotationKey(a store.Attempt, label, uploadID string) string {
	return stableKey(a, "upload-rotation/"+label+"/"+uploadID)
}

func nextUploadRotation(a store.Attempt, label string, current storagev1.Upload) uploadRotation {
	seed := "upload-generation/" + label + "/" + current.UploadID
	return uploadRotation{
		PreviousUploadID: current.UploadID,
		Keys: uploadKeys{
			Create:   stableKey(a, seed+"/create"),
			Put:      stableKey(a, seed+"/put"),
			Commit:   stableKey(a, seed+"/commit"),
			Finalize: stableKey(a, seed+"/finalize"),
		},
	}
}

func (p *Publisher) latestRotatableUpload(ctx context.Context, scope storagev1.Scope, class storagev1.ObjectClass, size int64, hash string, receipt storagev1.Upload) (storagev1.Upload, error) {
	if err := validateUpload(receipt, scope, class, size, hash); err != nil {
		return storagev1.Upload{}, err
	}
	current, err := p.storage.GetUpload(ctx, receipt.UploadID)
	if err != nil {
		return storagev1.Upload{}, err
	}
	if err = validateUpload(current, scope, class, size, hash); err != nil {
		return storagev1.Upload{}, err
	}
	if current.State != "created" && current.State != "writing" && current.State != "uploaded" || current.ArtifactID != "" || current.ResourceID != "" {
		return storagev1.Upload{}, errors.New("Storage upload is not safely rotatable")
	}
	return current, nil
}

func (p *Publisher) rotateUpload(ctx context.Context, job generationv1.Job, a store.Attempt, l store.PublicationLease, label string, current storagev1.Upload) (storagev1.Upload, uploadKeys, error) {
	var empty uploadKeys
	if current.State != "created" && current.State != "writing" && current.State != "uploaded" || current.ArtifactID != "" || current.ResourceID != "" {
		return storagev1.Upload{}, empty, errors.New("Storage upload is not safely rotatable")
	}
	plan := nextUploadRotation(a, label, current)
	key := uploadRotationKey(a, label, current.UploadID)
	plan, err := effect(ctx, p, a, l, intentUploadRotate, key, plan, func(q uploadRotation) (uploadRotation, error) { return q, nil })
	if err != nil {
		return storagev1.Upload{}, empty, err
	}
	if plan != nextUploadRotation(a, label, current) {
		return storagev1.Upload{}, empty, errors.New("persisted upload rotation bindings differ")
	}
	create := storagev1.CreateUploadRequest{Scope: job.Scope, Class: current.Class, SizeBytes: current.SizeBytes, SHA256: current.SHA256, IdempotencyKey: plan.Keys.Create}
	upload, err := effect(ctx, p, a, l, store.IntentUploadCreate, plan.Keys.Create, create, func(q storagev1.CreateUploadRequest) (storagev1.Upload, error) { return p.storage.CreateUpload(ctx, q) })
	if err != nil {
		return storagev1.Upload{}, empty, err
	}
	if err = validateUpload(upload, job.Scope, current.Class, current.SizeBytes, current.SHA256); err != nil {
		return storagev1.Upload{}, empty, err
	}
	return upload, plan.Keys, nil
}

func (p *Publisher) resumeUploadRotations(ctx context.Context, job generationv1.Job, a store.Attempt, l store.PublicationLease, label string, upload storagev1.Upload, keys uploadKeys) (storagev1.Upload, uploadKeys, int, error) {
	rotations := 0
	for {
		current, err := p.storage.GetUpload(ctx, upload.UploadID)
		if err != nil {
			return storagev1.Upload{}, keys, rotations, err
		}
		if err = validateUpload(current, job.Scope, upload.Class, upload.SizeBytes, upload.SHA256); err != nil {
			return storagev1.Upload{}, keys, rotations, err
		}
		markerKey := uploadRotationKey(a, label, current.UploadID)
		previous, err := p.state.FindIntent(ctx, job.JobID, a.AttemptID, intentUploadRotate, markerKey)
		if errors.Is(err, store.ErrNotFound) {
			return current, keys, rotations, nil
		}
		if err != nil {
			return storagev1.Upload{}, keys, rotations, err
		}
		if current.State == "committed" || current.ArtifactID != "" || current.ResourceID != "" {
			return storagev1.Upload{}, keys, rotations, errors.New("committed Storage upload cannot be rotated")
		}
		if rotations >= maxUploadRotations {
			return storagev1.Upload{}, keys, rotations, errors.New("Storage upload rotation limit exceeded")
		}
		expected := nextUploadRotation(a, label, current)
		var recorded uploadRotation
		if err = jsonvalue.Unmarshal(previous.Request, &recorded, 1<<20); err != nil || recorded != expected {
			return storagev1.Upload{}, keys, rotations, errors.New("persisted upload rotation is invalid")
		}
		plan, planErr := effect(ctx, p, a, l, intentUploadRotate, markerKey, expected, func(q uploadRotation) (uploadRotation, error) { return q, nil })
		if planErr != nil {
			return storagev1.Upload{}, keys, rotations, planErr
		}
		if plan != expected {
			return storagev1.Upload{}, keys, rotations, errors.New("persisted upload rotation bindings differ")
		}
		create := storagev1.CreateUploadRequest{Scope: job.Scope, Class: current.Class, SizeBytes: current.SizeBytes, SHA256: current.SHA256, IdempotencyKey: plan.Keys.Create}
		upload, err = effect(ctx, p, a, l, store.IntentUploadCreate, plan.Keys.Create, create, func(q storagev1.CreateUploadRequest) (storagev1.Upload, error) { return p.storage.CreateUpload(ctx, q) })
		if err != nil {
			return storagev1.Upload{}, keys, rotations, err
		}
		if err = validateUpload(upload, job.Scope, current.Class, current.SizeBytes, current.SHA256); err != nil {
			return storagev1.Upload{}, keys, rotations, err
		}
		keys = plan.Keys
		rotations++
	}
}

func (p *Publisher) putWithRotations(ctx context.Context, job generationv1.Job, a store.Attempt, l store.PublicationLease, upload storagev1.Upload, keys uploadKeys, rotationOffset int, label string, open func() (io.ReadCloser, error)) (storagev1.Upload, uploadKeys, int, error) {
	current, currentKeys, chainRotations, err := p.resumeUploadRotations(ctx, job, a, l, label, upload, keys)
	if err != nil {
		return storagev1.Upload{}, currentKeys, rotationOffset + chainRotations, err
	}
	rotations := rotationOffset + chainRotations
	if rotations > maxUploadRotations {
		return storagev1.Upload{}, currentKeys, rotations, errors.New("Storage upload rotation limit exceeded")
	}
	for {
		if err = p.put(ctx, job, a, l, current, currentKeys.Put, label, open); err == nil {
			latest, getErr := p.storage.GetUpload(ctx, current.UploadID)
			if getErr != nil {
				return storagev1.Upload{}, currentKeys, rotations, getErr
			}
			if getErr = validateUpload(latest, job.Scope, current.Class, current.SizeBytes, current.SHA256); getErr != nil {
				return storagev1.Upload{}, currentKeys, rotations, getErr
			}
			if latest.State != "uploaded" && latest.State != "committed" {
				return storagev1.Upload{}, currentKeys, rotations, errors.New("Storage PUT receipt is not uploaded")
			}
			return latest, currentKeys, rotations, nil
		}
		if !writerExpired(err) || rotations >= maxUploadRotations {
			return storagev1.Upload{}, currentKeys, rotations, err
		}
		current, err = p.latestRotatableUpload(ctx, job.Scope, current.Class, current.SizeBytes, current.SHA256, current)
		if err != nil {
			return storagev1.Upload{}, currentKeys, rotations, err
		}
		current, currentKeys, err = p.rotateUpload(ctx, job, a, l, label, current)
		if err != nil {
			return storagev1.Upload{}, currentKeys, rotations, err
		}
		rotations++
	}
}

func (p *Publisher) saveDerived(ctx context.Context, job generationv1.Job, a store.Attempt, l store.PublicationLease, staged *sandbox.Output, r outputRecord) (string, error) {
	// Persist the manifest before the first resource call. Recovery can then
	// rebuild the exact scoped Snapshot request from upload receipts even when
	// all local files disappeared after their Storage upload acknowledgement.
	var manifest storagev1.SnapshotManifest
	deriveKey := stableKey(a, "derive")
	old, err := p.state.FindIntent(ctx, job.JobID, a.AttemptID, store.IntentDeriveBundle, deriveKey)
	if err == nil {
		if err = jsonvalue.Unmarshal(old.Request, &manifest, 16<<20); err != nil {
			return "", err
		}
	} else if errors.Is(err, store.ErrNotFound) {
		if staged == nil || staged.Derived == nil {
			return "", errors.New("derived manifest unavailable before upload")
		}
		manifest = staged.Derived.Manifest()
	} else {
		return "", err
	}
	if manifest.ID != r.Terminal.DerivedContentID {
		return "", errors.New("durable derived manifest differs from renderer receipt")
	}
	manifestBytes, _ := json.Marshal(manifest)
	derive := store.IntentInput{JobID: job.JobID, RenderAttemptID: a.AttemptID, AttemptEpoch: a.FencingEpoch, PublicationEpoch: l.Epoch, Owner: l.Owner, Kind: store.IntentDeriveBundle, IdempotencyKey: deriveKey, Request: manifestBytes, Sensitive: true}
	if _, err = p.state.SaveIntent(ctx, derive); err != nil {
		return "", err
	}
	files, err := renderwire.PlanFiles(manifest)
	if err != nil {
		return "", err
	}
	refs := make([]storagev1.ResourceRef, 0, len(files))
	for _, file := range files {
		label := fmt.Sprintf("%s/%d/%s", file.Tree, file.FontIndex, file.Path)
		keys := initialResourceUploadKeys(a, label)
		create := storagev1.CreateUploadRequest{Scope: job.Scope, Class: storagev1.ClassResource, SizeBytes: file.Size, SHA256: file.SHA256, IdempotencyKey: keys.Create}
		upload, e := effect(ctx, p, a, l, store.IntentUploadCreate, keys.Create, create, func(q storagev1.CreateUploadRequest) (storagev1.Upload, error) { return p.storage.CreateUpload(ctx, q) })
		if e != nil {
			return "", e
		}
		if e = validateUpload(upload, job.Scope, storagev1.ClassResource, file.Size, file.SHA256); e != nil {
			return "", e
		}
		openResource := func() (io.ReadCloser, error) {
			if staged == nil || staged.Derived == nil {
				return nil, errors.New("resource bytes missing before Storage upload")
			}
			return staged.Derived.OpenVerified(file)
		}
		upload, keys, rotations, e := p.putWithRotations(ctx, job, a, l, upload, keys, 0, label, openResource)
		if e != nil {
			return "", e
		}
		var resource storagev1.Resource
		for {
			commit := struct {
				UploadID string                          `json:"uploadId"`
				Request  storagev1.CommitResourceRequest `json:"request"`
			}{upload.UploadID, storagev1.CommitResourceRequest{WriterToken: upload.WriterToken, IdempotencyKey: keys.Commit}}
			resource, e = effect(ctx, p, a, l, "resource.commit", keys.Commit, commit, func(q struct {
				UploadID string                          `json:"uploadId"`
				Request  storagev1.CommitResourceRequest `json:"request"`
			}) (storagev1.Resource, error) {
				return p.storage.CommitResource(ctx, q.UploadID, q.Request)
			})
			if e == nil {
				break
			}
			if !writerExpired(e) || rotations >= maxUploadRotations {
				return "", e
			}
			upload, e = p.latestRotatableUpload(ctx, job.Scope, storagev1.ClassResource, file.Size, file.SHA256, upload)
			if e != nil {
				return "", e
			}
			upload, keys, e = p.rotateUpload(ctx, job, a, l, label, upload)
			if e != nil {
				return "", e
			}
			rotations++
			upload, keys, rotations, e = p.putWithRotations(ctx, job, a, l, upload, keys, rotations, label, openResource)
			if e != nil {
				return "", e
			}
		}
		if resource.Scope != job.Scope || resource.SHA256 != file.SHA256 || resource.SizeBytes != file.Size || resource.Class != storagev1.ClassResource || resource.ResourceID == "" {
			return "", errors.New("derived resource receipt differs")
		}
		refs = append(refs, storagev1.ResourceRef{Tree: file.Tree, FontIndex: file.FontIndex, Path: file.Path, ResourceID: resource.ResourceID})
	}
	request := storagev1.CreateSnapshotRequest{Scope: job.Scope, Manifest: manifest, Resources: refs, IdempotencyKey: stableKey(a, "snapshot")}
	snapshot, err := effect(ctx, p, a, l, store.IntentSnapshotCreate, request.IdempotencyKey, request, func(q storagev1.CreateSnapshotRequest) (storagev1.Snapshot, error) {
		return p.storage.CreateSnapshot(ctx, q)
	})
	if err != nil {
		return "", err
	}
	if snapshot.SnapshotID == "" || snapshot.Scope != job.Scope || !reflect.DeepEqual(snapshot.Manifest, manifest) || !reflect.DeepEqual(snapshot.Resources, refs) {
		return "", errors.New("derived snapshot receipt differs")
	}
	if err = p.state.RecordPublishedDerivedIdentity(ctx, l, a.AttemptID, snapshot.SnapshotID, manifest.ID); err != nil {
		return "", err
	}
	if err = p.pins.EnsureDerivedPin(ctx, job, snapshot.SnapshotID); err != nil {
		return "", err
	}
	receipt, _ := json.Marshal(snapshot)
	if _, err = p.state.CompleteIntent(ctx, derive, receipt); err != nil {
		return "", err
	}
	return snapshot.SnapshotID, nil
}
