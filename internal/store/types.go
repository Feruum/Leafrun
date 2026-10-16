package store

import (
	"encoding/json"
	"time"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const (
	IntentContainerCreate  = "container.create"
	IntentContainerStart   = "container.start"
	IntentDeriveBundle     = "snapshot.derive_bundle"
	IntentRenderInput      = "render.input"
	IntentSnapshotCreate   = "snapshot.create"
	IntentUploadCreate     = "upload.create"
	IntentUploadPut        = "upload.put"
	IntentArtifactFinalize = "artifact.finalize"
)

// AdmitInput carries the exact canonical request and separately extracted
// parameter bytes. The store validates that the request fingerprint is the
// Contracts generation/v1 fingerprint before it persists either byte slice.
type AdmitInput struct {
	Identity             *identityv1.Identity
	WorkspaceID          string
	ProjectID            string
	InitiatorID          string
	Actor                storagev1.Principal
	IdempotencyKey       string
	NormalizedRequest    []byte
	Parameters           []byte
	Fingerprint          string
	SelectionRequest     []byte
	SelectionFingerprint string
	ResolvedExecution    *projectsv1.ResolvedExecution
	InputResources       []InputResourceIntent
	EngineVersion        string
	SnapshotID           string
	SourceContentID      string
	TargetID             string
	Entrypoint           string
}

// InputResourceIntent binds a canonical prepared file to the one durable pin
// acquisition shared by all fields that reference the same Storage object.
type InputResourceIntent struct {
	Resource       projectsv1.InputResource
	AcquisitionKey string
}

type InputPinAcquire struct {
	Resource       projectsv1.InputResource
	AcquisitionKey string
	PinID          string
	State          string
}

type PinnedInputResource struct {
	Resource       projectsv1.InputResource
	AcquisitionKey string
	PinID          string
	State          string
}

type PinAcquireIntent struct {
	JobID          string
	IdempotencyKey string
	Request        storagev1.AcquirePinRequest
	RequestBytes   []byte
}

type WorkflowStartIntent struct {
	JobID      string
	WorkflowID string
	Request    []byte
	State      string
	LeaseOwner string
	LeaseEpoch int64
	LeaseUntil *time.Time
	CreatedAt  time.Time
}

type Attempt struct {
	AttemptID                string
	JobID                    string
	AttemptNumber            int
	FencingEpoch             int64
	State                    string
	CreateContainerIntentKey string
	StagingToken             string
	PrivateStagingPath       string
	ContainerID              string
	StopConfirmed            bool
	OwnerLeaseUntil          time.Time
	HeartbeatAt              time.Time
	CreatedAt                time.Time
}

type ClaimAttemptInput struct {
	JobID                    string
	CreateContainerIntentKey string
	StagingToken             string
	PrivateStagingPath       string
}

type IntentInput struct {
	JobID            string
	RenderAttemptID  string
	AttemptEpoch     int64
	PublicationEpoch int64
	Owner            string
	Kind             string
	IdempotencyKey   string
	Request          []byte
	Sensitive        bool
}

type OperationIntent struct {
	ID               int64
	JobID            string
	RenderAttemptID  string
	AttemptEpoch     int64
	PublicationEpoch int64
	Kind             string
	IdempotencyKey   string
	Request          []byte
	Sensitive        bool
	Output           []byte
	CompletedAt      *time.Time
	AbandonedAt      *time.Time
	CreatedAt        time.Time
}

type PublicationLease struct {
	JobID      string
	Owner      string
	Epoch      int64
	LeaseUntil time.Time
	Candidate  *PublicationCandidate
}

// Candidate carries the exact Storage receipt from FinalizeArtifact. Artifact
// JSON is persisted as strict DTO bytes and never reconstructed from JSONB.
type PublicationCandidate struct {
	CandidateID      string
	JobID            string
	RenderAttemptID  string
	Scope            storagev1.Scope
	SnapshotID       string
	TargetID         string
	InputFingerprint string
	Artifact         storagev1.Artifact
	ArtifactBytes    []byte
	FinalizeReceipt  []byte
}

type TerminalInput struct {
	JobID            string
	Phase            string
	Diagnostics      json.RawMessage
	RawDiagnostics   string
	FailureCode      string
	attemptID        string
	attemptEpoch     int64
	publisher        string
	publicationEpoch int64
}

type SuccessInput struct {
	JobID            string
	Publisher        string
	PublicationEpoch int64
	CandidateID      string
	Artifact         storagev1.Artifact
	Diagnostics      json.RawMessage
	RawDiagnostics   string
}

type RetentionResult struct {
	ReceiptsDeleted        int64
	SensitiveFieldsCleared int64
	JobsDeleted            int64
}

type Owner struct{ store *Store }

func (s *Store) Owner() *Owner { return &Owner{store: s} }

type jobRow struct {
	jobID, workspaceID, projectID                                          string
	status, phase                                                          string
	version                                                                int64
	sourceSnapshotID, sourceContentID, derivedSnapshotID, derivedContentID string
	targetID, entrypoint, engineVersion, fingerprint                       string
	createdAt, updatedAt, deadline                                         time.Time
	terminalAt                                                             *time.Time
	artifact                                                               []byte
	failureCode                                                            string
	diagnostics                                                            []byte
	rawDiagnostics                                                         string
	parameters                                                             []byte
}

func (j jobRow) dto() generationv1.Job {
	out := generationv1.Job{
		JobID:  j.jobID,
		Scope:  generationv1.Scope{WorkspaceID: j.workspaceID, ProjectID: j.projectID},
		Status: generationv1.Status(j.status), Phase: j.phase, Version: j.version,
		SourceSnapshotID: j.sourceSnapshotID, SourceContentID: j.sourceContentID,
		DerivedSnapshotID: j.derivedSnapshotID, DerivedContentID: j.derivedContentID,
		TargetID: j.targetID, Entrypoint: j.entrypoint, EngineVersion: j.engineVersion,
		InputFingerprint: j.fingerprint, CreatedAt: j.createdAt, UpdatedAt: j.updatedAt,
		Deadline: j.deadline, TerminalAt: j.terminalAt, FailureCode: j.failureCode,
		Parameters: append(json.RawMessage(nil), j.parameters...), RawDiagnostics: j.rawDiagnostics,
	}
	_ = json.Unmarshal(j.diagnostics, &out.Diagnostics)
	if len(j.artifact) > 0 && string(j.artifact) != "null" {
		var a generationv1.ArtifactBinding
		if json.Unmarshal(j.artifact, &a) == nil {
			out.Artifact = &a
		}
	}
	return out
}

const jobSelect = `SELECT job_id,workspace_id,project_id,status,phase,version,source_snapshot_id,source_content_id,derived_snapshot_id,derived_content_id,target_id,entrypoint,engine_version,fingerprint,created_at,updated_at,deadline,terminal_at,artifact,failure_code,diagnostics,raw_diagnostics,COALESCE(parameters,''::bytea) FROM generation_jobs`

func scanJob(row interface{ Scan(...any) error }) (jobRow, error) {
	var j jobRow
	var terminal *time.Time
	err := row.Scan(&j.jobID, &j.workspaceID, &j.projectID, &j.status, &j.phase, &j.version, &j.sourceSnapshotID, &j.sourceContentID, &j.derivedSnapshotID, &j.derivedContentID, &j.targetID, &j.entrypoint, &j.engineVersion, &j.fingerprint, &j.createdAt, &j.updatedAt, &j.deadline, &terminal, &j.artifact, &j.failureCode, &j.diagnostics, &j.rawDiagnostics, &j.parameters)
	if err != nil {
		return jobRow{}, err
	}
	j.terminalAt = terminal
	return j, nil
}
