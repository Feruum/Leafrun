CREATE TABLE IF NOT EXISTS generation_jobs (
    job_id text PRIMARY KEY,
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    initiator_id text NOT NULL,
    idempotency_key text NOT NULL,
    normalized_request bytea NOT NULL,
    parameters bytea,
    fingerprint text NOT NULL,
    source_snapshot_id text NOT NULL,
    source_content_id text NOT NULL DEFAULT '',
    derived_snapshot_id text NOT NULL DEFAULT '',
    derived_content_id text NOT NULL DEFAULT '',
    target_id text NOT NULL,
    entrypoint text NOT NULL,
    engine_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('queued','preparing','rendering','publishing','cancelling','succeeded','failed','canceled','expired')),
    phase text NOT NULL,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    deadline timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminal_at timestamptz,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
    raw_diagnostics text NOT NULL DEFAULT '',
    failure_code text NOT NULL DEFAULT '',
    artifact jsonb,
    artifact_id text NOT NULL DEFAULT '',
    cancellation_requested boolean NOT NULL DEFAULT false,
    current_attempt_number integer NOT NULL DEFAULT 0 CHECK (current_attempt_number >= 0),
    attempt_epoch bigint NOT NULL DEFAULT 0 CHECK (attempt_epoch >= 0),
    publication_epoch bigint NOT NULL DEFAULT 0 CHECK (publication_epoch >= 0),
    publication_owner text NOT NULL DEFAULT '',
    publication_lease_until timestamptz,
    CHECK ((status IN ('succeeded','failed','canceled','expired')) = (terminal_at IS NOT NULL)),
    CHECK ((publication_owner = '') = (publication_lease_until IS NULL))
);
CREATE INDEX IF NOT EXISTS generation_jobs_queue_idx
    ON generation_jobs (created_at, job_id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS generation_jobs_deadline_idx
    ON generation_jobs (deadline) WHERE terminal_at IS NULL;
CREATE INDEX IF NOT EXISTS generation_jobs_terminal_idx
    ON generation_jobs (terminal_at) WHERE terminal_at IS NOT NULL;

-- Keep the terminal event identity outside Platform's encoded envelope so
-- retention never asks PostgreSQL JSONB to reinterpret arbitrary event bytes.
CREATE TABLE IF NOT EXISTS generation_terminal_outbox (
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    event_id text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id,event_id)
);

CREATE TABLE IF NOT EXISTS generation_admission_receipts (
    workspace_id text NOT NULL,
    initiator_id text NOT NULL,
    idempotency_key text NOT NULL,
    fingerprint text NOT NULL,
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, initiator_id, idempotency_key),
    UNIQUE (job_id)
);
CREATE INDEX IF NOT EXISTS generation_receipts_expiry_idx ON generation_admission_receipts(expires_at);

-- A job owns exactly one reservation at a time. A queued reservation is moved
-- to execution under the same transaction and global advisory quota lock.
CREATE TABLE IF NOT EXISTS generation_reservations (
    job_id text PRIMARY KEY REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    workspace_id text NOT NULL,
    reservation_type text NOT NULL CHECK (reservation_type IN ('queue','execution')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS generation_reservations_type_idx ON generation_reservations(reservation_type, workspace_id);

CREATE TABLE IF NOT EXISTS generation_pin_acquires (
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    acquire_number integer NOT NULL CHECK (acquire_number > 0),
    idempotency_key text NOT NULL,
    object_id text NOT NULL,
    request bytea NOT NULL,
    response jsonb,
    state text NOT NULL CHECK (state IN ('pending','active','released','failed')),
    storage_pin_id text NOT NULL DEFAULT '',
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, acquire_number),
    UNIQUE (job_id, idempotency_key)
);
ALTER TABLE generation_pin_acquires ADD COLUMN IF NOT EXISTS object_id text NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS generation_pin_acquire_object_state_idx ON generation_pin_acquires(job_id,object_id,state,acquire_number DESC);

CREATE TABLE IF NOT EXISTS generation_workflow_starts (
    job_id text PRIMARY KEY REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    workflow_id text NOT NULL UNIQUE,
    request bytea NOT NULL,
    state text NOT NULL CHECK (state IN ('pending','acknowledged')),
    lease_owner text NOT NULL DEFAULT '',
    lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    acknowledged_at timestamptz,
    CHECK ((lease_owner = '') = (lease_until IS NULL))
);
ALTER TABLE generation_workflow_starts ADD COLUMN IF NOT EXISTS lease_owner text NOT NULL DEFAULT '';
ALTER TABLE generation_workflow_starts ADD COLUMN IF NOT EXISTS lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0);
ALTER TABLE generation_workflow_starts ADD COLUMN IF NOT EXISTS lease_until timestamptz;
ALTER TABLE generation_workflow_starts DROP CONSTRAINT IF EXISTS generation_workflow_starts_state_check;
ALTER TABLE generation_workflow_starts ADD CONSTRAINT generation_workflow_starts_state_check CHECK (state IN ('pending','acknowledged','discarded'));
CREATE INDEX IF NOT EXISTS generation_workflow_dispatch_idx ON generation_workflow_starts(created_at,job_id) WHERE state='pending';

CREATE TABLE IF NOT EXISTS generation_attempts (
    attempt_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number BETWEEN 1 AND 3),
    fencing_epoch bigint NOT NULL CHECK (fencing_epoch > 0),
    state text NOT NULL CHECK (state IN ('active','stopped','retryable','finished')),
    create_container_intent_key text NOT NULL,
    staging_token text NOT NULL,
    private_staging_path text NOT NULL,
    container_id text NOT NULL DEFAULT '',
    stop_confirmed boolean NOT NULL DEFAULT false,
    owner_lease_until timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    stopped_at timestamptz,
    UNIQUE (job_id,attempt_number),
    UNIQUE (job_id,fencing_epoch),
    CHECK (stop_confirmed = (stopped_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS generation_attempts_lease_idx ON generation_attempts(owner_lease_until) WHERE state='active';
CREATE INDEX IF NOT EXISTS generation_attempts_container_idx ON generation_attempts(container_id) WHERE container_id <> '';
ALTER TABLE generation_attempts ADD COLUMN IF NOT EXISTS recovery_claimed boolean NOT NULL DEFAULT false;

-- Intent request bytes are immutable. A later publisher epoch can only use a
-- distinct intent key or explicitly replay its own exact request.
CREATE TABLE IF NOT EXISTS generation_operation_intents (
    intent_id bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    render_attempt_id text NOT NULL DEFAULT '',
    attempt_epoch bigint NOT NULL DEFAULT 0,
    publication_epoch bigint NOT NULL DEFAULT 0,
    kind text NOT NULL,
    idempotency_key text NOT NULL,
    request bytea NOT NULL,
    sensitive boolean NOT NULL DEFAULT false,
    output bytea,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    abandoned_at timestamptz,
    UNIQUE(job_id,render_attempt_id,attempt_epoch,publication_epoch,kind,idempotency_key),
    CHECK ((output IS NULL) = (completed_at IS NULL)),
    CHECK (abandoned_at IS NULL OR completed_at IS NULL)
);
ALTER TABLE generation_operation_intents ADD COLUMN IF NOT EXISTS sensitive boolean NOT NULL DEFAULT false;
ALTER TABLE generation_operation_intents ADD COLUMN IF NOT EXISTS abandoned_at timestamptz;
CREATE INDEX IF NOT EXISTS generation_intents_pending_idx ON generation_operation_intents(job_id,created_at) WHERE completed_at IS NULL;

CREATE TABLE IF NOT EXISTS generation_publication_candidates (
    candidate_id text PRIMARY KEY,
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    render_attempt_id text NOT NULL REFERENCES generation_attempts(attempt_id),
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    snapshot_id text NOT NULL,
    target_id text NOT NULL,
    input_fingerprint text NOT NULL,
    artifact_id text NOT NULL,
    sha256 text NOT NULL,
    size_bytes bigint NOT NULL CHECK(size_bytes > 0),
    expires_at timestamptz NOT NULL,
    artifact jsonb NOT NULL,
    artifact_bytes bytea NOT NULL,
    finalize_receipt bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(job_id,artifact_id)
);
ALTER TABLE generation_publication_candidates ADD COLUMN IF NOT EXISTS artifact_bytes bytea NOT NULL DEFAULT ''::bytea;
CREATE INDEX IF NOT EXISTS generation_candidates_job_idx ON generation_publication_candidates(job_id,expires_at DESC);

CREATE TABLE IF NOT EXISTS generation_cleanup_intents (
    cleanup_id bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    kind text NOT NULL,
    idempotency_key text NOT NULL,
    request bytea NOT NULL,
    state text NOT NULL CHECK(state IN ('pending','claimed','complete')),
    not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text NOT NULL DEFAULT '',
    lease_until timestamptz,
    output bytea,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE(job_id,kind,idempotency_key),
    CHECK ((state='complete') = (completed_at IS NOT NULL))
);
ALTER TABLE generation_cleanup_intents ADD COLUMN IF NOT EXISTS lease_owner text NOT NULL DEFAULT '';
ALTER TABLE generation_cleanup_intents ADD COLUMN IF NOT EXISTS lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0);
CREATE INDEX IF NOT EXISTS generation_cleanup_pending_idx ON generation_cleanup_intents(not_before,cleanup_id) WHERE state <> 'complete';
