ALTER TABLE generation_jobs
    ADD COLUMN IF NOT EXISTS selection_request bytea,
    ADD COLUMN IF NOT EXISTS selection_fingerprint text,
    ADD COLUMN IF NOT EXISTS actor_subject text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS actor_domain text NOT NULL DEFAULT '';

ALTER TABLE generation_jobs DROP CONSTRAINT IF EXISTS generation_jobs_selection_binding_check;
ALTER TABLE generation_jobs ADD CONSTRAINT generation_jobs_selection_binding_check CHECK (
    (selection_request IS NULL AND selection_fingerprint IS NULL AND actor_subject='' AND actor_domain='') OR
    (selection_request IS NOT NULL AND octet_length(selection_request) BETWEEN 1 AND 1048576
        AND selection_fingerprint ~ '^[0-9a-f]{64}$' AND actor_subject<>'' AND actor_domain<>'')
);

CREATE TABLE IF NOT EXISTS generation_input_resources (
    job_id text NOT NULL REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    field_pointer text NOT NULL,
    resource_id text NOT NULL,
    workspace_id text NOT NULL,
    project_id text NOT NULL,
    object_class text NOT NULL CHECK (object_class='resource'),
    filename text NOT NULL,
    generated_path text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 0 AND 26214400),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    pin_id text NOT NULL DEFAULT '',
    acquisition_key text NOT NULL,
    pin_state text NOT NULL DEFAULT 'pending' CHECK (pin_state IN ('pending','active','failed','released')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id,field_pointer)
);
CREATE INDEX IF NOT EXISTS generation_input_resources_acquire_idx
    ON generation_input_resources(job_id,acquisition_key,resource_id);
CREATE INDEX IF NOT EXISTS generation_input_resources_object_idx
    ON generation_input_resources(workspace_id,project_id,resource_id,pin_state);
