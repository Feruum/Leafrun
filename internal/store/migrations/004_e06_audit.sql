CREATE TABLE generation_audit_context (
 job_id text PRIMARY KEY REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
 operation_id uuid NOT NULL UNIQUE,
 canonical_scope jsonb NOT NULL CHECK (jsonb_typeof(canonical_scope)='object' AND octet_length(canonical_scope::text)<=2048),
 initiating_identity jsonb NOT NULL CHECK(jsonb_typeof(initiating_identity)='object' AND octet_length(initiating_identity::text)<=2048)
);
