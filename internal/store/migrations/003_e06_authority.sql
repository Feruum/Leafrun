ALTER TABLE generation_jobs ADD COLUMN IF NOT EXISTS original_identity jsonb;
ALTER TABLE generation_jobs ADD COLUMN IF NOT EXISTS authority_release_id text NOT NULL DEFAULT '';
ALTER TABLE generation_jobs ADD COLUMN IF NOT EXISTS authority_descriptor_digest text NOT NULL DEFAULT '';
ALTER TABLE generation_jobs ADD CONSTRAINT generation_jobs_original_identity_size CHECK (original_identity IS NULL OR (jsonb_typeof(original_identity)='object' AND octet_length(original_identity::text) BETWEEN 1 AND 2048));
