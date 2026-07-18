CREATE TABLE IF NOT EXISTS lifecycle_command_results (
    id BIGSERIAL PRIMARY KEY,
    lifecycle_id TEXT NOT NULL,
    repo TEXT NOT NULL,
    command_event_id UUID NOT NULL UNIQUE,
    command_id UUID NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    verdict TEXT NOT NULL CHECK (
        verdict IN ('accepted', 'applied', 'idempotent', 'stale',
                    'unauthorized', 'malformed', 'illegal')
    ),
    mutated BOOLEAN NOT NULL,
    expected_state_version INTEGER NOT NULL CHECK (expected_state_version >= 1),
    observed_state_version INTEGER NOT NULL CHECK (observed_state_version >= 1),
    resulting_state_version INTEGER,
    applied_event_id UUID,
    capability_id TEXT,
    reason_code TEXT NOT NULL,
    reply_envelope JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (lifecycle_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_command_results_lifecycle
    ON lifecycle_command_results(lifecycle_id, created_at, id);

CREATE OR REPLACE FUNCTION lifecycle_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS lifecycle_specs_append_only ON lifecycle_specs;
CREATE TRIGGER lifecycle_specs_append_only
BEFORE UPDATE OR DELETE ON lifecycle_specs
FOR EACH ROW EXECUTE FUNCTION lifecycle_reject_mutation();

DROP TRIGGER IF EXISTS lifecycle_history_append_only ON lifecycle_status_history;
CREATE TRIGGER lifecycle_history_append_only
BEFORE UPDATE OR DELETE ON lifecycle_status_history
FOR EACH ROW EXECUTE FUNCTION lifecycle_reject_mutation();

DROP TRIGGER IF EXISTS lifecycle_command_results_append_only ON lifecycle_command_results;
CREATE TRIGGER lifecycle_command_results_append_only
BEFORE UPDATE OR DELETE ON lifecycle_command_results
FOR EACH ROW EXECUTE FUNCTION lifecycle_reject_mutation();
