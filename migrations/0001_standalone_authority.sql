-- Standalone Lifecycle operational schema.
--
-- This is the first forward-only migration for a Lifecycle-owned database. It
-- deliberately refuses to adopt the old shared controller tables in place;
-- that cutover requires an explicit, audited export/import. It never selects
-- or writes a Candystore database.

DO $$
BEGIN
    IF to_regclass('public.lifecycles') IS NOT NULL THEN
        RAISE EXCEPTION
            'Lifecycle standalone migration refuses a pre-existing controller schema; use an audited export/import into a clean Lifecycle database';
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS lifecycles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    repo TEXT NOT NULL,
    repos JSONB,
    roadmap_id TEXT,
    status TEXT NOT NULL DEFAULT 'planned',
    health TEXT NOT NULL DEFAULT 'nominal',
    phase TEXT,
    progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    roadmap_version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    current_spec_version INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT lifecycle_status_valid CHECK (
        status IN ('planned', 'active', 'waiting', 'blocked', 'paused',
                   'disabled', 'completed', 'canceled', 'archived')
    ),
    CONSTRAINT lifecycle_health_valid CHECK (
        health IN ('nominal', 'at_risk', 'stalled', 'degraded', 'blocked')
    ),
    CONSTRAINT lifecycle_progress_valid CHECK (progress_percent BETWEEN 0 AND 100)
);

ALTER TABLE lifecycles ADD COLUMN IF NOT EXISTS current_spec_version INTEGER NOT NULL DEFAULT 1;
CREATE UNIQUE INDEX IF NOT EXISTS uq_lifecycles_repo ON lifecycles(repo);
CREATE INDEX IF NOT EXISTS idx_lifecycles_status ON lifecycles(status);

CREATE TABLE IF NOT EXISTS lifecycle_specs (
    lifecycle_id TEXT NOT NULL REFERENCES lifecycles(id) ON DELETE CASCADE,
    spec_version INTEGER NOT NULL CHECK (spec_version >= 1),
    policy_version TEXT NOT NULL,
    spec_document JSONB NOT NULL,
    spec_sha256 TEXT NOT NULL CHECK (spec_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL,
    PRIMARY KEY (lifecycle_id, spec_version)
);

CREATE TABLE IF NOT EXISTS lifecycle_state (
    lifecycle_id TEXT PRIMARY KEY REFERENCES lifecycles(id) ON DELETE CASCADE,
    spec_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    health TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'supervised',
    phase TEXT,
    progress_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    roadmap_version INTEGER NOT NULL DEFAULT 1,
    status_reason TEXT NOT NULL DEFAULT '',
    health_reason TEXT NOT NULL DEFAULT '',
    last_progress_at TIMESTAMPTZ,
    last_reconciled_at TIMESTAMPTZ,
    observed_through TIMESTAMPTZ,
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    state_fingerprint TEXT NOT NULL DEFAULT '',
    legal_frontier JSONB NOT NULL DEFAULT '[]'::jsonb,
    obligations JSONB NOT NULL DEFAULT '[]'::jsonb,
    capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_observation_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT lifecycle_state_status_valid CHECK (
        status IN ('planned', 'active', 'waiting', 'blocked', 'paused',
                   'disabled', 'completed', 'canceled', 'archived')
    ),
    CONSTRAINT lifecycle_state_health_valid CHECK (
        health IN ('nominal', 'at_risk', 'stalled', 'degraded', 'blocked')
    ),
    CONSTRAINT lifecycle_state_mode_valid CHECK (
        mode IN ('autonomous', 'supervised', 'manual', 'disabled')
    ),
    CONSTRAINT lifecycle_state_progress_valid CHECK (progress_percent BETWEEN 0 AND 100)
);

ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS spec_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'supervised';
ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS observed_through TIMESTAMPTZ;
ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS legal_frontier JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS obligations JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS capabilities JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE lifecycle_state ADD COLUMN IF NOT EXISTS source_observation_ids JSONB NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS idx_lifecycle_state_status ON lifecycle_state(status);

CREATE TABLE IF NOT EXISTS lifecycle_status_history (
    id BIGSERIAL PRIMARY KEY,
    lifecycle_id TEXT NOT NULL REFERENCES lifecycles(id) ON DELETE CASCADE,
    spec_version INTEGER NOT NULL,
    state_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    health TEXT NOT NULL,
    mode TEXT NOT NULL,
    phase TEXT,
    progress_percent DOUBLE PRECISION NOT NULL,
    roadmap_version INTEGER NOT NULL,
    status_reason TEXT NOT NULL,
    state_fingerprint TEXT NOT NULL,
    snapshot JSONB NOT NULL,
    transition JSONB NOT NULL,
    command_id UUID,
    reconciled_at TIMESTAMPTZ NOT NULL,
    UNIQUE (lifecycle_id, state_version)
);

ALTER TABLE lifecycle_status_history ADD COLUMN IF NOT EXISTS spec_version INTEGER;
ALTER TABLE lifecycle_status_history ADD COLUMN IF NOT EXISTS state_version INTEGER;
ALTER TABLE lifecycle_status_history ADD COLUMN IF NOT EXISTS mode TEXT;
ALTER TABLE lifecycle_status_history ADD COLUMN IF NOT EXISTS state_fingerprint TEXT;
ALTER TABLE lifecycle_status_history ADD COLUMN IF NOT EXISTS snapshot JSONB;
ALTER TABLE lifecycle_status_history ADD COLUMN IF NOT EXISTS command_id UUID;
CREATE INDEX IF NOT EXISTS idx_lifecycle_history_order
    ON lifecycle_status_history(lifecycle_id, state_version, id);

CREATE TABLE IF NOT EXISTS lifecycle_blockers (
    id TEXT PRIMARY KEY,
    lifecycle_id TEXT NOT NULL REFERENCES lifecycles(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'lifecycle',
    blocking BOOLEAN NOT NULL DEFAULT true,
    summary TEXT NOT NULL DEFAULT '',
    owner_kind TEXT,
    owner_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    fingerprint TEXT,
    source_observation_ids JSONB NOT NULL DEFAULT '[]'::jsonb
);

ALTER TABLE lifecycle_blockers ADD COLUMN IF NOT EXISTS source_observation_ids JSONB NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS idx_blockers_lifecycle ON lifecycle_blockers(lifecycle_id, resolved_at, id);

CREATE TABLE IF NOT EXISTS lifecycle_gates (
    id TEXT PRIMARY KEY,
    lifecycle_id TEXT NOT NULL REFERENCES lifecycles(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    blocking BOOLEAN NOT NULL DEFAULT true,
    reason TEXT NOT NULL DEFAULT '',
    continue_policy TEXT NOT NULL DEFAULT 'hold_until_resolved',
    owner_kind TEXT,
    owner_id TEXT,
    sla_due_at TIMESTAMPTZ,
    triggered_by_checkpoint_id TEXT,
    opened_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    resolution TEXT
);

CREATE INDEX IF NOT EXISTS idx_gates_lifecycle ON lifecycle_gates(lifecycle_id, resolved_at, id);

CREATE TABLE IF NOT EXISTS lifecycle_checkpoints (
    id TEXT PRIMARY KEY,
    lifecycle_id TEXT NOT NULL REFERENCES lifecycles(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    roadmap_version INTEGER NOT NULL DEFAULT 1,
    phase_id TEXT,
    reached_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS lifecycle_observations (
    observation_id UUID PRIMARY KEY,
    lifecycle_id TEXT NOT NULL REFERENCES lifecycles(id) ON DELETE CASCADE,
    source_event_id UUID NOT NULL UNIQUE,
    source_event_type TEXT NOT NULL,
    source_event_subject TEXT NOT NULL,
    source_event_source TEXT NOT NULL,
    source_event_producer TEXT NOT NULL,
    ordering_key TEXT NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0 CHECK (confidence BETWEEN 0 AND 1)
);

-- Existing extracted tables used a BIGSERIAL id and lacked source identity.
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS observation_id UUID;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS source_event_id UUID;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS source_event_type TEXT;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS source_event_subject TEXT;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS source_event_source TEXT;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS source_event_producer TEXT;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS ordering_key TEXT;
ALTER TABLE lifecycle_observations ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX IF NOT EXISTS uq_observation_source_event
    ON lifecycle_observations(source_event_id) WHERE source_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_observations_order
    ON lifecycle_observations(lifecycle_id, observed_at, source_event_id);

CREATE TABLE IF NOT EXISTS lifecycle_reconcile_queue (
    lifecycle_id TEXT PRIMARY KEY REFERENCES lifecycles(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INTEGER NOT NULL DEFAULT 0,
    leased_by TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE lifecycle_reconcile_queue ADD COLUMN IF NOT EXISTS as_of TIMESTAMPTZ;
UPDATE lifecycle_reconcile_queue SET as_of = COALESCE(as_of, available_at, created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') WHERE as_of IS NULL;
ALTER TABLE lifecycle_reconcile_queue ALTER COLUMN as_of SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_reconcile_available
    ON lifecycle_reconcile_queue(available_at, priority DESC, lifecycle_id);

CREATE TABLE IF NOT EXISTS sentinel_heartbeats (
    sentinel_id TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL DEFAULT 'global',
    scope_id TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL,
    last_successful_scan_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error_summary TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS lifecycle_event_outbox (
    id BIGSERIAL PRIMARY KEY,
    lifecycle_id TEXT,
    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    subject TEXT NOT NULL,
    envelope JSONB NOT NULL,
    payload JSONB,
    headers JSONB,
    event_sequence BIGINT,
    aggregate_version INTEGER,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by TEXT,
    lock_expires_at TIMESTAMPTZ,
    error TEXT
);

ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS event_id UUID;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS subject TEXT;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS envelope JSONB;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS headers JSONB;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS event_sequence BIGINT;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS aggregate_version INTEGER;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS locked_by TEXT;
ALTER TABLE lifecycle_event_outbox ADD COLUMN IF NOT EXISTS lock_expires_at TIMESTAMPTZ;
CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_event_id
    ON lifecycle_event_outbox(event_id) WHERE event_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_aggregate_sequence
    ON lifecycle_event_outbox(lifecycle_id, event_sequence)
    WHERE lifecycle_id IS NOT NULL AND event_sequence IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON lifecycle_event_outbox(next_attempt_at, created_at, id)
    WHERE published_at IS NULL;
