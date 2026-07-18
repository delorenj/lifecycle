-- Upgrade any currently active obligation projection to an explicit,
-- authority-owned occurrence. New occurrences are created by the authority
-- code; this migration only gives an already-persisted occurrence a stable
-- identity and its original state-history decision time.

DO $$
DECLARE
    state_row RECORD;
    obligation JSONB;
    migrated JSONB;
    activation_time TIMESTAMPTZ;
    digest TEXT;
    occurrence_id TEXT;
BEGIN
    FOR state_row IN
        SELECT lifecycle_id, status, state_version, last_reconciled_at, obligations
        FROM lifecycle_state
        WHERE jsonb_array_length(obligations) > 0
        FOR UPDATE
    LOOP
        SELECT reconciled_at INTO activation_time
        FROM lifecycle_status_history
        WHERE lifecycle_id = state_row.lifecycle_id
          AND state_version = state_row.state_version
        ORDER BY id
        LIMIT 1;
        activation_time := COALESCE(activation_time, state_row.last_reconciled_at);
        IF activation_time IS NULL THEN
            RAISE EXCEPTION
                'Lifecycle % obligation occurrence has no authority activation time',
                state_row.lifecycle_id;
        END IF;

        migrated := '[]'::jsonb;
        FOR obligation IN SELECT value FROM jsonb_array_elements(state_row.obligations)
        LOOP
            IF jsonb_typeof(obligation) <> 'object'
               OR NULLIF(obligation ->> 'id', '') IS NULL THEN
                RAISE EXCEPTION 'Lifecycle % has malformed projected obligation',
                    state_row.lifecycle_id;
            END IF;

            IF NOT obligation ? 'obligation_instance_id' THEN
                digest := md5(
                    'lifecycle-obligation-occurrence:'
                    || state_row.lifecycle_id || ':'
                    || (obligation ->> 'id') || ':'
                    || state_row.status || ':'
                    || state_row.state_version::text
                );
                occurrence_id :=
                    substr(digest, 1, 8) || '-'
                    || substr(digest, 9, 4) || '-'
                    || '5' || substr(digest, 14, 3) || '-'
                    || '8' || substr(digest, 18, 3) || '-'
                    || substr(digest, 21, 12);
                obligation := obligation || jsonb_build_object(
                    'obligation_instance_id', occurrence_id
                );
            END IF;
            IF NOT obligation ? 'activated_at' THEN
                obligation := obligation || jsonb_build_object(
                    'activated_at', activation_time
                );
            END IF;

            IF (obligation ->> 'obligation_instance_id') !~
               '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
               OR jsonb_typeof(obligation -> 'activated_at') <> 'string' THEN
                RAISE EXCEPTION
                    'Lifecycle % obligation % has invalid occurrence metadata',
                    state_row.lifecycle_id,
                    obligation ->> 'id';
            END IF;
            migrated := migrated || jsonb_build_array(obligation);
        END LOOP;

        UPDATE lifecycle_state
        SET obligations = migrated
        WHERE lifecycle_id = state_row.lifecycle_id;
    END LOOP;
END;
$$;
