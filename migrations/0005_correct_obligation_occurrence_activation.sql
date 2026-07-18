-- Correct the activation time assigned by the already-published 0004
-- migration. An obligation occurrence begins at the first state-history row
-- in the trailing continuous run of the lifecycle's current status, not at an
-- unrelated same-status state-version update. Existing occurrence identities
-- remain stable. A deterministic authority reconcile repairs the fingerprint,
-- version, history, and publication after migration.

DO $$
DECLARE
    state_row RECORD;
    obligation JSONB;
    corrected JSONB;
    activation_time TIMESTAMPTZ;
BEGIN
    FOR state_row IN
        SELECT lifecycle_id, status, state_version, last_reconciled_at, obligations
        FROM lifecycle_state
        WHERE jsonb_array_length(obligations) > 0
        FOR UPDATE
    LOOP
        SELECT history.reconciled_at
        INTO activation_time
        FROM lifecycle_status_history AS history
        WHERE history.lifecycle_id = state_row.lifecycle_id
          AND history.state_version <= state_row.state_version
          AND history.status = state_row.status
          AND NOT EXISTS (
              SELECT 1
              FROM lifecycle_status_history AS later
              WHERE later.lifecycle_id = history.lifecycle_id
                AND later.state_version > history.state_version
                AND later.state_version <= state_row.state_version
                AND later.status <> state_row.status
          )
        ORDER BY history.state_version ASC, history.id ASC
        LIMIT 1;

        activation_time := COALESCE(activation_time, state_row.last_reconciled_at);
        IF activation_time IS NULL THEN
            RAISE EXCEPTION
                'Lifecycle % obligation occurrence has no authority activation history',
                state_row.lifecycle_id;
        END IF;

        corrected := '[]'::jsonb;
        FOR obligation IN SELECT value FROM jsonb_array_elements(state_row.obligations)
        LOOP
            IF jsonb_typeof(obligation) <> 'object'
               OR NULLIF(obligation ->> 'obligation_instance_id', '') IS NULL
               OR NULLIF(obligation ->> 'activated_at', '') IS NULL THEN
                RAISE EXCEPTION
                    'Lifecycle % has malformed obligation occurrence metadata',
                    state_row.lifecycle_id;
            END IF;
            obligation := jsonb_set(
                obligation,
                '{activated_at}',
                to_jsonb(activation_time),
                false
            );
            corrected := corrected || jsonb_build_array(obligation);
        END LOOP;

        IF corrected IS DISTINCT FROM state_row.obligations THEN
            UPDATE lifecycle_state
            SET obligations = corrected,
                updated_at = now()
            WHERE lifecycle_id = state_row.lifecycle_id;

            INSERT INTO lifecycle_reconcile_queue
                (lifecycle_id, reason, as_of, available_at)
            VALUES (
                state_row.lifecycle_id,
                'migration:correct_obligation_occurrence_activation',
                GREATEST(state_row.last_reconciled_at, activation_time),
                now()
            )
            ON CONFLICT (lifecycle_id) DO UPDATE SET
                reason = EXCLUDED.reason,
                as_of = GREATEST(lifecycle_reconcile_queue.as_of, EXCLUDED.as_of),
                available_at = now(),
                leased_by = NULL,
                lease_expires_at = NULL,
                updated_at = now();
        END IF;
    END LOOP;
END;
$$;
