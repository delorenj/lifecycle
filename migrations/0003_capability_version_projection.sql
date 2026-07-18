-- Make authority-owned capability_version explicit in every current-state
-- projection. The immutable specification is the only migration source; a
-- missing or malformed match aborts the transaction instead of guessing.

DO $$
DECLARE
    state_row RECORD;
    capability JSONB;
    spec_capability JSONB;
    migrated JSONB;
    version_text TEXT;
BEGIN
    FOR state_row IN
        SELECT s.lifecycle_id, s.spec_version, s.capabilities, p.spec_document
        FROM lifecycle_state AS s
        JOIN lifecycle_specs AS p
          ON p.lifecycle_id = s.lifecycle_id
         AND p.spec_version = s.spec_version
        FOR UPDATE OF s
    LOOP
        IF jsonb_typeof(state_row.capabilities) <> 'array' THEN
            RAISE EXCEPTION 'Lifecycle % capabilities projection is not an array',
                state_row.lifecycle_id;
        END IF;
        IF jsonb_typeof(state_row.spec_document -> 'capabilities') <> 'array' THEN
            RAISE EXCEPTION 'Lifecycle % specification capabilities are not an array',
                state_row.lifecycle_id;
        END IF;

        migrated := '[]'::jsonb;
        FOR capability IN SELECT value FROM jsonb_array_elements(state_row.capabilities)
        LOOP
            IF jsonb_typeof(capability) <> 'object'
               OR NOT capability ? 'capability_id' THEN
                RAISE EXCEPTION 'Lifecycle % has malformed projected capability',
                    state_row.lifecycle_id;
            END IF;

            IF capability ? 'capability_version' THEN
                version_text := capability ->> 'capability_version';
            ELSE
                SELECT value INTO spec_capability
                FROM jsonb_array_elements(state_row.spec_document -> 'capabilities')
                WHERE value ->> 'capability_id' = capability ->> 'capability_id';
                IF spec_capability IS NULL THEN
                    RAISE EXCEPTION
                        'Lifecycle % capability % has no authoritative specification match',
                        state_row.lifecycle_id,
                        capability ->> 'capability_id';
                END IF;
                version_text := spec_capability ->> 'capability_version';
                capability := capability || jsonb_build_object(
                    'capability_version',
                    spec_capability -> 'capability_version'
                );
            END IF;

            IF version_text IS NULL OR version_text !~ '^[1-9][0-9]*$' THEN
                RAISE EXCEPTION
                    'Lifecycle % capability % has invalid authoritative capability_version',
                    state_row.lifecycle_id,
                    capability ->> 'capability_id';
            END IF;
            migrated := migrated || jsonb_build_array(capability);
        END LOOP;

        UPDATE lifecycle_state
        SET capabilities = migrated
        WHERE lifecycle_id = state_row.lifecycle_id;
    END LOOP;
END;
$$;
