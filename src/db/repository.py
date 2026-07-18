"""PostgreSQL repository for the standalone lifecycle authority."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg

from contracts import canonical_json, payload_sha256
from models import (
    Blocker,
    BlockerKind,
    CapabilityGrant,
    Checkpoint,
    CheckpointKind,
    CommandResult,
    FrontierItem,
    FrontierKind,
    Gate,
    GateKind,
    GatePolicy,
    GateResolution,
    IntentCommand,
    LifecycleHealth,
    LifecyclePolicy,
    LifecycleSpec,
    LifecycleState,
    LifecycleStatus,
    Obligation,
    ObligationStatus,
    Observation,
    OperatingMode,
    OutboxEvent,
    SkillRef,
)
from reconciler import reconcile
from specification import spec_from_json, spec_to_json


UTC = timezone.utc


@dataclass(frozen=True)
class AuthorityBundle:
    lifecycle_id: str
    name: str
    repo: str
    state: LifecycleState
    spec: LifecycleSpec
    observations: list[Observation]
    blockers: list[Blocker]
    gates: list[Gate]
    checkpoints: list[Checkpoint]
    sentinel_health: dict[str, str]


class ConcurrencyConflict(RuntimeError):
    pass


class LifecycleRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    # ------------------------------------------------------------------
    # Lifecycle/spec bootstrap and reads
    # ------------------------------------------------------------------

    async def create_authority_lifecycle(
        self,
        *,
        lifecycle_id: str,
        name: str,
        repo: str,
        spec: LifecycleSpec,
        created_by: str,
        created_at: datetime,
    ) -> LifecycleState:
        """Create registry, immutable spec, initial state, and history atomically."""

        if spec.lifecycle_id != lifecycle_id:
            raise ValueError("spec lifecycle_id does not match bootstrap lifecycle_id")
        document = spec_to_json(spec)
        document_sha256 = payload_sha256(document)
        result = reconcile(
            lifecycle_id=lifecycle_id,
            previous_state=None,
            observations=[],
            active_blockers=[],
            active_gates=[],
            checkpoints=[],
            sentinel_health={},
            spec=spec,
            as_of=created_at,
        )
        state = result.current_state
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1))",
                    f"lifecycle-bootstrap:{lifecycle_id}",
                )
                existing_registry = await connection.fetchrow(
                    """
                    SELECT name, repo, current_spec_version
                    FROM lifecycles
                    WHERE id = $1
                    """,
                    lifecycle_id,
                )
                if existing_registry is not None:
                    existing_spec_sha256 = await connection.fetchval(
                        """
                        SELECT spec_sha256
                        FROM lifecycle_specs
                        WHERE lifecycle_id = $1 AND spec_version = $2
                        """,
                        lifecycle_id,
                        spec.version,
                    )
                    existing_state = await connection.fetchrow(
                        "SELECT * FROM lifecycle_state WHERE lifecycle_id = $1",
                        lifecycle_id,
                    )
                    same_binding = (
                        existing_registry["name"] == name
                        and existing_registry["repo"] == repo
                        and existing_registry["current_spec_version"] == spec.version
                        and existing_spec_sha256 == document_sha256
                        and existing_state is not None
                    )
                    if not same_binding:
                        raise ConcurrencyConflict(
                            "lifecycle bootstrap identity or specification conflicts "
                            "with existing authority state"
                        )
                    return _row_to_state(existing_state)

                await connection.execute(
                    """
                    INSERT INTO lifecycles
                        (id, name, repo, status, health, created_by, created_at,
                         updated_at, current_spec_version, policy)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9::jsonb)
                    """,
                    lifecycle_id,
                    name,
                    repo,
                    state.status.value,
                    state.health.value,
                    created_by,
                    created_at,
                    spec.version,
                    canonical_json(state.policy.to_json()),
                )
                await connection.execute(
                    """
                    INSERT INTO lifecycle_specs
                        (lifecycle_id, spec_version, policy_version, spec_document,
                         spec_sha256, created_at, created_by)
                    VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
                    """,
                    lifecycle_id,
                    spec.version,
                    spec.policy_version,
                    canonical_json(document),
                    document_sha256,
                    created_at,
                    created_by,
                )
                inserted = await connection.fetchval(
                    """
                    INSERT INTO lifecycle_state
                        (lifecycle_id, spec_version, status, health, mode, phase,
                         progress_percent, roadmap_version, status_reason,
                         health_reason, last_progress_at, last_reconciled_at,
                         observed_through, state_version, state_fingerprint,
                         legal_frontier, obligations, capabilities,
                         source_observation_ids, updated_at, policy)
                    VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                         $13, $14, $15, $16::jsonb, $17::jsonb, $18::jsonb,
                         $19::jsonb, $12, $20::jsonb)
                    RETURNING lifecycle_id
                    """,
                    *_state_values(state),
                )
                if not inserted:
                    raise RuntimeError("lifecycle bootstrap did not produce state")
                await self._insert_history(
                    connection,
                    state=state,
                    transition={"reason": "LIFECYCLE_CREATED", "computed": True},
                    command_id=None,
                )
                return state

    async def get_lifecycle_state(self, lifecycle_id: str) -> LifecycleState | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM lifecycle_state WHERE lifecycle_id = $1", lifecycle_id
        )
        return _row_to_state(row) if row else None

    async def find_lifecycle_id_by_repo(self, repo: str) -> str | None:
        return await self.pool.fetchval("SELECT id FROM lifecycles WHERE repo = $1", repo)

    async def list_active_lifecycles(self) -> list[str]:
        rows = await self.pool.fetch(
            """
            SELECT lifecycle_id FROM lifecycle_state
            WHERE status NOT IN ('completed', 'canceled', 'archived', 'disabled')
            ORDER BY lifecycle_id
            """
        )
        return [row["lifecycle_id"] for row in rows]

    async def load_bundle(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
        *,
        as_of: datetime,
        for_update: bool,
    ) -> AuthorityBundle | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = await connection.fetchrow(
            f"""
            SELECT l.name, l.repo, s.*
            FROM lifecycles l
            JOIN lifecycle_state s ON s.lifecycle_id = l.id
            WHERE l.id = $1{suffix}
            """,
            lifecycle_id,
        )
        if not row:
            return None
        spec_row = await connection.fetchrow(
            """
            SELECT spec_document FROM lifecycle_specs
            WHERE lifecycle_id = $1 AND spec_version = $2
            """,
            lifecycle_id,
            row["spec_version"],
        )
        if not spec_row:
            raise RuntimeError("current lifecycle specification is missing")
        observations = await self._get_observations(connection, lifecycle_id, as_of)
        blockers = await self._get_blockers(connection, lifecycle_id)
        gates = await self._get_gates(connection, lifecycle_id)
        checkpoints = await self._get_checkpoints(connection, lifecycle_id)
        heartbeats = await connection.fetch(
            "SELECT sentinel_id, status FROM sentinel_heartbeats ORDER BY sentinel_id"
        )
        return AuthorityBundle(
            lifecycle_id=lifecycle_id,
            name=row["name"],
            repo=row["repo"],
            state=_row_to_state(row),
            spec=spec_from_json(_json_object(spec_row["spec_document"])),
            observations=observations,
            blockers=blockers,
            gates=gates,
            checkpoints=checkpoints,
            sentinel_health={item["sentinel_id"]: item["status"] for item in heartbeats},
        )

    async def lock_lifecycle_tx(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
    ) -> bool:
        """Serialize all authority effects for one lifecycle aggregate."""

        return bool(
            await connection.fetchval(
                """
                SELECT lifecycle_id FROM lifecycle_state
                WHERE lifecycle_id = $1
                FOR UPDATE
                """,
                lifecycle_id,
            )
        )

    # ------------------------------------------------------------------
    # Lossless observations
    # ------------------------------------------------------------------

    async def insert_observation_tx(
        self,
        connection: asyncpg.Connection,
        observation: Observation,
    ) -> bool:
        if not all(
            (
                observation.observation_id,
                observation.source_event_id,
                observation.source_event_type,
                observation.source_event_subject,
                observation.source_event_source,
                observation.source_event_producer,
                observation.ordering_key,
                observation.observed_at,
                observation.payload_hash,
            )
        ):
            raise ValueError("production observations require complete source provenance")
        inserted = await connection.fetchval(
            """
            INSERT INTO lifecycle_observations
                (observation_id, lifecycle_id, source_event_id, source_event_type,
                 source_event_subject, source_event_source, source_event_producer,
                 ordering_key, source, kind, observed_at, received_at, expires_at,
                 payload, payload_hash, confidence)
            VALUES
                ($1::uuid, $2, $3::uuid, $4, $5, $6, $7, $8, $9, $10, $11,
                 COALESCE($12, now()), $13, $14::jsonb, $15, $16)
            ON CONFLICT (source_event_id) DO NOTHING
            RETURNING observation_id::text
            """,
            observation.observation_id,
            observation.lifecycle_id,
            observation.source_event_id,
            observation.source_event_type,
            observation.source_event_subject,
            observation.source_event_source,
            observation.source_event_producer,
            observation.ordering_key,
            observation.source,
            observation.kind,
            observation.observed_at,
            observation.received_at,
            observation.expires_at,
            canonical_json(observation.payload),
            observation.payload_hash,
            observation.confidence,
        )
        return inserted is not None

    async def get_recent_observations(
        self,
        lifecycle_id: str,
        limit: int = 100,
        as_of: datetime | None = None,
    ) -> list[Observation]:
        decision_time = as_of or datetime.max.replace(tzinfo=UTC)
        async with self.pool.acquire() as connection:
            return await self._get_observations(connection, lifecycle_id, decision_time, limit)

    async def _get_observations(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
        as_of: datetime,
        limit: int | None = None,
    ) -> list[Observation]:
        query = """
            SELECT * FROM lifecycle_observations
            WHERE lifecycle_id = $1
              AND observed_at <= $2
              AND (expires_at IS NULL OR expires_at > $2)
            ORDER BY observed_at, source_event_id, observation_id
        """
        arguments: tuple[Any, ...] = (lifecycle_id, as_of)
        if limit is not None:
            query += " LIMIT $3"
            arguments = (*arguments, limit)
        rows = await connection.fetch(query, *arguments)
        return [_row_to_observation(row) for row in rows]

    # ------------------------------------------------------------------
    # Blockers, gates, checkpoints
    # ------------------------------------------------------------------

    async def get_active_blockers(self, lifecycle_id: str) -> list[Blocker]:
        async with self.pool.acquire() as connection:
            return await self._get_blockers(connection, lifecycle_id)

    async def _get_blockers(
        self, connection: asyncpg.Connection, lifecycle_id: str
    ) -> list[Blocker]:
        rows = await connection.fetch(
            """
            SELECT * FROM lifecycle_blockers
            WHERE lifecycle_id = $1 AND resolved_at IS NULL
            ORDER BY id
            """,
            lifecycle_id,
        )
        return [_row_to_blocker(row) for row in rows]

    async def get_active_gates(self, lifecycle_id: str) -> list[Gate]:
        async with self.pool.acquire() as connection:
            return await self._get_gates(connection, lifecycle_id)

    async def _get_gates(self, connection: asyncpg.Connection, lifecycle_id: str) -> list[Gate]:
        rows = await connection.fetch(
            """
            SELECT * FROM lifecycle_gates
            WHERE lifecycle_id = $1 AND resolved_at IS NULL
            ORDER BY id
            """,
            lifecycle_id,
        )
        return [_row_to_gate(row) for row in rows]

    async def resolve_gate_tx(
        self,
        connection: asyncpg.Connection,
        *,
        lifecycle_id: str,
        gate_id: str,
        resolution: str,
        resolved_at: datetime,
    ) -> None:
        result = await connection.execute(
            """
            UPDATE lifecycle_gates
            SET resolution = $1, resolved_at = $2
            WHERE lifecycle_id = $3 AND id = $4 AND resolved_at IS NULL
            """,
            resolution,
            resolved_at,
            lifecycle_id,
            gate_id,
        )
        if result != "UPDATE 1":
            raise ConcurrencyConflict("gate was no longer open")

    async def get_checkpoints(self, lifecycle_id: str) -> list[Checkpoint]:
        async with self.pool.acquire() as connection:
            return await self._get_checkpoints(connection, lifecycle_id)

    async def _get_checkpoints(
        self, connection: asyncpg.Connection, lifecycle_id: str
    ) -> list[Checkpoint]:
        rows = await connection.fetch(
            "SELECT * FROM lifecycle_checkpoints WHERE lifecycle_id = $1 ORDER BY id",
            lifecycle_id,
        )
        return [_row_to_checkpoint(row) for row in rows]

    # ------------------------------------------------------------------
    # Dirty queue and deterministic reconcile scheduling
    # ------------------------------------------------------------------

    async def claim_next_reconcile_job(self, worker_id: str, lease_seconds: int = 60) -> str | None:
        record = await self.claim_next_reconcile_job_record(worker_id, lease_seconds)
        return record[0] if record else None

    async def claim_next_reconcile_job_record(
        self, worker_id: str, lease_seconds: int = 60
    ) -> tuple[str, datetime] | None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT lifecycle_id, as_of FROM lifecycle_reconcile_queue
                    WHERE available_at <= now()
                      AND (
                        leased_by IS NULL
                        OR lease_expires_at IS NULL
                        OR lease_expires_at <= now()
                      )
                    ORDER BY priority DESC, available_at ASC, lifecycle_id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                )
                if not row:
                    return None
                lifecycle_id = row["lifecycle_id"]
                await connection.execute(
                    """
                    UPDATE lifecycle_reconcile_queue
                    SET leased_by = $1,
                        lease_expires_at = now() + $2::int * interval '1 second',
                        attempts = attempts + 1
                    WHERE lifecycle_id = $3
                    """,
                    worker_id,
                    lease_seconds,
                    lifecycle_id,
                )
                return lifecycle_id, _row_get(row, "as_of", datetime(1970, 1, 1, tzinfo=UTC))

    async def release_lease(self, lifecycle_id: str, requeue_delay_seconds: int = 0) -> None:
        await self.pool.execute(
            """
            UPDATE lifecycle_reconcile_queue
            SET leased_by = NULL,
                lease_expires_at = NULL,
                available_at = now() + $1::int * interval '1 second'
            WHERE lifecycle_id = $2
            """,
            requeue_delay_seconds,
            lifecycle_id,
        )

    async def delete_reconcile_job(self, lifecycle_id: str) -> None:
        await self.pool.execute(
            "DELETE FROM lifecycle_reconcile_queue WHERE lifecycle_id = $1",
            lifecycle_id,
        )

    async def delete_reconcile_job_tx(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
    ) -> None:
        await connection.execute(
            "DELETE FROM lifecycle_reconcile_queue WHERE lifecycle_id = $1",
            lifecycle_id,
        )

    async def complete_reconcile_job_tx(
        self,
        connection: asyncpg.Connection,
        *,
        lifecycle_id: str,
        worker_id: str,
        claimed_as_of: datetime,
    ) -> bool:
        """Delete only the exact claimed generation, preserving newer dirtiness."""

        deleted = await connection.fetchval(
            """
            DELETE FROM lifecycle_reconcile_queue
            WHERE lifecycle_id = $1
              AND leased_by = $2
              AND as_of <= $3
            RETURNING lifecycle_id
            """,
            lifecycle_id,
            worker_id,
            claimed_as_of,
        )
        return deleted is not None

    async def discard_reconcile_through_tx(
        self,
        connection: asyncpg.Connection,
        *,
        lifecycle_id: str,
        through: datetime,
    ) -> None:
        """Discard queue generations made obsolete by a newer command decision."""

        await connection.execute(
            """
            DELETE FROM lifecycle_reconcile_queue
            WHERE lifecycle_id = $1 AND as_of <= $2
            """,
            lifecycle_id,
            through,
        )

    async def enqueue_sweep(self, as_of: datetime) -> int:
        decision_time = as_of
        result = await self.pool.execute(
            """
            INSERT INTO lifecycle_reconcile_queue
                (lifecycle_id, reason, as_of, available_at)
            SELECT id, 'periodic_sweep', $1, now()
            FROM lifecycles
            WHERE status NOT IN ('completed', 'canceled', 'archived', 'disabled')
            ON CONFLICT (lifecycle_id) DO UPDATE SET
                reason = 'periodic_sweep',
                as_of = GREATEST(lifecycle_reconcile_queue.as_of, EXCLUDED.as_of),
                available_at = now()
            """,
            decision_time,
        )
        parts = result.split()
        return int(parts[-1]) if parts else 0

    async def _mark_dirty(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
        reason: str,
        as_of: datetime,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO lifecycle_reconcile_queue
                (lifecycle_id, reason, as_of, available_at)
            VALUES ($1, $2, GREATEST(
                $3,
                COALESCE(
                    (SELECT last_reconciled_at FROM lifecycle_state
                     WHERE lifecycle_id = $1),
                    $3
                )
            ), now())
            ON CONFLICT (lifecycle_id) DO UPDATE SET
                reason = EXCLUDED.reason,
                as_of = GREATEST(lifecycle_reconcile_queue.as_of, EXCLUDED.as_of),
                available_at = now(),
                leased_by = NULL,
                lease_expires_at = NULL,
                updated_at = now()
            """,
            lifecycle_id,
            reason,
            as_of,
        )

    async def mark_dirty_tx(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
        reason: str,
        as_of: datetime,
    ) -> None:
        await self._mark_dirty(connection, lifecycle_id, reason, as_of)

    # ------------------------------------------------------------------
    # State/history persistence
    # ------------------------------------------------------------------

    async def persist_state_tx(
        self,
        connection: asyncpg.Connection,
        *,
        state: LifecycleState,
        expected_previous_version: int,
        transition: dict[str, Any],
        command_id: str | None,
    ) -> None:
        updated = await connection.fetchval(
            """
            UPDATE lifecycle_state
            SET spec_version = $2, status = $3, health = $4, mode = $5,
                phase = $6, progress_percent = $7, roadmap_version = $8,
                status_reason = $9, health_reason = $10,
                last_progress_at = $11, last_reconciled_at = $12,
                observed_through = $13, state_version = $14,
                state_fingerprint = $15, legal_frontier = $16::jsonb,
                obligations = $17::jsonb, capabilities = $18::jsonb,
                source_observation_ids = $19::jsonb, updated_at = $12,
                policy = $20::jsonb
            WHERE lifecycle_id = $1 AND state_version = $21
            RETURNING lifecycle_id
            """,
            *_state_values(state),
            expected_previous_version,
        )
        if not updated:
            raise ConcurrencyConflict("lifecycle state version changed concurrently")
        await connection.execute(
            """
            UPDATE lifecycles
            SET status = $2, health = $3, phase = $4,
                progress_percent = $5, roadmap_version = $6,
                current_spec_version = $7, updated_at = $8
            WHERE id = $1
            """,
            state.lifecycle_id,
            state.status.value,
            state.health.value,
            state.phase,
            state.progress_percent,
            state.roadmap_version,
            state.spec_version,
            state.last_reconciled_at,
        )
        await self._insert_history(
            connection,
            state=state,
            transition=transition,
            command_id=command_id,
        )

    async def _insert_history(
        self,
        connection: asyncpg.Connection,
        *,
        state: LifecycleState,
        transition: dict[str, Any],
        command_id: str | None,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO lifecycle_status_history
                (lifecycle_id, spec_version, state_version, status, health, mode,
                 phase, progress_percent, roadmap_version, status_reason,
                 state_fingerprint, snapshot, transition, command_id,
                 reconciled_at)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                 $12::jsonb, $13::jsonb, $14::uuid, $15)
            """,
            state.lifecycle_id,
            state.spec_version,
            state.state_version,
            state.status.value,
            state.health.value,
            state.mode.value,
            state.phase,
            state.progress_percent,
            state.roadmap_version,
            state.status_reason,
            state.state_fingerprint,
            canonical_json(state_snapshot(state)),
            canonical_json(transition),
            command_id,
            state.last_reconciled_at,
        )

    # ------------------------------------------------------------------
    # Command result/idempotency ledger
    # ------------------------------------------------------------------

    async def get_command_result_tx(
        self,
        connection: asyncpg.Connection,
        lifecycle_id: str,
        idempotency_key: str,
    ) -> asyncpg.Record | None:
        return await connection.fetchrow(
            """
            SELECT * FROM lifecycle_command_results
            WHERE lifecycle_id = $1 AND idempotency_key = $2
            """,
            lifecycle_id,
            idempotency_key,
        )

    async def find_command_result_tx(
        self,
        connection: asyncpg.Connection,
        *,
        lifecycle_id: str,
        idempotency_key: str,
        command_event_id: str,
        command_id: str,
    ) -> asyncpg.Record | None:
        """Find every identity that can make command delivery a retry/conflict."""

        return await connection.fetchrow(
            """
            SELECT * FROM lifecycle_command_results
            WHERE (lifecycle_id = $1 AND idempotency_key = $2)
               OR command_event_id = $3::uuid
               OR command_id = $4::uuid
            ORDER BY id
            LIMIT 1
            """,
            lifecycle_id,
            idempotency_key,
            command_event_id,
            command_id,
        )

    async def lock_command_identities_tx(
        self,
        connection: asyncpg.Connection,
        *,
        command_event_id: str,
        command_id: str,
    ) -> None:
        """Serialize globally unique command identities across aggregates."""

        identities = sorted((f"event:{command_event_id}", f"command:{command_id}"))
        for identity in identities:
            await connection.execute(
                """
                SELECT pg_advisory_xact_lock(
                    hashtext('lifecycle-command-identity-v1'),
                    hashtext($1)
                )
                """,
                identity,
            )

    async def insert_command_result_tx(
        self,
        connection: asyncpg.Connection,
        *,
        command: IntentCommand,
        request_sha256: str,
        result: CommandResult,
        reply_envelope: dict[str, Any],
        created_at: datetime,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO lifecycle_command_results
                (lifecycle_id, repo, command_event_id, command_id,
                 idempotency_key, request_sha256, verdict, mutated,
                 expected_state_version, observed_state_version,
                 resulting_state_version, applied_event_id, capability_id,
                 reason_code, reply_envelope, created_at)
            VALUES
                ($1, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10,
                 $11, $12::uuid, $13, $14, $15::jsonb, $16)
            """,
            command.lifecycle_id,
            command.repo,
            command.event_id,
            command.command_id,
            command.idempotency_key,
            request_sha256,
            result.verdict.value,
            result.mutated,
            command.expected_state_version,
            result.observed_state_version,
            result.resulting_state_version,
            result.applied_event_id,
            result.capability_id,
            result.reason_code,
            canonical_json(reply_envelope),
            created_at,
        )

    # ------------------------------------------------------------------
    # Transactional outbox
    # ------------------------------------------------------------------

    async def reserve_outbox_identity_tx(
        self,
        connection: asyncpg.Connection,
        *,
        lifecycle_id: str | None,
    ) -> tuple[int, int | None]:
        """Reserve the identity embedded in a canonical publication envelope.

        Lifecycle callers hold the aggregate state-row lock, which serializes
        ``MAX(event_sequence) + 1`` without making delivery order a second
        authority mechanism.
        """

        outbox_id = await connection.fetchval(
            "SELECT nextval(pg_get_serial_sequence('lifecycle_event_outbox', 'id'))"
        )
        event_sequence: int | None = None
        if lifecycle_id is not None:
            event_sequence = await connection.fetchval(
                """
                SELECT COALESCE(MAX(event_sequence), 0) + 1
                FROM lifecycle_event_outbox
                WHERE lifecycle_id = $1
                """,
                lifecycle_id,
            )
        return int(outbox_id), (int(event_sequence) if event_sequence is not None else None)

    async def insert_reserved_outbox_tx(
        self,
        connection: asyncpg.Connection,
        *,
        outbox_id: int,
        event_sequence: int | None,
        lifecycle_id: str | None,
        envelope: dict[str, Any],
        aggregate_version: int | None,
        created_at: datetime,
    ) -> bool:
        inserted = await connection.fetchval(
            """
            INSERT INTO lifecycle_event_outbox
                (id, lifecycle_id, event_id, event_type, subject, envelope,
                 payload, headers, event_sequence, aggregate_version,
                 created_at, next_attempt_at)
            VALUES
                ($1, $2, $3::uuid, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb,
                 $9, $10, $11, now())
            ON CONFLICT (event_id) DO NOTHING
            RETURNING id
            """,
            outbox_id,
            lifecycle_id,
            envelope["id"],
            envelope["type"],
            envelope["subject"],
            canonical_json(envelope),
            canonical_json(envelope["data"]),
            canonical_json(
                {
                    "correlationid": envelope["correlationid"],
                    "causationid": envelope["causationid"],
                }
            ),
            event_sequence,
            aggregate_version,
            created_at,
        )
        return inserted is not None

    async def stage_outbox_tx(
        self,
        connection: asyncpg.Connection,
        *,
        lifecycle_id: str | None,
        envelope: dict[str, Any],
        aggregate_version: int | None,
        created_at: datetime,
    ) -> tuple[int, int | None]:
        outbox_id, event_sequence = await self.reserve_outbox_identity_tx(
            connection,
            lifecycle_id=lifecycle_id,
        )
        await self.insert_reserved_outbox_tx(
            connection,
            outbox_id=outbox_id,
            event_sequence=event_sequence,
            lifecycle_id=lifecycle_id,
            envelope=envelope,
            aggregate_version=aggregate_version,
            created_at=created_at,
        )
        return outbox_id, event_sequence

    async def claim_outbox(
        self,
        worker_id: str,
        *,
        batch_size: int = 100,
        lease_seconds: int = 30,
    ) -> list[OutboxEvent]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    WITH due AS (
                        SELECT candidate.id
                        FROM lifecycle_event_outbox AS candidate
                        WHERE candidate.published_at IS NULL
                          AND candidate.next_attempt_at <= now()
                          AND (
                            candidate.locked_by IS NULL
                            OR candidate.lock_expires_at <= now()
                          )
                          AND (
                            candidate.lifecycle_id IS NULL
                            OR NOT EXISTS (
                                SELECT 1
                                FROM lifecycle_event_outbox AS earlier
                                WHERE earlier.lifecycle_id = candidate.lifecycle_id
                                  AND earlier.published_at IS NULL
                                  AND (
                                    earlier.event_sequence < candidate.event_sequence
                                    OR (
                                        earlier.event_sequence = candidate.event_sequence
                                        AND earlier.id < candidate.id
                                    )
                                  )
                            )
                          )
                        ORDER BY candidate.created_at, candidate.id
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE lifecycle_event_outbox outbox
                    SET locked_by = $2,
                        lock_expires_at = now() + $3::int * interval '1 second'
                    FROM due
                    WHERE outbox.id = due.id
                    RETURNING outbox.*
                    """,
                    batch_size,
                    worker_id,
                    lease_seconds,
                )
        return [_row_to_outbox_event(row) for row in rows]

    async def mark_outbox_published(self, outbox_id: int, worker_id: str | None = None) -> None:
        await self.pool.execute(
            """
            UPDATE lifecycle_event_outbox
            SET published_at = now(), publish_attempts = publish_attempts + 1,
                locked_by = NULL, lock_expires_at = NULL, error = NULL
            WHERE id = $1 AND ($2::text IS NULL OR locked_by = $2)
            """,
            outbox_id,
            worker_id,
        )

    async def mark_outbox_failed(
        self, outbox_id: int, error: str, worker_id: str | None = None
    ) -> None:
        await self.pool.execute(
            """
            UPDATE lifecycle_event_outbox
            SET publish_attempts = publish_attempts + 1,
                next_attempt_at = now() +
                    LEAST(300, power(2, LEAST(publish_attempts, 8))) * interval '1 second',
                locked_by = NULL, lock_expires_at = NULL, error = LEFT($2, 1000)
            WHERE id = $1 AND ($3::text IS NULL OR locked_by = $3)
            """,
            outbox_id,
            error,
            worker_id,
        )

    async def outbox_pending_count(self) -> int:
        return int(
            await self.pool.fetchval(
                "SELECT COUNT(*) FROM lifecycle_event_outbox WHERE published_at IS NULL"
            )
        )

    # ------------------------------------------------------------------
    # Sentinel and health helpers
    # ------------------------------------------------------------------

    async def upsert_heartbeat(
        self,
        sentinel_id: str,
        scope_kind: str,
        scope_id: str | None,
        status: str,
        error_summary: str | None = None,
        observed_at: datetime | None = None,
    ) -> None:
        if observed_at is None:
            raise ValueError("sentinel heartbeat requires caller-supplied observed_at")
        at = observed_at
        await self.pool.execute(
            """
            INSERT INTO sentinel_heartbeats
                (sentinel_id, scope_kind, scope_id, last_seen_at, status,
                 error_summary, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $4)
            ON CONFLICT (sentinel_id) DO UPDATE SET
                scope_kind = EXCLUDED.scope_kind,
                scope_id = EXCLUDED.scope_id,
                last_seen_at = EXCLUDED.last_seen_at,
                status = EXCLUDED.status,
                error_summary = EXCLUDED.error_summary,
                updated_at = EXCLUDED.updated_at
            """,
            sentinel_id,
            scope_kind,
            scope_id,
            at,
            status,
            error_summary,
        )

    async def get_sentinel_health(self) -> dict[str, str]:
        rows = await self.pool.fetch(
            "SELECT sentinel_id, status FROM sentinel_heartbeats ORDER BY sentinel_id"
        )
        return {row["sentinel_id"]: row["status"] for row in rows}

    async def get_stale_sentinels(self, threshold_minutes: int = 15) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """
            SELECT sentinel_id, scope_kind, scope_id, last_seen_at, status
            FROM sentinel_heartbeats
            WHERE last_seen_at < now() - $1::int * interval '1 minute'
            ORDER BY sentinel_id
            """,
            threshold_minutes,
        )
        return [dict(row) for row in rows]

    async def ping(self) -> bool:
        return await self.pool.fetchval("SELECT 1") == 1


def state_snapshot(state: LifecycleState) -> dict[str, Any]:
    return {
        "lifecycle_id": state.lifecycle_id,
        "spec_version": state.spec_version,
        "state_version": state.state_version,
        "status": state.status.value,
        "health": state.health.value,
        "mode": state.mode.value,
        "phase": state.phase,
        "progress_percent": state.progress_percent,
        "roadmap_version": state.roadmap_version,
        "status_reason": state.status_reason,
        "health_reason": state.health_reason,
        "last_progress_at": _timestamp_json(state.last_progress_at),
        "last_reconciled_at": _timestamp_json(state.last_reconciled_at),
        "observed_through": _timestamp_json(state.observed_through),
        "state_fingerprint": state.state_fingerprint,
        "legal_frontier": [item.to_json() for item in state.legal_frontier],
        "obligations": [item.to_json() for item in state.obligations],
        "capabilities": [item.to_json() for item in state.capabilities],
        "source_observation_ids": list(state.source_observation_ids),
    }


def _state_values(state: LifecycleState) -> tuple[Any, ...]:
    return (
        state.lifecycle_id,
        state.spec_version,
        state.status.value,
        state.health.value,
        state.mode.value,
        state.phase,
        state.progress_percent,
        state.roadmap_version,
        state.status_reason,
        state.health_reason,
        state.last_progress_at,
        state.last_reconciled_at,
        state.observed_through,
        state.state_version,
        state.state_fingerprint,
        canonical_json([item.to_json() for item in state.legal_frontier]),
        canonical_json([item.to_json() for item in state.obligations]),
        canonical_json([item.to_json() for item in state.capabilities]),
        canonical_json(state.source_observation_ids),
        canonical_json(state.policy.to_json()),
    )


def _timestamp_json(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, TypeError):
        return default


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_object(value: Any) -> dict[str, Any]:
    decoded = _json_value(value, {})
    return decoded if isinstance(decoded, dict) else {}


def _json_list(value: Any) -> list[Any]:
    decoded = _json_value(value, [])
    return decoded if isinstance(decoded, list) else []


def _parse_wire_time(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


def _required_wire_time(value: Any) -> datetime:
    parsed = _parse_wire_time(value)
    if parsed is None:
        raise ValueError("required wire timestamp is missing")
    return parsed


def _row_to_state(row: Any) -> LifecycleState:
    policy = _json_object(_row_get(row, "policy", {}))
    frontier = [
        FrontierItem(
            id=item["id"],
            kind=FrontierKind(item["kind"]),
            action=item["action"],
            allowed=item["allowed"],
            capability_required=item.get("capability_required"),
            reason_code=item["reason_code"],
            expected_state_version=item["expected_state_version"],
        )
        for item in _json_list(_row_get(row, "legal_frontier", []))
    ]
    obligations = [
        Obligation(
            id=item["id"],
            kind=item["kind"],
            status=ObligationStatus(item["status"]),
            description=item["description"],
            skill_ref=SkillRef(**item["skill_ref"]),
            owner_id=item.get("owner_id"),
            due_at=_parse_wire_time(item.get("due_at")),
            source_observation_ids=tuple(item.get("source_observation_ids", [])),
        )
        for item in _json_list(_row_get(row, "obligations", []))
    ]
    capabilities = [
        CapabilityGrant(
            capability_id=item["capability_id"],
            capability_version=int(item["capability_version"]),
            actor_id=item["actor_id"],
            actions=tuple(item["actions"]),
            scope=item["scope"],
            issued_at=_required_wire_time(item["issued_at"]),
            expires_at=_parse_wire_time(item.get("expires_at")),
            state_version=item["state_version"],
        )
        for item in _json_list(_row_get(row, "capabilities", []))
    ]
    return LifecycleState(
        lifecycle_id=row["lifecycle_id"],
        status=LifecycleStatus(row["status"]),
        health=LifecycleHealth(row["health"]),
        phase=_row_get(row, "phase"),
        progress_percent=_row_get(row, "progress_percent", 0.0) or 0.0,
        roadmap_version=_row_get(row, "roadmap_version", 1) or 1,
        status_reason=_row_get(row, "status_reason", "") or "",
        health_reason=_row_get(row, "health_reason", "") or "",
        last_progress_at=_row_get(row, "last_progress_at"),
        last_reconciled_at=_row_get(row, "last_reconciled_at"),
        state_version=_row_get(row, "state_version", 1) or 1,
        state_fingerprint=_row_get(row, "state_fingerprint", "") or "",
        policy=LifecyclePolicy.from_json(policy),
        spec_version=_row_get(row, "spec_version", 1) or 1,
        mode=OperatingMode(_row_get(row, "mode", "supervised")),
        legal_frontier=frontier,
        obligations=obligations,
        capabilities=capabilities,
        source_observation_ids=[
            str(item) for item in _json_list(_row_get(row, "source_observation_ids", []))
        ],
        observed_through=_row_get(row, "observed_through"),
    )


def _row_to_observation(row: Any) -> Observation:
    payload = _json_object(_row_get(row, "payload", {}))
    return Observation(
        id=_row_get(row, "id"),
        lifecycle_id=row["lifecycle_id"],
        source=row["source"],
        kind=row["kind"],
        observed_at=row["observed_at"],
        expires_at=_row_get(row, "expires_at"),
        payload=payload,
        payload_hash=_row_get(row, "payload_hash"),
        confidence=_row_get(row, "confidence", 1.0) or 1.0,
        observation_id=(
            str(_row_get(row, "observation_id")) if _row_get(row, "observation_id") else None
        ),
        source_event_id=(
            str(_row_get(row, "source_event_id")) if _row_get(row, "source_event_id") else None
        ),
        source_event_type=_row_get(row, "source_event_type"),
        source_event_subject=_row_get(row, "source_event_subject"),
        source_event_source=_row_get(row, "source_event_source"),
        source_event_producer=_row_get(row, "source_event_producer"),
        ordering_key=_row_get(row, "ordering_key"),
        received_at=_row_get(row, "received_at"),
    )


def _row_to_blocker(row: Any) -> Blocker:
    return Blocker(
        id=row["id"],
        kind=BlockerKind(row["kind"]),
        lifecycle_id=row["lifecycle_id"],
        scope=row["scope"],
        blocking=row["blocking"],
        summary=row["summary"] or "",
        owner_kind=row["owner_kind"],
        owner_id=row["owner_id"],
        created_at=row["created_at"],
    )


def _row_to_gate(row: Any) -> Gate:
    return Gate(
        id=row["id"],
        kind=GateKind(row["kind"]),
        blocking=row["blocking"],
        reason=row["reason"] or "",
        continue_policy=GatePolicy(row["continue_policy"]),
        owner_kind=row["owner_kind"],
        owner_id=row["owner_id"],
        sla_due_at=row["sla_due_at"],
        triggered_by_checkpoint_id=row["triggered_by_checkpoint_id"],
        opened_at=row["opened_at"],
        resolved_at=row["resolved_at"],
        resolution=GateResolution(row["resolution"]) if row["resolution"] else None,
        lifecycle_id=row["lifecycle_id"],
    )


def _row_to_checkpoint(row: Any) -> Checkpoint:
    return Checkpoint(
        id=row["id"],
        kind=CheckpointKind(row["kind"]),
        name=row["name"],
        roadmap_version=row["roadmap_version"] or 1,
        phase_id=row["phase_id"],
        reached_at=row["reached_at"],
        invalidated_at=row["invalidated_at"],
        evidence=_json_list(row["evidence"]),
    )


def _row_to_outbox_event(row: Any) -> OutboxEvent:
    return OutboxEvent(
        id=row["id"],
        lifecycle_id=_row_get(row, "lifecycle_id") or "",
        event_type=row["event_type"],
        payload=_json_object(_row_get(row, "payload", {})),
        headers=_json_object(_row_get(row, "headers", {})),
        created_at=row["created_at"],
        published_at=row["published_at"],
        publish_attempts=row["publish_attempts"] or 0,
        error=row["error"],
        event_id=(str(row["event_id"]) if _row_get(row, "event_id") else None),
        subject=_row_get(row, "subject", "") or "",
        envelope=_json_object(_row_get(row, "envelope", {})),
        event_sequence=_row_get(row, "event_sequence"),
        next_attempt_at=_row_get(row, "next_attempt_at"),
    )


__all__ = [
    "AuthorityBundle",
    "ConcurrencyConflict",
    "LifecycleRepository",
    "state_snapshot",
]
