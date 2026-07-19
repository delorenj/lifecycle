"""Transactional standalone lifecycle authority.

All lifecycle decisions use an explicit caller/source ``as_of`` timestamp. The
only wall-clock use in the service is transport scheduling, leases, and health;
those values never participate in lifecycle truth.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from contracts import (
    ContractError,
    build_event_envelope,
    build_reply_envelope,
    format_timestamp,
    payload_sha256,
    recover_intent_command,
    stable_uuid,
    validate_intent_command,
    validate_obligation_evidence_submitted,
    validate_repo_task_recorded,
)
from db.repository import AuthorityBundle, LifecycleRepository
from models import (
    Blocker,
    CommandResult,
    CommandVerdict,
    Gate,
    LifecycleHealth,
    LifecycleState,
    LifecycleStatus,
    Observation,
    OperatingMode,
)
from reconciler import reconcile, state_fingerprint
from specification import (
    compute_frontier,
    compute_obligations,
    intent_is_legal,
    projected_capabilities,
    validate_capability,
)


UTC = timezone.utc
AUTHORITY_NAME = "delorenj/lifecycle"
SNAPSHOT_TYPE = "bloodbank.v1.lifecycle.snapshot.updated"
STATUS_TYPE = "bloodbank.v1.lifecycle.status.updated"
OBSERVATION_TYPE = "bloodbank.v1.lifecycle.observation.recorded"
TERMINAL_STATUSES = {
    LifecycleStatus.PAUSED,
    LifecycleStatus.DISABLED,
    LifecycleStatus.COMPLETED,
    LifecycleStatus.CANCELED,
    LifecycleStatus.ARCHIVED,
}


class UnaddressableCommand(ContractError):
    """A malformed command lacks fields required by the canonical reply schema."""


@dataclass(frozen=True)
class CommandHandlingResult:
    command: Any
    result: CommandResult
    reply_envelope: dict[str, Any]


def _state_summary(state: LifecycleState) -> dict[str, Any]:
    return {
        "status": state.status.value,
        "health": state.health.value,
        "phase": state.phase,
        "progress_percent": state.progress_percent,
    }


def _blocker_wire(blocker: Blocker, state: LifecycleState, as_of: datetime) -> dict[str, Any]:
    return {
        "id": blocker.id,
        "kind": blocker.kind.value,
        "scope": blocker.scope,
        "blocking": blocker.blocking,
        "summary": blocker.summary,
        "owner_kind": blocker.owner_kind,
        "owner_id": blocker.owner_id,
        "detected_at": format_timestamp(blocker.created_at or as_of),
        "source_observation_ids": list(state.source_observation_ids),
    }


def _gate_wire(gate: Gate, as_of: datetime) -> dict[str, Any]:
    return {
        "id": gate.id,
        "kind": gate.kind.value,
        "blocking": gate.blocking,
        "status": "resolved" if gate.resolved_at else "opened",
        "reason": gate.reason,
        "opened_at": format_timestamp(gate.opened_at or as_of),
        "resolved_at": format_timestamp(gate.resolved_at) if gate.resolved_at else None,
    }


def _reconciliation_id(state: LifecycleState) -> str:
    return stable_uuid(
        "lifecycle-reconciliation:"
        f"{state.lifecycle_id}:{state.spec_version}:{state.state_version}:"
        f"{state.state_fingerprint}"
    )


def _provenance(
    state: LifecycleState,
    bundle: AuthorityBundle,
    authority_instance: str,
) -> dict[str, Any]:
    return {
        "authority": AUTHORITY_NAME,
        "authority_instance": authority_instance,
        "reconciliation_id": _reconciliation_id(state),
        "policy_version": bundle.spec.policy_version,
        "source_observation_ids": list(state.source_observation_ids),
    }


def _freshness(state: LifecycleState, as_of: datetime) -> dict[str, Any]:
    observed_through = state.observed_through or as_of
    max_age_seconds = state.policy.observer_stale_after_minutes * 60
    age_seconds = max(0, int((as_of - observed_through).total_seconds()))
    has_observations = bool(state.source_observation_ids)
    return {
        "observed_through": format_timestamp(observed_through),
        "evaluated_at": format_timestamp(as_of),
        "status": ("fresh" if has_observations and age_seconds <= max_age_seconds else "stale"),
        "max_age_seconds": max_age_seconds,
    }


def _publication(
    *,
    outbox_id: int,
    lifecycle_id: str,
    state_version: int,
    event_sequence: int,
) -> dict[str, Any]:
    return {
        "outbox_id": outbox_id,
        "aggregate_id": lifecycle_id,
        "aggregate_version": state_version,
        "event_sequence": event_sequence,
    }


def _snapshot_data(
    *,
    bundle: AuthorityBundle,
    previous_state: LifecycleState | None,
    current_state: LifecycleState,
    gates: list[Gate],
    as_of: datetime,
    authority_instance: str,
    publication: dict[str, Any],
) -> dict[str, Any]:
    return {
        "contract_version": 3,
        "lifecycle_id": bundle.lifecycle_id,
        "repo": bundle.repo,
        "spec_version": current_state.spec_version,
        "state_version": current_state.state_version,
        "previous_state_version": (
            previous_state.state_version if previous_state is not None else None
        ),
        "state": _state_summary(current_state),
        "legal_frontier": [item.to_json() for item in current_state.legal_frontier],
        "obligations": [item.to_json() for item in current_state.obligations],
        "blockers": [
            _blocker_wire(blocker, current_state, as_of)
            for blocker in sorted(bundle.blockers, key=lambda item: item.id)
        ],
        "gates": [_gate_wire(gate, as_of) for gate in sorted(gates, key=lambda item: item.id)],
        "capabilities": [item.to_json() for item in current_state.capabilities],
        "provenance": _provenance(current_state, bundle, authority_instance),
        "freshness": _freshness(current_state, as_of),
        "publication": publication,
    }


def _status_data(
    *,
    bundle: AuthorityBundle,
    previous_state: LifecycleState | None,
    current_state: LifecycleState,
    as_of: datetime,
    authority_instance: str,
    transition_reason: str,
    publication: dict[str, Any],
) -> dict[str, Any]:
    return {
        "contract_version": 1,
        "lifecycle_id": bundle.lifecycle_id,
        "repo": bundle.repo,
        "spec_version": current_state.spec_version,
        "state_version": current_state.state_version,
        "previous_state_version": (
            previous_state.state_version if previous_state is not None else None
        ),
        "previous": _state_summary(previous_state) if previous_state else None,
        "current": _state_summary(current_state),
        "transition": {
            "reason": transition_reason,
            "computed": True,
            "detector": "delorenj/lifecycle@1.0.0",
            "confidence": 1.0,
        },
        "blockers": [
            _blocker_wire(blocker, current_state, as_of)
            for blocker in sorted(bundle.blockers, key=lambda item: item.id)
        ],
        "provenance": _provenance(current_state, bundle, authority_instance),
        "freshness": _freshness(current_state, as_of),
        "publication": publication,
    }


def _observation_data(
    observation: Observation,
    *,
    repo: str,
) -> dict[str, Any]:
    if observation.observed_at is None:
        raise ValueError("canonical observation requires source event time")
    return {
        "contract_version": 1,
        "observation_id": observation.observation_id,
        "lifecycle_id": observation.lifecycle_id,
        "repo": repo,
        "observation_kind": observation.kind,
        "source_event": {
            "event_id": observation.source_event_id,
            "type": observation.source_event_type,
            "subject": observation.source_event_subject,
            "source": observation.source_event_source,
            "producer": observation.source_event_producer,
            "ordering_key": observation.ordering_key,
            "observed_at": format_timestamp(observation.observed_at),
        },
        "payload_sha256": observation.payload_hash,
        "payload": observation.payload,
    }


def _row_result(row: Mapping[str, Any]) -> CommandResult:
    return CommandResult(
        verdict=CommandVerdict(row["verdict"]),
        mutated=bool(row["mutated"]),
        observed_state_version=int(row["observed_state_version"]),
        resulting_state_version=(
            int(row["resulting_state_version"])
            if row["resulting_state_version"] is not None
            else None
        ),
        applied_event_id=(str(row["applied_event_id"]) if row["applied_event_id"] else None),
        capability_id=row["capability_id"],
        reason_code=row["reason_code"],
    )


def _trusted_publication_time(value: datetime) -> datetime:
    """Normalize an explicitly trusted broker/direct-ingestion timestamp."""

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("trusted publication time must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _canonical_authority_time(value: datetime) -> datetime:
    """Project authority decisions onto the canonical millisecond wire clock."""

    value = _trusted_publication_time(value)
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def _project_command_state(
    *,
    bundle: AuthorityBundle,
    gates: list[Gate],
    as_of: datetime,
    intent_name: str,
    intent_target: str,
    reason_code: str,
) -> LifecycleState:
    state = deepcopy(bundle.state)
    if intent_name == "transition":
        state.status = LifecycleStatus(intent_target)
        if state.status in TERMINAL_STATUSES:
            state.health = LifecycleHealth.NOMINAL
            state.health_reason = "INTENTIONAL_NON_PROGRESS"
    elif intent_name == "set_mode":
        state.mode = OperatingMode(intent_target)

    state.spec_version = bundle.spec.version
    state.state_version = bundle.state.state_version + 1
    state.status_reason = reason_code
    state.last_reconciled_at = as_of
    state.obligations = compute_obligations(
        state,
        bundle.spec,
        bundle.observations,
        as_of,
    )
    state.legal_frontier = compute_frontier(
        state,
        bundle.spec,
        bundle.blockers,
        gates,
        state.obligations,
    )
    state.capabilities = projected_capabilities(bundle.spec, state.state_version)
    state.state_fingerprint = state_fingerprint(state)
    return state


class LifecycleAuthority:
    def __init__(
        self,
        repository: LifecycleRepository,
        *,
        authority_instance: str,
    ) -> None:
        if not authority_instance:
            raise ValueError("authority_instance must be non-empty")
        self.repository = repository
        self.authority_instance = authority_instance

    async def handle_command_envelope(
        self,
        envelope: Any,
        *,
        published_at: datetime,
    ) -> CommandHandlingResult:
        """Handle a command at its trusted immutable publication time.

        ``requested_at`` remains producer-authored request metadata and is used
        only by command-contract and capability-causality validation. Authority
        chronology is derived exclusively from this required trusted timestamp.
        """

        decision_at = _canonical_authority_time(published_at)
        try:
            command = validate_intent_command(envelope)
        except ContractError as strict_error:
            try:
                command = recover_intent_command(envelope)
            except ContractError as recovery_error:
                raise UnaddressableCommand(
                    recovery_error.reason_code,
                    f"command cannot be represented by canonical reply: {recovery_error.detail}",
                ) from strict_error
            return await self._handle_command(
                command,
                decision_at=decision_at,
                malformed_reason=strict_error.reason_code,
            )
        return await self._handle_command(command, decision_at=decision_at)

    async def _handle_command(
        self,
        command: Any,
        *,
        decision_at: datetime,
        malformed_reason: str | None = None,
    ) -> CommandHandlingResult:
        request_sha256 = payload_sha256(command.raw_envelope)
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                bundle = await self.repository.load_bundle(
                    connection,
                    command.lifecycle_id,
                    as_of=decision_at,
                    for_update=True,
                )
                if bundle is None:
                    await connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtext($1))",
                        f"lifecycle-command:{command.lifecycle_id}",
                    )
                await self.repository.lock_command_identities_tx(
                    connection,
                    command_event_id=command.event_id,
                    command_id=command.command_id,
                )

                existing = await self.repository.find_command_result_tx(
                    connection,
                    lifecycle_id=command.lifecycle_id,
                    idempotency_key=command.idempotency_key,
                    command_event_id=command.event_id,
                    command_id=command.command_id,
                )
                if existing is not None:
                    return await self._handle_recorded_command(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        existing=existing,
                        lifecycle_exists=bundle is not None,
                        decision_at=decision_at,
                    )

                observed_version = (
                    bundle.state.state_version
                    if bundle is not None
                    else command.expected_state_version
                )
                if malformed_reason is not None:
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.MALFORMED,
                        observed_version=observed_version,
                        reason_code=malformed_reason,
                        lifecycle_exists=bundle is not None,
                        decision_at=decision_at,
                    )
                if bundle is None:
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.MALFORMED,
                        observed_version=observed_version,
                        reason_code="LIFECYCLE_NOT_FOUND",
                        lifecycle_exists=False,
                        decision_at=decision_at,
                    )
                if command.repo != bundle.repo:
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.MALFORMED,
                        observed_version=observed_version,
                        reason_code="REPO_BINDING_MISMATCH",
                        lifecycle_exists=True,
                        decision_at=decision_at,
                    )
                if command.expected_state_version != bundle.state.state_version:
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.STALE,
                        observed_version=observed_version,
                        reason_code="EXPECTED_STATE_VERSION_MISMATCH",
                        lifecycle_exists=True,
                        decision_at=decision_at,
                    )
                if (
                    bundle.state.last_reconciled_at is not None
                    and decision_at < _canonical_authority_time(bundle.state.last_reconciled_at)
                ):
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.STALE,
                        observed_version=observed_version,
                        reason_code="PUBLICATION_TIME_BEFORE_CURRENT_STATE",
                        lifecycle_exists=True,
                        decision_at=decision_at,
                    )

                grant, capability_reason = validate_capability(command, bundle.spec)
                if grant is None:
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.UNAUTHORIZED,
                        observed_version=observed_version,
                        reason_code=capability_reason,
                        lifecycle_exists=True,
                        decision_at=decision_at,
                    )
                current_obligations = compute_obligations(
                    bundle.state,
                    bundle.spec,
                    bundle.observations,
                    decision_at,
                )
                legal, legal_reason = intent_is_legal(
                    command,
                    bundle.state,
                    bundle.spec,
                    bundle.blockers,
                    bundle.gates,
                    current_obligations,
                )
                if not legal:
                    return await self._record_rejection(
                        connection,
                        command=command,
                        request_sha256=request_sha256,
                        verdict=CommandVerdict.ILLEGAL,
                        observed_version=observed_version,
                        reason_code=legal_reason,
                        lifecycle_exists=True,
                        capability_id=grant.capability_id,
                        decision_at=decision_at,
                    )

                gates = list(bundle.gates)
                if command.intent.name == "resolve_gate":
                    await self.repository.resolve_gate_tx(
                        connection,
                        lifecycle_id=bundle.lifecycle_id,
                        gate_id=command.intent.target,
                        resolution=command.intent.parameters["resolution"],
                        resolved_at=decision_at,
                    )
                    gates = [gate for gate in gates if gate.id != command.intent.target]

                current_state = _project_command_state(
                    bundle=bundle,
                    gates=gates,
                    as_of=decision_at,
                    intent_name=command.intent.name,
                    intent_target=command.intent.target,
                    reason_code=legal_reason,
                )
                snapshot_event_id = stable_uuid(
                    "lifecycle-snapshot:"
                    f"{bundle.lifecycle_id}:{current_state.state_version}:"
                    f"{current_state.state_fingerprint}"
                )
                result = CommandResult(
                    verdict=CommandVerdict.APPLIED,
                    mutated=True,
                    observed_state_version=bundle.state.state_version,
                    resulting_state_version=current_state.state_version,
                    applied_event_id=snapshot_event_id,
                    capability_id=grant.capability_id,
                    reason_code=legal_reason,
                )
                reply = build_reply_envelope(
                    command=command,
                    result=result,
                    responded_at=decision_at,
                    authority_instance=self.authority_instance,
                )
                await self.repository.persist_state_tx(
                    connection,
                    state=current_state,
                    expected_previous_version=bundle.state.state_version,
                    transition={
                        "reason": legal_reason,
                        "computed": True,
                        "intent": {
                            "name": command.intent.name,
                            "target": command.intent.target,
                        },
                    },
                    command_id=command.command_id,
                )
                await self.repository.discard_reconcile_through_tx(
                    connection,
                    lifecycle_id=bundle.lifecycle_id,
                    through=decision_at,
                )
                await self._stage_state_publications(
                    connection,
                    bundle=bundle,
                    previous_state=bundle.state,
                    current_state=current_state,
                    gates=gates,
                    as_of=decision_at,
                    transition_reason=legal_reason,
                    correlation_id=command.correlation_id,
                    causation_id=command.event_id,
                    snapshot_event_id=snapshot_event_id,
                )
                await self.repository.insert_command_result_tx(
                    connection,
                    command=command,
                    request_sha256=request_sha256,
                    result=result,
                    reply_envelope=reply,
                    created_at=decision_at,
                )
                await self.repository.stage_outbox_tx(
                    connection,
                    lifecycle_id=bundle.lifecycle_id,
                    envelope=reply,
                    aggregate_version=current_state.state_version,
                    created_at=decision_at,
                )
                return CommandHandlingResult(command, result, reply)

    async def _handle_recorded_command(
        self,
        connection: Any,
        *,
        command: Any,
        request_sha256: str,
        existing: Mapping[str, Any],
        lifecycle_exists: bool,
        decision_at: datetime,
    ) -> CommandHandlingResult:
        recorded = _row_result(existing)
        same_request = (
            existing["request_sha256"] == request_sha256
            and str(existing["command_event_id"]) == command.event_id
            and str(existing["command_id"]) == command.command_id
            and existing["idempotency_key"] == command.idempotency_key
        )
        if not same_request:
            result = CommandResult(
                verdict=CommandVerdict.MALFORMED,
                mutated=False,
                observed_state_version=recorded.observed_state_version,
                resulting_state_version=None,
                applied_event_id=None,
                capability_id=None,
                reason_code="COMMAND_IDENTITY_REUSED",
            )
        elif recorded.verdict == CommandVerdict.APPLIED:
            result = CommandResult(
                verdict=CommandVerdict.IDEMPOTENT,
                mutated=False,
                observed_state_version=recorded.observed_state_version,
                resulting_state_version=recorded.resulting_state_version,
                applied_event_id=recorded.applied_event_id,
                capability_id=recorded.capability_id,
                reason_code="EFFECT_ALREADY_APPLIED",
            )
        else:
            result = recorded
        reply = build_reply_envelope(
            command=command,
            result=result,
            responded_at=decision_at,
            authority_instance=self.authority_instance,
        )
        await self.repository.stage_outbox_tx(
            connection,
            lifecycle_id=command.lifecycle_id if lifecycle_exists else None,
            envelope=reply,
            aggregate_version=result.resulting_state_version,
            created_at=decision_at,
        )
        return CommandHandlingResult(command, result, reply)

    async def _record_rejection(
        self,
        connection: Any,
        *,
        command: Any,
        request_sha256: str,
        verdict: CommandVerdict,
        observed_version: int,
        reason_code: str,
        lifecycle_exists: bool,
        capability_id: str | None = None,
        decision_at: datetime,
    ) -> CommandHandlingResult:
        result = CommandResult(
            verdict=verdict,
            mutated=False,
            observed_state_version=observed_version,
            resulting_state_version=None,
            applied_event_id=None,
            capability_id=capability_id,
            reason_code=reason_code,
        )
        reply = build_reply_envelope(
            command=command,
            result=result,
            responded_at=decision_at,
            authority_instance=self.authority_instance,
        )
        await self.repository.insert_command_result_tx(
            connection,
            command=command,
            request_sha256=request_sha256,
            result=result,
            reply_envelope=reply,
            created_at=decision_at,
        )
        await self.repository.stage_outbox_tx(
            connection,
            lifecycle_id=command.lifecycle_id if lifecycle_exists else None,
            envelope=reply,
            aggregate_version=None,
            created_at=decision_at,
        )
        return CommandHandlingResult(command, result, reply)

    async def ingest_repo_task_envelope(
        self,
        envelope: Any,
        *,
        received_at: datetime,
    ) -> bool:
        """Losslessly persist one bound canonical source event and enqueue replay."""

        received_at = _trusted_publication_time(received_at)
        repo = str(envelope.get("data", {}).get("repo", "")) if isinstance(envelope, dict) else ""
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                lifecycle_id = await connection.fetchval(
                    "SELECT id FROM lifecycles WHERE repo = $1",
                    repo,
                )
                if lifecycle_id is None:
                    return False
                if not await self.repository.lock_lifecycle_tx(connection, lifecycle_id):
                    return False
                observation = validate_repo_task_recorded(envelope, lifecycle_id)
                observation.received_at = received_at.astimezone(UTC)
                return await self._record_observation_tx(
                    connection=connection,
                    observation=observation,
                    repo=repo,
                    envelope=envelope,
                    received_at=received_at,
                )

    async def ingest_obligation_evidence_envelope(
        self,
        envelope: Any,
        *,
        received_at: datetime,
    ) -> bool:
        """Persist exact completion evidence as input for authority evaluation."""

        received_at = _trusted_publication_time(received_at)
        observation = validate_obligation_evidence_submitted(envelope)
        repo = str(observation.payload["repo"])
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                bound_repo = await connection.fetchval(
                    "SELECT repo FROM lifecycles WHERE id = $1",
                    observation.lifecycle_id,
                )
                if bound_repo is None:
                    return False
                if bound_repo != repo:
                    raise ContractError(
                        "REPO_BINDING_MISMATCH",
                        "completion evidence repo does not match lifecycle authority binding",
                    )
                if not await self.repository.lock_lifecycle_tx(
                    connection,
                    observation.lifecycle_id,
                ):
                    return False
                observation.received_at = received_at.astimezone(UTC)
                return await self._record_observation_tx(
                    connection=connection,
                    observation=observation,
                    repo=repo,
                    envelope=envelope,
                    received_at=received_at,
                )

    async def _record_observation_tx(
        self,
        *,
        connection: Any,
        observation: Observation,
        repo: str,
        envelope: Mapping[str, Any],
        received_at: datetime,
    ) -> bool:
        inserted = await self.repository.insert_observation_tx(connection, observation)
        if not inserted:
            return False
        event_id = stable_uuid(f"lifecycle-observation-recorded:{observation.observation_id}")
        recorded = build_event_envelope(
            event_type=OBSERVATION_TYPE,
            data=_observation_data(observation, repo=repo),
            event_id=event_id,
            occurred_at=received_at,
            correlation_id=str(envelope["correlationid"]),
            causation_id=observation.source_event_id,
            authority_instance=self.authority_instance,
        )
        await self.repository.stage_outbox_tx(
            connection,
            lifecycle_id=observation.lifecycle_id,
            envelope=recorded,
            aggregate_version=None,
            created_at=received_at,
        )
        if observation.observed_at is None:
            raise ValueError("source observation time is required")
        current_as_of = await connection.fetchval(
            "SELECT last_reconciled_at FROM lifecycle_state WHERE lifecycle_id = $1",
            observation.lifecycle_id,
        )
        # Producer-declared source time is evidence, not authority time.  A
        # future-dated event must never move the deterministic clock forward
        # or make a concurrently published command stale.  JetStream's
        # immutable publication timestamp is the trusted ingress boundary;
        # periodic sweeps will reconsider persisted future observations when
        # authority time actually reaches them.
        reconcile_as_of = (
            max(received_at, current_as_of) if current_as_of is not None else received_at
        )
        await self.repository.mark_dirty_tx(
            connection,
            observation.lifecycle_id,
            f"observation:{observation.source_event_type}",
            reconcile_as_of,
        )
        return True

    async def reconcile_claimed(
        self,
        *,
        lifecycle_id: str,
        as_of: datetime,
        worker_id: str,
    ) -> bool:
        """Reconcile one claimed queue generation in a single authority transaction."""

        claimed_as_of = as_of
        decision_at = _canonical_authority_time(as_of)
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                bundle = await self.repository.load_bundle(
                    connection,
                    lifecycle_id,
                    as_of=decision_at,
                    for_update=True,
                )
                if bundle is None:
                    await self.repository.complete_reconcile_job_tx(
                        connection,
                        lifecycle_id=lifecycle_id,
                        worker_id=worker_id,
                        claimed_as_of=claimed_as_of,
                    )
                    return False
                if (
                    bundle.state.last_reconciled_at is not None
                    and decision_at < _canonical_authority_time(bundle.state.last_reconciled_at)
                ):
                    await self.repository.complete_reconcile_job_tx(
                        connection,
                        lifecycle_id=lifecycle_id,
                        worker_id=worker_id,
                        claimed_as_of=claimed_as_of,
                    )
                    return False
                result = reconcile(
                    lifecycle_id=lifecycle_id,
                    previous_state=bundle.state,
                    observations=bundle.observations,
                    active_blockers=bundle.blockers,
                    active_gates=bundle.gates,
                    checkpoints=bundle.checkpoints,
                    sentinel_health=bundle.sentinel_health,
                    spec=bundle.spec,
                    as_of=decision_at,
                )
                if result.state_changed:
                    await self.repository.persist_state_tx(
                        connection,
                        state=result.current_state,
                        expected_previous_version=bundle.state.state_version,
                        transition={
                            "reason": result.current_state.status_reason,
                            "computed": True,
                        },
                        command_id=None,
                    )
                    correlation_id = _reconciliation_id(result.current_state)
                    snapshot_event_id = stable_uuid(
                        "lifecycle-snapshot:"
                        f"{bundle.lifecycle_id}:{result.current_state.state_version}:"
                        f"{result.current_state.state_fingerprint}"
                    )
                    await self._stage_state_publications(
                        connection,
                        bundle=bundle,
                        previous_state=bundle.state,
                        current_state=result.current_state,
                        gates=bundle.gates,
                        as_of=decision_at,
                        transition_reason=result.current_state.status_reason,
                        correlation_id=correlation_id,
                        causation_id=None,
                        snapshot_event_id=snapshot_event_id,
                    )
                await self.repository.complete_reconcile_job_tx(
                    connection,
                    lifecycle_id=lifecycle_id,
                    worker_id=worker_id,
                    claimed_as_of=claimed_as_of,
                )
                return result.state_changed

    async def _stage_state_publications(
        self,
        connection: Any,
        *,
        bundle: AuthorityBundle,
        previous_state: LifecycleState | None,
        current_state: LifecycleState,
        gates: list[Gate],
        as_of: datetime,
        transition_reason: str,
        correlation_id: str,
        causation_id: str | None,
        snapshot_event_id: str,
    ) -> None:
        outbox_id, event_sequence = await self.repository.reserve_outbox_identity_tx(
            connection,
            lifecycle_id=bundle.lifecycle_id,
        )
        if event_sequence is None:
            raise RuntimeError("lifecycle publication requires aggregate sequence")
        snapshot = build_event_envelope(
            event_type=SNAPSHOT_TYPE,
            data=_snapshot_data(
                bundle=bundle,
                previous_state=previous_state,
                current_state=current_state,
                gates=gates,
                as_of=as_of,
                authority_instance=self.authority_instance,
                publication=_publication(
                    outbox_id=outbox_id,
                    lifecycle_id=bundle.lifecycle_id,
                    state_version=current_state.state_version,
                    event_sequence=event_sequence,
                ),
            ),
            event_id=snapshot_event_id,
            occurred_at=as_of,
            correlation_id=correlation_id,
            causation_id=causation_id,
            authority_instance=self.authority_instance,
            schema_version=3,
        )
        await self.repository.insert_reserved_outbox_tx(
            connection,
            outbox_id=outbox_id,
            event_sequence=event_sequence,
            lifecycle_id=bundle.lifecycle_id,
            envelope=snapshot,
            aggregate_version=current_state.state_version,
            created_at=as_of,
        )

        status_changed = previous_state is None or (
            previous_state.status != current_state.status
            or previous_state.health != current_state.health
        )
        if not status_changed:
            return
        status_outbox_id, status_sequence = await self.repository.reserve_outbox_identity_tx(
            connection,
            lifecycle_id=bundle.lifecycle_id,
        )
        if status_sequence is None:
            raise RuntimeError("lifecycle publication requires aggregate sequence")
        status_event_id = stable_uuid(
            "lifecycle-status:"
            f"{bundle.lifecycle_id}:{current_state.state_version}:"
            f"{current_state.state_fingerprint}"
        )
        status = build_event_envelope(
            event_type=STATUS_TYPE,
            data=_status_data(
                bundle=bundle,
                previous_state=previous_state,
                current_state=current_state,
                as_of=as_of,
                authority_instance=self.authority_instance,
                transition_reason=transition_reason,
                publication=_publication(
                    outbox_id=status_outbox_id,
                    lifecycle_id=bundle.lifecycle_id,
                    state_version=current_state.state_version,
                    event_sequence=status_sequence,
                ),
            ),
            event_id=status_event_id,
            occurred_at=as_of,
            correlation_id=correlation_id,
            causation_id=causation_id,
            authority_instance=self.authority_instance,
        )
        await self.repository.insert_reserved_outbox_tx(
            connection,
            outbox_id=status_outbox_id,
            event_sequence=status_sequence,
            lifecycle_id=bundle.lifecycle_id,
            envelope=status,
            aggregate_version=current_state.state_version,
            created_at=as_of,
        )


__all__ = [
    "CommandHandlingResult",
    "LifecycleAuthority",
    "UnaddressableCommand",
]
