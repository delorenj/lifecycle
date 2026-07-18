"""Pure deterministic lifecycle reconciliation engine.

No decision in this module reads the wall clock.  Callers pass ``as_of``; the
compatibility fallback derives a stable value from supplied domain inputs so
the extracted evaluator tests retain their original call shape.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from models import (
    Blocker,
    BlockerKind,
    CapabilityGrant,
    Checkpoint,
    FrontierItem,
    Gate,
    GateKind,
    LifecycleHealth,
    LifecycleSignals,
    LifecycleSpec,
    LifecycleState,
    LifecycleStatus,
    LifecycleVerdict,
    Obligation,
    Observation,
    OperatingMode,
    OutboxEvent,
)
from specification import (
    compute_frontier,
    compute_obligations,
    default_spec,
    projected_capabilities,
    transition_guard_reason,
)


UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass
class ReconcileResult:
    lifecycle_id: str
    previous_state: LifecycleState | None
    current_state: LifecycleState
    state_changed: bool = False
    status_changed: bool = False
    health_changed: bool = False
    blockers_delta: list[dict[str, Any]] = field(default_factory=list)
    checkpoints_delta: list[dict[str, Any]] = field(default_factory=list)
    gates_delta: list[dict[str, Any]] = field(default_factory=list)
    outbox_events: list[OutboxEvent] = field(default_factory=list)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("lifecycle timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _derived_as_of(
    current_state: LifecycleState,
    observations: list[Observation],
    active_blockers: list[Blocker],
    active_gates: list[Gate],
    checkpoints: list[Checkpoint],
    explicit: datetime | None,
) -> datetime:
    if explicit is not None:
        return _aware(explicit)
    candidates = [
        value
        for value in (
            current_state.last_reconciled_at,
            current_state.last_progress_at,
            *(observation.observed_at for observation in observations),
            *(blocker.created_at for blocker in active_blockers),
            *(gate.opened_at for gate in active_gates),
            *(gate.sla_due_at for gate in active_gates),
            *(gate.resolved_at for gate in active_gates),
            *(checkpoint.reached_at for checkpoint in checkpoints),
            *(checkpoint.invalidated_at for checkpoint in checkpoints),
        )
        if value is not None
    ]
    return max((_aware(value) for value in candidates), default=EPOCH)


def _apply_operating_mode(
    current_state: LifecycleState,
    verdict: LifecycleVerdict,
) -> LifecycleVerdict:
    """Apply the explicit mode policy to an otherwise automatic verdict."""

    if current_state.mode == OperatingMode.DISABLED:
        return LifecycleVerdict(
            status=current_state.status,
            health=LifecycleHealth.NOMINAL,
            reason="MODE_DISABLED",
            blockers=verdict.blockers,
            signals=verdict.signals,
        )
    if current_state.mode == OperatingMode.MANUAL and verdict.status != current_state.status:
        return LifecycleVerdict(
            status=current_state.status,
            health=verdict.health,
            reason="MANUAL_MODE_HOLD",
            blockers=verdict.blockers,
            signals=verdict.signals,
        )
    return verdict


def evaluate_lifecycle(
    current_state: LifecycleState,
    observations: list[Observation],
    active_blockers: list[Blocker],
    active_gates: list[Gate],
    checkpoints: list[Checkpoint],
    sentinel_health: dict[str, str],
    as_of: datetime | None = None,
    *,
    require_observations: bool = False,
) -> LifecycleVerdict:
    """Compute a verdict from ordered facts and explicit time.

    ``require_observations=False`` preserves the exact extracted evaluator call
    shape. The standalone authority always supplies a versioned specification
    through :func:`reconcile`, which enables the fail-closed observation rule.
    """

    decision_time = _derived_as_of(
        current_state,
        observations,
        active_blockers,
        active_gates,
        checkpoints,
        as_of,
    )
    policy = current_state.policy

    if current_state.status in (
        LifecycleStatus.PAUSED,
        LifecycleStatus.DISABLED,
        LifecycleStatus.COMPLETED,
        LifecycleStatus.CANCELED,
        LifecycleStatus.ARCHIVED,
    ):
        return LifecycleVerdict(
            status=current_state.status,
            health=LifecycleHealth.NOMINAL,
            reason="INTENTIONAL_NON_PROGRESS",
            signals=LifecycleSignals(),
        )

    eligible_observations = [
        observation
        for observation in observations
        if observation.observed_at is None or _aware(observation.observed_at) <= decision_time
    ]
    signals = _aggregate_signals(eligible_observations)
    signals.open_blockers = len([blocker for blocker in active_blockers if blocker.blocking])

    blocking_gates = [gate for gate in active_gates if gate.blocking and gate.resolved_at is None]
    if blocking_gates:
        sla_breached = any(
            gate.sla_due_at is not None and _aware(gate.sla_due_at) < decision_time
            for gate in blocking_gates
        )
        return _apply_operating_mode(
            current_state,
            LifecycleVerdict(
                status=LifecycleStatus.WAITING,
                health=LifecycleHealth.AT_RISK if sla_breached else LifecycleHealth.NOMINAL,
                reason="BLOCKING_GATE_OPEN",
                blockers=[
                    Blocker(
                        id=gate.id,
                        kind=(
                            BlockerKind.HUMAN_REVIEW_REQUIRED
                            if gate.kind == GateKind.HUMAN_REVIEW
                            else BlockerKind.DEPENDENCY_NOT_READY
                        ),
                        lifecycle_id=current_state.lifecycle_id,
                        scope="lifecycle",
                        blocking=True,
                        summary=gate.reason or f"Gate {gate.kind.value} open",
                        owner_kind=gate.owner_kind,
                        owner_id=gate.owner_id,
                        created_at=gate.opened_at or decision_time,
                    )
                    for gate in sorted(blocking_gates, key=lambda item: item.id)
                ],
                signals=signals,
            ),
        )

    if signals.runnable_work_items == 0 and signals.open_blockers > 0:
        return _apply_operating_mode(
            current_state,
            LifecycleVerdict(
                status=LifecycleStatus.BLOCKED,
                health=LifecycleHealth.BLOCKED,
                reason="NO_RUNNABLE_WORK",
                blockers=sorted(active_blockers, key=lambda item: item.id),
                signals=signals,
            ),
        )

    observed_progress_at = signals.last_progress_at or current_state.last_progress_at
    if (
        signals.runnable_work_items > 0
        and observed_progress_at is not None
        and policy.progress_expected
    ):
        elapsed_minutes = (decision_time - _aware(observed_progress_at)).total_seconds() / 60
        if elapsed_minutes > policy.stalled_after_minutes:
            return _apply_operating_mode(
                current_state,
                LifecycleVerdict(
                    status=LifecycleStatus.ACTIVE,
                    health=LifecycleHealth.STALLED,
                    reason="RUNNABLE_WORK_NOT_ADVANCING",
                    signals=signals,
                ),
            )

    if require_observations and not eligible_observations:
        return _apply_operating_mode(
            current_state,
            LifecycleVerdict(
                status=LifecycleStatus.ACTIVE,
                health=LifecycleHealth.DEGRADED,
                reason="OBSERVATIONS_MISSING",
                signals=signals,
            ),
        )

    degraded_observers = sorted(
        sentinel_id for sentinel_id, status in sentinel_health.items() if status != "healthy"
    )
    if degraded_observers:
        return _apply_operating_mode(
            current_state,
            LifecycleVerdict(
                status=LifecycleStatus.ACTIVE,
                health=LifecycleHealth.DEGRADED,
                reason="OBSERVABILITY_DEGRADED",
                signals=signals,
            ),
        )

    return _apply_operating_mode(
        current_state,
        LifecycleVerdict(
            status=LifecycleStatus.ACTIVE,
            health=LifecycleHealth.NOMINAL,
            reason="PROGRESSING",
            signals=signals,
        ),
    )


def _observation_sort_key(observation: Observation) -> tuple[Any, ...]:
    return (
        _aware(observation.observed_at) if observation.observed_at else EPOCH,
        observation.source,
        observation.kind,
        observation.source_event_id or observation.observation_id or "",
        json.dumps(observation.payload, sort_keys=True, separators=(",", ":")),
    )


def _aggregate_signals(observations: list[Observation]) -> LifecycleSignals:
    signals = LifecycleSignals()
    for observation in sorted(observations, key=_observation_sort_key):
        payload = observation.payload
        if observation.kind == "work_items_snapshot":
            signals.open_work_items = payload.get("open_count", signals.open_work_items)
            signals.runnable_work_items = payload.get("runnable_count", signals.runnable_work_items)
        elif observation.kind == "agent_runs_snapshot":
            signals.active_agent_runs = payload.get("active_count", signals.active_agent_runs)
        elif observation.kind == "repo_activity_snapshot":
            if "last_commit_at" in payload:
                try:
                    signals.last_progress_at = _aware(
                        datetime.fromisoformat(
                            str(payload["last_commit_at"]).replace("Z", "+00:00")
                        )
                    )
                except (TypeError, ValueError):
                    pass
        elif observation.kind == "repo_task_event" and observation.observed_at:
            # Source time is progress evidence.  Provider/UI column values in
            # ``payload.from``/``payload.to`` are deliberately not interpreted
            # as lifecycle state.
            signals.last_progress_at = _aware(observation.observed_at)
    return signals


def _frontier_json(items: list[FrontierItem]) -> list[dict[str, Any]]:
    return [item.to_json() for item in items]


def _obligations_json(items: list[Obligation]) -> list[dict[str, Any]]:
    return [item.to_json() for item in items]


def _capabilities_json(items: list[CapabilityGrant]) -> list[dict[str, Any]]:
    return [item.to_json() for item in items]


def state_fingerprint(state: LifecycleState) -> str:
    """Hash all deterministic authority output, excluding timestamps/version."""

    payload = {
        "spec_version": state.spec_version,
        "status": state.status.value,
        "health": state.health.value,
        "mode": state.mode.value,
        "phase": state.phase,
        "progress_percent": state.progress_percent,
        "roadmap_version": state.roadmap_version,
        "status_reason": state.status_reason,
        "health_reason": state.health_reason,
        "legal_frontier": _frontier_json(state.legal_frontier),
        "obligations": _obligations_json(state.obligations),
        "capabilities": _capabilities_json(state.capabilities),
        "source_observation_ids": sorted(state.source_observation_ids),
        "observed_through": (
            state.observed_through.isoformat() if state.observed_through else None
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def reconcile(
    lifecycle_id: str,
    previous_state: LifecycleState | None,
    observations: list[Observation],
    active_blockers: list[Blocker],
    active_gates: list[Gate],
    checkpoints: list[Checkpoint],
    sentinel_health: dict[str, str],
    *,
    spec: LifecycleSpec | None = None,
    as_of: datetime | None = None,
) -> ReconcileResult:
    """Return one deterministic authority projection with no side effects."""

    authority_spec = spec or default_spec(lifecycle_id)
    seed_state = previous_state or LifecycleState(
        lifecycle_id=lifecycle_id,
        status=LifecycleStatus.PLANNED,
        health=LifecycleHealth.NOMINAL,
        spec_version=authority_spec.version,
        mode=authority_spec.default_mode,
    )
    decision_time = _derived_as_of(
        seed_state,
        observations,
        active_blockers,
        active_gates,
        checkpoints,
        as_of,
    )
    eligible_observations = [
        observation
        for observation in observations
        if observation.observed_at is None or _aware(observation.observed_at) <= decision_time
    ]

    current_state = LifecycleState(
        lifecycle_id=lifecycle_id,
        status=seed_state.status,
        health=seed_state.health,
        phase=seed_state.phase,
        progress_percent=seed_state.progress_percent,
        roadmap_version=seed_state.roadmap_version,
        status_reason=seed_state.status_reason,
        health_reason=seed_state.health_reason,
        last_progress_at=seed_state.last_progress_at,
        last_reconciled_at=decision_time,
        state_version=seed_state.state_version,
        policy=seed_state.policy,
        spec_version=authority_spec.version,
        mode=seed_state.mode,
        obligations=list(seed_state.obligations),
    )
    departure_obligations = compute_obligations(
        current_state,
        authority_spec,
        eligible_observations,
        decision_time,
    )
    verdict = evaluate_lifecycle(
        current_state=current_state,
        observations=eligible_observations,
        active_blockers=active_blockers,
        active_gates=active_gates,
        checkpoints=checkpoints,
        sentinel_health=sentinel_health,
        as_of=decision_time,
        require_observations=spec is not None,
    )
    automatic_guard = transition_guard_reason(
        current_state,
        verdict.status,
        authority_spec,
        active_blockers,
        active_gates,
        departure_obligations,
    )
    if automatic_guard is not None:
        verdict = LifecycleVerdict(
            status=current_state.status,
            health=verdict.health,
            reason=automatic_guard,
            blockers=verdict.blockers,
            signals=verdict.signals,
        )
    current_state.status = verdict.status
    current_state.health = verdict.health
    current_state.status_reason = verdict.reason
    current_state.health_reason = verdict.reason
    if verdict.signals.last_progress_at is not None:
        current_state.last_progress_at = verdict.signals.last_progress_at
    current_state.source_observation_ids = sorted(
        {
            identity
            for observation in eligible_observations
            if (identity := observation.observation_id or observation.source_event_id)
        }
    )
    observed_times = [
        _aware(observation.observed_at)
        for observation in eligible_observations
        if observation.observed_at is not None
    ]
    current_state.observed_through = max(
        observed_times,
        default=(
            previous_state.observed_through
            if previous_state is not None and previous_state.observed_through is not None
            else decision_time
        ),
    )

    provisional_version = 1 if previous_state is None else previous_state.state_version
    current_state.state_version = provisional_version
    current_state.obligations = compute_obligations(
        current_state, authority_spec, eligible_observations, decision_time
    )
    current_state.legal_frontier = compute_frontier(
        current_state,
        authority_spec,
        active_blockers,
        active_gates,
        current_state.obligations,
    )
    current_state.capabilities = projected_capabilities(authority_spec, provisional_version)
    provisional_fingerprint = state_fingerprint(current_state)
    state_changed = (
        previous_state is None or previous_state.state_fingerprint != provisional_fingerprint
    )
    if previous_state is not None and state_changed:
        current_state.state_version = previous_state.state_version + 1
        current_state.legal_frontier = compute_frontier(
            current_state,
            authority_spec,
            active_blockers,
            active_gates,
            current_state.obligations,
        )
        current_state.capabilities = projected_capabilities(
            authority_spec, current_state.state_version
        )
    current_state.state_fingerprint = state_fingerprint(current_state)
    if previous_state is not None:
        state_changed = previous_state.state_fingerprint != current_state.state_fingerprint

    status_changed = previous_state is None or previous_state.status != current_state.status
    health_changed = previous_state is None or previous_state.health != current_state.health

    outbox_events: list[OutboxEvent] = []
    if status_changed or health_changed:
        outbox_events.append(
            OutboxEvent(
                lifecycle_id=lifecycle_id,
                event_type="bloodbank.v1.lifecycle.status.updated",
                payload={
                    "lifecycle_id": lifecycle_id,
                    "previous": _state_to_json(previous_state) if previous_state else None,
                    "current": _state_to_json(current_state),
                    "transition": {
                        "reason": verdict.reason,
                        "computed": True,
                        "detector": "delorenj/lifecycle@1.0.0",
                    },
                    "blockers": [
                        _blocker_to_json(blocker, decision_time) for blocker in verdict.blockers
                    ],
                    "signals": _signals_to_json(verdict.signals),
                },
                created_at=decision_time,
            )
        )

    blockers_delta: list[dict[str, Any]] = []
    for blocker in sorted(verdict.blockers, key=lambda item: item.id):
        blockers_delta.append({"action": "detected", "blocker": blocker})
        outbox_events.append(
            OutboxEvent(
                lifecycle_id=lifecycle_id,
                event_type="bloodbank.v1.lifecycle.blocker.detected",
                payload={
                    "lifecycle_id": lifecycle_id,
                    "blocker": _blocker_to_json(blocker, decision_time),
                },
                created_at=decision_time,
            )
        )

    return ReconcileResult(
        lifecycle_id=lifecycle_id,
        previous_state=previous_state,
        current_state=current_state,
        state_changed=state_changed,
        status_changed=status_changed,
        health_changed=health_changed,
        blockers_delta=blockers_delta,
        outbox_events=outbox_events,
    )


def _state_to_json(state: LifecycleState) -> dict[str, Any]:
    return {
        "status": state.status.value,
        "health": state.health.value,
        "phase": state.phase,
        "progress_percent": state.progress_percent,
    }


def _blocker_to_json(blocker: Blocker, detected_at: datetime) -> dict[str, Any]:
    return {
        "id": blocker.id,
        "kind": blocker.kind.value,
        "scope": blocker.scope,
        "blocking": blocker.blocking,
        "summary": blocker.summary,
        "owner_kind": blocker.owner_kind,
        "owner_id": blocker.owner_id,
        "detected_at": (blocker.created_at or detected_at).isoformat().replace("+00:00", "Z"),
        "source_observation_ids": [],
    }


def _signals_to_json(signals: LifecycleSignals) -> dict[str, Any]:
    return {
        "open_work_items": signals.open_work_items,
        "runnable_work_items": signals.runnable_work_items,
        "active_agent_runs": signals.active_agent_runs,
        "open_blockers": signals.open_blockers,
        "last_progress_at": (
            signals.last_progress_at.isoformat().replace("+00:00", "Z")
            if signals.last_progress_at
            else None
        ),
    }


__all__ = [
    "ReconcileResult",
    "evaluate_lifecycle",
    "reconcile",
    "state_fingerprint",
]
