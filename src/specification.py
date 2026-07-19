"""Versioned lifecycle specification, guards, frontier, and obligations.

The functions in this module are pure.  Time-sensitive decisions receive an
explicit ``as_of`` value so replaying the same spec and inputs is stable.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
import uuid

from models import (
    ActorContext,
    Blocker,
    CapabilityContext,
    CapabilityGrant,
    FrontierItem,
    FrontierKind,
    Gate,
    IntentCommand,
    LifecycleSpec,
    LifecycleState,
    LifecycleStatus,
    Obligation,
    ObligationRule,
    ObligationStatus,
    Observation,
    OperatingMode,
    SkillRef,
    TransitionRule,
)


UTC = timezone.utc
CAPABILITY_ACTION = "lifecycle.intent.submit"
CAPABILITY_SCOPE_PREFIX = "lifecycle:"
OBLIGATION_EVIDENCE_TYPE = "bloodbank.v1.lifecycle.obligation_evidence.submitted"
OBLIGATION_EVIDENCE_SUBJECT = "bloodbank.evt.v1.lifecycle.obligation_evidence.submitted"
OBLIGATION_EVIDENCE_SOURCE = "urn:33god:service:momo"
OBLIGATION_EVIDENCE_PRODUCER = "momo"


def default_spec(
    lifecycle_id: str,
    *,
    version: int = 1,
    capabilities: Iterable[CapabilityGrant] = (),
) -> LifecycleSpec:
    """Return the deterministic v1 authority policy.

    No actor receives an implicit grant.  Callers must explicitly add grants to
    the versioned specification, which keeps bootstrap authority auditable.
    """

    transitions = (
        TransitionRule("transition", LifecycleStatus.PLANNED, LifecycleStatus.ACTIVE),
        TransitionRule("transition", LifecycleStatus.ACTIVE, LifecycleStatus.WAITING),
        TransitionRule("transition", LifecycleStatus.ACTIVE, LifecycleStatus.PAUSED),
        TransitionRule(
            "transition",
            LifecycleStatus.ACTIVE,
            LifecycleStatus.COMPLETED,
            guards=("no_blocking_gates", "no_blocking_blockers"),
        ),
        TransitionRule("transition", LifecycleStatus.ACTIVE, LifecycleStatus.CANCELED),
        TransitionRule(
            "transition",
            LifecycleStatus.WAITING,
            LifecycleStatus.ACTIVE,
            guards=("no_pending_obligations",),
        ),
        TransitionRule("transition", LifecycleStatus.WAITING, LifecycleStatus.CANCELED),
        TransitionRule("transition", LifecycleStatus.BLOCKED, LifecycleStatus.ACTIVE),
        TransitionRule("transition", LifecycleStatus.BLOCKED, LifecycleStatus.CANCELED),
        TransitionRule("transition", LifecycleStatus.PAUSED, LifecycleStatus.ACTIVE),
        TransitionRule("transition", LifecycleStatus.PAUSED, LifecycleStatus.CANCELED),
        TransitionRule("transition", LifecycleStatus.DISABLED, LifecycleStatus.PLANNED),
        TransitionRule("transition", LifecycleStatus.COMPLETED, LifecycleStatus.ARCHIVED),
        TransitionRule("transition", LifecycleStatus.CANCELED, LifecycleStatus.ARCHIVED),
    )
    obligations = (
        ObligationRule(
            id="independent-review",
            kind="independent_review",
            description="Obtain independent review before leaving a waiting gate.",
            skill_ref=SkillRef(name="bmad-code-review", selector="6.10.2"),
            when_statuses=(LifecycleStatus.WAITING,),
            owner_id="agent:independent-reviewer",
        ),
    )
    return LifecycleSpec(
        lifecycle_id=lifecycle_id,
        version=version,
        policy_version="1.0.0",
        default_mode=OperatingMode.SUPERVISED,
        transitions=transitions,
        obligation_rules=obligations,
        capabilities=tuple(sorted(capabilities, key=lambda grant: grant.capability_id)),
    )


def _guard_reason(
    rule: TransitionRule,
    blockers: list[Blocker],
    gates: list[Gate],
    obligations: list[Obligation],
) -> str | None:
    if "no_blocking_gates" in rule.guards and any(
        gate.blocking and gate.resolved_at is None for gate in gates
    ):
        return "BLOCKING_GATE_OPEN"
    if "no_blocking_blockers" in rule.guards and any(blocker.blocking for blocker in blockers):
        return "BLOCKING_BLOCKER_OPEN"
    if "no_pending_obligations" in rule.guards and any(
        obligation.status == ObligationStatus.PENDING for obligation in obligations
    ):
        return "PENDING_OBLIGATIONS"
    return None


def compute_frontier(
    state: LifecycleState,
    spec: LifecycleSpec,
    blockers: list[Blocker],
    gates: list[Gate],
    obligations: list[Obligation],
) -> list[FrontierItem]:
    """Compute a stable, explainable frontier for the current state."""

    items: list[FrontierItem] = []
    for rule in spec.transitions:
        if rule.from_status != state.status:
            continue
        mode_allowed = state.mode in rule.allowed_modes and state.mode != OperatingMode.DISABLED
        guard_reason = _guard_reason(rule, blockers, gates, obligations)
        allowed = mode_allowed and guard_reason is None
        if not mode_allowed:
            reason = "MODE_DISALLOWS_TRANSITION"
        elif guard_reason:
            reason = guard_reason
        elif state.mode in (OperatingMode.SUPERVISED, OperatingMode.MANUAL) and rule.to_status in (
            LifecycleStatus.COMPLETED,
            LifecycleStatus.CANCELED,
            LifecycleStatus.ARCHIVED,
        ):
            reason = "LEGAL_REQUIRES_CONFIRMATION"
        else:
            reason = "LEGAL_TRANSITION"
        items.append(
            FrontierItem(
                id=f"transition:{state.status.value}:{rule.to_status.value}",
                kind=FrontierKind.STATE_TRANSITION,
                action=rule.name,
                allowed=allowed,
                capability_required=CAPABILITY_ACTION,
                reason_code=reason,
                expected_state_version=state.state_version,
            )
        )

    for gate in sorted(gates, key=lambda item: item.id):
        if gate.resolved_at is not None:
            continue
        items.append(
            FrontierItem(
                id=f"gate:{gate.id}:resolve",
                kind=FrontierKind.GATE_RESOLUTION,
                action="resolve_gate",
                allowed=state.mode != OperatingMode.DISABLED,
                capability_required=CAPABILITY_ACTION,
                reason_code=(
                    "GATE_OPEN" if state.mode != OperatingMode.DISABLED else "MODE_DISABLED"
                ),
                expected_state_version=state.state_version,
            )
        )

    for mode in OperatingMode:
        if mode == state.mode:
            continue
        items.append(
            FrontierItem(
                id=f"mode:{mode.value}",
                kind=FrontierKind.COMMAND,
                action="set_mode",
                allowed=True,
                capability_required=CAPABILITY_ACTION,
                reason_code="MODE_CHANGE_LEGAL",
                expected_state_version=state.state_version,
            )
        )
    return sorted(items, key=lambda item: (item.kind.value, item.id))


def compute_obligations(
    state: LifecycleState,
    spec: LifecycleSpec,
    observations: list[Observation],
    as_of: datetime,
) -> list[Obligation]:
    """Evaluate spec obligation rules without consulting a wall clock."""

    source_ids = tuple(
        sorted(
            {
                identity
                for observation in observations
                if (identity := observation.observation_id or observation.source_event_id)
            }
        )
    )
    existing = {obligation.id: obligation for obligation in state.obligations}
    obligations: list[Obligation] = []
    for rule in sorted(spec.obligation_rules, key=lambda item: item.id):
        if state.status not in rule.when_statuses:
            continue
        occurrence = existing.get(rule.id)
        if occurrence is None:
            occurrence = Obligation(
                id=rule.id,
                obligation_instance_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        "lifecycle-obligation-occurrence:"
                        f"{state.lifecycle_id}:{rule.id}:{state.status.value}:"
                        f"{state.state_version}",
                    )
                ),
                activated_at=as_of,
                kind=rule.kind,
                status=ObligationStatus.PENDING,
                description=rule.description,
                skill_ref=rule.skill_ref,
                owner_id=rule.owner_id,
                due_at=(
                    as_of + timedelta(seconds=rule.due_after_seconds)
                    if rule.due_after_seconds is not None
                    else None
                ),
            )
        satisfied = any(
            _is_canonical_completion_evidence(
                observation,
                lifecycle_id=state.lifecycle_id,
                rule=rule,
                occurrence=occurrence,
            )
            for observation in observations
        )
        obligations.append(
            Obligation(
                id=rule.id,
                obligation_instance_id=occurrence.obligation_instance_id,
                activated_at=occurrence.activated_at,
                kind=rule.kind,
                status=(ObligationStatus.SATISFIED if satisfied else ObligationStatus.PENDING),
                description=rule.description,
                skill_ref=rule.skill_ref,
                owner_id=rule.owner_id,
                due_at=occurrence.due_at,
                source_observation_ids=source_ids,
            )
        )
    return obligations


def _is_canonical_completion_evidence(
    observation: Observation,
    *,
    lifecycle_id: str,
    rule: ObligationRule,
    occurrence: Obligation,
) -> bool:
    """Fail closed unless an observation is exact completed-skill evidence.

    Invocation requests and arbitrary source payload flags are deliberately not
    satisfaction evidence. The transport validator enforces the full Bloodbank
    schema; this pure predicate repeats the authority-relevant identity checks
    so replayed or manually imported observations cannot manufacture truth.
    """

    payload = observation.payload
    evidence = payload.get("evidence")
    skill_ref = payload.get("skill_ref")
    if (
        observation.kind != "obligation_evidence"
        or observation.source_event_type != OBLIGATION_EVIDENCE_TYPE
        or observation.source_event_subject != OBLIGATION_EVIDENCE_SUBJECT
        or observation.source_event_source != OBLIGATION_EVIDENCE_SOURCE
        or observation.source_event_producer != OBLIGATION_EVIDENCE_PRODUCER
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "contract_version",
            "lifecycle_id",
            "repo",
            "obligation_id",
            "obligation_instance_id",
            "obligation_kind",
            "target_actor_id",
            "invocation_id",
            "skill_ref",
            "completed_at",
            "evidence",
        }
        or payload.get("contract_version") != 2
        or payload.get("lifecycle_id") != lifecycle_id
        or payload.get("obligation_id") != rule.id
        or payload.get("obligation_instance_id") != occurrence.obligation_instance_id
        or payload.get("obligation_kind") != rule.kind
        or rule.owner_id is None
        or payload.get("target_actor_id") != rule.owner_id
        or not isinstance(skill_ref, dict)
        or set(skill_ref) != {"name", "selector"}
        or skill_ref != rule.skill_ref.to_json()
        or not isinstance(evidence, dict)
        or set(evidence) != {"kind", "outcome", "artifact_id", "artifact_sha256", "summary"}
        or evidence.get("kind") != "skill_completion"
        or evidence.get("outcome") != "completed"
        or not isinstance(evidence.get("artifact_id"), str)
        or not evidence["artifact_id"]
        or not isinstance(evidence.get("artifact_sha256"), str)
        or len(evidence["artifact_sha256"]) != 64
        or any(char not in "0123456789abcdef" for char in evidence["artifact_sha256"])
        or not isinstance(evidence.get("summary"), str)
        or not evidence["summary"]
        or len(evidence["summary"]) > 500
    ):
        return False
    try:
        completed_at = _parse_timestamp(str(payload["completed_at"]))
    except (KeyError, ValueError):
        return False
    return (
        observation.observed_at is not None
        and observation.received_at is not None
        and completed_at == observation.observed_at
        and completed_at >= occurrence.activated_at
        and completed_at <= observation.received_at
    )


def transition_guard_reason(
    state: LifecycleState,
    target_status: LifecycleStatus,
    spec: LifecycleSpec,
    blockers: list[Blocker],
    gates: list[Gate],
    obligations: list[Obligation],
) -> str | None:
    """Return an explicit authority guard preventing an automatic transition."""

    rule = find_transition(spec, state.status, target_status.value)
    if rule is None:
        return None
    return _guard_reason(rule, blockers, gates, obligations)


def projected_capabilities(spec: LifecycleSpec, state_version: int) -> list[CapabilityGrant]:
    return [
        replace(grant, state_version=state_version)
        for grant in sorted(spec.capabilities, key=lambda item: item.capability_id)
    ]


def validate_capability(
    command: IntentCommand,
    spec: LifecycleSpec,
    *,
    as_of: datetime,
) -> tuple[CapabilityGrant | None, str]:
    """Validate actor, context, scope, version, action, and authority-time validity."""

    context = command.capability
    if context.action != CAPABILITY_ACTION:
        return None, "CAPABILITY_ACTION_MISMATCH"
    if context.issued_to != command.actor.actor_id:
        return None, "CAPABILITY_ACTOR_MISMATCH"
    expected_scope = f"{CAPABILITY_SCOPE_PREFIX}{command.lifecycle_id}"
    if context.scope != expected_scope:
        return None, "CAPABILITY_SCOPE_MISMATCH"

    grant = next(
        (
            candidate
            for candidate in spec.capabilities
            if candidate.capability_id == context.capability_id
        ),
        None,
    )
    if grant is None:
        return None, "CAPABILITY_NOT_FOUND"
    if grant.capability_version != context.capability_version:
        return None, "CAPABILITY_VERSION_MISMATCH"
    if grant.actor_id != command.actor.actor_id or grant.scope != expected_scope:
        return None, "CAPABILITY_GRANT_MISMATCH"
    if CAPABILITY_ACTION not in grant.actions and command.intent.name not in grant.actions:
        return None, "CAPABILITY_ACTION_DENIED"
    if as_of < grant.issued_at:
        return None, "CAPABILITY_NOT_YET_VALID"
    if grant.expires_at is not None and as_of >= grant.expires_at:
        return None, "CAPABILITY_EXPIRED"
    return grant, "CAPABILITY_VALID"


def find_transition(
    spec: LifecycleSpec,
    from_status: LifecycleStatus,
    target: str,
) -> TransitionRule | None:
    try:
        to_status = LifecycleStatus(target)
    except ValueError:
        return None
    return next(
        (
            rule
            for rule in spec.transitions
            if rule.from_status == from_status and rule.to_status == to_status
        ),
        None,
    )


def intent_is_legal(
    command: IntentCommand,
    state: LifecycleState,
    spec: LifecycleSpec,
    blockers: list[Blocker],
    gates: list[Gate],
    obligations: list[Obligation],
) -> tuple[bool, str]:
    intent = command.intent
    if intent.name == "transition":
        rule = find_transition(spec, state.status, intent.target)
        if rule is None:
            return False, "TRANSITION_NOT_DEFINED"
        if state.mode == OperatingMode.DISABLED or state.mode not in rule.allowed_modes:
            return False, "MODE_DISALLOWS_TRANSITION"
        if reason := _guard_reason(rule, blockers, gates, obligations):
            return False, reason
        if (
            state.mode in (OperatingMode.SUPERVISED, OperatingMode.MANUAL)
            and rule.to_status
            in (
                LifecycleStatus.COMPLETED,
                LifecycleStatus.CANCELED,
                LifecycleStatus.ARCHIVED,
            )
            and intent.parameters.get("confirmed") is not True
        ):
            return False, "CONFIRMATION_REQUIRED"
        return True, "LEGAL_TRANSITION"
    if intent.name == "resolve_gate":
        gate = next((item for item in gates if item.id == intent.target), None)
        if gate is None or gate.resolved_at is not None:
            return False, "GATE_NOT_OPEN"
        if state.mode == OperatingMode.DISABLED:
            return False, "MODE_DISABLED"
        if intent.parameters.get("resolution") not in {
            "approved",
            "rejected",
            "bypassed",
            "auto_resolved",
            "superseded",
        }:
            return False, "GATE_RESOLUTION_INVALID"
        return True, "GATE_RESOLUTION_LEGAL"
    if intent.name == "set_mode":
        try:
            target_mode = OperatingMode(intent.target)
        except ValueError:
            return False, "MODE_UNKNOWN"
        if target_mode == state.mode:
            return False, "MODE_UNCHANGED"
        return True, "MODE_CHANGE_LEGAL"
    return False, "INTENT_NOT_IN_FRONTIER"


def spec_to_json(spec: LifecycleSpec) -> dict[str, Any]:
    return {
        "lifecycle_id": spec.lifecycle_id,
        "version": spec.version,
        "policy_version": spec.policy_version,
        "default_mode": spec.default_mode.value,
        "transitions": [
            {
                "name": rule.name,
                "from_status": rule.from_status.value,
                "to_status": rule.to_status.value,
                "guards": list(rule.guards),
                "allowed_modes": [mode.value for mode in rule.allowed_modes],
            }
            for rule in spec.transitions
        ],
        "obligation_rules": [
            {
                "id": rule.id,
                "kind": rule.kind,
                "description": rule.description,
                "skill_ref": rule.skill_ref.to_json(),
                "when_statuses": [status.value for status in rule.when_statuses],
                "owner_id": rule.owner_id,
                "due_after_seconds": rule.due_after_seconds,
            }
            for rule in spec.obligation_rules
        ],
        "capabilities": [
            {
                **grant.to_json(),
                "capability_version": grant.capability_version,
            }
            for grant in spec.capabilities
        ],
    }


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("spec timestamps must be timezone-aware")
    return parsed.astimezone(UTC)


def spec_from_json(document: dict[str, Any]) -> LifecycleSpec:
    return LifecycleSpec(
        lifecycle_id=str(document["lifecycle_id"]),
        version=int(document["version"]),
        policy_version=str(document["policy_version"]),
        default_mode=OperatingMode(document["default_mode"]),
        transitions=tuple(
            TransitionRule(
                name=str(rule["name"]),
                from_status=LifecycleStatus(rule["from_status"]),
                to_status=LifecycleStatus(rule["to_status"]),
                guards=tuple(str(item) for item in rule.get("guards", [])),
                allowed_modes=tuple(OperatingMode(item) for item in rule["allowed_modes"]),
            )
            for rule in document["transitions"]
        ),
        obligation_rules=tuple(
            ObligationRule(
                id=str(rule["id"]),
                kind=str(rule["kind"]),
                description=str(rule["description"]),
                skill_ref=SkillRef(**rule["skill_ref"]),
                when_statuses=tuple(LifecycleStatus(status) for status in rule["when_statuses"]),
                owner_id=rule.get("owner_id"),
                due_after_seconds=rule.get("due_after_seconds"),
            )
            for rule in document.get("obligation_rules", [])
        ),
        capabilities=tuple(
            CapabilityGrant(
                capability_id=str(grant["capability_id"]),
                capability_version=int(grant["capability_version"]),
                actor_id=str(grant["actor_id"]),
                actions=tuple(str(action) for action in grant["actions"]),
                scope=str(grant["scope"]),
                issued_at=_parse_timestamp(grant["issued_at"]),
                expires_at=(
                    _parse_timestamp(grant["expires_at"]) if grant.get("expires_at") else None
                ),
                state_version=int(grant["state_version"]),
            )
            for grant in document.get("capabilities", [])
        ),
    )


def actor_from_wire(value: dict[str, Any]) -> ActorContext:
    return ActorContext(
        actor_type=str(value["type"]),
        actor_id=str(value["agent_id"]),
        provider=value.get("provider"),
        cli=value.get("cli"),
        model=value.get("model"),
    )


def capability_context_from_wire(value: dict[str, Any]) -> CapabilityContext:
    return CapabilityContext(
        capability_id=str(value["capability_id"]),
        capability_version=int(value["capability_version"]),
        action=str(value["action"]),
        scope=str(value["scope"]),
        issued_to=str(value["issued_to"]),
    )
