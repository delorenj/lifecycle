from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from jsonschema import ValidationError
import pytest

from authority import _freshness
from contracts import (
    ContractError,
    build_reply_envelope,
    recover_intent_command,
    validate_intent_command,
    validate_repo_task_recorded,
)
from models import (
    Blocker,
    BlockerKind,
    CapabilityGrant,
    CommandResult,
    CommandVerdict,
    LifecycleHealth,
    LifecycleState,
    LifecycleStatus,
    OperatingMode,
    Gate,
    GateKind,
    SkillRef,
)
from reconciler import reconcile
from specification import (
    CAPABILITY_ACTION,
    compute_frontier,
    default_spec,
    intent_is_legal,
    validate_capability,
)
from tests.factories import command_envelope, repo_task_envelope
from tests.schema_validation import validate_with_bloodbank


NOW = datetime(2026, 7, 18, 16, 0, tzinfo=timezone.utc)


def _grant(*, expires_at: datetime | None = None) -> CapabilityGrant:
    return CapabilityGrant(
        capability_id="cap-test",
        capability_version=1,
        actor_id="agent:test",
        actions=(CAPABILITY_ACTION,),
        scope="lifecycle:lc_test",
        issued_at=NOW - timedelta(minutes=1),
        expires_at=expires_at,
        state_version=1,
    )


def test_spec_and_state_versions_are_explicit_and_reconcile_is_stable_across_time() -> None:
    spec = default_spec("lc_test", version=7, capabilities=(_grant(),))
    first = reconcile(
        lifecycle_id="lc_test",
        previous_state=None,
        observations=[],
        active_blockers=[],
        active_gates=[],
        checkpoints=[],
        sentinel_health={},
        spec=spec,
        as_of=NOW,
    )
    replay = reconcile(
        lifecycle_id="lc_test",
        previous_state=first.current_state,
        observations=[],
        active_blockers=[],
        active_gates=[],
        checkpoints=[],
        sentinel_health={},
        spec=spec,
        as_of=NOW + timedelta(hours=1),
    )

    assert first.current_state.spec_version == 7
    assert first.current_state.state_version == 1
    assert replay.state_changed is False
    assert replay.current_state.state_version == 1
    assert replay.current_state.state_fingerprint == first.current_state.state_fingerprint


def test_modes_and_legal_frontier_are_explicit_and_version_scoped() -> None:
    spec = default_spec("lc_test")
    state = LifecycleState(
        lifecycle_id="lc_test",
        status=LifecycleStatus.ACTIVE,
        health=LifecycleHealth.NOMINAL,
        mode=OperatingMode.DISABLED,
        state_version=12,
    )
    frontier = compute_frontier(state, spec, [], [])

    transition_items = [item for item in frontier if item.kind.value == "state_transition"]
    assert transition_items
    assert all(not item.allowed for item in transition_items)
    assert all(item.expected_state_version == 12 for item in frontier)
    assert any(item.action == "set_mode" and item.allowed for item in frontier)


@pytest.mark.parametrize(
    ("name", "selector"),
    [
        ("Bad_Skill", "1.0.0"),
        ("valid-skill", ""),
        ("valid-skill", "main branch"),
    ],
)
def test_skill_references_are_strict_name_selector_objects(name: str, selector: str) -> None:
    with pytest.raises(ValueError):
        SkillRef(name=name, selector=selector)
    with pytest.raises(TypeError):
        SkillRef(name="valid-skill", selector="1.0.0", version="extra")  # type: ignore[call-arg]


def test_default_obligation_is_skill_addressable() -> None:
    spec = default_spec("lc_test")
    obligation = spec.obligation_rules[0]
    assert obligation.skill_ref.to_json() == {
        "name": "bmad-code-review",
        "selector": "6.10.2",
    }


def test_repo_task_observation_preserves_identity_and_ignores_provider_columns() -> None:
    envelope = repo_task_envelope(suffix="17", observed_at=NOW)
    observation = validate_repo_task_recorded(envelope, "lc_test")
    result = reconcile(
        lifecycle_id="lc_test",
        previous_state=None,
        observations=[observation],
        active_blockers=[],
        active_gates=[],
        checkpoints=[],
        sentinel_health={},
        as_of=NOW,
    )

    assert observation.source_event_id == envelope["id"]
    assert observation.source_event_subject == envelope["subject"]
    assert observation.observed_at == NOW
    assert result.current_state.status == LifecycleStatus.ACTIVE
    assert result.current_state.source_observation_ids == [observation.observation_id]


def test_actor_capability_validation_is_fail_closed_and_uses_requested_time() -> None:
    command = validate_intent_command(command_envelope(suffix="cap", requested_at=NOW))
    valid_spec = default_spec("lc_test", capabilities=(_grant(),))
    expired_spec = default_spec(
        "lc_test",
        capabilities=(_grant(expires_at=NOW),),
    )

    grant, reason = validate_capability(command, valid_spec)
    expired, expired_reason = validate_capability(command, expired_spec)

    assert grant is not None
    assert reason == "CAPABILITY_VALID"
    assert expired is None
    assert expired_reason == "CAPABILITY_EXPIRED"


def test_transition_guards_fail_closed_on_blockers_and_gates() -> None:
    command = validate_intent_command(
        command_envelope(
            suffix="guard",
            requested_at=NOW,
            target="completed",
            parameters={"confirmed": True},
        )
    )
    state = LifecycleState(
        lifecycle_id="lc_test",
        status=LifecycleStatus.ACTIVE,
        health=LifecycleHealth.NOMINAL,
    )
    spec = default_spec("lc_test")
    gate = Gate(id="gate-review", kind=GateKind.HUMAN_REVIEW)
    blocker = Blocker(id="blocker-ci", kind=BlockerKind.CI_FAILING)

    gate_legal, gate_reason = intent_is_legal(command, state, spec, [], [gate])
    blocker_legal, blocker_reason = intent_is_legal(command, state, spec, [blocker], [])

    assert gate_legal is False
    assert gate_reason == "BLOCKING_GATE_OPEN"
    assert blocker_legal is False
    assert blocker_reason == "BLOCKING_BLOCKER_OPEN"


def test_command_contract_and_kind_correct_reply_verdicts() -> None:
    command_wire = command_envelope(suffix="reply", requested_at=NOW)
    command = validate_intent_command(command_wire)
    result = CommandResult(
        verdict=CommandVerdict.STALE,
        mutated=False,
        observed_state_version=9,
        resulting_state_version=None,
        applied_event_id=None,
        capability_id=None,
        reason_code="EXPECTED_STATE_VERSION_MISMATCH",
    )
    reply = build_reply_envelope(
        command=command,
        result=result,
        responded_at=NOW,
        authority_instance="test-1",
    )

    assert reply["kind"] == "reply"
    assert reply["subject"] == "bloodbank.rpy.v1.lifecycle.intent.submit"
    assert reply["data"]["verdict"] == "stale"
    assert reply["data"]["mutated"] is False
    assert reply["data"]["resulting_state_version"] is None
    validate_with_bloodbank(command_wire)
    validate_with_bloodbank(reply)


def test_malformed_command_with_routing_identity_can_be_replied_to() -> None:
    envelope = command_envelope(suffix="malformed", requested_at=NOW)
    envelope["data"]["intent"]["parameters"] = "not-an-object"

    with pytest.raises(ContractError):
        validate_intent_command(envelope)
    recovered = recover_intent_command(envelope)

    assert recovered.command_id == envelope["command_id"]
    assert recovered.lifecycle_id == "lc_test"
    assert recovered.expected_state_version == 1


def test_spec_version_change_is_authoritative_state_change() -> None:
    first_spec = default_spec("lc_test", version=1)
    first = reconcile("lc_test", None, [], [], [], [], {}, spec=first_spec, as_of=NOW)
    second = reconcile(
        "lc_test",
        first.current_state,
        [],
        [],
        [],
        [],
        {},
        spec=replace(first_spec, version=2),
        as_of=NOW,
    )
    assert second.state_changed is True
    assert second.current_state.spec_version == 2
    assert second.current_state.state_version == 2


@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_health", "expected_reason"),
    [
        (
            OperatingMode.AUTONOMOUS,
            LifecycleStatus.ACTIVE,
            LifecycleHealth.DEGRADED,
            "OBSERVATIONS_MISSING",
        ),
        (
            OperatingMode.SUPERVISED,
            LifecycleStatus.ACTIVE,
            LifecycleHealth.DEGRADED,
            "OBSERVATIONS_MISSING",
        ),
        (
            OperatingMode.MANUAL,
            LifecycleStatus.WAITING,
            LifecycleHealth.DEGRADED,
            "MANUAL_MODE_HOLD",
        ),
        (
            OperatingMode.DISABLED,
            LifecycleStatus.WAITING,
            LifecycleHealth.NOMINAL,
            "MODE_DISABLED",
        ),
    ],
)
def test_operating_modes_are_explicit_and_missing_observations_are_never_healthy(
    mode: OperatingMode,
    expected_status: LifecycleStatus,
    expected_health: LifecycleHealth,
    expected_reason: str,
) -> None:
    previous = LifecycleState(
        lifecycle_id="lc_test",
        status=LifecycleStatus.WAITING,
        health=LifecycleHealth.NOMINAL,
        mode=mode,
        state_version=4,
        last_reconciled_at=NOW,
    )
    result = reconcile(
        "lc_test",
        previous,
        [],
        [],
        [],
        [],
        {},
        spec=default_spec("lc_test"),
        as_of=NOW + timedelta(minutes=1),
    )

    assert result.current_state.status == expected_status
    assert result.current_state.health == expected_health
    assert result.current_state.status_reason == expected_reason
    assert result.current_state.health_reason == expected_reason


def test_freshness_requires_real_source_observations() -> None:
    state = LifecycleState(
        lifecycle_id="lc_test",
        status=LifecycleStatus.ACTIVE,
        health=LifecycleHealth.DEGRADED,
        observed_through=NOW,
    )

    assert _freshness(state, NOW)["status"] == "stale"
    state.source_observation_ids = ["obs-1"]
    assert _freshness(state, NOW)["status"] == "fresh"


def test_known_optional_actor_fields_match_canonical_schema_types() -> None:
    envelope = command_envelope(suffix="actor-type", requested_at=NOW)
    envelope["actor"]["provider"] = 33

    with pytest.raises(ValidationError):
        validate_with_bloodbank(envelope)
    with pytest.raises(ContractError, match="actor.provider"):
        validate_intent_command(envelope)
    recovered = recover_intent_command(envelope)
    assert recovered.actor.provider is None
    assert recovered.lifecycle_id == "lc_test"


@pytest.mark.parametrize("field", ["change_kind", "updated_by", "note"])
def test_known_optional_repo_fields_match_canonical_schema_types(field: str) -> None:
    envelope = repo_task_envelope(suffix=f"repo-{field}", observed_at=NOW)
    envelope["data"][field] = 33

    with pytest.raises(ValidationError):
        validate_with_bloodbank(envelope)
    with pytest.raises(ContractError, match=f"data.{field}"):
        validate_repo_task_recorded(envelope, "lc_test")
