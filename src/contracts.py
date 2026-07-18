"""Pinned Bloodbank v1 lifecycle wire contract helpers.

Bloodbank remains the schema owner.  This module implements the narrow runtime
consumer/producer surface locked to Bloodbank commit
``9b99939ce584b3569f28609f04b1847984381b16``.  Contract drift is checked by
``scripts/verify_bloodbank_contracts.py`` and all produced envelopes are tested
with Bloodbank's canonical validator.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

from models import (
    CommandResult,
    IntentCommand,
    LifecycleIntent,
    Observation,
    SkillRef,
)
from specification import actor_from_wire, capability_context_from_wire


BLOODBANK_CONTRACT_COMMIT = "9b99939ce584b3569f28609f04b1847984381b16"
COMMAND_TYPE = "bloodbank.v1.lifecycle.intent.submit"
COMMAND_SUBJECT = "bloodbank.cmd.v1.lifecycle.intent.submit"
REPLY_SUBJECT = "bloodbank.rpy.v1.lifecycle.intent.submit"
REPO_TASK_RECORDED_TYPE = "bloodbank.v1.repo.task.recorded"
REPO_TASK_RECORDED_SUBJECT = "bloodbank.evt.v1.repo.task.recorded"
OBLIGATION_EVIDENCE_TYPE = "bloodbank.v1.lifecycle.obligation_evidence.submitted"
OBLIGATION_EVIDENCE_SUBJECT = "bloodbank.evt.v1.lifecycle.obligation_evidence.submitted"
MOMO_SOURCE = "urn:33god:service:momo"
MOMO_PRODUCER = "momo"
AUTHORITY_SOURCE = "urn:33god:service:lifecycle"
AUTHORITY_PRODUCER = "delorenj/lifecycle"
AUTHORITY_SERVICE = "lifecycle"
AUTHORITY_ACTOR = {"type": "service", "agent_id": "delorenj.lifecycle"}
UTC = timezone.utc
_TYPE_RE = re.compile(r"^bloodbank\.v[0-9]+\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")


class ContractError(ValueError):
    """A stable fail-closed wire-contract error."""

    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(detail)
        self.reason_code = reason_code
        self.detail = detail


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ContractError("JSON_INVALID", "value is not canonical JSON") from exc


def payload_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def stable_uuid(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def _uuid(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ContractError("FIELD_TYPE_INVALID", f"{field} must be a UUID string")
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError) as exc:
        raise ContractError("FIELD_FORMAT_INVALID", f"{field} must be RFC 4122 UUID") from exc


def parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ContractError("FIELD_FORMAT_INVALID", f"{field} must be RFC 3339 date-time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError("FIELD_FORMAT_INVALID", f"{field} must be RFC 3339 date-time") from exc
    if parsed.tzinfo is None:
        raise ContractError("FIELD_FORMAT_INVALID", f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("wire timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _required_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError("FIELD_TYPE_INVALID", f"{field} must be an object")
    return value


def _nonblank(value: Any, field: str, *, whitespace_forbidden: bool = False) -> str:
    if not isinstance(value, str) or not value:
        raise ContractError("FIELD_REQUIRED", f"{field} must be non-empty")
    if whitespace_forbidden and any(char.isspace() for char in value):
        raise ContractError("FIELD_FORMAT_INVALID", f"{field} cannot contain whitespace")
    return value


def _exact_keys(value: Mapping[str, Any], allowed: set[str], field: str) -> None:
    extras = set(value) - allowed
    if extras:
        raise ContractError(
            "FIELD_ADDITIONAL_INVALID",
            f"{field} contains unsupported fields: {', '.join(sorted(extras))}",
        )


def subject_for(event_type: str, kind: str) -> str:
    if not _TYPE_RE.fullmatch(event_type):
        raise ContractError("TYPE_INVALID", "CloudEvent type is not a Bloodbank type")
    marker = {"event": "evt", "command": "cmd", "reply": "rpy"}.get(kind)
    if marker is None:
        raise ContractError("KIND_INVALID", "kind must be event, command, or reply")
    return f"bloodbank.{marker}.{event_type.removeprefix('bloodbank.')}"


def _validate_base(
    envelope: Any,
    *,
    event_type: str,
    kind: str,
    subject: str,
    domain: str,
    dataschema: str,
    schemaref: str,
) -> dict[str, Any]:
    value = _required_object(envelope, "envelope")
    required = {
        "specversion",
        "id",
        "source",
        "type",
        "subject",
        "time",
        "correlationid",
        "causationid",
        "producer",
        "service",
        "domain",
        "kind",
        "actor",
        "data",
    }
    missing = required - set(value)
    if missing:
        raise ContractError(
            "ENVELOPE_REQUIRED_FIELD_MISSING",
            f"missing envelope fields: {', '.join(sorted(missing))}",
        )
    constants = {
        "specversion": "1.0",
        "type": event_type,
        "subject": subject,
        "kind": kind,
        "domain": domain,
        "datacontenttype": "application/json",
        "dataschema": dataschema,
        "schemaref": schemaref,
    }
    for field, expected in constants.items():
        if value.get(field) != expected:
            raise ContractError("ENVELOPE_BINDING_MISMATCH", f"{field} must equal {expected!r}")
    _uuid(value["id"], "id")
    _uuid(value["correlationid"], "correlationid")
    if value["causationid"] is not None:
        _uuid(value["causationid"], "causationid")
    parse_timestamp(value["time"], "time")
    _nonblank(value["source"], "source", whitespace_forbidden=True)
    for field in ("producer", "service"):
        _nonblank(value[field], field)
    actor = _required_object(value["actor"], "actor")
    _nonblank(actor.get("type"), "actor.type")
    _nonblank(actor.get("agent_id"), "actor.agent_id")
    for field in ("cli", "provider", "model"):
        if field in actor and actor[field] is not None and not isinstance(actor[field], str):
            raise ContractError(
                "FIELD_TYPE_INVALID",
                f"actor.{field} must be a string or null",
            )
    _required_object(value["data"], "data")
    if kind == "event":
        _nonblank(value.get("ordering_key"), "ordering_key")
    return value


def validate_intent_command(envelope: Any) -> IntentCommand:
    value = _validate_base(
        envelope,
        event_type=COMMAND_TYPE,
        kind="command",
        subject=COMMAND_SUBJECT,
        domain="lifecycle",
        dataschema=(
            "apicurio://holyfields/bloodbank.v1.lifecycle.intent.submit.command/versions/1"
        ),
        schemaref="bloodbank.v1.lifecycle.intent.submit.command.v1",
    )
    command_id = _uuid(value.get("command_id"), "command_id")
    idempotency_key = _nonblank(value.get("idempotency_key"), "idempotency_key")
    if value.get("delivery") != "single_consumer":
        raise ContractError("DELIVERY_INVALID", "delivery must be single_consumer")

    data = value["data"]
    allowed = {
        "contract_version",
        "lifecycle_id",
        "repo",
        "expected_state_version",
        "intent",
        "capability",
        "requested_at",
    }
    _exact_keys(data, allowed, "data")
    if data.get("contract_version") != 1:
        raise ContractError("CONTRACT_VERSION_UNSUPPORTED", "contract_version must be 1")
    lifecycle_id = _nonblank(data.get("lifecycle_id"), "data.lifecycle_id")
    repo = _nonblank(data.get("repo"), "data.repo")
    expected_version = data.get("expected_state_version")
    if (
        isinstance(expected_version, bool)
        or not isinstance(expected_version, int)
        or expected_version < 1
    ):
        raise ContractError(
            "EXPECTED_STATE_VERSION_INVALID",
            "data.expected_state_version must be an integer >= 1",
        )

    intent = _required_object(data.get("intent"), "data.intent")
    _exact_keys(intent, {"name", "target", "parameters"}, "data.intent")
    name = _nonblank(intent.get("name"), "data.intent.name")
    target = _nonblank(intent.get("target"), "data.intent.target")
    parameters = _required_object(intent.get("parameters"), "data.intent.parameters")

    capability = _required_object(data.get("capability"), "data.capability")
    _exact_keys(
        capability,
        {"capability_id", "capability_version", "action", "scope", "issued_to"},
        "data.capability",
    )
    for field in ("capability_id", "action", "scope", "issued_to"):
        _nonblank(capability.get(field), f"data.capability.{field}")
    capability_version = capability.get("capability_version")
    if (
        isinstance(capability_version, bool)
        or not isinstance(capability_version, int)
        or capability_version < 1
    ):
        raise ContractError(
            "CAPABILITY_VERSION_INVALID", "capability_version must be an integer >= 1"
        )
    requested_at = parse_timestamp(data.get("requested_at"), "data.requested_at")
    return IntentCommand(
        event_id=_uuid(value["id"], "id"),
        command_id=command_id,
        idempotency_key=idempotency_key,
        lifecycle_id=lifecycle_id,
        repo=repo,
        expected_state_version=expected_version,
        intent=LifecycleIntent(name=name, target=target, parameters=dict(parameters)),
        capability=capability_context_from_wire(capability),
        actor=actor_from_wire(value["actor"]),
        requested_at=requested_at,
        correlation_id=_uuid(value["correlationid"], "correlationid"),
        causation_id=(
            _uuid(value["causationid"], "causationid") if value["causationid"] is not None else None
        ),
        source=value["source"],
        producer=value["producer"],
        service=value["service"],
        raw_envelope=dict(value),
    )


def recover_intent_command(envelope: Any) -> IntentCommand:
    """Recover reply/idempotency identity from an otherwise malformed command.

    Bloodbank's reply schema still requires valid UUID, lifecycle, repo, and
    expected-version fields. Commands that retain those routing fields can
    therefore receive a canonical ``malformed`` reply even when their intent or
    capability body fails strict validation. Structurally unaddressable bytes
    are poison messages and cannot be represented by the canonical reply schema.
    """

    value = _required_object(envelope, "envelope")
    data = _required_object(value.get("data"), "data")
    event_id = _uuid(value.get("id"), "id")
    correlation_id = _uuid(value.get("correlationid"), "correlationid")
    command_id = _uuid(value.get("command_id"), "command_id")
    idempotency_key = _nonblank(value.get("idempotency_key"), "idempotency_key")
    lifecycle_id = _nonblank(data.get("lifecycle_id"), "data.lifecycle_id")
    repo = _nonblank(data.get("repo"), "data.repo")
    expected_version = data.get("expected_state_version")
    if (
        isinstance(expected_version, bool)
        or not isinstance(expected_version, int)
        or expected_version < 1
    ):
        raise ContractError(
            "EXPECTED_STATE_VERSION_INVALID",
            "data.expected_state_version must be an integer >= 1",
        )
    raw_intent = data.get("intent")
    intent: dict[str, Any] = raw_intent if isinstance(raw_intent, dict) else {}
    raw_capability = data.get("capability")
    capability: dict[str, Any] = raw_capability if isinstance(raw_capability, dict) else {}
    requested_at: datetime | None = None
    for candidate, field in (
        (data.get("requested_at"), "data.requested_at"),
        (value.get("time"), "time"),
    ):
        try:
            requested_at = parse_timestamp(candidate, field)
            break
        except ContractError:
            continue
    if requested_at is None:
        raise ContractError(
            "FIELD_FORMAT_INVALID",
            "data.requested_at or time must be an RFC 3339 date-time",
        )
    raw_actor = value.get("actor")
    actor_value: dict[str, Any] = raw_actor if isinstance(raw_actor, dict) else {}
    actor = {
        "type": (
            actor_value["type"]
            if isinstance(actor_value.get("type"), str) and actor_value["type"]
            else "malformed"
        ),
        "agent_id": (
            actor_value["agent_id"]
            if isinstance(actor_value.get("agent_id"), str) and actor_value["agent_id"]
            else "malformed"
        ),
        "provider": (
            actor_value.get("provider")
            if isinstance(actor_value.get("provider"), str) or actor_value.get("provider") is None
            else None
        ),
        "cli": (
            actor_value.get("cli")
            if isinstance(actor_value.get("cli"), str) or actor_value.get("cli") is None
            else None
        ),
        "model": (
            actor_value.get("model")
            if isinstance(actor_value.get("model"), str) or actor_value.get("model") is None
            else None
        ),
    }
    causation_id: str | None = None
    if value.get("causationid") is not None:
        try:
            causation_id = _uuid(value["causationid"], "causationid")
        except ContractError:
            pass
    return IntentCommand(
        event_id=event_id,
        command_id=command_id,
        idempotency_key=idempotency_key,
        lifecycle_id=lifecycle_id,
        repo=repo,
        expected_state_version=expected_version,
        intent=LifecycleIntent(
            name=str(intent.get("name") or "malformed"),
            target=str(intent.get("target") or "malformed"),
            parameters=(
                dict(intent["parameters"]) if isinstance(intent.get("parameters"), dict) else {}
            ),
        ),
        capability=capability_context_from_wire(
            {
                "capability_id": str(capability.get("capability_id") or "malformed"),
                "capability_version": (
                    capability["capability_version"]
                    if isinstance(capability.get("capability_version"), int)
                    and not isinstance(capability.get("capability_version"), bool)
                    and capability["capability_version"] >= 1
                    else 0
                ),
                "action": str(capability.get("action") or "malformed"),
                "scope": str(capability.get("scope") or "malformed"),
                "issued_to": str(capability.get("issued_to") or "malformed"),
            }
        ),
        actor=actor_from_wire(actor),
        requested_at=requested_at,
        correlation_id=correlation_id,
        causation_id=causation_id,
        source=str(value.get("source") or "malformed"),
        producer=str(value.get("producer") or "malformed"),
        service=str(value.get("service") or "malformed"),
        raw_envelope=dict(value),
    )


def validate_repo_task_recorded(envelope: Any, lifecycle_id: str) -> Observation:
    value = _validate_base(
        envelope,
        event_type=REPO_TASK_RECORDED_TYPE,
        kind="event",
        subject=REPO_TASK_RECORDED_SUBJECT,
        domain="repo",
        dataschema=("apicurio://holyfields/bloodbank.v1.repo.task.recorded/versions/1"),
        schemaref="bloodbank.v1.repo.task.recorded.v1",
    )
    data = value["data"]
    _nonblank(data.get("repo"), "data.repo", whitespace_forbidden=True)
    _nonblank(data.get("task_id"), "data.task_id", whitespace_forbidden=True)
    _nonblank(data.get("title"), "data.title")
    for field in ("change_kind", "updated_by", "note"):
        if field in data and not isinstance(data[field], str):
            raise ContractError(
                "FIELD_TYPE_INVALID",
                f"data.{field} must be a string",
            )
    if "updated_at" in data:
        parse_timestamp(data["updated_at"], "data.updated_at")
    source_event_id = _uuid(value["id"], "id")
    observed_at = parse_timestamp(value["time"], "time")
    observation_id = stable_uuid(f"lifecycle-observation:{lifecycle_id}:{source_event_id}")
    return Observation(
        lifecycle_id=lifecycle_id,
        source=value["producer"],
        kind="repo_task_event",
        observed_at=observed_at,
        payload=dict(data),
        payload_hash=payload_sha256(data),
        observation_id=observation_id,
        source_event_id=source_event_id,
        source_event_type=value["type"],
        source_event_subject=value["subject"],
        source_event_source=value["source"],
        source_event_producer=value["producer"],
        ordering_key=value["ordering_key"],
    )


def validate_obligation_evidence_submitted(envelope: Any) -> Observation:
    """Validate Momo's exact completion evidence without accepting a verdict.

    The event is an authority input. It can become a satisfaction observation
    only after this strict identity, completion, and artifact-integrity check;
    Lifecycle still performs the obligation correlation and state transition.
    """

    value = _validate_base(
        envelope,
        event_type=OBLIGATION_EVIDENCE_TYPE,
        kind="event",
        subject=OBLIGATION_EVIDENCE_SUBJECT,
        domain="lifecycle",
        dataschema=(
            "apicurio://holyfields/bloodbank.v1.lifecycle.obligation_evidence.submitted/versions/2"
        ),
        schemaref="bloodbank.v1.lifecycle.obligation_evidence.submitted.v2",
    )
    if value["source"] != MOMO_SOURCE:
        raise ContractError("SOURCE_INVALID", f"source must equal {MOMO_SOURCE!r}")
    if value["producer"] != MOMO_PRODUCER or value["service"] != MOMO_PRODUCER:
        raise ContractError("PRODUCER_INVALID", "producer and service must equal 'momo'")
    actor = value["actor"]
    if actor.get("type") != "service" or actor.get("agent_id") != "momo":
        raise ContractError("ACTOR_INVALID", "completion evidence actor must be service momo")

    data = value["data"]
    _exact_keys(
        data,
        {
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
        },
        "data",
    )
    if data.get("contract_version") != 2:
        raise ContractError("CONTRACT_VERSION_UNSUPPORTED", "contract_version must be 2")
    lifecycle_id = _nonblank(data.get("lifecycle_id"), "data.lifecycle_id")
    _nonblank(data.get("repo"), "data.repo", whitespace_forbidden=True)
    _nonblank(data.get("obligation_id"), "data.obligation_id")
    _uuid(data.get("obligation_instance_id"), "data.obligation_instance_id")
    _nonblank(data.get("obligation_kind"), "data.obligation_kind")
    _nonblank(data.get("target_actor_id"), "data.target_actor_id")
    _uuid(data.get("invocation_id"), "data.invocation_id")

    skill_ref = _required_object(data.get("skill_ref"), "data.skill_ref")
    _exact_keys(skill_ref, {"name", "selector"}, "data.skill_ref")
    try:
        SkillRef(
            name=_nonblank(skill_ref.get("name"), "data.skill_ref.name"),
            selector=_nonblank(skill_ref.get("selector"), "data.skill_ref.selector"),
        )
    except ValueError as exc:
        raise ContractError("SKILL_REF_INVALID", str(exc)) from exc

    completed_at = parse_timestamp(data.get("completed_at"), "data.completed_at")
    if parse_timestamp(value["time"], "time") != completed_at:
        raise ContractError(
            "COMPLETION_TIME_MISMATCH",
            "CloudEvent time must equal data.completed_at",
        )
    evidence = _required_object(data.get("evidence"), "data.evidence")
    _exact_keys(
        evidence,
        {"kind", "outcome", "artifact_id", "artifact_sha256", "summary"},
        "data.evidence",
    )
    if evidence.get("kind") != "skill_completion":
        raise ContractError("EVIDENCE_KIND_INVALID", "evidence.kind must be skill_completion")
    if evidence.get("outcome") != "completed":
        raise ContractError("EVIDENCE_OUTCOME_INVALID", "evidence.outcome must be completed")
    _nonblank(evidence.get("artifact_id"), "data.evidence.artifact_id")
    artifact_sha256 = evidence.get("artifact_sha256")
    if not isinstance(artifact_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", artifact_sha256):
        raise ContractError(
            "EVIDENCE_INTEGRITY_INVALID",
            "data.evidence.artifact_sha256 must be lowercase SHA-256",
        )
    summary = _nonblank(evidence.get("summary"), "data.evidence.summary")
    if len(summary) > 500:
        raise ContractError("FIELD_LENGTH_INVALID", "data.evidence.summary exceeds 500 chars")

    source_event_id = _uuid(value["id"], "id")
    return Observation(
        lifecycle_id=lifecycle_id,
        source=value["producer"],
        kind="obligation_evidence",
        observed_at=completed_at,
        payload=dict(data),
        payload_hash=payload_sha256(data),
        observation_id=stable_uuid(f"lifecycle-observation:{lifecycle_id}:{source_event_id}"),
        source_event_id=source_event_id,
        source_event_type=value["type"],
        source_event_subject=value["subject"],
        source_event_source=value["source"],
        source_event_producer=value["producer"],
        ordering_key=value["ordering_key"],
    )


def build_event_envelope(
    *,
    event_type: str,
    data: dict[str, Any],
    event_id: str,
    occurred_at: datetime,
    correlation_id: str,
    causation_id: str | None,
    authority_instance: str,
    schema_version: int = 1,
) -> dict[str, Any]:
    subject = subject_for(event_type, "event")
    _uuid(event_id, "event_id")
    _uuid(correlation_id, "correlation_id")
    if causation_id is not None:
        _uuid(causation_id, "causation_id")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version < 1
    ):
        raise ValueError("schema_version must be an integer >= 1")
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": AUTHORITY_SOURCE,
        "type": event_type,
        "subject": subject,
        "time": format_timestamp(occurred_at),
        "datacontenttype": "application/json",
        "dataschema": f"apicurio://holyfields/{event_type}/versions/{schema_version}",
        "correlationid": correlation_id,
        "causationid": causation_id,
        "producer": AUTHORITY_PRODUCER,
        "service": AUTHORITY_SERVICE,
        "domain": "lifecycle",
        "schemaref": f"{event_type}.v{schema_version}",
        "kind": "event",
        "actor": {**AUTHORITY_ACTOR, "instance": authority_instance},
        "ordering_key": f"lifecycle:{data['lifecycle_id']}",
        "data": data,
    }


def build_reply_envelope(
    *,
    command: IntentCommand,
    result: CommandResult,
    responded_at: datetime,
    authority_instance: str,
) -> dict[str, Any]:
    event_id = stable_uuid(
        f"lifecycle-reply:{command.event_id}:{result.verdict.value}:"
        f"{result.resulting_state_version or 0}"
    )
    data = {
        "contract_version": 1,
        "lifecycle_id": command.lifecycle_id,
        "repo": command.repo,
        "reply_to_command_event_id": command.event_id,
        "command_id": command.command_id,
        "idempotency_key": command.idempotency_key,
        "expected_state_version": command.expected_state_version,
        "observed_state_version": result.observed_state_version,
        "verdict": result.verdict.value,
        "mutated": result.mutated,
        "resulting_state_version": result.resulting_state_version,
        "applied_event_id": result.applied_event_id,
        "capability_id": result.capability_id,
        "reason_code": result.reason_code,
        "responded_at": format_timestamp(responded_at),
    }
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": AUTHORITY_SOURCE,
        "type": COMMAND_TYPE,
        "subject": REPLY_SUBJECT,
        "time": format_timestamp(responded_at),
        "datacontenttype": "application/json",
        "dataschema": (
            "apicurio://holyfields/bloodbank.v1.lifecycle.intent.submit.reply/versions/1"
        ),
        "correlationid": command.correlation_id,
        "causationid": command.event_id,
        "producer": AUTHORITY_PRODUCER,
        "service": AUTHORITY_SERVICE,
        "domain": "lifecycle",
        "schemaref": "bloodbank.v1.lifecycle.intent.submit.reply.v1",
        "kind": "reply",
        "actor": {**AUTHORITY_ACTOR, "instance": authority_instance},
        "data": data,
    }


def envelope_bytes(envelope: Mapping[str, Any]) -> bytes:
    return canonical_json(envelope).encode("utf-8")


__all__ = [
    "AUTHORITY_ACTOR",
    "AUTHORITY_PRODUCER",
    "AUTHORITY_SERVICE",
    "AUTHORITY_SOURCE",
    "BLOODBANK_CONTRACT_COMMIT",
    "COMMAND_SUBJECT",
    "COMMAND_TYPE",
    "ContractError",
    "MOMO_PRODUCER",
    "MOMO_SOURCE",
    "OBLIGATION_EVIDENCE_SUBJECT",
    "OBLIGATION_EVIDENCE_TYPE",
    "REPLY_SUBJECT",
    "REPO_TASK_RECORDED_SUBJECT",
    "REPO_TASK_RECORDED_TYPE",
    "build_event_envelope",
    "build_reply_envelope",
    "canonical_json",
    "envelope_bytes",
    "format_timestamp",
    "parse_timestamp",
    "payload_sha256",
    "stable_uuid",
    "subject_for",
    "validate_intent_command",
    "validate_obligation_evidence_submitted",
    "validate_repo_task_recorded",
]
