from __future__ import annotations

from datetime import datetime
import uuid

from contracts import format_timestamp


def test_uuid(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"lifecycle-test:{name}"))


def command_envelope(
    *,
    suffix: str,
    lifecycle_id: str = "lc_test",
    repo: str = "delorenj/test",
    expected_state_version: int = 1,
    actor_id: str = "agent:test",
    capability_id: str = "cap-test",
    intent_name: str = "transition",
    target: str = "waiting",
    parameters: dict | None = None,
    requested_at: datetime,
) -> dict:
    event_id = test_uuid(f"command-event:{suffix}")
    correlation_id = test_uuid(f"correlation:{suffix}")
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": "urn:33god:agent:test",
        "type": "bloodbank.v1.lifecycle.intent.submit",
        "subject": "bloodbank.cmd.v1.lifecycle.intent.submit",
        "time": format_timestamp(requested_at),
        "datacontenttype": "application/json",
        "dataschema": (
            "apicurio://holyfields/bloodbank.v1.lifecycle.intent.submit.command/versions/1"
        ),
        "correlationid": correlation_id,
        "causationid": None,
        "producer": "test-client",
        "service": "test-client",
        "domain": "lifecycle",
        "schemaref": "bloodbank.v1.lifecycle.intent.submit.command.v1",
        "kind": "command",
        "actor": {"type": "agent_api", "agent_id": actor_id},
        "command_id": test_uuid(f"command-id:{suffix}"),
        "idempotency_key": f"lifecycle-test:{suffix}",
        "delivery": "single_consumer",
        "data": {
            "contract_version": 1,
            "lifecycle_id": lifecycle_id,
            "repo": repo,
            "expected_state_version": expected_state_version,
            "intent": {
                "name": intent_name,
                "target": target,
                "parameters": parameters or {},
            },
            "capability": {
                "capability_id": capability_id,
                "capability_version": 1,
                "action": "lifecycle.intent.submit",
                "scope": f"lifecycle:{lifecycle_id}",
                "issued_to": actor_id,
            },
            "requested_at": format_timestamp(requested_at),
        },
    }


def repo_task_envelope(
    *,
    suffix: str,
    observed_at: datetime,
    repo: str = "delorenj/test",
) -> dict:
    return {
        "specversion": "1.0",
        "id": test_uuid(f"repo-task:{suffix}"),
        "source": "urn:33god:integration:test",
        "type": "bloodbank.v1.repo.task.recorded",
        "subject": "bloodbank.evt.v1.repo.task.recorded",
        "time": format_timestamp(observed_at),
        "datacontenttype": "application/json",
        "dataschema": ("apicurio://holyfields/bloodbank.v1.repo.task.recorded/versions/1"),
        "correlationid": test_uuid(f"repo-task-correlation:{suffix}"),
        "causationid": None,
        "producer": "test-repo-adapter",
        "service": "test-repo-adapter",
        "domain": "repo",
        "schemaref": "bloodbank.v1.repo.task.recorded.v1",
        "kind": "event",
        "actor": {"type": "service", "agent_id": "test.repo.adapter"},
        "ordering_key": f"task:{repo}:TASK-{suffix}",
        "data": {
            "repo": repo,
            "task_id": f"TASK-{suffix}",
            "title": f"Task {suffix}",
            "change_kind": "status",
            "from": "provider-backlog",
            "to": "provider-done",
            "updated_at": format_timestamp(observed_at),
        },
    }


def obligation_evidence_envelope(
    *,
    suffix: str,
    completed_at: datetime,
    lifecycle_id: str = "lc_test",
    repo: str = "delorenj/test",
    obligation_id: str = "independent-review",
    obligation_kind: str = "independent_review",
    target_actor_id: str = "agent:independent-reviewer",
    obligation_instance_id: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> dict:
    invocation_id = test_uuid(f"obligation-invocation:{suffix}")
    occurrence_id = obligation_instance_id or test_uuid(f"obligation-instance:{lifecycle_id}:1")
    return {
        "specversion": "1.0",
        "id": test_uuid(f"obligation-evidence:{suffix}"),
        "source": "urn:33god:service:momo",
        "type": "bloodbank.v1.lifecycle.obligation_evidence.submitted",
        "subject": "bloodbank.evt.v1.lifecycle.obligation_evidence.submitted",
        "time": format_timestamp(completed_at),
        "datacontenttype": "application/json",
        "dataschema": (
            "apicurio://holyfields/bloodbank.v1.lifecycle.obligation_evidence.submitted/versions/2"
        ),
        "correlationid": correlation_id or test_uuid(f"obligation-correlation:{suffix}"),
        "causationid": causation_id or invocation_id,
        "producer": "momo",
        "service": "momo",
        "domain": "lifecycle",
        "schemaref": "bloodbank.v1.lifecycle.obligation_evidence.submitted.v2",
        "kind": "event",
        "actor": {"type": "service", "agent_id": "momo"},
        "ordering_key": f"lifecycle:{lifecycle_id}",
        "data": {
            "contract_version": 2,
            "lifecycle_id": lifecycle_id,
            "repo": repo,
            "obligation_id": obligation_id,
            "obligation_instance_id": occurrence_id,
            "obligation_kind": obligation_kind,
            "target_actor_id": target_actor_id,
            "invocation_id": invocation_id,
            "skill_ref": {
                "name": "bmad-code-review",
                "selector": "6.10.2",
            },
            "completed_at": format_timestamp(completed_at),
            "evidence": {
                "kind": "skill_completion",
                "outcome": "completed",
                "artifact_id": f"review:{lifecycle_id}:{obligation_id}:{suffix}",
                "artifact_sha256": "a" * 64,
                "summary": "Independent review completed with durable findings.",
            },
        },
    }
