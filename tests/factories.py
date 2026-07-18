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
