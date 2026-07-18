from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


BLOODBANK_ROOT = Path(__file__).resolve().parents[2] / "bloodbank"
SCHEMAS_ROOT = BLOODBANK_ROOT / "schemas"
SCHEMA_BY_REF = {
    "bloodbank.v1.lifecycle.intent.submit.command.v1": (
        "bloodbank/v1/lifecycle/intent.submit.command.v1.json"
    ),
    "bloodbank.v1.lifecycle.intent.submit.reply.v1": (
        "bloodbank/v1/lifecycle/intent.submit.reply.v1.json"
    ),
    "bloodbank.v1.lifecycle.observation.recorded.v1": (
        "bloodbank/v1/lifecycle/observation.recorded.v1.json"
    ),
    "bloodbank.v1.lifecycle.snapshot.updated.v1": (
        "bloodbank/v1/lifecycle/snapshot.updated.v1.json"
    ),
    "bloodbank.v1.lifecycle.status.updated.v1": ("bloodbank/v1/lifecycle/status.updated.v1.json"),
    "bloodbank.v1.repo.task.recorded.v1": ("bloodbank/v1/repo/task.recorded.v1.json"),
}


def validate_with_bloodbank(envelope: dict) -> None:
    registry = Registry()
    for path in SCHEMAS_ROOT.rglob("*.json"):
        document = json.loads(path.read_text(encoding="utf-8"))
        if "$id" in document:
            registry = registry.with_resource(
                document["$id"],
                Resource.from_contents(document),
            )
    relative = SCHEMA_BY_REF[envelope["schemaref"]]
    schema = json.loads((SCHEMAS_ROOT / relative).read_text(encoding="utf-8"))
    Draft202012Validator(
        schema,
        registry=registry,
        format_checker=FormatChecker(),
    ).validate(envelope)
