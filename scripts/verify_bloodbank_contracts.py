#!/usr/bin/env python3
"""Fail closed if the pinned Bloodbank lifecycle/transport surface drifts."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "contracts" / "bloodbank-v1.lock.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bloodbank-root",
        type=Path,
        default=ROOT.parent / "bloodbank",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    bloodbank = args.bloodbank_root.resolve()
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=bloodbank,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if commit != lock["bloodbank_commit"]:
        raise SystemExit(
            f"Bloodbank commit drift: expected {lock['bloodbank_commit']}, got {commit}"
        )
    failures = []
    for relative, expected in sorted(lock["files"].items()):
        path = bloodbank / relative
        actual = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "missing"
        if actual != expected:
            failures.append(f"{relative}: expected {expected}, got {actual}")
    if failures:
        raise SystemExit("Bloodbank contract drift:\n" + "\n".join(failures))
    streams = json.loads((bloodbank / "compose/nats/streams.json").read_text())
    subjects = {stream["name"]: stream["subjects"] for stream in streams["streams"]}
    if "bloodbank.evt.v1.>" not in subjects.get("BLOODBANK_EVENTS", []):
        raise SystemExit("BLOODBANK_EVENTS no longer covers canonical lifecycle events")
    command_subjects = subjects.get("BLOODBANK_COMMANDS", [])
    if not {"bloodbank.cmd.v1.>", "bloodbank.rpy.v1.>"}.issubset(command_subjects):
        raise SystemExit("BLOODBANK_COMMANDS command/reply coverage drifted")
    print(
        json.dumps(
            {
                "bloodbank_commit": commit,
                "locked_files": len(lock["files"]),
                "status": "verified",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
