"""Regression guard: the checked-in Bloodbank lock must pin the selected checkout."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from contracts import BLOODBANK_CONTRACT_COMMIT

ROOT = Path(__file__).resolve().parents[1]
BLOODBANK_ROOT = Path(__file__).resolve().parents[2] / "bloodbank"
LOCK = ROOT / "contracts" / "bloodbank-v1.lock.json"


def _lock() -> dict:
    return json.loads(LOCK.read_text(encoding="utf-8"))


def _bloodbank_head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=BLOODBANK_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_lock_commit_matches_selected_bloodbank_checkout() -> None:
    """Fail when the lock commit drifts from the selected Bloodbank checkout."""
    assert _lock()["bloodbank_commit"] == _bloodbank_head()


def test_lock_commit_matches_runtime_contract_constant() -> None:
    """The runtime constant and the checked-in lock must pin the same commit."""
    assert BLOODBANK_CONTRACT_COMMIT == _lock()["bloodbank_commit"]


def test_locked_file_digests_match_selected_checkout() -> None:
    """Every locked schema/transport byte digest must match the selected checkout."""
    lock = _lock()
    mismatches = []
    for relative, expected in sorted(lock["files"].items()):
        path = BLOODBANK_ROOT / relative
        actual = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "missing"
        if actual != expected:
            mismatches.append(relative)
    assert mismatches == []
