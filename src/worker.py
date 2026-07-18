"""Reconcile worker.

Claims a lifecycle from the dirty queue, runs reconciliation, persists
results transactionally, and releases the lease.
"""

from __future__ import annotations

import uuid

import structlog

from authority import LifecycleAuthority
from db.repository import LifecycleRepository

logger = structlog.get_logger()


class ReconcileWorker:
    def __init__(
        self,
        repo: LifecycleRepository,
        authority: LifecycleAuthority,
        worker_id: str | None = None,
    ) -> None:
        self.repo = repo
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.authority = authority

    async def run_once(self) -> bool:
        """Claim one job, reconcile, persist, release. Returns True if work was done."""
        record = await self.repo.claim_next_reconcile_job_record(
            worker_id=self.worker_id,
            lease_seconds=60,
        )
        if record is None:
            return False
        lifecycle_id, as_of = record
        try:
            await self.authority.reconcile_claimed(
                lifecycle_id=lifecycle_id,
                as_of=as_of,
                worker_id=self.worker_id,
            )
        except Exception:
            logger.exception(
                "reconcile_failed",
                lifecycle_id=lifecycle_id,
                worker=self.worker_id,
            )
            await self.repo.release_lease(lifecycle_id, requeue_delay_seconds=30)
        return True
