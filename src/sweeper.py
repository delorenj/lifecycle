"""Periodic sweeper — enqueues all active lifecycles for reconciliation.

This is the backstop for silence/time-based failures. Even if no sentinel
emits observations, the sweeper ensures lifecycles are re-evaluated on
cadence so staleness (stalled, blocked, degraded observers) is detected.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Callable

import structlog

from db.repository import LifecycleRepository

logger = structlog.get_logger()


class Sweeper:
    def __init__(
        self,
        repo: LifecycleRepository,
        clock: Callable[[], datetime],
    ) -> None:
        self.repo = repo
        self.clock = clock

    async def run_once(self) -> int:
        """Enqueue all active lifecycles. Returns count enqueued."""
        count = await self.repo.enqueue_sweep(self.clock())
        logger.info("sweep_enqueued", count=count)
        return count

    async def run_loop(self, interval_seconds: float = 300.0) -> None:
        """Run sweep every N seconds."""
        while True:
            await self.run_once()
            await asyncio.sleep(interval_seconds)
