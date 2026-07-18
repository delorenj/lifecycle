"""Lifecycle production service orchestration."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog

from authority import LifecycleAuthority
from bloodbank import BloodbankTransport, JetStreamRuntime, RuntimeMetrics
from db.repository import LifecycleRepository
from health import HealthApplication, HealthServer
from sweeper import Sweeper
from worker import ReconcileWorker


logger = structlog.get_logger()
UTC = timezone.utc


class LifecycleService:
    def __init__(
        self,
        *,
        repository: LifecycleRepository,
        authority_instance: str,
        nats_servers: list[str],
        health_host: str,
        health_port: int,
    ) -> None:
        self.repository = repository
        self.metrics = RuntimeMetrics()
        self.authority = LifecycleAuthority(
            repository,
            authority_instance=authority_instance,
        )
        self.transport = BloodbankTransport(
            servers=nats_servers,
            client_name=f"lifecycle-{authority_instance}",
            metrics=self.metrics,
        )
        self.runtime = JetStreamRuntime(
            repository=repository,
            authority=self.authority,
            transport=self.transport,
        )
        self.reconcile_worker = ReconcileWorker(
            repository,
            authority=self.authority,
        )
        self.sweeper = Sweeper(repository, clock=utc_now)
        self.health = HealthServer(
            HealthApplication(
                repository=repository,
                transport=self.transport,
                metrics=self.metrics,
            ),
            host=health_host,
            port=health_port,
        )

    async def _connection_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if not self.transport.connected or not self.transport.consumers_bound:
                nc = self.transport.nc
                if nc is not None and nc.is_reconnecting:
                    await _wait_or_stop(stop, 1)
                    continue
                if nc is not None and not nc.is_closed:
                    await nc.close()
                self.transport.nc = None
                self.transport.js = None
                try:
                    await self.transport.connect()
                    logger.info("bloodbank_connected")
                except Exception as exc:
                    self.metrics.increment("nats_initial_connect_retry")
                    logger.warning("bloodbank_connect_failed", error=str(exc))
            await _wait_or_stop(stop, 1)

    async def _reconcile_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            worked = await self.reconcile_worker.run_once()
            if not worked:
                await _wait_or_stop(stop, 0.5)

    async def _sweep_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.sweeper.run_once()
            await _wait_or_stop(stop, 180)

    async def run(self, stop: asyncio.Event) -> None:
        await self.health.start()
        logger.info("lifecycle_service_started")
        tasks = [
            asyncio.create_task(self._connection_loop(stop), name="bloodbank-connect"),
            asyncio.create_task(self.runtime.command_loop(stop), name="command-consumer"),
            asyncio.create_task(self.runtime.observation_loop(stop), name="observation-consumer"),
            asyncio.create_task(self.runtime.outbox_loop(stop), name="outbox-publisher"),
            asyncio.create_task(self._reconcile_loop(stop), name="reconciler"),
            asyncio.create_task(self._sweep_loop(stop), name="sweeper"),
        ]
        stop_task = asyncio.create_task(stop.wait(), name="service-stop")
        failure: BaseException | None = None
        try:
            done, _ = await asyncio.wait(
                [stop_task, *tasks],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task not in done:
                failed_task = next(task for task in tasks if task in done)
                failure = failed_task.exception()
                if failure is None:
                    failure = RuntimeError(
                        f"service worker exited unexpectedly: {failed_task.get_name()}"
                    )
                stop.set()
        finally:
            stop_task.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(stop_task, *tasks, return_exceptions=True)
            await self.transport.close()
            await self.health.stop()
            logger.info("lifecycle_service_stopped")
        if failure is not None:
            raise failure


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        cache_logger_on_first_use=True,
    )


def utc_now() -> datetime:
    """Runtime boundary clock; authority code receives this value explicitly."""

    return datetime.now(UTC)


__all__ = ["LifecycleService", "configure_logging", "utc_now"]
