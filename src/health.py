"""HTTP liveness, readiness, and minimal operational metrics."""

from __future__ import annotations

from typing import Any

from aiohttp import web

from bloodbank import BloodbankTransport, RuntimeMetrics
from db.migrations import migration_status
from db.repository import LifecycleRepository


class HealthApplication:
    def __init__(
        self,
        *,
        repository: LifecycleRepository,
        transport: BloodbankTransport,
        metrics: RuntimeMetrics,
    ) -> None:
        self.repository = repository
        self.transport = transport
        self.metrics = metrics
        self.started = False

    async def live(self, request: web.Request) -> web.Response:
        del request
        status = 200 if self.started else 503
        return web.json_response(
            {"status": "live" if self.started else "starting"},
            status=status,
        )

    async def ready(self, request: web.Request) -> web.Response:
        del request
        checks: dict[str, Any] = {}
        try:
            checks["database"] = "ready" if await self.repository.ping() else "failed"
            migrations = await migration_status(self.repository.pool)
            checks["migrations"] = {
                "status": "current" if migrations.current else "behind_or_drifted",
                "applied": migrations.applied,
                "available": migrations.available,
            }
        except Exception as exc:
            checks["database"] = f"failed:{type(exc).__name__}"
            checks["migrations"] = {"status": "unavailable"}
        nats_ready, nats_reason = await self.transport.ready()
        checks["bloodbank"] = nats_reason
        ready = (
            checks.get("database") == "ready"
            and checks.get("migrations", {}).get("status") == "current"
            and nats_ready
        )
        checks["outbox_pending"] = (
            await self.repository.outbox_pending_count()
            if checks.get("database") == "ready"
            else None
        )
        return web.json_response(
            {"status": "ready" if ready else "not_ready", "checks": checks},
            status=200 if ready else 503,
        )

    async def prometheus(self, request: web.Request) -> web.Response:
        del request
        return web.Response(
            text=self.metrics.render_prometheus(),
            content_type="text/plain",
        )

    def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/livez", self.live)
        app.router.add_get("/readyz", self.ready)
        app.router.add_get("/metrics", self.prometheus)
        return app


class HealthServer:
    def __init__(
        self,
        application: HealthApplication,
        *,
        host: str,
        port: int,
    ) -> None:
        self.application = application
        self.host = host
        self.port = port
        self.runner: web.AppRunner | None = None

    async def start(self) -> None:
        self.runner = web.AppRunner(self.application.create_app())
        await self.runner.setup()
        await web.TCPSite(self.runner, self.host, self.port).start()
        self.application.started = True

    async def stop(self) -> None:
        self.application.started = False
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None


__all__ = ["HealthApplication", "HealthServer"]
