from __future__ import annotations

import json
from typing import cast

from aiohttp import web
import asyncpg

from bloodbank import RuntimeMetrics
from db.migrations import MigrationStatus
import health as health_module
from health import HealthApplication


class _RaceRepository:
    pool = object()

    async def ping(self) -> bool:
        return True

    async def outbox_pending_count(self) -> int:
        raise asyncpg.ConnectionDoesNotExistError("connection closed after migration probe")


class _ReadyTransport:
    async def ready(self) -> tuple[bool, str]:
        return True, "ready"


async def test_ready_fails_closed_when_database_drops_before_outbox_probe(monkeypatch) -> None:
    repository = _RaceRepository()

    async def current_migrations(pool: object) -> MigrationStatus:
        assert pool is repository.pool
        return MigrationStatus(applied=2, available=2, current=True)

    monkeypatch.setattr(health_module, "migration_status", current_migrations)
    application = HealthApplication(
        repository=repository,
        transport=_ReadyTransport(),
        metrics=RuntimeMetrics(),
    )

    response = await application.ready(cast(web.Request, None))

    assert response.status == 503
    assert json.loads(response.text) == {
        "status": "not_ready",
        "checks": {
            "database": "failed:ConnectionDoesNotExistError",
            "migrations": {"status": "unavailable"},
            "outbox_pending": None,
            "bloodbank": "ready",
        },
    }
