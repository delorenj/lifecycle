from __future__ import annotations

import asyncpg

from tests.integration import conftest as integration_conftest


async def test_pool_readiness_retries_postgres_startup_error(monkeypatch) -> None:
    attempts = 0
    sleep_delays: list[float] = []
    expected_pool = object()

    async def create_pool(*args, **kwargs):
        nonlocal attempts
        del args, kwargs
        attempts += 1
        if attempts == 1:
            raise asyncpg.CannotConnectNowError("the database system is starting up")
        return expected_pool

    async def sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(integration_conftest.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(integration_conftest.asyncio, "sleep", sleep)

    pool = await integration_conftest._create_pool_with_retry("postgresql://test")

    assert pool is expected_pool
    assert attempts == 2
    assert sleep_delays == [0.2]
