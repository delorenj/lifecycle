"""Regression tests for lifecycle-controller runtime blockers."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

import bloodbank as bloodbank_module
from bloodbank import BloodbankTransport, JetStreamRuntime, RuntimeMetrics
from db.repository import LifecycleRepository, _row_to_state
from main import _redact_database_url
from models import OutboxEvent
from service import LifecycleService
from worker import ReconcileWorker


def _compact(sql: str) -> str:
    return " ".join(sql.split())


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeAcquire:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn

    async def __aenter__(self) -> "_FakeConnection":
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self) -> None:
        self.fetchrow_sql = ""
        self.execute_sql = ""
        self.execute_args = ()

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetchrow(self, sql: str):
        self.fetchrow_sql = sql
        return {"lifecycle_id": "lc_1"}

    async def execute(self, sql: str, *args):
        self.execute_sql = sql
        self.execute_args = args


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConnection()
        self.execute_sql = ""
        self.execute_args = ()
        self.fetch_sql = ""
        self.fetch_args = ()

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)

    async def execute(self, sql: str, *args):
        self.execute_sql = sql
        self.execute_args = args

    async def fetch(self, sql: str, *args):
        self.fetch_sql = sql
        self.fetch_args = args
        return []


@pytest.mark.asyncio
async def test_claim_next_reconcile_job_uses_safe_interval_math_and_expired_leases():
    pool = _FakePool()
    repo = LifecycleRepository(pool)

    lifecycle_id = await repo.claim_next_reconcile_job("worker-a", lease_seconds=42)

    claim_sql = _compact(pool.conn.fetchrow_sql)
    update_sql = _compact(pool.conn.execute_sql)
    assert lifecycle_id == "lc_1"
    assert "lease_expires_at <= now()" in claim_sql
    assert "interval '$" not in update_sql
    assert "$2::int * interval '1 second'" in update_sql
    assert pool.conn.execute_args == ("worker-a", 42, "lc_1")


@pytest.mark.asyncio
async def test_release_and_stale_sentinel_queries_use_safe_interval_math():
    pool = _FakePool()
    repo = LifecycleRepository(pool)

    await repo.release_lease("lc_1", requeue_delay_seconds=30)
    release_sql = _compact(pool.execute_sql)
    assert "interval '$" not in release_sql
    assert "$1::int * interval '1 second'" in release_sql
    assert pool.execute_args == (30, "lc_1")

    await repo.get_stale_sentinels(threshold_minutes=15)
    stale_sql = _compact(pool.fetch_sql)
    assert "interval '$" not in stale_sql
    assert "$1::int * interval '1 minute'" in stale_sql
    assert pool.fetch_args == (15,)


def test_row_to_state_decodes_jsonb_policy_string():
    state = _row_to_state(
        {
            "lifecycle_id": "lc_1",
            "status": "active",
            "health": "nominal",
            "phase": None,
            "progress_percent": 0,
            "roadmap_version": 1,
            "status_reason": "",
            "health_reason": "",
            "last_progress_at": None,
            "last_reconciled_at": None,
            "state_version": 1,
            "state_fingerprint": "",
            "policy": '{"progress_expected": false, "stalled_after_minutes": 12}',
        }
    )

    assert state.policy.progress_expected is False
    assert state.policy.stalled_after_minutes == 12


@pytest.mark.parametrize("state_changed", [True, False])
@pytest.mark.asyncio
async def test_successful_reconcile_deletes_queue_job(monkeypatch, state_changed: bool):
    del monkeypatch

    class FakeRepo:
        def __init__(self) -> None:
            self.released = []

        async def claim_next_reconcile_job_record(self, worker_id: str, lease_seconds: int = 60):
            assert worker_id == "worker-a"
            assert lease_seconds == 60
            return "lc_1", datetime(2026, 7, 18, tzinfo=timezone.utc)

        async def release_lease(self, lifecycle_id: str, requeue_delay_seconds: int = 0):
            self.released.append((lifecycle_id, requeue_delay_seconds))

    class FakeAuthority:
        def __init__(self) -> None:
            self.calls = []

        async def reconcile_claimed(self, **kwargs):
            self.calls.append(kwargs)
            return state_changed

    repo = FakeRepo()
    authority = FakeAuthority()

    worked = await ReconcileWorker(repo, worker_id="worker-a", authority=authority).run_once()

    assert worked is True
    assert authority.calls == [
        {
            "lifecycle_id": "lc_1",
            "as_of": datetime(2026, 7, 18, tzinfo=timezone.utc),
            "worker_id": "worker-a",
        }
    ]
    assert repo.released == []


@pytest.mark.asyncio
async def test_default_outbox_publish_keeps_event_unpublished():
    class FakeRepo:
        def __init__(self) -> None:
            self.published = []
            self.failed = []
            self.claim_calls = 0

        async def claim_outbox(
            self,
            worker_id: str,
            *,
            batch_size: int,
            lease_seconds: int,
        ):
            assert worker_id == "publisher-test"
            assert lease_seconds == 30
            self.claim_calls += 1
            if self.claim_calls > 1:
                return []
            assert batch_size == 100
            return [
                OutboxEvent(
                    id=123,
                    lifecycle_id="lc_1",
                    event_type="bloodbank.v1.lifecycle.status.updated",
                    event_id="00000000-0000-4000-8000-000000000123",
                    subject="bloodbank.evt.v1.lifecycle.status.updated",
                    envelope={"id": "00000000-0000-4000-8000-000000000123"},
                )
            ]

        async def mark_outbox_published(self, outbox_id: int, worker_id: str):
            self.published.append(outbox_id)

        async def mark_outbox_failed(self, outbox_id: int, error: str, worker_id: str):
            self.failed.append((outbox_id, error))

    class UnavailableTransport:
        def __init__(self) -> None:
            self.metrics = RuntimeMetrics()

        async def publish_outbox(self, event: OutboxEvent) -> None:
            del event
            raise RuntimeError("Bloodbank JetStream is unavailable")

    repo = FakeRepo()
    published_count = await JetStreamRuntime(
        repository=repo,
        authority=object(),
        transport=UnavailableTransport(),
        worker_id="publisher-test",
    ).publish_outbox_once()

    assert published_count == 0
    assert repo.published == []
    assert repo.failed == [(123, "Bloodbank JetStream is unavailable")]


def test_redact_database_url_credentials():
    assert (
        _redact_database_url("postgresql://user:secret@localhost:5432/candystore")
        == "postgresql://***@localhost:5432/candystore"
    )
    assert _redact_database_url("postgresql://localhost:5432/candystore") == (
        "postgresql://localhost:5432/candystore"
    )


@pytest.mark.asyncio
async def test_authority_bundle_observation_query_has_no_hidden_limit() -> None:
    class CaptureConnection:
        def __init__(self) -> None:
            self.sql = ""
            self.args = ()

        async def fetch(self, sql: str, *args):
            self.sql = sql
            self.args = args
            return []

    connection = CaptureConnection()
    repo = LifecycleRepository(object())

    assert (
        await repo._get_observations(
            connection,
            "lc_1",
            datetime(2026, 7, 18, tzinfo=timezone.utc),
        )
        == []
    )
    assert "LIMIT" not in connection.sql
    assert connection.args == (
        "lc_1",
        datetime(2026, 7, 18, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_partial_consumer_binding_closes_and_resets_transport(monkeypatch) -> None:
    class FakeJetStream:
        def __init__(self) -> None:
            self.calls = 0

        async def pull_subscribe(self, *args, **kwargs):
            del args, kwargs
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("observation binding failed")
            return object()

    class FakeNats:
        def __init__(self) -> None:
            self.is_connected = True
            self.is_closed = False
            self.closed = False
            self.js = FakeJetStream()

        def jetstream(self):
            return self.js

        async def flush(self, timeout: float):
            del timeout

        async def close(self):
            self.closed = True
            self.is_closed = True
            self.is_connected = False

    fake_nats = FakeNats()

    async def fake_connect(**kwargs):
        del kwargs
        return fake_nats

    monkeypatch.setattr(bloodbank_module.nats, "connect", fake_connect)
    transport = BloodbankTransport(
        servers=["nats://test.invalid:4222"],
        client_name="partial-binding-test",
    )

    with pytest.raises(RuntimeError, match="observation binding failed"):
        await transport.connect()

    assert fake_nats.closed is True
    assert transport.nc is None
    assert transport.js is None
    assert transport.command_subscription is None
    assert transport.observation_subscription is None
    assert transport.metrics.counters["nats_binding_failed"] == 1


@pytest.mark.asyncio
async def test_service_fails_when_an_authority_worker_exits() -> None:
    class FakeHealth:
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        async def start(self):
            self.started = True

        async def stop(self):
            self.stopped = True

    class FakeTransport:
        connected = True
        consumers_bound = True
        nc = None

        def __init__(self) -> None:
            self.closed = False

        async def close(self):
            self.closed = True

    class FakeRuntime:
        async def command_loop(self, stop):
            del stop
            raise RuntimeError("command consumer stopped")

        async def observation_loop(self, stop):
            await stop.wait()

        async def outbox_loop(self, stop):
            await stop.wait()

    class FakeWorker:
        async def run_once(self):
            return False

    class FakeSweeper:
        async def run_once(self):
            return 0

    service = object.__new__(LifecycleService)
    service.health = FakeHealth()
    service.transport = FakeTransport()
    service.runtime = FakeRuntime()
    service.reconcile_worker = FakeWorker()
    service.sweeper = FakeSweeper()
    stop = asyncio.Event()

    with pytest.raises(RuntimeError, match="command consumer stopped"):
        await service.run(stop)

    assert stop.is_set()
    assert service.health.started is True
    assert service.health.stopped is True
    assert service.transport.closed is True
