from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import os
import time
import urllib.error
import urllib.request
import uuid

import asyncpg
import nats
from nats.js.errors import NotFoundError
import pytest

from contracts import canonical_json
from tests.factories import command_envelope
from tests.integration.conftest import _docker, _free_port, _wait_tcp


pytestmark = pytest.mark.integration


def _http_json(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _wait_http(url: str, expected: int, timeout: float = 30) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            status, body = _http_json(url)
            last = (status, body)
            if status == expected:
                return body
        except (OSError, ValueError) as exc:
            last = exc
        time.sleep(0.2)
    raise AssertionError(f"{url} did not return {expected}; last={last!r}")


def _wait_container_health(container: str, timeout: float = 30) -> str:
    deadline = time.monotonic() + timeout
    last = "unknown"
    while time.monotonic() < deadline:
        last = _docker(
            "inspect",
            "--format",
            "{{.State.Health.Status}}",
            container,
        ).stdout.strip()
        if last == "healthy":
            return last
        time.sleep(0.2)
    raise AssertionError(f"container health stayed {last}")


@pytest.mark.asyncio
async def test_production_image_migration_health_and_canonical_flow(
    integration_resources,
) -> None:
    image = os.getenv("LIFECYCLE_TEST_IMAGE")
    if not image:
        pytest.skip("set LIFECYCLE_TEST_IMAGE to validate a built production image")
    stack = integration_resources.stack
    suffix = uuid.uuid4().hex[:8]
    migration_name = f"lifecycle-it-migrate-{suffix}"
    bootstrap_name = f"lifecycle-it-bootstrap-{suffix}"
    service_name = f"lifecycle-it-service-{suffix}"
    database_name = f"lifecycle_image_{suffix}"
    health_port = _free_port()
    await integration_resources.pool.execute(f'CREATE DATABASE "{database_name}" OWNER lifecycle')
    database_url = (
        f"postgresql://lifecycle:lifecycle@{stack.postgres_container}:5432/{database_name}"
    )
    host_database_url = (
        f"postgresql://lifecycle:lifecycle@127.0.0.1:{stack.postgres_port}/{database_name}"
    )
    image_pool = await asyncpg.create_pool(host_database_url, min_size=1, max_size=3)
    nats_url = f"nats://{stack.nats_container}:4222"
    common = (
        "--network",
        stack.network,
        "-e",
        f"LIFECYCLE_DATABASE_URL={database_url}",
        "-e",
        f"BLOODBANK_NATS_URLS={nats_url}",
    )
    lifecycle_id = f"lc_image_{suffix}"
    repo_name = f"delorenj/image-{suffix}"
    actor_id = f"agent:image:{suffix}"
    capability_id = f"cap-image-{suffix}"
    try:
        migration = _docker(
            "run",
            "--rm",
            "--name",
            migration_name,
            *common,
            image,
            "migrate",
        )
        migration_result = json.loads(migration.stdout.strip())
        assert migration_result["current"] is True
        assert migration_result["applied"] == migration_result["available"]

        bootstrap = _docker(
            "run",
            "--rm",
            "--name",
            bootstrap_name,
            *common,
            image,
            "bootstrap",
            "--lifecycle-id",
            lifecycle_id,
            "--name",
            f"Image {suffix}",
            "--repo",
            repo_name,
            "--actor-id",
            actor_id,
            "--capability-id",
            capability_id,
            "--as-of",
            "2026-07-18T19:00:00Z",
        )
        assert json.loads(bootstrap.stdout)["state_version"] == 1

        _docker(
            "run",
            "-d",
            "--name",
            service_name,
            *common,
            "-e",
            f"LIFECYCLE_INSTANCE=image-{suffix}",
            "-p",
            f"127.0.0.1:{health_port}:8080",
            image,
            "serve",
        )
        _wait_tcp(health_port)
        live = _wait_http(f"http://127.0.0.1:{health_port}/livez", 200)
        ready = _wait_http(f"http://127.0.0.1:{health_port}/readyz", 200)
        assert live == {"status": "live"}
        assert ready["status"] == "ready"
        assert ready["checks"]["migrations"]["status"] == "current"

        command = command_envelope(
            suffix=f"image-{suffix}",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=parse_image_time(),
        )
        await integration_resources.js.publish(
            command["subject"],
            canonical_json(command).encode(),
            headers={"Nats-Msg-Id": command["id"]},
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            row = await image_pool.fetchrow(
                """
                SELECT status, state_version FROM lifecycle_state
                WHERE lifecycle_id = $1
                """,
                lifecycle_id,
            )
            pending = int(
                await image_pool.fetchval(
                    """
                    SELECT COUNT(*) FROM lifecycle_event_outbox
                    WHERE lifecycle_id = $1 AND published_at IS NULL
                    """,
                    lifecycle_id,
                )
            )
            if row and row["status"] == "waiting" and row["state_version"] == 2 and pending == 0:
                break
            await asyncio.sleep(0.2)
        else:
            diagnostics = {
                "state": dict(row) if row else None,
                "pending": pending,
                "command_results": [
                    dict(item)
                    for item in await image_pool.fetch(
                        """
                        SELECT verdict, reason_code, observed_state_version
                        FROM lifecycle_command_results WHERE lifecycle_id = $1
                        """,
                        lifecycle_id,
                    )
                ],
                "command_consumer": str(
                    await integration_resources.js.consumer_info(
                        "BLOODBANK_COMMANDS",
                        "lifecycle-authority-commands-v1",
                    )
                ),
            }
            raise AssertionError(
                f"image service did not complete canonical command flow: {diagnostics}"
            )

        assert _wait_container_health(service_name) == "healthy"

        stack.stop_nats()
        try:
            _wait_http(f"http://127.0.0.1:{health_port}/readyz", 503)
            assert _wait_http(f"http://127.0.0.1:{health_port}/livez", 200) == {"status": "live"}
        finally:
            stack.start_nats()
        _wait_http(f"http://127.0.0.1:{health_port}/readyz", 200)
    finally:
        result = _docker("logs", service_name, check=False)
        if result.stdout:
            print(result.stdout)
        _docker("rm", "-f", service_name, check=False)
        _docker("rm", "-f", migration_name, check=False)
        _docker("rm", "-f", bootstrap_name, check=False)
        cleanup_nc = await nats.connect(stack.nats_url)
        cleanup_js = cleanup_nc.jetstream()
        try:
            for stream, durable in (
                ("BLOODBANK_COMMANDS", "lifecycle-authority-commands-v1"),
                ("BLOODBANK_EVENTS", "lifecycle-authority-repo-task-recorded-v1"),
                ("BLOODBANK_EVENTS", "lifecycle-authority-obligation-evidence-v1"),
            ):
                try:
                    await cleanup_js.delete_consumer(stream, durable)
                except NotFoundError:
                    pass
        finally:
            await cleanup_nc.close()
        await image_pool.close()
        await integration_resources.pool.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            database_name,
        )
        await integration_resources.pool.execute(f'DROP DATABASE "{database_name}"')


def parse_image_time() -> datetime:
    return datetime(2026, 7, 18, 19, 0, 1, tzinfo=timezone.utc)
