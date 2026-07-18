from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
import socket
import subprocess
import time
import uuid

import asyncpg
import nats
from nats.js.api import RetentionPolicy, StorageType
from nats.js.errors import NotFoundError
import pytest
import pytest_asyncio

from db.migrations import apply_migrations


POSTGRES_IMAGE = "postgres@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416"
NATS_IMAGE = "nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927"


_TRANSIENT_POSTGRES_CONNECTION_ERRORS = (
    OSError,
    asyncpg.CannotConnectNowError,
    asyncpg.PostgresConnectionError,
)


@dataclass(frozen=True)
class DockerStack:
    suffix: str
    network: str
    postgres_container: str
    postgres_volume: str
    postgres_port: int
    nats_container: str
    nats_volume: str
    nats_port: int

    @property
    def database_url(self) -> str:
        return f"postgresql://lifecycle:lifecycle@127.0.0.1:{self.postgres_port}/lifecycle"

    @property
    def nats_url(self) -> str:
        return f"nats://127.0.0.1:{self.nats_port}"

    def stop_nats(self) -> None:
        _docker("stop", self.nats_container)

    def start_nats(self) -> None:
        _docker("start", self.nats_container)
        _wait_tcp(self.nats_port)


@dataclass
class IntegrationResources:
    stack: DockerStack
    pool: asyncpg.Pool
    nc: nats.NATS
    js: object


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_tcp(port: int, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"port {port} did not become ready")


def _wait_postgres(container: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = _docker(
            "exec",
            container,
            "pg_isready",
            "-U",
            "lifecycle",
            "-d",
            "lifecycle",
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(0.2)
    raise TimeoutError("isolated PostgreSQL did not become ready")


async def _create_pool_with_retry(
    database_url: str,
    *,
    timeout: float = 30,
) -> asyncpg.Pool:
    deadline = time.monotonic() + timeout
    while True:
        try:
            return await asyncpg.create_pool(
                database_url,
                min_size=1,
                max_size=8,
            )
        except _TRANSIENT_POSTGRES_CONNECTION_ERRORS:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            await asyncio.sleep(min(0.2, remaining))


@pytest.fixture(scope="session")
def docker_stack() -> DockerStack:
    if os.getenv("LIFECYCLE_RUN_INTEGRATION") != "1":
        pytest.skip("set LIFECYCLE_RUN_INTEGRATION=1 for isolated container tests")
    suffix = uuid.uuid4().hex[:12]
    network = f"lifecycle-it-net-{suffix}"
    postgres_container = f"lifecycle-it-pg-{suffix}"
    postgres_volume = f"lifecycle-it-pgdata-{suffix}"
    nats_container = f"lifecycle-it-nats-{suffix}"
    nats_volume = f"lifecycle-it-natsdata-{suffix}"
    postgres_port = _free_port()
    nats_port = _free_port()
    _docker("network", "create", network)
    _docker("volume", "create", postgres_volume)
    _docker("volume", "create", nats_volume)
    try:
        _docker(
            "run",
            "-d",
            "--name",
            postgres_container,
            "--network",
            network,
            "-e",
            "POSTGRES_USER=lifecycle",
            "-e",
            "POSTGRES_PASSWORD=lifecycle",
            "-e",
            "POSTGRES_DB=lifecycle",
            "-v",
            f"{postgres_volume}:/var/lib/postgresql/data",
            "-p",
            f"127.0.0.1:{postgres_port}:5432",
            POSTGRES_IMAGE,
        )
        _docker(
            "run",
            "-d",
            "--name",
            nats_container,
            "--network",
            network,
            "-v",
            f"{nats_volume}:/data",
            "-p",
            f"127.0.0.1:{nats_port}:4222",
            NATS_IMAGE,
            "-js",
            "-sd",
            "/data",
        )
        _wait_postgres(postgres_container)
        _wait_tcp(nats_port)
        stack = DockerStack(
            suffix=suffix,
            network=network,
            postgres_container=postgres_container,
            postgres_volume=postgres_volume,
            postgres_port=postgres_port,
            nats_container=nats_container,
            nats_volume=nats_volume,
            nats_port=nats_port,
        )
        yield stack
    finally:
        _docker("rm", "-f", postgres_container, check=False)
        _docker("rm", "-f", nats_container, check=False)
        _docker("volume", "rm", postgres_volume, check=False)
        _docker("volume", "rm", nats_volume, check=False)
        _docker("network", "rm", network, check=False)


@pytest_asyncio.fixture
async def integration_resources(
    docker_stack: DockerStack,
) -> IntegrationResources:
    pool = await _create_pool_with_retry(docker_stack.database_url)
    await apply_migrations(pool)
    nc = await nats.connect(docker_stack.nats_url)
    js = nc.jetstream()
    for name, subjects, retention in (
        (
            "BLOODBANK_EVENTS",
            ["bloodbank.evt.v1.>"],
            RetentionPolicy.LIMITS,
        ),
        (
            "BLOODBANK_COMMANDS",
            ["bloodbank.cmd.v1.>", "bloodbank.rpy.v1.>"],
            RetentionPolicy.WORK_QUEUE,
        ),
    ):
        try:
            await js.stream_info(name)
        except NotFoundError:
            await js.add_stream(
                name=name,
                subjects=subjects,
                retention=retention,
                storage=StorageType.FILE,
            )
    try:
        yield IntegrationResources(docker_stack, pool, nc, js)
    finally:
        if not nc.is_closed:
            try:
                await nc.drain()
            except Exception:
                await nc.close()
        await pool.close()
