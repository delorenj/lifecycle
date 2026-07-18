"""Lifecycle authority command-line and production process entrypoint."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
import json
import os
import signal
import socket
from typing import Any

import aiohttp
import asyncpg

from db.migrations import apply_migrations, migration_status
from db.repository import LifecycleRepository
from models import CapabilityGrant, OperatingMode
from service import LifecycleService, configure_logging
from specification import CAPABILITY_ACTION, default_spec
from contracts import parse_timestamp


def _redact_database_url(database_url: str) -> str:
    if "://" not in database_url:
        return database_url
    scheme, rest = database_url.split("://", 1)
    if "@" not in rest:
        return database_url
    _, host_and_path = rest.rsplit("@", 1)
    return f"{scheme}://***@{host_and_path}"


def _database_url(args: argparse.Namespace) -> str:
    value = args.database_url or os.getenv("LIFECYCLE_DATABASE_URL")
    if not value:
        raise RuntimeError(
            "LIFECYCLE_DATABASE_URL (or --database-url) is required; "
            "Lifecycle never defaults to Candystore"
        )
    return value


async def _pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )


async def _migrate(args: argparse.Namespace) -> int:
    pool = await _pool(_database_url(args))
    try:
        status = await apply_migrations(pool)
        print(
            json.dumps(
                {
                    "mode": "migrate",
                    "current": status.current,
                    "applied": status.applied,
                    "available": status.available,
                },
                sort_keys=True,
            )
        )
        return 0 if status.current else 1
    finally:
        await pool.close()


async def _bootstrap(args: argparse.Namespace) -> int:
    created_at = parse_timestamp(args.as_of, "--as-of")
    grant = CapabilityGrant(
        capability_id=args.capability_id,
        capability_version=1,
        actor_id=args.actor_id,
        actions=(CAPABILITY_ACTION,),
        scope=f"lifecycle:{args.lifecycle_id}",
        issued_at=created_at,
        expires_at=None,
        state_version=1,
    )
    spec = replace(
        default_spec(args.lifecycle_id, capabilities=(grant,)),
        default_mode=OperatingMode(args.mode),
    )
    pool = await _pool(_database_url(args))
    try:
        status = await migration_status(pool)
        if not status.current:
            raise RuntimeError("lifecycle migrations are not current; run migrate")
        state = await LifecycleRepository(pool).create_authority_lifecycle(
            lifecycle_id=args.lifecycle_id,
            name=args.name,
            repo=args.repo,
            spec=spec,
            created_by=args.actor_id,
            created_at=created_at,
        )
        print(
            json.dumps(
                {
                    "lifecycle_id": state.lifecycle_id,
                    "spec_version": state.spec_version,
                    "state_version": state.state_version,
                    "status": state.status.value,
                    "mode": state.mode.value,
                    "capability_id": args.capability_id,
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        await pool.close()


async def _serve(args: argparse.Namespace) -> int:
    configure_logging()
    pool = await _pool(_database_url(args))
    status = await migration_status(pool)
    if not status.current:
        await pool.close()
        raise RuntimeError("lifecycle migrations are not current; run the explicit migrate mode")
    instance = args.authority_instance or os.getenv("LIFECYCLE_INSTANCE") or socket.gethostname()
    nats_value = args.nats_servers or os.getenv("BLOODBANK_NATS_URLS", "nats://127.0.0.1:4222")
    service = LifecycleService(
        repository=LifecycleRepository(pool),
        authority_instance=instance,
        nats_servers=[item.strip() for item in nats_value.split(",") if item.strip()],
        health_host=args.health_host,
        health_port=args.health_port,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    try:
        await service.run(stop)
        return 0
    finally:
        await pool.close()


async def _healthcheck(args: argparse.Namespace) -> int:
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(args.url) as response:
                body: Any = await response.json()
                print(json.dumps(body, sort_keys=True))
                return 0 if response.status == 200 else 1
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        print(json.dumps({"status": "unreachable", "error": str(exc)}))
        return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lifecycle")
    parser.add_argument("--database-url", help="Lifecycle-owned PostgreSQL DSN")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("migrate", help="Apply forward-only migrations once")

    bootstrap = subparsers.add_parser("bootstrap", help="Create a versioned lifecycle")
    bootstrap.add_argument("--lifecycle-id", required=True)
    bootstrap.add_argument("--name", required=True)
    bootstrap.add_argument("--repo", required=True)
    bootstrap.add_argument("--actor-id", required=True)
    bootstrap.add_argument("--capability-id", required=True)
    bootstrap.add_argument("--as-of", required=True, help="RFC3339 bootstrap time")
    bootstrap.add_argument(
        "--mode",
        choices=[mode.value for mode in OperatingMode],
        default=OperatingMode.SUPERVISED.value,
    )

    serve = subparsers.add_parser("serve", help="Run the authority service")
    serve.add_argument("--nats-servers", help="Comma-separated Bloodbank NATS URLs")
    serve.add_argument("--authority-instance")
    serve.add_argument("--health-host", default="0.0.0.0")
    serve.add_argument("--health-port", type=int, default=8080)

    healthcheck = subparsers.add_parser("healthcheck", help="Probe one HTTP endpoint")
    healthcheck.add_argument("--url", default="http://127.0.0.1:8080/livez")
    healthcheck.add_argument("--timeout", type=float, default=3.0)
    return parser


async def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "migrate":
        return await _migrate(args)
    if args.command == "bootstrap":
        return await _bootstrap(args)
    if args.command == "serve":
        return await _serve(args)
    if args.command == "healthcheck":
        return await _healthcheck(args)
    raise AssertionError(f"unsupported command: {args.command}")


def cli() -> None:
    raise SystemExit(asyncio.run(_dispatch(_parser().parse_args())))


if __name__ == "__main__":
    cli()
