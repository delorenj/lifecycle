"""Forward-only PostgreSQL migration runner."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import asyncpg


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
LOCK_NAME = "delorenj/lifecycle:migrations:v1"


@dataclass(frozen=True)
class MigrationStatus:
    applied: int
    available: int
    current: bool


def migration_files(directory: Path = MIGRATIONS_DIR) -> list[Path]:
    files = sorted(directory.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    versions = [path.name.split("_", 1)[0] for path in files]
    if len(versions) != len(set(versions)):
        raise RuntimeError("duplicate lifecycle migration version")
    return files


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def _ensure_ledger(connection: asyncpg.Connection) -> None:
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lifecycle_schema_migrations (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


async def apply_migrations(
    pool: asyncpg.Pool,
    directory: Path = MIGRATIONS_DIR,
) -> MigrationStatus:
    files = migration_files(directory)
    async with pool.acquire() as connection:
        await _ensure_ledger(connection)
        await connection.execute("SELECT pg_advisory_lock(hashtext($1))", LOCK_NAME)
        try:
            rows = await connection.fetch(
                "SELECT version, sha256 FROM lifecycle_schema_migrations ORDER BY version"
            )
            applied = {row["version"]: row["sha256"] for row in rows}
            for path in files:
                version = path.name.split("_", 1)[0]
                checksum = _checksum(path)
                if version in applied:
                    if applied[version] != checksum:
                        raise RuntimeError(f"applied lifecycle migration {version} checksum drift")
                    continue
                async with connection.transaction():
                    await connection.execute(path.read_text(encoding="utf-8"))
                    await connection.execute(
                        """
                        INSERT INTO lifecycle_schema_migrations(version, name, sha256)
                        VALUES ($1, $2, $3)
                        """,
                        version,
                        path.name,
                        checksum,
                    )
                applied[version] = checksum
        finally:
            await connection.execute("SELECT pg_advisory_unlock(hashtext($1))", LOCK_NAME)
    return MigrationStatus(
        applied=len(applied),
        available=len(files),
        current=len(applied) == len(files),
    )


async def migration_status(
    pool: asyncpg.Pool,
    directory: Path = MIGRATIONS_DIR,
) -> MigrationStatus:
    files = migration_files(directory)
    async with pool.acquire() as connection:
        ledger_exists = await connection.fetchval(
            "SELECT to_regclass('public.lifecycle_schema_migrations') IS NOT NULL"
        )
        if not ledger_exists:
            return MigrationStatus(applied=0, available=len(files), current=False)
        rows = await connection.fetch(
            "SELECT version, sha256 FROM lifecycle_schema_migrations ORDER BY version"
        )
    applied = {row["version"]: row["sha256"] for row in rows}
    expected = {path.name.split("_", 1)[0]: _checksum(path) for path in files}
    drift = any(applied.get(version) != checksum for version, checksum in expected.items())
    return MigrationStatus(
        applied=len(applied),
        available=len(files),
        current=not drift and applied.keys() == expected.keys(),
    )


__all__ = [
    "MIGRATIONS_DIR",
    "MigrationStatus",
    "apply_migrations",
    "migration_files",
    "migration_status",
]
