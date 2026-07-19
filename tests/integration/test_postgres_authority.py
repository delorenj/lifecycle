from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import uuid

import asyncpg
import pytest

from authority import LifecycleAuthority
from db.migrations import apply_migrations
from db.repository import ConcurrencyConflict, LifecycleRepository
from models import CapabilityGrant, CommandVerdict
from specification import CAPABILITY_ACTION, default_spec
from tests.factories import command_envelope, obligation_evidence_envelope, repo_task_envelope
from tests.schema_validation import validate_with_bloodbank


pytestmark = pytest.mark.integration
NOW = datetime(2026, 7, 18, 17, 0, tzinfo=timezone.utc)


async def _bootstrap(resources, suffix: str, *, capability_version: int = 1):
    lifecycle_id = f"lc_{suffix}"
    repo_name = f"delorenj/test-{suffix}"
    actor_id = f"agent:{suffix}"
    capability_id = f"cap-{suffix}"
    repository = LifecycleRepository(resources.pool)
    grant = CapabilityGrant(
        capability_id=capability_id,
        capability_version=capability_version,
        actor_id=actor_id,
        actions=(CAPABILITY_ACTION,),
        scope=f"lifecycle:{lifecycle_id}",
        issued_at=NOW - timedelta(minutes=1),
        expires_at=None,
        state_version=1,
    )
    await repository.create_authority_lifecycle(
        lifecycle_id=lifecycle_id,
        name=f"Test {suffix}",
        repo=repo_name,
        spec=default_spec(lifecycle_id, capabilities=(grant,)),
        created_by=actor_id,
        created_at=NOW,
    )
    return repository, lifecycle_id, repo_name, actor_id, capability_id


async def _counts(pool, lifecycle_id: str) -> dict[str, int]:
    values = {}
    for name, table in (
        ("history", "lifecycle_status_history"),
        ("commands", "lifecycle_command_results"),
        ("outbox", "lifecycle_event_outbox"),
        ("observations", "lifecycle_observations"),
    ):
        values[name] = int(
            await pool.fetchval(
                f"SELECT COUNT(*) FROM {table} WHERE lifecycle_id = $1",
                lifecycle_id,
            )
        )
    return values


@pytest.mark.asyncio
async def test_atomic_command_idempotency_and_all_stable_rejections(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-1")
    applied_envelope = command_envelope(
        suffix=f"{suffix}-applied",
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        actor_id=actor_id,
        capability_id=capability_id,
        target="waiting",
        requested_at=NOW + timedelta(seconds=1),
    )

    applied = await authority.handle_command_envelope(
        applied_envelope,
        published_at=NOW + timedelta(seconds=1),
    )
    retry = await authority.handle_command_envelope(
        applied_envelope,
        published_at=NOW + timedelta(seconds=1),
    )

    assert applied.result.verdict == CommandVerdict.APPLIED
    assert applied.result.mutated is True
    assert applied.result.resulting_state_version == 2
    assert retry.result.verdict == CommandVerdict.IDEMPOTENT
    assert retry.result.mutated is False
    assert retry.result.applied_event_id == applied.result.applied_event_id

    stale = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-stale",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=1,
            actor_id=actor_id,
            capability_id=capability_id,
            target="active",
            requested_at=NOW + timedelta(seconds=2),
        ),
        published_at=NOW + timedelta(seconds=2),
    )
    unauthorized = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-unauthorized",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=2,
            actor_id="agent:intruder",
            capability_id=capability_id,
            target="active",
            requested_at=NOW + timedelta(seconds=3),
        ),
        published_at=NOW + timedelta(seconds=3),
    )
    illegal = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-illegal",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=2,
            actor_id=actor_id,
            capability_id=capability_id,
            target="completed",
            parameters={"confirmed": True},
            requested_at=NOW + timedelta(seconds=4),
        ),
        published_at=NOW + timedelta(seconds=4),
    )
    malformed_envelope = command_envelope(
        suffix=f"{suffix}-malformed",
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        expected_state_version=2,
        actor_id=actor_id,
        capability_id=capability_id,
        target="active",
        requested_at=NOW + timedelta(seconds=5),
    )
    malformed_envelope["data"]["intent"]["parameters"] = "invalid"
    malformed = await authority.handle_command_envelope(
        malformed_envelope,
        published_at=NOW + timedelta(seconds=5),
    )
    malformed_actor_envelope = command_envelope(
        suffix=f"{suffix}-malformed-actor",
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        expected_state_version=2,
        actor_id=actor_id,
        capability_id=capability_id,
        target="active",
        requested_at=NOW + timedelta(seconds=6),
    )
    malformed_actor_envelope["actor"]["provider"] = 33
    malformed_actor = await authority.handle_command_envelope(
        malformed_actor_envelope,
        published_at=NOW + timedelta(seconds=6),
    )

    assert stale.result.verdict == CommandVerdict.STALE
    assert unauthorized.result.verdict == CommandVerdict.UNAUTHORIZED
    assert illegal.result.verdict == CommandVerdict.ILLEGAL
    assert malformed.result.verdict == CommandVerdict.MALFORMED
    assert malformed_actor.result.verdict == CommandVerdict.MALFORMED
    assert all(
        not item.result.mutated
        for item in (stale, unauthorized, illegal, malformed, malformed_actor)
    )
    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.state_version == 2
    assert state.status.value == "waiting"
    counts = await _counts(integration_resources.pool, lifecycle_id)
    assert counts == {
        "history": 2,
        "commands": 6,
        "outbox": 9,
        "observations": 0,
    }


@pytest.mark.asyncio
async def test_transaction_aborts_state_history_result_and_outbox_atomically(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )

    class FailingResultRepository(LifecycleRepository):
        async def insert_command_result_tx(self, *args, **kwargs):
            del args, kwargs
            raise RuntimeError("injected command-result failure")

    authority = LifecycleAuthority(
        FailingResultRepository(integration_resources.pool),
        authority_instance="integration-atomic-abort",
    )
    envelope = command_envelope(
        suffix=f"{suffix}-atomic-abort",
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        actor_id=actor_id,
        capability_id=capability_id,
        target="waiting",
        requested_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(RuntimeError, match="injected command-result failure"):
        await authority.handle_command_envelope(
            envelope,
            published_at=NOW + timedelta(seconds=1),
        )

    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.state_version == 1
    counts = await _counts(integration_resources.pool, lifecycle_id)
    assert counts == {
        "history": 1,
        "commands": 0,
        "outbox": 0,
        "observations": 0,
    }


@pytest.mark.asyncio
async def test_expected_version_lock_serializes_racing_mutations(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-race")
    commands = [
        command_envelope(
            suffix=f"{suffix}-{mode}",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            intent_name="set_mode",
            target=mode,
            requested_at=NOW + timedelta(seconds=1),
        )
        for mode in ("manual", "autonomous")
    ]

    results = await asyncio.gather(
        *(
            authority.handle_command_envelope(
                command,
                published_at=NOW + timedelta(seconds=1),
            )
            for command in commands
        )
    )

    assert sorted(item.result.verdict.value for item in results) == ["applied", "stale"]
    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.state_version == 2
    assert state.mode.value in {"manual", "autonomous"}
    counts = await _counts(integration_resources.pool, lifecycle_id)
    assert counts["history"] == 2
    assert counts["commands"] == 2
    assert counts["outbox"] == 3


@pytest.mark.asyncio
async def test_observation_dedup_and_restart_replay_have_no_duplicate_effect(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, _, _ = await _bootstrap(integration_resources, suffix)
    authority = LifecycleAuthority(repository, authority_instance="integration-observe")
    envelope = repo_task_envelope(
        suffix=suffix,
        observed_at=NOW + timedelta(seconds=10),
        repo=repo_name,
    )

    assert await authority.ingest_repo_task_envelope(
        envelope,
        received_at=NOW + timedelta(seconds=11),
    )
    assert not await authority.ingest_repo_task_envelope(
        envelope,
        received_at=NOW + timedelta(seconds=12),
    )
    row = await integration_resources.pool.fetchrow(
        "SELECT * FROM lifecycle_observations WHERE lifecycle_id = $1",
        lifecycle_id,
    )
    assert row["source_event_id"] == uuid.UUID(envelope["id"])
    assert row["source_event_subject"] == envelope["subject"]
    assert row["source_event_source"] == envelope["source"]
    assert row["source_event_producer"] == envelope["producer"]
    assert row["observed_at"] == NOW + timedelta(seconds=10)

    claimed = await repository.claim_next_reconcile_job_record("worker-first")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=11))
    changed = await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id="worker-first",
    )
    assert changed is True

    async with integration_resources.pool.acquire() as connection:
        async with connection.transaction():
            await repository.mark_dirty_tx(
                connection,
                lifecycle_id,
                "deterministic-replay",
                NOW + timedelta(seconds=10),
            )
    restarted = LifecycleAuthority(
        LifecycleRepository(integration_resources.pool),
        authority_instance="integration-restarted",
    )
    claimed_again = await repository.claim_next_reconcile_job_record("worker-restart")
    assert claimed_again is not None
    replay_changed = await restarted.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed_again[1],
        worker_id="worker-restart",
    )

    assert replay_changed is False
    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.state_version == 2
    counts = await _counts(integration_resources.pool, lifecycle_id)
    assert counts["observations"] == 1
    assert counts["history"] == 2
    assert counts["outbox"] == 3


@pytest.mark.asyncio
async def test_older_claimed_reconcile_cannot_overwrite_newer_command(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    async with integration_resources.pool.acquire() as connection:
        async with connection.transaction():
            await repository.mark_dirty_tx(
                connection,
                lifecycle_id,
                "stale-worker-test",
                NOW,
            )
            await connection.execute(
                """
                UPDATE lifecycle_reconcile_queue
                SET leased_by = 'worker-stale',
                    lease_expires_at = now() + interval '60 seconds'
                WHERE lifecycle_id = $1
                """,
                lifecycle_id,
            )
    claimed = (lifecycle_id, NOW)
    assert claimed == (lifecycle_id, NOW)

    authority = LifecycleAuthority(repository, authority_instance="integration-monotonic")
    applied = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-newer-command",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=10),
        ),
        published_at=NOW + timedelta(seconds=10),
    )
    stale_changed = await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id="worker-stale",
    )

    assert applied.result.verdict == CommandVerdict.APPLIED
    assert stale_changed is False
    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.status.value == "waiting"
    assert state.state_version == 2
    assert (
        int(
            await integration_resources.pool.fetchval(
                "SELECT COUNT(*) FROM lifecycle_status_history WHERE lifecycle_id = $1",
                lifecycle_id,
            )
        )
        == 2
    )


@pytest.mark.asyncio
async def test_bootstrap_is_idempotent_but_rejects_binding_or_spec_conflicts(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    grant = CapabilityGrant(
        capability_id=capability_id,
        capability_version=1,
        actor_id=actor_id,
        actions=(CAPABILITY_ACTION,),
        scope=f"lifecycle:{lifecycle_id}",
        issued_at=NOW - timedelta(minutes=1),
        expires_at=None,
        state_version=1,
    )
    spec = default_spec(lifecycle_id, capabilities=(grant,))

    replayed = await repository.create_authority_lifecycle(
        lifecycle_id=lifecycle_id,
        name=f"Test {suffix}",
        repo=repo_name,
        spec=spec,
        created_by=actor_id,
        created_at=NOW + timedelta(hours=1),
    )
    assert replayed.state_version == 1

    with pytest.raises(ConcurrencyConflict, match="bootstrap identity"):
        await repository.create_authority_lifecycle(
            lifecycle_id=lifecycle_id,
            name=f"Different {suffix}",
            repo=repo_name,
            spec=spec,
            created_by=actor_id,
            created_at=NOW,
        )
    with pytest.raises(ConcurrencyConflict, match="bootstrap identity"):
        await repository.create_authority_lifecycle(
            lifecycle_id=lifecycle_id,
            name=f"Test {suffix}",
            repo=repo_name,
            spec=replace(spec, version=2),
            created_by=actor_id,
            created_at=NOW,
        )

    counts = await _counts(integration_resources.pool, lifecycle_id)
    assert counts == {
        "history": 1,
        "commands": 0,
        "outbox": 0,
        "observations": 0,
    }


@pytest.mark.asyncio
async def test_command_published_before_current_authority_time_is_stale_without_mutation(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-command-time")
    applied = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-newer",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=1),
        ),
        published_at=NOW + timedelta(seconds=10, microseconds=987654),
    )
    before = await _counts(integration_resources.pool, lifecycle_id)

    stale = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-older",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=2,
            actor_id=actor_id,
            capability_id=capability_id,
            target="active",
            requested_at=NOW + timedelta(seconds=20),
        ),
        published_at=NOW + timedelta(seconds=5, microseconds=123456),
    )

    assert applied.result.verdict == CommandVerdict.APPLIED
    assert stale.result.verdict == CommandVerdict.STALE
    assert stale.result.reason_code == "REQUESTED_AT_BEFORE_CURRENT_STATE"
    assert stale.result.mutated is False
    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.status.value == "waiting"
    assert state.state_version == 2
    after = await _counts(integration_resources.pool, lifecycle_id)
    assert after["history"] == before["history"]
    assert after["commands"] == before["commands"] + 1
    assert after["outbox"] == before["outbox"] + 1


@pytest.mark.asyncio
async def test_future_requested_at_cannot_advance_or_poison_authority_chronology(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-trusted-time")
    future_requested_at = datetime(2099, 1, 1, tzinfo=timezone.utc)
    first_publication = NOW + timedelta(seconds=1, microseconds=987654)
    second_publication = NOW + timedelta(seconds=2, microseconds=654321)
    first_decision = first_publication.replace(microsecond=987000)
    second_decision = second_publication.replace(microsecond=654000)

    first_envelope = command_envelope(
        suffix=f"{suffix}-future-request",
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        actor_id=actor_id,
        capability_id=capability_id,
        intent_name="set_mode",
        target="manual",
        requested_at=future_requested_at,
    )
    first_envelope["time"] = "2099-01-01T00:00:00Z"
    first_envelope["data"]["requested_at"] = "2099-01-01T00:00:00Z"
    first = await authority.handle_command_envelope(
        first_envelope,
        published_at=first_publication,
    )
    after_first = await repository.get_lifecycle_state(lifecycle_id)

    second = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-real-time-request",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=2,
            actor_id=actor_id,
            capability_id=capability_id,
            intent_name="set_mode",
            target="autonomous",
            requested_at=NOW + timedelta(seconds=2),
        ),
        published_at=second_publication,
    )
    after_second = await repository.get_lifecycle_state(lifecycle_id)

    assert first_envelope["data"]["requested_at"] == "2099-01-01T00:00:00Z"
    assert first.command.requested_at == future_requested_at
    assert first.result.verdict == CommandVerdict.APPLIED
    assert after_first is not None
    assert after_first.last_reconciled_at == first_decision
    assert after_first.last_reconciled_at < future_requested_at
    assert second.result.verdict == CommandVerdict.APPLIED
    assert second.result.observed_state_version == 2
    assert second.result.resulting_state_version == 3
    assert after_second is not None
    assert after_second.mode.value == "autonomous"
    assert after_second.last_reconciled_at == second_decision

    command_times = await integration_resources.pool.fetch(
        """
        SELECT created_at FROM lifecycle_command_results
        WHERE lifecycle_id = $1 ORDER BY id
        """,
        lifecycle_id,
    )
    outbox_times = await integration_resources.pool.fetch(
        """
        SELECT DISTINCT created_at FROM lifecycle_event_outbox
        WHERE lifecycle_id = $1 ORDER BY created_at
        """,
        lifecycle_id,
    )
    assert [row["created_at"] for row in command_times] == [first_decision, second_decision]
    assert [row["created_at"] for row in outbox_times] == [first_decision, second_decision]


@pytest.mark.asyncio
async def test_causal_command_uses_snapshot_precision_after_broker_publication(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-wire-time")
    publication = NOW + timedelta(seconds=10, microseconds=789)
    envelope = repo_task_envelope(
        suffix=f"{suffix}-wire-time",
        observed_at=NOW + timedelta(seconds=9),
        repo=repo_name,
    )
    assert await authority.ingest_repo_task_envelope(
        envelope,
        received_at=publication,
    )
    claimed = await repository.claim_next_reconcile_job_record(f"wire-time-{suffix}")
    assert claimed == (lifecycle_id, publication)
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"wire-time-{suffix}",
    )
    observed = await repository.get_lifecycle_state(lifecycle_id)
    assert observed is not None
    assert observed.last_reconciled_at == NOW + timedelta(seconds=10)

    applied = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-causal-command",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=observed.state_version,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=observed.last_reconciled_at,
        ),
        published_at=NOW + timedelta(seconds=10, microseconds=999),
    )
    assert applied.result.verdict == CommandVerdict.APPLIED
    assert applied.result.observed_state_version == observed.state_version
    assert applied.result.resulting_state_version == observed.state_version + 1


@pytest.mark.asyncio
async def test_global_command_identity_race_is_serialized_across_lifecycles(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    first = await _bootstrap(integration_resources, f"{suffix}a")
    second = await _bootstrap(integration_resources, f"{suffix}b")
    first_repo, first_id, first_name, first_actor, first_capability = first
    second_repo, second_id, second_name, second_actor, second_capability = second
    first_authority = LifecycleAuthority(first_repo, authority_instance="identity-race-a")
    second_authority = LifecycleAuthority(second_repo, authority_instance="identity-race-b")
    shared_suffix = f"{suffix}-shared-identity"
    first_command = command_envelope(
        suffix=shared_suffix,
        lifecycle_id=first_id,
        repo=first_name,
        actor_id=first_actor,
        capability_id=first_capability,
        target="waiting",
        requested_at=NOW + timedelta(seconds=1),
    )
    second_command = command_envelope(
        suffix=shared_suffix,
        lifecycle_id=second_id,
        repo=second_name,
        actor_id=second_actor,
        capability_id=second_capability,
        target="waiting",
        requested_at=NOW + timedelta(seconds=1),
    )

    results = await asyncio.gather(
        first_authority.handle_command_envelope(
            first_command,
            published_at=NOW + timedelta(seconds=1),
        ),
        second_authority.handle_command_envelope(
            second_command,
            published_at=NOW + timedelta(seconds=1),
        ),
    )

    assert sorted(item.result.verdict.value for item in results) == ["applied", "malformed"]
    malformed = next(item for item in results if item.result.verdict == CommandVerdict.MALFORMED)
    assert malformed.result.reason_code == "COMMAND_IDENTITY_REUSED"
    states = [
        await first_repo.get_lifecycle_state(first_id),
        await second_repo.get_lifecycle_state(second_id),
    ]
    assert sorted(state.state_version for state in states if state is not None) == [1, 2]
    assert (
        int(
            await integration_resources.pool.fetchval(
                """
                SELECT COUNT(*) FROM lifecycle_command_results
                WHERE command_event_id = $1::uuid
                """,
                first_command["id"],
            )
        )
        == 1
    )


@pytest.mark.asyncio
async def test_outbox_claiming_preserves_per_lifecycle_sequence_during_backoff(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    database_name = f"lifecycle_order_{suffix}"
    await integration_resources.pool.execute(f'CREATE DATABASE "{database_name}" OWNER lifecycle')
    database_url = (
        "postgresql://lifecycle:lifecycle@127.0.0.1:"
        f"{integration_resources.stack.postgres_port}/{database_name}"
    )
    isolated_pool = await asyncpg.create_pool(database_url, min_size=1, max_size=4)
    try:
        await apply_migrations(isolated_pool)
        isolated_resources = SimpleNamespace(pool=isolated_pool)
        repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
            isolated_resources, suffix
        )
        authority = LifecycleAuthority(
            repository,
            authority_instance="integration-outbox-order",
        )
        await authority.handle_command_envelope(
            command_envelope(
                suffix=f"{suffix}-ordered",
                lifecycle_id=lifecycle_id,
                repo=repo_name,
                actor_id=actor_id,
                capability_id=capability_id,
                target="waiting",
                requested_at=NOW + timedelta(seconds=1),
            ),
            published_at=NOW + timedelta(seconds=1),
        )

        first = await repository.claim_outbox("order-worker-a", batch_size=10)
        assert [event.event_sequence for event in first] == [1]
        assert first[0].id is not None
        await repository.mark_outbox_failed(
            first[0].id,
            "injected outage",
            "order-worker-a",
        )
        assert await repository.claim_outbox("order-worker-b", batch_size=10) == []

        await isolated_pool.execute(
            """
            UPDATE lifecycle_event_outbox
            SET next_attempt_at = now()
            WHERE lifecycle_id = $1 AND event_sequence = 1
            """,
            lifecycle_id,
        )
        retried = await repository.claim_outbox("order-worker-b", batch_size=10)
        assert [event.event_sequence for event in retried] == [1]
        assert retried[0].id is not None
        await repository.mark_outbox_published(retried[0].id, "order-worker-b")
        second = await repository.claim_outbox("order-worker-c", batch_size=10)
        assert [event.event_sequence for event in second] == [2]
    finally:
        await isolated_pool.close()
        await integration_resources.pool.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            database_name,
        )
        await integration_resources.pool.execute(f'DROP DATABASE "{database_name}"')


@pytest.mark.asyncio
async def test_pending_obligation_rejects_command_until_canonical_evidence_unlocks(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-obligation")

    observation = repo_task_envelope(
        suffix=f"{suffix}-work",
        observed_at=NOW + timedelta(seconds=1),
        repo=repo_name,
    )
    assert await authority.ingest_repo_task_envelope(
        observation,
        received_at=NOW + timedelta(seconds=1),
    )
    waiting = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-waiting",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=2),
        ),
        published_at=NOW + timedelta(seconds=2),
    )
    assert waiting.result.verdict == CommandVerdict.APPLIED

    pending = await repository.get_lifecycle_state(lifecycle_id)
    assert pending is not None
    assert pending.status.value == "waiting"
    assert pending.state_version == 2
    assert pending.obligations[0].id == "independent-review"
    assert pending.obligations[0].status.value == "pending"
    active_frontier = next(
        item for item in pending.legal_frontier if item.id == "transition:waiting:active"
    )
    assert active_frontier.allowed is False
    assert active_frontier.reason_code == "PENDING_OBLIGATIONS"

    rejected = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-premature-active",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=2,
            actor_id=actor_id,
            capability_id=capability_id,
            target="active",
            requested_at=NOW + timedelta(seconds=3),
        ),
        published_at=NOW + timedelta(seconds=3),
    )
    assert rejected.result.verdict == CommandVerdict.ILLEGAL
    assert rejected.result.reason_code == "PENDING_OBLIGATIONS"
    assert rejected.result.mutated is False
    unchanged = await repository.get_lifecycle_state(lifecycle_id)
    assert unchanged is not None
    assert unchanged.status.value == "waiting"
    assert unchanged.state_version == 2

    evidence = obligation_evidence_envelope(
        suffix=f"{suffix}-completed",
        completed_at=NOW + timedelta(seconds=4),
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        obligation_instance_id=pending.obligations[0].obligation_instance_id,
    )
    validate_with_bloodbank(evidence)
    assert await authority.ingest_obligation_evidence_envelope(
        evidence,
        received_at=NOW + timedelta(seconds=4),
    )
    assert not await authority.ingest_obligation_evidence_envelope(
        evidence,
        received_at=NOW + timedelta(seconds=5),
    )

    claimed = await repository.claim_next_reconcile_job_record(f"obligation-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=4))
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"obligation-{suffix}",
    )
    unlocked = await repository.get_lifecycle_state(lifecycle_id)
    assert unlocked is not None
    assert unlocked.status.value == "active"
    assert unlocked.state_version == 3
    assert unlocked.obligations == []

    evidence_row = await integration_resources.pool.fetchrow(
        """
        SELECT source_event_id, source_event_type, source_event_subject,
               source_event_source, source_event_producer, payload
        FROM lifecycle_observations
        WHERE lifecycle_id = $1 AND kind = 'obligation_evidence'
        """,
        lifecycle_id,
    )
    assert evidence_row is not None
    assert evidence_row["source_event_id"] == uuid.UUID(evidence["id"])
    assert evidence_row["source_event_type"] == evidence["type"]
    assert evidence_row["source_event_subject"] == evidence["subject"]
    assert evidence_row["source_event_source"] == evidence["source"]
    assert evidence_row["source_event_producer"] == evidence["producer"]
    assert json.loads(evidence_row["payload"]) == evidence["data"]

    snapshots = await integration_resources.pool.fetch(
        """
        SELECT envelope FROM lifecycle_event_outbox
        WHERE lifecycle_id = $1
          AND event_type = 'bloodbank.v1.lifecycle.snapshot.updated'
        ORDER BY event_sequence
        """,
        lifecycle_id,
    )
    assert len(snapshots) == 2
    waiting_snapshot = json.loads(snapshots[0]["envelope"])
    validate_with_bloodbank(waiting_snapshot)
    assert waiting_snapshot["schemaref"].endswith(".v3")
    assert waiting_snapshot["data"]["capabilities"][0]["capability_version"] == 1
    assert waiting_snapshot["data"]["obligations"][0]["status"] == "pending"
    snapshot_frontier = next(
        item
        for item in waiting_snapshot["data"]["legal_frontier"]
        if item["id"] == "transition:waiting:active"
    )
    assert snapshot_frontier["allowed"] is False
    assert snapshot_frontier["reason_code"] == "PENDING_OBLIGATIONS"


@pytest.mark.asyncio
async def test_obligation_occurrence_rejects_history_and_survives_restart_cycle(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="occurrence-before")
    waiting_result = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-waiting",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=1),
        ),
        published_at=NOW + timedelta(seconds=1),
    )
    assert waiting_result.result.verdict == CommandVerdict.APPLIED
    waiting = await repository.get_lifecycle_state(lifecycle_id)
    assert waiting is not None
    first_occurrence = waiting.obligations[0]
    assert first_occurrence.activated_at == NOW + timedelta(seconds=1)

    prepublished_future_claim = obligation_evidence_envelope(
        suffix=f"{suffix}-prepublished-future-claim",
        completed_at=NOW + timedelta(seconds=2),
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        obligation_instance_id=first_occurrence.obligation_instance_id,
    )
    assert await authority.ingest_obligation_evidence_envelope(
        prepublished_future_claim,
        received_at=NOW + timedelta(milliseconds=500),
    )
    claimed = await repository.claim_next_reconcile_job_record(f"prepublished-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=1))
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"prepublished-{suffix}",
    )
    after_prepublished = await repository.get_lifecycle_state(lifecycle_id)
    assert after_prepublished is not None
    assert after_prepublished.status.value == "waiting"
    assert after_prepublished.last_reconciled_at == NOW + timedelta(seconds=1)
    assert after_prepublished.obligations[0].status.value == "pending"

    preactivation = obligation_evidence_envelope(
        suffix=f"{suffix}-preactivation",
        completed_at=NOW,
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        obligation_instance_id=first_occurrence.obligation_instance_id,
    )
    assert await authority.ingest_obligation_evidence_envelope(
        preactivation,
        received_at=NOW + timedelta(seconds=2),
    )
    claimed = await repository.claim_next_reconcile_job_record(f"pre-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=2))
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"pre-{suffix}",
    )
    after_pre = await repository.get_lifecycle_state(lifecycle_id)
    assert after_pre is not None
    assert after_pre.status.value == "waiting"
    assert after_pre.obligations[0].status.value == "pending"
    assert (
        after_pre.obligations[0].obligation_instance_id == first_occurrence.obligation_instance_id
    )

    wrong_occurrence = obligation_evidence_envelope(
        suffix=f"{suffix}-prior",
        completed_at=NOW + timedelta(seconds=2),
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        obligation_instance_id="00000000-0000-4000-8000-000000000099",
    )
    assert await authority.ingest_obligation_evidence_envelope(
        wrong_occurrence,
        received_at=NOW + timedelta(seconds=2),
    )
    claimed = await repository.claim_next_reconcile_job_record(f"prior-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=2))
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"prior-{suffix}",
    )
    after_wrong = await repository.get_lifecycle_state(lifecycle_id)
    assert after_wrong is not None
    assert after_wrong.status.value == "waiting"
    assert after_wrong.obligations[0].status.value == "pending"

    valid = obligation_evidence_envelope(
        suffix=f"{suffix}-valid",
        completed_at=NOW + timedelta(seconds=3),
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        obligation_instance_id=first_occurrence.obligation_instance_id,
    )
    assert await authority.ingest_obligation_evidence_envelope(
        valid,
        received_at=NOW + timedelta(seconds=3),
    )
    claimed = await repository.claim_next_reconcile_job_record(f"valid-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=3))
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"valid-{suffix}",
    )
    active = await repository.get_lifecycle_state(lifecycle_id)
    assert active is not None
    assert active.status.value == "active"
    assert active.obligations == []

    repeated_result = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-waiting-again",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=active.state_version,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=4),
        ),
        published_at=NOW + timedelta(seconds=4),
    )
    assert repeated_result.result.verdict == CommandVerdict.APPLIED
    repeated = await repository.get_lifecycle_state(lifecycle_id)
    assert repeated is not None
    second_occurrence = repeated.obligations[0]
    assert second_occurrence.obligation_instance_id != first_occurrence.obligation_instance_id
    assert second_occurrence.activated_at == NOW + timedelta(seconds=4)

    restarted_repository = LifecycleRepository(integration_resources.pool)
    restarted_authority = LifecycleAuthority(
        restarted_repository,
        authority_instance="occurrence-after",
    )
    async with integration_resources.pool.acquire() as connection:
        async with connection.transaction():
            await restarted_repository.mark_dirty_tx(
                connection,
                lifecycle_id,
                "restart-occurrence-proof",
                NOW + timedelta(seconds=5),
            )
    claimed = await restarted_repository.claim_next_reconcile_job_record(f"restart-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=5))
    assert await restarted_authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"restart-{suffix}",
    )
    restarted = await restarted_repository.get_lifecycle_state(lifecycle_id)
    assert restarted is not None
    assert restarted.status.value == "waiting"
    assert (
        restarted.obligations[0].obligation_instance_id == second_occurrence.obligation_instance_id
    )
    assert restarted.obligations[0].activated_at == second_occurrence.activated_at

    history_before = int(
        await integration_resources.pool.fetchval(
            "SELECT COUNT(*) FROM lifecycle_status_history WHERE lifecycle_id = $1",
            lifecycle_id,
        )
    )
    async with integration_resources.pool.acquire() as connection:
        async with connection.transaction():
            await restarted_repository.mark_dirty_tx(
                connection,
                lifecycle_id,
                "stable-reconcile-proof",
                NOW + timedelta(seconds=6),
            )
    claimed = await restarted_repository.claim_next_reconcile_job_record(f"stable-{suffix}")
    assert claimed == (lifecycle_id, NOW + timedelta(seconds=6))
    assert not await restarted_authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"stable-{suffix}",
    )
    history_after = int(
        await integration_resources.pool.fetchval(
            "SELECT COUNT(*) FROM lifecycle_status_history WHERE lifecycle_id = $1",
            lifecycle_id,
        )
    )
    assert history_after == history_before


@pytest.mark.asyncio
async def test_obligation_occurrence_migration_uses_persisted_activation_history(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="occurrence-migration")
    await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-waiting",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=1),
        ),
        published_at=NOW + timedelta(seconds=1),
    )
    before = await repository.get_lifecycle_state(lifecycle_id)
    assert before is not None
    first_occurrence_id = before.obligations[0].obligation_instance_id
    mode_result = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-same-status-mode",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=before.state_version,
            actor_id=actor_id,
            capability_id=capability_id,
            intent_name="set_mode",
            target="autonomous",
            requested_at=NOW + timedelta(seconds=3),
        ),
        published_at=NOW + timedelta(seconds=3),
    )
    assert mode_result.result.verdict == CommandVerdict.APPLIED
    same_status = await repository.get_lifecycle_state(lifecycle_id)
    assert same_status is not None
    assert same_status.status.value == "waiting"
    assert same_status.state_version == before.state_version + 1

    evidence_between_versions = obligation_evidence_envelope(
        suffix=f"{suffix}-between-v2-v3",
        completed_at=NOW + timedelta(seconds=2),
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        obligation_instance_id=first_occurrence_id,
    )
    assert await authority.ingest_obligation_evidence_envelope(
        evidence_between_versions,
        received_at=NOW + timedelta(seconds=2),
    )
    await integration_resources.pool.execute(
        """
        UPDATE lifecycle_state
        SET obligations = (
            SELECT jsonb_agg(
                jsonb_set(
                    item,
                    '{activated_at}',
                    to_jsonb(TIMESTAMPTZ '2026-07-18 17:00:03+00'),
                    false
                )
            )
            FROM jsonb_array_elements(obligations) AS item
        )
        WHERE lifecycle_id = $1
        """,
        lifecycle_id,
    )
    await integration_resources.pool.execute(
        "DELETE FROM lifecycle_schema_migrations WHERE version = '0005'"
    )

    status = await apply_migrations(integration_resources.pool)
    assert status.current is True
    migrated = await repository.get_lifecycle_state(lifecycle_id)
    assert migrated is not None
    assert migrated.obligations[0].activated_at == before.last_reconciled_at
    assert migrated.obligations[0].obligation_instance_id == first_occurrence_id
    await integration_resources.pool.execute(
        "UPDATE lifecycle_reconcile_queue SET priority = 100000 WHERE lifecycle_id = $1",
        lifecycle_id,
    )
    claimed = await repository.claim_next_reconcile_job_record(f"migration-{suffix}")
    assert claimed == (lifecycle_id, same_status.last_reconciled_at)
    assert await authority.reconcile_claimed(
        lifecycle_id=lifecycle_id,
        as_of=claimed[1],
        worker_id=f"migration-{suffix}",
    )
    unlocked = await repository.get_lifecycle_state(lifecycle_id)
    assert unlocked is not None
    assert unlocked.status.value == "active"
    assert unlocked.state_version == same_status.state_version + 1
    repeated_result = await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-repeated-waiting",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            expected_state_version=unlocked.state_version,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=4),
        ),
        published_at=NOW + timedelta(seconds=4),
    )
    assert repeated_result.result.verdict == CommandVerdict.APPLIED
    repeated = await repository.get_lifecycle_state(lifecycle_id)
    assert repeated is not None
    assert repeated.obligations[0].obligation_instance_id != first_occurrence_id
    restarted = await LifecycleRepository(integration_resources.pool).get_lifecycle_state(
        lifecycle_id
    )
    assert restarted == repeated
    assert (await apply_migrations(integration_resources.pool)).current is True


@pytest.mark.asyncio
async def test_capability_projection_migration_uses_exact_specification_version(
    integration_resources,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, *_ = await _bootstrap(
        integration_resources,
        suffix,
        capability_version=7,
    )
    await integration_resources.pool.execute(
        """
        UPDATE lifecycle_state
        SET capabilities = (
            SELECT jsonb_agg(item - 'capability_version')
            FROM jsonb_array_elements(capabilities) AS item
        )
        WHERE lifecycle_id = $1
        """,
        lifecycle_id,
    )
    await integration_resources.pool.execute(
        "DELETE FROM lifecycle_schema_migrations WHERE version = '0003'"
    )

    status = await apply_migrations(integration_resources.pool)
    assert status.current is True
    state = await repository.get_lifecycle_state(lifecycle_id)
    assert state is not None
    assert state.capabilities[0].capability_version == 7


@pytest.mark.asyncio
async def test_migrations_repeat_and_authority_ledgers_are_append_only(
    integration_resources,
) -> None:
    first_status = await apply_migrations(integration_resources.pool)
    second_status = await apply_migrations(integration_resources.pool)
    assert first_status.current is True
    assert second_status == first_status

    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="integration-append-only")
    await authority.handle_command_envelope(
        command_envelope(
            suffix=f"{suffix}-append-only",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            intent_name="set_mode",
            target="manual",
            requested_at=NOW + timedelta(seconds=1),
        ),
        published_at=NOW + timedelta(seconds=1),
    )

    statements = (
        "UPDATE lifecycle_specs SET created_by = 'mutated' WHERE lifecycle_id = $1",
        "DELETE FROM lifecycle_status_history WHERE lifecycle_id = $1",
        "UPDATE lifecycle_command_results SET reason_code = 'mutated' WHERE lifecycle_id = $1",
    )
    for statement in statements:
        with pytest.raises(asyncpg.PostgresError, match="append-only"):
            await integration_resources.pool.execute(statement, lifecycle_id)

    counts = await _counts(integration_resources.pool, lifecycle_id)
    assert counts["history"] == 2
    assert counts["commands"] == 1
