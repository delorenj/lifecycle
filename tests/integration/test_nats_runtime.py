from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import uuid

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import FetchTimeoutError
import pytest

from authority import LifecycleAuthority
from bloodbank import (
    COMMAND_STREAM,
    EVENT_STREAM,
    BloodbankTransport,
    JetStreamRuntime,
    _trusted_publication_time,
)
from contracts import canonical_json, parse_timestamp
from db.repository import LifecycleRepository
from models import CapabilityGrant, CommandVerdict
from specification import CAPABILITY_ACTION, default_spec
from tests.factories import command_envelope, obligation_evidence_envelope, repo_task_envelope
from tests.schema_validation import validate_with_bloodbank


pytestmark = pytest.mark.integration
NOW = datetime(2026, 7, 18, 18, 0, tzinfo=timezone.utc)


async def _bootstrap(resources, suffix: str):
    lifecycle_id = f"lc_nats_{suffix}"
    repo_name = f"delorenj/nats-{suffix}"
    actor_id = f"agent:nats:{suffix}"
    capability_id = f"cap-nats-{suffix}"
    repository = LifecycleRepository(resources.pool)
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
    await repository.create_authority_lifecycle(
        lifecycle_id=lifecycle_id,
        name=f"NATS {suffix}",
        repo=repo_name,
        spec=default_spec(lifecycle_id, capabilities=(grant,)),
        created_by=actor_id,
        created_at=NOW,
    )
    return repository, lifecycle_id, repo_name, actor_id, capability_id


async def _assert_fresh_authority_database(resources) -> None:
    assert await resources.pool.fetchval("SELECT current_database()") == resources.database_name
    assert resources.database_name != "lifecycle"
    assert await resources.pool.fetchval("SELECT COUNT(*) FROM lifecycle_event_outbox") == 0


async def _transport(resources, suffix: str) -> BloodbankTransport:
    transport = BloodbankTransport(
        servers=[resources.stack.nats_url],
        client_name=f"lifecycle-it-{suffix}",
        command_durable=f"lifecycle-it-command-{suffix}",
        observation_durable=f"lifecycle-it-observation-{suffix}",
        evidence_durable=f"lifecycle-it-evidence-{suffix}",
    )
    await transport.connect()
    ready, reason = await transport.ready()
    assert ready, reason
    return transport


async def _delete_test_consumers(resources, suffix: str) -> None:
    for stream, durable in (
        ("BLOODBANK_COMMANDS", f"lifecycle-it-command-{suffix}"),
        ("BLOODBANK_EVENTS", f"lifecycle-it-observation-{suffix}"),
        ("BLOODBANK_EVENTS", f"lifecycle-it-evidence-{suffix}"),
    ):
        try:
            await resources.js.delete_consumer(stream, durable)
        except Exception:
            pass


async def _fetch_for_lifecycle(subscription, lifecycle_id: str):
    for _ in range(100):
        messages = await subscription.fetch(batch=1, timeout=2)
        message = messages[0]
        envelope = json.loads(message.data)
        if envelope.get("data", {}).get("lifecycle_id") == lifecycle_id:
            return message
        await message.ack_sync()
    raise AssertionError(f"consumer did not reach lifecycle {lifecycle_id}")


async def _claim_for_lifecycle(resources, repository, lifecycle_id: str, worker_id: str):
    await resources.pool.execute(
        "UPDATE lifecycle_reconcile_queue SET priority = 100000 WHERE lifecycle_id = $1",
        lifecycle_id,
    )
    claimed = await repository.claim_next_reconcile_job_record(worker_id)
    assert claimed is not None
    assert claimed[0] == lifecycle_id
    return claimed


async def _capture_for_lifecycle(resources, lifecycle_id: str, suffix: str):
    capture_nc = await nats.connect(resources.stack.nats_url)
    capture_js = capture_nc.jetstream()
    event_durable = f"lifecycle-it-capture-events-{suffix}"
    reply_durable = f"lifecycle-it-capture-replies-{suffix}"
    events = []
    replies = []
    try:
        event_sub = await capture_js.pull_subscribe(
            "bloodbank.evt.v1.lifecycle.>",
            durable=event_durable,
            stream="BLOODBANK_EVENTS",
        )
        reply_sub = await capture_js.pull_subscribe(
            "bloodbank.rpy.v1.lifecycle.intent.submit",
            durable=reply_durable,
            stream="BLOODBANK_COMMANDS",
        )
        for subscription, destination in ((event_sub, events), (reply_sub, replies)):
            while True:
                try:
                    messages = await subscription.fetch(batch=20, timeout=0.5)
                except (FetchTimeoutError, NatsTimeoutError):
                    break
                for message in messages:
                    envelope = json.loads(message.data)
                    if envelope["data"]["lifecycle_id"] == lifecycle_id:
                        destination.append(envelope)
                    await message.ack_sync()
    finally:
        for stream, durable in (
            ("BLOODBANK_EVENTS", event_durable),
            ("BLOODBANK_COMMANDS", reply_durable),
        ):
            try:
                await capture_js.delete_consumer(stream, durable)
            except Exception:
                pass
        await capture_nc.close()
    return events, replies


@pytest.mark.asyncio
async def test_real_canonical_observation_command_reply_and_outbox_flow(
    integration_resources,
) -> None:
    await _assert_fresh_authority_database(integration_resources)
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="nats-e2e")
    transport = await _transport(integration_resources, suffix)
    runtime = JetStreamRuntime(
        repository=repository,
        authority=authority,
        transport=transport,
        worker_id=f"publisher-{suffix}",
    )
    try:
        observation = repo_task_envelope(
            suffix=suffix,
            observed_at=NOW + timedelta(seconds=1),
            repo=repo_name,
        )
        await integration_resources.js.publish(
            observation["subject"],
            canonical_json(observation).encode(),
            headers={"Nats-Msg-Id": observation["id"]},
        )
        observation_messages = await transport.observation_subscription.fetch(
            batch=1,
            timeout=2,
        )
        await runtime.handle_observation_message(observation_messages[0])

        command = command_envelope(
            suffix=f"{suffix}-command",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=NOW + timedelta(seconds=3),
        )
        await integration_resources.js.publish(
            command["subject"],
            canonical_json(command).encode(),
            headers={"Nats-Msg-Id": command["id"]},
        )
        command_messages = await transport.command_subscription.fetch(batch=1, timeout=2)
        trusted_command_publication = _trusted_publication_time(
            command_messages[0],
            expected_stream=COMMAND_STREAM,
        )
        canonical_command_publication = trusted_command_publication.replace(
            microsecond=(trusted_command_publication.microsecond // 1000) * 1000
        )
        await runtime.handle_command_message(command_messages[0])

        assert await runtime.publish_outbox_once(batch_size=20) == 4
        assert await repository.outbox_pending_count() == 0
        state = await repository.get_lifecycle_state(lifecycle_id)
        assert state is not None
        assert state.status.value == "waiting"
        assert state.state_version == 2
        assert state.last_reconciled_at == canonical_command_publication
        assert state.last_reconciled_at.microsecond % 1000 == 0
        assert state.obligations[0].status.value == "pending"
        waiting_frontier = next(
            item for item in state.legal_frontier if item.id == "transition:waiting:active"
        )
        assert waiting_frontier.allowed is False
        assert waiting_frontier.reason_code == "PENDING_OBLIGATIONS"

        evidence = obligation_evidence_envelope(
            suffix=f"{suffix}-completion",
            completed_at=state.obligations[0].activated_at,
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            obligation_instance_id=state.obligations[0].obligation_instance_id,
        )
        await integration_resources.js.publish(
            evidence["subject"],
            canonical_json(evidence).encode(),
            headers={"Nats-Msg-Id": evidence["id"]},
        )
        evidence_messages = await transport.evidence_subscription.fetch(batch=1, timeout=2)
        trusted_publication = _trusted_publication_time(
            evidence_messages[0],
            expected_stream=EVENT_STREAM,
        )
        await runtime.handle_evidence_message(evidence_messages[0])
        claimed = await repository.claim_next_reconcile_job_record(f"evidence-{suffix}")
        assert claimed == (lifecycle_id, trusted_publication)
        assert await authority.reconcile_claimed(
            lifecycle_id=lifecycle_id,
            as_of=claimed[1],
            worker_id=f"evidence-{suffix}",
        )
        assert await runtime.publish_outbox_once(batch_size=20) == 3
        state = await repository.get_lifecycle_state(lifecycle_id)
        assert state is not None
        assert state.status.value == "active"
        assert state.state_version == 3

        events, replies = await _capture_for_lifecycle(
            integration_resources,
            lifecycle_id,
            suffix,
        )
        assert sorted(event["type"] for event in events) == [
            "bloodbank.v1.lifecycle.obligation_evidence.submitted",
            "bloodbank.v1.lifecycle.observation.recorded",
            "bloodbank.v1.lifecycle.observation.recorded",
            "bloodbank.v1.lifecycle.snapshot.updated",
            "bloodbank.v1.lifecycle.snapshot.updated",
            "bloodbank.v1.lifecycle.status.updated",
            "bloodbank.v1.lifecycle.status.updated",
        ]
        assert len(replies) == 1
        assert replies[0]["kind"] == "reply"
        assert replies[0]["data"]["verdict"] == "applied"
        assert len({event["id"] for event in events}) == 7
        snapshots = [
            event for event in events if event["type"] == "bloodbank.v1.lifecycle.snapshot.updated"
        ]
        assert [snapshot["schemaref"] for snapshot in snapshots] == [
            "bloodbank.v1.lifecycle.snapshot.updated.v3",
            "bloodbank.v1.lifecycle.snapshot.updated.v3",
        ]
        assert snapshots[0]["data"]["state"]["status"] == "waiting"
        assert snapshots[0]["data"]["obligations"][0]["status"] == "pending"
        assert (
            next(
                item
                for item in snapshots[0]["data"]["legal_frontier"]
                if item["id"] == "transition:waiting:active"
            )["allowed"]
            is False
        )
        assert snapshots[0]["data"]["capabilities"][0]["capability_version"] == 1
        assert snapshots[1]["data"]["state"]["status"] == "active"
        for envelope in [observation, command, evidence, *events, *replies]:
            validate_with_bloodbank(envelope)
    finally:
        await transport.close()
        await _delete_test_consumers(integration_resources, suffix)


@pytest.mark.asyncio
async def test_nats_obligation_occurrence_rejects_old_evidence_then_unlocks(
    integration_resources,
) -> None:
    await _assert_fresh_authority_database(integration_resources)
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="nats-occurrence")
    transport = await _transport(integration_resources, suffix)
    runtime = JetStreamRuntime(
        repository=repository,
        authority=authority,
        transport=transport,
        worker_id=f"occurrence-{suffix}",
    )
    try:
        initial = await repository.get_lifecycle_state(lifecycle_id)
        assert initial is not None
        occurrence_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                "lifecycle-obligation-occurrence:"
                f"{lifecycle_id}:independent-review:waiting:{initial.state_version + 1}",
            )
        )
        claimed_completion = datetime.now(timezone.utc) + timedelta(seconds=1.5)
        prepublished = obligation_evidence_envelope(
            suffix=f"{suffix}-prepublished",
            completed_at=claimed_completion,
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            obligation_instance_id=occurrence_id,
        )
        claimed_completion = parse_timestamp(prepublished["time"], "time")
        validate_with_bloodbank(prepublished)
        await integration_resources.js.publish(
            prepublished["subject"],
            canonical_json(prepublished).encode(),
            headers={"Nats-Msg-Id": prepublished["id"]},
        )
        prepublished_message = await _fetch_for_lifecycle(
            transport.evidence_subscription,
            lifecycle_id,
        )
        trusted_publication = _trusted_publication_time(
            prepublished_message,
            expected_stream=EVENT_STREAM,
        )
        planned_activation = trusted_publication + timedelta(seconds=0.5)
        await runtime.handle_evidence_message(prepublished_message)

        # Reproduce the production race: the worker drains future-claimed
        # evidence before the command arrives.  Broker publication time is the
        # authority boundary, so the producer's future completion cannot move
        # last_reconciled_at past the subsequent activation command.
        claimed = await _claim_for_lifecycle(
            integration_resources,
            repository,
            lifecycle_id,
            f"occurrence-{suffix}-prepublished-ingress",
        )
        assert claimed == (lifecycle_id, trusted_publication)
        assert not await authority.reconcile_claimed(
            lifecycle_id=lifecycle_id,
            as_of=claimed[1],
            worker_id=f"occurrence-{suffix}-prepublished-ingress",
        )
        after_ingress = await repository.get_lifecycle_state(lifecycle_id)
        assert after_ingress is not None
        assert after_ingress.last_reconciled_at < claimed_completion

        delay = (planned_activation - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)
        command = command_envelope(
            suffix=f"{suffix}-waiting",
            lifecycle_id=lifecycle_id,
            repo=repo_name,
            actor_id=actor_id,
            capability_id=capability_id,
            target="waiting",
            requested_at=claimed_completion,
        )
        await integration_resources.js.publish(
            command["subject"],
            canonical_json(command).encode(),
            headers={"Nats-Msg-Id": command["id"]},
        )
        message = await _fetch_for_lifecycle(transport.command_subscription, lifecycle_id)
        command_publication = _trusted_publication_time(
            message,
            expected_stream=COMMAND_STREAM,
        )
        activation = command_publication.replace(
            microsecond=(command_publication.microsecond // 1000) * 1000
        )
        assert trusted_publication < activation < claimed_completion
        await runtime.handle_command_message(message)
        waiting = await repository.get_lifecycle_state(lifecycle_id)
        assert waiting is not None
        assert waiting.last_reconciled_at == activation
        assert waiting.obligations[0].obligation_instance_id == occurrence_id
        assert waiting.obligations[0].activated_at == activation
        assert waiting.obligations[0].status.value == "pending"

        async with integration_resources.pool.acquire() as connection:
            async with connection.transaction():
                await repository.mark_dirty_tx(
                    connection,
                    lifecycle_id,
                    "prepublished-future-claim-sweep",
                    claimed_completion,
                )
        claimed = await _claim_for_lifecycle(
            integration_resources,
            repository,
            lifecycle_id,
            f"occurrence-{suffix}-prepublished",
        )
        assert claimed == (lifecycle_id, claimed_completion)
        assert await authority.reconcile_claimed(
            lifecycle_id=lifecycle_id,
            as_of=claimed[1],
            worker_id=f"occurrence-{suffix}-prepublished",
        )
        after_replay = await repository.get_lifecycle_state(lifecycle_id)
        assert after_replay is not None
        assert after_replay.status.value == "waiting"
        assert after_replay.obligations[0].status.value == "pending"
        persisted_publication = await integration_resources.pool.fetchval(
            "SELECT received_at FROM lifecycle_observations WHERE source_event_id = $1",
            uuid.UUID(prepublished["id"]),
        )
        assert persisted_publication == trusted_publication

        evidence_cases = (
            obligation_evidence_envelope(
                suffix=f"{suffix}-prior",
                completed_at=datetime.now(timezone.utc),
                lifecycle_id=lifecycle_id,
                repo=repo_name,
                obligation_instance_id="00000000-0000-4000-8000-000000000099",
            ),
            obligation_evidence_envelope(
                suffix=f"{suffix}-valid",
                completed_at=datetime.now(timezone.utc),
                lifecycle_id=lifecycle_id,
                repo=repo_name,
                obligation_instance_id=occurrence_id,
            ),
        )
        expected_statuses = ("waiting", "active")
        for index, (evidence, expected_status) in enumerate(
            zip(evidence_cases, expected_statuses, strict=True),
            start=1,
        ):
            validate_with_bloodbank(evidence)
            await integration_resources.js.publish(
                evidence["subject"],
                canonical_json(evidence).encode(),
                headers={"Nats-Msg-Id": evidence["id"]},
            )
            message = await _fetch_for_lifecycle(transport.evidence_subscription, lifecycle_id)
            await runtime.handle_evidence_message(message)
            claimed = await _claim_for_lifecycle(
                integration_resources,
                repository,
                lifecycle_id,
                f"occurrence-{suffix}-{index}",
            )
            assert claimed is not None
            assert await authority.reconcile_claimed(
                lifecycle_id=lifecycle_id,
                as_of=claimed[1],
                worker_id=f"occurrence-{suffix}-{index}",
            )
            state = await repository.get_lifecycle_state(lifecycle_id)
            assert state is not None
            assert state.status.value == expected_status
            if expected_status == "waiting":
                assert state.obligations[0].status.value == "pending"
                assert state.obligations[0].obligation_instance_id == occurrence_id

        assert await runtime.publish_outbox_once(batch_size=30) == 11
        assert await repository.outbox_pending_count() == 0
        events, replies = await _capture_for_lifecycle(
            integration_resources,
            lifecycle_id,
            suffix,
        )
        snapshots = [
            event for event in events if event["type"] == "bloodbank.v1.lifecycle.snapshot.updated"
        ]
        assert len(snapshots) == 4
        assert all(snapshot["schemaref"].endswith(".v3") for snapshot in snapshots)
        assert snapshots[0]["data"]["obligations"][0]["obligation_instance_id"] == (occurrence_id)
        assert len(replies) == 1
        for envelope in [*events, *replies]:
            validate_with_bloodbank(envelope)
    finally:
        await transport.close()
        await _delete_test_consumers(integration_resources, suffix)


@pytest.mark.asyncio
async def test_publisher_outage_commit_retry_and_restart_catchup(
    integration_resources,
) -> None:
    await _assert_fresh_authority_database(integration_resources)
    suffix = uuid.uuid4().hex[:8]
    repository, lifecycle_id, repo_name, actor_id, capability_id = await _bootstrap(
        integration_resources, suffix
    )
    authority = LifecycleAuthority(repository, authority_instance="nats-outage")
    transport = await _transport(integration_resources, suffix)
    runtime = JetStreamRuntime(
        repository=repository,
        authority=authority,
        transport=transport,
        worker_id=f"publisher-before-{suffix}",
    )
    command = command_envelope(
        suffix=f"{suffix}-outage",
        lifecycle_id=lifecycle_id,
        repo=repo_name,
        actor_id=actor_id,
        capability_id=capability_id,
        target="waiting",
        requested_at=NOW + timedelta(seconds=10),
    )

    integration_resources.stack.stop_nats()
    try:
        for _ in range(50):
            if not transport.connected:
                break
            await asyncio.sleep(0.1)
        applied = await authority.handle_command_envelope(
            command,
            published_at=NOW + timedelta(seconds=10),
        )
        assert applied.result.verdict == CommandVerdict.APPLIED
        assert await runtime.publish_outbox_once(batch_size=20) == 0

        state = await repository.get_lifecycle_state(lifecycle_id)
        assert state is not None
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
        assert (
            int(
                await integration_resources.pool.fetchval(
                    "SELECT COUNT(*) FROM lifecycle_command_results WHERE lifecycle_id = $1",
                    lifecycle_id,
                )
            )
            == 1
        )
        failed_rows = await integration_resources.pool.fetch(
            """
            SELECT id, event_id, event_sequence, event_type, subject,
                   aggregate_version, publish_attempts, published_at
            FROM lifecycle_event_outbox WHERE lifecycle_id = $1 ORDER BY id
            """,
            lifecycle_id,
        )
        assert len(failed_rows) == 3
        assert [row["publish_attempts"] for row in failed_rows] == [1, 0, 0]
        assert all(row["published_at"] is None for row in failed_rows)
        pending_outbox_ids = [row["id"] for row in failed_rows]
        pending_event_ids = [str(row["event_id"]) for row in failed_rows]
        pending_sequences = [row["event_sequence"] for row in failed_rows]
        assert pending_sequences == sorted(pending_sequences)
        assert pending_sequences == list(
            range(pending_sequences[0], pending_sequences[0] + len(pending_sequences))
        )
        assert [row["aggregate_version"] for row in failed_rows] == [2, 2, 2]
    finally:
        integration_resources.stack.start_nats()

    for _ in range(100):
        if transport.connected:
            break
        await asyncio.sleep(0.1)
    assert transport.connected
    await transport.close()

    restarted_transport = await _transport(integration_resources, suffix)
    restarted_runtime = JetStreamRuntime(
        repository=LifecycleRepository(integration_resources.pool),
        authority=LifecycleAuthority(
            LifecycleRepository(integration_resources.pool),
            authority_instance="nats-restarted",
        ),
        transport=restarted_transport,
        worker_id=f"publisher-after-{suffix}",
    )
    try:
        await asyncio.sleep(1.2)
        assert await restarted_runtime.publish_outbox_once(batch_size=20) == 3
        assert await repository.outbox_pending_count() == 0
        drained_rows = await integration_resources.pool.fetch(
            """
            SELECT id, event_id, event_sequence, published_at
            FROM lifecycle_event_outbox
            WHERE lifecycle_id = $1 AND id = ANY($2::bigint[])
            ORDER BY id
            """,
            lifecycle_id,
            pending_outbox_ids,
        )
        assert [row["id"] for row in drained_rows] == pending_outbox_ids
        assert [str(row["event_id"]) for row in drained_rows] == pending_event_ids
        assert [row["event_sequence"] for row in drained_rows] == pending_sequences
        assert all(row["published_at"] is not None for row in drained_rows)

        retry = await authority.handle_command_envelope(
            command,
            published_at=NOW + timedelta(seconds=10),
        )
        assert retry.result.verdict == CommandVerdict.IDEMPOTENT
        assert await restarted_runtime.publish_outbox_once(batch_size=20) == 1
        assert await restarted_runtime.publish_outbox_once(batch_size=20) == 0

        state = await repository.get_lifecycle_state(lifecycle_id)
        assert state is not None
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
        events, replies = await _capture_for_lifecycle(
            integration_resources,
            lifecycle_id,
            suffix,
        )
        assert [event["type"] for event in events].count(
            "bloodbank.v1.lifecycle.snapshot.updated"
        ) == 1
        assert [event["type"] for event in events].count(
            "bloodbank.v1.lifecycle.status.updated"
        ) == 1
        assert sorted(reply["data"]["verdict"] for reply in replies) == [
            "applied",
            "idempotent",
        ]
        assert len({event["id"] for event in events}) == len(events)
        observed_ids = {event["id"] for event in events} | {reply["id"] for reply in replies}
        assert set(pending_event_ids).issubset(observed_ids)
        authority_sequences = [
            event["data"]["publication"]["event_sequence"]
            for event in events
            if event["type"]
            in {
                "bloodbank.v1.lifecycle.snapshot.updated",
                "bloodbank.v1.lifecycle.status.updated",
            }
        ]
        assert authority_sequences == sorted(authority_sequences)
        for envelope in [*events, *replies]:
            validate_with_bloodbank(envelope)
    finally:
        await restarted_transport.close()
        await _delete_test_consumers(integration_resources, suffix)
