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
from bloodbank import BloodbankTransport, JetStreamRuntime
from contracts import canonical_json
from db.repository import LifecycleRepository
from models import CapabilityGrant, CommandVerdict
from specification import CAPABILITY_ACTION, default_spec
from tests.factories import command_envelope, repo_task_envelope
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


async def _transport(resources, suffix: str) -> BloodbankTransport:
    transport = BloodbankTransport(
        servers=[resources.stack.nats_url],
        client_name=f"lifecycle-it-{suffix}",
        command_durable=f"lifecycle-it-command-{suffix}",
        observation_durable=f"lifecycle-it-observation-{suffix}",
    )
    await transport.connect()
    ready, reason = await transport.ready()
    assert ready, reason
    return transport


async def _delete_test_consumers(resources, suffix: str) -> None:
    for stream, durable in (
        ("BLOODBANK_COMMANDS", f"lifecycle-it-command-{suffix}"),
        ("BLOODBANK_EVENTS", f"lifecycle-it-observation-{suffix}"),
    ):
        try:
            await resources.js.delete_consumer(stream, durable)
        except Exception:
            pass


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
        clock=lambda: NOW + timedelta(seconds=2),
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
        await runtime.handle_command_message(command_messages[0])

        assert await runtime.publish_outbox_once(batch_size=20) == 4
        assert await repository.outbox_pending_count() == 0
        state = await repository.get_lifecycle_state(lifecycle_id)
        assert state is not None
        assert state.status.value == "waiting"
        assert state.state_version == 2

        events, replies = await _capture_for_lifecycle(
            integration_resources,
            lifecycle_id,
            suffix,
        )
        assert sorted(event["type"] for event in events) == [
            "bloodbank.v1.lifecycle.observation.recorded",
            "bloodbank.v1.lifecycle.snapshot.updated",
            "bloodbank.v1.lifecycle.status.updated",
        ]
        assert len(replies) == 1
        assert replies[0]["kind"] == "reply"
        assert replies[0]["data"]["verdict"] == "applied"
        assert len({event["id"] for event in events}) == 3
        for envelope in [observation, command, *events, *replies]:
            validate_with_bloodbank(envelope)
    finally:
        await transport.close()
        await _delete_test_consumers(integration_resources, suffix)


@pytest.mark.asyncio
async def test_publisher_outage_commit_retry_and_restart_catchup(
    integration_resources,
) -> None:
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
        applied = await authority.handle_command_envelope(command)
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
            SELECT publish_attempts, published_at
            FROM lifecycle_event_outbox WHERE lifecycle_id = $1 ORDER BY id
            """,
            lifecycle_id,
        )
        assert len(failed_rows) == 3
        assert [row["publish_attempts"] for row in failed_rows] == [1, 0, 0]
        assert all(row["published_at"] is None for row in failed_rows)
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

        retry = await authority.handle_command_envelope(command)
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
        for envelope in [*events, *replies]:
            validate_with_bloodbank(envelope)
    finally:
        await restarted_transport.close()
        await _delete_test_consumers(integration_resources, suffix)
