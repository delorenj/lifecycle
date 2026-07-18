"""Canonical Bloodbank JetStream consumers and transactional-outbox delivery."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import nats
import structlog
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.client import JetStreamContext
from nats.js.errors import FetchTimeoutError

from authority import LifecycleAuthority, UnaddressableCommand
from contracts import ContractError, envelope_bytes
from db.repository import LifecycleRepository
from models import OutboxEvent


logger = structlog.get_logger()
UTC = timezone.utc
COMMAND_STREAM = "BLOODBANK_COMMANDS"
EVENT_STREAM = "BLOODBANK_EVENTS"
COMMAND_SUBJECT = "bloodbank.cmd.v1.lifecycle.intent.submit"
OBSERVATION_SUBJECT = "bloodbank.evt.v1.repo.task.recorded"
EVIDENCE_SUBJECT = "bloodbank.evt.v1.lifecycle.obligation_evidence.submitted"


def _trusted_publication_time(message: Msg, *, expected_stream: str) -> datetime:
    """Return the immutable JetStream storage timestamp for a durable message.

    A local consumer clock is not evidence of when a replayed event entered the
    canonical stream. Missing or inconsistent JetStream metadata is therefore
    an operational retry condition rather than a poison-message verdict.
    """

    metadata = message.metadata
    if metadata.stream != expected_stream:
        raise RuntimeError(
            f"JetStream metadata stream {metadata.stream!r} does not equal {expected_stream!r}"
        )
    published_at = metadata.timestamp
    if (
        not isinstance(published_at, datetime)
        or published_at.tzinfo is None
        or published_at.utcoffset() is None
    ):
        raise RuntimeError("JetStream metadata is missing a trusted publication timestamp")
    return published_at.astimezone(UTC)


@dataclass
class RuntimeMetrics:
    counters: dict[str, int] = field(default_factory=dict)

    def increment(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def render_prometheus(self) -> str:
        lines = [
            "# HELP lifecycle_runtime_events_total Lifecycle runtime event counters.",
            "# TYPE lifecycle_runtime_events_total counter",
        ]
        for name, value in sorted(self.counters.items()):
            lines.append(f'lifecycle_runtime_events_total{{event="{name}"}} {value}')
        return "\n".join(lines) + "\n"


class BloodbankTransport:
    def __init__(
        self,
        *,
        servers: list[str],
        client_name: str,
        command_durable: str = "lifecycle-authority-commands-v1",
        observation_durable: str = "lifecycle-authority-repo-task-recorded-v1",
        evidence_durable: str = "lifecycle-authority-obligation-evidence-v1",
        metrics: RuntimeMetrics | None = None,
    ) -> None:
        self.servers = servers
        self.client_name = client_name
        self.command_durable = command_durable
        self.observation_durable = observation_durable
        self.evidence_durable = evidence_durable
        self.metrics = metrics or RuntimeMetrics()
        self.nc: NATS | None = None
        self.js: JetStreamContext | None = None
        self.command_subscription: JetStreamContext.PullSubscription | None = None
        self.observation_subscription: JetStreamContext.PullSubscription | None = None
        self.evidence_subscription: JetStreamContext.PullSubscription | None = None

    @property
    def connected(self) -> bool:
        return self.nc is not None and self.nc.is_connected

    @property
    def consumers_bound(self) -> bool:
        return all(
            subscription is not None
            for subscription in (
                self.command_subscription,
                self.observation_subscription,
                self.evidence_subscription,
            )
        )

    async def connect(self) -> None:
        if self.connected and self.consumers_bound:
            return
        if self.nc is not None:
            await self.close()

        async def disconnected_cb() -> None:
            self.metrics.increment("nats_disconnected")
            logger.warning("nats_disconnected")

        async def reconnected_cb() -> None:
            self.metrics.increment("nats_reconnected")
            logger.info(
                "nats_reconnected",
                server=str(self.nc.connected_url) if self.nc is not None else "unknown",
            )

        async def error_cb(error: Exception) -> None:
            self.metrics.increment("nats_async_error")
            logger.warning("nats_async_error", error=str(error))

        try:
            self.nc = await nats.connect(
                servers=self.servers,
                name=self.client_name,
                connect_timeout=2,
                allow_reconnect=True,
                max_reconnect_attempts=-1,
                reconnect_time_wait=1,
                disconnected_cb=disconnected_cb,
                reconnected_cb=reconnected_cb,
                error_cb=error_cb,
            )
            self.js = self.nc.jetstream()
            self.command_subscription = await self.js.pull_subscribe(
                COMMAND_SUBJECT,
                durable=self.command_durable,
                stream=COMMAND_STREAM,
                config=ConsumerConfig(
                    durable_name=self.command_durable,
                    filter_subject=COMMAND_SUBJECT,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=30,
                    max_deliver=-1,
                    max_ack_pending=256,
                ),
            )
            self.observation_subscription = await self.js.pull_subscribe(
                OBSERVATION_SUBJECT,
                durable=self.observation_durable,
                stream=EVENT_STREAM,
                config=ConsumerConfig(
                    durable_name=self.observation_durable,
                    filter_subject=OBSERVATION_SUBJECT,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=30,
                    max_deliver=-1,
                    max_ack_pending=256,
                ),
            )
            self.evidence_subscription = await self.js.pull_subscribe(
                EVIDENCE_SUBJECT,
                durable=self.evidence_durable,
                stream=EVENT_STREAM,
                config=ConsumerConfig(
                    durable_name=self.evidence_durable,
                    filter_subject=EVIDENCE_SUBJECT,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=30,
                    max_deliver=-1,
                    max_ack_pending=256,
                ),
            )
        except Exception:
            self.metrics.increment("nats_binding_failed")
            await self.close()
            raise
        self.metrics.increment("nats_connected")

    async def close(self) -> None:
        if self.nc is not None and not self.nc.is_closed:
            if self.nc.is_connected:
                try:
                    await self.nc.flush(timeout=2)
                except Exception:
                    pass
            await self.nc.close()
        self.nc = None
        self.js = None
        self.command_subscription = None
        self.observation_subscription = None
        self.evidence_subscription = None

    async def ready(self) -> tuple[bool, str]:
        if not self.connected or self.js is None:
            return False, "nats_disconnected"
        if (
            self.command_subscription is None
            or self.observation_subscription is None
            or self.evidence_subscription is None
        ):
            return False, "consumers_unbound"
        try:
            await self.js.stream_info(COMMAND_STREAM)
            await self.js.stream_info(EVENT_STREAM)
            await self.command_subscription.consumer_info()
            await self.observation_subscription.consumer_info()
            await self.evidence_subscription.consumer_info()
            return True, "ready"
        except Exception as exc:
            return False, f"stream_unavailable:{type(exc).__name__}"

    async def publish_outbox(self, event: OutboxEvent) -> None:
        if self.js is None or not self.connected:
            raise RuntimeError("Bloodbank JetStream is unavailable")
        if not event.subject or not event.event_id or not event.envelope:
            raise ValueError("outbox event is missing canonical publication identity")
        await self.js.publish(
            event.subject,
            envelope_bytes(event.envelope),
            headers={"Nats-Msg-Id": event.event_id},
            timeout=5,
        )


class JetStreamRuntime:
    def __init__(
        self,
        *,
        repository: LifecycleRepository,
        authority: LifecycleAuthority,
        transport: BloodbankTransport,
        worker_id: str | None = None,
    ) -> None:
        self.repository = repository
        self.authority = authority
        self.transport = transport
        self.worker_id = worker_id or f"outbox-{uuid.uuid4().hex[:12]}"

    async def handle_command_message(self, message: Msg) -> None:
        try:
            envelope = json.loads(message.data)
            result = await self.authority.handle_command_envelope(envelope)
            await message.ack_sync()
            self.transport.metrics.increment(f"command_{result.result.verdict.value}")
        except (json.JSONDecodeError, UnicodeDecodeError, UnaddressableCommand) as exc:
            self.transport.metrics.increment("command_poison")
            logger.warning("command_poison", error=str(exc))
            await message.term()
        except Exception as exc:
            self.transport.metrics.increment("command_retry")
            logger.exception("command_processing_failed", error=str(exc))
            await message.nak(delay=1)

    async def handle_observation_message(self, message: Msg) -> None:
        try:
            envelope = json.loads(message.data)
            inserted = await self.authority.ingest_repo_task_envelope(
                envelope,
                received_at=_trusted_publication_time(
                    message,
                    expected_stream=EVENT_STREAM,
                ),
            )
            await message.ack_sync()
            self.transport.metrics.increment(
                "observation_recorded" if inserted else "observation_ignored"
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ContractError) as exc:
            self.transport.metrics.increment("observation_malformed")
            logger.warning("observation_malformed", error=str(exc))
            await message.term()
        except Exception as exc:
            self.transport.metrics.increment("observation_retry")
            logger.exception("observation_processing_failed", error=str(exc))
            await message.nak(delay=1)

    async def handle_evidence_message(self, message: Msg) -> None:
        try:
            envelope = json.loads(message.data)
            inserted = await self.authority.ingest_obligation_evidence_envelope(
                envelope,
                received_at=_trusted_publication_time(
                    message,
                    expected_stream=EVENT_STREAM,
                ),
            )
            await message.ack_sync()
            self.transport.metrics.increment(
                "evidence_recorded" if inserted else "evidence_ignored"
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ContractError) as exc:
            self.transport.metrics.increment("evidence_malformed")
            logger.warning("evidence_malformed", error=str(exc))
            await message.term()
        except Exception as exc:
            self.transport.metrics.increment("evidence_retry")
            logger.exception("evidence_processing_failed", error=str(exc))
            await message.nak(delay=1)

    async def publish_outbox_once(self, batch_size: int = 100) -> int:
        published = 0
        examined = 0
        while examined < batch_size:
            events = await self.repository.claim_outbox(
                self.worker_id,
                batch_size=batch_size - examined,
                lease_seconds=30,
            )
            if not events:
                break
            examined += len(events)
            for event in events:
                if event.id is None:
                    continue
                try:
                    await self.transport.publish_outbox(event)
                    await self.repository.mark_outbox_published(event.id, self.worker_id)
                    published += 1
                    self.transport.metrics.increment("outbox_published")
                except Exception as exc:
                    await self.repository.mark_outbox_failed(
                        event.id,
                        str(exc),
                        self.worker_id,
                    )
                    self.transport.metrics.increment("outbox_retry")
                    logger.warning(
                        "outbox_publish_failed",
                        outbox_id=event.id,
                        event_id=event.event_id,
                        subject=event.subject,
                        error=str(exc),
                    )
        return published

    async def _pull_loop(
        self,
        *,
        subscription_name: str,
        handler: Callable[[Msg], Any],
        stop: asyncio.Event,
    ) -> None:
        while not stop.is_set():
            subscription = getattr(self.transport, subscription_name)
            if subscription is None:
                await _wait_or_stop(stop, 0.5)
                continue
            try:
                messages = await subscription.fetch(batch=20, timeout=1)
            except (FetchTimeoutError, NatsTimeoutError):
                continue
            except Exception as exc:
                logger.warning("consumer_fetch_failed", error=str(exc))
                setattr(self.transport, subscription_name, None)
                await _wait_or_stop(stop, 1)
                continue
            for message in messages:
                await handler(message)

    async def command_loop(self, stop: asyncio.Event) -> None:
        await self._pull_loop(
            subscription_name="command_subscription",
            handler=self.handle_command_message,
            stop=stop,
        )

    async def observation_loop(self, stop: asyncio.Event) -> None:
        await self._pull_loop(
            subscription_name="observation_subscription",
            handler=self.handle_observation_message,
            stop=stop,
        )

    async def evidence_loop(self, stop: asyncio.Event) -> None:
        await self._pull_loop(
            subscription_name="evidence_subscription",
            handler=self.handle_evidence_message,
            stop=stop,
        )

    async def outbox_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            published = await self.publish_outbox_once()
            if published == 0:
                await _wait_or_stop(stop, 0.5)


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return


__all__ = [
    "BloodbankTransport",
    "JetStreamRuntime",
    "RuntimeMetrics",
]
