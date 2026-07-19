"""Focused tests for raw ACK-reply trusted publication time parsing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from bloodbank import (
    COMMAND_STREAM,
    EVENT_STREAM,
    JetStreamRuntime,
    RuntimeMetrics,
    _parse_ack_reply_publication_time,
    _trusted_publication_time,
)
from contracts import parse_timestamp

REPRODUCED_TIMESTAMP_NS = 1784431974431017921
# Canonical RFC3339 stream parsing truncates the reproduced nanoseconds to
# 2026-07-19T03:32:54.431017Z; nats-py's float-derived metadata.timestamp
# rounds them up to 2026-07-19T03:32:54.431018Z and broke the live
# trusted-publication predicate.
EXACT_PUBLICATION = datetime(2026, 7, 19, 3, 32, 54, 431017, tzinfo=timezone.utc)
ROUNDED_METADATA_TIMESTAMP = datetime(2026, 7, 19, 3, 32, 54, 431018, tzinfo=timezone.utc)

V1_ACK_REPLY = f"$JS.ACK.{EVENT_STREAM}.lifecycle-evidence-v1.1.42.7.{REPRODUCED_TIMESTAMP_NS}.9"
V2_ACK_REPLY_11_TOKENS = (
    f"$JS.ACK.delo.acchash.{COMMAND_STREAM}.lifecycle-commands-v1.1.42.7."
    f"{REPRODUCED_TIMESTAMP_NS}.9"
)
V2_ACK_REPLY_12_TOKENS = f"{V2_ACK_REPLY_11_TOKENS}.r2d2"


class _FakeMessage:
    def __init__(
        self,
        *,
        reply: str,
        metadata_stream: str,
        data: bytes = b"{}",
    ) -> None:
        self.reply = reply
        self.data = data
        self._metadata = SimpleNamespace(
            stream=metadata_stream,
            timestamp=ROUNDED_METADATA_TIMESTAMP,
        )
        self.acked = False
        self.nak_delays: list[int] = []
        self.terminated = False

    @property
    def metadata(self) -> Any:
        return self._metadata

    async def ack_sync(self) -> None:
        self.acked = True

    async def nak(self, *, delay: int) -> None:
        self.nak_delays.append(delay)

    async def term(self) -> None:
        self.terminated = True


class _AuthorityMustNotRun:
    async def handle_command_envelope(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("authority must not handle an untrusted command")

    async def ingest_obligation_evidence_envelope(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("authority must not ingest untrusted evidence")


def test_v1_ack_reply_parses_reproduced_nanoseconds_exactly() -> None:
    published_at = _parse_ack_reply_publication_time(
        V1_ACK_REPLY,
        expected_stream=EVENT_STREAM,
    )

    assert published_at == EXACT_PUBLICATION
    assert published_at == parse_timestamp("2026-07-19T03:32:54.431017921Z", "test")
    assert published_at != ROUNDED_METADATA_TIMESTAMP
    assert published_at.tzinfo is not None
    assert published_at.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    "reply",
    [V2_ACK_REPLY_11_TOKENS, V2_ACK_REPLY_12_TOKENS],
    ids=["11-tokens", "12-tokens"],
)
def test_v2_ack_reply_parses_reproduced_nanoseconds_exactly(reply: str) -> None:
    published_at = _parse_ack_reply_publication_time(
        reply,
        expected_stream=COMMAND_STREAM,
    )

    assert published_at == EXACT_PUBLICATION
    assert published_at == parse_timestamp("2026-07-19T03:32:54.431017921Z", "test")
    assert published_at != ROUNDED_METADATA_TIMESTAMP
    assert published_at.tzinfo is not None
    assert published_at.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    ("reply", "expected_stream"),
    [
        (V1_ACK_REPLY, EVENT_STREAM),
        (V2_ACK_REPLY_12_TOKENS, COMMAND_STREAM),
    ],
    ids=["v1", "v2"],
)
def test_trusted_publication_time_ignores_rounded_metadata_timestamp(
    reply: str,
    expected_stream: str,
) -> None:
    message = _FakeMessage(reply=reply, metadata_stream=expected_stream)

    published_at = _trusted_publication_time(message, expected_stream=expected_stream)  # type: ignore[arg-type]

    assert published_at == EXACT_PUBLICATION
    assert message.metadata.timestamp == ROUNDED_METADATA_TIMESTAMP
    assert published_at != message.metadata.timestamp


@pytest.mark.parametrize(
    "reply",
    [
        "",
        "_INBOX.delo.xyz",
        "bloodbank.evt.v1.repo.task.recorded",
        "$JS.API.STREAM.INFO.BLOODBANK_EVENTS",
        f"$JS.ACK.{EVENT_STREAM}.consumer.1.2.3.4",
        f"$JS.ACK.delo.acchash.{EVENT_STREAM}.consumer.1.2.3.4",
        f"$JS.ACK.{EVENT_STREAM}.consumer.1.2.3.abc.9",
        f"$JS.ACK.{EVENT_STREAM}.consumer.1.2.3.-1.9",
        f"$JS.ACK.{EVENT_STREAM}.consumer.1.2.3..9",
    ],
    ids=[
        "missing-reply",
        "plain-inbox",
        "plain-subject",
        "js-api-not-ack",
        "too-few-tokens",
        "v2-too-few-tokens",
        "non-integer-timestamp",
        "negative-timestamp",
        "empty-timestamp",
    ],
)
def test_ack_reply_failures_fail_closed(reply: str) -> None:
    message = _FakeMessage(reply=reply, metadata_stream=EVENT_STREAM)

    with pytest.raises(RuntimeError):
        _trusted_publication_time(message, expected_stream=EVENT_STREAM)  # type: ignore[arg-type]


def test_wrong_raw_stream_fails_closed_even_when_metadata_matches() -> None:
    reply = f"$JS.ACK.BLOODBANK_OTHER.consumer.1.42.7.{REPRODUCED_TIMESTAMP_NS}.9"
    message = _FakeMessage(reply=reply, metadata_stream=EVENT_STREAM)

    with pytest.raises(RuntimeError, match="BLOODBANK_OTHER"):
        _trusted_publication_time(message, expected_stream=EVENT_STREAM)  # type: ignore[arg-type]


def test_raw_metadata_stream_mismatch_fails_closed() -> None:
    message = _FakeMessage(reply=V1_ACK_REPLY, metadata_stream="BLOODBANK_OTHER")

    with pytest.raises(RuntimeError, match="BLOODBANK_OTHER"):
        _trusted_publication_time(message, expected_stream=EVENT_STREAM)  # type: ignore[arg-type]


async def test_command_handler_passes_exact_publication_time_to_authority() -> None:
    class RecordingAuthority:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def handle_command_envelope(
            self,
            envelope: Any,
            *,
            published_at: datetime,
        ) -> Any:
            self.calls.append({"envelope": envelope, "published_at": published_at})
            return SimpleNamespace(result=SimpleNamespace(verdict=SimpleNamespace(value="applied")))

    authority = RecordingAuthority()
    transport = SimpleNamespace(metrics=RuntimeMetrics())
    runtime = JetStreamRuntime(
        repository=SimpleNamespace(),
        authority=authority,
        transport=transport,
    )
    message = _FakeMessage(reply=V2_ACK_REPLY_12_TOKENS, metadata_stream=COMMAND_STREAM)

    await runtime.handle_command_message(message)  # type: ignore[arg-type]

    assert [call["published_at"] for call in authority.calls] == [EXACT_PUBLICATION]
    assert message.acked is True
    assert message.nak_delays == []
    assert message.terminated is False
    assert transport.metrics.counters == {"command_applied": 1}


async def test_evidence_handler_passes_exact_publication_time_to_authority() -> None:
    class RecordingAuthority:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def ingest_obligation_evidence_envelope(
            self,
            envelope: Any,
            *,
            received_at: datetime,
        ) -> bool:
            self.calls.append({"envelope": envelope, "received_at": received_at})
            return True

    authority = RecordingAuthority()
    transport = SimpleNamespace(metrics=RuntimeMetrics())
    runtime = JetStreamRuntime(
        repository=SimpleNamespace(),
        authority=authority,
        transport=transport,
    )
    message = _FakeMessage(reply=V1_ACK_REPLY, metadata_stream=EVENT_STREAM)

    await runtime.handle_evidence_message(message)  # type: ignore[arg-type]

    assert [call["received_at"] for call in authority.calls] == [EXACT_PUBLICATION]
    assert message.acked is True
    assert message.nak_delays == []
    assert message.terminated is False
    assert transport.metrics.counters == {"evidence_recorded": 1}


@pytest.mark.parametrize(
    "reply",
    [
        "",
        "_INBOX.delo.xyz",
        f"$JS.ACK.BLOODBANK_OTHER.consumer.1.42.7.{REPRODUCED_TIMESTAMP_NS}.9",
    ],
    ids=["missing-reply", "non-js-reply", "wrong-raw-stream"],
)
async def test_command_fail_closed_replies_nak_before_authority(reply: str) -> None:
    transport = SimpleNamespace(metrics=RuntimeMetrics())
    runtime = JetStreamRuntime(
        repository=SimpleNamespace(),
        authority=_AuthorityMustNotRun(),
        transport=transport,
    )
    message = _FakeMessage(reply=reply, metadata_stream=COMMAND_STREAM)

    await runtime.handle_command_message(message)  # type: ignore[arg-type]

    assert message.nak_delays == [1]
    assert message.acked is False
    assert message.terminated is False
    assert transport.metrics.counters == {"command_retry": 1}


async def test_command_raw_metadata_stream_mismatch_naks_before_authority() -> None:
    transport = SimpleNamespace(metrics=RuntimeMetrics())
    runtime = JetStreamRuntime(
        repository=SimpleNamespace(),
        authority=_AuthorityMustNotRun(),
        transport=transport,
    )
    reply = f"$JS.ACK.{COMMAND_STREAM}.consumer.1.42.7.{REPRODUCED_TIMESTAMP_NS}.9"
    message = _FakeMessage(reply=reply, metadata_stream="BLOODBANK_OTHER")

    await runtime.handle_command_message(message)  # type: ignore[arg-type]

    assert message.nak_delays == [1]
    assert message.acked is False
    assert message.terminated is False
    assert transport.metrics.counters == {"command_retry": 1}


@pytest.mark.parametrize(
    "reply",
    [
        "",
        "_INBOX.delo.xyz",
        f"$JS.ACK.BLOODBANK_OTHER.consumer.1.42.7.{REPRODUCED_TIMESTAMP_NS}.9",
    ],
    ids=["missing-reply", "non-js-reply", "wrong-raw-stream"],
)
async def test_evidence_fail_closed_replies_nak_before_authority(reply: str) -> None:
    transport = SimpleNamespace(metrics=RuntimeMetrics())
    runtime = JetStreamRuntime(
        repository=SimpleNamespace(),
        authority=_AuthorityMustNotRun(),
        transport=transport,
    )
    message = _FakeMessage(reply=reply, metadata_stream=EVENT_STREAM)

    await runtime.handle_evidence_message(message)  # type: ignore[arg-type]

    assert message.nak_delays == [1]
    assert message.acked is False
    assert message.terminated is False
    assert transport.metrics.counters == {"evidence_retry": 1}


async def test_evidence_raw_metadata_stream_mismatch_naks_before_authority() -> None:
    transport = SimpleNamespace(metrics=RuntimeMetrics())
    runtime = JetStreamRuntime(
        repository=SimpleNamespace(),
        authority=_AuthorityMustNotRun(),
        transport=transport,
    )
    message = _FakeMessage(reply=V1_ACK_REPLY, metadata_stream="BLOODBANK_OTHER")

    await runtime.handle_evidence_message(message)  # type: ignore[arg-type]

    assert message.nak_delays == [1]
    assert message.acked is False
    assert message.terminated is False
    assert transport.metrics.counters == {"evidence_retry": 1}
