"""Record, OutgoingMessage, and get_header — the acceptance criteria for
issue #16 are: both dataclasses are frozen and slotted, bytes in and bytes
out, and a header can be read back off a Record via the shared helper."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest

from kafka_reliability.core.message import OutgoingMessage, Record, get_header


def _record(headers: tuple[tuple[str, bytes], ...] = ()) -> Record:
    return Record(
        topic="orders",
        partition=0,
        offset=42,
        key=b"order-1",
        value=b"payload",
        headers=headers,
        timestamp=datetime.now(UTC),
    )


def test_record_is_frozen():
    record = _record()
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.offset = 43  # type: ignore[misc]


def test_record_is_slotted():
    record = _record()
    assert not hasattr(record, "__dict__")


def test_outgoing_message_is_frozen():
    message = OutgoingMessage(topic="orders", value=b"payload")
    with pytest.raises(dataclasses.FrozenInstanceError):
        message.value = b"other"  # type: ignore[misc]


def test_outgoing_message_is_slotted():
    message = OutgoingMessage(topic="orders", value=b"payload")
    assert not hasattr(message, "__dict__")


def test_outgoing_message_defaults_key_and_headers():
    message = OutgoingMessage(topic="orders", value=b"payload")
    assert message.key is None
    assert message.headers == {}


def test_get_header_returns_value_for_present_header():
    record = _record(headers=(("event-id", b"abc123"),))
    assert get_header(record.headers, "event-id") == b"abc123"


def test_get_header_returns_none_for_absent_header():
    record = _record(headers=(("event-id", b"abc123"),))
    assert get_header(record.headers, "missing") is None


def test_get_header_returns_first_match_when_name_repeats():
    headers = (("trace", b"first"), ("trace", b"second"))
    assert get_header(headers, "trace") == b"first"
