"""core.headers — the acceptance criterion for issue #17 is that an unused
header is inert: a record without them reads back as absent, nothing else
changes."""

from __future__ import annotations

from datetime import UTC, datetime

from kafka_reliability.core import headers
from kafka_reliability.core.message import Record, get_header

EXPECTED = {
    "EVENT_ID": "x-event-id",
    "REPLAY_ID": "x-replay-id",
    "REPLAY_AT": "x-replay-at",
    "DLQ_SOURCE_TOPIC": "x-dlq-source-topic",
    "DLQ_SOURCE_PARTITION": "x-dlq-source-partition",
    "DLQ_SOURCE_OFFSET": "x-dlq-source-offset",
    "DLQ_SOURCE_TIMESTAMP": "x-dlq-source-timestamp",
    "DLQ_CONSUMER_GROUP": "x-dlq-consumer-group",
    "DLQ_ERROR_TYPE": "x-dlq-error-type",
    "DLQ_ERROR_MESSAGE": "x-dlq-error-message",
    "DLQ_ATTEMPTS": "x-dlq-attempts",
    "DLQ_FIRST_FAILED_AT": "x-dlq-first-failed-at",
    "DLQ_TRACE_ID": "x-dlq-trace-id",
    "DLQ_REPLAY_COUNT": "x-dlq-replay-count",
}


def _public_constants() -> dict[str, object]:
    return {n: v for n, v in vars(headers).items() if n.isupper()}


def test_constants_match_documented_names():
    assert _public_constants() == EXPECTED


def test_constants_are_lowercase_x_prefixed_strings():
    for value in EXPECTED.values():
        assert isinstance(value, str)
        assert value == value.lower()
        assert value.startswith("x-")


def test_constant_values_are_unique():
    values = list(_public_constants().values())
    assert len(values) == len(set(values))


def test_all_lists_every_constant():
    assert sorted(headers.__all__) == sorted(EXPECTED)


def test_unused_headers_are_inert():
    record = Record(
        topic="orders",
        partition=0,
        offset=1,
        key=b"k",
        value=b"v",
        headers=(("traceparent", b"00-abc"),),
        timestamp=datetime.now(UTC),
    )
    for name in EXPECTED.values():
        assert get_header(record.headers, name) is None
