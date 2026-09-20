from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kafka_reliability.core.clock import ManualClock
from kafka_reliability.core.errors import (
    ConfigurationError,
    PermanentError,
    ProducerError,
    TransientError,
    UnclassifiedError,
)
from kafka_reliability.core.headers import (
    DLQ_ATTEMPTS,
    DLQ_ERROR_MESSAGE,
    DLQ_ERROR_TYPE,
    DLQ_FIRST_FAILED_AT,
    DLQ_REPLAY_COUNT,
    DLQ_SOURCE_OFFSET,
    DLQ_TRACE_ID,
    REPLAY_ID,
)
from kafka_reliability.core.message import Record
from kafka_reliability.producers import InMemoryProducer
from kafka_reliability.replay.dlq import DlqRouter, typed_errors

TS = datetime(2026, 9, 5, 10, 14, 22, tzinfo=UTC)


def rec(headers=(), value=b"\x00\xffpayload", key=b"order-8812") -> Record:
    return Record("orders", 3, 184203, key, value, tuple(headers), TS)


def router(producer=None, **kw) -> tuple[DlqRouter, InMemoryProducer]:
    p = producer or InMemoryProducer()
    kw.setdefault("topic", "orders.dlq")
    kw.setdefault("classify", typed_errors)
    return DlqRouter(producer=p, consumer_group="billing", clock=ManualClock(TS), **kw), p


async def test_key_value_and_headers_are_preserved_byte_for_byte_with_diagnostics_added():
    r, p = router()
    original = (
        ("traceparent", b"00-abc123-def-01"),
        ("x-binary", b"\x00\x01\xfe"),
        (REPLAY_ID, b"r1"),
    )
    await r.route(rec(original), ValueError("bad total"), attempts=3)

    (m,) = p.sent
    assert (m.topic, m.key, m.value) == ("orders.dlq", b"order-8812", b"\x00\xffpayload")
    for name, value in original:
        assert m.headers[name] == value  # untouched, including binary and replay headers
    assert m.headers["x-dlq-source-topic"] == b"orders"
    assert m.headers["x-dlq-source-partition"] == b"3"
    assert m.headers[DLQ_SOURCE_OFFSET] == b"184203"
    assert m.headers["x-dlq-source-timestamp"] == b"2026-09-05T10:14:22Z"
    assert m.headers["x-dlq-consumer-group"] == b"billing"
    assert (m.headers[DLQ_ERROR_TYPE], m.headers[DLQ_ERROR_MESSAGE]) == (
        b"ValueError",
        b"bad total",
    )
    assert (
        m.headers[DLQ_ATTEMPTS] == b"3"
        and m.headers[DLQ_FIRST_FAILED_AT] == b"2026-09-05T10:14:22Z"
    )
    assert m.headers[DLQ_TRACE_ID] == b"abc123"


async def test_redead_lettering_replaces_the_diagnosis_but_keeps_the_replay_counter():
    r, p = router()
    old = [
        (DLQ_ERROR_TYPE, b"OldError"),
        (DLQ_REPLAY_COUNT, b"2"),
        (DLQ_FIRST_FAILED_AT, b"2026-09-01T00:00:00Z"),
    ]
    await r.route(rec(old), KeyError("k"))
    h = p.sent[0].headers
    assert h[DLQ_ERROR_TYPE] == b"KeyError"  # fresh diagnosis
    assert h[DLQ_REPLAY_COUNT] == b"2"  # the poison threshold still works
    assert h[DLQ_FIRST_FAILED_AT] == b"2026-09-01T00:00:00Z"  # first failure stays first


async def test_long_error_text_is_truncated_on_a_utf8_boundary():
    r, p = router(max_error_bytes=10)
    await r.route(rec(), ValueError("ñ" * 20))  # 2 bytes each
    text = p.sent[0].headers[DLQ_ERROR_MESSAGE]
    assert len(text) <= 10 and text.decode("utf-8") == "ñ" * 5


async def test_error_message_can_be_omitted():
    r, p = router(include_error_message=False)
    await r.route(rec(), ValueError("customer card 4111..."))
    assert DLQ_ERROR_MESSAGE not in p.sent[0].headers and DLQ_ERROR_TYPE in p.sent[0].headers


async def test_topic_callable_supports_per_source_and_per_group_topologies():
    r, p = router(topic=lambda rec_: f"{rec_.topic}.dlq.billing")
    await r.route(rec(), ValueError())
    assert p.sent[0].topic == "orders.dlq.billing"
    bad, _ = router(topic=lambda rec_: "")
    with pytest.raises(ConfigurationError):
        bad.build(rec(), ValueError())


async def test_a_failed_dlq_produce_surfaces_so_the_offset_is_not_committed():
    p = InMemoryProducer()
    p.fail_next()
    r, _ = router(p)
    with pytest.raises(ProducerError):
        await r.route(rec(), ValueError())


async def test_the_full_stack_trace_goes_to_the_log_keyed_by_trace_id(caplog):
    r, _ = router()
    try:
        raise ValueError("boom")
    except ValueError as exc:
        with caplog.at_level("ERROR", logger="kafka_reliability.replay.dlq"):
            await r.route(rec([("traceparent", b"00-tr4ce-x-01")]), exc)
    assert "trace_id=tr4ce" in caplog.text and "Traceback" in caplog.text


# --- classification ---------------------------------------------------------------------------


def test_classification_is_required_with_no_default():
    with pytest.raises(TypeError):
        DlqRouter(producer=InMemoryProducer(), topic="d", consumer_group="g")  # type: ignore[call-arg]
    with pytest.raises(ConfigurationError, match="no default policy"):
        DlqRouter(producer=InMemoryProducer(), topic="d", consumer_group="g", classify=None)  # type: ignore[arg-type]


def test_typed_errors_classify_both_ways_and_refuse_to_guess():
    r, _ = router()
    assert r.should_dead_letter(PermanentError("malformed")) is True
    assert r.should_dead_letter(TransientError("gateway down")) is False
    with pytest.raises(UnclassifiedError) as info:
        r.should_dead_letter(ConnectionError("?"))
    assert isinstance(info.value.__cause__, ConnectionError)


def test_a_predicate_is_the_alternative_policy():
    r, _ = router(classify=lambda e: not isinstance(e, ConnectionError))
    assert r.should_dead_letter(ValueError()) is True
    assert r.should_dead_letter(ConnectionError()) is False
