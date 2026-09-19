"""Shared conformance suite for `Producer` implementations.

Subclass `ProducerConformance` (name the subclass `Test...` so pytest collects
it) and implement `make_harness`. The same tests then run against the
in-memory producer and both adapters, so they cannot drift apart."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import pytest

from kafka_reliability.core.errors import ProducerError
from kafka_reliability.core.message import OutgoingMessage
from kafka_reliability.producers.port import Producer

Delivered = tuple[str, bytes | None, bytes, dict[str, bytes]]


@dataclass
class Harness:
    producer: Producer
    # Make the next send fail at the (fake) client, with an exception.
    inject_failure: Callable[[], None]
    # Everything the (fake) client actually received: topic, key, value, headers.
    delivered: Callable[[], list[Delivered]]
    cleanup: Callable[[], Awaitable[None]] | None = field(default=None)


class ProducerConformance:
    async def make_harness(self) -> Harness:
        raise NotImplementedError

    @pytest.fixture
    async def harness(self):
        h = await self.make_harness()
        yield h
        if h.cleanup is not None:
            await h.cleanup()

    async def test_satisfies_the_protocol(self, harness: Harness):
        assert isinstance(harness.producer, Producer)

    async def test_send_delivers_topic_key_value_and_headers(self, harness: Harness):
        message = OutgoingMessage(
            topic="orders", value=b"payload", key=b"k1", headers={"event-id": b"abc"}
        )
        await harness.producer.send(message)
        assert harness.delivered() == [("orders", b"k1", b"payload", {"event-id": b"abc"})]

    async def test_send_without_key_or_headers(self, harness: Harness):
        await harness.producer.send(OutgoingMessage(topic="t", value=b"v"))
        assert harness.delivered() == [("t", None, b"v", {})]

    async def test_sends_keep_their_order(self, harness: Harness):
        for i in range(5):
            await harness.producer.send(OutgoingMessage(topic="t", value=str(i).encode()))
        assert [d[2] for d in harness.delivered()] == [b"0", b"1", b"2", b"3", b"4"]

    async def test_failed_send_raises_producer_error_with_cause(self, harness: Harness):
        harness.inject_failure()
        with pytest.raises(ProducerError) as excinfo:
            await harness.producer.send(OutgoingMessage(topic="t", value=b"v"))
        assert excinfo.value.__cause__ is not None or "injected" in str(excinfo.value)
        assert harness.delivered() == []

    async def test_producer_is_usable_after_a_failed_send(self, harness: Harness):
        harness.inject_failure()
        with pytest.raises(ProducerError):
            await harness.producer.send(OutgoingMessage(topic="t", value=b"lost"))
        await harness.producer.send(OutgoingMessage(topic="t", value=b"ok"))
        assert [d[2] for d in harness.delivered()] == [b"ok"]

    async def test_flush_with_nothing_in_flight(self, harness: Harness):
        await harness.producer.flush()
        await harness.producer.flush(timeout=1.0)

    async def test_flush_after_send(self, harness: Harness):
        await harness.producer.send(OutgoingMessage(topic="t", value=b"v"))
        await harness.producer.flush(timeout=1.0)
        assert len(harness.delivered()) == 1
