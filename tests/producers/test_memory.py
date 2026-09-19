"""InMemoryProducer: the public test double, plus the conformance suite and the
protocol acceptance test for user-supplied objects."""

from __future__ import annotations

import pytest

from kafka_reliability.core.errors import KafkaReliabilityError, ProducerError
from kafka_reliability.core.message import OutgoingMessage
from kafka_reliability.producers import InMemoryProducer, Producer

from .conformance import Harness, ProducerConformance


class TestInMemoryConformance(ProducerConformance):
    async def make_harness(self) -> Harness:
        producer = InMemoryProducer()
        return Harness(
            producer=producer,
            inject_failure=lambda: producer.fail_next(cause=RuntimeError("injected")),
            delivered=lambda: [(m.topic, m.key, m.value, dict(m.headers)) for m in producer.sent],
        )


class ThirdPartyPublisher:
    """Stands in for e.g. a FastStream publisher wrapped by the user: it knows
    nothing about this library except the two method names."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def send(self, message: OutgoingMessage) -> None:
        self.calls.append(message.topic)

    async def flush(self, timeout: float | None = None) -> None:
        return None


def test_arbitrary_object_with_two_methods_is_a_producer():
    producer: Producer = ThirdPartyPublisher()  # also checked by mypy
    assert isinstance(producer, Producer)


def test_object_missing_flush_is_not_a_producer():
    class OnlySend:
        async def send(self, message: OutgoingMessage) -> None: ...

    assert not isinstance(OnlySend(), Producer)


async def test_records_and_asserts_messages():
    producer = InMemoryProducer()
    await producer.send(OutgoingMessage("a", b"1", key=b"k", headers={"h": b"x"}))
    await producer.send(OutgoingMessage("b", b"2"))

    assert [m.topic for m in producer.sent] == ["a", "b"]
    assert [m.value for m in producer.messages_for("a")] == [b"1"]
    assert producer.assert_sent(topic="a", key=b"k", headers={"h": b"x"}).value == b"1"
    with pytest.raises(AssertionError):
        producer.assert_sent(topic="a", value=b"nope")


async def test_flush_is_counted_and_clear_resets():
    producer = InMemoryProducer()
    await producer.send(OutgoingMessage("a", b"1"))
    await producer.flush()
    assert producer.flush_count == 1
    producer.clear()
    assert producer.sent == () and producer.flush_count == 0


async def test_fail_next_fails_n_sends_then_recovers():
    producer = InMemoryProducer()
    cause = RuntimeError("broker down")
    producer.fail_next(2, cause=cause)

    for _ in range(2):
        with pytest.raises(ProducerError) as excinfo:
            await producer.send(OutgoingMessage("a", b"x"))
        assert excinfo.value.__cause__ is cause
    await producer.send(OutgoingMessage("a", b"ok"))

    assert [m.value for m in producer.sent] == [b"ok"]
    assert len(producer.failed) == 2
    assert issubclass(ProducerError, KafkaReliabilityError)


async def test_fail_when_predicate_targets_specific_messages():
    producer = InMemoryProducer()
    producer.fail_when(lambda m: m.topic == "poison")

    await producer.send(OutgoingMessage("fine", b"1"))
    with pytest.raises(ProducerError):
        await producer.send(OutgoingMessage("poison", b"2"))

    producer.fail_when(None)
    await producer.send(OutgoingMessage("poison", b"3"))
    assert [m.value for m in producer.sent] == [b"1", b"3"]
