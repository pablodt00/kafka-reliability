"""confluent-kafka adapter, tested against a fake `confluent_kafka` module — no
broker, and confluent-kafka itself need not be installed. Real-broker tests
wait on issue #61."""

from __future__ import annotations

import importlib
import sys
import threading
import types
from typing import Any

import pytest

from kafka_reliability.core.errors import ConfigurationError, MissingExtraError, ProducerError
from kafka_reliability.core.message import OutgoingMessage

from .conformance import Harness, ProducerConformance

MODULE = "kafka_reliability.producers.confluent"


class FakeKafkaException(Exception):
    pass


class FakeConfluentProducer:
    """Enqueues on produce; delivery callbacks fire only from poll()/flush(),
    like librdkafka, and therefore on whichever thread called them."""

    instances: list[FakeConfluentProducer] = []

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config
        self.received: list[tuple[str, bytes | None, bytes, dict[str, bytes]]] = []
        self.fail_delivery_next = False
        self.buffer_full_times = 0
        self.leave_undelivered = False
        self.poll_threads: set[int] = set()
        self._queue: list[tuple[Any, Any]] = []
        self._lock = threading.Lock()
        FakeConfluentProducer.instances.append(self)

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        if self.buffer_full_times > 0:
            self.buffer_full_times -= 1
            raise BufferError("queue full")
        with self._lock:
            if self.fail_delivery_next:
                self.fail_delivery_next = False
                self._queue.append((on_delivery, "injected delivery failure"))
            else:
                self.received.append((topic, key, value, dict(headers or [])))
                self._queue.append((on_delivery, None))

    def _serve(self) -> int:
        with self._lock:
            queued, self._queue = self._queue, []
        for callback, err in queued:
            callback(err, None)
        return len(queued)

    def poll(self, timeout: float = 0) -> int:
        self.poll_threads.add(threading.get_ident())
        served = self._serve()
        if not served:
            threading.Event().wait(min(timeout, 0.005))
        return served

    def flush(self, timeout: float = -1) -> int:
        if self.leave_undelivered:
            return 1
        self._serve()
        return 0


@pytest.fixture
def adapter_module(monkeypatch):
    fake = types.ModuleType("confluent_kafka")
    fake.Producer = FakeConfluentProducer  # type: ignore[attr-defined]
    fake.KafkaException = FakeKafkaException  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake)
    sys.modules.pop(MODULE, None)
    FakeConfluentProducer.instances.clear()
    module = importlib.import_module(MODULE)
    yield module
    sys.modules.pop(MODULE, None)


class TestConfluentConformance(ProducerConformance):
    @pytest.fixture(autouse=True)
    def _module(self, adapter_module):
        self.module = adapter_module

    async def make_harness(self) -> Harness:
        client = FakeConfluentProducer()
        adapter = self.module.ConfluentProducerAdapter(client, poll_interval=0.01)

        def inject() -> None:
            client.fail_delivery_next = True

        return Harness(
            producer=adapter,
            inject_failure=inject,
            delivered=lambda: client.received,
            cleanup=adapter.close,
        )


def test_missing_extra_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)
    sys.modules.pop(MODULE, None)
    with pytest.raises(MissingExtraError, match=r"kafka-reliability\[confluent\]"):
        importlib.import_module(MODULE)
    sys.modules.pop(MODULE, None)


def test_factory_sets_safe_defaults(adapter_module):
    adapter_module.create_producer("localhost:9092", **{"linger.ms": 5})
    config = FakeConfluentProducer.instances[-1].config
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["linger.ms"] == 5


@pytest.mark.parametrize(
    "override",
    [{"acks": 1}, {"acks": "0"}, {"enable.idempotence": False}, {"enable.idempotence": "false"}],
)
def test_factory_refuses_to_weaken_defaults(adapter_module, override):
    with pytest.raises(ConfigurationError):
        adapter_module.create_producer("localhost:9092", **override)


async def test_delivery_callback_never_runs_on_the_event_loop_thread(adapter_module):
    client = FakeConfluentProducer()
    adapter = adapter_module.ConfluentProducerAdapter(client, poll_interval=0.01)
    await adapter.send(OutgoingMessage("t", b"v"))
    await adapter.close()
    assert threading.get_ident() not in client.poll_threads
    assert client.poll_threads  # the background poller did the work


async def test_concurrent_sends_pipeline(adapter_module):
    import asyncio

    client = FakeConfluentProducer()
    adapter = adapter_module.ConfluentProducerAdapter(client, poll_interval=0.01)
    await asyncio.gather(*(adapter.send(OutgoingMessage("t", str(i).encode())) for i in range(20)))
    await adapter.close()
    assert len(client.received) == 20


async def test_full_queue_is_retried_then_succeeds(adapter_module):
    client = FakeConfluentProducer()
    client.buffer_full_times = 3
    adapter = adapter_module.ConfluentProducerAdapter(client, poll_interval=0.001)
    await adapter.send(OutgoingMessage("t", b"v"))
    await adapter.close()
    assert len(client.received) == 1


async def test_permanently_full_queue_raises_producer_error(adapter_module):
    client = FakeConfluentProducer()
    client.buffer_full_times = 10_000
    adapter = adapter_module.ConfluentProducerAdapter(
        client, poll_interval=0.001, queue_full_retries=3
    )
    with pytest.raises(ProducerError, match="queue stayed full"):
        await adapter.send(OutgoingMessage("t", b"v"))
    await adapter.close()


async def test_delivery_failure_is_chained_to_a_kafka_exception(adapter_module):
    client = FakeConfluentProducer()
    client.fail_delivery_next = True
    adapter = adapter_module.ConfluentProducerAdapter(client, poll_interval=0.01)
    with pytest.raises(ProducerError) as excinfo:
        await adapter.send(OutgoingMessage("t", b"v"))
    assert isinstance(excinfo.value.__cause__, FakeKafkaException)
    await adapter.close()


async def test_flush_with_messages_left_raises(adapter_module):
    client = FakeConfluentProducer()
    client.leave_undelivered = True
    adapter = adapter_module.ConfluentProducerAdapter(client, poll_interval=0.01)
    with pytest.raises(ProducerError, match="undelivered"):
        await adapter.flush(timeout=0.01)
