"""aiokafka adapter, tested against a fake `aiokafka` module — no broker, and
aiokafka itself need not be installed. Real-broker tests wait on issue #61."""

from __future__ import annotations

import asyncio
import importlib
import sys
import types
from typing import Any

import pytest

from kafka_reliability.core.errors import ConfigurationError, MissingExtraError, ProducerError
from kafka_reliability.core.message import OutgoingMessage

from .conformance import Harness, ProducerConformance

MODULE = "kafka_reliability.producers.aiokafka"


class FakeKafkaError(Exception):
    pass


class FakeAIOKafkaProducer:
    instances: list[FakeAIOKafkaProducer] = []

    def __init__(self, **config: Any) -> None:
        self.config = config
        self.received: list[tuple[str, bytes | None, bytes, dict[str, bytes]]] = []
        self.fail_next = False
        self.started = self.stopped = False
        self.flush_delay = 0.0
        FakeAIOKafkaProducer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        if self.fail_next:
            self.fail_next = False
            raise FakeKafkaError("injected")
        self.received.append((topic, key, value, dict(headers or [])))

    async def flush(self) -> None:
        await asyncio.sleep(self.flush_delay)


@pytest.fixture
def adapter_module(monkeypatch):
    fake = types.ModuleType("aiokafka")
    fake.AIOKafkaProducer = FakeAIOKafkaProducer  # type: ignore[attr-defined]
    errors = types.ModuleType("aiokafka.errors")
    errors.KafkaError = FakeKafkaError  # type: ignore[attr-defined]
    fake.errors = errors  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aiokafka", fake)
    monkeypatch.setitem(sys.modules, "aiokafka.errors", errors)
    sys.modules.pop(MODULE, None)
    FakeAIOKafkaProducer.instances.clear()
    module = importlib.import_module(MODULE)
    yield module
    sys.modules.pop(MODULE, None)


class TestAIOKafkaConformance(ProducerConformance):
    @pytest.fixture(autouse=True)
    def _module(self, adapter_module):
        self.module = adapter_module

    async def make_harness(self) -> Harness:
        client = FakeAIOKafkaProducer()
        adapter = self.module.AIOKafkaProducerAdapter(client)

        def inject() -> None:
            client.fail_next = True

        return Harness(producer=adapter, inject_failure=inject, delivered=lambda: client.received)


def test_missing_extra_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "aiokafka", None)
    sys.modules.pop(MODULE, None)
    with pytest.raises(MissingExtraError, match=r"kafka-reliability\[aiokafka\]"):
        importlib.import_module(MODULE)
    sys.modules.pop(MODULE, None)


def test_factory_sets_safe_defaults(adapter_module):
    adapter = adapter_module.create_producer("localhost:9092", linger_ms=5)
    config = FakeAIOKafkaProducer.instances[-1].config
    assert config["acks"] == "all"
    assert config["enable_idempotence"] is True
    assert config["bootstrap_servers"] == "localhost:9092"
    assert config["linger_ms"] == 5
    assert adapter is not None


@pytest.mark.parametrize("override", [{"acks": 1}, {"acks": 0}, {"enable_idempotence": False}])
def test_factory_refuses_to_weaken_defaults(adapter_module, override):
    with pytest.raises(ConfigurationError):
        adapter_module.create_producer("localhost:9092", **override)


def test_factory_accepts_restating_the_defaults(adapter_module):
    adapter_module.create_producer("localhost:9092", acks=-1, enable_idempotence=True)


async def test_factory_adapter_owns_client_lifecycle(adapter_module):
    async with adapter_module.create_producer("localhost:9092"):
        client = FakeAIOKafkaProducer.instances[-1]
        assert client.started and not client.stopped
    assert client.stopped


async def test_handed_in_client_is_never_started_or_stopped(adapter_module):
    client = FakeAIOKafkaProducer()
    adapter = adapter_module.AIOKafkaProducerAdapter(client)
    await adapter.start()
    await adapter.stop()
    assert not client.started and not client.stopped


async def test_client_error_is_mapped_and_chained(adapter_module):
    client = FakeAIOKafkaProducer()
    client.fail_next = True
    adapter = adapter_module.AIOKafkaProducerAdapter(client)
    with pytest.raises(ProducerError) as excinfo:
        await adapter.send(OutgoingMessage("t", b"v"))
    assert isinstance(excinfo.value.__cause__, FakeKafkaError)


async def test_flush_timeout_becomes_producer_error(adapter_module):
    client = FakeAIOKafkaProducer()
    client.flush_delay = 5
    adapter = adapter_module.AIOKafkaProducerAdapter(client)
    with pytest.raises(ProducerError, match="within"):
        await adapter.flush(timeout=0.01)
