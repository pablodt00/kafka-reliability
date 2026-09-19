"""The Producer protocol and its concrete adapters. Shared by outbox and
replay; the one place a Kafka client is imported.

Only the protocol and the in-memory test double are imported here. The
`aiokafka` and `confluent` adapters live in their own submodules so that
importing this package never requires a Kafka client."""

from kafka_reliability.producers.memory import InMemoryProducer
from kafka_reliability.producers.port import Producer

__all__ = ["InMemoryProducer", "Producer"]
