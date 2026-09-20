"""The `kafka-reliability-replay` console script: `replay.cli` wired to the
aiokafka reader and producer. Requires the [cli] and [aiokafka] extras.

This is the composition root, so it lives in `contrib/`: `replay` itself depends
only on the `Producer`/`Reader` protocols, never on a concrete adapter."""

from __future__ import annotations

from typing import Any


def _reader(bootstrap_servers: str, group_id: str | None) -> Any:
    from kafka_reliability.replay.reader_aiokafka import AiokafkaReader

    return AiokafkaReader(bootstrap_servers, group_id=group_id)


def _producer(bootstrap_servers: str) -> Any:
    from kafka_reliability.producers.aiokafka import create_producer

    return create_producer(bootstrap_servers)


def main() -> None:
    from kafka_reliability.replay.cli import Factories
    from kafka_reliability.replay.cli import main as command

    command(obj=Factories(reader=_reader, producer=_producer))


if __name__ == "__main__":  # pragma: no cover
    main()
