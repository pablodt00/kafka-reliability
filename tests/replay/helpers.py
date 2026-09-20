from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kafka_reliability.core.message import Record
from kafka_reliability.replay.reader import InMemoryReader

T0 = datetime(2026, 9, 4, 0, 0, tzinfo=UTC)


def make_record(
    partition: int, offset: int, *, minutes: int = 0, key: str = "k", headers=(), value=b"v"
) -> Record:
    return Record(
        "orders.dlq",
        partition,
        offset,
        key.encode(),
        value,
        tuple(headers),
        T0 + timedelta(minutes=minutes),
    )


def make_reader() -> InMemoryReader:
    """p0: offsets 100..109, one record per minute; p1: 50..54, every 30 min; p2 empty."""
    return InMemoryReader(
        "orders.dlq",
        {
            0: [make_record(0, 100 + i, minutes=i, key=f"a{i}") for i in range(10)],
            1: [make_record(1, 50 + i, minutes=30 * i, key=f"b{i}") for i in range(5)],
            2: [],
        },
    )
