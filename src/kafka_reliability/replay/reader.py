"""The `Reader` protocol: how replay reads a topic, plus `InMemoryReader`.

Replay is read-and-republish only, so the read side needs exactly five things:
partition discovery, offset bounds, timestamp lookup, a range read and — only if
the operator asks — an offset commit. It deliberately has no delete, no seek on
a live consumer group and no topic-config call: the tool never destroys anything.

Any object with these methods satisfies it. `AiokafkaReader` (in
`replay.reader_aiokafka`) is the shipped adapter. No third-party imports
permitted in this module.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import datetime, tzinfo
from typing import Protocol, runtime_checkable

from kafka_reliability.core.message import Record


@runtime_checkable
class Reader(Protocol):
    async def partitions(self, topic: str) -> Sequence[int]: ...

    async def beginning_offsets(self, topic: str, partitions: Sequence[int]) -> Mapping[int, int]:
        """First available offset of each partition."""
        ...

    async def end_offsets(self, topic: str, partitions: Sequence[int]) -> Mapping[int, int]:
        """The offset one past the last record of each partition."""
        ...

    async def offsets_for_times(
        self, topic: str, timestamps_ms: Mapping[int, int]
    ) -> Mapping[int, int | None]:
        """Per partition, the first offset whose timestamp is `>=` the given epoch
        milliseconds, or `None` if there is none (Kafka's `offsets_for_times`)."""
        ...

    def read(self, topic: str, partition: int, start: int, end: int) -> AsyncIterator[Record]:
        """Records with `start <= offset < end`, in offset order."""
        ...

    async def commit(self, topic: str, partition: int, offset: int) -> None:
        """Commit `offset` as the next to read, for the reader's own group. Only
        called when the operator passed `commit_source_offsets`."""
        ...


class InMemoryReader:
    """A `Reader` over lists of records, for tests. Offsets are list positions
    plus `base_offset`; `records` must be in offset order per partition."""

    def __init__(self, topic: str, partitions: Mapping[int, Sequence[Record]]) -> None:
        self.topic = topic
        self._data = {p: list(rs) for p, rs in partitions.items()}
        self.committed: dict[int, int] = {}
        self.reads: list[tuple[int, int, int]] = []

    async def partitions(self, topic: str) -> Sequence[int]:
        return sorted(self._data)

    async def beginning_offsets(self, topic: str, partitions: Sequence[int]) -> Mapping[int, int]:
        return {p: (self._data[p][0].offset if self._data[p] else 0) for p in partitions}

    async def end_offsets(self, topic: str, partitions: Sequence[int]) -> Mapping[int, int]:
        return {p: (self._data[p][-1].offset + 1 if self._data[p] else 0) for p in partitions}

    async def offsets_for_times(
        self, topic: str, timestamps_ms: Mapping[int, int]
    ) -> Mapping[int, int | None]:
        out: dict[int, int | None] = {}
        for p, ms in timestamps_ms.items():
            wanted = datetime.fromtimestamp(ms / 1000, tz=self._tz(p))
            out[p] = next((r.offset for r in self._data[p] if r.timestamp >= wanted), None)
        return out

    def _tz(self, partition: int) -> tzinfo | None:  # records carry aware datetimes
        rows = self._data[partition]
        return rows[0].timestamp.tzinfo if rows else None

    async def read(self, topic: str, partition: int, start: int, end: int) -> AsyncIterator[Record]:
        self.reads.append((partition, start, end))
        for r in self._data[partition]:
            if start <= r.offset < end:
                yield r

    async def commit(self, topic: str, partition: int, offset: int) -> None:
        self.committed[partition] = offset
