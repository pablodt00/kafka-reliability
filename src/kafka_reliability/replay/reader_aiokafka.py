"""AiokafkaReader — the `Reader` over an `AIOKafkaConsumer`. Requires the
[aiokafka] extra.

It assigns partitions explicitly (no consumer-group membership, so it never
triggers a rebalance of anyone's live group), seeks, and reads a bounded range.
It never deletes, never seeks a live group and never touches topic config.
`group_id` is only needed for `commit()`, which replay calls only when the
operator asked to commit source offsets.

Verified against fakes only: there is no broker harness yet (issue #61), so
check it against your cluster with a dry run before relying on it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, require_extra
from kafka_reliability.core.message import Record

try:
    import aiokafka
    from aiokafka.structs import OffsetAndMetadata, TopicPartition
except ImportError as exc:
    require_extra(package="aiokafka", extra="aiokafka", cause=exc)


class AiokafkaReader:
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        *,
        group_id: str | None = None,
        consumer_factory: Callable[..., Any] | None = None,
        poll_timeout_ms: int = 1000,
        **consumer_kwargs: Any,
    ) -> None:
        if consumer_kwargs.get("enable_auto_commit"):
            raise ConfigurationError("replay never auto-commits; leave enable_auto_commit unset")
        self._factory = consumer_factory or aiokafka.AIOKafkaConsumer
        self._kwargs = {
            **consumer_kwargs,
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
            "enable_auto_commit": False,
        }
        self._poll_timeout_ms = poll_timeout_ms
        self._consumer: Any = None

    async def start(self) -> None:
        if self._consumer is None:
            self._consumer = self._factory(**self._kwargs)
            await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def __aenter__(self) -> AiokafkaReader:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def _c(self) -> Any:
        await self.start()
        return self._consumer

    async def partitions(self, topic: str) -> Sequence[int]:
        c = await self._c()
        await c.topics()  # refresh metadata
        found = c.partitions_for_topic(topic)
        if not found:
            raise ConfigurationError(f"topic {topic!r} does not exist or has no partitions")
        return sorted(found)

    async def beginning_offsets(self, topic: str, partitions: Sequence[int]) -> Mapping[int, int]:
        c = await self._c()
        got = await c.beginning_offsets([TopicPartition(topic, p) for p in partitions])
        return {tp.partition: off for tp, off in got.items()}

    async def end_offsets(self, topic: str, partitions: Sequence[int]) -> Mapping[int, int]:
        c = await self._c()
        got = await c.end_offsets([TopicPartition(topic, p) for p in partitions])
        return {tp.partition: off for tp, off in got.items()}

    async def offsets_for_times(
        self, topic: str, timestamps_ms: Mapping[int, int]
    ) -> Mapping[int, int | None]:
        c = await self._c()
        got = await c.offsets_for_times(
            {TopicPartition(topic, p): ms for p, ms in timestamps_ms.items()}
        )
        return {tp.partition: (None if v is None else v.offset) for tp, v in got.items()}

    async def read(self, topic: str, partition: int, start: int, end: int) -> AsyncIterator[Record]:
        if start >= end:
            return
        c = await self._c()
        tp = TopicPartition(topic, partition)
        c.assign([tp])
        c.seek(tp, start)
        while True:
            batch = await c.getmany(tp, timeout_ms=self._poll_timeout_ms, max_records=500)
            for msg in batch.get(tp, []):
                if msg.offset >= end:
                    return
                yield Record(
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    key=msg.key,
                    value=msg.value if msg.value is not None else b"",
                    headers=tuple((str(k), v) for k, v in (msg.headers or ())),
                    timestamp=datetime.fromtimestamp(msg.timestamp / 1000, tz=UTC),
                )
                if msg.offset + 1 >= end:
                    return
            if not batch.get(tp) and await c.position(tp) >= end:
                return  # gaps (compaction, transaction markers) at the tail of the range

    async def commit(self, topic: str, partition: int, offset: int) -> None:
        c = await self._c()
        if self._kwargs["group_id"] is None:
            raise ConfigurationError("committing source offsets needs AiokafkaReader(group_id=...)")
        await c.commit({TopicPartition(topic, partition): OffsetAndMetadata(offset, "")})
