"""Raw aiokafka loop. Create the consumer with enable_auto_commit=False."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from kafka_reliability.core.message import Record


def to_record(msg) -> Record:  # an aiokafka ConsumerRecord
    return Record(
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        key=msg.key,
        value=msg.value,
        headers=tuple(msg.headers or ()),
        timestamp=datetime.fromtimestamp(msg.timestamp / 1000, tz=UTC),
    )


async def handle_record(dedup, record: Record, handler, *, retry_delay: float = 1.0) -> None:
    """Run `handler` at most once per message; return when the offset is safe to commit."""
    while True:
        async with dedup.process(record) as decision:  # an exception releases the claim
            if decision:
                await handler(record)
        if decision.commit_offset:
            return
        await asyncio.sleep(retry_delay)  # live lease elsewhere: wait, never skip past it


async def consume(consumer, dedup, handler) -> None:
    async for msg in consumer:
        await handle_record(dedup, to_record(msg), handler)
        await consumer.commit()  # after the work and the dedup record, never before
