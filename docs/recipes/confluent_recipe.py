"""confluent-kafka loop. Configure the consumer with enable.auto.commit=False.

The client is synchronous, so poll and commit run in a worker thread and the
handler runs on the event loop with the Deduplicator."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from kafka_reliability.core.message import Record


def to_record(msg) -> Record:  # a confluent_kafka.Message
    _, millis = msg.timestamp()
    return Record(
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        key=msg.key(),
        value=msg.value(),
        headers=tuple(msg.headers() or ()),
        timestamp=datetime.fromtimestamp(millis / 1000, tz=UTC),
    )


async def handle_message(dedup, consumer, msg, handler, *, retry_delay: float = 1.0) -> None:
    record = to_record(msg)
    while True:
        async with dedup.process(record) as decision:
            if decision:
                await handler(record)
        if decision.commit_offset:
            # Synchronous commit of exactly this message, after the work and the dedup record.
            await asyncio.to_thread(consumer.commit, message=msg, asynchronous=False)
            return
        await asyncio.sleep(retry_delay)  # live lease elsewhere: do not commit, do not move on


async def consume(consumer, dedup, handler, *, stop: asyncio.Event) -> None:
    while not stop.is_set():
        msg = await asyncio.to_thread(consumer.poll, 1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(msg.error())
        await handle_message(dedup, consumer, msg, handler)
