"""FastStream subscriber with manual acknowledgement (ack_policy=MANUAL on your
subscriber; the parameter name differs across FastStream versions).

    @broker.subscriber("orders", group_id="billing", ack_policy=AckPolicy.MANUAL)
    @deduplicated(dedup)
    async def on_order(body: OrderCreated) -> None: ...
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime

from kafka_reliability.core.message import Record


def to_record(msg) -> Record:  # a FastStream StreamMessage wrapping a Kafka record
    raw = msg.raw_message
    return Record(
        topic=raw.topic,
        partition=raw.partition,
        offset=raw.offset,
        key=raw.key,
        value=raw.value,
        headers=tuple(raw.headers or ()),
        timestamp=datetime.fromtimestamp(raw.timestamp / 1000, tz=UTC),
    )


def deduplicated(dedup):
    def decorator(handler):
        @functools.wraps(handler)
        async def wrapper(body, msg):
            async with dedup.process(to_record(msg)) as decision:
                if decision:
                    await handler(body)
            if decision.commit_offset:
                await msg.ack()  # after the work and the dedup record
            else:
                await msg.nack()  # live lease elsewhere: redeliver, never lose it

        return wrapper

    return decorator
