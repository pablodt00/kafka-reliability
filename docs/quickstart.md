# Quickstart: the strongest guarantee, in five minutes

Postgres is your system of record; Kafka is how other services learn about
changes. This wires the **outbox** (an event is durable in the same transaction
as the business write) to a **transactional-mode consumer** (the dedup record
commits with the handler's side effects). That combination is *at-least-once
delivery with effectively-once processing* — the strongest thing this library
can offer — and needs no infrastructure you do not already run.

```
pip install "kafka-reliability[outbox-asyncpg,dedup-postgres,aiokafka]"
```

## 1. Create the tables

Emitted, never run for you — paste into your migration tool:

```python
from kafka_reliability.dedup.backends.postgres import dedup_ddl
from kafka_reliability.outbox.schema import outbox_ddl

print(outbox_ddl())   # the outbox table + its two partial indexes
print(dedup_ddl())    # processed_messages
```

## 2. Enqueue inside your transaction

```python
from kafka_reliability.outbox.backends.asyncpg import AsyncpgOutboxWriter

writer = AsyncpgOutboxWriter()

async with pool.acquire() as conn, conn.transaction():   # a Connection, never a Pool
    await conn.execute("INSERT INTO orders (id, total) VALUES ($1, $2)", order_id, total)
    event_id = await writer.enqueue(
        conn, topic="orders", payload=payload,
        aggregatetype="order", aggregateid=str(order_id), type="OrderCreated",
    )
```

The event and the order commit or roll back together. `aggregateid` becomes the
Kafka key (per-key ordering).

## 3. Run the relay (one small process)

```python
import asyncio, asyncpg
from kafka_reliability.outbox.backends.asyncpg_relay import AsyncpgRelayStore
from kafka_reliability.outbox.relay import OutboxRelay
from kafka_reliability.producers.aiokafka import create_producer

async def main() -> None:
    conn = await asyncpg.connect(DSN)            # dedicated: it holds the advisory lock
    async with create_producer("kafka:9092") as producer:
        await OutboxRelay(AsyncpgRelayStore(conn), producer).run()

asyncio.run(main())
```

Run two during a deploy if you like: one is elected by an advisory lock, the
other waits. If the connection drops, `run()` raises rather than continue
unlocked — let your supervisor restart it.

## 4. Consume with the dedup record in the handler's transaction

```python
from kafka_reliability.core.headers import EVENT_ID
from kafka_reliability.dedup import keys
from kafka_reliability.dedup.backends.postgres import PostgresDedupStore
from kafka_reliability.dedup.deduplicator import Deduplicator

dedup = Deduplicator(
    store=PostgresDedupStore(pool),
    group="billing",                       # namespaces records per consumer group
    key=keys.from_header(EVENT_ID),        # no default key, on purpose
)

# consumer created with enable_auto_commit=False
async for msg in consumer:
    record = to_record(msg)                # see docs/recipes/aiokafka_recipe.py
    async with pool.acquire() as conn, conn.transaction():
        async with dedup.process(record, conn=conn) as decision:
            if decision:
                await handle(record, conn)  # your writes, on the same connection
    if decision.commit_offset:
        await consumer.commit()            # after the work and the dedup record
```

A redelivered message hits the primary-key conflict and is skipped; a handler
exception rolls the transaction back, dedup row included, so the message is
retried rather than suppressed.

## What this does *not* do

The dedup record is only atomic with your side effects when they are writes to
the **same Postgres** on the connection you pass in. If the handler calls a
payment API or sends an email, the module narrows the window and no more — pass
an idempotency key to the provider. See [delivery semantics](delivery-semantics.md).

Not run in CI yet: no Postgres/Kafka harness (issue #61). The snippets follow the
tested API; try them against a scratch database first.
