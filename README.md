# kafka-reliability

A framework-agnostic Python library for three reliability problems every
Kafka-based service hits:

- **Transactional outbox** — get an event out of a database transaction and into
  Kafka without losing it.
- **Idempotent consumer** — survive Kafka's at-least-once redelivery.
- **DLQ replay** — drain a dead-letter topic safely.

The target is **at-least-once delivery with effectively-once processing**. The
library never promises "exactly once".

> **Status: pre-alpha.** Under construction; see the table below for what exists.

| Area | Status |
|---|---|
| `core` (messages, headers, clock, errors) and `metrics` | implemented |
| `producers` (protocol, in-memory, aiokafka, confluent-kafka) | implemented |
| `outbox` schema and write path (DDL, `BaseOutboxWriter`, asyncpg / psycopg / SQLAlchemy / Django writers) | implemented |
| `outbox` relay and retention, `dedup`, `replay` | planned |

The design record lives in [`docs/claude/`](docs/claude/); every decision, with
its trade-offs, is in [`06-decisions.md`](docs/claude/06-decisions.md).

## Install

```
pip install kafka-reliability                    # core only, no third-party deps
pip install "kafka-reliability[aiokafka]"        # aiokafka producer adapter
pip install "kafka-reliability[confluent]"       # confluent-kafka producer adapter
```

Each extra pulls in exactly one third-party package:

| Extra | Package |
|---|---|
| `aiokafka` | `aiokafka` |
| `confluent` | `confluent-kafka` |
| `outbox-asyncpg`, `dedup-postgres` | `asyncpg` |
| `outbox-psycopg` | `psycopg[binary]` |
| `outbox-sqlalchemy` | `sqlalchemy` |
| `outbox-django` | `django` |
| `dedup-redis` | `redis` |
| `otel` | OpenTelemetry API + SDK |
| `cli` | `click` |

## Producers

The outbox relay and the replay runner publish through a two-method protocol,
never through a concrete Kafka client:

```python
class Producer(Protocol):
    async def send(self, message: OutgoingMessage) -> None: ...
    async def flush(self, timeout: float | None = None) -> None: ...
```

`send` returns only after the broker has acknowledged the message and raises
`ProducerError` on failure. There is no partitioner control, no serializer and
no config passthrough — configure your client and hand it in.

**Bring your own.** Any object with those two coroutines works — for example a
thin wrapper around a FastStream publisher. The library never imports it.

**aiokafka / confluent-kafka.** Wrap a client you built, or use the factory,
which sets `acks=all` and idempotence as defaults you cannot weaken:

```python
from kafka_reliability.producers.aiokafka import create_producer

async with create_producer("localhost:9092") as producer:
    await producer.send(OutgoingMessage(topic="orders", value=b"...", key=b"42"))
```

Producer idempotence only deduplicates the *client's own retries*. A relay that
restarts and publishes a row again is a fresh produce call, so consumers must
still deduplicate.

**Testing your own code.** `InMemoryProducer` is public API:

```python
from kafka_reliability.producers import InMemoryProducer

producer = InMemoryProducer()
await my_service.publish(producer)
producer.assert_sent(topic="orders", key=b"42")

producer.fail_next()  # next send raises ProducerError
```

## Outbox: enqueue inside your own transaction

The writer never opens a connection and never commits: you hand it the
connection or session your transaction already uses, so the event and your
business write commit or roll back together. Paste `outbox_ddl()` into your own
migration (the library never runs migrations).

```python
from kafka_reliability.outbox.backends.asyncpg import AsyncpgOutboxWriter
from kafka_reliability.outbox.schema import outbox_ddl

print(outbox_ddl())  # or payload="jsonb"; see 02-outbox.md
writer = AsyncpgOutboxWriter()

async with conn.transaction():  # conn: asyncpg.Connection, never a pool
    await conn.execute("INSERT INTO orders ...")
    event_id = await writer.enqueue(
        conn,
        topic="orders",
        payload=b"...",
        aggregatetype="order",
        aggregateid="o-1",
        type="OrderCreated",
    )
```

Delivery is at-least-once: consumers of an outbox-fed topic must deduplicate
(effectively-once processing).

## Development

```
pip install -e ".[dev]"
pytest              # fast unit suite
ruff check . && ruff format --check .
mypy src
```

The outbox write-path conformance suite needs a real Postgres. Point it at any
database you can create tables in (it uses the tables `outbox`, `outbox_j` and
`biz`, and drops them afterwards):

```
KAFKA_RELIABILITY_TEST_PG_DSN=postgresql://user:pw@host:5432/db pytest -m integration
```

Without the variable those tests are skipped. The container harness (Kafka,
Redis, and a managed Postgres) is tracked in issue #61.
