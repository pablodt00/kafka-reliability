# kafka-reliability

A framework-agnostic Python library for three reliability problems every
Kafka-based service hits:

- **Transactional outbox** — get an event out of a database transaction and into
  Kafka without losing it.
- **Idempotent consumer** — survive Kafka's at-least-once redelivery.
- **DLQ replay** — drain a dead-letter topic safely.

The target is **at-least-once delivery with effectively-once processing**. The
library never promises "exactly once" — see
[delivery semantics](docs/delivery-semantics.md) for what it does and does not
guarantee.

> **Status: pre-alpha.** Every module is implemented and unit-tested against
> fakes. The parts that need a real Postgres, Redis or Kafka (advisory-lock
> election, `ON CONFLICT` under concurrency, `SET NX`, the aiokafka reader) are
> covered by `integration` tests that skip without a server; the container
> harness that runs them in CI is tracked in issue #61. Treat those paths as
> unverified until you have run them against your own infrastructure.

## Should you use this? Honest answers first

- **You already run Kafka Connect → use Debezium** for the relay half. It has
  lower latency, adds no read load to the primary, handles ordering and offsets
  with more care, and has years of production hardening. The argument for a
  Python polling relay is *operational cost* (nothing else to run), not technical
  superiority. The outbox table uses Debezium's column names, so graduating is a
  connector config, not a data migration ([D1](docs/claude/06-decisions.md)).
- **You are on FastStream and only need the outbox → use `faststream-outbox`.**
- **You need hundreds of thousands of events per second** → a poll-based relay is
  the wrong tool; use CDC.
- **You use MySQL, DynamoDB or another broker (RabbitMQ, NATS, SQS)** → not
  supported, and not planned ([non-goals](docs/claude/00-overview.md)).

It is a good fit if Postgres is your system of record, you run Python services
(FastAPI, Litestar, Django, Celery, plain asyncio), your volume is hundreds to
low thousands of events per second per service, and you want to reason about
delivery semantics explicitly. The three modules are independent — adopt one at a
time: `outbox`, `dedup` and `replay` never import each other.

## What is in it

| Module | What it gives you | Docs |
|---|---|---|
| `outbox` | Enqueue in your own transaction (asyncpg / psycopg / SQLAlchemy / Django), polling relay with per-key ordering, retention sweep | [outbox](docs/outbox.md) |
| `dedup` | `Deduplicator.process()` over Postgres / Redis / SQLite / in-memory stores; recipes for aiokafka, confluent-kafka, FastStream, Celery | [dedup](docs/dedup.md) |
| `replay` | `DlqRouter`, select-then-act replay with dry run, safety rails, audit log and CLI | [replay](docs/replay.md) |
| `producers` | The two-method `Producer` protocol, aiokafka / confluent adapters, `InMemoryProducer` | below |

Start with the [five-minute quickstart](docs/quickstart.md), which shows the
strongest guarantee the library offers (Postgres, transactional mode). Running it
in production? Read the [operations guide](docs/operations.md).

## Install

```
pip install kafka-reliability                    # core only, no third-party deps
pip install "kafka-reliability[outbox-asyncpg,dedup-postgres,aiokafka]"
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

The SQLite and in-memory dedup stores use the standard library and need no extra.
The relay and the Postgres dedup store currently support **asyncpg only**.

## Producers

The outbox relay and the replay runner publish through a two-method protocol,
never through a concrete Kafka client:

```python
class Producer(Protocol):
    async def send(self, message: OutgoingMessage) -> None: ...
    async def flush(self, timeout: float | None = None) -> None: ...
```

`send` returns only after the broker has acknowledged the message and raises
`ProducerError` on failure. **Bring your own:** any object with those two
coroutines works — for example a thin wrapper around a FastStream publisher.

```python
from kafka_reliability.producers.aiokafka import create_producer

async with create_producer("localhost:9092") as producer:  # acks=all, idempotent
    await producer.send(OutgoingMessage(topic="orders", value=b"...", key=b"42"))
```

Producer idempotence only deduplicates the *client's own retries*. A relay that
restarts and publishes a row again is a fresh produce call, so consumers must
still deduplicate. To test your own code use `InMemoryProducer`
(`assert_sent`, `fail_next`).

## Metrics

Every module reports through a three-method `MetricsSink`; the default discards.
Labels are bounded — never a dedup key, message key, partition or offset
([D11](docs/claude/06-decisions.md)). `contrib.otel.OtelMetrics` (extra `otel`)
adapts it to OpenTelemetry; a Prometheus adapter is ten lines (see its docstring).
Alert on `outbox.oldest_pending_seconds`.

## Development

```
pip install -e ".[dev]"
pytest                    # fast unit suite
ruff check . && ruff format --check .
mypy src
KAFKA_RELIABILITY_TEST_PG_DSN=postgresql://user:pw@host:5432/db \
KAFKA_RELIABILITY_TEST_REDIS_URL=redis://localhost:6379/0 pytest -m integration
```

Versioning and the public API surface: [CHANGELOG](CHANGELOG.md). The design
record, with every decision's trade-offs, is in
[`docs/claude/`](docs/claude/06-decisions.md).
