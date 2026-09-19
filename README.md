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
| `outbox`, `dedup`, `replay` | planned |

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

## Development

```
pip install -e ".[dev]"
pytest              # fast unit suite
ruff check . && ruff format --check .
mypy src
```

The integration suite (real Kafka/Postgres/Redis) is tracked in issue #61.
