# 05 — Architecture

> Design context. The code blocks below are **signature sketches**, not an
> implementation and not a frozen API. No bodies, deliberately. Names are
> provisional; several are contested in `06-open-questions.md`.

## The organising constraint

**Each module must be usable entirely on its own.** A team that wants only the
dedup store must not install a Postgres driver they do not use; a team that
wants only the outbox must not need a Kafka client on their web servers. This is
not tidiness — it is the adoption path. Nobody rewrites their producer and their
consumer and their runbooks in one PR, and a library that requires them to is a
library they will not adopt.

Concretely, three rules that CI can check:

1. `kafka_reliability.outbox`, `.dedup` and `.replay` never import each other.
2. Each module's third-party imports are lazy or extras-gated, so
   `pip install kafka-reliability` with no extras imports cleanly and every
   module fails with a clear message naming the missing extra.
3. The outbox *write* path imports no Kafka client at all. A test asserting
   `"aiokafka" not in sys.modules` after `from kafka_reliability.outbox import
   OutboxWriter` is cheap and catches the regression that would otherwise creep
   in via a shared `types` module.

Shared code lives in a small `core` package with no third-party dependencies
beyond the standard library. If something wants to be shared and cannot meet
that bar, it should be duplicated instead. A little duplication is cheaper than
a dependency-coupling bug that only shows up in a user's install.

## How the modules relate

They compose through data, not through imports:

```mermaid
flowchart TB
    subgraph core["kafka_reliability.core (stdlib only)"]
      M[Message / Record types]
      H[header name constants]
      E[exceptions]
    end
    subgraph outbox["kafka_reliability.outbox"]
      OW[OutboxWriter] --> OT[(outbox table)]
      OT --> OR[OutboxRelay] --> P1[Producer port]
    end
    subgraph dedup["kafka_reliability.dedup"]
      DS[DedupStore protocol]
      DS --- PGS[PostgresDedupStore]
      DS --- RS[RedisDedupStore]
      DG[Deduplicator]
    end
    subgraph replay["kafka_reliability.replay"]
      RS2[ReplaySelector] --> RR[ReplayRunner] --> P2[Producer port]
      DLQ[DlqRouter]
    end
    core -.-> outbox
    core -.-> dedup
    core -.-> replay
    outbox -. "event-id header" .-> dedup
    replay -. "replay-id header" .-> dedup
```

The two dotted lines are the entire coupling between modules, and both are
header conventions defined in `core`:

- The outbox stamps an **event ID** header on every relayed record. If the
  consumer uses the dedup module with `key.from_header(EVENT_ID)`, outbox
  duplicates are suppressed. If it does not, nothing breaks — the header is
  ignored.
- The replay tool stamps a **replay ID** header. The dedup module's replay
  policy reads it. Same property: an unused header is inert.

Header names are constants in `core` so all three agree, but no module requires
another to be installed for its own headers to work.

## Package layout

```
kafka_reliability/
├── core/
│   ├── message.py        # Message, Record — plain dataclasses, bytes in/out
│   ├── headers.py        # EVENT_ID, REPLAY_ID, DLQ_* constants
│   ├── clock.py          # Clock protocol; injectable for tests
│   └── errors.py         # exception hierarchy
├── outbox/
│   ├── writer.py         # enqueue into the caller's transaction
│   ├── relay.py          # poll → produce → mark sent
│   ├── schema.py         # DDL text + SQLAlchemy Table factory
│   ├── retention.py      # chunked sweep of published rows
│   └── backends/
│       ├── asyncpg.py
│       ├── psycopg.py
│       └── sqlalchemy.py
├── dedup/
│   ├── store.py          # DedupStore protocol, ClaimResult
│   ├── keys.py           # key-derivation helpers
│   ├── deduplicator.py   # claim/confirm control flow, replay policy
│   └── backends/
│       ├── postgres.py
│       └── redis.py
├── replay/
│   ├── selector.py       # offset / timestamp / predicate selection
│   ├── runner.py         # dry-run + execute, one code path
│   ├── dlq.py            # DlqRouter: route a failed message + headers
│   ├── audit.py          # JSONL audit sink
│   └── cli.py            # argparse/typer entry point
└── producers/
    ├── port.py           # the Producer protocol
    ├── aiokafka.py
    └── confluent.py
```

`producers/` is shared by `outbox` and `replay` and is the one place a Kafka
client is imported. Both modules depend on the **protocol** in `port.py`, never
on a concrete adapter, so a user can supply their own producer (a FastStream
publisher, a test double) without the library knowing.

## Dependency boundaries

| Package | May import |
|---|---|
| `core` | stdlib only |
| `producers.port` | `core` |
| `producers.aiokafka` | `core`, `producers.port`, `aiokafka` |
| `outbox.writer` | `core` + the user's DB driver (lazily) — **no Kafka client** |
| `outbox.relay` | `core`, `producers.port`, DB driver |
| `dedup` | `core` + its chosen backend driver |
| `replay` | `core`, `producers.port`, a Kafka **consumer** |

Extras: `[outbox-asyncpg]`, `[outbox-psycopg]`, `[outbox-sqlalchemy]`,
`[dedup-postgres]`, `[dedup-redis]`, `[aiokafka]`, `[confluent]`, `[cli]`.
Trade-off: many extras is a documentation burden and a support-matrix burden. The
alternative — a fat install — makes the "use one module" story false, and that
story is the reason the library exists.

## Sync or async

**Decision: async-first, with sync facades for the outbox writer and the dedup
store only.** Those two sit on the caller's hot path and must match the caller's
world: Django and Celery are overwhelmingly synchronous, and telling a Celery
user to run an event loop to insert a row is absurd. The relay and the replay
runner are long-running I/O loops with no such constraint and are async only.

The trade-off is a maintained duplication of two small surfaces. It is contained
because the sync paths are *just SQL* — no Kafka, no concurrency — so they are
genuinely separate implementations rather than a `asyncio.run()` wrapper, which
would deadlock inside a running loop.

## API sketch — core

```python
# kafka_reliability/core/message.py
@dataclass(frozen=True, slots=True)
class Record:
    """A message as it exists on a topic."""
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: tuple[tuple[str, bytes], ...]
    timestamp: datetime

@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    """A message to be produced."""
    topic: str
    value: bytes
    key: bytes | None = None
    headers: Mapping[str, bytes] = field(default_factory=dict)
```

```python
# kafka_reliability/producers/port.py
class Producer(Protocol):
    async def send(self, message: OutgoingMessage) -> None: ...
    async def flush(self, timeout: float | None = None) -> None: ...
```

Deliberately three methods, no partitioner control, no serializers, no config
passthrough. Anything richer belongs to the client the user configured and
handed in.

## API sketch — outbox

```python
# kafka_reliability/outbox/writer.py
class OutboxWriter:
    def __init__(self, *, table: str = "outbox", event_id_header: str = EVENT_ID) -> None: ...

    async def enqueue(
        self,
        conn: Any,                       # the CALLER's connection/session, in their transaction
        *,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        headers: Mapping[str, bytes] | None = None,
        event_id: str | None = None,     # generated if omitted
    ) -> str: ...                        # returns the event id

    async def enqueue_many(self, conn: Any, messages: Sequence[OutgoingMessage]) -> list[str]: ...

class SyncOutboxWriter:
    def enqueue(self, conn: Any, *, topic: str, value: bytes, ...) -> str: ...
```

`conn: Any` is uncomfortable and intentional: it may be an `asyncpg.Connection`,
a `psycopg.AsyncConnection`, or a SQLAlchemy `AsyncSession`, and the backend
adapter dispatches on it. The alternative — a `Connection` protocol — would fit
none of the three cleanly (SQLAlchemy sessions are not DBAPI connections) and
would push a wrapper type onto users who already have a session in hand. The
trade-off is that a wrong argument fails at runtime rather than in a type
checker, so the error message must be excellent.

```python
# kafka_reliability/outbox/relay.py
@dataclass(frozen=True)
class RelayConfig:
    batch_size: int = 100
    poll_interval: float = 0.1
    ordering: Literal["strict", "sharded"] = "strict"
    shard_count: int = 1
    shard_index: int = 0
    max_attempts: int = 10
    advisory_lock_key: int | None = None

class OutboxRelay:
    def __init__(self, *, pool: Any, producer: Producer, table: str = "outbox",
                 config: RelayConfig = RelayConfig()) -> None: ...

    async def run(self, *, stop: asyncio.Event | None = None) -> None: ...
    async def run_once(self) -> RelayBatchResult: ...   # one pass; for tests and external schedulers
    async def stats(self) -> OutboxStats: ...           # pending count, oldest pending age, failed count
```

```python
# kafka_reliability/outbox/schema.py
def outbox_ddl(table: str = "outbox") -> str: ...                    # paste into your migration
def make_outbox_table(metadata: Any, table: str = "outbox") -> Any: ...  # SQLAlchemy Table

# kafka_reliability/outbox/retention.py
async def sweep_published(pool: Any, *, older_than: timedelta, chunk: int = 10_000,
                          table: str = "outbox") -> int: ...
```

## API sketch — dedup

```python
# kafka_reliability/dedup/store.py
class ClaimResult(Enum):
    CLAIMED = auto()        # first time — process it
    ALREADY_DONE = auto()   # completed before — skip
    IN_PROGRESS = auto()    # another worker holds a live lease — skip, do not commit offset

class DedupStore(Protocol):
    supports_transactions: bool

    async def claim(self, group: str, key: str, *, ttl: timedelta,
                    lease: timedelta | None = None, conn: Any | None = None) -> ClaimResult: ...
    async def confirm(self, group: str, key: str, *, conn: Any | None = None) -> None: ...
    async def release(self, group: str, key: str, *, conn: Any | None = None) -> None: ...
    async def purge(self, group: str, keys: Iterable[str]) -> int: ...
```

`claim`, not `seen`. A boolean `seen()` cannot be implemented race-free, and an
API that invites the race is the wrong API (`03-idempotent-consumer.md`). The
`conn` parameter is how a Postgres store joins the handler's transaction; it is
`None` for Redis, and `supports_transactions` lets the `Deduplicator` refuse the
strong mode rather than degrade silently.

```python
# kafka_reliability/dedup/keys.py
def from_header(name: str) -> Callable[[Record], str]: ...
def from_json_path(path: str) -> Callable[[Record], str]: ...
def topic_partition_offset() -> Callable[[Record], str]: ...
def payload_hash(algorithm: str = "sha256") -> Callable[[Record], str]: ...
```

```python
# kafka_reliability/dedup/deduplicator.py
class Deduplicator:
    def __init__(self, *, store: DedupStore, group: str,
                 key: Callable[[Record], str],          # no default, on purpose
                 ttl: timedelta = timedelta(days=7),
                 mode: Literal["transactional", "claim_confirm", "record_after"] = "transactional",
                 lease: timedelta = timedelta(minutes=5),
                 replay_policy: Literal["suppress", "bypass", "namespace"] = "suppress",
                 on_store_unavailable: Literal["fail_closed", "fail_open"] = "fail_closed") -> None: ...

    @asynccontextmanager
    async def process(self, record: Record, *, conn: Any | None = None) -> AsyncIterator[bool]: ...
```

The context manager is the whole ergonomic bet:

```python
async with dedup.process(record, conn=session) as should_process:
    if should_process:
        await handle(record, session)
```

Exiting normally confirms; an exception releases the claim so the message is
retried. It works identically inside FastStream, a raw `aiokafka` loop, or a
Celery task, because it knows nothing about any of them.

## API sketch — replay and DLQ

```python
# kafka_reliability/replay/dlq.py
class DlqRouter:
    def __init__(self, *, producer: Producer, topic: str | Callable[[Record], str],
                 consumer_group: str, include_error_message: bool = True,
                 max_error_bytes: int = 1024) -> None: ...

    async def route(self, record: Record, error: BaseException, *, attempts: int = 1) -> None: ...
```

```python
# kafka_reliability/replay/selector.py
@dataclass(frozen=True)
class Selection:
    topic: str
    from_offset: Mapping[int, int] | None = None
    to_offset: Mapping[int, int] | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None
    partitions: Sequence[int] | None = None
    predicate: Callable[[Record], bool] | None = None
    max_messages: int | None = None

class ReplaySelector:
    def __init__(self, *, consumer_factory: Callable[[], Any]) -> None: ...
    async def resolve(self, selection: Selection) -> ResolvedSelection: ...   # timestamps → concrete offsets
```

```python
# kafka_reliability/replay/runner.py
@dataclass(frozen=True)
class ReplayPlan:
    replay_id: str
    resolved: ResolvedSelection
    target_topic: str
    matched: int
    skipped: Mapping[str, int]          # reason → count
    samples: Sequence[Record]

@dataclass(frozen=True)
class ReplayOptions:
    target_topic: str                    # required; never inferred
    rate_per_second: float | None = 100.0
    max_replay_count: int = 3
    allow_same_topic: bool = False
    preserve_key: bool = True
    commit_source_offsets: bool = False
    audit_path: Path | None = None

class ReplayRunner:
    def __init__(self, *, selector: ReplaySelector, producer: Producer,
                 options: ReplayOptions) -> None: ...

    async def dry_run(self, selection: Selection) -> ReplayPlan: ...
    async def execute(self, selection: Selection) -> ReplayResult: ...
```

`dry_run` and `execute` share one traversal with a single branch at the produce
call, so what the dry run reports is what the execution does
(`04-replay-dlq.md`).

## Testing shape

Not an implementation detail — it constrains the API:

- `run_once()` on the relay and `dry_run()` on the replay runner exist partly so
  tests never need `sleep()`. Any design requiring a test to wait for a poll
  interval is the wrong design.
- An `InMemoryProducer` implementing the `Producer` protocol ships with the
  library and is public. Users need it to test their own code.
- A `Clock` protocol in `core` so TTL and lease expiry are testable without
  freezing global time.
- Postgres and Redis behaviour is tested against real servers (testcontainers or
  docker-compose), never fakes. `SKIP LOCKED` semantics, `ON CONFLICT` behaviour
  under concurrency, and Redis `SET NX EX` atomicity are exactly the things a
  fake gets wrong — and they are the things this library's correctness rests on.
