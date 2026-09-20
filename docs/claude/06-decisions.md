# 06 — Decisions

> **Everything here is decided.** This file replaces an earlier
> `06-open-questions.md`; nothing in the design is left to the implementer to
> choose. Each entry gives the decision, the trade-off accepted, the
> alternatives rejected and why, and — because a decision without a reversal
> condition is dogma — **what evidence would overturn it**.
>
> Research current as of **2026-09-05**. Where a decision rests on a verified
> external fact, the fact is cited; where it rests on judgement, it says so.

Decisions that only affect one module are stated in that module's document and
summarised here. Decisions that span modules, or that were genuinely contested,
are argued here in full.

---

## D1 — The outbox table uses Debezium-compatible column names

**Decided: yes.** The default schema adopts the column names the Debezium
Outbox Event Router expects, and adds operational columns alongside them.

Verified 2026-09-05: the EventRouter SMT's default configuration expects
`id`, `aggregatetype`, `aggregateid`, `type` and `payload`; it routes by
`aggregatetype` to `outbox.event.${routedByValue}` and keys the record by
`aggregateid`.

This matters because `01-prior-art.md` concedes that Debezium is the better
relay for anyone running Kafka Connect. If our table is shaped like Debezium's,
**graduating from our polling relay to Debezium is a connector config change,
not a data migration** — you stop our relay, point Debezium at the same table,
and the events keep flowing. A library that makes its own replacement easy is
more adoptable, not less.

The schema in `02-outbox.md` therefore carries both sets of columns: the
Debezium five, plus `topic`, `status`, `attempts`, `last_error`, `created_at`,
`published_at` and `headers`, which Debezium ignores.

**Trade-off accepted:** Debezium keys the record from `aggregateid` and derives
the topic from `aggregatetype` + a route pattern, whereas our relay reads an
explicit `topic` column and a `key` column. Carrying both is redundant storage
(the topic is derivable, and `aggregateid` usually equals the key) and one more
thing to keep consistent. We accept the redundancy because the explicit `topic`
column is what makes our relay simple and what lets a single table feed
arbitrarily-named topics that no route pattern would produce.

`payload` is the sharper trade. Debezium's default expects JSON; `02-outbox.md`
wants `BYTEA` so that Avro and Protobuf payloads survive byte-for-byte. **Both
DDL variants ship**: `outbox_ddl(payload="bytea")` is the default and the correct
choice for binary formats, and `outbox_ddl(payload="jsonb")` is Debezium-native
for JSON shops. The cost is a documented incompatibility — if you chose `bytea`
and later want Debezium, you either convert the column or configure
`BinaryHandlingMode`. Naming that cost up front is better than discovering it
during a migration.

**Rejected:** designing the schema independently (what the first draft did). It
saves nothing and makes the graduation path a migration.

**Reversed if:** Debezium changes its default field names, or the redundant
columns prove to actually drift in practice.

---

## D2 — Outbox ordering defaults to strict, with sharding opt-in

**Decided: `ordering="strict"` by default** — one relay elected by a Postgres
advisory lock, rows walked in `seq` order.

**Trade-off accepted:** throughput is capped at one process. This is a real cost
and it is bounded: produces are pipelined and acks batched, so a single relay
sustains thousands of messages/second, which covers the target scale in
`00-overview.md` with room to spare.

**Rejected:** sharded `SKIP LOCKED` by default. It is faster and it silently
reorders per-key events for anyone who did not read the docs — a bug that only
appears under concurrency, which is to say in production, long after adoption. A
slow-but-correct default is recoverable by changing a config value; a
fast-but-wrong one corrupts a consumer's view of an aggregate and you find out
from a customer. **Also rejected:** making the field required with no default,
which is correct in spirit and hostile to a five-minute quickstart.

Sharding remains fully supported (`ordering="sharded"`, `shard_count`,
`shard_index`) and the rule is enforced in the implementation, not just
documented: **shards are computed from a hash of the message key, never from the
row id.** Sharding by row id is the trap, and the API should make it
unavailable rather than merely discouraged.

**Reversed if:** benchmarking shows the single-relay ceiling lands below the
target scale.

---

## D3 — Headers are `JSONB` with text values, validated at write time

**Decided: `headers JSONB`, values must be UTF-8 decodable, checked in
`enqueue()`.**

**Trade-off accepted:** binary header values must be base64-encoded by the
caller. Kafka headers are `bytes -> bytes`, so this is a genuine narrowing of
the underlying model.

It buys something specific and valuable: headers are readable in SQL. During an
incident, `SELECT headers->>'traceparent' FROM outbox WHERE status = 'failed'`
is exactly the query you want, and no binary encoding gives you it.

The validation placement is the important half. Rejecting a non-decodable value
**at `enqueue()`**, inside the user's transaction, turns a latent relay-time
failure into an immediate, local, obvious error at the call site that caused it.
A relay-time failure would surface in a background process, minutes later,
detached from the code responsible.

**Rejected:** base64 for all values (lossless, but discards the readability that
was the entire reason to choose JSONB); a length-prefixed `BYTEA` blob (opaque
to SQL); a `outbox_headers` side table (lossless and queryable, but adds an
insert per header to the one path that must stay cheap).

**Reversed if:** binary header values turn out to be common in practice. My
reading is that they are rare, and that is a judgement, not a measurement.

---

## D4 — The dedup module ships a context manager *and* consumer recipes, not a consumer

**Decided: `Deduplicator.process()` as the entire runtime API**, plus
integration recipes for `aiokafka`, `confluent-kafka`, FastStream and Celery
that live in the docs and are **executed in CI**.

**Trade-off accepted:** offset-commit ordering and rebalance handling remain the
user's responsibility, and those are exactly what people get wrong. The
mitigation is that the recipes are tested code, not prose — a recipe that CI
runs cannot silently rot, and copying 40 tested lines is a much smaller ask than
adopting a framework.

**Rejected:** shipping an opinionated `ReliableConsumer`. It would get commit
ordering right for those who took it, and it would drag a Kafka consumer
dependency into the dedup module, double the API surface, and start the library
down the road to being a consumer framework — which `00-overview.md` rules out
and which FastStream already does better.

To make the recipes short enough to be copied correctly, the module provides the
two pieces that are hard to write from scratch: `ClaimResult.IN_PROGRESS` tells
the caller **not** to commit the offset (so another worker's live lease is not
lost), and `Deduplicator.process()` releases the claim on exception so a failed
handler is retried rather than permanently suppressed.

**Reversed if:** the recipes turn out to be copied incorrectly often enough to
show up in bug reports.

---

## D5 — Postgres is the default dedup store

**Decided: Postgres default; Redis, SQLite and in-memory also ship.**

**Trade-off accepted:** every message costs an insert on the primary database,
so a 10k msg/s consumer adds 10k writes/s of pure overhead, plus a periodic
expiry sweep and its vacuum cost.

Postgres wins the default because it is the **only** backend that can put the
dedup record in the same transaction as the handler's side effects — the strong
`transactional` mode of `03-idempotent-consumer.md`. The quickstart should
demonstrate the strongest guarantee the library can offer, and it needs no
infrastructure the target user does not already run.

**Rejected as default:** Redis. It is faster, its TTL is free, and it keeps load
off the primary — and it cannot join the handler's transaction, it puts a new
availability dependency on the consumer's hot path, and its default persistence
can lose the last seconds of writes on a crash, which means duplicates
processed. All fine trade-offs to *choose*; wrong ones to *inherit* by default.

The `DedupStore` protocol exposes `supports_transactions`, so `Deduplicator`
raises at construction if `mode="transactional"` is requested with a store that
cannot honour it. Silent degradation from the strong mode to a weak one is the
failure this flag exists to prevent.

**Reversed if:** the Postgres insert overhead measurably degrades a consumer at
the target scale.

---

## D6 — An expired in-progress lease means reprocess, loudly

**Decided: reprocess.** The claim is released, the message is delivered again,
and a distinct metric (`dedup.lease_expired`) and log event fire every time.

The ambiguity is real and unresolvable locally: a worker that died holding a
lease may have died before its side effect or after it, and the side effect is
in another system. No local bookkeeping can distinguish the two.

**Trade-off accepted:** in exactly this case, a non-idempotent side effect can
run twice — in a module whose purpose is preventing that. This is an awkward
thing to have to document and it is the right call: at-least-once is the safe
direction, and losing work silently is worse than repeating it visibly. Users
whose side effect is a payment must additionally pass an idempotency key to
their payment provider, which is the correct architecture regardless of what
this library does.

**Rejected:** marking the record `abandoned` and alerting. It never duplicates
and it silently drops work whenever the alert is missed, which is most of the
time. **Rejected:** making it configurable with no default, which taxes every
user with a decision to spare us one.

**Default lease: 5 minutes**, configurable. Too short and a slow handler has its
own work duplicated underneath it; too long and a crashed worker blocks that key
for the duration. Five minutes is a judgement call sized to "longer than almost
any handler, shorter than anyone's patience" — not a measurement, and the docs
say so.

---

## D7 — Replay never transforms payloads

**Decided: no transform hook.** Replay reads records and republishes them
byte-for-byte, with only the `x-replay-id` / `x-replay-at` headers added.

**Trade-off accepted:** an operator who needs to fix one field in 400 dead
messages must write a script — the very ad-hoc-script situation this module
exists to replace. That is a real gap and it is the smaller cost.

What a transform hook destroys is auditability. The value of replay is that
"what went in came out", so the audit log plus the source coordinates fully
describe what happened. Once an arbitrary callable sits in the middle, "what did
that replay do" is unanswerable without the script that ran it, and that script
is not in version control at 2am.

**Rejected:** an unconstrained `Callable[[Record], Record | None]`. **Rejected
for now, and the shape it would take if reversed:** transforms permitted only
with a mandatory audit log recording input *and* output bytes for every record —
which preserves auditability at the cost of an audit file that may contain
sensitive payloads in full.

**Reversed if:** real users report the gap. Adding a hook later is easy; removing
one is not, which is why the restrictive choice goes first.

---

## D8 — Share groups are supported at the DLQ boundary; the retry ladder stays documented for consumer groups

**Decided**, and this one changed on research rather than being guessed.

Verified 2026-09-05: share groups (KIP-932) are generally available in Apache
Kafka 4.2. The broker acquires records under a time-limited lock (30s default),
supports per-record acknowledge/release/reject, counts delivery attempts, and
**archives a record once it hits the delivery attempt limit (5 by default)** —
broker-side poison-message protection. First-class DLQ routing is not in 4.2;
it is KIP-1191, targeting 4.4. *(The 4.4 target is a roadmap statement, not a
shipped fact.)*

What this means for the design, concretely:

- **The retry ladder is largely obsolete for share-group consumers.** Delivery
  counting and the archive limit are exactly what the ladder was emulating, done
  better and in the broker. `04-replay-dlq.md` keeps the ladder documented
  because classic consumer groups still need it, and marks it as such.
- **The retry ladder remains necessary for ordering-sensitive workloads**, which
  cannot use share groups at all: share-partition records may be delivered out
  of order, particularly on redelivery. Ordering per key is precisely what the
  outbox works to preserve, so an outbox-fed topic whose keys carry ordering
  requirements stays on consumer groups. These two features do not compose, and
  the docs must say so rather than recommend share groups generally.
- **`DlqRouter` works unchanged under both.** It routes a record the handler
  rejected; whether the consumer then commits an offset or acknowledges a
  record is the caller's business. Under share groups the useful placement is
  before the archive limit is reached — otherwise the record is archived and
  the application never sees it again.
- **The replay module is unaffected.** It reads a topic by offset or timestamp
  and produces to another; neither end has a consumer-group concept in it.

**Trade-off accepted:** the DLQ documentation must now explain two topologies
rather than one, and must be revisited when KIP-1191 lands, since native DLQ
routing would make our `DlqRouter` redundant for share-group users. Saying that
now is better than being quietly superseded.

---

## D9 — Backend-specific writer classes, not `conn: Any`

**Decided: explicit typed classes per backend** —
`AsyncpgOutboxWriter`, `PsycopgOutboxWriter`, `SqlAlchemyOutboxWriter`,
`DjangoOutboxWriter`, each with a correctly-typed `conn`/`session` parameter, plus
sync variants where the driver has one.

**Trade-off accepted:** more classes to maintain and document, and switching
drivers is an import change rather than a no-op. That last point is arguably a
feature: switching your database driver *is* a change, and having it show up in
the diff is honest.

**Rejected:** the single `OutboxWriter` with `conn: Any` and runtime dispatch
that `05-architecture.md` originally sketched. It works with everything and
type-checks nothing, so passing a connection pool where a connection was
expected — a mistake that quietly breaks the pattern's atomicity guarantee, the
entire point of the library — fails at runtime with whatever message we
remembered to write. A type checker catching it at the call site is worth three
extra classes. **Rejected:** a `Connection` protocol, which fits neither
SQLAlchemy sessions nor Django's connection handling cleanly.

The extras map onto this exactly: `[outbox-asyncpg]`, `[outbox-psycopg]`,
`[outbox-sqlalchemy]`, `[outbox-django]`.

---

## D10 — One distribution named `kafka-reliability`

**Decided: a single distribution, single top-level package `kafka_reliability`,
independence enforced by CI rather than by packaging.**

**Trade-off accepted:** module independence is a convention that a careless
import could break, rather than a structural impossibility. Mitigated by making
it a test: an import-graph check asserts that `outbox`, `dedup` and `replay`
never import one another, and a separate test asserts no Kafka client is in
`sys.modules` after importing the outbox writer. A rule that CI enforces is
about as strong as a packaging boundary and far cheaper.

**Rejected:** three distributions sharing a namespace package. It would make
independence structural, and it would mean three release cycles, three
changelogs and three version-compatibility matrices for one small library —
plus namespace packages' long history of confusing exactly the users who would
be installing only one of them.

The name stays `kafka-reliability` (distribution) / `kafka_reliability`
(package). It is boring, it slightly overclaims, and it is what someone would
type into a search box. `00-overview.md` does the honest scoping in prose, which
is where scoping belongs.

---

## D11 — Metrics via a callback protocol, with an optional OpenTelemetry adapter

**Decided:** a `MetricsSink` protocol (`counter`, `gauge`, `histogram`) that
users wire to whatever they run, plus a ready-made OTel adapter behind an
`[otel]` extra. Default sink is a no-op.

**Trade-off accepted:** users on Prometheus write a small adapter. The
alternative — depending on OpenTelemetry directly — makes a real dependency out
of a library that mostly does not need one, and OTel's Python API has moved
under people before.

The metric set is fixed here, because metrics define what operators can alert
on and adding them later means adding alerts later:

| Metric | Type | Labels | Why |
|---|---|---|---|
| `outbox.pending` | gauge | `table` | backlog size |
| `outbox.oldest_pending_seconds` | gauge | `table` | **the alarm that matters** — relay stalled |
| `outbox.published` | counter | `table`, `topic` | throughput |
| `outbox.failed` | counter | `table`, `topic` | rows that will not publish |
| `outbox.relay_errors` | counter | `table`, `kind` | produce failures |
| `dedup.claimed` / `dedup.duplicate` | counter | `group` | duplicate rate — a spike means an upstream problem |
| `dedup.in_progress` | counter | `group` | rebalance contention |
| `dedup.lease_expired` | counter | `group` | D6's visible failure |
| `dedup.store_errors` | counter | `group`, `kind` | store availability |
| `replay.produced` | counter | `source_topic`, `target_topic` | audit |
| `replay.skipped` | counter | `source_topic`, `reason` | why a replay was smaller than expected |

**Labels never include the dedup key, the message key, the partition, or the
offset.** Unbounded label cardinality is how a metrics backend gets taken down
by a library, and it is a much easier mistake to make than to detect.

---

## D12 — Python 3.11+, Kafka 3.5+ supported, 4.x recommended

**Decided.** Python 3.11 minimum: `asyncio.TaskGroup` and `ExceptionGroup` are
directly useful in a relay supervising concurrent produces, and `Self` cleans up
the builder-ish APIs. **Trade-off:** it excludes 3.10, which some users are
still on; the cost of supporting 3.10 is hand-rolled task supervision in the one
place where getting it wrong loses messages.

Kafka 3.5+ is the tested floor, and nothing in the design requires a 4.x-only
behaviour — verified by review of the design, **not** against a live 3.5 cluster,
so the CI matrix must actually test it. 4.x is recommended, and the docs explain
what it improves: KIP-848's incremental rebalances shorten the window in which
duplicate delivery happens (D6, `03-idempotent-consumer.md`), KIP-890 hardens
transactions, and 4.2 brings the share groups of D8.

---

## D13 — Coverage: which backends and integrations ship

Added in response to the requirement that the library be usable in as many
projects as possible. The shape of the coverage follows from the module
boundaries, so widening it is cheap where the boundary is narrow and expensive
where it is not.

**Outbox writers** (each typed, per D9): asyncpg, psycopg 3 (sync + async),
SQLAlchemy Core/ORM (sync + async), Django ORM. These four cover essentially
every Python-on-Postgres project. Django matters disproportionately: it is a
large fraction of the target audience and its transaction handling
(`atomic()`, `on_commit()`) is idiomatic enough that a generic adapter would be
wrong. **Trade-off:** four adapters is four times the integration-test matrix,
and they must all be tested against a real Postgres, since transaction
enlistment is exactly what a mock cannot verify.

**Dedup stores:** Postgres (strong mode), Redis, SQLite, in-memory. SQLite makes
the library usable in single-node deployments and in tests without a container;
in-memory is for unit tests and is documented as unsafe for anything else — it
does not survive a restart, so it silently provides no guarantee across the one
event that most needs it. **Rejected:** MySQL, DynamoDB, Memcached. Each is a
plausible ask and none is close enough to the target audience to justify the
support surface; the `DedupStore` protocol is public and small enough that a
user can implement one in an afternoon, which is the intended answer.

**Producers:** aiokafka, confluent-kafka, in-memory (public, for user tests), and
any user-supplied object satisfying the `Producer` protocol — which is how a
FastStream publisher gets used without the library knowing FastStream exists.

**Consumer-side integration:** recipes only, per D4 — `aiokafka`,
`confluent-kafka`, FastStream middleware, Celery task. Executed in CI.

**Deliberately still not covered**, because these would change what the library
*is* rather than widen where it runs: other brokers (RabbitMQ, NATS, SQS — the
patterns generalise, the partition-and-offset model does not), other databases
for the *outbox* specifically (`SKIP LOCKED`, transactional DDL and identity
semantics are load-bearing), CDC relay (D1 makes graduating to Debezium a config
change instead), schema registry and serialization (payloads are `bytes`), and
consumer-loop ownership (D4). `00-overview.md` holds the full non-goals list;
these are the ones someone will specifically ask for.

---

## D14 — Producer semantics: ack-on-return, `ProducerError`, poll-thread bridge for confluent-kafka

**Decided:**

- **`send` returns only after the broker has acknowledged the message**, and
  raises `ProducerError` (a `core` exception, original chained as `__cause__`)
  if it cannot. The outbox relay marks a row sent as soon as `send` returns, so
  a `send` that returned early would turn a crash into silent loss.
  `flush(timeout)` waits for anything in flight and raises `ProducerError` if
  messages remain when it expires. The protocol has **two** methods, `send` and
  `flush`.
- **Adapters raise `ProducerError`, not their client's exceptions**, so callers
  never import `aiokafka` or `confluent_kafka` to catch a failure. It is a new
  class rather than a reuse of `RelayError` because replay and the in-memory
  producer raise it too, and "relay" would mislead them.
- **`acks=all` and idempotence are enforced in the convenience factories**
  (`create_producer`): restating them is fine, weakening either raises
  `ConfigurationError`. A client the caller builds and hands in is theirs; the
  library cannot see its config and documents the requirement instead
  (`02-outbox.md`).
- **The confluent-kafka adapter drives `poll()` from a background thread** and
  resolves a per-message future through `loop.call_soon_threadsafe`.

**Trade-off accepted:** awaiting the ack per `send` means a caller that wants
throughput must issue sends concurrently (`asyncio.gather`) rather than rely on
fire-and-forget; both adapters pipeline concurrent sends. The confluent adapter
owns a thread that runs while it is open, so `close()` must flush *before*
stopping the poller. The factories refuse weaker settings even for users who
have a reason for them; they can hand in their own client.

**Rejected:** fire-and-forget `send` plus a callback (the relay would need its
own ack bookkeeping and the protocol would grow); the confluent-kafka 2.x
`AIOProducer` (pins a minimum version for a newer API and was not verifiable
against a real broker when this was decided); reusing `RelayError`.

**What would reverse it:** real-broker evidence (issue #61) that the poll thread
is a throughput bottleneck or misbehaves on shutdown would justify moving to
`AIOProducer`; a user need for batching semantics the protocol cannot express
would justify a `send_batch` — not silently widening `send`.

---

## D15 — Relay implementation: dedicated connection, per-shard advisory locks, one attempt per pass

**Decided:**

- **The relay runs on one dedicated `asyncpg.Connection`** (not a `Pool`),
  behind a small `RelayStore` protocol. asyncpg is the only relay driver for
  now (`[outbox-asyncpg]`); the protocol is what lets relay behaviour be tested
  without a database and leaves room for other drivers.
- **Leadership is a session-level advisory lock on that same connection**, so
  losing the connection loses the lock *and* fails the next query. The relay
  then raises `StoreUnavailableError` and stops rather than continuing unlocked.
- **Sharded mode takes one advisory lock per `(key, shard_index)`** instead of
  relying on `FOR UPDATE SKIP LOCKED`. Shards are disjoint by construction, so
  the only way two relays contend is two relays started with the same
  `shard_index` — which the per-shard lock prevents. `SKIP LOCKED` would have
  meant holding a transaction open across every Kafka produce and committing
  the mark-sent writes only once per batch, growing the duplicate window on a
  crash from a few rows to the whole batch.
- **The shard filter is `abs(hashtext(aggregateid)::bigint) % shard_count`.**
  `hashtext` can return negatives and Postgres `%` keeps the sign, so an
  un-`abs`ed filter would match no shard for negative-hash keys and stall them.
- **A `failed` row blocks its key** via `NOT EXISTS` on a new partial index
  `outbox_failed_idx (aggregateid) WHERE status = 'failed'`. The empty key
  ("no ordering requirement") is never blocked and each such row is its own
  ordering unit.
- **One produce attempt per key per pass.** `attempts`/`last_error` persist
  across passes and the row goes `failed` at `max_attempts`; `run()` backs off
  exponentially between failing passes. There is no per-row retry timestamp
  (it would be another column), so backoff is per relay, not per row.
- **Mark-sent is one `UPDATE` per key group**, after that group's sends.

**Trade-off accepted:** a transient failure on one key slows the polling cadence
of all keys (bounded by `max_backoff`); a crash mid-group republishes that
group's already-acked rows (duplicates, never loss). The DDL gained an index, so
existing outbox tables need `CREATE INDEX ..._failed_idx`.

**Rejected:** `SKIP LOCKED` with one transaction per batch (above); a pooled
connection (the lock would belong to whichever session ran the query); an
in-relay retry loop with sleeps (blocks the whole pass on one bad key).

**Reversed if:** real-Postgres testing (issue #61) shows the per-pass `NOT
EXISTS` or the three stats queries costing measurably at target scale, or users
need per-row backoff badly enough to justify a `next_attempt_at` column.

---

## D16 — Dedup API details that refine D4–D6

**Decided:**

- **`claim()` returns a `Claim(result, lease_expired)`**, not a bare `ClaimResult`.
  D6 requires a distinct metric and log *every time* a lease expires, and only the
  store can see that the row it took over was an expired `in_progress` one. Redis
  cannot (an expired key is an absent key), so it never reports it; it still
  reprocesses.
- **`process()` yields a `Decision`, not a bool.** It is truthy when the handler
  should run and carries `commit_offset`, false only for `IN_PROGRESS`. D4 says
  `IN_PROGRESS` must tell the caller not to commit the offset; a plain bool cannot.
  Recipes wait and retry the same record rather than skip past it, because
  committing a *later* offset would lose the skipped message if the lease holder
  dies.
- **`mode` defaults to `None`**: `transactional` when `process()` is given a `conn`,
  `claim_confirm` otherwise. A `conn` against a store that cannot join a
  transaction **raises**; it never downgrades. `record_after` is explicit only.
- **`is_done()` exists for `record_after` alone.** It is the one read-then-write in
  the protocol, racy by design and documented as such.
- **`purge()` is one method:** `(group, key)` forgets a record (pre-replay);
  without `key` it sweeps expired rows in chunks. Stores that expire natively
  return 0 for a sweep.
- **Transactional mode does not release on exception.** The caller's transaction
  rollback removes the dedup row; a `DELETE` on an aborted transaction would fail
  and mask the real error.
- **Postgres times come from the database clock** (`now()`), never the application's.
- **The Postgres claim is up to three statements** on the duplicate path (insert,
  take over if expired, read state) so it can report `lease_expired`; the happy
  path is one.
- **Only asyncpg** for the Postgres store; SQLite uses stdlib `sqlite3`, synchronous
  on the event loop (microsecond statements, single writer).

**Trade-off accepted:** `Claim`/`Decision` are slightly more API than the sketch in
D4. **Reversed if:** a second driver makes the per-statement claim too slow (fold it
into one CTE).

---

## D17 — Replay reads through a `Reader` protocol; the CLI's wiring lives in `contrib`

**Decided:**

- **`replay` reads Kafka through a small `Reader` protocol**
  (`partitions`, `beginning_offsets`, `end_offsets`, `offsets_for_times`, `read`,
  `commit`) with an aiokafka adapter (`[aiokafka]`) and an `InMemoryReader`. The
  read side has no delete, no live-group seek and no config call, so D7's "never
  destroys anything" is structural. `ReplaySelector` takes a `reader`, replacing
  the `consumer_factory` sketched earlier.
- **Ranges are half-open `[from, to)`**, and the printed plan
  (`184100 → 184260 (160 records)`) is reconstructible by offset through
  `ResolvedSelection.as_selection()`.
- **`replay/cli.py` is a click command that receives its Kafka clients** as click's
  `obj` (`Factories`); the aiokafka wiring is `contrib/replay_cli.py`, the console
  script. The existing boundary test forbids `outbox`/`replay` importing a concrete
  producer adapter; a composition root in `contrib` honours it instead of
  weakening it.
- **The CLI filter is a constrained expression** (`header == 'v' [and ...]`), never
  `eval`.
- **Confirmation** above `confirm_above` (10,000) compares the *offset span*
  (an upper bound) before reading; `--execute` on the CLI additionally runs a dry
  run and prompts with the real matched count, unless `--yes`.
- **A malformed `x-dlq-replay-count` is skipped** (`invalid_replay_count`): if the
  poison guard cannot be evaluated, do not replay.

**Trade-off accepted:** `AiokafkaReader` is verified against fakes only until the
broker harness (issue #61) exists. **Reversed if:** a second consumer client needs
first-class support — add it as another `Reader`, not a new code path.
