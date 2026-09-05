# 01 — Prior art

> All version numbers and release dates in this document were checked on
> **2026-09-05**, mostly against the PyPI JSON API and vendor release
> announcements. Anything I could not verify directly is marked
> *(unverified)*. Version-dependent claims rot fast; re-check before quoting
> them anywhere user-facing.

The purpose of this survey is to answer one question honestly: *is this library
redundant?* For several of the tools below the answer is "partly, and here is
exactly which part". For at least one — Debezium — the answer is "yes, if you
can run it, use it". Writing a library whose docs cannot say that is how you end
up with a library nobody should adopt.

## Kafka clients

### confluent-kafka-python — 2.15.0 (2026-06-30)

The Confluent-maintained client, a thin Python layer over `librdkafka`. It is
the reference implementation in practice: transactional producer API
(`init_transactions` / `begin_transaction` / `send_offsets_to_transaction` /
`commit_transaction`), idempotent producer, full consumer group support,
Schema Registry integration, and since the 2.x line an `AIOProducer` /
`AIOConsumer` pair for asyncio applications. It tracks new KIPs quickly because
`librdkafka` does.

**What it does not do:** nothing above the protocol. There is no outbox, no
deduplication store, no DLQ tooling, no replay. Its idempotence guarantee is
producer-side — it prevents *the client's own retries* from writing duplicate
records to a partition. That is a different and much narrower thing than a
consumer recognising that it has already processed a business event, and
conflating the two is the single most common misunderstanding in this space.

**Overlap with this library:** none, by construction. It is a dependency
candidate, not a competitor.

### aiokafka — 0.14.0 (2026-04-29)

Pure-Python asyncio client, originally derived from `kafka-python`. Supports
consumer groups and the transactional producer API. Its feature set trails
`librdkafka` and its throughput is lower, which is the expected cost of pure
Python; in exchange it has no C build step and integrates naturally with an
asyncio event loop.

**What it does not do:** same answer as above — it is a client. Community
write-ups about "exactly-once with aiokafka" are describing how to compose the
transactional API with careful offset handling, which is precisely the kind of
discipline this library should package rather than leave as a blog post.

**Overlap:** none. Also a dependency candidate. The choice between the two
should be the *user's*, which is an argument for keeping the client behind a
narrow internal port (`05-architecture.md`).

### kafka-python / kafka-python-ng — 2.2.3 (2024-10-02)

The original pure-Python client and its community fork. As of this writing the
fork's most recent PyPI release is nearly two years old. It should be treated as
legacy: not a support target, not a dependency.

## Frameworks

### FastStream — 0.7.5 (2026-08-27)

The most relevant framework in this space. FastStream gives you FastAPI-shaped
ergonomics for message brokers: decorator-based subscribers and publishers,
Pydantic/msgspec validation, dependency injection, AsyncAPI doc generation,
in-memory testing, and a common surface across Kafka, RabbitMQ, NATS, Redis and
MQTT.

**What it does not do:** FastStream is a *transport* framework. It does not
solve the dual-write problem, does not ship a deduplication store, and does not
provide replay tooling. Its Kafka support includes the usual client-level
knobs (partitions, consumer groups, batching) but the reliability patterns above
the transport are left to the application.

**Overlap:** the boundary is clean and this library should stay on its side of
it. A user should be able to run a FastStream subscriber whose handler body
consults this library's dedup store, and a FastStream publisher fed by this
library's outbox relay. Reimplementing routing or serialization would be
duplicated effort and a worse product.

### faststream-outbox — 0.13.1 (2026-07-27)

**This is the closest thing to a direct competitor and deserves a fair
hearing.** It implements the transactional outbox as a FastStream broker: you
call `broker.publish(body, queue=..., session=session)` inside your SQLAlchemy
transaction, a Postgres table (built via `make_outbox_table`) receives the row in
that same transaction, and a polling subscriber relays rows to a real broker
(Kafka, RabbitMQ, NATS, Redis) via a stacked decorator. The project is
well-presented — 100% coverage badge, active releases through mid-2026.

Its own documentation is explicit that handlers must be idempotent because a
crash between the side effect and the outbox row's `DELETE` re-delivers the
message; that is the correct semantics and the correct thing to say.

**Where it differs from what is proposed here:**

- It is *FastStream-native by design*. The outbox is modelled as a FastStream
  broker, and the relay as a FastStream subscriber. That is elegant if you are
  on FastStream and a hard adoption barrier if you are not — a Django or Celery
  team would be pulling in a broker framework to get a Postgres table and a
  polling loop.
- It is SQLAlchemy-coupled (session-based publish, `MetaData`-based table
  construction).
- It solves one of the three problems. There is no dedup store and no replay
  tooling.

**Honest conclusion:** for a team already on FastStream that only needs the
outbox, `faststream-outbox` is very likely the better choice, and this
library's README should say so rather than pretend otherwise. The case for a
separate library rests on (a) framework independence and (b) the other two
modules. If those two things stop being true, this library has no reason to
exist. *(I was unable to fetch its full documentation — the docs host is blocked
from this environment — so the description above comes from its PyPI
description and search results; the characterisation of its internals is
unverified beyond that.)*

### outbox-streaming — 0.1.0 (2022-07-25)

A Python transactional-outbox implementation whose only release is from 2022 and
which its own README described as early-stage and not production-ready. It is
effectively abandoned. Worth citing as evidence that the need is real and that
previous attempts have not stuck, not as a live alternative.

## CDC and the JVM ecosystem

### Debezium — 3.4.0.Final stable (December 2025); 3.6 in release-candidate stage as of mid-2026 *(the 3.6 status is unverified — check debezium.io/releases before relying on it)*

Debezium reads the Postgres WAL via logical decoding and turns row changes into
Kafka records. Combined with the **Outbox Event Router** SMT
(`io.debezium.transforms.outbox.EventRouter`), it reads an outbox table and
reshapes each row into a clean message on a per-aggregate topic. This is the
canonical, most battle-tested implementation of the outbox relay in existence,
with a MongoDB variant and a Quarkus extension for the write side.

**What it does not do:** nothing on the consumer side. Debezium is a source
connector; deduplication and DLQ replay are not its problem. Kafka Connect's
own `errors.deadletterqueue.topic.name` provides DLQ *routing* for connectors,
not replay.

**Overlap — and the honest bit:** for the relay half of the outbox pattern,
Debezium is strictly better than a polling relay on every axis except
operational cost. It has lower latency (WAL-driven, not poll-interval-driven),
it does not add read load to the primary, it handles ordering and offsets with
far more care than a first-party poller will, and it has years of production
hardening. **If you are already running Kafka Connect, use Debezium for the
relay.** The argument for a Python polling relay is narrow and entirely
operational: a Connect cluster plus a replication slot plus connector config is
a large fixed cost, and for a team with a handful of topics and a modest event
rate, a library that runs inside a process they already deploy is a genuinely
different product. That is a real trade-off, not a technical superiority claim,
and the docs must present it that way. See `02-outbox.md` for the mechanics of
both and `06-open-questions.md` for whether a CDC-backed relay should be offered
later.

### Spring / JVM idempotency and DLQ helpers

Spring Kafka ships `DeadLetterPublishingRecoverer` and a retry/backoff topic
topology out of the box, and the JVM ecosystem has well-known idempotent-consumer
recipes (a processed-message table written in the same transaction as the
business change). These are the reference designs this library's consumer side
should copy rather than invent. The relevant observation is that **Python has no
equivalent** — the patterns are described in blog posts and reimplemented per
service.

### Commercial DLQ tooling

Conduktor and Kpow both provide DLQ inspection and replay through a UI, with
RBAC and audit trails; some managed CDC vendors gate replay behind support
tickets. These validate the operational need. They are not substitutes for a
library: they are UIs for platform teams, not something a service repo can
depend on, script in CI, or run in a `make replay-dlq` target. A CLI and a
Python API occupy a different niche.

## Kafka itself (4.2.1, May 2026; 4.2.0 in February 2026)

Two recent platform changes are directly relevant and should shape the design
rather than be ignored:

- **KIP-848**, the broker-driven incremental rebalance protocol, is GA as of
  Kafka 4.0 and enabled with `group.protocol=consumer`. Rebalances are
  incremental and much shorter. This reduces the *frequency* of duplicate
  delivery at rebalance boundaries but does not remove it, so the dedup module's
  reason for existing is unchanged. It does affect how long a dedup key must
  plausibly live (`03-idempotent-consumer.md`).
- **Share groups (queues)** became production-ready in 4.2, and Kafka Streams
  gained dead-letter-queue support in the same release. Share groups change the
  per-partition-ownership model that a lot of DLQ-and-retry topology advice
  assumes. This library targets classic consumer groups; whether share groups
  make some of the retry-topic topology in `04-replay-dlq.md` obsolete is an
  open question, recorded as such. *(I have not evaluated share groups against
  these patterns in any depth — treat that as speculative.)*

Also worth stating plainly, because it is the most common objection: Kafka's
transactional producer (with KIP-890's server-side hardening in 4.0) gives
exactly-once semantics for read-process-write *within Kafka*. It does not extend
to a Postgres commit or an outbound HTTP call. Every problem this library
addresses lives precisely at that boundary.

## Summary judgement

| Tool | Solves outbox | Solves dedup | Solves replay | Verdict |
|---|---|---|---|---|
| confluent-kafka-python 2.15.0 | no | producer-side only | no | dependency, not competitor |
| aiokafka 0.14.0 | no | producer-side only | no | dependency, not competitor |
| FastStream 0.7.5 | no | no | no | complementary; do not compete |
| faststream-outbox 0.13.1 | yes (FastStream-coupled) | no | no | **use it if you are on FastStream and only need the outbox** |
| outbox-streaming 0.1.0 | partially | no | no | abandoned |
| Debezium 3.4 | yes (relay half, better) | no | no | **use it if you run Kafka Connect** |
| Spring Kafka | yes (JVM) | yes (JVM) | partial (JVM) | reference design, wrong language |
| Conduktor / Kpow | no | no | yes (UI) | validates need; not a dependency |

The remaining gap — framework-agnostic Python, all three modules, independently
adoptable, honest about at-least-once — is real but narrower than it first
appears. The library is justified by breadth and neutrality, not by doing any
single one of these things better than the specialists.

## Sources

- [confluent-kafka on PyPI](https://pypi.org/project/confluent-kafka/)
- [aiokafka documentation](https://aiokafka.readthedocs.io/en/stable/kafka-python_difference.html)
- [FastStream](https://github.com/ag2ai/faststream) and [Kafka routing docs](https://faststream.ag2.ai/0.5/kafka/kafka/)
- [faststream-outbox on PyPI](https://pypi.org/project/faststream-outbox/)
- [outbox-streaming](https://github.com/hyzyla/outbox-streaming)
- [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html), [Debezium releases](https://debezium.io/releases/)
- [Apache Kafka 4.2.0 release announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/), [4.2.1](https://kafka.apache.org/blog/2026/05/30/apache-kafka-4.2.1-release-announcement/)
- [KIP-848 consumer rebalance protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [Conduktor: building idempotent consumers](https://www.conduktor.io/blog/building-idempotent-consumers), [DLQ glossary](https://www.conduktor.io/glossary/dead-letter-queues-for-error-handling)
- [Confluent: Kafka dead letter queue](https://www.confluent.io/learn/kafka-dead-letter-queue/)
- [Push-based outbox with Postgres logical replication](https://event-driven.io/en/push_based_outbox_pattern_with_postgres_logical_replication/)
