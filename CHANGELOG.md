# Changelog

Semantic versioning. Until 1.0 a minor bump may break the public API; after it,
only a major bump will. Entries are written per change, not at release time.

## Public API surface (a compatibility commitment)

Users implement or depend on these, so their signatures are versioned:

- `producers.port.Producer` and `producers.InMemoryProducer`
- `dedup.store.DedupStore` (with `Claim`, `ClaimResult`) and `dedup.Deduplicator`
- `metrics.MetricsSink`, `NullMetrics` and the metric names in `metrics.METRIC_SPECS`
- `outbox.relay.RelayStore` / `RelayConfig`, `replay.reader.Reader`
- the header names in `core.headers`
- the DDL emitted by `outbox_ddl()` and `dedup_ddl()`

Everything else (module internals, private helpers) may change in any release.

## 0.1.0 — unreleased

First release. Pre-alpha.

- **core / producers / metrics:** shared types, header constants, clock, errors;
  `Producer` protocol with aiokafka and confluent-kafka adapters and
  `InMemoryProducer`; `MetricsSink` with fixed, bounded metrics.
- **outbox:** schema and typed writers (asyncpg, psycopg, SQLAlchemy, Django);
  polling `OutboxRelay` (status-column claiming, advisory-lock election, key-hash
  sharding, `failed` rows that block their key, `stats()`); chunked
  `sweep_published`. Relay is asyncpg-only. DDL includes `outbox_failed_idx`.
- **dedup:** `DedupStore` protocol, key helpers, Postgres / Redis / SQLite /
  in-memory stores, `Deduplicator` (three modes, lease expiry, replay policy,
  fail-closed default, revocation signals), consumer recipes run in CI.
- **replay:** `DlqRouter` with `PermanentError` / `TransientError`, `Selection`,
  `ReplaySelector`, `ReplayRunner` (dry run / execute), safety rails, JSONL audit,
  `Reader` protocol with an aiokafka adapter, and the `kafka-reliability-replay` CLI.
- **contrib:** `OtelMetrics` (`[otel]`).
- Known gap: no real-broker/real-server CI yet (issue #61).
