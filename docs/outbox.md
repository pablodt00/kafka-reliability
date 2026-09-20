# Outbox

Get an event out of a database transaction and into Kafka. Adopt it alone — it
never imports `dedup` or `replay`. Design: [`claude/02-outbox.md`](claude/02-outbox.md).

- **Write path** — `AsyncpgOutboxWriter`, `PsycopgOutboxWriter`,
  `SqlAlchemyOutboxWriter`, `DjangoOutboxWriter`: one typed class per driver, on
  the connection or session *you* hold. The write path never imports a Kafka client.
- **Schema** — `outbox_ddl()` / `make_outbox_table()` / `django_migration()`.
  Debezium-compatible column names; `payload` is `bytea` by default, `jsonb` if
  you prefer.
- **Relay** — `OutboxRelay(AsyncpgRelayStore(conn), producer, RelayConfig(...))`.
  `run_once()` for tests and schedulers, `run(stop=)` for a service, `stats()`
  for the backlog. asyncpg only.
- **Retention** — `sweep_published(pool, older_than=timedelta(days=7))`, chunked;
  you schedule it.

**Ordering.** Per key (`aggregateid`), never global. `ordering="strict"` (default):
one relay elected by an advisory lock. `ordering="sharded"`: `shard_count` relays,
each with a `shard_index`; the shard is a hash of the *key*, never of the row id.

**Failures.** A produce failure increments `attempts` and stores `last_error`; at
`max_attempts` the row is `failed` and **blocks its own key** (never skipped,
never reordered) while other keys flow. Fix the cause, then reset the row
(`UPDATE outbox SET status='pending', attempts=0 WHERE id = ...`).

**Refuses to guarantee:** no duplicates (consumers must deduplicate), latency
below the poll interval, throughput beyond one process in strict mode.
