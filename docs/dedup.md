# Dedup (idempotent consumer)

Turns at-least-once delivery into effectively-once *processing* when the dedup
record commits with your side effects. Adopt it alone. Design:
[`claude/03-idempotent-consumer.md`](claude/03-idempotent-consumer.md).

```python
async with dedup.process(record, conn=conn) as decision:
    if decision:
        await handle(record, conn)
if decision.commit_offset:     # False only for IN_PROGRESS: another worker's live lease
    await consumer.commit()
```

- **`Deduplicator`** — `store`, `group`, `key` (required, no default), `ttl=7d`,
  `mode`, `lease=5m`, `replay_policy="suppress"`, `on_store_unavailable="fail_closed"`.
  Modes: `transactional` (a `conn` is passed and the store can join it),
  `claim_confirm` (no `conn`), `record_after` (explicit only; weak). A `conn`
  against a store that cannot join a transaction raises — no silent downgrade.
- **Keys** — `keys.from_header(EVENT_ID)` (preferred), `from_json_path`,
  `payload_hash`, `topic_partition_offset`; each docstring says what it cannot catch.
- **Stores** — `PostgresDedupStore` (only one with the strong mode),
  `RedisDedupStore`, `SqliteDedupStore`, `InMemoryDedupStore` (**unit tests only:
  no guarantee across a restart**). Implement `DedupStore` for anything else.
- **Rebalances** — `dedup.revocations` exposes one `asyncio.Event` per
  assignment. It is a signal, not a cancel: the library never interrupts a running
  handler; the new owner's claim is what prevents the double side effect.
- **Recipes** — [`docs/recipes/`](recipes/README.md): aiokafka, confluent-kafka,
  FastStream, Celery; run in CI.

**Refuses to:** own the consumer loop, deserialize payloads, or silently degrade.
Redis cannot join your transaction and cannot report `lease_expired`; see its
docstring for the durability costs.
