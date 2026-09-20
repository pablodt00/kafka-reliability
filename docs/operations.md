# Operations guide

What an on-call engineer reads.

## Alerting

- **`outbox.oldest_pending_seconds`** is the alarm that matters: it means the
  relay has stalled and the outbox table is growing in your primary database.
  Also watch `outbox.pending`, `outbox.failed` (a row that will not publish and is
  blocking its key) and `outbox.relay_errors{kind}`.
- `dedup.store_errors` (store availability), `dedup.lease_expired` (a duplicate
  side effect was possible), `dedup.in_progress` (rebalance contention) and a
  sudden rise in `dedup.duplicate` (an upstream problem).
- `replay.skipped{reason}` explains why a replay was smaller than expected.

## TTL sizing

Set the dedup TTL **longer than your maximum replay window, not your maximum retry
window.** Replaying a 30-day-old DLQ with a 7-day TTL re-runs your handlers —
either what you wanted or a disaster. Users who never replay can cut it to hours.
Also keep published outbox rows (`sweep_published(older_than=...)`) at least as
long as you might need to inspect them.

## Vacuum and volume

The outbox and `processed_messages` are high-churn insert/delete tables, which is
what makes autovacuum expensive. Sweep in chunks (both sweeps are), watch dead
tuples and table bloat, and tune per-table autovacuum settings for your load —
**benchmark before adopting a specific `autovacuum_vacuum_scale_factor` number;
this library does not publish one.** At high volume, time-partition the table and
drop old partitions; that is documented, not automated. The relay's queries need
`outbox_pending_idx` and `outbox_failed_idx`; check with `EXPLAIN` that they are
used.

## Recommended replay workflow

1. **Dry run** — the default. Read the resolved offsets, the per-partition counts
   and the skipped-by-reason line.
2. **Shadow replay** to a test topic (`--to-topic orders.replay-test`).
3. **Verify** with a staging instance of the fixed consumer.
4. **Real replay**, with `--audit-log`, at a rate your downstream can take.

Before a real replay decide the dedup `replay_policy`: `suppress` (default) skips
messages already processed; `bypass` reprocesses; `namespace` dedups within this
run. Check `--max-replay-count` for poison messages.

## DLQ topology

One DLQ per source topic is the default. Use one per consumer group when several
groups fail differently, so billing's replay does not re-deliver to inventory. A
retry ladder (escalating-delay topics) handles transient failures but **destroys
ordering** — unusable for any key where order matters — and is yours to run.

**Share groups (Kafka 4.2)** count deliveries and archive a poison record in the
broker, which largely replaces the ladder; but share groups and per-key ordering
do not compose, so an outbox-fed, ordering-sensitive topic stays on consumer
groups. `DlqRouter` works under both; route before the delivery-attempt limit
archives the record. Revisit when KIP-1191 lands.
