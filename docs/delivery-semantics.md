# Delivery semantics

**The target is at-least-once delivery with effectively-once processing.** This
library never promises "exactly once" without that qualification; an unqualified
claim in any doc or docstring is a bug (a test enforces it).

## What each module guarantees — and refuses to

**Outbox.** An event enqueued in your transaction is published to Kafka *at least
once*, and never for a transaction that rolled back. *It converts a correctness
problem (lost or phantom events) into a duplicates problem.* The relay's Kafka
ack and its "mark sent" write are themselves a dual write: a crash between them
republishes the row, with the same `x-event-id`. Refuses to guarantee: no
duplicates, ordering across keys, or delivery if you point it at a topic that
cannot accept the message (that row is marked `failed` and blocks its key).

**Dedup.** *It does not make a consumer idempotent.* It turns duplicate
*deliveries* into single *processing* — but only when the dedup record and the
handler's side effects commit together:

| Mode | Holds when | Otherwise |
|---|---|---|
| `transactional` (Postgres / SQLite) | side effects are writes on the connection you pass in | — this is the strong mode |
| `claim_confirm` | side effects are elsewhere | a crashed worker's expired lease is reprocessed, so the side effect may run twice (D6) |
| `record_after` | the handler is already idempotent | a crash after the handler and before the record reprocesses; concurrent duplicates can both run |

If you can make the handler naturally idempotent (an upsert on a business key, a
conditional update), do that instead — a dedup store is a stateful dependency on
the hot path of every message.

**Replay.** Republishes byte-for-byte and can never un-send. It creates
duplicates deliberately, which is why the dedup layer has a `replay_policy`
(default `suppress`).

## The window nobody can close

An expired `in_progress` lease means either "the worker died before its side
effect" or "after it, before confirming". No local bookkeeping can tell, because
the side effect lives in another system. The library reprocesses (losing work
silently is worse than repeating it visibly) and reports every occurrence via
`dedup.lease_expired`. If the side effect is a payment, send an idempotency key to
the provider — that is the right design regardless of this library.

## What you must do

- `enable.auto.commit=false`, and commit **after** the handler and the dedup
  record. Committing first turns every crash into silent loss.
- Pick a dedup key that means "the same event" (`x-event-id`), not "the same
  bytes" or "the same offset".
- Keep the TTL longer than your maximum *replay* window.
- Producer `acks=all` + idempotence (the factories enforce it; a client you build
  yourself is yours to configure).

Kafka's own read-process-write transactions give exactly-once *within a single
Kafka cluster* only; they do not extend to a database or an HTTP call, which is
where these patterns live.
