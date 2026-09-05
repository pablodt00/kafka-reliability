# 06 — Open questions

> **Everything in this document is UNDECIDED.** Decisions recorded in
> `02`–`05` are provisional-but-committed; these are not committed at all. Do not
> read a preference expressed here as a decision. Where I have a lean, it is
> labelled as a lean and should lose to evidence.

Each entry states the question, the options with their trade-offs, what would
resolve it, and whether it must be settled before v1 or can wait.

---

## Q1 — Should the outbox offer a CDC-backed relay later?

**Blocks v1?** No. Blocks the schema, though.

`02-outbox.md` decides polling for v1 because a Python logical-decoding client
that mismanages a replication slot can fill the primary's disk, and because
"nothing else to run" is the library's adoption case. But polling forces us to
solve late-commit ordering, adds latency, and adds query load — all of which CDC
gets right for free.

- **Polling only, forever.** Simple, one code path, one set of failure modes.
  Permanently worse latency and ordering than the alternative, and users who
  outgrow it leave for Debezium — which is fine, and should be documented as the
  graduation path.
- **Add a logical-replication relay behind the same interface.** Better on every
  technical axis. Costs slot management, `wal_level=logical`, a `pgoutput` or
  `wal2json` decoder, and a *much* worse failure mode when a consumer stalls.
- **Support Debezium's outbox table conventions** without implementing CDC —
  i.e. make our schema compatible with `EventRouter`'s expected columns so a
  user can point Debezium at our table and drop our relay. Cheap, and it makes
  the graduation path a config change rather than a migration.

**Lean:** the third, and it is the one that constrains the schema now — column
names should be checked against the `EventRouter` defaults (`aggregatetype`,
`aggregateid`, `type`, `payload`) before the schema is fixed. **Resolution:** read
the EventRouter docs properly and decide whether compatibility costs anything we
care about. *I have not done this check; the schema in `02-outbox.md` was
designed independently and probably is not compatible as written.*

---

## Q2 — Which ordering default: strict single-relay, or sharded `SKIP LOCKED`?

**Blocks v1?** Yes.

`02-outbox.md` leans strict-by-default. The counter-argument is that most users'
topics do not need per-key ordering, and a single-process default caps
throughput for everyone to protect a minority.

- **Strict default.** Correct without the user thinking. Caps throughput at one
  process; a user who never needed ordering pays for it.
- **Sharded default.** Faster out of the box. Silently reorders per-key events
  for anyone who did not read the docs — a bug that appears only under
  concurrency, i.e. in production, i.e. the worst kind.
- **No default; the config field is required.** Forces the decision at the point
  where the user has the context to make it. Hostile to a five-minute quickstart.

**Lean:** strict default, since a slow-but-correct default is recoverable and a
fast-but-wrong one is not. **Resolution:** benchmark the single-relay ceiling. If
it is comfortably above the target scale in `00-overview.md`, the argument is
over.

---

## Q3 — Headers as `JSONB` or a binary-preserving encoding?

**Blocks v1?** Yes — it is in the schema.

Kafka headers are `bytes -> bytes`. `02-outbox.md` picks `JSONB` for
debuggability and accepts that binary values must be base64'd by the user.

- **`JSONB` with text values.** Readable in SQL, which matters a lot during
  incidents. Lossy for binary headers; the user must encode, and if they forget,
  the failure is at relay time, not write time.
- **`JSONB` with values always base64.** Lossless, always. Unreadable in SQL,
  which throws away the entire reason for choosing JSONB.
- **`BYTEA` holding a length-prefixed encoding.** Lossless and compact.
  Completely opaque to SQL.
- **A side table `outbox_headers(outbox_id, name BYTEA, value BYTEA)`.**
  Lossless and queryable. An extra insert per header on the write path, which is
  the one path that must stay cheap.

**Lean:** JSONB with text values plus a documented base64 convention and a
validation error at `enqueue` time (not relay time) if a value is not
UTF-8-decodable. **Resolution:** find out whether anyone actually uses binary
header values in practice. My impression is that they are rare, but that is an
impression, not data.

---

## Q4 — Does the dedup module own the consumer loop?

**Blocks v1?** Yes, for the API shape.

`05-architecture.md` sketches a context manager and no loop. But then offset
commits, revocation handling, and the `IN_PROGRESS` case (where the offset must
*not* be committed) are all the user's responsibility, and those are precisely
what people get wrong.

- **Context manager only.** Framework-agnostic, tiny surface, composes with
  everything. Leaves the hardest parts (commit ordering, revocation) to the user,
  so the library can be used correctly and still produce duplicates.
- **Also ship an opinionated `ReliableConsumer`.** Gets commit ordering and
  revocation right for the users who take it. Doubles the surface, drags in a
  Kafka consumer dependency, and starts down the road to being a framework —
  which `00-overview.md` rules out.
- **Context manager plus documented recipes** (a ~40-line `aiokafka` loop, a
  FastStream middleware) in docs and tested in CI, not shipped as API.

**Lean:** the third. Recipes carry the knowledge without the maintenance burden
or the scope creep, and CI-tested docs do not rot. Weak lean — the argument that
"a documented recipe nobody copies correctly is not a solution" is a good one.

---

## Q5 — Default dedup store: Postgres or Redis?

**Blocks v1?** No; both ship. It shapes the docs and the quickstart.

- **Postgres default.** Only option supporting the strong same-transaction mode;
  no extra infrastructure for teams already on Postgres (which is the assumed
  audience). Slower, and adds write load to the primary.
- **Redis default.** Faster, TTL for free, no vacuum pressure. Weaker guarantee,
  a new availability dependency on the consumer's hot path, and durability that
  is easy to misconfigure.

**Lean:** Postgres, because the quickstart should show the strongest guarantee
the library can offer, and because it needs nothing the target user does not
already run. **Resolution:** measure the insert overhead at a realistic message
rate. If Postgres dedup meaningfully degrades a modest consumer, the lean is
wrong.

---

## Q6 — What happens to an expired `IN_PROGRESS` lease?

**Blocks v1?** Yes.

`03-idempotent-consumer.md` decides "reprocess", because at-least-once is the
safe direction. It is genuinely ambiguous: the worker may have died before or
after the side effect, and nothing local can tell.

- **Reprocess.** Never loses work. May duplicate a non-idempotent side effect —
  in a module whose entire purpose is preventing exactly that, which is an
  awkward thing to have to explain.
- **Do not reprocess; mark `abandoned` and alert.** Never duplicates. Silently
  drops work unless someone acts on the alert, and alerts are ignored.
- **Configurable, no default.** Forces the user to think. One more required
  decision on top of the key function.

**Lean:** reprocess, loudly (distinct metric, distinct log event, documented).
**Open sub-question:** what is a sane default lease? Too short and a slow handler
gets its own work duplicated under it; too long and a crashed worker blocks a
key for that duration. 5 minutes is a guess, not a finding.

---

## Q7 — Should replay be allowed to transform payloads?

**Blocks v1?** No. Easier to add than to remove.

`04-replay-dlq.md` says no: replay is republish, not migrate.

- **No transforms.** Replay stays auditable and trivially reasoned about — what
  went in comes out. An operator who needs to fix one field writes a script,
  which is exactly the ad-hoc-script situation this module exists to replace.
- **Allow a `Callable[[Record], Record | None]`.** Enormously useful in a real
  incident (drop a bad field, correct a topic name, fix an encoding). Turns
  replay into a data-mutation tool whose audit trail no longer captures what
  actually happened, and makes "what did that replay do" unanswerable without
  the script.
- **Allow transforms only with an audit log recording input and output bytes.**
  Keeps auditability, at the cost of an audit log that may contain sensitive
  payloads in full.

**Lean:** no transforms in v1, revisit with real user reports. The third option
is the shape it should take if it happens.

---

## Q8 — Do share groups (Kafka 4.2) change the DLQ topology advice?

**Blocks v1?** No, but it may date the docs quickly.

Share groups went production-ready in Kafka 4.2 (February 2026) and Kafka
Streams gained native DLQ support in the same release. Share groups break the
one-consumer-per-partition assumption that a lot of retry-ladder advice rests
on — per-message acknowledgement means a poison message need not block a
partition, which is a large part of why retry topics exist.

**I have not evaluated this in any depth. Treat everything in this entry as
speculative.** The retry-ladder discussion in `04-replay-dlq.md` may be
describing a workaround for a problem the broker now solves. Options: ignore
share groups for v1 and say the docs assume classic consumer groups (honest,
possibly dated); investigate and add guidance; or design the DLQ router to work
under both.

**Resolution:** someone needs to actually read KIP-932 and the 4.2 share-group
docs and report back. Until then, `04-replay-dlq.md` should carry an explicit
"assumes classic consumer groups" note.

---

## Q9 — `conn: Any` on the outbox writer

**Blocks v1?** Yes.

`05-architecture.md` accepts an untyped connection so asyncpg, psycopg and
SQLAlchemy sessions all work.

- **`Any` + runtime dispatch.** Works with everything, zero friction, no
  wrappers. No type safety, and a wrong argument fails at runtime — mitigated
  only by error-message quality.
- **A `Protocol` per backend, with an overloaded writer.** Type-safe. Does not
  fit SQLAlchemy sessions cleanly and produces an unpleasant generic signature.
- **Explicit backend classes** (`AsyncpgOutboxWriter`, `SqlAlchemyOutboxWriter`).
  Typed and honest. Triples the writer classes and makes swapping drivers an
  import change — which, arguably, it should be.

**Lean:** the third, actually, despite `05` sketching the first. Three small
typed classes are probably better than one clever untyped one, and it makes the
extras story exact. Unresolved — someone should sketch both and compare the
resulting user code.

---

## Q10 — Package name and public import path

**Blocks v1?** Yes, trivially, and it is irreversible in practice.

`kafka-reliability` is descriptive and unmemorable, and it claims more than the
library delivers (it does not make Kafka reliable; it addresses three specific
patterns around it). Alternatives: something naming the patterns
(`outbox-dedup-replay` — accurate, ugly), or an invented name (memorable,
unsearchable, and a second thing to explain).

**Lean:** keep `kafka-reliability` for the distribution and `kafka_reliability`
for the package. It is boring, and it is what someone would search for.
**Sub-question that matters more:** should the three modules be *separate
distributions* that share a namespace package? That would make the independence
claim in `05-architecture.md` structural rather than conventional — but three
packages means three release cycles and three changelogs for one small library,
and namespace packages have a long history of confusing people.

---

## Q11 — Metrics: what, and through what?

**Blocks v1?** No, but retrofitting metrics is worse than designing them in.

`00-overview.md` rules out shipping observability infrastructure. That leaves
*how* the numbers get out.

- **OpenTelemetry directly.** Standard, and the ecosystem has converged on it.
  A real dependency, and OTel's Python API has moved under people before.
- **A callback protocol** the user wires to their own system. Zero dependencies,
  total flexibility. Everyone writes the same adapter.
- **Both:** a callback protocol with an optional OTel adapter behind an extra.

**Lean:** the third. **The more important open question is *which* metrics,**
because they define what operators can alert on. At minimum: outbox pending
count and oldest-pending age (the backlog alarm), relay publish rate and error
rate, dedup claim/hit/in-progress/lease-expiry counts, and replay
records-produced. Exact names and label cardinality are unresolved, and label
cardinality is where this kind of thing goes wrong — a `topic` label is fine, a
`dedup_key` label is a catastrophe.

---

## Q12 — Minimum supported Python and Kafka versions

**Blocks v1?** Yes.

Python: 3.11 buys `asyncio.TaskGroup`, `Self`, and better exception groups —
all directly useful for a relay managing concurrent produces. 3.10 covers more
users. 3.12+ is cleanest and cuts too many.

Kafka: 4.x brings KIP-848 and KIP-890 (`01-prior-art.md`), but plenty of
production clusters are still on 3.x and will be for years. The library should
work against 3.x and *document* what 4.x improves, rather than requiring it.
**Unverified:** whether any part of the design actually depends on a 4.x-only
behaviour. I do not believe it does, but that has not been checked against a
real cluster.

**Lean:** Python 3.11+, Kafka 3.5+ tested, 4.x recommended.
