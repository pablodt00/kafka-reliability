# Replay and DLQ

Operational tooling for the boring, careful version of the 2am DLQ script. Adopt
it alone. Design: [`claude/04-replay-dlq.md`](claude/04-replay-dlq.md).

**The tool never destroys anything:** read and republish only — no deletes, no
offset rewinding, no topic-config changes; source offsets are committed only if
you ask. It also cannot un-send, which is why dry run is the default.

- **Route** — `DlqRouter(producer=..., topic=..., consumer_group=..., classify=...)`
  and `await router.route(record, error, attempts=n)`. Key, value and headers are
  preserved byte-for-byte; `x-dlq-*` headers describe the failure. `classify` is
  required — `typed_errors` (raise `PermanentError` / `TransientError`) or your own
  predicate. No retry ladder is run for you.
- **Select** — `Selection` (offsets `[from, to)`, timestamps, partitions,
  predicate, `max_messages`) → `ReplaySelector(reader=...).resolve()` prints the
  concrete per-partition offsets so the run is reproducible by offset.
- **Run** — `ReplayRunner.dry_run(selection)` then `.execute(selection)`: one
  traversal, one branch at the produce call. `ReplayOptions(target_topic=...)`
  carries the rails: 100/s rate limit, confirmation above ~10k, replay-count
  threshold 3, same-topic refusal, no inferred target, no offset commits.
- **Audit** — `x-replay-id` / `x-replay-at` on every record; JSONL audit log.
- **CLI** — `kafka-reliability-replay` (extras `cli`, `aiokafka`); dry run unless
  `--execute`; `--where "x-dlq-error-type == 'TimeoutError'"` filters by header.

**Refuses to:** transform payloads (D7), run continuously, or delete. Predicate
filtering is client-side — the whole range is read. The recommended workflow is in
the [operations guide](operations.md).
