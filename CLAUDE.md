# CLAUDE.md

## What this is
`kafka-reliability` is a framework-agnostic Python library for three
reliability problems every Kafka-based service hits: getting an event out of
a database transaction into Kafka without losing it (transactional outbox),
surviving Kafka's at-least-once redelivery (idempotent consumer / dedup), and
draining a dead-letter topic safely (DLQ replay). It targets **at-least-once
delivery with effectively-once processing** — never "exactly once" without
that qualification. Full context: `docs/claude/00-overview.md`.

## Design docs are the source of truth
`docs/claude/` is the design record, not background reading —
`06-decisions.md` records every design decision made so far, each with its
accepted trade-off, its rejected alternatives, and the evidence that would
reverse it. If a change you're making would contradict a decision recorded
there, update `06-decisions.md` in the same PR; don't let the code and the
doc disagree.

## Package layout
```
kafka_reliability/
├── core/            stdlib-only: Message/Record types, header constants, Clock, errors
├── outbox/          enqueue writer, relay (poll → produce → mark sent), schema, retention
│   └── backends/    one typed writer class per DB driver (asyncpg, psycopg, sqlalchemy, django)
├── dedup/           DedupStore protocol, key derivation, Deduplicator control flow
│   └── backends/    postgres (the only transactional-mode store), redis, sqlite, memory
├── replay/          selection, dry-run/execute runner, DlqRouter, JSONL audit, CLI
├── producers/       the Producer protocol + aiokafka/confluent/memory adapters
├── metrics.py        MetricsSink protocol, no-op default
└── contrib/         optional integrations behind extras: OpenTelemetry adapter, replay CLI wiring
```
Full rationale: `docs/claude/05-architecture.md`.

## Non-negotiables
- `outbox`, `dedup`, and `replay` never import each other. They compose only
  through `core` header constants (`EVENT_ID`, `REPLAY_ID`), never through
  direct imports.
- `core` has no third-party imports — stdlib only.
- The outbox *write* path (`outbox.writer` and `outbox.backends.*`) never
  imports a Kafka client.
- Every module's third-party imports are lazy/extras-gated: `pip install
  kafka-reliability` with no extras must still let every submodule import
  cleanly; each extra pulls in exactly one third-party package.
- Never write or imply "exactly once" without the "at-least-once delivery,
  effectively-once processing" qualification.
- Metric labels stay bounded: never a dedup key, message key, partition, or
  offset (D11).
- Before finishing work that touches `outbox/`, `dedup/`, `replay/`, or
  `producers/`, run the `check-module-boundaries` skill
  (`.claude/skills/check-module-boundaries`) to verify none of the above broke.
- Before finishing any change that alters public API, extras, module layout, or
  a design decision, run the `sync-docs` skill so `README.md` and `docs/claude/`
  match the code. To plan a GitHub issue/epic from a link, use `plan-issue`.

## Git workflow
Never run `git commit` or `git push` (or open/update a PR) unless the user
explicitly asks for that specific action in that message. Implementing a
task, including one planned and approved via plan mode, means making the
code changes and leaving them uncommitted in the working tree — commit and
push are separate, later steps the user asks for on their own.

## Commands
- Dev install: `pip install -e ".[dev]"` (inside a venv — the SessionStart
  hook does this automatically for a Claude Code web session; see
  `.claude/hooks/install-deps.sh`).
- Fast unit suite: `pytest`
- Integration suite: `pytest -m integration`. Today that is the outbox
  write-path conformance suite (`tests/outbox/test_conformance.py`), the relay
  (`tests/outbox/test_relay_integration.py`) and the dedup stores
  (`tests/dedup/test_integration.py`), which need a Postgres named by
  `KAFKA_RELIABILITY_TEST_PG_DSN` (and Redis via
  `KAFKA_RELIABILITY_TEST_REDIS_URL`) and skip without them. The
  container harness that provides one is tracked in issue #61.
- Lint: `ruff check .`
- Format check: `ruff format --check .` (apply fixes locally with `ruff format .`)
- Type check: `mypy src`
- Container harness (Kafka/Postgres/Redis): not available yet — tracked in
  issue #61.

## Extras
Ten backend extras map 1:1 to a single third-party driver each — see
`[project.optional-dependencies]` in `pyproject.toml`. Adding a new backend
file always means adding to, or reusing, exactly one extra — never adding a
dependency to `core`, to another module's extra, or as a mandatory dependency.
