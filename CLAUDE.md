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
└── contrib/         optional integrations (OpenTelemetry adapter) behind extras
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

## Commands
- Dev install: `pip install -e ".[dev]"` (inside a venv — the SessionStart
  hook does this automatically for a Claude Code web session; see
  `.claude/hooks/install-deps.sh`).
- Fast unit suite: `pytest`
- Integration suite: not available yet — tracked in issue #61 (container
  harness); will be `pytest -m integration` once real integration tests exist.
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
