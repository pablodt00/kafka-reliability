---
name: check-module-boundaries
description: Verify kafka-reliability's module-independence rules haven't been broken — that outbox/dedup/replay never import each other, that core stays stdlib-only, and that importing the outbox write path never pulls in a Kafka client. Use this before finishing any change that touches outbox/, dedup/, replay/, or producers/, when adding a new backend file, when the user asks whether something "breaks module independence" or "pulls in the wrong dependency," or before opening a PR in this repo.
---

# Check module boundaries

kafka-reliability's entire adoption story rests on four rules from
`docs/claude/05-architecture.md` ("The organising constraint"): outbox, dedup
and replay never import each other; core is stdlib-only; the outbox write
path never imports a Kafka client; and a backend without its extra installed
raises a clear error naming that extra. These are enforced by
`tests/test_module_boundaries.py` in CI (issue #15, mitigating D10's
packaging trade-off). This skill's script mirrors the first three of those
checks for a faster, dependency-free spot-check — e.g. mid-edit, before
committing — without invoking pytest. It does not check the fourth rule,
which needs `sys.modules` patching machinery that only makes sense inside
the pytest suite.

## Run the check

Authoritative check (all four rules, run in CI):

    pytest tests/test_module_boundaries.py

Fast manual check (rules 1–3 only, no pytest needed):

    python .claude/skills/check-module-boundaries/scripts/check_boundaries.py

Exit code 0 and "All module-boundary rules hold." means clean. A non-zero
exit prints each violation with the offending file and import.

## What it checks
1. No file under `outbox/`, `dedup/`, or `replay/` imports either of the
   other two.
2. No file under `core/` imports anything outside the standard library.
3. Importing `kafka_reliability.outbox.writer` never leaves `aiokafka` (or
   any Kafka client) in `sys.modules`.

## If it finds something
- A cross-import between outbox/dedup/replay: the two modules must
  communicate through a `core` header constant instead (see how
  `core.headers.EVENT_ID` / `REPLAY_ID` work) — never through a direct
  import.
- A non-stdlib import in `core`: move the code to the module that actually
  needs the dependency, even if that means some duplication — `core` staying
  dependency-free is a design decision, not an oversight (05-architecture.md).
- A Kafka client leaking into the outbox write path: check the writer isn't
  importing something from `producers/` or `outbox/relay.py` — the writer
  must only ever touch its one DB driver.
