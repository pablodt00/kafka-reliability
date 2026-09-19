---
name: sync-docs
description: Keep README.md, docs/claude/*, and CLAUDE.md in step with the code. Use before finishing any change that alters public API, adds or changes an extra, moves modules, changes a design decision, or adds metrics/errors — and whenever the user asks whether the docs are up to date. Never commits or pushes.
---

# Sync docs

The docs are the design record (`CLAUDE.md`: "don't let the code and the doc
disagree"). Run this before finishing a change, alongside
`check-module-boundaries`.

## Steps

1. **See what changed** (read-only): `git status --short` and `git diff`.
   Never commit, push, or stage anything.
2. **Classify each change**, then update the matching doc:

   | Change | Update |
   |---|---|
   | Contradicts or adds a design decision | `docs/claude/06-decisions.md` — new or edited `Dn` with: decision, trade-off accepted, rejected alternatives, evidence that would reverse it |
   | Public API, module layout, dependency boundaries | `docs/claude/05-architecture.md` (API sketches, layout tree, boundary table) |
   | Outbox / dedup / replay behaviour | `02-outbox.md`, `03-idempotent-consumer.md`, `04-replay-dlq.md` |
   | Something users can now do, install or run | `README.md` (status table, install/extras, usage) |
   | Commands, extras, non-negotiables, workflow | `CLAUDE.md` |
   | New metric or error type | `metrics.py` table in `06-decisions.md` (D11) / errors mention in `05-architecture.md` |

3. **Check for stale claims.** Grep the docs for every name, signature, extra,
   or count that changed (e.g. `grep -rn "three methods" docs README.md`).
   Also grep for "exactly once" — every use needs the qualification "at-least-once
   delivery, effectively-once processing".
4. **Check the README status table** still matches what is actually
   implemented (a stub file is "planned", not "implemented").
5. **Report**: which docs you updated and why, and which you looked at and
   deliberately left alone. If nothing needed changing, say so.

## Rules
- Docs describe what exists or is decided, not aspirations; mark the rest "planned".
- Keep entries concise; don't duplicate a decision's rationale outside `06-decisions.md`.
- Run the check commands the docs mention (`pytest`, `ruff`, `mypy`) only if you also changed code.
