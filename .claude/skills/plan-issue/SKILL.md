---
name: plan-issue
description: Plan the implementation of a GitHub issue or epic from its link, including all its sub-issues, and present a short, human-readable plan. Use when the user pastes a github.com/.../issues/N link and asks to plan it, or says "plan this epic/issue". Plans only — asks clarifying questions, makes no git operations.
---

# Plan an issue (and its sub-issues)

Input: a GitHub issue URL. Output: a concise plan the user can approve. This
skill plans; it does not implement, and it **never runs git operations**
(no commit, push, branch, PR) — implementation later means uncommitted code
changes only.

## Steps

1. **Enter plan mode** if not already in it (`/plan`), and write the plan to the
   plan file.
2. **Read the issue.** Parse owner/repo/number from the link. With the GitHub MCP
   `issue_read`: `get` the issue, then `get_sub_issues` and `get` each
   sub-issue (also `get_parent` if the issue may itself be a sub-issue).
3. **Read the project context.** `CLAUDE.md`, the `docs/claude/` docs the issue
   references (always `06-decisions.md`), and the code it touches. For a broad
   scope use an Explore agent; for a known file, just read it.
4. **Ask questions** with `AskUserQuestion` — only for real decisions:
   gaps or contradictions between the issue, the docs, and the code, or choices
   with genuine trade-offs. Give a recommended option. Skip anything with a
   conventional default; state those as assumptions instead.
5. **Write the plan** in the format below, then call `ExitPlanMode`.

## Plan format — plain, human, concise

Short bullets, no walls of prose. One screen per sub-issue at most.

- **Goal** — one or two lines: why this exists and what "done" looks like.
- **What I'll do** — a numbered list, one line per item, grouped by sub-issue
  (`#20 — <title>`). Each item says what gets built and where
  (`producers/memory.py`). File paths and signatures go in a sub-bullet only if needed.
- **Decisions** — what I decided (and why, in a few words) and what I need from you.
- **Docs & extras** — which docs change (`06-decisions.md` entry? README? architecture?).
- **How I'll check it** — `pytest`, `ruff check .`, `ruff format --check .`,
  `mypy src`, and the `check-module-boundaries` and `sync-docs` skills.
- **Out of scope** — what I'm deliberately not touching (e.g. work owned by another issue).

## Rules
- Respect `CLAUDE.md` non-negotiables (module independence, stdlib-only `core`,
  extras-gated imports, no unqualified "exactly once", bounded metric labels).
- If the plan would contradict a decision in `06-decisions.md`, the plan must
  include updating it.
- Say plainly which parts can't be verified here (e.g. real-broker tests).
- Never include git operations in the plan.
