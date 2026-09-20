"""DedupStore protocol and ClaimResult. claim/confirm/release/purge, not a
race-prone boolean seen().

`claim` is the whole point of the API shape. A read-then-write `seen(key)`
cannot be implemented race-free: two consumers after a rebalance both pass the
check and both run the handler. A claim is one atomic operation — a primary
key conflict, a `SET NX` — whose outcome tells the caller what to do.

A store is *not* what makes a handler idempotent. It turns at-least-once
delivery into effectively-once processing only when the dedup record commits
together with the handler's side effects (`supports_transactions`), or the
handler is idempotent on its own. Otherwise it narrows the window.

No third-party imports permitted in this module.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, Protocol, runtime_checkable

ClaimState = Literal["in_progress", "done"]

IN_PROGRESS_STATE = "in_progress"
DONE_STATE = "done"


class ClaimResult(enum.Enum):
    CLAIMED = "claimed"
    """First time (or a previous claim expired): process the message."""

    ALREADY_DONE = "already_done"
    """Done before: skip it, and it is safe to commit the offset."""

    IN_PROGRESS = "in_progress"
    """Another worker holds a live lease: skip it and do NOT commit the offset,
    so the message is redelivered if that worker dies."""


@dataclass(frozen=True, slots=True)
class Claim:
    """The outcome of `claim()`. `lease_expired` is True when this claim took
    over an `in_progress` row whose lease had run out (06-decisions.md D6) — the
    one case where a side effect may be repeated, so it is always reported."""

    result: ClaimResult
    lease_expired: bool = False


@runtime_checkable
class DedupStore(Protocol):
    """Where the Deduplicator records what it has processed.

    Records are namespaced by consumer `group`: two services consuming one topic
    must both process every message. `conn` stays `Any` (unlike the outbox
    writer) because stores accept different connection types; a store that
    cannot use it has `supports_transactions = False`, and the Deduplicator
    refuses the transactional mode against it instead of ignoring the argument.
    """

    supports_transactions: bool

    async def claim(
        self,
        group: str,
        key: str,
        *,
        state: ClaimState,
        expires_in: timedelta,
        conn: Any = None,
    ) -> Claim:
        """Atomically claim `(group, key)`.

        Inserts the record in `state`, expiring after `expires_in` (a lease for
        `in_progress`, the TTL for `done`). An expired record of either state is
        claimable again. With `conn`, the write joins the caller's transaction.
        Raises `StoreUnavailableError` if the store cannot be reached.
        """
        ...

    async def confirm(
        self, group: str, key: str, *, expires_in: timedelta, conn: Any = None
    ) -> None:
        """Mark a claimed `in_progress` record `done`, expiring after `expires_in`."""
        ...

    async def release(self, group: str, key: str, *, conn: Any = None) -> None:
        """Drop an `in_progress` claim so the message can be retried. Never
        touches a `done` record."""
        ...

    async def is_done(self, group: str, key: str, *, conn: Any = None) -> bool:
        """Whether a live `done` record exists. Racy by nature — used only by the
        deliberately weak `record_after` mode, never as a substitute for `claim`."""
        ...

    async def purge(
        self,
        *,
        group: str | None = None,
        key: str | None = None,
        chunk: int = 10_000,
        conn: Any = None,
    ) -> int:
        """Delete records; returns how many were removed.

        With `group` and `key`: forget that one record (e.g. before a replay).
        Without `key`: sweep expired records in chunks of `chunk` (all groups, or
        just `group`). Stores that expire natively (Redis) return 0 for a sweep.
        """
        ...


__all__ = [
    "DONE_STATE",
    "IN_PROGRESS_STATE",
    "Claim",
    "ClaimResult",
    "ClaimState",
    "DedupStore",
]
