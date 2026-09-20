"""InMemoryDedupStore — for unit tests ONLY.

It does not survive a restart, so it silently provides **no guarantee** across
the one event that most needs one: a crashed consumer coming back. It also
cannot share a transaction with a handler. Do not use it in production; it warns
when constructed outside a pytest run. No third-party imports permitted."""

from __future__ import annotations

import sys
import warnings
from datetime import timedelta
from typing import Any

from kafka_reliability.core.clock import Clock, SystemClock
from kafka_reliability.dedup.store import (
    DONE_STATE,
    IN_PROGRESS_STATE,
    Claim,
    ClaimResult,
    ClaimState,
)


class InMemoryDedupStore:
    """A dict. Atomic within one event loop; durable never."""

    supports_transactions = False

    def __init__(self, clock: Clock | None = None) -> None:
        if "pytest" not in sys.modules:
            warnings.warn(
                "InMemoryDedupStore is for unit tests only: it loses every record on restart, "
                "so it provides no deduplication guarantee in production",
                stacklevel=2,
            )
        self._clock: Clock = clock or SystemClock()
        self._rows: dict[tuple[str, str], tuple[str, object]] = {}

    async def claim(
        self, group: str, key: str, *, state: ClaimState, expires_in: timedelta, conn: Any = None
    ) -> Claim:
        now = self._clock.now()
        existing = self._rows.get((group, key))
        lease_expired = False
        if existing is not None:
            old_state, expires_at = existing
            if expires_at > now:  # type: ignore[operator]
                return Claim(
                    ClaimResult.ALREADY_DONE if old_state == DONE_STATE else ClaimResult.IN_PROGRESS
                )
            lease_expired = old_state == IN_PROGRESS_STATE
        self._rows[(group, key)] = (state, now + expires_in)
        return Claim(ClaimResult.CLAIMED, lease_expired)

    async def confirm(
        self, group: str, key: str, *, expires_in: timedelta, conn: Any = None
    ) -> None:
        self._rows[(group, key)] = (DONE_STATE, self._clock.now() + expires_in)

    async def release(self, group: str, key: str, *, conn: Any = None) -> None:
        row = self._rows.get((group, key))
        if row is not None and row[0] == IN_PROGRESS_STATE:
            del self._rows[(group, key)]

    async def is_done(self, group: str, key: str, *, conn: Any = None) -> bool:
        row = self._rows.get((group, key))
        return row is not None and row[0] == DONE_STATE and row[1] > self._clock.now()  # type: ignore[operator]

    async def purge(
        self,
        *,
        group: str | None = None,
        key: str | None = None,
        chunk: int = 10_000,
        conn: Any = None,
    ) -> int:
        if key is not None:
            if group is None:
                raise ValueError("purge(key=...) needs group=")
            return 1 if self._rows.pop((group, key), None) is not None else 0
        now = self._clock.now()
        doomed = [
            k
            for k, (_, exp) in self._rows.items()
            if exp <= now and (group is None or k[0] == group)  # type: ignore[operator]
        ]  # no I/O to chunk in memory
        for k in doomed:
            del self._rows[k]
        return len(doomed)
