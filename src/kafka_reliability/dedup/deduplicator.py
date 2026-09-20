"""Deduplicator: the entire runtime API of the dedup module (06-decisions.md D4).

    async with dedup.process(record, conn=session) as decision:
        if decision:
            await handle(record, session)

One async context manager that knows nothing about any consumer framework.
It does not own the consumer loop, does not deserialize payloads, and does not
silently degrade. Offset-commit ordering stays yours: commit **after** the
`async with` block, only when `decision.commit_offset` is true, and set
`enable.auto.commit=false` — auto-commit fires on a timer unrelated to whether
the handler finished, and committing before the work turns every crash into
silent message loss that no dedup store can repair.

This turns at-least-once delivery into effectively-once *processing* only when
the dedup record commits together with the handler's side effects
(`transactional` mode, against a store with `supports_transactions`), or the
handler is idempotent on its own. In the other modes it narrows the window.

No third-party imports permitted in this module.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError
from kafka_reliability.core.headers import REPLAY_ID
from kafka_reliability.core.message import Record, get_header
from kafka_reliability.dedup.keys import KeyFunction
from kafka_reliability.dedup.store import ClaimResult, DedupStore
from kafka_reliability.metrics import (
    DEDUP_CLAIMED,
    DEDUP_DUPLICATE,
    DEDUP_IN_PROGRESS,
    DEDUP_LEASE_EXPIRED,
    DEDUP_STORE_ERRORS,
    MetricsSink,
    NullMetrics,
)

log = logging.getLogger("kafka_reliability.dedup")

Mode = Literal["transactional", "claim_confirm", "record_after"]
ReplayPolicy = Literal["suppress", "bypass", "namespace"]
OnStoreUnavailable = Literal["fail_closed", "fail_open"]

_MODES = ("transactional", "claim_confirm", "record_after")
_REPLAY_POLICIES = ("suppress", "bypass", "namespace")
_UNAVAILABLE = ("fail_closed", "fail_open")


@dataclass(frozen=True, slots=True)
class Decision:
    """What `process()` yields. Truthy when the handler should run.

    `commit_offset` is False only for `ClaimResult.IN_PROGRESS`: another worker
    holds a live lease, so do NOT commit this offset — if that worker dies the
    message must be redelivered. Everything else (processed, or already done) is
    safe to commit once the `async with` block has exited.
    """

    result: ClaimResult | None  # None: dedup bypassed (replay_policy or fail_open)
    reason: str = ""

    def __bool__(self) -> bool:
        return self.result in (ClaimResult.CLAIMED, None)

    @property
    def commit_offset(self) -> bool:
        return self.result is not ClaimResult.IN_PROGRESS


class RevocationSignals:
    """One `asyncio.Event` per assignment, set when the partition is revoked.

    That is all it does. The library never cancels a running handler:
    interrupting one mid-side-effect turns a clean duplicate into a partial
    write, strictly worse than the wasted work it would save. A handler checks
    `signal.is_set()` at its own safe points, or ignores it and finishes; the new
    owner's *claim* on the same key is what prevents the double side effect.

    Wire `assign` / `revoke` to your consumer's rebalance callbacks.
    """

    def __init__(self) -> None:
        self._events: dict[tuple[str, int], asyncio.Event] = {}

    def signal_for(self, topic: str, partition: int) -> asyncio.Event:
        """The event for an assignment; created on first use."""
        return self._events.setdefault((topic, partition), asyncio.Event())

    def assign(self, partitions: Iterable[tuple[str, int]]) -> None:
        """Start fresh (unset) signals for newly assigned partitions."""
        for tp in partitions:
            self._events[tp] = asyncio.Event()

    def revoke(self, partitions: Iterable[tuple[str, int]]) -> None:
        """Set the signal of each revoked partition."""
        for tp in partitions:
            self._events.setdefault(tp, asyncio.Event()).set()


class Deduplicator:
    """Deduplication control flow over a `DedupStore`.

    `key` has **no default**, on purpose: what "the same message" means is yours
    to decide (`dedup.keys` has one-line helpers). `group` namespaces records so
    two services consuming one topic both process everything.

    `mode` is never defaulted for you in a way that degrades: left as `None` it
    is `transactional` when `process()` is given a `conn` and `claim_confirm`
    when it is not — and a `conn` against a store that cannot join a transaction
    raises instead of quietly downgrading. `record_after` is exposed, named
    honestly, and must be chosen explicitly: it is a latency optimisation, not a
    guarantee.

    `ttl` should exceed your maximum **replay** window, not your maximum retry
    window. `lease` is how long an `in_progress` claim blocks others; an expired
    lease means reprocess, loudly (D6) — 5 minutes is a judgement call, not a
    measurement.

    `replay_policy` applies to records carrying the replay header: `suppress`
    (default) treats them as ordinary duplicates, `bypass` processes them
    regardless, `namespace` deduplicates within that replay run only.
    `on_store_unavailable`: `fail_closed` (default) raises so the consumer stops;
    `fail_open` processes without dedup — explicitly chosen, and still counted
    and logged as an error, never a shrug.
    """

    def __init__(
        self,
        *,
        store: DedupStore,
        group: str,
        key: KeyFunction,
        ttl: timedelta = timedelta(days=7),
        mode: Mode | None = None,
        lease: timedelta = timedelta(minutes=5),
        replay_policy: ReplayPolicy = "suppress",
        on_store_unavailable: OnStoreUnavailable = "fail_closed",
        metrics: MetricsSink | None = None,
    ) -> None:
        if not group:
            raise ConfigurationError("group must not be empty")
        if mode is not None and mode not in _MODES:
            raise ConfigurationError(f"mode must be one of {_MODES}, got {mode!r}")
        if replay_policy not in _REPLAY_POLICIES:
            raise ConfigurationError(f"replay_policy must be one of {_REPLAY_POLICIES}")
        if on_store_unavailable not in _UNAVAILABLE:
            raise ConfigurationError(f"on_store_unavailable must be one of {_UNAVAILABLE}")
        if ttl <= timedelta(0) or lease <= timedelta(0):
            raise ConfigurationError("ttl and lease must be positive")
        if mode == "transactional" and not store.supports_transactions:
            raise ConfigurationError(
                f"mode='transactional' needs a store with supports_transactions=True; "
                f"{type(store).__name__} cannot share the handler's transaction. Use "
                "mode='claim_confirm', or a Postgres/SQLite store — silently degrading "
                "to a weaker mode is the failure this check exists to prevent"
            )
        self._store, self._group, self._key = store, group, key
        self._ttl, self._lease, self._mode = ttl, lease, mode
        self._replay_policy = replay_policy
        self._on_unavailable = on_store_unavailable
        self._metrics: MetricsSink = metrics or NullMetrics()
        self.revocations = RevocationSignals()

    def _resolve_mode(self, conn: Any) -> Mode:
        mode = self._mode
        if mode is None:
            mode = "transactional" if conn is not None else "claim_confirm"
        if mode == "transactional":
            if conn is None:
                raise ConfigurationError(
                    "transactional mode needs conn= (the handler's transaction); pass it "
                    "to process(), or choose mode='claim_confirm'"
                )
            if not self._store.supports_transactions:
                raise ConfigurationError(
                    f"conn= given but {type(self._store).__name__} cannot join a transaction"
                )
        return mode

    @asynccontextmanager
    async def process(self, record: Record, *, conn: Any = None) -> AsyncIterator[Decision]:
        """Guard one record. Yields a `Decision`; run the handler only if truthy.

        Leaving the block normally records the message as done; an exception
        releases the claim so the message is retried rather than permanently
        suppressed (in `transactional` mode the caller's transaction rollback
        does that, and the exception is re-raised untouched).
        """
        mode = self._resolve_mode(conn)
        group, key = self._group, self._key(record)
        replay_id = get_header(record.headers, REPLAY_ID)
        if replay_id is not None:
            if self._replay_policy == "bypass":
                yield Decision(None, "replay_policy=bypass")
                return
            if self._replay_policy == "namespace":
                group = f"{group}\x1freplay:{replay_id.decode('utf-8', 'replace')}"

        metric_group = self._group  # bounded label: never the key, never the replay id
        try:
            decision = await self._enter(mode, group, key, conn, metric_group)
        except StoreUnavailableError as exc:
            if not self._store_failed("claim", exc):
                raise
            yield Decision(None, "store unavailable, on_store_unavailable=fail_open")
            return

        if decision.result is not ClaimResult.CLAIMED:
            yield decision
            return

        try:
            yield decision
        except BaseException:
            if mode == "claim_confirm":
                await self._safely("release", self._store.release(group, key))
            raise
        if mode == "claim_confirm":
            await self._finish("confirm", self._store.confirm(group, key, expires_in=self._ttl))
        elif mode == "record_after":
            await self._finish("record", self._record_after(group, key, metric_group))

    async def _enter(
        self, mode: Mode, group: str, key: str, conn: Any, metric_group: str
    ) -> Decision:
        if mode == "record_after":
            if await self._store.is_done(group, key):
                self._metrics.counter(DEDUP_DUPLICATE, group=metric_group)
                return Decision(ClaimResult.ALREADY_DONE)
            return Decision(ClaimResult.CLAIMED)  # recorded after the handler, not now

        if mode == "transactional":
            claim = await self._store.claim(
                group, key, state="done", expires_in=self._ttl, conn=conn
            )
        else:
            claim = await self._store.claim(group, key, state="in_progress", expires_in=self._lease)
        if claim.lease_expired:
            self._metrics.counter(DEDUP_LEASE_EXPIRED, group=metric_group)
            log.warning(
                "dedup lease expired: reprocessing a message a previous worker claimed but never "
                "confirmed; its side effect may run twice (group=%s)",
                metric_group,
            )
        counter = {
            ClaimResult.CLAIMED: DEDUP_CLAIMED,
            ClaimResult.ALREADY_DONE: DEDUP_DUPLICATE,
            ClaimResult.IN_PROGRESS: DEDUP_IN_PROGRESS,
        }[claim.result]
        self._metrics.counter(counter, group=metric_group)
        return Decision(claim.result)

    async def _record_after(self, group: str, key: str, metric_group: str) -> None:
        claim = await self._store.claim(group, key, state="done", expires_in=self._ttl)
        if claim.result is ClaimResult.CLAIMED:
            self._metrics.counter(DEDUP_CLAIMED, group=metric_group)
        else:  # another worker finished first: the side effect ran twice — say so
            self._metrics.counter(DEDUP_DUPLICATE, group=metric_group)
            log.warning(
                "record_after: another worker completed the same message (group=%s)", metric_group
            )

    def _store_failed(self, kind: str, exc: Exception) -> bool:
        """Count and log a store failure; True if `fail_open` says carry on."""
        self._metrics.counter(DEDUP_STORE_ERRORS, group=self._group, kind=kind)
        log.error(
            "dedup store unavailable during %s (group=%s, on_store_unavailable=%s): %s",
            kind,
            self._group,
            self._on_unavailable,
            exc,
        )
        return self._on_unavailable == "fail_open"

    async def _finish(self, kind: str, op: Any) -> None:
        try:
            await op
        except StoreUnavailableError as exc:
            if not self._store_failed(kind, exc):
                raise

    async def _safely(self, kind: str, op: Any) -> None:
        """Best effort while an exception is already propagating: never mask it."""
        try:
            await op
        except StoreUnavailableError as exc:
            self._store_failed(kind, exc)


__all__ = [
    "Decision",
    "Deduplicator",
    "Mode",
    "OnStoreUnavailable",
    "ReplayPolicy",
    "RevocationSignals",
]
