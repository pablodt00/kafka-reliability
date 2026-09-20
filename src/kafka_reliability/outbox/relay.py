"""OutboxRelay: poll the outbox table, produce to Kafka, mark rows sent.

Delivery is **at-least-once**, never exactly-once. The relay's Kafka ack and
its "mark sent" write are themselves a dual write: a crash between the two
republishes the row. The outbox pattern converts a correctness problem (a lost
event) into a duplicates problem, so every consumer of an outbox-fed topic
must deduplicate — that is the job of the dedup module, and the two together
give at-least-once delivery with effectively-once processing.

Rows are claimed by their `status` column, never by a `seq` cursor. Sequence
values are allocated at INSERT but commit order is not insertion order, so a
cursor that jumped past seq 105 would lose seq 104 if it committed late;
querying for `pending` is immune, because a late row simply shows up later.

Ordering is per key (`aggregateid`, which is the Kafka key). Rows of one key
are produced strictly one after another; different keys are produced
concurrently. A row that cannot be published is marked `failed` and blocks its
own key — never skipped, never reordered — while other keys keep flowing.

This module depends on `core` and the `producers.port` protocol only. The
database is reached through the `RelayStore` protocol; the asyncpg
implementation lives in `outbox.backends.asyncpg_relay`.
"""

from __future__ import annotations

import asyncio
import uuid
import zlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final, Literal, Protocol

from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError
from kafka_reliability.core.headers import EVENT_ID
from kafka_reliability.core.message import OutgoingMessage
from kafka_reliability.metrics import (
    OUTBOX_FAILED,
    OUTBOX_OLDEST_PENDING_SECONDS,
    OUTBOX_PENDING,
    OUTBOX_PUBLISHED,
    OUTBOX_RELAY_ERRORS,
    MetricsSink,
    NullMetrics,
)
from kafka_reliability.producers.port import Producer

Ordering = Literal["strict", "sharded"]

_INT32_MIN: Final = -(2**31)
_INT32_MAX: Final = 2**31 - 1


@dataclass(frozen=True, slots=True)
class RelayConfig:
    """How the relay polls, orders and gives up.

    `ordering="strict"` (default) elects one relay per table with an advisory
    lock: throughput is one process, ordering is correct with nothing to
    reason about (06-decisions.md D2). `ordering="sharded"` is the opt-in
    throughput mode: run `shard_count` relays, each with its own `shard_index`.
    A row's shard is a hash of its message key, never of its row id — a key
    always lands in the same shard, so per-key order holds; the API has no way
    to shard by row id.

    `max_attempts` is the number of produce attempts before a row is marked
    `failed`. Between failing passes `run()` backs off exponentially from
    `retry_backoff` up to `max_backoff` seconds.
    """

    batch_size: int = 100
    poll_interval: float = 0.1
    ordering: Ordering = "strict"
    shard_count: int = 1
    shard_index: int = 0
    max_attempts: int = 5
    advisory_lock_key: int | None = None
    retry_backoff: float = 0.5
    max_backoff: float = 30.0
    standby_interval: float = 1.0

    def __post_init__(self) -> None:
        if self.ordering not in ("strict", "sharded"):
            raise ConfigurationError(
                f"ordering must be 'strict' or 'sharded', got {self.ordering!r}"
            )
        if self.batch_size < 1:
            raise ConfigurationError("batch_size must be at least 1")
        if self.max_attempts < 1:
            raise ConfigurationError("max_attempts must be at least 1")
        if min(self.poll_interval, self.retry_backoff, self.max_backoff, self.standby_interval) < 0:
            raise ConfigurationError("intervals and backoffs must not be negative")
        if self.shard_count < 1:
            raise ConfigurationError("shard_count must be at least 1")
        if not 0 <= self.shard_index < self.shard_count:
            raise ConfigurationError(
                f"shard_index must be in [0, shard_count): got {self.shard_index} "
                f"with shard_count={self.shard_count}"
            )
        if self.ordering == "strict" and self.shard_count != 1:
            raise ConfigurationError(
                "shard_count only applies to ordering='sharded'; strict ordering runs one relay"
            )
        if self.ordering == "sharded" and self.shard_count < 2:
            raise ConfigurationError("ordering='sharded' needs shard_count of at least 2")
        key = self.advisory_lock_key
        if key is not None and not _INT32_MIN <= key <= _INT32_MAX:
            raise ConfigurationError("advisory_lock_key must fit in a signed 32-bit integer")


def default_advisory_lock_key(table: str) -> int:
    """The default lock key: a stable 31-bit hash of the table name."""
    return zlib.crc32(table.encode("utf-8")) & _INT32_MAX


@dataclass(frozen=True, slots=True)
class RelayRow:
    """One pending outbox row as the relay sees it."""

    id: uuid.UUID
    seq: int
    topic: str
    aggregateid: str
    payload: bytes
    headers: dict[str, str]
    attempts: int


@dataclass(frozen=True, slots=True)
class OutboxStats:
    """Backlog snapshot. `oldest_pending_seconds` is measured by the database
    clock, and is the alarm that matters: it means the relay has stalled."""

    pending: int
    oldest_pending_seconds: float
    failed: int


@dataclass(frozen=True, slots=True)
class RelayBatchResult:
    """What one `run_once()` did. `leader` is False when another relay holds
    the lock and this one did nothing."""

    leader: bool = True
    claimed: int = 0
    published: int = 0
    retried: int = 0
    failed: int = 0
    stats: OutboxStats | None = field(default=None, compare=False)


class RelayStore(Protocol):
    """The database side of the relay. One instance is one relay's connection."""

    table: str

    async def acquire_leadership(self, key: int, shard: int) -> bool:
        """Try to become the relay for `(key, shard)`; True if held (idempotent)."""
        ...

    async def release_leadership(self) -> None:
        """Give up leadership if held."""
        ...

    async def claim(self, limit: int, *, shard_count: int, shard_index: int) -> list[RelayRow]:
        """Return up to `limit` pending rows in `seq` order, skipping every key
        that has a `failed` row, restricted to this shard when `shard_count > 1`.

        Raises `StoreUnavailableError` if the connection (and with it the
        advisory lock) is lost.
        """
        ...

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None: ...

    async def record_failure(self, row_id: uuid.UUID, error: str, *, give_up: bool) -> int:
        """Increment `attempts`, store `last_error`, and if `give_up` set
        `status='failed'`. Returns the new attempt count."""
        ...

    async def stats(self) -> OutboxStats: ...


@dataclass(slots=True)
class _GroupOutcome:
    published: int = 0
    retried: int = 0
    failed: int = 0
    store_error: StoreUnavailableError | None = None


class OutboxRelay:
    """Publishes pending outbox rows to Kafka, at least once.

    `run_once()` performs one claim → produce → mark-sent pass and returns what
    happened, so tests never need `sleep()`. `run(stop=...)` loops it. Call
    `close()` (or let `run()` return) to release the advisory lock.
    """

    def __init__(
        self,
        store: RelayStore,
        producer: Producer,
        config: RelayConfig | None = None,
        *,
        metrics: MetricsSink | None = None,
    ) -> None:
        self._store = store
        self._producer = producer
        self._config = config or RelayConfig()
        self._metrics: MetricsSink = metrics or NullMetrics()
        self._lock_key = (
            self._config.advisory_lock_key
            if self._config.advisory_lock_key is not None
            else default_advisory_lock_key(store.table)
        )
        # strict: shard slot 0; sharded: one lock per shard, so two relays
        # configured with the same shard_index cannot both run.
        self._lock_shard = 0 if self._config.ordering == "strict" else self._config.shard_index + 1

    @property
    def config(self) -> RelayConfig:
        return self._config

    async def run_once(self) -> RelayBatchResult:
        """One pass. Raises `StoreUnavailableError` if the database (and so the
        advisory lock) is lost; the relay then holds no lock and should not go on."""
        cfg = self._config
        try:
            if not await self._store.acquire_leadership(self._lock_key, self._lock_shard):
                return RelayBatchResult(leader=False)
            rows = await self._store.claim(
                cfg.batch_size, shard_count=cfg.shard_count, shard_index=cfg.shard_index
            )
        except StoreUnavailableError:
            self._metrics.counter(OUTBOX_RELAY_ERRORS, table=self._store.table, kind="store")
            raise

        outcomes = await asyncio.gather(*(self._publish_key(g) for g in _group_by_key(rows)))
        for outcome in outcomes:
            if outcome.store_error is not None:
                # Produced but not marked: republished next pass — a duplicate, never a loss.
                self._metrics.counter(OUTBOX_RELAY_ERRORS, table=self._store.table, kind="store")
                raise outcome.store_error
        try:
            stats = await self.stats()
        except StoreUnavailableError:
            self._metrics.counter(OUTBOX_RELAY_ERRORS, table=self._store.table, kind="store")
            raise
        return RelayBatchResult(
            claimed=len(rows),
            published=sum(o.published for o in outcomes),
            retried=sum(o.retried for o in outcomes),
            failed=sum(o.failed for o in outcomes),
            stats=stats,
        )

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Loop `run_once()` until `stop` is set, then release leadership.

        A lost database connection raises `StoreUnavailableError` out of here:
        the relay stops rather than continue without its lock, and the
        supervisor (systemd, Kubernetes, your task group) restarts it.
        """
        stop = stop or asyncio.Event()
        failing_passes = 0
        try:
            while not stop.is_set():
                result = await self.run_once()
                if not result.leader:
                    delay = self._config.standby_interval
                elif result.retried or result.failed:
                    failing_passes += 1
                    delay = min(
                        self._config.max_backoff,
                        self._config.retry_backoff * 2 ** (failing_passes - 1),
                    )
                else:
                    failing_passes = 0
                    delay = (
                        0.0
                        if result.claimed >= self._config.batch_size
                        else self._config.poll_interval
                    )
                await _sleep_or_stop(delay, stop)
        finally:
            await self.close()

    async def stats(self) -> OutboxStats:
        """Backlog snapshot; also emits the `outbox.pending` and
        `outbox.oldest_pending_seconds` gauges. One cheap partial-index query
        set, safe to run every poll."""
        stats = await self._store.stats()
        table = self._store.table
        self._metrics.gauge(OUTBOX_PENDING, stats.pending, table=table)
        self._metrics.gauge(
            OUTBOX_OLDEST_PENDING_SECONDS, stats.oldest_pending_seconds, table=table
        )
        return stats

    async def close(self) -> None:
        """Release the advisory lock. Safe to call repeatedly."""
        await self._store.release_leadership()

    async def _publish_key(self, rows: list[RelayRow]) -> _GroupOutcome:
        """Produce one key's rows in order; stop at the first failure so nothing
        behind it can overtake it."""
        outcome = _GroupOutcome()
        sent: list[uuid.UUID] = []
        table = self._store.table
        try:
            for row in rows:
                try:
                    await self._producer.send(_to_message(row))
                except Exception as exc:
                    if isinstance(exc, asyncio.CancelledError):  # pragma: no cover
                        raise
                    self._metrics.counter(OUTBOX_RELAY_ERRORS, table=table, kind="produce")
                    give_up = row.attempts + 1 >= self._config.max_attempts
                    await self._store.record_failure(row.id, _describe(exc), give_up=give_up)
                    if give_up:
                        outcome.failed += 1
                        self._metrics.counter(OUTBOX_FAILED, table=table, topic=row.topic)
                    else:
                        outcome.retried += 1
                    break
                sent.append(row.id)
        except StoreUnavailableError as exc:
            outcome.store_error = exc
        finally:
            if sent:
                try:
                    await self._store.mark_published(sent)
                    outcome.published = len(sent)
                    for row in rows[: len(sent)]:
                        self._metrics.counter(OUTBOX_PUBLISHED, table=table, topic=row.topic)
                except StoreUnavailableError as exc:
                    outcome.store_error = outcome.store_error or exc
        return outcome


def _group_by_key(rows: Sequence[RelayRow]) -> list[list[RelayRow]]:
    """Split claimed rows into ordering units, preserving `seq` order in each.

    A non-empty key is one unit. An empty key means "no ordering requirement",
    so each such row is its own unit and cannot block another.
    """
    groups: dict[str, list[RelayRow]] = {}
    singles: list[list[RelayRow]] = []
    for row in rows:
        if row.aggregateid:
            groups.setdefault(row.aggregateid, []).append(row)
        else:
            singles.append([row])
    return [*groups.values(), *singles]


def _to_message(row: RelayRow) -> OutgoingMessage:
    headers = {name: value.encode("utf-8") for name, value in row.headers.items()}
    headers[EVENT_ID] = str(row.id).encode("ascii")  # stamped last: a stored header cannot forge it
    return OutgoingMessage(
        topic=row.topic,
        value=row.payload,
        key=row.aggregateid.encode("utf-8") or None,
        headers=headers,
    )


def _describe(exc: BaseException) -> str:
    cause = exc.__cause__
    text = f"{type(exc).__name__}: {exc}"
    if cause is not None:
        text += f" (caused by {type(cause).__name__}: {cause})"
    return text[:2000]


async def _sleep_or_stop(delay: float, stop: asyncio.Event) -> None:
    if delay <= 0:
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except TimeoutError:
        pass


__all__ = [
    "OutboxRelay",
    "OutboxStats",
    "RelayBatchResult",
    "RelayConfig",
    "RelayRow",
    "RelayStore",
    "default_advisory_lock_key",
]
