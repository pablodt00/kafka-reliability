"""ReplayRunner: one traversal, `dry_run()` and `execute()`.

**The tool never destroys anything.** It reads and republishes: no record
deletion, no offset rewinding, no topic-config changes, and source offsets are
committed only if the operator asks. The corollary is that a replay cannot be
un-sent, which is why dry-run exists and `execute()` is a separate, explicit call.

`dry_run()` and `execute()` share `_traverse`, which has a single branch at the
produce site. A dry run is a genuine execution of everything except the produce
— resolve offsets, read the range, apply the predicate, apply the poison check,
build every message, report — never an estimate, because two implementations
drift and the drift is discovered during an incident.

Replay adds `x-replay-id` / `x-replay-at` and increments `x-dlq-replay-count`;
it changes nothing else. There is no transform hook (06-decisions.md D7).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from kafka_reliability.core.clock import Clock, SystemClock
from kafka_reliability.core.errors import ConfigurationError, KafkaReliabilityError
from kafka_reliability.core.headers import DLQ_REPLAY_COUNT, REPLAY_AT, REPLAY_ID
from kafka_reliability.core.message import OutgoingMessage, Record, get_header
from kafka_reliability.metrics import REPLAY_PRODUCED, REPLAY_SKIPPED, MetricsSink, NullMetrics
from kafka_reliability.producers.port import Producer
from kafka_reliability.replay.audit import AuditSink, JsonlAuditSink, encode_key
from kafka_reliability.replay.selector import ReplaySelector, ResolvedSelection, Selection

SKIP_PREDICATE = "predicate"
SKIP_REPLAY_COUNT = "replay_count_threshold"
SKIP_BAD_REPLAY_COUNT = "invalid_replay_count"


class ConfirmationRequired(KafkaReliabilityError):
    """The replay is larger than `confirm_above` and nobody confirmed it."""


@dataclass(frozen=True)
class ReplayOptions:
    """Safety rails. Each exists because of a specific way replays go wrong.

    `target_topic` is required and never inferred: a one-character typo in an
    inferred name is a hard-to-detect disaster, and typing it puts the intent on
    the record. `rate_per_second` defaults to 100 — dumping 50,000 messages at
    line rate into a live topic starves real-time consumers; `None` disables it.
    `max_replay_count` (default 3) is the poison-loop guard, read from
    `x-dlq-replay-count`. Same-topic replays need `allow_same_topic`. A run larger
    than `confirm_above` (~10,000) needs `confirm` (or `assume_yes`).
    `preserve_key` keeps partitioning consistent with the original stream, which
    means replayed records interleave with live traffic on those partitions.
    `commit_source_offsets` is off by default: committing makes a replay
    non-repeatable and hides what was consumed.
    """

    target_topic: str
    rate_per_second: float | None = 100.0
    max_replay_count: int = 3
    allow_same_topic: bool = False
    preserve_key: bool = True
    commit_source_offsets: bool = False
    audit_path: Path | None = None
    confirm_above: int = 10_000
    assume_yes: bool = False

    def __post_init__(self) -> None:
        if not self.target_topic:
            raise ConfigurationError("target_topic is required; it is never inferred")
        if self.rate_per_second is not None and self.rate_per_second <= 0:
            raise ConfigurationError("rate_per_second must be positive (or None for unlimited)")
        if self.max_replay_count < 1:
            raise ConfigurationError("max_replay_count must be at least 1")


@dataclass(frozen=True)
class PartitionReport:
    partition: int
    start: int
    end: int
    scanned: int
    matched: int


@dataclass(frozen=True)
class Sample:
    key: bytes | None
    headers: dict[str, bytes]
    value_size: int


@dataclass(frozen=True)
class ReplayPlan:
    """What a replay does (dry run) or did (`ReplayResult.plan`)."""

    replay_id: str
    resolved: ResolvedSelection
    target_topic: str
    partitions: tuple[PartitionReport, ...]
    matched: int
    skipped: dict[str, int]
    oldest: datetime | None
    newest: datetime | None
    samples: tuple[Sample, ...]
    dry_run: bool

    def format(self) -> str:
        sel = self.resolved.selection
        lines = [
            "DRY RUN — no messages produced" if self.dry_run else "REPLAY EXECUTED",
            f"source:  {sel.topic}        target: {self.target_topic}",
            f"replay:  {self.replay_id}",
        ]
        if sel.from_timestamp or sel.to_timestamp:
            lines.append(f"range:   {_iso(sel.from_timestamp)} → {_iso(sel.to_timestamp)}")
        by_partition = {p.partition: p for p in self.partitions}
        for r in self.resolved.ranges:
            p = by_partition[r.partition]
            if r.count == 0:
                lines.append(f"  partition {r.partition}   (no records in range)")
            else:
                lines.append(
                    f"  partition {r.partition}   offsets {r.start} → {r.end}    "
                    f"scanned {p.scanned}   matched {p.matched}"
                )
        lines += [f"  note: {n}" for n in self.resolved.notes]
        if sel.predicate is not None:
            lines.append("filter:  client-side predicate (the whole range is read)")
        if self.skipped:
            detail = ", ".join(f"{n} ({reason})" for reason, n in sorted(self.skipped.items()))
            lines.append(f"skipped: {sum(self.skipped.values())} — {detail}")
        verb = "would be produced" if self.dry_run else "produced"
        lines.append(f"total:   {self.matched} messages {verb} to '{self.target_topic}'")
        if self.oldest and self.newest:
            lines.append(f"oldest:  {_iso(self.oldest)}   newest: {_iso(self.newest)}")
        for s in self.samples:
            shown = {k: v.decode("utf-8", "replace") for k, v in s.headers.items()}
            lines.append(
                f"sample:  key={encode_key(s.key)}  headers={shown}  value={s.value_size} bytes"
            )
        if self.dry_run:
            lines.append("run with --execute to replay")
        return "\n".join(lines)


@dataclass(frozen=True)
class ReplayResult:
    plan: ReplayPlan
    produced: int
    committed_offsets: dict[int, int] = field(default_factory=dict)


class RateLimiter:
    """Spaces calls `1/rate` apart. Clock and sleep are injectable so tests
    never really wait."""

    def __init__(
        self,
        rate: float | None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._interval = 0.0 if rate is None else 1.0 / rate
        self._monotonic, self._sleep = monotonic, sleep
        self._next = 0.0

    async def wait(self) -> None:
        if self._interval == 0.0:
            return
        now = self._monotonic()
        if self._next > now:
            await self._sleep(self._next - now)
            now = self._next
        self._next = max(now, self._next) + self._interval


class ReplayRunner:
    def __init__(
        self,
        *,
        selector: ReplaySelector,
        producer: Producer,
        options: ReplayOptions,
        audit: AuditSink | None = None,
        metrics: MetricsSink | None = None,
        clock: Clock | None = None,
        limiter: RateLimiter | None = None,
        confirm: Callable[[ResolvedSelection], bool] | None = None,
    ) -> None:
        self._selector, self._producer, self._options = selector, producer, options
        self._audit = audit or (JsonlAuditSink(options.audit_path) if options.audit_path else None)
        self._metrics: MetricsSink = metrics or NullMetrics()
        self._clock: Clock = clock or SystemClock()
        self._limiter = limiter or RateLimiter(options.rate_per_second)
        self._confirm = confirm

    async def dry_run(self, selection: Selection) -> ReplayPlan:
        """Everything except the produce. Never touches the target topic."""
        return (await self._traverse(selection, execute=False)).plan

    async def execute(self, selection: Selection) -> ReplayResult:
        """Really republish. Raises `ConfirmationRequired` for a large run nobody
        confirmed, and `ConfigurationError` for a same-topic loop."""
        return await self._traverse(selection, execute=True)

    def _check_target(self, selection: Selection) -> None:
        if selection.topic == self._options.target_topic and not self._options.allow_same_topic:
            raise ConfigurationError(
                f"refusing to replay {selection.topic!r} into itself (a loop); "
                "pass allow_same_topic=True only if that is really the intent"
            )

    async def _traverse(self, selection: Selection, *, execute: bool) -> ReplayResult:
        opts = self._options
        self._check_target(selection)
        resolved = await self._selector.resolve(selection)
        bound = min(resolved.total_records, selection.max_messages or resolved.total_records)
        if execute and bound > opts.confirm_above and not opts.assume_yes:
            if self._confirm is None or not self._confirm(resolved):
                raise ConfirmationRequired(
                    f"replaying up to {bound} messages (> {opts.confirm_above}) needs confirmation"
                )

        replay_id, started = str(uuid.uuid4()), self._clock.now()
        reader = self._selector.reader
        scanned: Counter[int] = Counter()
        matched: Counter[int] = Counter()
        skipped: Counter[str] = Counter()
        oldest = newest = None
        samples: list[Sample] = []
        last_offset: dict[int, int] = {}
        produced = total = 0

        try:
            for rng in resolved.ranges:
                if rng.count == 0:
                    continue
                async for record in reader.read(selection.topic, rng.partition, rng.start, rng.end):
                    if selection.max_messages is not None and total >= selection.max_messages:
                        break
                    scanned[rng.partition] += 1
                    last_offset[rng.partition] = record.offset
                    reason = self._skip_reason(record, selection)
                    if reason is not None:
                        skipped[reason] += 1
                        if execute:
                            self._metrics.counter(
                                REPLAY_SKIPPED, source_topic=selection.topic, reason=reason
                            )
                        continue
                    message = self._build(record, replay_id, started)
                    matched[rng.partition] += 1
                    total += 1
                    oldest = record.timestamp if oldest is None else min(oldest, record.timestamp)
                    newest = record.timestamp if newest is None else max(newest, record.timestamp)
                    if len(samples) < 3:
                        samples.append(Sample(record.key, dict(record.headers), len(record.value)))
                    if execute:  # the ONE branch between a dry run and a real run
                        await self._limiter.wait()
                        await self._producer.send(message)
                        produced += 1
                        self._metrics.counter(
                            REPLAY_PRODUCED,
                            source_topic=selection.topic,
                            target_topic=opts.target_topic,
                        )
                        if self._audit is not None:
                            self._audit.write(_audit_entry(record, message, replay_id, started))
        finally:
            if execute and self._audit is not None:
                self._audit.close()

        committed: dict[int, int] = {}
        if execute and opts.commit_source_offsets:
            for partition, offset in last_offset.items():
                await reader.commit(selection.topic, partition, offset + 1)
                committed[partition] = offset + 1

        plan = ReplayPlan(
            replay_id=replay_id,
            resolved=resolved,
            target_topic=opts.target_topic,
            partitions=tuple(
                PartitionReport(
                    r.partition, r.start, r.end, scanned[r.partition], matched[r.partition]
                )
                for r in resolved.ranges
            ),
            matched=total,
            skipped=dict(skipped),
            oldest=oldest,
            newest=newest,
            samples=tuple(samples),
            dry_run=not execute,
        )
        return ReplayResult(plan, produced, committed)

    def _skip_reason(self, record: Record, selection: Selection) -> str | None:
        if selection.predicate is not None and not selection.predicate(record):
            return SKIP_PREDICATE
        raw = get_header(record.headers, DLQ_REPLAY_COUNT)
        if raw is not None:
            try:
                count = int(raw.decode("ascii"))
            except (UnicodeDecodeError, ValueError):
                return SKIP_BAD_REPLAY_COUNT  # cannot prove it is safe: do not replay
            if count >= self._options.max_replay_count:
                return SKIP_REPLAY_COUNT
        return None

    def _build(self, record: Record, replay_id: str, started: datetime) -> OutgoingMessage:
        headers = {
            n: v for n, v in record.headers if n not in (REPLAY_ID, REPLAY_AT, DLQ_REPLAY_COUNT)
        }
        raw = get_header(record.headers, DLQ_REPLAY_COUNT)
        count = int(raw.decode("ascii")) if raw is not None else 0
        headers[DLQ_REPLAY_COUNT] = str(count + 1).encode()
        headers[REPLAY_ID] = replay_id.encode()
        headers[REPLAY_AT] = _iso(started).encode()
        return OutgoingMessage(
            topic=self._options.target_topic,
            value=record.value,  # byte-for-byte: never transformed
            key=record.key if self._options.preserve_key else None,
            headers=headers,
        )


def _audit_entry(
    record: Record, message: OutgoingMessage, replay_id: str, started: datetime
) -> dict[str, object]:
    return {
        "replay_id": replay_id,
        "replayed_at": _iso(started),
        "source_topic": record.topic,
        "source_partition": record.partition,
        "source_offset": record.offset,
        "target_topic": message.topic,
        "key": encode_key(record.key),
    }


def _iso(when: datetime | None) -> str:
    return "…" if when is None else when.strftime("%Y-%m-%dT%H:%M:%SZ")


__all__ = [
    "ConfirmationRequired",
    "PartitionReport",
    "RateLimiter",
    "ReplayOptions",
    "ReplayPlan",
    "ReplayResult",
    "ReplayRunner",
    "Sample",
]
