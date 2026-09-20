"""Selection and `ReplaySelector.resolve()` — the "select" half of *select,
then act* (04-replay-dlq.md).

Ranges are half-open: `[start, end)`. `resolve()` turns timestamps into concrete
per-partition offsets, across all partitions at once, and the result prints as

    partition 3   offsets 184100 → 184260   (160 records)

so an operator can re-run the exact same replay by offset
(`ResolvedSelection.as_selection()`) if the timestamp resolution was not what
they expected. Resolving the same timestamps twice against an unchanged topic
yields the same offsets.

Two caveats are stated in the output, not just the docs: a partition with no
records in the window resolves to nothing, and a record's timestamp is
producer-set under `CreateTime`, so a skewed producer clock puts records in the
wrong window. No third-party imports permitted in this module.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.core.message import Record
from kafka_reliability.replay.reader import Reader

TIMESTAMP_CAVEAT = (
    "timestamps are the records' CreateTime (producer-set) unless the topic uses "
    "LogAppendTime: a skewed producer clock puts records in the wrong window"
)


@dataclass(frozen=True)
class Selection:
    """What to replay. Combine offsets or timestamps with an optional predicate.

    `from_offset` / `to_offset` are per-partition, `[from, to)`. `predicate` runs
    client-side, so the whole selected range is read however few records match.
    """

    topic: str
    from_offset: Mapping[int, int] | None = None
    to_offset: Mapping[int, int] | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None
    partitions: Sequence[int] | None = None
    predicate: Callable[[Record], bool] | None = None
    max_messages: int | None = None

    def __post_init__(self) -> None:
        if not self.topic:
            raise ConfigurationError("Selection needs a topic")
        for name in ("from_timestamp", "to_timestamp"):
            value = getattr(self, name)
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ConfigurationError(f"{name} must be timezone-aware")
        if self.max_messages is not None and self.max_messages < 1:
            raise ConfigurationError("max_messages must be at least 1")
        if (self.from_offset or self.to_offset) and (self.from_timestamp or self.to_timestamp):
            raise ConfigurationError("choose offsets or timestamps for a range, not both")


@dataclass(frozen=True)
class PartitionRange:
    partition: int
    start: int
    end: int  # exclusive

    @property
    def count(self) -> int:
        return max(0, self.end - self.start)


@dataclass(frozen=True)
class ResolvedSelection:
    """`selection` with every range made concrete. Print it; it is the plan."""

    selection: Selection
    ranges: tuple[PartitionRange, ...]
    notes: tuple[str, ...] = field(default=())

    @property
    def total_records(self) -> int:
        """Upper bound on records to read (offsets, not matches: compacted or
        transactional gaps read fewer)."""
        return sum(r.count for r in self.ranges)

    def format(self) -> str:
        lines = []
        for r in self.ranges:
            if r.count == 0:
                lines.append(f"  partition {r.partition}   (no records in range)")
            else:
                lines.append(
                    f"  partition {r.partition}   offsets {r.start} → {r.end}   ({r.count} records)"
                )
        lines += [f"  note: {n}" for n in self.notes]
        return "\n".join(lines)

    def as_selection(self) -> Selection:
        """The same replay expressed by offset: exactly reproducible."""
        active = [r for r in self.ranges if r.count]
        s = self.selection
        return Selection(
            topic=s.topic,
            from_offset={r.partition: r.start for r in active},
            to_offset={r.partition: r.end for r in active},
            partitions=[r.partition for r in active],
            predicate=s.predicate,
            max_messages=s.max_messages,
        )


class ReplaySelector:
    def __init__(self, *, reader: Reader) -> None:
        self._reader = reader

    @property
    def reader(self) -> Reader:
        return self._reader

    async def resolve(self, selection: Selection) -> ResolvedSelection:
        r, topic = self._reader, selection.topic
        available = list(await r.partitions(topic))
        if selection.partitions is not None:
            unknown = sorted(set(selection.partitions) - set(available))
            if unknown:
                raise ConfigurationError(f"{topic!r} has no partition(s) {unknown}")
            chosen = sorted(set(selection.partitions))
        else:
            chosen = sorted(available)
        if not chosen:
            raise ConfigurationError(f"{topic!r} has no partitions to read")

        begin = await r.beginning_offsets(topic, chosen)
        end = await r.end_offsets(topic, chosen)
        starts = {p: begin[p] for p in chosen}
        stops = {p: end[p] for p in chosen}
        notes: list[str] = []

        if selection.from_offset:
            starts.update({p: o for p, o in selection.from_offset.items() if p in starts})
        if selection.to_offset:
            stops.update({p: o for p, o in selection.to_offset.items() if p in stops})
        if selection.from_timestamp is not None:
            found = await r.offsets_for_times(topic, _ms(selection.from_timestamp, chosen))
            for p in chosen:  # None: nothing at or after the timestamp
                starts[p] = end[p] if found[p] is None else found[p]  # type: ignore[assignment]
        if selection.to_timestamp is not None:
            found = await r.offsets_for_times(topic, _ms(selection.to_timestamp, chosen))
            for p in chosen:  # first offset at/after `to` is the exclusive end
                stops[p] = end[p] if found[p] is None else found[p]  # type: ignore[assignment]
        if selection.from_timestamp is not None or selection.to_timestamp is not None:
            notes.append(TIMESTAMP_CAVEAT)

        ranges = tuple(
            PartitionRange(p, max(starts[p], begin[p]), min(stops[p], end[p])) for p in chosen
        )
        empty = [str(x.partition) for x in ranges if x.count == 0]
        if empty and (selection.from_timestamp or selection.to_timestamp):
            notes.append(f"partition(s) {', '.join(empty)} have no records in the window")
        return ResolvedSelection(selection, ranges, tuple(notes))


def _ms(when: datetime, partitions: Sequence[int]) -> dict[int, int]:
    ms = int(when.timestamp() * 1000)
    return {p: ms for p in partitions}


__all__ = [
    "TIMESTAMP_CAVEAT",
    "PartitionRange",
    "ReplaySelector",
    "ResolvedSelection",
    "Selection",
]
