"""MetricsSink protocol (counter/gauge/histogram) and NullMetrics, the no-op
default. Labels must stay bounded — never a dedup key, message key,
partition, or offset.

The metric names and their permitted labels are fixed by D11 in
`docs/claude/06-decisions.md`; `METRIC_SPECS` is the code mirror of that
table. No third-party imports permitted in this module."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, Protocol, runtime_checkable


@runtime_checkable
class MetricsSink(Protocol):
    """Where the library reports metrics. Labels must be bounded strings."""

    def counter(self, name: str, value: int = 1, **labels: str) -> None:
        """Increment a monotonic counter."""
        ...

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Record the current value of a gauge."""
        ...

    def histogram(self, name: str, value: float, **labels: str) -> None:
        """Record one observation of a distribution."""
        ...


class NullMetrics:
    """The default sink: discards everything."""

    def counter(self, name: str, value: int = 1, **labels: str) -> None:
        pass

    def gauge(self, name: str, value: float, **labels: str) -> None:
        pass

    def histogram(self, name: str, value: float, **labels: str) -> None:
        pass


# Metric names (D11).
OUTBOX_PENDING: Final = "outbox.pending"
OUTBOX_OLDEST_PENDING_SECONDS: Final = "outbox.oldest_pending_seconds"
OUTBOX_PUBLISHED: Final = "outbox.published"
OUTBOX_FAILED: Final = "outbox.failed"
OUTBOX_RELAY_ERRORS: Final = "outbox.relay_errors"
DEDUP_CLAIMED: Final = "dedup.claimed"
DEDUP_DUPLICATE: Final = "dedup.duplicate"
DEDUP_IN_PROGRESS: Final = "dedup.in_progress"
DEDUP_LEASE_EXPIRED: Final = "dedup.lease_expired"
DEDUP_STORE_ERRORS: Final = "dedup.store_errors"
REPLAY_PRODUCED: Final = "replay.produced"
REPLAY_SKIPPED: Final = "replay.skipped"


@dataclass(frozen=True)
class MetricSpec:
    """The instrument kind and the only label names a metric may carry."""

    kind: Literal["counter", "gauge"]
    labels: frozenset[str]


METRIC_SPECS: Final[Mapping[str, MetricSpec]] = {
    OUTBOX_PENDING: MetricSpec("gauge", frozenset({"table"})),
    OUTBOX_OLDEST_PENDING_SECONDS: MetricSpec("gauge", frozenset({"table"})),
    OUTBOX_PUBLISHED: MetricSpec("counter", frozenset({"table", "topic"})),
    OUTBOX_FAILED: MetricSpec("counter", frozenset({"table", "topic"})),
    OUTBOX_RELAY_ERRORS: MetricSpec("counter", frozenset({"table", "kind"})),
    DEDUP_CLAIMED: MetricSpec("counter", frozenset({"group"})),
    DEDUP_DUPLICATE: MetricSpec("counter", frozenset({"group"})),
    DEDUP_IN_PROGRESS: MetricSpec("counter", frozenset({"group"})),
    DEDUP_LEASE_EXPIRED: MetricSpec("counter", frozenset({"group"})),
    DEDUP_STORE_ERRORS: MetricSpec("counter", frozenset({"group", "kind"})),
    REPLAY_PRODUCED: MetricSpec("counter", frozenset({"source_topic", "target_topic"})),
    REPLAY_SKIPPED: MetricSpec("counter", frozenset({"source_topic", "reason"})),
}

# Unbounded-cardinality identifiers that must never be used as a label (D11).
FORBIDDEN_LABELS: Final = frozenset({"key", "dedup_key", "message_key", "partition", "offset"})

__all__ = [
    "DEDUP_CLAIMED",
    "DEDUP_DUPLICATE",
    "DEDUP_IN_PROGRESS",
    "DEDUP_LEASE_EXPIRED",
    "DEDUP_STORE_ERRORS",
    "FORBIDDEN_LABELS",
    "METRIC_SPECS",
    "OUTBOX_FAILED",
    "OUTBOX_OLDEST_PENDING_SECONDS",
    "OUTBOX_PENDING",
    "OUTBOX_PUBLISHED",
    "OUTBOX_RELAY_ERRORS",
    "REPLAY_PRODUCED",
    "REPLAY_SKIPPED",
    "MetricSpec",
    "MetricsSink",
    "NullMetrics",
]
