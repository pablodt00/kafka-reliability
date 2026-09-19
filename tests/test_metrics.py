"""metrics — fixed names and bounded label vocabulary (D11)."""

from __future__ import annotations

import pytest

from kafka_reliability import metrics
from kafka_reliability.metrics import (
    FORBIDDEN_LABELS,
    METRIC_SPECS,
    MetricsSink,
    NullMetrics,
)

D11_NAMES = {
    "OUTBOX_PENDING": "outbox.pending",
    "OUTBOX_OLDEST_PENDING_SECONDS": "outbox.oldest_pending_seconds",
    "OUTBOX_PUBLISHED": "outbox.published",
    "OUTBOX_FAILED": "outbox.failed",
    "OUTBOX_RELAY_ERRORS": "outbox.relay_errors",
    "DEDUP_CLAIMED": "dedup.claimed",
    "DEDUP_DUPLICATE": "dedup.duplicate",
    "DEDUP_IN_PROGRESS": "dedup.in_progress",
    "DEDUP_LEASE_EXPIRED": "dedup.lease_expired",
    "DEDUP_STORE_ERRORS": "dedup.store_errors",
    "REPLAY_PRODUCED": "replay.produced",
    "REPLAY_SKIPPED": "replay.skipped",
}

LABEL_VOCABULARY = frozenset(
    {"table", "topic", "kind", "group", "source_topic", "target_topic", "reason"}
)


class RecordingMetrics:
    """Test sink that records emissions and validates them against METRIC_SPECS."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def counter(self, name: str, value: int = 1, **labels: str) -> None:
        self.calls.append(("counter", name, labels))

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self.calls.append(("gauge", name, labels))

    def histogram(self, name: str, value: float, **labels: str) -> None:
        self.calls.append(("histogram", name, labels))

    def assert_emissions_valid(self) -> None:
        for kind, name, labels in self.calls:
            assert name in METRIC_SPECS, f"unknown metric {name!r}"
            spec = METRIC_SPECS[name]
            assert kind == spec.kind, f"{name} emitted as {kind}, expected {spec.kind}"
            extra = set(labels) - spec.labels
            assert not extra, f"{name} emitted with labels outside its spec: {sorted(extra)}"


def test_null_metrics_satisfies_protocol_and_discards():
    sink = NullMetrics()
    assert isinstance(sink, MetricsSink)
    assert sink.counter("outbox.published", table="t", topic="x") is None
    assert sink.gauge("outbox.pending", 3.0, table="t") is None
    assert sink.histogram("anything", 1.5, group="g") is None


def test_constants_match_d11_table():
    for const, value in D11_NAMES.items():
        assert getattr(metrics, const) == value


def test_spec_covers_exactly_the_d11_metrics():
    assert set(METRIC_SPECS) == set(D11_NAMES.values())


def test_spec_labels_come_from_fixed_vocabulary_and_never_forbidden():
    for name, spec in METRIC_SPECS.items():
        assert spec.labels <= LABEL_VOCABULARY, name
        assert not spec.labels & FORBIDDEN_LABELS, name


def test_only_outbox_backlog_metrics_are_gauges():
    gauges = {name for name, spec in METRIC_SPECS.items() if spec.kind == "gauge"}
    assert gauges == {"outbox.pending", "outbox.oldest_pending_seconds"}


def test_recording_sink_accepts_valid_emissions():
    sink = RecordingMetrics()
    sink.counter(metrics.OUTBOX_PUBLISHED, table="outbox", topic="orders")
    sink.gauge(metrics.OUTBOX_PENDING, 4, table="outbox")
    sink.counter(metrics.REPLAY_SKIPPED, source_topic="dlq", reason="poison")
    sink.assert_emissions_valid()


@pytest.mark.parametrize(
    "emit",
    [
        lambda s: s.counter("made.up", 1),
        lambda s: s.counter(metrics.DEDUP_CLAIMED, group="g", partition="3"),
        lambda s: s.counter(metrics.DEDUP_DUPLICATE, group="g", dedup_key="abc"),
        lambda s: s.gauge(metrics.DEDUP_CLAIMED, 1, group="g"),
        lambda s: s.histogram(metrics.OUTBOX_PENDING, 1.0, table="t"),
    ],
)
def test_recording_sink_rejects_invalid_emissions(emit):
    sink = RecordingMetrics()
    emit(sink)
    with pytest.raises(AssertionError):
        sink.assert_emissions_valid()
