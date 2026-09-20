from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402

from kafka_reliability.contrib.otel import OtelMetrics  # noqa: E402
from kafka_reliability.metrics import FORBIDDEN_LABELS, METRIC_SPECS, MetricsSink  # noqa: E402


@pytest.fixture
def otel():
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("test")
    sink = OtelMetrics(meter)

    def points():
        out = {}
        data = reader.get_metrics_data()
        for rm in data.resource_metrics if data else []:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    out[m.name] = [
                        (dict(p.attributes), getattr(p, "value", None)) for p in m.data.data_points
                    ]
        return out

    return sink, points


def test_it_is_a_metrics_sink(otel):
    assert isinstance(otel[0], MetricsSink)


def test_counters_and_gauges_reach_otel_with_their_labels(otel):
    sink, points = otel
    sink.counter("outbox.published", 2, table="outbox", topic="orders")
    sink.counter("outbox.published", table="outbox", topic="orders")
    sink.gauge("outbox.pending", 7, table="outbox")
    got = points()
    assert got["outbox.published"] == [({"table": "outbox", "topic": "orders"}, 3)]
    assert got["outbox.pending"] == [({"table": "outbox"}, 7)]


def test_instruments_are_created_once_not_per_call(otel):
    sink, _ = otel
    for _ in range(5):
        sink.counter("dedup.claimed", group="g")
    assert len(sink._instruments) == 1


def test_labels_outside_the_bounded_vocabulary_are_never_forwarded(otel, caplog):
    sink, points = otel
    sink.counter(
        "dedup.claimed", group="billing", dedup_key="order-8812", offset="99", partition="3"
    )
    sink.counter("outbox.published", table="t", topic="x", key="k1")
    for name, series in points().items():
        for attrs, _ in series:
            assert set(attrs) <= METRIC_SPECS[name].labels
            assert not set(attrs) & FORBIDDEN_LABELS
    assert points()["dedup.claimed"][0][0] == {"group": "billing"}
    assert "dropping label" in caplog.text


def test_unknown_metrics_are_ignored(otel):
    sink, points = otel
    sink.counter("made.up", foo="bar")
    assert points() == {}
