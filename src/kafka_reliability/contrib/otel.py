"""OtelMetrics: an optional OpenTelemetry adapter for the MetricsSink
protocol. Requires the [otel] extra.

    from opentelemetry import metrics
    sink = OtelMetrics(metrics.get_meter("kafka_reliability"))

Instruments are created once per metric name and cached, not per call. Labels are
filtered to the bounded vocabulary of D11 (`metrics.METRIC_SPECS`): a label a
metric is not allowed to carry — above all a dedup key, message key, partition or
offset — is dropped, never forwarded, because unbounded cardinality is how a
metrics backend gets taken down by a library. Unknown metric names are ignored.

Prefer Prometheus? The protocol is three methods; the adapter is about ten lines:

    class PrometheusMetrics:
        def __init__(self): self._m = {}
        def _get(self, cls, name, labels):
            key = name.replace(".", "_")
            if key not in self._m:
                self._m[key] = cls(key, name, sorted(labels))
            return self._m[key]
        def counter(self, name, value=1, **labels):
            self._get(Counter, name, labels).labels(**labels).inc(value)
        def gauge(self, name, value, **labels):
            self._get(Gauge, name, labels).labels(**labels).set(value)
        def histogram(self, name, value, **labels):
            self._get(Histogram, name, labels).labels(**labels).observe(value)
"""

from __future__ import annotations

import logging
from typing import Any

from kafka_reliability.core.errors import require_extra
from kafka_reliability.metrics import FORBIDDEN_LABELS, METRIC_SPECS

try:
    import opentelemetry.metrics  # noqa: F401
except ImportError as exc:
    require_extra(package="opentelemetry-api", extra="otel", cause=exc)

log = logging.getLogger("kafka_reliability.otel")


class OtelMetrics:
    """A `MetricsSink` that records to an OpenTelemetry `Meter`."""

    def __init__(self, meter: Any) -> None:
        self._meter = meter
        self._instruments: dict[tuple[str, str], Any] = {}
        self._warned: set[tuple[str, str]] = set()

    def counter(self, name: str, value: int = 1, **labels: str) -> None:
        attrs = self._attributes(name, labels)
        if attrs is not None:
            self._instrument("counter", name).add(value, attrs)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        attrs = self._attributes(name, labels)
        if attrs is not None:
            self._instrument("gauge", name).set(value, attrs)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        attrs = self._attributes(name, labels)
        if attrs is not None:
            self._instrument("histogram", name).record(value, attrs)

    def _instrument(self, kind: str, name: str) -> Any:
        instrument = self._instruments.get((kind, name))
        if instrument is None:
            create = {
                "counter": self._meter.create_counter,
                "gauge": self._meter.create_gauge,
                "histogram": self._meter.create_histogram,
            }[kind]
            instrument = self._instruments[(kind, name)] = create(name)
        return instrument

    def _attributes(self, name: str, labels: dict[str, str]) -> dict[str, str] | None:
        spec = METRIC_SPECS.get(name)
        if spec is None:
            self._warn_once(name, "", f"ignoring unknown metric {name!r}")
            return None
        allowed = spec.labels - FORBIDDEN_LABELS
        for label in labels.keys() - allowed:
            self._warn_once(
                name, label, f"dropping label {label!r} from {name!r}: not in D11's bounded set"
            )
        return {k: v for k, v in labels.items() if k in allowed}

    def _warn_once(self, name: str, label: str, message: str) -> None:
        if (name, label) not in self._warned:
            self._warned.add((name, label))
            log.warning(message)
