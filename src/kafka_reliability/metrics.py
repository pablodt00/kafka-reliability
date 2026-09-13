"""MetricsSink protocol (counter/gauge/histogram) and NullMetrics, the no-op
default. Labels must stay bounded — never a dedup key, message key,
partition, or offset."""
