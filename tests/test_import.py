"""Bare-import smoke test: the acceptance criterion for issue #13 is that
`import kafka_reliability` (and every declared subpackage) succeeds with no
extras installed. Extras-gating error messages are a later issue's concern —
no backend file imports a third-party package yet."""

import kafka_reliability
import kafka_reliability.core
import kafka_reliability.outbox
import kafka_reliability.outbox.backends
import kafka_reliability.dedup
import kafka_reliability.dedup.backends
import kafka_reliability.replay
import kafka_reliability.producers
import kafka_reliability.contrib
import kafka_reliability.metrics


def test_bare_import_succeeds():
    assert kafka_reliability is not None
