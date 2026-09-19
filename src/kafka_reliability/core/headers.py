"""Header name constants shared across modules: EVENT_ID, REPLAY_ID, and
DLQ_* headers. The only coupling between outbox, dedup, and replay.

An unused header is inert: a consumer that ignores these names sees no
behaviour change, and no module requires another to be installed for its own
headers to work."""

from __future__ import annotations

from typing import Final

# Stamped by the outbox relay; read by the dedup key helper.
EVENT_ID: Final = "x-event-id"

# Stamped by the replay runner; read by the dedup replay policy.
REPLAY_ID: Final = "x-replay-id"
REPLAY_AT: Final = "x-replay-at"

# Diagnostic headers added by the DLQ router (04-replay-dlq.md, "DLQ record shape").
DLQ_SOURCE_TOPIC: Final = "x-dlq-source-topic"
DLQ_SOURCE_PARTITION: Final = "x-dlq-source-partition"
DLQ_SOURCE_OFFSET: Final = "x-dlq-source-offset"
DLQ_SOURCE_TIMESTAMP: Final = "x-dlq-source-timestamp"
DLQ_CONSUMER_GROUP: Final = "x-dlq-consumer-group"
DLQ_ERROR_TYPE: Final = "x-dlq-error-type"
DLQ_ERROR_MESSAGE: Final = "x-dlq-error-message"
DLQ_ATTEMPTS: Final = "x-dlq-attempts"
DLQ_FIRST_FAILED_AT: Final = "x-dlq-first-failed-at"
DLQ_TRACE_ID: Final = "x-dlq-trace-id"

# Incremented on every replay; drives the replay poison threshold.
DLQ_REPLAY_COUNT: Final = "x-dlq-replay-count"

__all__ = [
    "DLQ_ATTEMPTS",
    "DLQ_CONSUMER_GROUP",
    "DLQ_ERROR_MESSAGE",
    "DLQ_ERROR_TYPE",
    "DLQ_FIRST_FAILED_AT",
    "DLQ_REPLAY_COUNT",
    "DLQ_SOURCE_OFFSET",
    "DLQ_SOURCE_PARTITION",
    "DLQ_SOURCE_TIMESTAMP",
    "DLQ_SOURCE_TOPIC",
    "DLQ_TRACE_ID",
    "EVENT_ID",
    "REPLAY_AT",
    "REPLAY_ID",
]
