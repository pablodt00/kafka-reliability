"""DlqRouter: route a failed message and its error to a dead-letter topic,
with headers describing the failure (04-replay-dlq.md, "DLQ record shape").

The original key, value and headers are preserved **byte-for-byte**; the
diagnosis travels in namespaced `x-dlq-*` headers. Replay is therefore a plain
republish, and a consumer needs no knowledge that a message was ever
dead-lettered. The trade-off: headers must survive every hop, and error text is
capped by header size limits — full stack traces go to the log, keyed by trace
ID.

The router does not decide *whether* to dead-letter: that is the caller's
`classify` policy, which is required and has no default (a default would be a
guess about someone else's failure semantics). It does not run a retry ladder,
and works unchanged under classic consumer groups and share groups — whether the
caller then commits an offset or acknowledges a record is the caller's business.
Under share groups, route before the broker's delivery-attempt limit archives the
record. Revisit if KIP-1191 lands native DLQ routing.

Duplicate header names on the source record collapse to the last value, because
`OutgoingMessage.headers` is a mapping.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from kafka_reliability.core.clock import Clock, SystemClock
from kafka_reliability.core.errors import (
    ConfigurationError,
    PermanentError,
    TransientError,
    UnclassifiedError,
)
from kafka_reliability.core.headers import (
    DLQ_ATTEMPTS,
    DLQ_CONSUMER_GROUP,
    DLQ_ERROR_MESSAGE,
    DLQ_ERROR_TYPE,
    DLQ_FIRST_FAILED_AT,
    DLQ_REPLAY_COUNT,
    DLQ_SOURCE_OFFSET,
    DLQ_SOURCE_PARTITION,
    DLQ_SOURCE_TIMESTAMP,
    DLQ_SOURCE_TOPIC,
    DLQ_TRACE_ID,
)
from kafka_reliability.core.message import OutgoingMessage, Record, get_header
from kafka_reliability.producers.port import Producer

log = logging.getLogger("kafka_reliability.replay.dlq")

Classifier = Callable[[BaseException], bool]
"""Returns True if the error is permanent: dead-letter it. False: retry."""

_DLQ_PREFIX = "x-dlq-"


def typed_errors(error: BaseException) -> bool:
    """A `Classifier` for handlers that raise `PermanentError` / `TransientError`.

    Any other exception raises `UnclassifiedError` instead of being guessed at.
    """
    if isinstance(error, PermanentError):
        return True
    if isinstance(error, TransientError):
        return False
    raise UnclassifiedError(
        f"{type(error).__name__} is neither PermanentError nor TransientError; "
        "classify it explicitly"
    ) from error


class DlqRouter:
    """Produce failed records to a dead-letter topic.

    `topic` is a name (one DLQ per source topic, or a shared one) or a callable
    from the failed record to a topic name — e.g. `lambda r: f"{r.topic}.dlq"` or
    a per-consumer-group `f"{r.topic}.dlq.billing"`. `classify` is required.
    `include_error_message=False` omits the error text (it may hold sensitive
    data); `max_error_bytes` caps it, cutting on a UTF-8 boundary.
    """

    def __init__(
        self,
        *,
        producer: Producer,
        topic: str | Callable[[Record], str],
        consumer_group: str,
        classify: Classifier,
        include_error_message: bool = True,
        max_error_bytes: int = 1024,
        clock: Clock | None = None,
    ) -> None:
        if isinstance(topic, str) and not topic:
            raise ConfigurationError("DLQ topic must not be empty")
        if not callable(classify):
            raise ConfigurationError(
                "classify is required: pass a predicate (True = permanent, dead-letter) or "
                "`typed_errors`. The library has no default policy for retry versus dead-letter"
            )
        if max_error_bytes < 0:
            raise ConfigurationError("max_error_bytes must not be negative")
        self._producer, self._topic, self._group = producer, topic, consumer_group
        self._classify = classify
        self._include_error = include_error_message
        self._max_error_bytes = max_error_bytes
        self._clock: Clock = clock or SystemClock()

    def should_dead_letter(self, error: BaseException) -> bool:
        """Apply the caller's policy: True to dead-letter, False to retry."""
        return bool(self._classify(error))

    def dead_letter_topic(self, record: Record) -> str:
        topic = self._topic(record) if callable(self._topic) else self._topic
        if not topic:
            raise ConfigurationError("the DLQ topic callable returned an empty topic")
        return topic

    def build(self, record: Record, error: BaseException, *, attempts: int = 1) -> OutgoingMessage:
        """The dead-letter message for `record`, without producing it."""
        headers: dict[str, bytes] = {}
        for name, value in record.headers:
            # A record dead-lettered before (then replayed and failed again) carries an old
            # diagnosis; replace it, but keep the replay counter that drives the poison guard.
            if name.startswith(_DLQ_PREFIX) and name not in (DLQ_REPLAY_COUNT, DLQ_FIRST_FAILED_AT):
                continue
            headers[name] = value

        now = self._clock.now()
        headers[DLQ_SOURCE_TOPIC] = record.topic.encode()
        headers[DLQ_SOURCE_PARTITION] = str(record.partition).encode()
        headers[DLQ_SOURCE_OFFSET] = str(record.offset).encode()
        headers[DLQ_SOURCE_TIMESTAMP] = _iso(record.timestamp).encode()
        headers[DLQ_CONSUMER_GROUP] = self._group.encode()
        headers[DLQ_ERROR_TYPE] = type(error).__name__.encode()
        headers[DLQ_ATTEMPTS] = str(attempts).encode()
        headers.setdefault(DLQ_FIRST_FAILED_AT, _iso(now).encode())
        if self._include_error:
            headers[DLQ_ERROR_MESSAGE] = _truncate(str(error), self._max_error_bytes)
        trace_id = _trace_id(record)
        if trace_id is not None:
            headers[DLQ_TRACE_ID] = trace_id
        return OutgoingMessage(
            topic=self.dead_letter_topic(record),
            value=record.value,
            key=record.key,
            headers=headers,
        )

    async def route(self, record: Record, error: BaseException, *, attempts: int = 1) -> None:
        """Dead-letter `record`. Raises `ProducerError` if the DLQ produce fails —
        do not commit the source offset then, or the message is lost."""
        message = self.build(record, error, attempts=attempts)
        log.error(
            "dead-lettering %s[%d]@%d to %s after %d attempt(s), trace_id=%s",
            record.topic,
            record.partition,
            record.offset,
            message.topic,
            attempts,
            message.headers.get(DLQ_TRACE_ID, b"").decode() or "-",
            exc_info=error,  # the full stack trace lives here, keyed by trace ID
        )
        await self._producer.send(message)


def _iso(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _truncate(text: str, limit: int) -> bytes:
    return text.encode("utf-8")[:limit].decode("utf-8", errors="ignore").encode("utf-8")


def _trace_id(record: Record) -> bytes | None:
    """The trace ID of a W3C `traceparent` header, or an existing DLQ trace header."""
    existing = get_header(record.headers, DLQ_TRACE_ID)
    if existing is not None:
        return existing
    parent = get_header(record.headers, "traceparent")
    if parent is not None:
        parts = parent.split(b"-")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
    return None


__all__ = ["Classifier", "DlqRouter", "typed_errors"]
