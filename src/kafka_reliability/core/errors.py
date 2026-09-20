"""Exception hierarchy shared across the outbox, dedup, and replay modules."""

from __future__ import annotations

from typing import NoReturn


class KafkaReliabilityError(Exception):
    """Base of every exception this library raises."""


class ConfigurationError(KafkaReliabilityError):
    """The library was set up in a way that cannot work.

    For example `mode="transactional"` against a store that cannot honour it.
    """


class StoreUnavailableError(KafkaReliabilityError):
    """A dedup or outbox store could not be reached."""


class ProducerError(KafkaReliabilityError):
    """A producer could not deliver a message, or could not flush.

    Producer adapters raise this in place of their client's own exceptions, so
    callers never import `aiokafka` or `confluent_kafka` to catch a failure.
    The original exception is chained as `__cause__`.
    """


class HeaderValidationError(KafkaReliabilityError, ValueError):
    """An outbox header cannot be stored.

    Raised by `enqueue()` inside the caller's transaction, so the error lands
    at the call site that caused it rather than in a relay minutes later
    (06-decisions.md D3). Binary header values must be base64-encoded by the
    caller.
    """


class RelayError(KafkaReliabilityError):
    """The outbox relay failed to produce a message to Kafka."""


class DedupKeyError(KafkaReliabilityError, ValueError):
    """A dedup key could not be derived from a record.

    Raised loudly — a record with no usable key is never silently processed or
    silently skipped, because either would defeat the point of deduplication.
    """


class PermanentError(KafkaReliabilityError):
    """Raise from a handler to say "retrying cannot help": dead-letter this record.

    `DlqRouter.should_dead_letter` treats it as permanent. The library ships no
    default policy for other exceptions (04-replay-dlq.md).
    """


class TransientError(KafkaReliabilityError):
    """Raise from a handler to say "this may succeed later": retry, do not dead-letter."""


class UnclassifiedError(KafkaReliabilityError):
    """An exception was neither `PermanentError` nor `TransientError`.

    Raised by `typed_errors` rather than guessing: whether a bare `ConnectionError`
    is worth retrying is the handler's knowledge, not the library's. The original
    exception is chained as `__cause__`.
    """


class MissingExtraError(ConfigurationError, ImportError):
    """A backend's third-party driver is not installed.

    Raised in place of a bare ImportError/ModuleNotFoundError so the message
    always names the pip extra to install, not just a driver's module name.
    """


def require_extra(*, package: str, extra: str, cause: ImportError) -> NoReturn:
    """Re-raise `cause` as a MissingExtraError naming `extra`.

    Call from the `except ImportError:` block around a backend's third-party
    import:

        try:
            import asyncpg
        except ImportError as exc:
            require_extra(package="asyncpg", extra="outbox-asyncpg", cause=exc)
    """
    raise MissingExtraError(
        f"{package!r} is required for this backend but is not installed. "
        f"Install it with: pip install 'kafka-reliability[{extra}]'"
    ) from cause
