"""Exception hierarchy shared across the outbox, dedup, and replay modules."""

from __future__ import annotations

from typing import NoReturn


class KafkaReliabilityError(Exception):
    """Base of every exception this library raises."""


class MissingExtraError(KafkaReliabilityError, ImportError):
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
