"""Clock protocol, injectable so TTL and lease expiry are testable without
freezing global time. No third-party imports permitted in this module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

_DEFAULT_START = datetime(2026, 1, 1, tzinfo=UTC)


@runtime_checkable
class Clock(Protocol):
    """A source of the current time."""

    def now(self) -> datetime:
        """Return the current time as a timezone-aware UTC datetime."""
        ...


class SystemClock:
    """The real wall clock."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    """A clock that only moves when told to, for tests."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = _DEFAULT_START if start is None else _require_aware(start)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward by `delta`."""
        if delta < timedelta(0):
            raise ValueError("ManualClock cannot move backwards; use set()")
        self._now += delta

    def set(self, when: datetime) -> None:
        """Jump the clock to `when`."""
        self._now = _require_aware(when)


def _require_aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock times must be timezone-aware")
    return value
