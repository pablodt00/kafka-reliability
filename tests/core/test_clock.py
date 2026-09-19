"""core.clock — TTL and lease expiry must be testable without sleeping."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kafka_reliability.core.clock import Clock, ManualClock, SystemClock


def test_system_clock_is_aware_utc_and_non_decreasing():
    clock = SystemClock()
    first = clock.now()
    second = clock.now()
    assert first.utcoffset() == timedelta(0)
    assert second >= first


def test_both_clocks_satisfy_the_protocol():
    assert isinstance(SystemClock(), Clock)
    assert isinstance(ManualClock(), Clock)


def test_manual_clock_is_frozen_until_advanced():
    clock = ManualClock()
    assert clock.now() == clock.now()
    start = clock.now()
    clock.advance(timedelta(minutes=5))
    assert clock.now() == start + timedelta(minutes=5)


def test_manual_clock_start_and_set():
    start = datetime(2030, 6, 1, tzinfo=UTC)
    clock = ManualClock(start)
    assert clock.now() == start
    later = datetime(2031, 1, 1, tzinfo=UTC)
    clock.set(later)
    assert clock.now() == later


def test_manual_clock_rejects_naive_datetimes():
    with pytest.raises(ValueError):
        ManualClock(datetime(2030, 1, 1))
    with pytest.raises(ValueError):
        ManualClock().set(datetime(2030, 1, 1))


def test_manual_clock_rejects_negative_advance():
    with pytest.raises(ValueError):
        ManualClock().advance(timedelta(seconds=-1))


def test_lease_expiry_without_sleeping():
    clock: Clock = ManualClock()
    lease = timedelta(minutes=5)
    expires_at = clock.now() + lease
    assert clock.now() < expires_at
    assert isinstance(clock, ManualClock)
    clock.advance(lease)
    assert clock.now() >= expires_at
