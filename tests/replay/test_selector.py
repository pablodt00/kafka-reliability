from __future__ import annotations

from datetime import timedelta

import pytest

from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.replay.selector import ReplaySelector, Selection

from .helpers import T0, make_reader


async def resolve(**kw):
    return await ReplaySelector(reader=make_reader()).resolve(Selection("orders.dlq", **kw))


def spans(resolved):
    return {r.partition: (r.start, r.end) for r in resolved.ranges}


async def test_no_bounds_selects_every_partition_in_full():
    assert spans(await resolve()) == {0: (100, 110), 1: (50, 55), 2: (0, 0)}


async def test_offset_range_is_half_open_and_clamped():
    r = await resolve(partitions=[0], from_offset={0: 102}, to_offset={0: 500})
    assert spans(r) == {0: (102, 110)}


async def test_timestamps_resolve_to_concrete_offsets_across_all_partitions():
    r = await resolve(
        from_timestamp=T0 + timedelta(minutes=3), to_timestamp=T0 + timedelta(minutes=6)
    )
    assert spans(r)[0] == (103, 106)  # records at minutes 3,4,5
    assert spans(r)[1] == (51, 51)  # p1's only record in that window: none (next is at 30 min)
    assert spans(r)[2] == (0, 0)


async def test_a_partition_with_no_records_in_the_window_resolves_to_nothing_and_says_so():
    r = await resolve(from_timestamp=T0 + timedelta(days=1))
    assert all(x.count == 0 for x in r.ranges)
    text = r.format()
    assert "(no records in range)" in text
    assert (
        "CreateTime" in text and "skewed producer clock" in text
    )  # in the output, not only the docs
    assert "have no records in the window" in text


async def test_resolving_the_same_timestamps_twice_gives_the_same_offsets():
    kw = dict(from_timestamp=T0 + timedelta(minutes=2), to_timestamp=T0 + timedelta(minutes=90))
    assert spans(await resolve(**kw)) == spans(await resolve(**kw))


async def test_the_printed_plan_reconstructs_the_run_by_offset():
    first = await resolve(
        from_timestamp=T0 + timedelta(minutes=2), to_timestamp=T0 + timedelta(minutes=8)
    )
    assert "partition 0   offsets 102 → 108   (6 records)" in first.format()
    again = await ReplaySelector(reader=make_reader()).resolve(first.as_selection())
    assert spans(again)[0] == spans(first)[0] == (102, 108)
    assert [r.count for r in again.ranges if r.count] == [r.count for r in first.ranges if r.count]


async def test_bad_selections_are_rejected():
    with pytest.raises(ConfigurationError):
        await resolve(partitions=[9])
    with pytest.raises(ConfigurationError):
        Selection("t", from_offset={0: 1}, from_timestamp=T0)
    with pytest.raises(ConfigurationError):
        Selection("t", from_timestamp=T0.replace(tzinfo=None))
    with pytest.raises(ConfigurationError):
        Selection("t", max_messages=0)
