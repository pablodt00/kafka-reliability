from __future__ import annotations

import json
from datetime import timedelta

import pytest

from kafka_reliability.core.errors import ConfigurationError, ProducerError
from kafka_reliability.core.headers import DLQ_REPLAY_COUNT, REPLAY_AT, REPLAY_ID
from kafka_reliability.metrics import REPLAY_PRODUCED, REPLAY_SKIPPED
from kafka_reliability.producers import InMemoryProducer
from kafka_reliability.replay.audit import MemoryAuditSink
from kafka_reliability.replay.reader import InMemoryReader
from kafka_reliability.replay.runner import (
    ConfirmationRequired,
    RateLimiter,
    ReplayOptions,
    ReplayRunner,
)
from kafka_reliability.replay.selector import ReplaySelector, Selection

from .helpers import T0, make_reader, make_record


def runner(reader=None, producer=None, **opts):
    reader = reader or make_reader()
    producer = producer or InMemoryProducer()
    extras = {k: opts.pop(k) for k in ("audit", "metrics", "limiter", "confirm") if k in opts}
    opts.setdefault("target_topic", "orders")
    opts.setdefault("rate_per_second", None)
    r = ReplayRunner(
        selector=ReplaySelector(reader=reader),
        producer=producer,
        options=ReplayOptions(**opts),
        **extras,
    )
    return r, reader, producer


SEL = Selection("orders.dlq")


async def test_dry_run_produces_nothing_but_reports_exactly_what_execute_does():
    r, reader, producer = runner()
    plan = await r.dry_run(SEL)
    assert producer.sent == () and plan.dry_run and plan.matched == 15

    result = await r.execute(SEL)
    assert result.produced == len(producer.sent) == plan.matched
    assert [(p.partition, p.scanned, p.matched) for p in result.plan.partitions] == [
        (p.partition, p.scanned, p.matched) for p in plan.partitions
    ]
    assert plan.skipped == result.plan.skipped


async def test_dry_run_really_reads_and_applies_the_predicate_to_every_record():
    r, reader, _ = runner()
    plan = await r.dry_run(Selection("orders.dlq", predicate=lambda rec: rec.key.startswith(b"a")))
    assert (plan.matched, plan.skipped) == (10, {"predicate": 5})  # skipped-by-reason, not "~10"
    assert sorted(reader.reads) == [(0, 100, 110), (1, 50, 55)]  # it really consumed the range


async def test_records_are_republished_byte_for_byte_with_only_replay_headers_added():
    rec = make_record(
        0,
        0,
        headers=[("h", b"\x00\xff"), (REPLAY_ID, b"old"), (DLQ_REPLAY_COUNT, b"1")],
        value=b"\x00pay\xff",
    )
    reader = InMemoryReader("orders.dlq", {0: [rec]})
    r, _, p = runner(reader)
    await r.execute(SEL)
    (m,) = p.sent
    assert (m.topic, m.key, m.value) == ("orders", rec.key, rec.value)
    assert m.headers["h"] == b"\x00\xff"
    assert m.headers[DLQ_REPLAY_COUNT] == b"2"  # incremented on every replay
    assert m.headers[REPLAY_ID] != b"old" and REPLAY_AT in m.headers
    assert set(m.headers) == {"h", DLQ_REPLAY_COUNT, REPLAY_ID, REPLAY_AT}


async def test_every_record_of_a_run_shares_one_replay_id_and_runs_differ():
    r, _, p = runner()
    await r.execute(SEL)
    ids = {m.headers[REPLAY_ID] for m in p.sent}
    assert len(ids) == 1
    await r.execute(SEL)
    assert len({m.headers[REPLAY_ID] for m in p.sent}) == 2


async def test_poison_messages_past_the_replay_count_threshold_are_skipped_with_a_reason():
    recs = [make_record(0, i, headers=[(DLQ_REPLAY_COUNT, str(i).encode())]) for i in range(5)]
    recs.append(make_record(0, 5, headers=[(DLQ_REPLAY_COUNT, b"many")]))
    r, _, p = runner(InMemoryReader("orders.dlq", {0: recs}))
    plan = await r.dry_run(SEL)
    assert plan.matched == 3 and plan.skipped == {
        "replay_count_threshold": 2,
        "invalid_replay_count": 1,
    }
    assert "skipped: 3" in plan.format()
    r2, _, _ = runner(InMemoryReader("orders.dlq", {0: recs}), max_replay_count=10)
    assert (await r2.dry_run(SEL)).matched == 5


async def test_same_topic_is_refused_unless_explicitly_allowed():
    r, _, p = runner(target_topic="orders.dlq")
    for call in (r.dry_run, r.execute):
        with pytest.raises(ConfigurationError, match="itself"):
            await call(SEL)
    ok, _, _ = runner(target_topic="orders.dlq", allow_same_topic=True)
    assert (await ok.dry_run(SEL)).matched == 15


def test_the_target_topic_is_required_and_never_inferred():
    with pytest.raises(TypeError):
        ReplayOptions()  # type: ignore[call-arg]
    with pytest.raises(ConfigurationError):
        ReplayOptions(target_topic="")


async def test_max_messages_bounds_the_run():
    r, _, p = runner()
    result = await r.execute(Selection("orders.dlq", max_messages=4))
    assert result.produced == 4 == len(p.sent)


async def test_large_runs_need_confirmation():
    r, _, p = runner(confirm_above=5)
    with pytest.raises(ConfirmationRequired):
        await r.execute(SEL)
    assert p.sent == ()
    declined = ReplayRunner(
        selector=ReplaySelector(reader=make_reader()),
        producer=p,
        options=ReplayOptions("orders", rate_per_second=None, confirm_above=5),
        confirm=lambda _: False,
    )
    with pytest.raises(ConfirmationRequired):
        await declined.execute(SEL)
    accepted = ReplayRunner(
        selector=ReplaySelector(reader=make_reader()),
        producer=p,
        options=ReplayOptions("orders", rate_per_second=None, confirm_above=5),
        confirm=lambda _: True,
    )
    assert (await accepted.execute(SEL)).produced == 15
    yes, _, _ = runner(confirm_above=5, assume_yes=True)
    assert (await yes.execute(SEL)).produced == 15
    await r.dry_run(SEL)  # a dry run never needs confirmation


async def test_source_offsets_are_not_committed_unless_asked():
    r, reader, _ = runner()
    await r.execute(SEL)
    assert reader.committed == {}
    r2, reader2, _ = runner(commit_source_offsets=True)
    result = await r2.execute(SEL)
    assert reader2.committed == {0: 110, 1: 55} == result.committed_offsets
    r3, reader3, _ = runner(commit_source_offsets=True)
    await r3.dry_run(SEL)
    assert reader3.committed == {}


async def test_preserve_key_can_be_turned_off():
    r, _, p = runner(preserve_key=False)
    await r.execute(SEL)
    assert all(m.key is None for m in p.sent)


async def test_the_default_rate_limit_is_100_per_second_and_paces_produces():
    assert ReplayOptions("t").rate_per_second == 100.0
    now, slept = [0.0], []

    async def sleep(d):
        slept.append(d)
        now[0] += d

    limiter = RateLimiter(100, monotonic=lambda: now[0], sleep=sleep)
    r, _, p = runner(limiter=limiter)
    await r.execute(Selection("orders.dlq", max_messages=5))
    assert len(slept) == 4 and all(abs(s - 0.01) < 1e-9 for s in slept)


async def test_audit_answers_did_that_replay_include_a_given_key(tmp_path):
    path = tmp_path / "audit.jsonl"
    r, _, _ = runner(audit_path=path)
    result = await r.execute(Selection("orders.dlq", predicate=lambda rec: rec.key == b"b3"))
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(lines) == 1 and lines[0]["key"] == "b3"
    assert lines[0]["replay_id"] == result.plan.replay_id
    assert (lines[0]["source_topic"], lines[0]["source_partition"], lines[0]["source_offset"]) == (
        "orders.dlq",
        1,
        53,
    )
    assert lines[0]["target_topic"] == "orders"


async def test_dry_run_writes_no_audit_and_a_failed_produce_leaves_an_accurate_trail(tmp_path):
    audit = MemoryAuditSink()
    r, _, p = runner(audit=audit)
    await r.dry_run(SEL)
    assert audit.entries == []
    p.fail_when(lambda m: m.key == b"a3")
    with pytest.raises(ProducerError):
        await r.execute(SEL)
    assert [e["key"] for e in audit.entries] == ["a0", "a1", "a2"]  # exactly what was sent


async def test_metrics_use_only_bounded_labels():
    calls = []

    class M:
        def counter(self, name, value=1, **labels):
            calls.append((name, labels))

        def gauge(self, *a, **k): ...
        def histogram(self, *a, **k): ...

    r, _, _ = runner(metrics=M())
    await r.execute(Selection("orders.dlq", predicate=lambda rec: rec.partition == 0))
    names = {n for n, _ in calls}
    assert names == {REPLAY_PRODUCED, REPLAY_SKIPPED}
    assert all(set(labels) <= {"source_topic", "target_topic", "reason"} for _, labels in calls)


async def test_the_report_has_the_documented_shape():
    r, _, _ = runner()
    text = (
        await r.dry_run(
            Selection("orders.dlq", from_timestamp=T0, to_timestamp=T0 + timedelta(hours=1))
        )
    ).format()
    for expected in (
        "DRY RUN — no messages produced",
        "source:  orders.dlq",
        "target: orders",
        "partition 0   offsets 100 → 110",
        "scanned 10   matched 10",
        "(no records in range)",
        "would be produced to 'orders'",
        "oldest:",
        "sample:  key=",
        "run with --execute to replay",
    ):
        assert expected in text, expected
