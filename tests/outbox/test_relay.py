"""The relay against an in-memory store: every failure mode in 02-outbox.md,
with no sleeps and no broker. Real-Postgres behaviour (advisory locks,
`hashtext`, partial-index use) is in test_relay_sql.py and the integration suite."""

from __future__ import annotations

import asyncio
from collections import defaultdict

import pytest

from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError
from kafka_reliability.core.headers import EVENT_ID
from kafka_reliability.metrics import OUTBOX_FAILED, OUTBOX_PENDING, OUTBOX_PUBLISHED
from kafka_reliability.outbox.relay import OutboxRelay, RelayConfig, default_advisory_lock_key
from kafka_reliability.producers import InMemoryProducer

from .fake_store import FakeDb, FakeRelayStore


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, float, dict[str, str]]] = []

    def counter(self, name: str, value: int = 1, **labels: str) -> None:
        self.calls.append(("counter", name, value, labels))

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self.calls.append(("gauge", name, value, labels))

    def histogram(self, name: str, value: float, **labels: str) -> None:
        self.calls.append(("histogram", name, value, labels))

    def named(self, name: str) -> list[tuple[str, str, float, dict[str, str]]]:
        return [c for c in self.calls if c[1] == name]


def relay_for(
    db: FakeDb, producer: InMemoryProducer, **cfg: object
) -> tuple[OutboxRelay, FakeRelayStore]:
    store = FakeRelayStore(db)
    return OutboxRelay(store, producer, RelayConfig(**cfg)), store  # type: ignore[arg-type]


async def test_run_once_publishes_in_seq_order_and_marks_sent():
    db, producer = FakeDb(), InMemoryProducer()
    first = db.insert("k1", b"a")
    db.insert("k2", b"b")
    db.rows[0].headers = {"traceparent": "00-abc", EVENT_ID: "forged"}
    relay, _ = relay_for(db, producer)

    result = await relay.run_once()

    assert (result.published, result.claimed, result.failed) == (2, 2, 0)
    assert {m.value for m in producer.sent} == {b"a", b"b"}
    message = producer.assert_sent(topic="t", value=b"a", key=b"k1")
    assert message.headers[EVENT_ID] == str(first.id).encode()  # stamped, not forgeable
    assert message.headers["traceparent"] == b"00-abc"
    assert {r.status for r in db.rows} == {"published"}
    assert (await relay.run_once()).claimed == 0


async def test_empty_key_is_produced_without_a_key():
    db, producer = FakeDb(), InMemoryProducer()
    db.insert("")
    relay, _ = relay_for(db, producer)
    await relay.run_once()
    assert producer.sent[0].key is None


async def test_late_committing_row_is_not_lost():
    """seq 1 commits after seq 2 has been relayed: a status-based claim still finds it."""
    db, producer = FakeDb(), InMemoryProducer()
    late = db.insert("k1", b"late", visible=False)
    db.insert("k2", b"early")
    relay, _ = relay_for(db, producer)

    await relay.run_once()
    assert [m.value for m in producer.sent] == [b"early"]

    late.visible = True
    await relay.run_once()
    assert [m.value for m in producer.sent] == [b"early", b"late"]


async def test_per_key_order_is_preserved_with_interleaved_keys():
    db, producer = FakeDb(), InMemoryProducer()
    for i in range(30):
        db.insert(f"k{i % 3}", str(i).encode())
    relay, _ = relay_for(db, producer, batch_size=7)

    while (await relay.run_once()).claimed:
        pass

    per_key: dict[bytes | None, list[int]] = defaultdict(list)
    for m in producer.sent:
        per_key[m.key].append(int(m.value))
    assert len(producer.sent) == 30
    assert all(v == sorted(v) for v in per_key.values())


# --- strict ordering: one elected relay --------------------------------------------------------


async def test_second_relay_waits_instead_of_double_publishing():
    db, producer = FakeDb(), InMemoryProducer()
    for i in range(5):
        db.insert("k", str(i).encode())
    a, _ = relay_for(db, producer)
    b, _ = relay_for(db, producer)

    assert (await a.run_once()).published == 5
    standby = await b.run_once()
    assert (standby.leader, standby.claimed) == (False, 0)
    assert [int(m.value) for m in producer.sent] == [0, 1, 2, 3, 4]


async def test_standby_takes_over_when_the_leader_releases():
    db, producer = FakeDb(), InMemoryProducer()
    a, _ = relay_for(db, producer)
    b, _ = relay_for(db, producer)
    await a.run_once()
    assert not (await b.run_once()).leader

    await a.close()
    db.insert("k", b"after-failover")
    assert (await b.run_once()).published == 1


async def test_lost_connection_stops_the_relay_rather_than_running_unlocked():
    db, producer = FakeDb(), InMemoryProducer()
    db.insert("k")
    relay, store = relay_for(db, producer)
    await relay.run_once()

    store.drop_connection()
    db.insert("k")
    with pytest.raises(StoreUnavailableError):
        await relay.run_once()
    assert len(producer.sent) == 1


async def test_run_loops_until_stopped_and_releases_leadership():
    db, producer = FakeDb(), InMemoryProducer()
    for i in range(5):
        db.insert(f"k{i}")
    relay, _ = relay_for(db, producer, batch_size=2, poll_interval=0)
    stop = asyncio.Event()

    async def stop_when_drained() -> None:
        while len(producer.sent) < 5:
            await asyncio.sleep(0)
        stop.set()

    await asyncio.wait_for(asyncio.gather(relay.run(stop), stop_when_drained()), timeout=5)
    assert len(producer.sent) == 5 and db.locks == {}


# --- sharded ordering --------------------------------------------------------------------------


async def test_shards_partition_keys_and_keep_per_key_order():
    db, producer = FakeDb(), InMemoryProducer()
    for i in range(60):
        db.insert(f"k{i % 6}", str(i).encode())
    relays = [
        relay_for(db, producer, ordering="sharded", shard_count=3, shard_index=i)[0]
        for i in range(3)
    ]

    results = await asyncio.gather(*(r.run_once() for r in relays))

    assert all(r.leader for r in results)  # one lock per shard: all three run
    assert len(producer.sent) == 60  # each row exactly once
    per_key: dict[bytes | None, list[int]] = defaultdict(list)
    for m in producer.sent:
        per_key[m.key].append(int(m.value))
    assert all(v == sorted(v) for v in per_key.values())


async def test_two_relays_with_the_same_shard_index_cannot_both_run():
    db, producer = FakeDb(), InMemoryProducer()
    cfg = dict(ordering="sharded", shard_count=2, shard_index=0)
    a, _ = relay_for(db, producer, **cfg)
    b, _ = relay_for(db, producer, **cfg)
    assert (await a.run_once()).leader
    assert not (await b.run_once()).leader


@pytest.mark.parametrize(
    "cfg",
    [
        dict(ordering="sharded", shard_count=3, shard_index=3),
        dict(ordering="sharded", shard_count=3, shard_index=-1),
        dict(ordering="sharded", shard_count=1),
        dict(ordering="strict", shard_count=2),
        dict(ordering="by-id"),
        dict(batch_size=0),
        dict(max_attempts=0),
        dict(advisory_lock_key=2**40),
    ],
)
def test_bad_config_is_rejected_at_construction(cfg):
    with pytest.raises(ConfigurationError):
        RelayConfig(**cfg)


def test_default_lock_key_is_stable_per_table():
    assert default_advisory_lock_key("outbox") == default_advisory_lock_key("outbox")
    assert default_advisory_lock_key("outbox") != default_advisory_lock_key("app.outbox")


# --- failure handling --------------------------------------------------------------------------


async def test_transient_failure_is_retried_and_counted():
    db, producer = FakeDb(), InMemoryProducer()
    row = db.insert("k")
    producer.fail_next(cause=RuntimeError("broker down"))
    relay, _ = relay_for(db, producer, max_attempts=3)

    first = await relay.run_once()
    assert (first.retried, first.published, row.status, row.attempts) == (1, 0, "pending", 1)
    assert "broker down" in (row.last_error or "")

    second = await relay.run_once()
    assert (second.published, row.status) == (1, "published")


async def test_poison_row_fails_after_max_attempts_and_blocks_only_its_key():
    db, producer = FakeDb(), InMemoryProducer()
    poison = db.insert("bad", b"too-big")
    behind = db.insert("bad", b"behind-the-poison")
    db.insert("good", b"fine-1")
    producer.fail_when(lambda m: m.value == b"too-big", cause=ValueError("MessageSizeTooLarge"))
    metrics = Recorder()
    store = FakeRelayStore(db)
    relay = OutboxRelay(store, producer, RelayConfig(max_attempts=2), metrics=metrics)

    await relay.run_once()
    result = await relay.run_once()

    assert (poison.status, poison.attempts) == ("failed", 2)
    assert "MessageSizeTooLarge" in (poison.last_error or "")  # explains why without a log search
    assert result.failed == 1
    assert behind.status == "pending"  # not skipped, not reordered, not dropped
    db.insert("good", b"fine-2")
    await relay.run_once()
    assert [m.value for m in producer.messages_for("t")] == [b"fine-1", b"fine-2"]
    assert behind.status == "pending"
    assert metrics.named(OUTBOX_FAILED)[0][3] == {"table": "outbox", "topic": "t"}


async def test_rows_behind_a_failing_row_in_the_same_batch_are_not_produced():
    db, producer = FakeDb(), InMemoryProducer()
    db.insert("k", b"1")
    db.insert("k", b"2")
    db.insert("k", b"3")
    producer.fail_when(lambda m: m.value == b"2")
    relay, _ = relay_for(db, producer)

    await relay.run_once()

    assert [m.value for m in producer.sent] == [b"1"]  # 3 must not overtake 2
    assert [r.status for r in db.rows] == ["published", "pending", "pending"]


async def test_an_unkeyed_failure_does_not_block_other_unkeyed_rows():
    db, producer = FakeDb(), InMemoryProducer()
    db.insert("", b"bad")
    db.insert("", b"ok")
    producer.fail_when(lambda m: m.value == b"bad")
    relay, _ = relay_for(db, producer, max_attempts=1)

    await relay.run_once()
    db.insert("", b"ok-2")
    await relay.run_once()

    assert [m.value for m in producer.sent] == [b"ok", b"ok-2"]


async def test_crash_between_ack_and_mark_sent_is_a_duplicate_never_a_loss():
    db, producer = FakeDb(), InMemoryProducer()
    row = db.insert("k", b"payload")
    relay, store = relay_for(db, producer)
    store.fail_next_mark = True

    with pytest.raises(StoreUnavailableError):
        await relay.run_once()
    assert len(producer.sent) == 1 and row.status == "pending"  # acked by Kafka, not marked

    await relay.run_once()  # restart
    assert len(producer.sent) == 2 and row.status == "published"
    assert {m.headers[EVENT_ID] for m in producer.sent} == {str(row.id).encode()}  # dedupable


# --- stats and metrics -------------------------------------------------------------------------


async def test_stats_reports_backlog_and_emits_gauges():
    db, producer = FakeDb(), InMemoryProducer()
    db.insert("k")
    db.insert("k")
    metrics = Recorder()
    relay = OutboxRelay(FakeRelayStore(db), producer, metrics=metrics)

    stats = await relay.stats()

    assert (stats.pending, stats.failed) == (2, 0)
    assert metrics.named(OUTBOX_PENDING) == [("gauge", OUTBOX_PENDING, 2, {"table": "outbox"})]


async def test_metric_labels_stay_within_the_bounded_vocabulary():
    from kafka_reliability.metrics import FORBIDDEN_LABELS, METRIC_SPECS

    db, producer = FakeDb(), InMemoryProducer()
    db.insert("some-message-key")
    producer.fail_next()
    metrics = Recorder()
    relay = OutboxRelay(FakeRelayStore(db), producer, RelayConfig(max_attempts=1), metrics=metrics)
    await relay.run_once()
    db.insert("k2")
    await relay.run_once()

    assert metrics.named(OUTBOX_PUBLISHED)
    for _, name, _, labels in metrics.calls:
        assert set(labels) <= METRIC_SPECS[name].labels
        assert not set(labels) & FORBIDDEN_LABELS
