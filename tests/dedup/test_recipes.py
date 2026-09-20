"""The consumer recipes in docs/recipes, executed. Each is driven with fakes
shaped like its client's objects, against the real Deduplicator and a real
store, and must show: commit after the work, IN_PROGRESS = no commit and no
skipping, and an exception releasing the claim."""

from __future__ import annotations

import asyncio
import importlib.util
from datetime import timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from kafka_reliability.core.headers import EVENT_ID
from kafka_reliability.dedup import keys
from kafka_reliability.dedup.backends.memory import InMemoryDedupStore
from kafka_reliability.dedup.deduplicator import Deduplicator

RECIPES = Path(__file__).resolve().parents[2] / "docs" / "recipes"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"recipe_{name}", RECIPES / f"{name}_recipe.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_dedup(store: InMemoryDedupStore | None = None) -> Deduplicator:
    return Deduplicator(
        store=store or InMemoryDedupStore(), group="g", key=keys.from_header(EVENT_ID)
    )


def kafka_msg(offset: int, event_id: str = "e1") -> SimpleNamespace:
    return SimpleNamespace(
        topic="orders", partition=0, offset=offset, key=b"k", value=b"v",
        headers=[(EVENT_ID, event_id.encode())], timestamp=1_767_225_600_000,
    )  # fmt: skip


# --- aiokafka ---------------------------------------------------------------------------------


class FakeAioConsumer:
    def __init__(self, msgs):
        self.msgs, self.commits = msgs, []

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self.msgs:
            self.position = m.offset + 1
            yield m

    async def commit(self):
        self.commits.append(self.position)


async def test_aiokafka_recipe_commits_after_the_work_and_dedups_redelivery():
    recipe, ran = load("aiokafka"), []

    async def handler(record):
        ran.append(record.offset)

    consumer = FakeAioConsumer(
        [kafka_msg(1), kafka_msg(2, "e2"), kafka_msg(3)]
    )  # 3 = redelivery of e1
    await recipe.consume(consumer, make_dedup(), handler)
    assert ran == [1, 2] and consumer.commits == [2, 3, 4]  # the duplicate is committed too


async def test_aiokafka_recipe_failure_releases_the_claim_and_does_not_commit():
    recipe, dedup = load("aiokafka"), make_dedup()

    async def failing(record):
        raise RuntimeError("boom")

    consumer = FakeAioConsumer([kafka_msg(1)])
    with pytest.raises(RuntimeError):
        await recipe.consume(consumer, dedup, failing)
    assert consumer.commits == []

    ran = []

    async def ok(record):
        ran.append(1)

    await recipe.consume(FakeAioConsumer([kafka_msg(1)]), dedup, ok)
    assert ran == [1]  # retried, not suppressed


async def test_aiokafka_recipe_waits_on_a_live_lease_instead_of_skipping_past_it():
    recipe, dedup = load("aiokafka"), make_dedup()
    record = recipe.to_record(kafka_msg(1))
    holder = dedup.process(record)
    assert await holder.__aenter__()  # another worker's live lease

    ran, sleeps = [], []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) == 2:  # the other worker finishes
            await holder.__aexit__(None, None, None)
        await real_sleep(0)

    recipe.asyncio.sleep = fake_sleep
    try:

        async def handler(r):
            ran.append(r.offset)

        await recipe.handle_record(dedup, record, handler)
    finally:
        recipe.asyncio.sleep = real_sleep
    assert len(sleeps) == 2 and ran == []  # waited twice; never processed it twice


# --- confluent-kafka --------------------------------------------------------------------------


class FakeConfluentMsg:
    def __init__(self, offset: int, event_id: str = "e1"):
        self._offset, self._headers = offset, [(EVENT_ID, event_id.encode())]

    def topic(self):
        return "orders"  # noqa: E704

    def partition(self):
        return 0  # noqa: E704

    def offset(self):
        return self._offset  # noqa: E704

    def key(self):
        return b"k"  # noqa: E704

    def value(self):
        return b"v"  # noqa: E704

    def headers(self):
        return self._headers  # noqa: E704

    def timestamp(self):
        return (1, 1_767_225_600_000)  # noqa: E704

    def error(self):
        return None  # noqa: E704


class FakeConfluentConsumer:
    def __init__(self, msgs, stop: asyncio.Event):
        self.msgs, self.stop, self.committed = list(msgs), stop, []

    def poll(self, timeout):
        if not self.msgs:
            self.stop.set()
            return None
        return self.msgs.pop(0)

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.committed.append(message.offset())


async def test_confluent_recipe_commits_each_message_after_its_work():
    recipe, ran, stop = load("confluent"), [], asyncio.Event()

    async def handler(record):
        ran.append(record.offset)

    consumer = FakeConfluentConsumer(
        [FakeConfluentMsg(1), FakeConfluentMsg(2, "e2"), FakeConfluentMsg(3)], stop
    )
    await recipe.consume(consumer, make_dedup(), handler, stop=stop)
    assert ran == [1, 2] and consumer.committed == [1, 2, 3]


async def test_confluent_recipe_failure_does_not_commit():
    recipe, stop = load("confluent"), asyncio.Event()

    async def failing(record):
        raise ValueError

    consumer = FakeConfluentConsumer([FakeConfluentMsg(1)], stop)
    with pytest.raises(ValueError):
        await recipe.consume(consumer, make_dedup(), failing, stop=stop)
    assert consumer.committed == []


# --- FastStream -------------------------------------------------------------------------------


class FakeStreamMessage:
    def __init__(self, offset: int, event_id: str = "e1"):
        self.raw_message = kafka_msg(offset, event_id)
        self.acked = self.nacked = 0

    async def ack(self):
        self.acked += 1

    async def nack(self):
        self.nacked += 1


async def test_faststream_recipe_acks_after_the_work_and_nacks_a_live_lease():
    recipe, dedup, ran = load("faststream"), make_dedup(), []

    @recipe.deduplicated(dedup)
    async def on_order(body):
        ran.append(body)

    first, dup = FakeStreamMessage(1), FakeStreamMessage(2)
    await on_order("a", first)
    await on_order("b", dup)  # same event id
    assert ran == ["a"] and (first.acked, dup.acked) == (1, 1)

    holder = dedup.process(recipe.to_record(FakeStreamMessage(9, "busy")))
    assert await holder.__aenter__()
    busy = FakeStreamMessage(9, "busy")
    await on_order("c", busy)
    assert (busy.acked, busy.nacked) == (0, 1) and ran == ["a"]


async def test_faststream_recipe_failure_neither_acks_nor_nacks_and_releases():
    recipe, dedup = load("faststream"), make_dedup()

    @recipe.deduplicated(dedup)
    async def bad(body):
        raise RuntimeError

    msg = FakeStreamMessage(1)
    with pytest.raises(RuntimeError):
        await bad("x", msg)
    assert (msg.acked, msg.nacked) == (0, 0)
    async with dedup.process(recipe.to_record(msg)) as d:
        assert d  # the claim was released


# --- Celery -----------------------------------------------------------------------------------


class Retry(Exception):
    pass


class FakeTask:
    def retry(self, countdown):
        self.countdown = countdown
        return Retry()


def test_celery_recipe_runs_once_retries_on_a_live_lease_and_releases_on_failure(tmp_path):
    from kafka_reliability.dedup.backends.sqlite import SqliteDedupStore

    recipe, ran = load("celery"), []
    store = SqliteDedupStore(str(tmp_path / "d.db"))
    store.create_table()
    dedup = make_dedup(store)  # type: ignore[arg-type]
    record = load("aiokafka").to_record(kafka_msg(1))

    async def handler(r):
        ran.append(r.offset)

    recipe.run_deduplicated(FakeTask(), dedup, record, handler)
    recipe.run_deduplicated(FakeTask(), dedup, record, handler)  # redelivery
    assert ran == [1]

    async def failing(r):
        raise RuntimeError

    other = load("aiokafka").to_record(kafka_msg(2, "e2"))
    with pytest.raises(RuntimeError):
        recipe.run_deduplicated(FakeTask(), dedup, other, failing)
    recipe.run_deduplicated(FakeTask(), dedup, other, handler)
    assert ran == [1, 2]

    live = load("aiokafka").to_record(kafka_msg(3, "e3"))
    asyncio.run(_claim_and_hold(dedup, live))
    with pytest.raises(Retry):
        recipe.run_deduplicated(FakeTask(), dedup, live, handler)


async def _claim_and_hold(dedup: Deduplicator, record) -> None:
    # A worker mid-handler: it holds an in_progress claim and never finishes.
    await dedup._store.claim("g", "e3", state="in_progress", expires_in=timedelta(minutes=5))
