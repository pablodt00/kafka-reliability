from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from kafka_reliability.core.clock import ManualClock
from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError
from kafka_reliability.core.headers import EVENT_ID, REPLAY_ID
from kafka_reliability.core.message import Record
from kafka_reliability.dedup import keys
from kafka_reliability.dedup.backends.memory import InMemoryDedupStore
from kafka_reliability.dedup.backends.sqlite import SqliteDedupStore, dedup_ddl
from kafka_reliability.dedup.deduplicator import Deduplicator, RevocationSignals
from kafka_reliability.dedup.store import ClaimResult
from kafka_reliability.metrics import (
    DEDUP_CLAIMED,
    DEDUP_DUPLICATE,
    DEDUP_IN_PROGRESS,
    DEDUP_LEASE_EXPIRED,
    DEDUP_STORE_ERRORS,
    FORBIDDEN_LABELS,
    METRIC_SPECS,
)


def rec(event_id: str = "e1", *, replay: str | None = None, offset: int = 1) -> Record:
    headers = [(EVENT_ID, event_id.encode())]
    if replay:
        headers.append((REPLAY_ID, replay.encode()))
    return Record("orders", 0, offset, b"k", b"v", tuple(headers), datetime(2026, 1, 1, tzinfo=UTC))


class Metrics:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []

    def counter(self, name: str, value: int = 1, **labels: str) -> None:
        self.calls.append((name, labels))

    def gauge(self, name: str, value: float, **labels: str) -> None: ...
    def histogram(self, name: str, value: float, **labels: str) -> None: ...

    def count(self, name: str) -> int:
        return sum(1 for n, _ in self.calls if n == name)


class Flaky(InMemoryDedupStore):
    """Fails the named operations with StoreUnavailableError."""

    def __init__(self, *fail: str, clock: ManualClock | None = None) -> None:
        super().__init__(clock)
        self.fail = set(fail)

    async def claim(self, *a, **kw):
        if "claim" in self.fail:
            raise StoreUnavailableError("down")
        return await super().claim(*a, **kw)

    async def release(self, *a, **kw):
        if "release" in self.fail:
            raise StoreUnavailableError("down")
        return await super().release(*a, **kw)

    async def confirm(self, *a, **kw):
        if "confirm" in self.fail:
            raise StoreUnavailableError("down")
        return await super().confirm(*a, **kw)


def make(store=None, **kw) -> Deduplicator:
    return Deduplicator(
        store=store or InMemoryDedupStore(), group="billing", key=keys.from_header(EVENT_ID), **kw
    )


async def handle(dedup: Deduplicator, record: Record, ran: list[str], **kw):
    async with dedup.process(record, **kw) as decision:
        if decision:
            ran.append(record.headers[0][1].decode())
    return decision


# --- modes and construction -------------------------------------------------------------------


def test_transactional_is_refused_at_construction_against_a_store_that_cannot_join():
    with pytest.raises(ConfigurationError, match="silently degrading"):
        make(mode="transactional")


async def test_a_conn_against_a_store_that_cannot_join_raises_instead_of_downgrading():
    with pytest.raises(ConfigurationError):
        async with make().process(rec(), conn=object()):
            pass


async def test_transactional_mode_without_a_conn_is_an_error():
    dedup = make(SqliteDedupStore(), mode="transactional")
    with pytest.raises(ConfigurationError, match="needs conn"):
        async with dedup.process(rec()):
            pass


def test_the_key_function_has_no_default_and_bad_config_is_rejected():
    with pytest.raises(TypeError):
        Deduplicator(store=InMemoryDedupStore(), group="g")  # type: ignore[call-arg]
    for bad in (
        dict(mode="fast"),
        dict(replay_policy="x"),
        dict(on_store_unavailable="x"),
        dict(ttl=timedelta(0)),
    ):
        with pytest.raises(ConfigurationError):
            make(**bad)


async def test_claim_confirm_processes_once_and_reports_duplicates():
    dedup, ran = make(), []
    first = await handle(dedup, rec(), ran)
    second = await handle(dedup, rec(), ran)
    assert ran == ["e1"]
    assert first.result is ClaimResult.CLAIMED and first.commit_offset
    assert second.result is ClaimResult.ALREADY_DONE and not second and second.commit_offset


async def test_an_exception_releases_the_claim_so_the_message_is_retried():
    dedup, ran = make(), []
    with pytest.raises(RuntimeError, match="boom"):
        async with dedup.process(rec()) as decision:
            assert decision
            raise RuntimeError("boom")
    await handle(dedup, rec(), ran)
    assert ran == ["e1"]  # not permanently suppressed


async def test_a_live_lease_means_skip_and_do_not_commit_the_offset():
    dedup, ran = make(), []
    inside = asyncio.Event()
    release = asyncio.Event()

    async def slow_worker():
        async with dedup.process(rec()) as d:
            assert d
            inside.set()
            await release.wait()

    task = asyncio.create_task(slow_worker())
    await inside.wait()
    other = await handle(dedup, rec(), ran)
    assert (other.result, other.commit_offset, bool(other)) == (
        ClaimResult.IN_PROGRESS,
        False,
        False,
    )
    release.set()
    await task
    assert ran == []


async def test_record_after_is_explicit_and_records_only_after_success():
    dedup, ran = make(mode="record_after"), []
    with pytest.raises(RuntimeError):
        async with dedup.process(rec()) as d:
            assert d
            raise RuntimeError
    await handle(dedup, rec(), ran)  # nothing was recorded by the failure
    await handle(dedup, rec(), ran)
    assert ran == ["e1"]


async def test_transactional_mode_commits_the_dedup_row_with_the_handlers_transaction():
    store = SqliteDedupStore()
    handler_db = sqlite3.connect(":memory:", isolation_level=None)
    handler_db.executescript(dedup_ddl())
    dedup = make(store)  # mode left None: conn present -> transactional
    handler_db.execute("BEGIN")
    with pytest.raises(RuntimeError):
        async with dedup.process(rec(), conn=handler_db) as d:
            assert d
            raise RuntimeError("handler failed")
    handler_db.execute("ROLLBACK")

    ran: list[str] = []
    handler_db.execute("BEGIN")
    await handle(dedup, rec(), ran, conn=handler_db)
    handler_db.execute("COMMIT")
    again = await handle(dedup, rec(), ran, conn=handler_db)
    assert ran == ["e1"] and again.result is ClaimResult.ALREADY_DONE


# --- lease expiry: reprocess, loudly ----------------------------------------------------------


async def test_expired_lease_reprocesses_and_fires_the_distinct_metric_and_log(caplog):
    clock, metrics, ran = ManualClock(), Metrics(), []
    dedup = make(InMemoryDedupStore(clock), lease=timedelta(minutes=5), metrics=metrics)
    dead = dedup.process(rec())
    assert await dead.__aenter__()  # a worker claims, then "dies": never exits
    clock.advance(timedelta(minutes=6))

    with caplog.at_level("WARNING", logger="kafka_reliability.dedup"):
        await handle(dedup, rec(), ran)

    assert ran == ["e1"]
    assert metrics.count(DEDUP_LEASE_EXPIRED) == 1
    assert "lease expired" in caplog.text


# --- replay policy ----------------------------------------------------------------------------


async def test_replayed_messages_are_suppressed_when_you_configure_nothing():
    dedup, ran = make(), []
    await handle(dedup, rec(), ran)
    replayed = await handle(dedup, rec(replay="run-1"), ran)
    assert ran == ["e1"] and replayed.result is ClaimResult.ALREADY_DONE


async def test_bypass_processes_replays_regardless():
    dedup, ran = make(replay_policy="bypass"), []
    await handle(dedup, rec(), ran)
    d = await handle(dedup, rec(replay="run-1"), ran)
    assert ran == ["e1", "e1"] and d.result is None and d.commit_offset


async def test_namespace_dedups_within_one_replay_run_only():
    dedup, ran = make(replay_policy="namespace"), []
    await handle(dedup, rec(), ran)
    await handle(dedup, rec(replay="run-1"), ran)  # reprocessed: new namespace
    await handle(dedup, rec(replay="run-1"), ran)  # suppressed within run-1
    await handle(dedup, rec(replay="run-2"), ran)  # a different run reprocesses
    assert ran == ["e1", "e1", "e1"]


# --- store unavailability ---------------------------------------------------------------------


async def test_fail_closed_is_the_default_and_raises():
    metrics = Metrics()
    dedup = make(Flaky("claim"), metrics=metrics)
    with pytest.raises(StoreUnavailableError):
        async with dedup.process(rec()):
            pytest.fail("the handler must not run")
    assert metrics.count(DEDUP_STORE_ERRORS) == 1


async def test_fail_open_processes_without_dedup_but_still_counts_and_logs(caplog):
    metrics, ran = Metrics(), []
    dedup = make(Flaky("claim"), on_store_unavailable="fail_open", metrics=metrics)
    with caplog.at_level("ERROR", logger="kafka_reliability.dedup"):
        d = await handle(dedup, rec(), ran)
    assert ran == ["e1"] and d.result is None
    assert metrics.count(DEDUP_STORE_ERRORS) == 1 and "unavailable" in caplog.text


async def test_a_failing_release_never_masks_the_handlers_exception():
    dedup = make(Flaky("release"))
    with pytest.raises(RuntimeError, match="handler"):
        async with dedup.process(rec()):
            raise RuntimeError("handler")


async def test_a_failing_confirm_is_fail_closed_by_default():
    dedup = make(Flaky("confirm"))
    with pytest.raises(StoreUnavailableError):
        async with dedup.process(rec()):
            pass


# --- rebalance: the new owner's claim prevents the double side effect --------------------------


async def test_revocation_sets_a_signal_and_never_cancels_the_handler():
    signals = RevocationSignals()
    dedup, ran = make(), []
    dedup.revocations = signals
    signals.assign([("orders", 0)])
    finished = []
    inside, go = asyncio.Event(), asyncio.Event()

    async def old_owner():
        async with dedup.process(rec()) as d:
            if d:
                inside.set()
                await go.wait()
                finished.append(signals.signal_for("orders", 0).is_set())

    task = asyncio.create_task(old_owner())
    await inside.wait()
    signals.revoke([("orders", 0)])  # the partition moves; the handler is NOT cancelled
    new_owner = await handle(dedup, rec(), ran)  # same message, new owner
    go.set()
    await task

    assert new_owner.result is ClaimResult.IN_PROGRESS and ran == []  # the claim blocked it
    assert finished == [True]  # old handler finished, and saw the signal at its safe point


def test_assign_starts_a_fresh_unset_signal():
    signals = RevocationSignals()
    signals.revoke([("t", 1)])
    assert signals.signal_for("t", 1).is_set()
    signals.assign([("t", 1)])
    assert not signals.signal_for("t", 1).is_set()


# --- metrics stay bounded ---------------------------------------------------------------------


async def test_metric_labels_are_bounded_and_carry_no_key_or_replay_id():
    metrics = Metrics()
    dedup = make(metrics=metrics, replay_policy="namespace")
    for r in (rec("secret-key-1"), rec("secret-key-1"), rec("k2", replay="run-77")):
        await handle(dedup, r, [])
    seen = {n for n, _ in metrics.calls}
    assert {DEDUP_CLAIMED, DEDUP_DUPLICATE} <= seen
    for name, labels in metrics.calls:
        assert set(labels) <= METRIC_SPECS[name].labels and not set(labels) & FORBIDDEN_LABELS
        assert all("secret" not in v and "run-77" not in v for v in labels.values())
    assert DEDUP_IN_PROGRESS in METRIC_SPECS
