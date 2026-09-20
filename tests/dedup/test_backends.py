"""Every dedup backend against the shared conformance suite, plus the
per-backend behaviour the suite cannot express."""

from __future__ import annotations

import sqlite3
import warnings
from datetime import timedelta
from typing import Any

import pytest

from kafka_reliability.core.clock import ManualClock
from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError
from kafka_reliability.dedup.backends.memory import InMemoryDedupStore
from kafka_reliability.dedup.backends.sqlite import SqliteDedupStore, dedup_ddl
from kafka_reliability.dedup.store import ClaimResult, DedupStore

from .conformance import TTL, DedupStoreConformance, Harness


class TestInMemory(DedupStoreConformance):
    async def make_harness(self) -> Harness:
        clock = ManualClock()

        async def advance(d: timedelta) -> None:
            clock.advance(d)

        return Harness(InMemoryDedupStore(clock), advance)


class TestSqlite(DedupStoreConformance):
    async def make_harness(self) -> Harness:
        clock = ManualClock()
        store = SqliteDedupStore(clock=clock)
        store.create_table()

        async def advance(d: timedelta) -> None:
            clock.advance(d)

        return Harness(store, advance, cleanup=_close(store))


def _close(store: SqliteDedupStore):
    async def cleanup() -> None:
        store.close()

    return cleanup


# --- Redis, against a fake that models SET NX PX / GET / DEL / EVAL with a manual clock ------


class FakeRedis:
    def __init__(self, clock: ManualClock) -> None:
        self.clock, self.data, self.down = clock, {}, False

    def _live(self, k: str) -> str | None:
        item = self.data.get(k)
        if item is None or item[1] <= self.clock.now():
            self.data.pop(k, None)
            return None
        return item[0]

    def _guard(self) -> None:
        if self.down:
            raise ConnectionError("redis down")

    async def set(self, k: str, v: str, nx: bool = False, px: int | None = None) -> bool | None:
        self._guard()
        if nx and self._live(k) is not None:
            return None
        self.data[k] = (v, self.clock.now() + timedelta(milliseconds=px or 0))
        return True

    async def get(self, k: str) -> bytes | None:
        self._guard()
        v = self._live(k)
        return None if v is None else v.encode()

    async def delete(self, k: str) -> int:
        self._guard()
        return 1 if self.data.pop(k, None) is not None else 0

    async def eval(self, script: str, n: int, k: str, expected: str) -> int:
        self._guard()
        return await self.delete(k) if self._live(k) == expected else 0


redis_store_module = pytest.importorskip("redis")


class TestRedis(DedupStoreConformance):
    async def make_harness(self) -> Harness:
        from kafka_reliability.dedup.backends.redis import RedisDedupStore

        clock = ManualClock()

        async def advance(d: timedelta) -> None:
            clock.advance(d)

        return Harness(RedisDedupStore(FakeRedis(clock)), advance, sweeps_expired=False)


async def test_redis_unreachable_is_a_store_unavailable_error():
    from kafka_reliability.dedup.backends.redis import RedisDedupStore

    fake = FakeRedis(ManualClock())
    fake.down = True
    with pytest.raises(StoreUnavailableError):
        await RedisDedupStore(fake).claim("g", "k", state="done", expires_in=TTL)


async def test_redis_cannot_join_a_transaction_and_group_keys_cannot_collide():
    from kafka_reliability.dedup.backends.redis import RedisDedupStore

    store = RedisDedupStore(FakeRedis(ManualClock()))
    assert store.supports_transactions is False
    await store.claim("a:b", "c", state="done", expires_in=TTL)
    assert (
        await store.claim("a", "b:c", state="done", expires_in=TTL)
    ).result is ClaimResult.CLAIMED


# --- in-memory: the durability limitation is asserted, not glossed over -----------------------


async def test_in_memory_store_forgets_everything_on_restart():
    first = InMemoryDedupStore()
    await first.claim("g", "k", state="done", expires_in=TTL)
    restarted = InMemoryDedupStore()  # what a crashed consumer coming back gets
    assert (await restarted.claim("g", "k", state="done", expires_in=TTL)).result is (
        ClaimResult.CLAIMED
    )  # a duplicate: no guarantee across the event that most needs one


def test_in_memory_store_warns_outside_pytest(monkeypatch):
    import sys

    monkeypatch.delitem(sys.modules, "pytest")
    with pytest.warns(UserWarning, match="unit tests only"):
        InMemoryDedupStore()
    monkeypatch.setitem(sys.modules, "pytest", pytest)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        InMemoryDedupStore()


# --- sqlite: the dedup row joins the handler's own transaction --------------------------------


async def test_sqlite_dedup_row_rolls_back_with_the_handlers_transaction():
    store = SqliteDedupStore()
    store.create_table()
    handler_db = sqlite3.connect(":memory:", isolation_level=None)
    handler_db.executescript(dedup_ddl())
    handler_db.execute("BEGIN")
    claim = await store.claim("g", "k", state="done", expires_in=TTL, conn=handler_db)
    assert claim.result is ClaimResult.CLAIMED
    handler_db.execute("ROLLBACK")  # the handler failed
    assert (await store.claim("g", "k", state="done", expires_in=TTL, conn=handler_db)).result is (
        ClaimResult.CLAIMED
    )


async def test_sqlite_rejects_unsafe_table_names_and_bad_purge():
    with pytest.raises(ConfigurationError):
        SqliteDedupStore(table="x; drop")
    store = SqliteDedupStore()
    with pytest.raises(ConfigurationError):
        await store.purge(key="k")


@pytest.mark.parametrize("cls", [InMemoryDedupStore, SqliteDedupStore])
def test_backends_satisfy_the_protocol(cls: Any):
    assert isinstance(cls(), DedupStore)
