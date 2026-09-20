"""SQL text and retention chunking, checked without a database. Behaviour on a
real Postgres is the integration suite's job."""

from __future__ import annotations

from datetime import timedelta

import pytest

pytest.importorskip("asyncpg")

from kafka_reliability.core.errors import ConfigurationError  # noqa: E402
from kafka_reliability.outbox.backends.asyncpg_relay import (  # noqa: E402
    AsyncpgRelayStore,
    claim_sql,
)
from kafka_reliability.outbox.retention import sweep_published  # noqa: E402


def test_claim_is_by_status_in_seq_order_and_never_by_a_seq_cursor():
    sql = claim_sql("outbox", sharded=False)
    assert "o.status = 'pending'" in sql and "ORDER BY o.seq" in sql
    assert "seq >" not in sql and "seq >=" not in sql


def test_claim_excludes_keys_with_a_failed_row_but_never_the_empty_key():
    sql = claim_sql("outbox", sharded=False)
    assert "f.status = 'failed'" in sql and "o.aggregateid <> ''" in sql


def test_shard_filter_hashes_the_key_and_has_no_row_id_variant():
    sharded = claim_sql("outbox", sharded=True)
    assert "hashtext(o.aggregateid)" in sharded
    assert "abs(" in sharded and "::bigint" in sharded  # hashtext can be negative
    assert "hashtext" not in claim_sql("outbox", sharded=False)
    assert "o.seq %" not in sharded and "o.id %" not in sharded


def test_claim_rejects_unsafe_table_names_via_the_store():
    class Conn:
        def is_closed(self) -> bool:
            return False

    with pytest.raises(ConfigurationError):
        AsyncpgRelayStore(Conn(), table="outbox; DROP TABLE users")  # type: ignore[arg-type]


class FakePool:
    """Answers `DELETE <n>` like asyncpg: `remaining` rows, `chunk` at a time."""

    def __init__(self, remaining: int) -> None:
        self.remaining, self.calls = remaining, []

    async def execute(self, sql: str, older_than: timedelta, chunk: int) -> str:
        self.calls.append((sql, older_than, chunk))
        n = min(self.remaining, chunk)
        self.remaining -= n
        return f"DELETE {n}"


async def test_sweep_deletes_in_chunks_and_returns_the_total():
    pool = FakePool(25_000)
    assert await sweep_published(pool, chunk=10_000) == 25_000
    assert [c[2] for c in pool.calls] == [10_000, 10_000, 10_000]  # last chunk short: stop
    assert all("status = 'published'" in c[0] for c in pool.calls)


async def test_sweep_uses_the_given_age_and_table():
    pool = FakePool(0)
    await sweep_published(pool, older_than=timedelta(days=30), table="app.outbox")
    sql, age, _ = pool.calls[0]
    assert age == timedelta(days=30) and "app.outbox" in sql


async def test_sweep_stops_on_an_exact_multiple_after_one_empty_chunk():
    pool = FakePool(20_000)
    assert await sweep_published(pool, chunk=10_000) == 20_000
    assert len(pool.calls) == 3


async def test_sweep_rejects_bad_arguments():
    with pytest.raises(ConfigurationError):
        await sweep_published(FakePool(0), chunk=0)
    with pytest.raises(ConfigurationError):
        await sweep_published(FakePool(0), table="a b")
