"""The relay against a real Postgres: advisory-lock election, the `hashtext`
shard filter, `NOT EXISTS` blocking and the stats queries. Skips without
KAFKA_RELIABILITY_TEST_PG_DSN; run with `pytest -m integration`."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import defaultdict

import pytest

from kafka_reliability.outbox.relay import OutboxRelay, RelayConfig
from kafka_reliability.outbox.schema import outbox_ddl
from kafka_reliability.producers import InMemoryProducer

pytestmark = pytest.mark.integration


@pytest.fixture
async def db():
    dsn = os.environ.get("KAFKA_RELIABILITY_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set KAFKA_RELIABILITY_TEST_PG_DSN")
    import asyncpg

    table = f"outbox_{uuid.uuid4().hex[:8]}"
    admin = await asyncpg.connect(dsn)
    await admin.execute(outbox_ddl(table))
    conns: list[asyncpg.Connection] = []

    async def connect() -> asyncpg.Connection:
        c = await asyncpg.connect(dsn)
        conns.append(c)
        return c

    async def insert(key: str, n: int) -> None:
        await admin.execute(
            f"INSERT INTO {table} (id, aggregatetype, aggregateid, type, payload, topic) "
            "VALUES ($1, 'a', $2, 't', $3, 'orders')",
            uuid.uuid4(), key, str(n).encode(),
        )  # fmt: skip

    yield admin, table, connect, insert
    for c in conns:
        await c.close()
    await admin.execute(f"DROP TABLE {table}")
    await admin.close()


def per_key_order(producer: InMemoryProducer) -> dict[bytes | None, list[int]]:
    out: dict[bytes | None, list[int]] = defaultdict(list)
    for m in producer.sent:
        out[m.key].append(int(m.value))
    return out


async def test_two_strict_relays_publish_each_row_once_in_key_order(db):
    from kafka_reliability.outbox.backends.asyncpg_relay import AsyncpgRelayStore

    admin, table, connect, insert = db
    for i in range(40):
        await insert(f"k{i % 4}", i)
    producer = InMemoryProducer()
    relays = [
        OutboxRelay(AsyncpgRelayStore(await connect(), table=table), producer) for _ in range(2)
    ]
    results = await asyncio.gather(*(r.run_once() for r in relays))
    assert sorted(r.leader for r in results) == [False, True]
    assert len(producer.sent) == 40
    assert all(v == sorted(v) for v in per_key_order(producer).values())
    assert await admin.fetchval(f"SELECT count(*) FROM {table} WHERE status='published'") == 40


async def test_sharded_relays_cover_every_key_exactly_once(db):
    from kafka_reliability.outbox.backends.asyncpg_relay import AsyncpgRelayStore

    admin, table, connect, insert = db
    for i in range(90):
        await insert(f"key-{i % 9}", i)  # enough distinct keys that some hash negative
    producer = InMemoryProducer()
    relays = [
        OutboxRelay(
            AsyncpgRelayStore(await connect(), table=table),
            producer,
            RelayConfig(ordering="sharded", shard_count=3, shard_index=i),
        )
        for i in range(3)
    ]
    await asyncio.gather(*(r.run_once() for r in relays))
    assert len(producer.sent) == 90
    assert all(v == sorted(v) for v in per_key_order(producer).values())


async def test_poison_row_blocks_its_key_and_stats_see_it(db):
    from kafka_reliability.outbox.backends.asyncpg_relay import AsyncpgRelayStore

    admin, table, connect, insert = db
    await insert("bad", 1)
    await insert("bad", 2)
    await insert("good", 3)
    producer = InMemoryProducer()
    producer.fail_when(lambda m: m.value == b"1")
    relay = OutboxRelay(
        AsyncpgRelayStore(await connect(), table=table), producer, RelayConfig(max_attempts=1)
    )
    result = await relay.run_once()
    assert result.failed == 1 and [m.value for m in producer.sent] == [b"3"]
    await relay.run_once()
    assert [m.value for m in producer.sent] == [b"3"]  # key "bad" stays blocked
    stats = await relay.stats()
    assert (stats.pending, stats.failed) == (1, 1) and stats.oldest_pending_seconds >= 0
