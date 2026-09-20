"""Real-server dedup tests: `ON CONFLICT` under concurrency and `SET NX EX`
atomicity are exactly what a fake gets wrong (issue #7).

    KAFKA_RELIABILITY_TEST_PG_DSN=postgresql://... \\
    KAFKA_RELIABILITY_TEST_REDIS_URL=redis://localhost:6379/0 pytest -m integration

Each test skips without its variable. Not run in the default suite."""

from __future__ import annotations

import os
import uuid
from datetime import timedelta

import pytest

from .conformance import DedupStoreConformance, Harness

pytestmark = pytest.mark.integration


class TestPostgres(DedupStoreConformance):
    async def make_harness(self) -> Harness:
        dsn = os.environ.get("KAFKA_RELIABILITY_TEST_PG_DSN")
        if not dsn:
            pytest.skip("set KAFKA_RELIABILITY_TEST_PG_DSN")
        import asyncpg

        from kafka_reliability.dedup.backends.postgres import PostgresDedupStore, dedup_ddl

        table = f"dedup_{uuid.uuid4().hex[:8]}"
        pool = await asyncpg.create_pool(dsn, min_size=8, max_size=8)
        await pool.execute(dedup_ddl(table))
        store = PostgresDedupStore(pool, table=table)

        async def advance(delta: timedelta) -> None:  # the DB clock is authoritative: age the rows
            await pool.execute(f"UPDATE {table} SET expires_at = expires_at - $1::interval", delta)

        async def cleanup() -> None:
            await pool.execute(f"DROP TABLE {table}")
            await pool.close()

        return Harness(store, advance, cleanup=cleanup)


class TestRedis(DedupStoreConformance):
    async def make_harness(self) -> Harness:
        url = os.environ.get("KAFKA_RELIABILITY_TEST_REDIS_URL")
        if not url:
            pytest.skip("set KAFKA_RELIABILITY_TEST_REDIS_URL")
        import asyncio

        import redis.asyncio as aioredis

        from kafka_reliability.dedup.backends.redis import RedisDedupStore

        client = aioredis.from_url(url)
        prefix = f"kr-test:{uuid.uuid4().hex[:8]}"

        async def advance(delta: timedelta) -> None:  # real TTLs: tests use short expiries
            await asyncio.sleep(delta.total_seconds())

        async def cleanup() -> None:
            async for k in client.scan_iter(f"{prefix}:*"):
                await client.delete(k)
            await client.aclose()

        return Harness(RedisDedupStore(client, prefix=prefix), advance, False, cleanup)
