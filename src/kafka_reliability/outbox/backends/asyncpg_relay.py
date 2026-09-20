"""AsyncpgRelayStore — the relay's database side, on one dedicated asyncpg
connection. Requires the [outbox-asyncpg] extra.

The advisory lock is session-level, so it lives exactly as long as the
connection does. The relay's queries run on that same connection: if the
connection drops, the lock is gone *and* the next query fails, which surfaces
as `StoreUnavailableError` and stops the relay instead of letting it carry on
unlocked.

Election is per `(key, shard)`: strict mode holds one lock for the table,
sharded mode one per shard, so two relays started with the same `shard_index`
cannot both run. Rows are claimed by `status = 'pending'`, never by a `seq`
cursor (02-outbox.md, "Concurrency and ordering").
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Sequence
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError, require_extra
from kafka_reliability.outbox.relay import OutboxStats, RelayRow
from kafka_reliability.outbox.schema import validate_table_name

try:
    import asyncpg
except ImportError as exc:
    require_extra(package="asyncpg", extra="outbox-asyncpg", cause=exc)

_CONNECTION_ERRORS = (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError)


def claim_sql(table: str, *, sharded: bool) -> str:
    """The claim query. `$1` is the batch size; sharded adds `$2` count, `$3` index.

    A key with a `failed` row is excluded so its stream stays blocked; the
    empty key (no ordering requirement) is never blocked. The shard filter
    hashes `aggregateid` — there is no row-id variant. `hashtext` can be
    negative, so it is taken as `abs(...::bigint)` (abs of an int4 minimum
    overflows): otherwise negative-hash keys would match no shard and stall.
    """
    shard = "AND abs(hashtext(o.aggregateid)::bigint) % $2 = $3" if sharded else ""
    return (
        "SELECT o.id, o.seq, o.topic, o.aggregateid, o.payload, o.headers, o.attempts\n"
        f"FROM {table} o\n"
        "WHERE o.status = 'pending'\n"
        f"  {shard}\n"
        "  AND NOT EXISTS (\n"
        f"      SELECT 1 FROM {table} f\n"
        "       WHERE f.status = 'failed' AND o.aggregateid <> '' AND f.aggregateid = o.aggregateid)\n"
        "ORDER BY o.seq\n"
        "LIMIT $1"
    )


class AsyncpgRelayStore:
    """Relay storage over the caller's dedicated `asyncpg.Connection` (not a Pool)."""

    def __init__(self, conn: asyncpg.Connection, *, table: str = "outbox") -> None:
        if isinstance(conn, asyncpg.Pool):
            raise ConfigurationError(
                "AsyncpgRelayStore needs a dedicated asyncpg.Connection, not a Pool: the "
                "advisory lock belongs to one session and must live as long as the relay"
            )
        self.table = validate_table_name(table)
        self._conn = conn
        self._io = asyncio.Lock()  # one asyncpg connection runs one query at a time
        self._held: tuple[int, int] | None = None

    async def acquire_leadership(self, key: int, shard: int) -> bool:
        if self._held == (key, shard):
            self._check_alive()
            return True
        got = await self._call("fetchval", "SELECT pg_try_advisory_lock($1, $2)", key, shard)
        if got:
            self._held = (key, shard)
        return bool(got)

    async def release_leadership(self) -> None:
        if self._held is None:
            return
        key, shard = self._held
        self._held = None
        if self._conn.is_closed():
            return  # the server dropped the session, and the lock with it
        try:
            await self._call("execute", "SELECT pg_advisory_unlock($1, $2)", key, shard)
        except StoreUnavailableError:
            pass

    async def claim(self, limit: int, *, shard_count: int, shard_index: int) -> list[RelayRow]:
        sharded = shard_count > 1
        args: tuple[Any, ...] = (limit, shard_count, shard_index) if sharded else (limit,)
        records = await self._call("fetch", claim_sql(self.table, sharded=sharded), *args)
        return [
            RelayRow(
                id=r["id"],
                seq=r["seq"],
                topic=r["topic"],
                aggregateid=r["aggregateid"],
                payload=_payload_bytes(r["payload"]),
                headers=_headers(r["headers"]),
                attempts=r["attempts"],
            )
            for r in records
        ]

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
        await self._call(
            "execute",
            f"UPDATE {self.table} SET status = 'published', published_at = now(), "
            "last_error = NULL WHERE id = ANY($1::uuid[])",
            list(ids),
        )

    async def record_failure(self, row_id: uuid.UUID, error: str, *, give_up: bool) -> int:
        status = ", status = 'failed'" if give_up else ""
        attempts = await self._call(
            "fetchval",
            f"UPDATE {self.table} SET attempts = attempts + 1, last_error = $2{status} "
            "WHERE id = $1 RETURNING attempts",
            row_id,
            error,
        )
        return int(attempts)

    async def stats(self) -> OutboxStats:
        # Three queries, each answerable from a partial index: the pending
        # count and the oldest pending row (lowest seq) from outbox_pending_idx,
        # the failed count from outbox_failed_idx. Age is measured with the
        # database clock, never the application's.
        pending = await self._call(
            "fetchval", f"SELECT count(*) FROM {self.table} WHERE status = 'pending'"
        )
        oldest = await self._call(
            "fetchval",
            f"SELECT extract(epoch FROM now() - created_at) FROM {self.table} "
            "WHERE status = 'pending' ORDER BY seq LIMIT 1",
        )
        failed = await self._call(
            "fetchval", f"SELECT count(*) FROM {self.table} WHERE status = 'failed'"
        )
        return OutboxStats(int(pending), float(oldest or 0.0), int(failed))

    def _check_alive(self) -> None:
        if self._conn.is_closed():
            self._held = None
            raise StoreUnavailableError("outbox relay connection is closed; advisory lock lost")

    async def _call(self, method: str, sql: str, *args: Any) -> Any:
        self._check_alive()
        async with self._io:
            try:
                return await getattr(self._conn, method)(sql, *args)
            except _CONNECTION_ERRORS as exc:
                self._held = None
                raise StoreUnavailableError(
                    "outbox relay lost its database connection (and its advisory lock)"
                ) from exc


def _payload_bytes(value: Any) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


def _headers(value: Any) -> dict[str, str]:
    loaded = json.loads(value) if isinstance(value, str) else value
    return {str(k): str(v) for k, v in dict(loaded or {}).items()}
