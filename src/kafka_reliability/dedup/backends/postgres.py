"""PostgresDedupStore — the default store and the only one that supports the
strong, same-transaction mode (06-decisions.md D5). Requires the
[dedup-postgres] extra (asyncpg).

`claim` is `INSERT ... ON CONFLICT DO NOTHING`: the primary key
`(consumer_group, dedup_key)` is the concurrency control, with no explicit
locking. Pass the handler's own `asyncpg.Connection` (inside its transaction) as
`conn=` and the dedup row commits or rolls back with the handler's side effects.
Without `conn` the store uses its pool, one autocommitted statement at a time —
which is what the claim-then-confirm mode needs.

`expires_at` is stored, not derived from `processed_at + ttl`, so changing the
configured TTL never expires or resurrects existing records. All times come from
the database clock, so application clock skew cannot shorten a lease.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError, require_extra
from kafka_reliability.dedup.store import (
    DONE_STATE,
    IN_PROGRESS_STATE,
    Claim,
    ClaimResult,
    ClaimState,
)

try:
    import asyncpg
except ImportError as exc:
    require_extra(package="asyncpg", extra="dedup-postgres", cause=exc)

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
_CONNECTION_ERRORS = (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError)


def dedup_ddl(table: str = "processed_messages") -> str:
    """`CREATE TABLE` / `CREATE INDEX` for the dedup table. Emitted, never run."""
    _check_table(table)
    base = table.rsplit(".", 1)[-1]
    return (
        f"CREATE TABLE {table} (\n"
        f"    consumer_group TEXT        NOT NULL,\n"
        f"    dedup_key      TEXT        NOT NULL,\n"
        f"    state          TEXT        NOT NULL DEFAULT 'done'\n"
        f"                   CHECK (state IN ('in_progress','done')),\n"
        f"    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),\n"
        f"    expires_at     TIMESTAMPTZ NOT NULL,\n"
        f"    PRIMARY KEY (consumer_group, dedup_key)\n"
        f");\n"
        f"CREATE INDEX {base}_expiry_idx ON {table} (expires_at);\n"
    )


class PostgresDedupStore:
    supports_transactions = True

    def __init__(self, pool: Any = None, *, table: str = "processed_messages") -> None:
        self._table = _check_table(table)
        self._pool = pool

    def _target(self, conn: Any) -> Any:
        target = conn if conn is not None else self._pool
        if target is None:
            raise ConfigurationError(
                "PostgresDedupStore needs an asyncpg Pool (for claim-then-confirm) or a "
                "conn= (for the transactional mode)"
            )
        return target

    async def _exec(self, conn: Any, method: str, sql: str, *args: Any) -> Any:
        try:
            return await getattr(self._target(conn), method)(sql, *args)
        except _CONNECTION_ERRORS as exc:
            raise StoreUnavailableError("dedup store (Postgres) is unreachable") from exc

    async def claim(
        self, group: str, key: str, *, state: ClaimState, expires_in: timedelta, conn: Any = None
    ) -> Claim:
        t = self._table
        for _ in range(3):
            inserted = await self._exec(
                conn,
                "fetchval",
                f"INSERT INTO {t} (consumer_group, dedup_key, state, expires_at) "
                "VALUES ($1, $2, $3, now() + $4::interval) "
                "ON CONFLICT (consumer_group, dedup_key) DO NOTHING RETURNING 1",
                group,
                key,
                state,
                expires_in,
            )
            if inserted:
                return Claim(ClaimResult.CLAIMED)
            # Conflict. If the existing record has expired (a finished record past its
            # TTL, or a lease nobody renewed) take it over, learning what it was.
            old_state = await self._exec(
                conn,
                "fetchval",
                f"UPDATE {t} AS t SET state = $3, processed_at = now(), "
                "expires_at = now() + $4::interval "
                f"FROM (SELECT state FROM {t} WHERE consumer_group = $1 AND dedup_key = $2) AS old "
                "WHERE t.consumer_group = $1 AND t.dedup_key = $2 AND t.expires_at <= now() "
                "RETURNING old.state",
                group,
                key,
                state,
                expires_in,
            )
            if old_state is not None:
                return Claim(ClaimResult.CLAIMED, lease_expired=old_state == IN_PROGRESS_STATE)
            live = await self._exec(
                conn,
                "fetchval",
                f"SELECT state FROM {t} WHERE consumer_group = $1 AND dedup_key = $2",
                group,
                key,
            )
            if live == DONE_STATE:
                return Claim(ClaimResult.ALREADY_DONE)
            if live == IN_PROGRESS_STATE:
                return Claim(ClaimResult.IN_PROGRESS)
            # The row vanished between statements (released or swept): try again.
        raise StoreUnavailableError("could not settle a dedup claim under heavy contention")

    async def confirm(
        self, group: str, key: str, *, expires_in: timedelta, conn: Any = None
    ) -> None:
        await self._exec(
            conn,
            "execute",
            f"UPDATE {self._table} SET state = 'done', processed_at = now(), "
            "expires_at = now() + $3::interval WHERE consumer_group = $1 AND dedup_key = $2",
            group,
            key,
            expires_in,
        )

    async def release(self, group: str, key: str, *, conn: Any = None) -> None:
        await self._exec(
            conn,
            "execute",
            f"DELETE FROM {self._table} WHERE consumer_group = $1 AND dedup_key = $2 "
            "AND state = 'in_progress'",
            group,
            key,
        )

    async def is_done(self, group: str, key: str, *, conn: Any = None) -> bool:
        found = await self._exec(
            conn,
            "fetchval",
            f"SELECT 1 FROM {self._table} WHERE consumer_group = $1 AND dedup_key = $2 "
            "AND state = 'done' AND expires_at > now()",
            group,
            key,
        )
        return bool(found)

    async def purge(
        self,
        *,
        group: str | None = None,
        key: str | None = None,
        chunk: int = 10_000,
        conn: Any = None,
    ) -> int:
        t = self._table
        if key is not None:
            if group is None:
                raise ConfigurationError("purge(key=...) needs group=")
            status = await self._exec(
                conn,
                "execute",
                f"DELETE FROM {t} WHERE consumer_group = $1 AND dedup_key = $2",
                group,
                key,
            )
            return _deleted(status)
        if chunk < 1:
            raise ConfigurationError("chunk must be at least 1")
        # Chunked: an unbounded DELETE on this churn hotspot is a long lock and a burst of WAL.
        total = 0
        while True:
            status = await self._exec(
                conn,
                "execute",
                f"WITH doomed AS (SELECT consumer_group, dedup_key FROM {t} "
                "WHERE expires_at <= now() AND ($2::text IS NULL OR consumer_group = $2) "
                "LIMIT $1) "
                f"DELETE FROM {t} d USING doomed WHERE d.consumer_group = doomed.consumer_group "
                "AND d.dedup_key = doomed.dedup_key",
                chunk,
                group,
            )
            deleted = _deleted(status)
            total += deleted
            if deleted < chunk:
                return total


def _deleted(status: str) -> int:
    return int(status.rsplit(" ", 1)[-1])  # asyncpg returns "DELETE <n>"


def _check_table(table: str) -> str:
    if not _TABLE_RE.match(table):
        raise ConfigurationError(f"invalid dedup table name {table!r}: use a plain identifier")
    return table
