"""SqliteDedupStore — single-node deployments and container-free tests, on the
standard-library `sqlite3` (no extra).

Same claim shape as Postgres: the primary key `(consumer_group, dedup_key)` is
the concurrency control. Calls run synchronously on the event loop; SQLite
statements here are microseconds, but this is not a store for a hot multi-process
consumer fleet — SQLite allows one writer.

`supports_transactions` is true in the sense that matters: pass the
`sqlite3.Connection` your handler writes through as `conn=` and the dedup row
commits or rolls back with the handler's own transaction. Without `conn` the
store uses its own connection and commits each statement.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import timedelta
from typing import Any

from kafka_reliability.core.clock import Clock, SystemClock
from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.dedup.store import (
    DONE_STATE,
    IN_PROGRESS_STATE,
    Claim,
    ClaimResult,
    ClaimState,
)

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def dedup_ddl(table: str = "processed_messages") -> str:
    """`CREATE TABLE` for SQLite. Emitted, not run — see `SqliteDedupStore.create_table`."""
    _check_table(table)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        f"    consumer_group TEXT NOT NULL,\n"
        f"    dedup_key      TEXT NOT NULL,\n"
        f"    state          TEXT NOT NULL DEFAULT 'done',\n"
        f"    processed_at   REAL NOT NULL,\n"
        f"    expires_at     REAL NOT NULL,\n"
        f"    PRIMARY KEY (consumer_group, dedup_key)\n"
        f");\n"
        f"CREATE INDEX IF NOT EXISTS {table}_expiry_idx ON {table} (expires_at);"
    )


class SqliteDedupStore:
    supports_transactions = True

    def __init__(
        self,
        database: str = ":memory:",
        *,
        table: str = "processed_messages",
        clock: Clock | None = None,
    ) -> None:
        self._table = _check_table(table)
        self._clock: Clock = clock or SystemClock()
        self._db = sqlite3.connect(database, isolation_level=None)  # autocommit; we own it

    def create_table(self) -> None:
        """Run the DDL on the store's own connection (convenience for tests/single-node)."""
        self._db.executescript(dedup_ddl(self._table))

    def close(self) -> None:
        self._db.close()

    def _now(self) -> float:
        return self._clock.now().timestamp()

    def _run(self, conn: Any, sql: str, params: tuple[Any, ...]) -> sqlite3.Cursor:
        # With the caller's connection the write joins their open transaction;
        # with our own (autocommit) it commits immediately.
        return (conn or self._db).execute(sql, params)

    async def claim(
        self, group: str, key: str, *, state: ClaimState, expires_in: timedelta, conn: Any = None
    ) -> Claim:
        t, now = self._table, self._now()
        expires = now + expires_in.total_seconds()
        cur = self._run(
            conn,
            f"INSERT INTO {t} (consumer_group, dedup_key, state, processed_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT (consumer_group, dedup_key) DO NOTHING",
            (group, key, state, now, expires),
        )
        if cur.rowcount == 1:
            return Claim(ClaimResult.CLAIMED)
        row = self._run(
            conn,
            f"SELECT state, expires_at FROM {t} WHERE consumer_group = ? AND dedup_key = ?",
            (group, key),
        ).fetchone()
        old_state, old_expires = row
        if old_expires > now:
            return Claim(
                ClaimResult.ALREADY_DONE if old_state == DONE_STATE else ClaimResult.IN_PROGRESS
            )
        cur = self._run(
            conn,
            f"UPDATE {t} SET state = ?, processed_at = ?, expires_at = ? "
            "WHERE consumer_group = ? AND dedup_key = ? AND expires_at <= ?",
            (state, now, expires, group, key, now),
        )
        return Claim(ClaimResult.CLAIMED, lease_expired=old_state == IN_PROGRESS_STATE)

    async def confirm(
        self, group: str, key: str, *, expires_in: timedelta, conn: Any = None
    ) -> None:
        now = self._now()
        self._run(
            conn,
            f"UPDATE {self._table} SET state = 'done', processed_at = ?, expires_at = ? "
            "WHERE consumer_group = ? AND dedup_key = ?",
            (now, now + expires_in.total_seconds(), group, key),
        )

    async def release(self, group: str, key: str, *, conn: Any = None) -> None:
        self._run(
            conn,
            f"DELETE FROM {self._table} WHERE consumer_group = ? AND dedup_key = ? "
            "AND state = 'in_progress'",
            (group, key),
        )

    async def is_done(self, group: str, key: str, *, conn: Any = None) -> bool:
        return (
            self._run(
                conn,
                f"SELECT 1 FROM {self._table} WHERE consumer_group = ? AND dedup_key = ? "
                "AND state = 'done' AND expires_at > ?",
                (group, key, self._now()),
            ).fetchone()
            is not None
        )

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
            return self._run(
                conn, f"DELETE FROM {t} WHERE consumer_group = ? AND dedup_key = ?", (group, key)
            ).rowcount
        if chunk < 1:
            raise ConfigurationError("chunk must be at least 1")
        scope, extra = ("AND consumer_group = ?", (group,)) if group is not None else ("", ())
        total = 0
        while True:
            deleted = self._run(
                conn,
                f"DELETE FROM {t} WHERE rowid IN (SELECT rowid FROM {t} "
                f"WHERE expires_at <= ? {scope} LIMIT ?)",
                (self._now(), *extra, chunk),
            ).rowcount
            total += deleted
            if deleted < chunk:
                return total


def _check_table(table: str) -> str:
    if not _TABLE_RE.match(table):
        raise ConfigurationError(f"invalid dedup table name {table!r}: use a plain identifier")
    return table
