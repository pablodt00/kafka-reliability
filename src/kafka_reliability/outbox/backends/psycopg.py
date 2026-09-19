"""Sync and async PsycopgOutboxWriter classes (psycopg 3). Requires the
[outbox-psycopg] extra.

The sync writer is a separate implementation, not an `asyncio.run()` wrapper,
which would deadlock inside a running loop (05-architecture.md, "Sync or async").
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence

from kafka_reliability.core.errors import ConfigurationError, require_extra
from kafka_reliability.outbox.writer import BaseOutboxWriter, OutboxMessage

try:
    import psycopg
except ImportError as exc:
    require_extra(package="psycopg", extra="outbox-psycopg", cause=exc)


def _require_connection(conn: object) -> None:
    # A pool has connection() but no cursor(); a connection has cursor().
    if not hasattr(conn, "cursor"):
        raise ConfigurationError(
            "expected the psycopg connection of the caller's open transaction, not a "
            f"pool ({type(conn).__name__}): a pool would insert in its own transaction "
            "and break outbox atomicity"
        )


class PsycopgOutboxWriter(BaseOutboxWriter):
    """Async writer on the caller's `psycopg.AsyncConnection`. Never commits."""

    async def enqueue(
        self,
        conn: psycopg.AsyncConnection[object],
        *,
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        _require_connection(conn)
        row = self._build_row(
            OutboxMessage(topic, payload, aggregatetype, aggregateid, type, headers or {}, event_id)
        )
        await conn.execute(self._insert_sql("format"), self._params(row, id_as_str=True))
        return row.id

    async def enqueue_many(
        self, conn: psycopg.AsyncConnection[object], messages: Sequence[OutboxMessage]
    ) -> list[uuid.UUID]:
        _require_connection(conn)
        rows = self._build_rows(tuple(messages))
        if rows:
            async with conn.cursor() as cur:
                await cur.executemany(
                    self._insert_sql("format"), [self._params(r, id_as_str=True) for r in rows]
                )
        return [r.id for r in rows]


class SyncPsycopgOutboxWriter(BaseOutboxWriter):
    """Sync writer on the caller's `psycopg.Connection`. Never commits."""

    def enqueue(
        self,
        conn: psycopg.Connection[object],
        *,
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        _require_connection(conn)
        row = self._build_row(
            OutboxMessage(topic, payload, aggregatetype, aggregateid, type, headers or {}, event_id)
        )
        conn.execute(self._insert_sql("format"), self._params(row, id_as_str=True))
        return row.id

    def enqueue_many(
        self, conn: psycopg.Connection[object], messages: Sequence[OutboxMessage]
    ) -> list[uuid.UUID]:
        _require_connection(conn)
        rows = self._build_rows(tuple(messages))
        if rows:
            with conn.cursor() as cur:
                cur.executemany(
                    self._insert_sql("format"), [self._params(r, id_as_str=True) for r in rows]
                )
        return [r.id for r in rows]
