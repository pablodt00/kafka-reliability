"""AsyncpgOutboxWriter — enqueue via an asyncpg.Connection. Requires the
[outbox-asyncpg] extra."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence

from kafka_reliability.core.errors import ConfigurationError, require_extra
from kafka_reliability.outbox.writer import BaseOutboxWriter, OutboxMessage

try:
    import asyncpg
except ImportError as exc:
    require_extra(package="asyncpg", extra="outbox-asyncpg", cause=exc)


class AsyncpgOutboxWriter(BaseOutboxWriter):
    """Insert outbox rows on the caller's `asyncpg.Connection`.

    The connection must be inside the caller's transaction
    (`async with conn.transaction():`). Passing a pool would run the insert in
    its own transaction and silently break the atomicity this pattern exists
    for, so it is rejected — by the type checker and at runtime.
    """

    async def enqueue(
        self,
        conn: asyncpg.Connection,
        *,
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """Insert one row and return its event ID. One `INSERT`, no commit."""
        _require_connection(conn)
        row = self._build_row(
            OutboxMessage(topic, payload, aggregatetype, aggregateid, type, headers or {}, event_id)
        )
        await conn.execute(self._insert_sql("numeric"), *self._params(row, id_as_str=False))
        return row.id

    async def enqueue_many(
        self, conn: asyncpg.Connection, messages: Sequence[OutboxMessage]
    ) -> list[uuid.UUID]:
        """Insert a batch in one round trip; every message is validated first."""
        _require_connection(conn)
        rows = self._build_rows(tuple(messages))
        if rows:
            await conn.executemany(
                self._insert_sql("numeric"),
                [self._params(r, id_as_str=False) for r in rows],
            )
        return [r.id for r in rows]


def _require_connection(conn: object) -> None:
    if isinstance(conn, asyncpg.Pool):
        raise ConfigurationError(
            "AsyncpgOutboxWriter needs the asyncpg.Connection of the caller's open "
            "transaction, not a Pool: a pool would insert in its own transaction and "
            "break outbox atomicity"
        )
