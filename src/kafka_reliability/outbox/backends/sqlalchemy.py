"""Sync and async SqlAlchemyOutboxWriter classes (Core/ORM). Requires the
[outbox-sqlalchemy] extra.

Writers take the `Session` / `AsyncSession` the caller already has and insert
through the `Table` from `make_outbox_table`, so Core and ORM users share one
path. They enlist in the session's current transaction: they never `commit()`,
and never `flush()` beyond the single `execute` (the caller owns the session
lifecycle).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from kafka_reliability.core.errors import require_extra
from kafka_reliability.outbox.schema import PayloadType
from kafka_reliability.outbox.writer import BaseOutboxWriter, OutboxMessage, OutboxRow

try:
    import sqlalchemy as sa
    from sqlalchemy.dialects import postgresql as pg
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session
except ImportError as exc:
    require_extra(package="sqlalchemy", extra="outbox-sqlalchemy", cause=exc)


class _SqlAlchemyBase(BaseOutboxWriter):
    def __init__(self, *, table: sa.Table) -> None:
        payload: PayloadType = "jsonb" if isinstance(table.c.payload.type, pg.JSONB) else "bytea"
        super().__init__(table=table.fullname, payload=payload)
        self._sa_table = table

    def _values(self, row: OutboxRow) -> dict[str, Any]:
        return {
            "id": row.id,
            "aggregatetype": row.aggregatetype,
            "aggregateid": row.aggregateid,
            "type": row.type,
            # jsonb column: hand SQLAlchemy the parsed value, it serialises it
            "payload": json.loads(row.payload) if self.payload_type == "jsonb" else row.payload,
            "topic": row.topic,
            "headers": row.headers,
        }

    def _message(
        self,
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None,
        event_id: uuid.UUID | None,
    ) -> OutboxRow:
        return self._build_row(
            OutboxMessage(topic, payload, aggregatetype, aggregateid, type, headers or {}, event_id)
        )


class SqlAlchemyOutboxWriter(_SqlAlchemyBase):
    """Async writer on the caller's `AsyncSession`."""

    async def enqueue(
        self,
        session: AsyncSession,
        *,
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        row = self._message(topic, payload, aggregatetype, aggregateid, type, headers, event_id)
        await session.execute(self._sa_table.insert().values(**self._values(row)))
        return row.id

    async def enqueue_many(
        self, session: AsyncSession, messages: Sequence[OutboxMessage]
    ) -> list[uuid.UUID]:
        rows = self._build_rows(tuple(messages))
        if rows:
            await session.execute(self._sa_table.insert(), [self._values(r) for r in rows])
        return [r.id for r in rows]


class SyncSqlAlchemyOutboxWriter(_SqlAlchemyBase):
    """Sync writer on the caller's `Session`."""

    def enqueue(
        self,
        session: Session,
        *,
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        row = self._message(topic, payload, aggregatetype, aggregateid, type, headers, event_id)
        session.execute(self._sa_table.insert().values(**self._values(row)))
        return row.id

    def enqueue_many(self, session: Session, messages: Sequence[OutboxMessage]) -> list[uuid.UUID]:
        rows = self._build_rows(tuple(messages))
        if rows:
            session.execute(self._sa_table.insert(), [self._values(r) for r in rows])
        return [r.id for r in rows]
