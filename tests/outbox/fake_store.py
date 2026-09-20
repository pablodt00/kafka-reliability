"""An in-memory `RelayStore`, so relay behaviour is tested without a database.

It models what the relay relies on: rows claimed by status in `seq` order,
keys with a `failed` row excluded, per-(key, shard) leadership shared between
stores on one `FakeDb`, rows that become visible late (uncommitted
transactions), and a connection that can drop or fail a write on demand. What
it cannot model is Postgres itself — `hashtext`, `pg_try_advisory_lock` and
partial-index use are covered by the SQL-text tests and by the integration
suite.
"""

from __future__ import annotations

import uuid
import zlib
from collections.abc import Sequence
from dataclasses import dataclass, field

from kafka_reliability.core.errors import StoreUnavailableError
from kafka_reliability.outbox.relay import OutboxStats, RelayRow


@dataclass
class FakeRow:
    id: uuid.UUID
    seq: int
    topic: str
    aggregateid: str
    payload: bytes
    headers: dict[str, str] = field(default_factory=dict)
    status: str = "pending"
    attempts: int = 0
    last_error: str | None = None
    visible: bool = True  # False = its transaction has not committed yet


class FakeDb:
    def __init__(self) -> None:
        self.rows: list[FakeRow] = []
        self.locks: dict[tuple[int, int], FakeRelayStore] = {}

    def insert(
        self, key: str, payload: bytes = b"x", *, topic: str = "t", visible: bool = True
    ) -> FakeRow:
        row = FakeRow(uuid.uuid4(), len(self.rows) + 1, topic, key, payload, visible=visible)
        self.rows.append(row)
        return row

    def by_status(self, status: str) -> list[FakeRow]:
        return [r for r in self.rows if r.status == status]


class FakeRelayStore:
    def __init__(self, db: FakeDb, table: str = "outbox") -> None:
        self.db = db
        self.table = table
        self.connected = True
        self.fail_next_mark = False
        self.marks = 0

    def drop_connection(self) -> None:
        self.connected = False
        for key, holder in list(self.db.locks.items()):
            if holder is self:
                del self.db.locks[key]

    def _check(self) -> None:
        if not self.connected:
            raise StoreUnavailableError("connection lost")

    async def acquire_leadership(self, key: int, shard: int) -> bool:
        self._check()
        holder = self.db.locks.setdefault((key, shard), self)
        return holder is self

    async def release_leadership(self) -> None:
        for key, holder in list(self.db.locks.items()):
            if holder is self:
                del self.db.locks[key]

    async def claim(self, limit: int, *, shard_count: int, shard_index: int) -> list[RelayRow]:
        self._check()
        blocked = {r.aggregateid for r in self.db.rows if r.status == "failed" and r.aggregateid}
        out: list[RelayRow] = []
        for r in sorted(self.db.rows, key=lambda r: r.seq):
            if r.status != "pending" or not r.visible:
                continue
            if r.aggregateid and r.aggregateid in blocked:
                continue
            if shard_count > 1 and zlib.crc32(r.aggregateid.encode()) % shard_count != shard_index:
                continue
            out.append(
                RelayRow(
                    r.id, r.seq, r.topic, r.aggregateid, r.payload, dict(r.headers), r.attempts
                )
            )
            if len(out) == limit:
                break
        return out

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
        self._check()
        if self.fail_next_mark:
            self.fail_next_mark = False
            raise StoreUnavailableError("connection lost before mark-sent")
        self.marks += 1
        for r in self.db.rows:
            if r.id in ids:
                r.status = "published"

    async def record_failure(self, row_id: uuid.UUID, error: str, *, give_up: bool) -> int:
        self._check()
        (r,) = [r for r in self.db.rows if r.id == row_id]
        r.attempts += 1
        r.last_error = error
        if give_up:
            r.status = "failed"
        return r.attempts

    async def stats(self) -> OutboxStats:
        self._check()
        pending = [r for r in self.db.rows if r.status == "pending" and r.visible]
        return OutboxStats(len(pending), float(len(pending)), len(self.db.by_status("failed")))
