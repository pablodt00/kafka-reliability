"""Backend writers without a database: fakes prove the writer executes exactly
one INSERT on the caller's connection and never commits."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from kafka_reliability.core.errors import ConfigurationError, HeaderValidationError
from kafka_reliability.outbox.writer import OutboxMessage

KW = dict(topic="t", payload=b"p", aggregatetype="a", aggregateid="", type="T")


def test_asyncpg_rejects_a_pool_and_does_one_insert_on_a_connection():
    asyncpg = pytest.importorskip("asyncpg")
    from kafka_reliability.outbox.backends.asyncpg import AsyncpgOutboxWriter

    writer = AsyncpgOutboxWriter()
    pool = MagicMock(spec=asyncpg.Pool)
    with pytest.raises(ConfigurationError, match="Pool"):
        asyncio.run(writer.enqueue(pool, **KW))

    conn = MagicMock(spec=["execute", "executemany", "commit"])
    conn.execute = AsyncMock()
    event_id = asyncio.run(writer.enqueue(conn, **KW))
    conn.execute.assert_awaited_once()
    assert conn.execute.await_args.args[0].startswith("INSERT INTO outbox")
    assert conn.execute.await_args.args[1] == event_id
    assert not conn.commit.called


def test_asyncpg_enqueue_many_is_one_executemany_and_validates_first():
    pytest.importorskip("asyncpg")
    from kafka_reliability.outbox.backends.asyncpg import AsyncpgOutboxWriter

    writer = AsyncpgOutboxWriter()
    conn = MagicMock(spec=["execute", "executemany"])
    conn.executemany = AsyncMock()
    bad = OutboxMessage(**KW, headers={"k": b"\xff"})
    with pytest.raises(HeaderValidationError):
        asyncio.run(writer.enqueue_many(conn, [OutboxMessage(**KW), bad]))
    conn.executemany.assert_not_called()

    ids = asyncio.run(writer.enqueue_many(conn, [OutboxMessage(**KW), OutboxMessage(**KW)]))
    conn.executemany.assert_awaited_once()
    assert len(ids) == 2 and len(set(ids)) == 2


def test_psycopg_rejects_a_pool_like_object_and_never_commits():
    pytest.importorskip("psycopg")
    from kafka_reliability.outbox.backends.psycopg import (
        PsycopgOutboxWriter,
        SyncPsycopgOutboxWriter,
    )

    pool = SimpleNamespace(connection=lambda: None)  # a pool has connection(), not cursor()
    with pytest.raises(ConfigurationError, match="pool"):
        SyncPsycopgOutboxWriter().enqueue(pool, **KW)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="pool"):
        asyncio.run(PsycopgOutboxWriter().enqueue(pool, **KW))  # type: ignore[arg-type]

    conn = MagicMock(spec=["execute", "cursor", "commit"])
    SyncPsycopgOutboxWriter().enqueue(conn, **KW)
    conn.execute.assert_called_once()
    assert not conn.commit.called

    aconn = MagicMock(spec=["execute", "cursor", "commit"])
    aconn.execute = AsyncMock()
    asyncio.run(PsycopgOutboxWriter().enqueue(aconn, **KW))
    aconn.execute.assert_awaited_once()


def test_sqlalchemy_writers_execute_one_insert_and_never_commit_or_flush():
    sa = pytest.importorskip("sqlalchemy")
    from kafka_reliability.outbox.backends.sqlalchemy import (
        SqlAlchemyOutboxWriter,
        SyncSqlAlchemyOutboxWriter,
    )
    from kafka_reliability.outbox.schema import make_outbox_table

    table = make_outbox_table(sa.MetaData(), "outbox")
    session = MagicMock(spec=["execute", "commit", "flush", "rollback"])
    event_id = SyncSqlAlchemyOutboxWriter(table=table).enqueue(session, **KW)
    session.execute.assert_called_once()
    assert not (session.commit.called or session.flush.called or session.rollback.called)
    assert str(session.execute.call_args.args[0]).startswith("INSERT INTO outbox")
    assert event_id

    asession = MagicMock(spec=["execute", "commit", "flush"])
    asession.execute = AsyncMock()
    asyncio.run(SqlAlchemyOutboxWriter(table=table).enqueue(asession, **KW))
    asession.execute.assert_awaited_once()
    assert not asession.commit.called


def test_django_writer_refuses_to_run_outside_atomic(monkeypatch):
    pytest.importorskip("django")
    from kafka_reliability.outbox.backends import django as dj

    monkeypatch.setattr(dj, "connections", {"default": SimpleNamespace(in_atomic_block=False)})
    with pytest.raises(ConfigurationError, match=r"atomic\(\)"):
        dj.DjangoOutboxWriter().enqueue(**KW)
    with pytest.raises(ConfigurationError, match="'other'"):
        monkeypatch.setattr(dj, "connections", {"other": SimpleNamespace(in_atomic_block=False)})
        dj.DjangoOutboxWriter().enqueue(using="other", **KW)


def test_django_writer_inserts_through_the_named_alias_inside_atomic(monkeypatch):
    pytest.importorskip("django")
    from kafka_reliability.outbox.backends import django as dj

    cursor = MagicMock()
    conn = MagicMock(in_atomic_block=True)
    conn.cursor.return_value.__enter__.return_value = cursor
    monkeypatch.setattr(dj, "connections", {"analytics": conn})
    event_id = dj.DjangoOutboxWriter().enqueue(using="analytics", **KW)
    cursor.execute.assert_called_once()
    assert cursor.execute.call_args.args[1][0] == str(event_id)
