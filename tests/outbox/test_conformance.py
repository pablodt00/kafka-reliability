"""Shared write-path conformance suite against a real Postgres.

Transaction enlistment is exactly what a mock cannot verify (06-decisions.md
D13), so every writer runs the same scenarios here. Adding a fifth writer means
adding one `Harness` subclass and one entry in `HARNESSES` — not a new test file.

Runs only with `pytest -m integration` and a reachable database:

    KAFKA_RELIABILITY_TEST_PG_DSN=postgresql://user:pw@host:5432/db pytest -m integration

Without the variable every test here is skipped. The database is used only by
this suite; users of the library never need it.
"""

from __future__ import annotations

import asyncio
import os
import threading
import uuid
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from kafka_reliability.core.errors import ConfigurationError, HeaderValidationError
from kafka_reliability.outbox.schema import make_outbox_table, outbox_ddl

pytestmark = pytest.mark.integration

DSN_ENV = "KAFKA_RELIABILITY_TEST_PG_DSN"
KW: dict[str, Any] = dict(
    topic="orders", payload=b'{"n": 1}', aggregatetype="order", aggregateid="o-1", type="Created"
)


@pytest.fixture(scope="session")
def dsn() -> str:
    value = os.environ.get(DSN_ENV)
    if not value:
        pytest.skip(f"set {DSN_ENV} to run the real-Postgres conformance suite")
    return value


# --- one Harness per writer: an imperative view of "the caller's transaction" ----------------


class Harness:
    """begin -> enqueue / business_write -> commit | rollback, on one writer."""

    def __init__(self, dsn: str, table: str = "outbox", payload: str = "bytea") -> None:
        self.dsn, self.table, self.payload = dsn, table, payload

    def begin(self) -> None: ...
    def enqueue(self, **kw: Any) -> uuid.UUID: ...
    def business_write(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def backend_pid(self) -> int: ...
    def close(self) -> None: ...


class AsyncHarness(Harness):
    """Drives a coroutine-based writer from sync test code on a private loop."""

    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        self.loop = asyncio.new_event_loop()

    def run(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    def close(self) -> None:
        try:
            self._close()
        finally:
            self.loop.close()

    def _close(self) -> None: ...


class AsyncpgHarness(AsyncHarness):
    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        from kafka_reliability.outbox.backends.asyncpg import AsyncpgOutboxWriter

        self.writer = AsyncpgOutboxWriter(table=self.table, payload=self.payload)  # type: ignore[arg-type]

    def begin(self) -> None:
        import asyncpg

        self.conn = self.run(asyncpg.connect(self.dsn))
        self.tx = self.conn.transaction()
        self.run(self.tx.start())

    def enqueue(self, **kw: Any) -> uuid.UUID:
        return self.run(self.writer.enqueue(self.conn, **kw))  # type: ignore[no-any-return]

    def business_write(self) -> None:
        self.run(self.conn.execute("INSERT INTO biz (note) VALUES ('x')"))

    def commit(self) -> None:
        self.run(self.tx.commit())

    def rollback(self) -> None:
        self.run(self.tx.rollback())

    def backend_pid(self) -> int:
        return self.run(self.conn.fetchval("SELECT pg_backend_pid()"))  # type: ignore[no-any-return]

    def _close(self) -> None:
        if getattr(self, "conn", None) is not None:
            self.conn.terminate()


class PsycopgAsyncHarness(AsyncHarness):
    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        from kafka_reliability.outbox.backends.psycopg import PsycopgOutboxWriter

        self.writer = PsycopgOutboxWriter(table=self.table, payload=self.payload)  # type: ignore[arg-type]

    def begin(self) -> None:
        import psycopg

        self.conn = self.run(psycopg.AsyncConnection.connect(self.dsn))  # implicit transaction

    def enqueue(self, **kw: Any) -> uuid.UUID:
        return self.run(self.writer.enqueue(self.conn, **kw))  # type: ignore[no-any-return]

    def business_write(self) -> None:
        self.run(self.conn.execute("INSERT INTO biz (note) VALUES ('x')"))

    def commit(self) -> None:
        self.run(self.conn.commit())

    def rollback(self) -> None:
        self.run(self.conn.rollback())

    def backend_pid(self) -> int:
        return self.conn.info.backend_pid

    def _close(self) -> None:
        if getattr(self, "conn", None) is not None:
            try:
                self.run(self.conn.close())
            except Exception:
                pass


class PsycopgSyncHarness(Harness):
    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        from kafka_reliability.outbox.backends.psycopg import SyncPsycopgOutboxWriter

        self.writer = SyncPsycopgOutboxWriter(table=self.table, payload=self.payload)  # type: ignore[arg-type]

    def begin(self) -> None:
        import psycopg

        self.conn = psycopg.connect(self.dsn)

    def enqueue(self, **kw: Any) -> uuid.UUID:
        return self.writer.enqueue(self.conn, **kw)

    def business_write(self) -> None:
        self.conn.execute("INSERT INTO biz (note) VALUES ('x')")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def backend_pid(self) -> int:
        return self.conn.info.backend_pid

    def close(self) -> None:
        if getattr(self, "conn", None) is not None:
            try:
                self.conn.close()
            except Exception:
                pass


def _sa_url(dsn: str, driver: str) -> str:
    return dsn.replace("postgresql://", f"postgresql+{driver}://", 1)


def _sa_table(name: str, payload: str) -> Any:
    import sqlalchemy as sa

    return make_outbox_table(sa.MetaData(), name, payload)  # type: ignore[arg-type]


class SqlAlchemySyncHarness(Harness):
    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        from kafka_reliability.outbox.backends.sqlalchemy import SyncSqlAlchemyOutboxWriter

        self.writer = SyncSqlAlchemyOutboxWriter(table=_sa_table(self.table, self.payload))

    def begin(self) -> None:
        import sqlalchemy as sa
        from sqlalchemy.orm import Session

        self.engine = sa.create_engine(_sa_url(self.dsn, "psycopg"))
        self.session = Session(self.engine)

    def enqueue(self, **kw: Any) -> uuid.UUID:
        return self.writer.enqueue(self.session, **kw)

    def business_write(self) -> None:
        import sqlalchemy as sa

        self.session.execute(sa.text("INSERT INTO biz (note) VALUES ('x')"))

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()

    def backend_pid(self) -> int:
        import sqlalchemy as sa

        return self.session.execute(sa.text("SELECT pg_backend_pid()")).scalar_one()

    def close(self) -> None:
        if getattr(self, "engine", None) is not None:
            try:
                self.session.close()
            except Exception:
                pass
            self.engine.dispose()


class SqlAlchemyAsyncHarness(AsyncHarness):
    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        from kafka_reliability.outbox.backends.sqlalchemy import SqlAlchemyOutboxWriter

        self.writer = SqlAlchemyOutboxWriter(table=_sa_table(self.table, self.payload))

    def begin(self) -> None:
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

        self.engine = create_async_engine(_sa_url(self.dsn, "asyncpg"))
        self.session = AsyncSession(self.engine)

    def enqueue(self, **kw: Any) -> uuid.UUID:
        return self.run(self.writer.enqueue(self.session, **kw))  # type: ignore[no-any-return]

    def business_write(self) -> None:
        import sqlalchemy as sa

        self.run(self.session.execute(sa.text("INSERT INTO biz (note) VALUES ('x')")))

    def commit(self) -> None:
        self.run(self.session.commit())

    def rollback(self) -> None:
        self.run(self.session.rollback())

    def backend_pid(self) -> int:
        import sqlalchemy as sa

        return self.run(self.session.execute(sa.text("SELECT pg_backend_pid()"))).scalar_one()  # type: ignore[no-any-return]

    def _close(self) -> None:
        if getattr(self, "engine", None) is not None:
            try:
                self.run(self.session.close())
            except Exception:
                pass
            self.run(self.engine.dispose())


def _configure_django(dsn: str) -> None:
    import django
    from django.conf import settings

    if settings.configured:
        return
    url = urlparse(dsn)
    host = parse_qs(url.query).get("host", [url.hostname or ""])[0]
    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": url.path.lstrip("/"),
                "USER": url.username or "",
                "PASSWORD": url.password or "",
                "HOST": host,
                "PORT": str(url.port or ""),
            }
        },
        DEFAULT_AUTO_FIELD="django.db.models.AutoField",
        USE_TZ=True,
    )
    django.setup()


class DjangoHarness(Harness):
    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        _configure_django(self.dsn)
        from kafka_reliability.outbox.backends.django import DjangoOutboxWriter

        self.writer = DjangoOutboxWriter(table=self.table, payload=self.payload)  # type: ignore[arg-type]

    def begin(self) -> None:
        from django.db import transaction

        self.atomic = transaction.atomic()
        self.atomic.__enter__()

    def enqueue(self, **kw: Any) -> uuid.UUID:
        return self.writer.enqueue(**kw)

    def _cursor(self) -> Any:
        from django.db import connection

        return connection.cursor()

    def business_write(self) -> None:
        with self._cursor() as cur:
            cur.execute("INSERT INTO biz (note) VALUES ('x')")

    def commit(self) -> None:
        self.atomic.__exit__(None, None, None)

    def rollback(self) -> None:
        exc = RuntimeError("rollback")
        self.atomic.__exit__(RuntimeError, exc, None)

    def backend_pid(self) -> int:
        with self._cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            return int(cur.fetchone()[0])

    def close(self) -> None:
        from django.db import connections

        try:
            connections.close_all()
        except Exception:
            pass


HARNESSES = {
    "asyncpg": AsyncpgHarness,
    "psycopg-async": PsycopgAsyncHarness,
    "psycopg-sync": PsycopgSyncHarness,
    "sqlalchemy-async": SqlAlchemyAsyncHarness,
    "sqlalchemy-sync": SqlAlchemySyncHarness,
    "django": DjangoHarness,
}


@pytest.fixture(params=list(HARNESSES))
def make(request: pytest.FixtureRequest, dsn: str, db: Any):  # noqa: ANN201
    made: list[Harness] = []

    def factory(table: str = "outbox", payload: str = "bytea") -> Harness:
        h = HARNESSES[request.param](dsn, table, payload)
        made.append(h)
        return h

    yield factory
    for h in made:
        h.close()


# --- database fixtures (one admin connection, separate from every writer) ---------------------


class Db:
    def __init__(self, dsn: str) -> None:
        import psycopg

        self.conn = psycopg.connect(dsn, autocommit=True)

    def q(self, sql: str, *args: Any) -> list[tuple[Any, ...]]:
        return self.conn.execute(sql, args or None).fetchall()

    def rows(self, table: str = "outbox") -> list[tuple[Any, ...]]:
        return self.q(f"SELECT id, topic, aggregateid, headers, payload FROM {table}")

    def biz(self) -> int:
        return int(self.q("SELECT count(*) FROM biz")[0][0])


@pytest.fixture(scope="session")
def _schema(dsn: str) -> Any:
    db = Db(dsn)
    db.conn.execute("DROP TABLE IF EXISTS outbox, outbox_j, biz CASCADE")
    db.conn.execute(outbox_ddl("outbox"))
    db.conn.execute(outbox_ddl("outbox_j", "jsonb"))
    db.conn.execute("CREATE TABLE biz (id serial PRIMARY KEY, note text)")
    yield db
    db.conn.execute("DROP TABLE IF EXISTS outbox, outbox_j, biz CASCADE")
    db.conn.close()


@pytest.fixture
def db(_schema: Db) -> Db:
    _schema.conn.execute("TRUNCATE outbox, outbox_j, biz")
    return _schema


# --- the scenarios ----------------------------------------------------------------------------


def test_commit_leaves_exactly_one_row_with_the_returned_event_id(make: Any, db: Db) -> None:
    h = make()
    h.begin()
    h.business_write()
    event_id = h.enqueue(**KW, headers={"traceparent": b"00-abc"})
    h.commit()
    ((row_id, topic, key, headers, payload),) = db.rows()
    assert row_id == event_id and topic == "orders" and key == "o-1"
    assert headers == {"traceparent": "00-abc"} and bytes(payload) == KW["payload"]
    assert db.biz() == 1


def test_rollback_leaves_no_row(make: Any, db: Db) -> None:
    h = make()
    h.begin()
    h.business_write()
    h.enqueue(**KW)
    h.rollback()
    assert db.rows() == [] and db.biz() == 0


def test_the_row_is_invisible_to_others_until_commit(make: Any, db: Db) -> None:
    h = make()
    h.begin()
    h.enqueue(**KW)
    assert db.rows() == []
    h.commit()
    assert len(db.rows()) == 1


def test_killing_the_connection_between_the_two_writes_loses_both(make: Any, db: Db) -> None:
    h = make()
    h.begin()
    h.business_write()
    h.enqueue(**KW)
    db.q("SELECT pg_terminate_backend(%s)", h.backend_pid())
    with pytest.raises(Exception):  # noqa: B017 - each driver raises its own error type
        h.commit()
    assert db.rows() == [] and db.biz() == 0


def test_non_utf8_header_is_rejected_at_the_call_site_and_the_transaction_survives(
    make: Any, db: Db
) -> None:
    h = make()
    h.begin()
    h.business_write()
    with pytest.raises(HeaderValidationError):
        h.enqueue(**KW, headers={"sig": b"\xff\xfe"})
    h.commit()  # validation ran before any SQL, so the caller's transaction is not poisoned
    assert db.rows() == [] and db.biz() == 1


def test_enqueue_opens_no_connection_of_its_own(make: Any, db: Db) -> None:
    h = make()
    h.begin()
    h.business_write()
    before = db.q("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")[0][0]
    for _ in range(3):
        h.enqueue(**KW)
    after = db.q("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")[0][0]
    h.rollback()
    assert after == before


def test_empty_aggregateid_is_stored_as_the_empty_string(make: Any, db: Db) -> None:
    h = make()
    h.begin()
    h.enqueue(**{**KW, "aggregateid": ""})
    h.commit()
    assert db.rows()[0][2] == ""


def test_caller_supplied_event_id_is_used(make: Any, db: Db) -> None:
    h = make()
    eid = uuid.uuid4()
    h.begin()
    assert h.enqueue(**KW, event_id=eid) == eid
    h.commit()
    assert db.rows()[0][0] == eid


def test_jsonb_payload_variant_round_trips(make: Any, db: Db) -> None:
    h = make("outbox_j", "jsonb")
    h.begin()
    h.enqueue(**KW)
    h.commit()
    assert db.q("SELECT payload FROM outbox_j")[0][0] == {"n": 1}


def test_concurrent_writers_do_not_block_each_other(make: Any, db: Db) -> None:
    first = make()
    first.begin()
    first.enqueue(**KW)  # left open: uncommitted while the second writer runs
    done: list[uuid.UUID] = []
    errors: list[BaseException] = []

    def second_writer() -> None:
        try:
            h = (
                make()
            )  # constructed in-thread: sync drivers and Django connections are thread-local
            h.begin()
            done.append(h.enqueue(**KW))
            h.commit()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=second_writer)
    t.start()
    t.join(timeout=10)
    assert not t.is_alive(), "second writer blocked behind the first writer's open transaction"
    assert not errors and len(done) == 1
    first.commit()
    assert len(db.rows()) == 2


# --- DDL ---------------------------------------------------------------------------------------


def test_ddl_status_check_and_partial_index_are_real(db: Db) -> None:
    import psycopg

    with pytest.raises(psycopg.errors.CheckViolation):
        db.conn.execute(
            "INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload, topic, status) "
            "VALUES (gen_random_uuid(), 'a', '', 't', ''::bytea, 'x', 'bogus')"
        )
    (index_def,) = db.q("SELECT indexdef FROM pg_indexes WHERE indexname = 'outbox_pending_idx'")[0]
    assert "(seq)" in index_def and "status = 'pending'" in index_def


def test_ddl_seq_is_a_generated_always_identity(db: Db) -> None:
    (identity,) = db.q(
        "SELECT attidentity FROM pg_attribute WHERE attrelid = 'outbox'::regclass AND attname = 'seq'"
    )[0]
    assert identity == "a"


def test_payload_json_variant_applies_and_is_queryable(db: Db) -> None:
    db.conn.execute("DROP TABLE IF EXISTS outbox_q CASCADE")
    db.conn.execute("DROP FUNCTION IF EXISTS outbox_q_payload_json(bytea)")
    try:
        db.conn.execute(outbox_ddl("outbox_q", payload_json=True))
        db.conn.execute(
            "INSERT INTO outbox_q (id, aggregatetype, aggregateid, type, payload, topic) "
            "VALUES (gen_random_uuid(), 'a', '', 't', convert_to('{\"k\": 7}', 'UTF8'), 'x')"
        )
        assert db.q("SELECT payload_json->>'k' FROM outbox_q")[0][0] == "7"
    finally:
        db.conn.execute("DROP TABLE IF EXISTS outbox_q CASCADE")
        db.conn.execute("DROP FUNCTION IF EXISTS outbox_q_payload_json(bytea)")


def test_schema_qualified_ddl_applies(db: Db) -> None:
    db.conn.execute("CREATE SCHEMA IF NOT EXISTS outbox_test_schema")
    try:
        db.conn.execute(outbox_ddl("outbox_test_schema.events"))
    finally:
        db.conn.execute("DROP SCHEMA outbox_test_schema CASCADE")


def test_django_writer_outside_atomic_raises_against_a_real_connection(dsn: str, db: Db) -> None:
    _configure_django(dsn)
    from kafka_reliability.outbox.backends.django import DjangoOutboxWriter

    with pytest.raises(ConfigurationError, match="atomic"):
        DjangoOutboxWriter().enqueue(**KW)
    assert db.rows() == []
