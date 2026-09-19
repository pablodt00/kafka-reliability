"""DDL text and table factories for the outbox: raw SQL, a SQLAlchemy Table,
and a Django migration body.

The library emits DDL; it never runs migrations. Paste `outbox_ddl()` into your
own migration (or use `make_outbox_table` / `django_migration`). Column names
follow the Debezium Outbox Event Router (06-decisions.md D1). This module
imports only the standard library at import time; SQLAlchemy and Django are
imported lazily by the factories that need them.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Final, Literal

from kafka_reliability.core.errors import ConfigurationError, require_extra

if TYPE_CHECKING:
    from sqlalchemy import MetaData, Table

PayloadType = Literal["bytea", "jsonb"]

STATUS_PENDING: Final = "pending"
STATUS_PUBLISHED: Final = "published"
STATUS_FAILED: Final = "failed"

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_TABLE_RE = re.compile(rf"^{_IDENT}(\.{_IDENT})?$")


def validate_table_name(table: str) -> str:
    """Return `table` if it is a plain, optionally schema-qualified identifier.

    The name is interpolated into SQL, so anything else — quotes, spaces,
    semicolons — is rejected rather than escaped.
    """
    if not _TABLE_RE.match(table):
        raise ConfigurationError(
            f"invalid outbox table name {table!r}: use a plain identifier, "
            "optionally schema-qualified ('outbox' or 'app.outbox')"
        )
    return table


def _check_payload(payload: str) -> None:
    if payload not in ("bytea", "jsonb"):
        raise ConfigurationError(f"payload must be 'bytea' or 'jsonb', got {payload!r}")


def outbox_ddl(
    table: str = "outbox",
    payload: PayloadType = "bytea",
    *,
    payload_json: bool = False,
) -> str:
    """Return the `CREATE TABLE` / `CREATE INDEX` statements for the outbox.

    `payload="bytea"` (default) keeps Avro/Protobuf payloads byte-for-byte;
    `payload="jsonb"` is Debezium-native for JSON shops. Choosing `bytea` and
    later moving to Debezium needs a column conversion or `BinaryHandlingMode`
    (D1).

    `payload_json=True` (bytea only) adds a generated `payload_json JSONB`
    column so payloads are SQL-queryable, at the cost of storing them twice
    and of rejecting any insert whose payload is not UTF-8 JSON.

    Applies to PostgreSQL 12 and up (`GENERATED ... AS IDENTITY` needs 10,
    stored generated columns need 12).
    """
    validate_table_name(table)
    _check_payload(payload)
    if payload_json and payload != "bytea":
        raise ConfigurationError("payload_json only applies to payload='bytea'")

    base = table.rsplit(".", 1)[-1]
    payload_type = payload.upper()
    statements: list[str] = []

    generated = ""
    if payload_json:
        # convert_from() is only STABLE, which a generated column rejects; a
        # UTF-8 decode is deterministic, so an IMMUTABLE wrapper is safe.
        statements.append(
            f"CREATE FUNCTION {table}_payload_json(bytea) RETURNS jsonb\n"
            f"    LANGUAGE sql IMMUTABLE STRICT\n"
            f"    AS $$ SELECT convert_from($1, 'UTF8')::jsonb $$;"
        )
        generated = (
            f",\n    payload_json JSONB GENERATED ALWAYS AS ({table}_payload_json(payload)) STORED"
        )

    statements.append(
        f"CREATE TABLE {table} (\n"
        f"    id            UUID        PRIMARY KEY,\n"
        f"    aggregatetype TEXT        NOT NULL,\n"
        f"    aggregateid   TEXT        NOT NULL,\n"
        f"    type          TEXT        NOT NULL,\n"
        f"    payload       {payload_type:<5}       NOT NULL,\n"
        f"    seq           BIGINT      GENERATED ALWAYS AS IDENTITY,\n"
        f"    topic         TEXT        NOT NULL,\n"
        f"    headers       JSONB       NOT NULL DEFAULT '{{}}',\n"
        f"    status        TEXT        NOT NULL DEFAULT '{STATUS_PENDING}'\n"
        f"                  CHECK (status IN ('{STATUS_PENDING}','{STATUS_PUBLISHED}','{STATUS_FAILED}')),\n"
        f"    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),\n"
        f"    published_at  TIMESTAMPTZ,\n"
        f"    attempts      INT         NOT NULL DEFAULT 0,\n"
        f"    last_error    TEXT"
        f"{generated}\n"
        f");"
    )
    statements.append(f"CREATE INDEX {base}_pending_idx ON {table} (seq) WHERE status = 'pending';")
    return "\n".join(statements) + "\n"


def _drop_ddl(table: str, *, payload_json: bool) -> str:
    sql = f"DROP TABLE {table};"
    if payload_json:
        sql += f"\nDROP FUNCTION {table}_payload_json(bytea);"
    return sql


def make_outbox_table(
    metadata: MetaData,
    table: str = "outbox",
    payload: PayloadType = "bytea",
) -> Table:
    """Return a SQLAlchemy `Table` matching `outbox_ddl()`, registered on `metadata`.

    Core and ORM users share this one table: `SqlAlchemyOutboxWriter` inserts
    through it, and Alembic can autogenerate the migration from it. Requires
    the [outbox-sqlalchemy] extra.
    """
    validate_table_name(table)
    _check_payload(payload)
    try:
        import sqlalchemy as sa
        from sqlalchemy.dialects import postgresql as pg
    except ImportError as exc:
        require_extra(package="sqlalchemy", extra="outbox-sqlalchemy", cause=exc)

    schema, _, name = table.rpartition(".")
    payload_col: Any = pg.JSONB() if payload == "jsonb" else sa.LargeBinary()
    return sa.Table(
        name,
        metadata,
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("aggregatetype", sa.Text, nullable=False),
        sa.Column("aggregateid", sa.Text, nullable=False),
        sa.Column("type", sa.Text, nullable=False),
        sa.Column("payload", payload_col, nullable=False),
        sa.Column("seq", sa.BigInteger, sa.Identity(always=True)),
        sa.Column("topic", sa.Text, nullable=False),
        sa.Column("headers", pg.JSONB, nullable=False, server_default=sa.text("'{}'")),
        sa.Column("status", sa.Text, nullable=False, server_default=sa.text(f"'{STATUS_PENDING}'")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text),
        sa.CheckConstraint(
            f"status IN ('{STATUS_PENDING}','{STATUS_PUBLISHED}','{STATUS_FAILED}')",
            name=f"{name}_status_check",
        ),
        sa.Index(
            f"{name}_pending_idx",
            "seq",
            postgresql_where=sa.text("status = 'pending'"),
        ),
        schema=schema or None,
    )


def django_migration(
    table: str = "outbox",
    payload: PayloadType = "bytea",
    *,
    payload_json: bool = False,
) -> Any:
    """Return a `django.db.migrations.RunSQL` that creates the outbox table.

    Put it in your own app's migration `operations` list; the library ships no
    Django app and no migration that runs itself. The reverse drops the table.
    Requires the [outbox-django] extra.
    """
    sql = outbox_ddl(table, payload, payload_json=payload_json)
    try:
        from django.db.migrations import RunSQL
    except ImportError as exc:
        require_extra(package="django", extra="outbox-django", cause=exc)
    return RunSQL(sql=sql, reverse_sql=_drop_ddl(table, payload_json=payload_json))
