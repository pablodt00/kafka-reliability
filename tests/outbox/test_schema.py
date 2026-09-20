"""outbox.schema: DDL text and the SQLAlchemy / Django factories (no database)."""

from __future__ import annotations

import pytest

from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.outbox.schema import (
    django_migration,
    make_outbox_table,
    outbox_ddl,
    validate_table_name,
)

DEBEZIUM_FIVE = ("id", "aggregatetype", "aggregateid", "type", "payload")
OURS = (
    "seq",
    "topic",
    "headers",
    "status",
    "created_at",
    "published_at",
    "attempts",
    "last_error",
)


def test_bytea_ddl_has_every_column_and_the_partial_index():
    ddl = outbox_ddl()
    for col in (*DEBEZIUM_FIVE, *OURS):
        assert f"\n    {col} " in ddl
    assert "payload       BYTEA" in ddl
    assert "BIGINT      GENERATED ALWAYS AS IDENTITY" in ddl
    assert "CHECK (status IN ('pending','published','failed'))" in ddl
    assert "CREATE INDEX outbox_pending_idx ON outbox (seq) WHERE status = 'pending';" in ddl


def test_ddl_has_a_partial_index_on_failed_rows_for_the_relay_block_check():
    assert (
        "CREATE INDEX outbox_failed_idx ON outbox (aggregateid) WHERE status = 'failed';"
        in outbox_ddl()
    )


def test_jsonb_variant_changes_only_the_payload_type():
    assert "payload       JSONB" in outbox_ddl(payload="jsonb")


def test_table_name_is_injectable_and_schema_qualified_names_work():
    ddl = outbox_ddl("app.order_outbox")
    assert "CREATE TABLE app.order_outbox (" in ddl
    assert "CREATE INDEX order_outbox_pending_idx ON app.order_outbox" in ddl


def test_payload_json_adds_a_generated_column_for_bytea_only():
    assert "payload_json JSONB GENERATED ALWAYS AS" in outbox_ddl(payload_json=True)
    assert "payload_json" not in outbox_ddl()
    with pytest.raises(ConfigurationError):
        outbox_ddl(payload="jsonb", payload_json=True)


@pytest.mark.parametrize("bad", ["", "out box", "outbox; DROP TABLE x", 'a"b', "a.b.c", "1abc"])
def test_unsafe_table_names_are_rejected(bad):
    with pytest.raises(ConfigurationError):
        validate_table_name(bad)
    with pytest.raises(ConfigurationError):
        outbox_ddl(bad)


def test_unknown_payload_type_is_rejected():
    with pytest.raises(ConfigurationError):
        outbox_ddl(payload="text")  # type: ignore[arg-type]


def test_make_outbox_table_matches_the_ddl_columns():
    sa = pytest.importorskip("sqlalchemy")
    table = make_outbox_table(sa.MetaData(), "outbox")
    assert [c.name for c in table.c] == [*DEBEZIUM_FIVE, "seq", *OURS[1:]]
    assert table.c.seq.identity is not None and table.c.seq.identity.always
    assert type(table.c.payload.type).__name__ == "LargeBinary"
    assert type(make_outbox_table(sa.MetaData(), "o", "jsonb").c.payload.type).__name__ == "JSONB"
    assert {i.name for i in table.indexes} == {"outbox_pending_idx", "outbox_failed_idx"}


def test_make_outbox_table_supports_schema_qualified_names():
    sa = pytest.importorskip("sqlalchemy")
    table = make_outbox_table(sa.MetaData(), "app.outbox")
    assert table.schema == "app" and table.name == "outbox"


def test_django_migration_wraps_the_ddl_in_runsql():
    pytest.importorskip("django")
    op = django_migration("outbox")
    assert op.sql == outbox_ddl("outbox")
    assert op.reverse_sql == "DROP TABLE outbox;"
    assert "DROP FUNCTION" in django_migration("outbox", payload_json=True).reverse_sql
