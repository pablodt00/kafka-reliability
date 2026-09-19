"""BaseOutboxWriter: row construction, header validation, event-id minting.
Unit-testable with no database."""

from __future__ import annotations

import json
import uuid

import pytest

from kafka_reliability.core.errors import ConfigurationError, HeaderValidationError
from kafka_reliability.outbox.writer import BaseOutboxWriter, OutboxMessage, validate_headers


def msg(**over):
    base = dict(
        topic="orders",
        payload=b"\x00\x01",
        aggregatetype="order",
        aggregateid="o-1",
        type="Created",
    )
    return OutboxMessage(**{**base, **over})


def test_mints_a_uuid_event_id_when_none_supplied():
    row = BaseOutboxWriter()._build_row(msg())
    assert isinstance(row.id, uuid.UUID)
    assert BaseOutboxWriter()._build_row(msg()).id != row.id


def test_uses_the_callers_event_id():
    eid = uuid.uuid4()
    assert BaseOutboxWriter()._build_row(msg(event_id=eid)).id == eid


def test_row_carries_every_field_and_no_event_id_header():
    row = BaseOutboxWriter()._build_row(msg(headers={"traceparent": b"00-abc"}))
    assert (row.topic, row.aggregatetype, row.aggregateid, row.type) == (
        "orders",
        "order",
        "o-1",
        "Created",
    )
    assert row.payload == b"\x00\x01"
    assert row.headers == {"traceparent": "00-abc"}  # the relay stamps x-event-id, not the writer


def test_empty_aggregateid_means_no_key_and_is_allowed():
    assert BaseOutboxWriter()._build_row(msg(aggregateid="")).aggregateid == ""


def test_non_utf8_header_value_is_rejected_with_a_helpful_error():
    with pytest.raises(HeaderValidationError, match="base64"):
        validate_headers({"sig": b"\xff\xfe"})


@pytest.mark.parametrize(
    "headers",
    [{"k": "not-bytes"}, {"": b"v"}, {"k": b"a\x00b"}],
)
def test_other_unstorable_headers_are_rejected(headers):
    with pytest.raises(HeaderValidationError):
        validate_headers(headers)  # type: ignore[arg-type]


def test_header_error_is_also_a_valueerror():
    assert issubclass(HeaderValidationError, ValueError)


def test_empty_topic_and_nul_text_are_rejected():
    with pytest.raises(ValueError):
        BaseOutboxWriter()._build_row(msg(topic=""))
    with pytest.raises(ValueError):
        BaseOutboxWriter()._build_row(msg(aggregateid="a\x00b"))


def test_batch_validates_everything_before_returning_any_row():
    good, bad = msg(), msg(headers={"k": b"\xff"})
    with pytest.raises(HeaderValidationError):
        BaseOutboxWriter()._build_rows((good, bad))


def test_jsonb_variant_decodes_the_payload_to_text():
    w = BaseOutboxWriter(payload="jsonb")
    assert w._build_row(msg(payload=b'{"a": 1}')).payload == '{"a": 1}'
    with pytest.raises(ValueError):
        w._build_row(msg(payload=b"\xff"))


def test_insert_sql_by_param_style():
    w = BaseOutboxWriter(table="app.outbox")
    assert w._insert_sql("numeric") == (
        "INSERT INTO app.outbox (id, aggregatetype, aggregateid, type, payload, topic, headers) "
        "VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb)"
    )
    assert "%s::uuid, %s, %s, %s, %s, %s, %s::jsonb" in w._insert_sql("format")
    assert "$5::jsonb" in BaseOutboxWriter(payload="jsonb")._insert_sql("numeric")


def test_params_encode_headers_as_json_text():
    w = BaseOutboxWriter()
    row = w._build_row(msg(headers={"a": b"1"}))
    params = w._params(row, id_as_str=True)
    assert params[0] == str(row.id) and json.loads(params[-1]) == {"a": "1"}


def test_bad_table_or_payload_fails_at_construction():
    with pytest.raises(ConfigurationError):
        BaseOutboxWriter(table="x; drop")
    with pytest.raises(ConfigurationError):
        BaseOutboxWriter(payload="text")  # type: ignore[arg-type]
