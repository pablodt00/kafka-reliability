from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kafka_reliability.core.errors import ConfigurationError, DedupKeyError
from kafka_reliability.core.headers import EVENT_ID
from kafka_reliability.core.message import Record
from kafka_reliability.dedup import keys


def rec(value: bytes = b"{}", headers: tuple[tuple[str, bytes], ...] = ()) -> Record:
    return Record("orders", 3, 42, b"k", value, headers, datetime(2026, 1, 1, tzinfo=UTC))


def test_from_header_reads_the_event_id_and_raises_when_absent():
    assert keys.from_header(EVENT_ID)(rec(headers=((EVENT_ID, b"abc"),))) == "abc"
    with pytest.raises(DedupKeyError):
        keys.from_header(EVENT_ID)(rec())
    with pytest.raises(DedupKeyError):
        keys.from_header("h")(rec(headers=(("h", b"\xff"),)))


def test_from_json_path():
    r = rec(b'{"event_id": "e1", "a": {"items": [{"id": 7}]}, "flag": true}')
    assert keys.from_json_path("$.event_id")(r) == "e1"
    assert keys.from_json_path("$.a.items[0].id")(r) == "7"
    for bad in ("$.missing", "$.flag", "$.a", "$.a.items[3].id"):
        with pytest.raises(DedupKeyError):
            keys.from_json_path(bad)(r)
    with pytest.raises(DedupKeyError):
        keys.from_json_path("$.x")(rec(b"not json"))
    for bad_path in ("event_id", "$", "$..x"):
        with pytest.raises(ConfigurationError):
            keys.from_json_path(bad_path)


def test_payload_hash_collapses_identical_payloads():
    f = keys.payload_hash()
    assert f(rec(b"same")) == f(rec(b"same")) != f(rec(b"other"))
    assert len(f(rec(b"x"))) == 64
    with pytest.raises(ConfigurationError):
        keys.payload_hash("nope")


def test_topic_partition_offset():
    assert keys.topic_partition_offset()(rec()) == "orders:3:42"


def test_every_helper_docstring_names_what_it_cannot_catch():
    for helper in (
        keys.from_header,
        keys.from_json_path,
        keys.payload_hash,
        keys.topic_partition_offset,
    ):
        assert "annot catch" in (helper.__doc__ or ""), helper.__name__


def test_there_is_no_default_key_function():
    assert not hasattr(keys, "default_key")
