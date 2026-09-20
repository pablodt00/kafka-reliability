"""Key-derivation helpers. The dedup key defines what "the same message" means,
and getting it wrong is the main failure mode of the pattern, in both
directions: too broad silently drops distinct work, too narrow deduplicates
nothing. There is deliberately **no default key function** — the friction is
one line, chosen on purpose. Each helper's docstring says what it cannot catch.

A key function is `Callable[[Record], str]`. No third-party imports permitted
in this module."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, DedupKeyError
from kafka_reliability.core.message import Record, get_header

KeyFunction = Callable[[Record], str]

_STEP = re.compile(r"\.([A-Za-z_][A-Za-z0-9_-]*)|\[(\d+)\]")


def from_header(name: str) -> KeyFunction:
    """Key on a header — the preferred choice. Pair it with the outbox's
    `EVENT_ID` header (`core.headers.EVENT_ID`).

    The only key that survives republishing to another topic, a partition-count
    change, or a replay. Cannot catch: two different events a producer stamped
    with the same ID, or messages that never carried the header (those raise
    `DedupKeyError` rather than being processed unchecked).
    """
    if not name:
        raise ConfigurationError("from_header needs a header name")

    def key(record: Record) -> str:
        value = get_header(record.headers, name)
        if value is None:
            raise DedupKeyError(
                f"record {record.topic}[{record.partition}]@{record.offset} has no {name!r} header"
            )
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DedupKeyError(f"header {name!r} is not valid UTF-8") from exc

    return key


def from_json_path(path: str) -> KeyFunction:
    """Key on a field of a JSON payload, e.g. `"$.event_id"` or `"$.a.items[0].id"`.

    Supports dotted names and `[n]` indexes only. Cannot catch: a payload that
    is not JSON or lacks the field (raises `DedupKeyError`), or a producer that
    reuses the field for distinct events. Deserialises the payload for every
    message — prefer `from_header` when you can.
    """
    if not path.startswith("$"):
        raise ConfigurationError(f"JSON path must start with '$', got {path!r}")
    steps: list[str | int] = []
    rest = path[1:]
    while rest:
        m = _STEP.match(rest)
        if m is None:
            raise ConfigurationError(f"unsupported JSON path {path!r}: use $.a.b[0].c")
        steps.append(m.group(1) if m.group(1) is not None else int(m.group(2)))
        rest = rest[m.end() :]
    if not steps:
        raise ConfigurationError("JSON path must select a field, not the whole document")

    def key(record: Record) -> str:
        try:
            node: Any = json.loads(record.value)
            for step in steps:
                node = node[step]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise DedupKeyError(f"cannot read {path!r} from the record's payload") from exc
        if isinstance(node, bool) or not isinstance(node, str | int):
            raise DedupKeyError(f"{path!r} must be a string or integer, got {type(node).__name__}")
        return str(node)

    return key


def payload_hash(algorithm: str = "sha256") -> KeyFunction:
    """Key on a hash of the payload bytes. Needs no producer cooperation.

    Cannot catch — and actively harms: two genuinely distinct events with
    identical payloads (a repeated click, two identical sensor readings)
    collapse into one, which is silent, unfalsifiable data loss. Use only when
    the payload provably contains something unique.
    """
    try:
        hashlib.new(algorithm)
    except ValueError as exc:
        raise ConfigurationError(f"unknown hash algorithm {algorithm!r}") from exc

    def key(record: Record) -> str:
        return hashlib.new(algorithm, record.value).hexdigest()

    return key


def topic_partition_offset() -> KeyFunction:
    """Key on the record's physical coordinates.

    Suppresses consumer-side redelivery only (rebalances, failed offset
    commits). Cannot catch producer-side duplicates: an event republished by a
    relay retry lands at a new offset, and a replayed message has a new offset
    by definition. Also meaningless across a partition-count change.
    """

    def key(record: Record) -> str:
        return f"{record.topic}:{record.partition}:{record.offset}"

    return key


__all__ = ["KeyFunction", "from_header", "from_json_path", "payload_hash", "topic_partition_offset"]
