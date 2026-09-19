"""One typed writer class per backend: enqueue a message into the caller's own
transaction. No Kafka client is imported anywhere in this module.

`BaseOutboxWriter` is the backend-agnostic half — row construction, header
validation, event-id minting. Each backend subclass only supplies the SQL
execution, on a connection or session the *caller* already holds. The library
never opens a connection, never commits, and never touches Kafka on this path
(02-outbox.md, "What the API must guarantee").

The outbox converts a correctness problem into a duplicates problem: a row
enqueued here is published at least once, so consumers must deduplicate.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, NamedTuple

from kafka_reliability.core.errors import HeaderValidationError
from kafka_reliability.outbox.schema import PayloadType, _check_payload, validate_table_name

ParamStyle = Literal["numeric", "format"]

_COLUMNS = ("id", "aggregatetype", "aggregateid", "type", "payload", "topic", "headers")


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """One event to enqueue.

    `aggregateid` becomes the Kafka key; an empty string means "no key" —
    round-robin partitioning, no ordering requirement. Header values are
    `bytes` that must be UTF-8 decodable; base64 binary values yourself.
    """

    topic: str
    payload: bytes
    aggregatetype: str
    aggregateid: str
    type: str
    headers: Mapping[str, bytes] = field(default_factory=dict)
    event_id: uuid.UUID | None = None


class OutboxRow(NamedTuple):
    """A validated row, ready for a backend to insert."""

    id: uuid.UUID
    aggregatetype: str
    aggregateid: str
    type: str
    payload: bytes | str  # str for the jsonb variant
    topic: str
    headers: dict[str, str]


def validate_headers(headers: Mapping[str, bytes]) -> dict[str, str]:
    """Decode header values to text, raising `HeaderValidationError` on any that cannot be stored.

    Runs inside the caller's transaction so a bad header fails at the call
    site that produced it (06-decisions.md D3).
    """
    out: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not name:
            raise HeaderValidationError(f"header names must be non-empty str, got {name!r}")
        if not isinstance(value, bytes | bytearray | memoryview):
            raise HeaderValidationError(
                f"header {name!r}: value must be bytes, got {type(value).__name__}"
            )
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HeaderValidationError(
                f"header {name!r}: value is not valid UTF-8; base64-encode binary values"
            ) from exc
        if "\x00" in text:
            raise HeaderValidationError(f"header {name!r}: value contains a NUL character")
        out[name] = text
    return out


class BaseOutboxWriter:
    """Shared row construction, header validation and event-id minting.

    `payload` must match the DDL the table was created with (`outbox_ddl`).
    """

    def __init__(self, *, table: str = "outbox", payload: PayloadType = "bytea") -> None:
        self.table = validate_table_name(table)
        _check_payload(payload)
        self.payload_type: PayloadType = payload

    def _build_row(self, message: OutboxMessage) -> OutboxRow:
        if not message.topic:
            raise ValueError("topic must be non-empty")
        for label, text in (
            ("topic", message.topic),
            ("aggregatetype", message.aggregatetype),
            ("aggregateid", message.aggregateid),
            ("type", message.type),
        ):
            if "\x00" in text:
                raise ValueError(f"{label} contains a NUL character")
        headers = validate_headers(message.headers)
        payload: bytes | str = message.payload
        if self.payload_type == "jsonb":
            try:
                payload = bytes(message.payload).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("a jsonb outbox payload must be UTF-8 JSON") from exc
        return OutboxRow(
            id=message.event_id or uuid.uuid4(),
            aggregatetype=message.aggregatetype,
            aggregateid=message.aggregateid,
            type=message.type,
            payload=payload,
            topic=message.topic,
            headers=headers,
        )

    def _build_rows(
        self, messages: list[OutboxMessage] | tuple[OutboxMessage, ...]
    ) -> list[OutboxRow]:
        # Validate every message before any is written, so a bad one fails the
        # whole call instead of leaving a partial batch in the transaction.
        return [self._build_row(m) for m in messages]

    def _insert_sql(self, style: ParamStyle) -> str:
        """`INSERT` with casts so every driver can bind text for uuid/jsonb."""
        casts = {"id": "uuid", "headers": "jsonb"}
        if self.payload_type == "jsonb":
            casts["payload"] = "jsonb"
        values = ", ".join(
            (f"${i}" if style == "numeric" else "%s") + (f"::{casts[col]}" if col in casts else "")
            for i, col in enumerate(_COLUMNS, start=1)
        )
        return f"INSERT INTO {self.table} ({', '.join(_COLUMNS)}) VALUES ({values})"

    @staticmethod
    def _params(row: OutboxRow, *, id_as_str: bool) -> tuple[object, ...]:
        return (
            str(row.id) if id_as_str else row.id,
            row.aggregatetype,
            row.aggregateid,
            row.type,
            row.payload,
            row.topic,
            json.dumps(row.headers),
        )
