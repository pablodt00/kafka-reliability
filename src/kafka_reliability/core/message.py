"""Record and OutgoingMessage — plain, frozen dataclasses for messages in and
out of Kafka. No third-party imports permitted in this module."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class Record:
    """A message as it exists on a topic."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: tuple[tuple[str, bytes], ...]
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    """A message to be produced."""

    topic: str
    value: bytes
    key: bytes | None = None
    headers: Mapping[str, bytes] = field(default_factory=dict)


def get_header(headers: tuple[tuple[str, bytes], ...], name: str) -> bytes | None:
    """Return the value of the first header named `name`, or None.

    Kafka headers may repeat a name; the first match wins, matching how the
    Kafka clients themselves expose header order.
    """
    for header_name, value in headers:
        if header_name == name:
            return value
    return None
