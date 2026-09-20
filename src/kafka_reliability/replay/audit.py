"""JSONL audit sink: one line per replayed record, so "did that replay include
order 8812" is answered by `grep`, not a log search. Append-only; each line is
flushed as it is written, so a crashed replay still leaves an accurate trail.

The key is recorded (UTF-8 text, or `hex:...` when it is not text) — that is what
makes the audit answerable. It is not the payload: payloads never enter the audit
file, which is one reason replay has no transform hook (06-decisions.md D7).
No third-party imports permitted in this module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import IO, Any, Protocol


class AuditSink(Protocol):
    def write(self, entry: dict[str, Any]) -> None: ...
    def close(self) -> None: ...


def encode_key(key: bytes | None) -> str | None:
    if key is None:
        return None
    try:
        return key.decode("utf-8")
    except UnicodeDecodeError:
        return "hex:" + key.hex()


class JsonlAuditSink:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._file: IO[str] | None = None

    def write(self, entry: dict[str, Any]) -> None:
        if self._file is None:
            self._file = self.path.open("a", encoding="utf-8")
        self._file.write(json.dumps(entry, sort_keys=True) + "\n")
        self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class MemoryAuditSink:
    """Collects entries in a list, for tests."""

    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    def write(self, entry: dict[str, Any]) -> None:
        self.entries.append(entry)

    def close(self) -> None:
        pass
