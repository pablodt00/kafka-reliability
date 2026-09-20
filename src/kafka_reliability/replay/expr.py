"""A constrained header-filter expression for the CLI, instead of arbitrary code
on the command line:

    x-dlq-error-type == 'TimeoutError'
    x-dlq-consumer-group == 'billing' and x-dlq-error-type != 'ValueError'

Only `==` and `!=` against a quoted literal, joined by `and`. A missing header
never equals anything (`==` is false, `!=` is true). Filtering is client-side, so
the tool reads the whole selected range however few records match. No
third-party imports permitted in this module."""

from __future__ import annotations

import re
from collections.abc import Callable

from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.core.message import Record, get_header

_CLAUSE = re.compile(
    r"""^\s*(?P<name>[A-Za-z0-9._-]+)\s*(?P<op>==|!=)\s*(?:'(?P<sq>[^']*)'|"(?P<dq>[^"]*)")\s*$"""
)


def parse_header_filter(expression: str) -> Callable[[Record], bool]:
    clauses: list[tuple[str, str, str]] = []
    for part in re.split(r"\s+and\s+", expression.strip()):
        m = _CLAUSE.match(part)
        if m is None:
            raise ConfigurationError(
                f"cannot parse filter clause {part!r}: use  header == 'value'  or  header != "
                "'value', joined by 'and'"
            )
        value = m.group("sq") if m.group("sq") is not None else m.group("dq")
        clauses.append((m.group("name"), m.group("op"), value))

    def predicate(record: Record) -> bool:
        for name, op, wanted in clauses:
            raw = get_header(record.headers, name)
            actual = None if raw is None else raw.decode("utf-8", "replace")
            if (actual == wanted) != (op == "=="):
                return False
        return True

    return predicate
