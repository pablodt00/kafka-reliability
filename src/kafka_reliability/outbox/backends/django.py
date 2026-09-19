"""DjangoOutboxWriter — uses the current atomic() block on a named database
alias. Requires the [outbox-django] extra.

No Django app, no models, no migrations that run themselves: `django_migration()`
in `outbox.schema` is the integration point.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence

from kafka_reliability.core.errors import ConfigurationError, require_extra
from kafka_reliability.outbox.writer import BaseOutboxWriter, OutboxMessage

try:
    from django.db import connections
except ImportError as exc:
    require_extra(package="django", extra="outbox-django", cause=exc)


class DjangoOutboxWriter(BaseOutboxWriter):
    """Insert outbox rows through Django's connection for a database alias.

    Must be called inside the caller's `transaction.atomic()` block; outside
    one, Django autocommits the insert on its own and the dual write is back,
    so it raises `ConfigurationError` instead.

    Do **not** use `transaction.on_commit()` to publish instead: the outbox row
    exists precisely to make the publish survive the commit, and `on_commit`
    loses the event if the process dies between the commit and the callback.
    """

    def enqueue(
        self,
        *,
        using: str = "default",
        topic: str,
        payload: bytes,
        aggregatetype: str,
        aggregateid: str,
        type: str,
        headers: Mapping[str, bytes] | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        connection = _atomic_connection(using)
        row = self._build_row(
            OutboxMessage(topic, payload, aggregatetype, aggregateid, type, headers or {}, event_id)
        )
        with connection.cursor() as cursor:
            cursor.execute(self._insert_sql("format"), self._params(row, id_as_str=True))
        return row.id

    def enqueue_many(
        self, messages: Sequence[OutboxMessage], *, using: str = "default"
    ) -> list[uuid.UUID]:
        connection = _atomic_connection(using)
        rows = self._build_rows(tuple(messages))
        if rows:
            with connection.cursor() as cursor:
                cursor.executemany(
                    self._insert_sql("format"), [self._params(r, id_as_str=True) for r in rows]
                )
        return [r.id for r in rows]


def _atomic_connection(using: str):  # type: ignore[no-untyped-def]
    connection = connections[using]
    if not connection.in_atomic_block:
        raise ConfigurationError(
            f"DjangoOutboxWriter.enqueue() called outside transaction.atomic() on database "
            f"{using!r}: the insert would autocommit on its own, silently reintroducing the "
            "dual write. Wrap the business write and the enqueue in one atomic() block."
        )
    return connection
