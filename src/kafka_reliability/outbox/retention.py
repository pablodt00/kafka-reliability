"""Chunked sweep of already-published outbox rows, to keep the table small.

The library is not a scheduler: call `sweep_published` from your own cron,
Celery beat or task loop. Published rows are dead weight to the relay, but keep
them longer than your longest replay window — see 02-outbox.md.

For high volume, partitioning the table by time and dropping old partitions is
the recommended upgrade path; it is documented rather than automated, because
managing partitions would mean owning your schema.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from kafka_reliability.core.errors import ConfigurationError
from kafka_reliability.outbox.schema import validate_table_name


def sweep_sql(table: str) -> str:
    """One chunk: `$1` is the age cutoff, `$2` the chunk size."""
    validate_table_name(table)
    return (
        f"WITH doomed AS (SELECT id FROM {table}\n"
        f"                 WHERE status = 'published' AND published_at < now() - $1::interval\n"
        f"                 LIMIT $2)\n"
        f"DELETE FROM {table} t USING doomed WHERE t.id = doomed.id"
    )


async def sweep_published(
    pool: Any,
    *,
    older_than: timedelta = timedelta(days=7),
    chunk: int = 10_000,
    table: str = "outbox",
) -> int:
    """Delete `published` rows older than `older_than`, `chunk` rows per statement.

    Each chunk is its own transaction, so no single statement holds a long lock
    or writes a huge burst of WAL. Returns the number of rows deleted, for the
    caller to log. `pool` is an `asyncpg.Pool` or `asyncpg.Connection` (anything
    with an awaitable `execute`); only `published` rows are ever touched —
    `pending` and `failed` rows are never deleted.

    The sweep finds old rows with a scan of the table, not an index (an index on
    published rows would tax the relay's hot path); that is fine for a periodic
    job and is the reason to partition by time at very high volume.
    """
    if chunk < 1:
        raise ConfigurationError("chunk must be at least 1")
    if older_than < timedelta(0):
        raise ConfigurationError("older_than must not be negative")
    sql = sweep_sql(table)
    total = 0
    while True:
        status: str = await pool.execute(sql, older_than, chunk)
        deleted = int(status.rsplit(" ", 1)[-1])  # asyncpg returns "DELETE <n>"
        total += deleted
        if deleted < chunk:
            return total
