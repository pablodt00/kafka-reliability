"""AsyncpgOutboxWriter — enqueue via an asyncpg.Connection. Requires the
[outbox-asyncpg] extra."""

from kafka_reliability.core.errors import require_extra

try:
    import asyncpg  # noqa: F401
except ImportError as exc:
    require_extra(package="asyncpg", extra="outbox-asyncpg", cause=exc)
