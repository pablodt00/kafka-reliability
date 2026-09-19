"""Producer adapter backed by aiokafka. Requires the [aiokafka] extra.

Two ways in:

* `AIOKafkaProducerAdapter(client)` wraps an `AIOKafkaProducer` the caller
  constructed and started. The library does not own client configuration, so
  the caller is responsible for `acks="all"` and idempotence on that client,
  and for `start()`/`stop()`.
* `create_producer(...)` is a convenience factory that builds the client with
  `acks="all"` and `enable_idempotence=True`. These are defaults, not options:
  overriding either to something weaker raises `ConfigurationError`. Without
  `acks=all` the relay can mark a row sent that a leader election then loses,
  converting the safe failure (a duplicate) into the unsafe one (loss)
  (docs/claude/02-outbox.md). The adapter it returns owns the client, so
  `start()`/`stop()` (or `async with`) act on it.

Producer idempotence deduplicates the *client's own retries* within a producer
session only. It does not deduplicate a fresh publish of the same outbox row
after a relay restart, which is a new produce call — delivery stays
at-least-once, and consumers still need to deduplicate.

aiokafka's errors are mapped to `ProducerError` (original chained as
`__cause__`), so callers never import `aiokafka` to catch them."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, ProducerError, require_extra
from kafka_reliability.core.message import OutgoingMessage

try:
    from aiokafka import AIOKafkaProducer
    from aiokafka.errors import KafkaError
except ImportError as exc:
    require_extra(package="aiokafka", extra="aiokafka", cause=exc)

_SAFE_ACKS = ("all", -1, "-1")


class AIOKafkaProducerAdapter:
    """Adapts an `AIOKafkaProducer` to the `Producer` protocol."""

    def __init__(self, client: Any, *, owns_client: bool = False) -> None:
        self._client = client
        self._owns_client = owns_client

    async def send(self, message: OutgoingMessage) -> None:
        headers = [(name, value) for name, value in message.headers.items()] or None
        try:
            await self._client.send_and_wait(
                message.topic, value=message.value, key=message.key, headers=headers
            )
        except KafkaError as exc:
            raise ProducerError(f"aiokafka failed to deliver to {message.topic!r}: {exc}") from exc

    async def flush(self, timeout: float | None = None) -> None:
        try:
            async with asyncio.timeout(timeout):
                await self._client.flush()
        except TimeoutError as exc:
            raise ProducerError(f"aiokafka flush did not complete within {timeout}s") from exc
        except KafkaError as exc:
            raise ProducerError(f"aiokafka flush failed: {exc}") from exc

    async def start(self) -> None:
        """Start the underlying client. A no-op unless the adapter owns it."""
        if self._owns_client:
            await self._client.start()

    async def stop(self) -> None:
        """Stop the underlying client. A no-op unless the adapter owns it."""
        if self._owns_client:
            await self._client.stop()

    async def __aenter__(self) -> AIOKafkaProducerAdapter:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()


def create_producer(
    bootstrap_servers: str | list[str], **overrides: Any
) -> AIOKafkaProducerAdapter:
    """Build an adapter that owns a new `AIOKafkaProducer`.

    Extra keyword arguments are passed to `AIOKafkaProducer`. `acks` and
    `enable_idempotence` may be restated but not weakened. The client is not
    started; call `await adapter.start()` or use `async with`.
    """
    if overrides.get("acks", "all") not in _SAFE_ACKS:
        raise ConfigurationError(
            f"acks={overrides['acks']!r} is not allowed: the outbox relay requires acks='all'."
        )
    if not overrides.get("enable_idempotence", True):
        raise ConfigurationError("enable_idempotence=False is not allowed: it is a default.")
    overrides["acks"] = "all"
    overrides["enable_idempotence"] = True
    client = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, **overrides)
    return AIOKafkaProducerAdapter(client, owns_client=True)
