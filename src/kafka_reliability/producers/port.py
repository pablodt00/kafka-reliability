"""The Producer protocol: send and flush. No partitioner control, no
serializers, no config passthrough — that belongs to the client the caller
configured.

Any object with these two coroutines satisfies the protocol; it does not need
to import or subclass anything from this library (a FastStream publisher
wrapped in a thin class is enough). No third-party imports permitted here."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from kafka_reliability.core.message import OutgoingMessage


@runtime_checkable
class Producer(Protocol):
    """Where the outbox relay and the replay runner publish messages."""

    async def send(self, message: OutgoingMessage) -> None:
        """Publish `message` and return once the broker has acknowledged it.

        The relay marks an outbox row sent as soon as this returns, so an
        implementation must not return before the broker's ack. Raises
        `ProducerError` if the message could not be delivered.
        """
        ...

    async def flush(self, timeout: float | None = None) -> None:
        """Wait for any message still in flight to be acknowledged.

        `timeout` is in seconds; `None` waits indefinitely. Raises
        `ProducerError` if messages remain undelivered when it expires.
        """
        ...
