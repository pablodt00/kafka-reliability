"""Producer adapter backed by confluent-kafka. Requires the [confluent] extra.

`confluent_kafka.Producer` is synchronous and callback-based over librdkafka:
`produce()` enqueues and returns immediately, and delivery reports are only
delivered when something calls `poll()` or `flush()`. This adapter bridges that
to the protocol's `async send` **by driving `poll()` from a dedicated
background thread**, not by using the 2.x `AIOProducer`:

* `send` creates a future on the running loop, enqueues with `produce()` and
  awaits the delivery callback, which resolves the future through
  `loop.call_soon_threadsafe`. The event loop never blocks on librdkafka.
* One daemon thread calls `poll()` for as long as the adapter is open. Sends
  from many tasks pipeline freely, so throughput is bounded by librdkafka, not
  by one round trip per message.
* It works on every confluent-kafka release with a `Producer`, rather than
  pinning a minimum version for a newer async API.
* Shutdown ordering matters: `close()` flushes first and only then stops the
  poller. Stopping the poller before flushing would strand delivery reports.

Like the aiokafka adapter, `create_producer(...)` builds a client with
`acks=all` and `enable.idempotence=true` as defaults, not options — weakening
either raises `ConfigurationError` (docs/claude/02-outbox.md). Producer
idempotence deduplicates the *client's own retries* only; it does not
deduplicate a fresh publish after a relay restart, so delivery stays
at-least-once.

confluent-kafka errors are mapped to `ProducerError` (original chained as
`__cause__`)."""

from __future__ import annotations

import asyncio
import threading
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, ProducerError, require_extra
from kafka_reliability.core.message import OutgoingMessage

try:
    from confluent_kafka import KafkaException
    from confluent_kafka import Producer as _ConfluentProducer
except ImportError as exc:
    require_extra(package="confluent-kafka", extra="confluent", cause=exc)

_SAFE_ACKS = ("all", "-1", -1)
_TRUE = (True, "true", "True")


class ConfluentProducerAdapter:
    """Adapts a `confluent_kafka.Producer` to the `Producer` protocol."""

    def __init__(
        self,
        client: Any,
        *,
        poll_interval: float = 0.1,
        queue_full_retries: int = 50,
    ) -> None:
        self._client = client
        self._poll_interval = poll_interval
        self._queue_full_retries = queue_full_retries
        self._stop = threading.Event()
        self._poller: threading.Thread | None = None
        self._lock = threading.Lock()

    def _ensure_poller(self) -> None:
        with self._lock:
            if self._poller is None or not self._poller.is_alive():
                self._stop.clear()
                self._poller = threading.Thread(
                    target=self._poll_forever, name="kafka-reliability-poll", daemon=True
                )
                self._poller.start()

    def _poll_forever(self) -> None:
        while not self._stop.is_set():
            self._client.poll(self._poll_interval)

    async def send(self, message: OutgoingMessage) -> None:
        loop = asyncio.get_running_loop()
        delivered: asyncio.Future[None] = loop.create_future()

        def resolve(err: Any) -> None:
            if delivered.done():
                return
            if err is None:
                delivered.set_result(None)
            else:
                delivered.set_exception(_delivery_error(message.topic, err))

        def on_delivery(err: Any, _msg: Any) -> None:
            # Runs on the polling (or flushing) thread, never the event loop's.
            try:
                loop.call_soon_threadsafe(resolve, err)
            except RuntimeError:
                pass  # loop closed: nobody is waiting any more

        self._ensure_poller()
        headers = [(name, value) for name, value in message.headers.items()] or None
        for _ in range(self._queue_full_retries + 1):
            try:
                self._client.produce(
                    message.topic,
                    value=message.value,
                    key=message.key,
                    headers=headers,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                # librdkafka's local queue is full; let the poller drain it.
                await asyncio.sleep(self._poll_interval)
            except KafkaException as exc:
                raise ProducerError(
                    f"confluent-kafka rejected a message for {message.topic!r}: {exc}"
                ) from exc
        else:
            raise ProducerError(
                f"confluent-kafka producer queue stayed full sending to {message.topic!r}"
            )
        await delivered

    async def flush(self, timeout: float | None = None) -> None:
        loop = asyncio.get_running_loop()
        try:
            if timeout is None:
                remaining = await loop.run_in_executor(None, self._client.flush)
            else:
                remaining = await loop.run_in_executor(None, self._client.flush, timeout)
        except KafkaException as exc:
            raise ProducerError(f"confluent-kafka flush failed: {exc}") from exc
        if remaining:
            raise ProducerError(
                f"confluent-kafka flush timed out with {remaining} message(s) undelivered"
            )

    async def close(self, timeout: float | None = None) -> None:
        """Flush, then stop the poller thread. Flush first: see module docstring."""
        try:
            await self.flush(timeout)
        finally:
            self._stop.set()
            poller = self._poller
            if poller is not None:
                await asyncio.get_running_loop().run_in_executor(None, poller.join)


def _delivery_error(topic: str, err: Any) -> ProducerError:
    error = ProducerError(f"confluent-kafka failed to deliver to {topic!r}: {err}")
    error.__cause__ = KafkaException(err)
    return error


def create_producer(bootstrap_servers: str, **overrides: Any) -> ConfluentProducerAdapter:
    """Build an adapter around a new `confluent_kafka.Producer`.

    Extra keyword arguments are librdkafka config keys (use dotted names via
    `**{"linger.ms": 5}`). `acks` and `enable.idempotence` may be restated but
    not weakened. Call `await adapter.close()` when finished.
    """
    if overrides.get("acks", "all") not in _SAFE_ACKS:
        raise ConfigurationError(
            f"acks={overrides['acks']!r} is not allowed: the outbox relay requires acks='all'."
        )
    if overrides.get("enable.idempotence", True) not in _TRUE:
        raise ConfigurationError("enable.idempotence=false is not allowed: it is a default.")
    config = {
        **overrides,
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
    }
    return ConfluentProducerAdapter(_ConfluentProducer(config))
