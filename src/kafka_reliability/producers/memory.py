"""InMemoryProducer: a public test double implementing the Producer protocol,
for testing user code without a real Kafka client. No third-party imports
permitted in this module."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from kafka_reliability.core.errors import ProducerError
from kafka_reliability.core.message import OutgoingMessage


class InMemoryProducer:
    """Records every message "sent" to it instead of talking to Kafka.

    Failure injection lets relay and replay tests exercise produce failures
    without a broker: a failed `send` raises `ProducerError`, is recorded in
    `failed` and is *not* recorded in `sent`.
    """

    def __init__(self) -> None:
        self._sent: list[OutgoingMessage] = []
        self._failed: list[OutgoingMessage] = []
        self._fail_next = 0
        self._fail_error: Exception | None = None
        self._fail_when: Callable[[OutgoingMessage], bool] | None = None
        self.flush_count = 0

    @property
    def sent(self) -> tuple[OutgoingMessage, ...]:
        """Messages successfully sent, in send order."""
        return tuple(self._sent)

    @property
    def failed(self) -> tuple[OutgoingMessage, ...]:
        """Messages whose send was made to fail, in send order."""
        return tuple(self._failed)

    async def send(self, message: OutgoingMessage) -> None:
        if self._fail_next > 0:
            self._fail_next -= 1
            self._fail(message)
        if self._fail_when is not None and self._fail_when(message):
            self._fail(message)
        self._sent.append(message)

    async def flush(self, timeout: float | None = None) -> None:
        self.flush_count += 1

    def _fail(self, message: OutgoingMessage) -> None:
        self._failed.append(message)
        error = ProducerError("InMemoryProducer: injected send failure")
        if self._fail_error is not None:
            raise error from self._fail_error
        raise error

    # -- failure injection ---------------------------------------------------

    def fail_next(self, n: int = 1, *, cause: Exception | None = None) -> None:
        """Make the next `n` sends raise `ProducerError` (chained to `cause`)."""
        self._fail_next = n
        self._fail_error = cause

    def fail_when(
        self,
        predicate: Callable[[OutgoingMessage], bool] | None,
        *,
        cause: Exception | None = None,
    ) -> None:
        """Make every send for which `predicate(message)` is true fail.

        Pass `None` to remove the rule.
        """
        self._fail_when = predicate
        self._fail_error = cause

    # -- assertion helpers ---------------------------------------------------

    def messages_for(self, topic: str) -> list[OutgoingMessage]:
        """Successfully sent messages on `topic`, in send order."""
        return [m for m in self._sent if m.topic == topic]

    def assert_sent(
        self,
        *,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Mapping[str, bytes] | None = None,
    ) -> OutgoingMessage:
        """Assert a matching message was sent and return the first match.

        Only the fields given are compared; `headers` must be a subset of the
        message's headers.
        """
        for message in self.messages_for(topic):
            if value is not None and message.value != value:
                continue
            if key is not None and message.key != key:
                continue
            if headers is not None and any(
                message.headers.get(name) != val for name, val in headers.items()
            ):
                continue
            return message
        raise AssertionError(
            f"no message sent matching topic={topic!r} value={value!r} key={key!r} "
            f"headers={headers!r}; sent: {self._sent!r}"
        )

    def clear(self) -> None:
        """Forget recorded messages and the flush count (failure rules stay)."""
        self._sent.clear()
        self._failed.clear()
        self.flush_count = 0
