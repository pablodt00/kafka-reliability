# Consumer recipes

Tested code, not prose (`06-decisions.md` D4). Each file is ~40 lines you copy
into your service; `tests/dedup/test_recipes.py` imports and runs them in CI, so
a signature change in `Deduplicator` breaks the build rather than the reader.

Every recipe shows the same three things:

1. **Manual offset commits, after the work and the dedup record.** Set
   `enable.auto.commit=false`. Auto-commit fires on a timer with no relationship
   to whether the handler finished; committing before the work turns every crash
   into silent message loss that no dedup store can repair.
2. **`IN_PROGRESS` means "do not commit, and do not move past it".** Another
   worker holds a live lease. Skipping the message *and* committing a later
   offset would lose it if that worker dies, so each recipe waits and retries the
   same record.
3. **An exception releases the claim** (the `Deduplicator` does it) and the
   offset stays uncommitted, so the message is retried, not suppressed.

The recipes are duck-typed: they import no consumer framework, and the tests
drive them with small fakes shaped like the client objects. That verifies the
control flow; it does not verify the framework's real objects (a real broker
harness is issue #61) — check the attribute names against your client version.

| Recipe | Consumer |
|---|---|
| `aiokafka_recipe.py` | `AIOKafkaConsumer`, manual commit |
| `confluent_recipe.py` | `confluent_kafka.Consumer`, polled off the event loop |
| `faststream_recipe.py` | FastStream subscriber, manual ack |
| `celery_recipe.py` | Celery task with `acks_late` |
