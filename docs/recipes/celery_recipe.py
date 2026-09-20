"""Celery task consuming records handed over by your Kafka bridge.

Use acks_late=True and reject_on_worker_lost=True so a crashed worker's task is
redelivered (and the dedup store, not luck, prevents the double side effect).
Celery tasks are synchronous, so each runs its own event loop: build the store
connection inside the loop (or use a store that needs none, such as Redis with a
per-call client), never share an asyncio pool across tasks.

    @app.task(bind=True, acks_late=True, reject_on_worker_lost=True)
    def process_order(self, record: dict):
        run_deduplicated(self, dedup_factory(), Record(**record), handle)
"""

from __future__ import annotations

import asyncio

from kafka_reliability.core.message import Record


def run_deduplicated(task, dedup, record: Record, handler, *, countdown: int = 5) -> None:
    async def guarded():
        async with dedup.process(record) as decision:
            if decision:
                await handler(record)
        return decision

    decision = asyncio.run(guarded())  # an exception releases the claim; Celery retries/rejects
    if not decision.commit_offset:
        raise task.retry(countdown=countdown)  # live lease elsewhere: come back later
