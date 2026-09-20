"""Shared conformance suite for `DedupStore` implementations (issue #36).

Subclass `DedupStoreConformance` (named `Test...`) and implement `make_harness`.
The same scenarios then run against every backend, so they cannot drift."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta

import pytest

from kafka_reliability.dedup.store import ClaimResult, DedupStore

TTL = timedelta(days=7)
LEASE = timedelta(minutes=5)


@dataclass
class Harness:
    store: DedupStore
    advance: Callable[[timedelta], Awaitable[None]]  # move time forward past expiries
    sweeps_expired: bool = True  # False for stores that expire natively
    cleanup: Callable[[], Awaitable[None]] | None = field(default=None)


class DedupStoreConformance:
    async def make_harness(self) -> Harness:
        raise NotImplementedError

    @pytest.fixture
    async def h(self):
        harness = await self.make_harness()
        yield harness
        if harness.cleanup is not None:
            await harness.cleanup()

    async def test_first_claim_wins_then_done_is_already_done(self, h: Harness):
        s = h.store
        assert (await s.claim("g", "k", state="done", expires_in=TTL)).result is ClaimResult.CLAIMED
        again = await s.claim("g", "k", state="done", expires_in=TTL)
        assert again.result is ClaimResult.ALREADY_DONE and not again.lease_expired

    async def test_live_lease_is_in_progress_until_confirmed(self, h: Harness):
        s = h.store
        assert (await s.claim("g", "k", state="in_progress", expires_in=LEASE)).result is (
            ClaimResult.CLAIMED
        )
        assert (await s.claim("g", "k", state="in_progress", expires_in=LEASE)).result is (
            ClaimResult.IN_PROGRESS
        )
        assert not await s.is_done("g", "k")
        await s.confirm("g", "k", expires_in=TTL)
        assert (await s.claim("g", "k", state="in_progress", expires_in=LEASE)).result is (
            ClaimResult.ALREADY_DONE
        )
        assert await s.is_done("g", "k")

    async def test_release_frees_an_in_progress_claim(self, h: Harness):
        s = h.store
        await s.claim("g", "k", state="in_progress", expires_in=LEASE)
        await s.release("g", "k")
        assert (await s.claim("g", "k", state="in_progress", expires_in=LEASE)).result is (
            ClaimResult.CLAIMED
        )

    async def test_release_never_touches_a_done_record(self, h: Harness):
        s = h.store
        await s.claim("g", "k", state="done", expires_in=TTL)
        await s.release("g", "k")
        assert (await s.claim("g", "k", state="done", expires_in=TTL)).result is (
            ClaimResult.ALREADY_DONE
        )

    async def test_groups_are_namespaced(self, h: Harness):
        s = h.store
        await s.claim("billing", "k", state="done", expires_in=TTL)
        assert (await s.claim("inventory", "k", state="done", expires_in=TTL)).result is (
            ClaimResult.CLAIMED
        )

    async def test_a_done_record_expires_after_its_ttl(self, h: Harness):
        s = h.store
        await s.claim("g", "k", state="done", expires_in=timedelta(hours=1))
        await h.advance(timedelta(hours=2))
        again = await s.claim("g", "k", state="done", expires_in=TTL)
        assert again.result is ClaimResult.CLAIMED and not again.lease_expired

    async def test_an_expired_lease_is_reclaimed_and_reported(self, h: Harness):
        s = h.store
        await s.claim("g", "k", state="in_progress", expires_in=LEASE)
        await h.advance(LEASE + timedelta(seconds=1))
        again = await s.claim("g", "k", state="in_progress", expires_in=LEASE)
        assert again.result is ClaimResult.CLAIMED
        if h.sweeps_expired:  # Redis cannot tell an expired lease from an absent key
            assert again.lease_expired

    async def test_concurrent_claims_yield_exactly_one_winner(self, h: Harness):
        results = await asyncio.gather(
            *(h.store.claim("g", "same", state="in_progress", expires_in=LEASE) for _ in range(25))
        )
        outcomes = [r.result for r in results]
        assert outcomes.count(ClaimResult.CLAIMED) == 1
        assert outcomes.count(ClaimResult.IN_PROGRESS) == 24

    async def test_purge_key_forgets_one_record(self, h: Harness):
        s = h.store
        await s.claim("g", "a", state="done", expires_in=TTL)
        await s.claim("g", "b", state="done", expires_in=TTL)
        assert await s.purge(group="g", key="a") == 1
        assert (await s.claim("g", "a", state="done", expires_in=TTL)).result is ClaimResult.CLAIMED
        assert (await s.claim("g", "b", state="done", expires_in=TTL)).result is (
            ClaimResult.ALREADY_DONE
        )

    async def test_purge_sweeps_only_expired_records_in_chunks(self, h: Harness):
        s = h.store
        for i in range(5):
            await s.claim("g", f"old{i}", state="done", expires_in=timedelta(hours=1))
        await s.claim("g", "fresh", state="done", expires_in=timedelta(days=30))
        await h.advance(timedelta(hours=2))
        removed = await s.purge(chunk=2)
        assert removed == (5 if h.sweeps_expired else 0)
        assert (await s.claim("g", "fresh", state="done", expires_in=TTL)).result is (
            ClaimResult.ALREADY_DONE
        )
