"""RedisDedupStore — the throughput alternative. Requires the [dedup-redis]
extra.

`claim` is `SET key value NX PX ttl`: one atomic round trip, and expiry is
native, so there is no sweep job and no vacuum pressure.

What you give up, stated plainly:

* It **cannot join the handler's transaction** (`supports_transactions =
  False`), so the Deduplicator refuses the strong mode against it.
* Availability becomes a correctness question. Unreachable Redis raises
  `StoreUnavailableError`; the Deduplicator's `on_store_unavailable` decides
  whether to stop (`fail_closed`, the default) or process without dedup.
* Durability is your Redis config. Default persistence can lose the last seconds
  of writes on a crash, and a replica failover can lose more; a lost key is a
  duplicate processed. Tolerable for "don't send the email twice", not for
  "don't charge the card twice".
* An expired lease is indistinguishable from a key that never existed, so this
  store cannot report `lease_expired` (the `dedup.lease_expired` metric stays
  silent for it); it still reprocesses, which is the D6 behaviour.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from kafka_reliability.core.errors import ConfigurationError, StoreUnavailableError, require_extra
from kafka_reliability.dedup.store import (
    DONE_STATE,
    IN_PROGRESS_STATE,
    Claim,
    ClaimResult,
    ClaimState,
)

try:
    import redis.exceptions as redis_exceptions
except ImportError as exc:
    require_extra(package="redis", extra="dedup-redis", cause=exc)

# Delete only if still an in-progress claim, atomically.
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0"
)


class RedisDedupStore:
    """Dedup records in Redis. `client` is a `redis.asyncio.Redis`."""

    supports_transactions = False

    def __init__(self, client: Any, *, prefix: str = "kafka_reliability:dedup") -> None:
        self._client = client
        self._prefix = prefix

    def _key(self, group: str, key: str) -> str:
        # Length-prefix the group so ("a:b", "c") and ("a", "b:c") cannot collide.
        return f"{self._prefix}:{len(group)}:{group}:{key}"

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        try:
            return await getattr(self._client, method)(*args, **kwargs)
        except (redis_exceptions.RedisError, OSError) as exc:
            raise StoreUnavailableError("dedup store (Redis) is unreachable") from exc

    async def claim(
        self, group: str, key: str, *, state: ClaimState, expires_in: timedelta, conn: Any = None
    ) -> Claim:
        k, px = self._key(group, key), max(1, int(expires_in.total_seconds() * 1000))
        for _ in range(3):
            if await self._call("set", k, state, nx=True, px=px):
                return Claim(ClaimResult.CLAIMED)
            live = await self._call("get", k)
            live = live.decode() if isinstance(live, bytes) else live
            if live == DONE_STATE:
                return Claim(ClaimResult.ALREADY_DONE)
            if live == IN_PROGRESS_STATE:
                return Claim(ClaimResult.IN_PROGRESS)
            # expired between SET and GET: try again
        raise StoreUnavailableError("could not settle a dedup claim under heavy contention")

    async def confirm(
        self, group: str, key: str, *, expires_in: timedelta, conn: Any = None
    ) -> None:
        px = max(1, int(expires_in.total_seconds() * 1000))
        await self._call("set", self._key(group, key), DONE_STATE, px=px)

    async def release(self, group: str, key: str, *, conn: Any = None) -> None:
        await self._call("eval", _RELEASE_LUA, 1, self._key(group, key), IN_PROGRESS_STATE)

    async def is_done(self, group: str, key: str, *, conn: Any = None) -> bool:
        live = await self._call("get", self._key(group, key))
        live = live.decode() if isinstance(live, bytes) else live
        return bool(live == DONE_STATE)

    async def purge(
        self,
        *,
        group: str | None = None,
        key: str | None = None,
        chunk: int = 10_000,
        conn: Any = None,
    ) -> int:
        if key is None:
            return 0  # Redis expires natively; there is nothing to sweep
        if group is None:
            raise ConfigurationError("purge(key=...) needs group=")
        return int(await self._call("delete", self._key(group, key)))
