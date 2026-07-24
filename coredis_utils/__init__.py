from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, AnyStr, Generic, overload

from coredis import PureToken, Redis, RedisCluster

from coredis_utils.cache import CachedFunction, P, R

VERSION = "0.5.0"
__version__ = VERSION


class CoredisUtils(Generic[AnyStr]):
    """
    Wraps a coredis client to provide additional functionality.

    :param client: basic or cluster client to use
    :param prefix: prefix for all keys used in Redis
    :param ttl: default TTL for cached results/idempotency keys, defaults to 5 minutes
    :param signing_secret:
        if provided, used to sign results cached in Redis, which improves security since
        serialization uses pickle. To generate a key, try: `secrets.token_urlsafe(32)`
    """

    __slots__ = ("_client", "prefix", "signing_secret", "ttl")

    @overload
    def __init__(
        self: CoredisUtils[bytes],
        client: Redis[bytes] | RedisCluster[bytes],
        *,
        prefix: str | None = ...,
        ttl: timedelta | int = ...,
        signing_secret: str | None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self: CoredisUtils[str],
        client: Redis[str] | RedisCluster[str],
        *,
        prefix: str | None = ...,
        ttl: timedelta | int = ...,
        signing_secret: str | None = ...,
    ) -> None: ...

    def __init__(
        self: CoredisUtils[Any],
        client: Redis[Any] | RedisCluster[Any],
        *,
        prefix: str | None = "coredis-utils",
        ttl: timedelta | int = 300,
        signing_secret: str | None = None,
    ) -> None:
        # Redis connection
        self._client = client
        self.prefix = prefix + ":" if prefix else ""
        self.ttl = ttl
        self.signing_secret = signing_secret.encode() if signing_secret else None

    async def idempotent(self, key: str, ttl: timedelta | int | None = 60) -> bool:
        """
        Shields code from being run multiple times.

        :param key: idempotency key to use
        :param ttl: how long to prevent duplicate runs, defaults to 1 minute
        """
        ttl = ttl if ttl is not None else self.ttl
        return await self._client.set(
            f"{self.prefix}idempotent:{key}", 1, condition=PureToken.NX, ex=ttl
        )

    async def limit(self, key: str, limit: int, period: timedelta) -> bool:
        """
        Limits the number of successful calls per period to the given number using a
        fixed-window rate limiting algorithm. Requires INCREX from Redis >= 8.8.0.

        :param key: unique identifier, usually some combination of user/IP/route
        :param limit: maximum number of calls that can succeed per period
        :param period: duration of window before more calls can succeed
        """
        _, remaining = await self._client.increx(
            f"{self.prefix}limit:{key}", upperbound=limit, ex=period, enx=True
        )
        return bool(remaining)

    @overload
    def cached(self, fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...

    @overload
    def cached(
        self,
        *,
        ttl: timedelta | int | None = ...,
        error_ttl: timedelta | int = ...,
        exclude: set[str] | None = ...,
        key_fns: dict[str, Callable[[Any], Any]] | None = ...,
    ) -> Callable[[Callable[P, Awaitable[R]]], CachedFunction[P, R]]: ...

    def cached(
        self,
        fn: Callable[P, Awaitable[R]] | None = None,
        *,
        ttl: timedelta | int | None = None,
        error_ttl: timedelta | int = 0,
        exclude: set[str] | None = None,
        key_fns: dict[str, Callable[[Any], Any]] | None = None,
    ) -> Any:
        """
        Cache results in Redis with both per-process and per-worker thundering herd
        protection on cache miss.

        Cache key is generated using a SHA256 hash of pickled arguments. Use `exclude`
        to exclude arguments from hashing or `key_fns` to modify which parts of an
        argument get hashed. This is useful to exclude or modify parameters that can't
        be hashed or don't reliably hash to the same value (eg database sessions, HTTP
        clients, SQLAlchemy objects).

        :param ttl: duration to cache results, defaults to `CoredisUtils.ttl`
        :param error_ttl: duration to cache errors, defaults to 0 (disabled)
        :param exclude: argument names to exclude from cache key generation
        :param key_fns: mapping of argument name -> lambda to modify argument
        """
        ttl = ttl or self.ttl

        def decorator(_fn: Callable[P, Awaitable[R]]) -> CachedFunction[P, R]:
            return CachedFunction(
                self,
                self._client,
                _fn,
                ttl=ttl,
                error_ttl=error_ttl,
                exclude=exclude,
                key_fns=key_fns,
            )

        if fn is None:
            return decorator
        return decorator(fn)


__all__ = ["CoredisUtils"]
