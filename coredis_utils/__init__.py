from __future__ import annotations

import hmac
import inspect
import pickle
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
from hashlib import sha256
from typing import Any, AnyStr, Generic, ParamSpec, TypeVar, overload

from anyio import sleep
from coredis import PureToken, Redis, RedisCluster
from coredis.commands import CommandRequest
from coredis.typing import KeyT

P = ParamSpec("P")
R = TypeVar("R")

VERSION = "0.1.0"
LIMITER_SCRIPT = """
local val = redis.call('incr', KEYS[1])
if val == 1 then
  redis.call('expire', KEYS[1], ARGV[1])
end
return val
"""
__version__ = VERSION


def _limit(key: KeyT, period: int) -> CommandRequest[int]: ...


def _make_cache_key(
    prefix: str,
    fn: Callable[..., Any],
    signature: inspect.Signature,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> str:
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    canonical = pickle.dumps(tuple(bound.arguments.items()))
    digest = sha256(canonical).hexdigest()[:16]
    return f"{prefix}cache:{fn.__module__}.{fn.__qualname__}:{digest}"


def _ttl_seconds(ttl: timedelta | int) -> int:
    if isinstance(ttl, timedelta):
        return round(ttl.total_seconds())
    return ttl


# cache envelopes to distinguish a stored value from a stored error
@dataclass(slots=True, frozen=True)
class _Ok:
    value: Any


@dataclass(slots=True, frozen=True)
class _Err:
    exc: BaseException


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

    __slots__ = ("_client", "_limit", "prefix", "signing_secret", "ttl")

    @overload
    def __init__(
        self: CoredisUtils[bytes],
        client: Redis[bytes] | RedisCluster[bytes],
        *,
        prefix: str | None = ...,
        ttl: timedelta | int | None = ...,
        signing_secret: str | None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self: CoredisUtils[str],
        client: Redis[str] | RedisCluster[str],
        *,
        prefix: str | None = ...,
        ttl: timedelta | int | None = ...,
        signing_secret: str | None = ...,
    ) -> None: ...

    def __init__(
        self: CoredisUtils[Any],
        client: Redis[Any] | RedisCluster[Any],
        *,
        prefix: str | None = "coredis-utils",
        ttl: timedelta | int | None = 300,
        signing_secret: str | None = None,
    ) -> None:
        # Redis connection
        self._client = client
        self.prefix = prefix + ":" if prefix else ""
        self.ttl = ttl
        self.signing_secret = signing_secret.encode() if signing_secret else None
        # coredis FFI stubs for Lua script
        self._limit = client.register_script(LIMITER_SCRIPT).wraps()(_limit)

    async def idempotent(self, key: str, ttl: timedelta | int | None = 60) -> bool:
        """
        Shields code from being run multiple times.

        :param key: idempotency key to use
        :param ttl: how long to prevent duplicate runs, defaults to 1 minute
        """
        ttl = ttl or self.ttl
        return await self._client.set(
            f"{self.prefix}idempotent:{key}", 1, condition=PureToken.NX, ex=ttl
        )

    async def limit(self, key: str, limit: int, period: timedelta | int) -> bool:
        """
        Limits the number of successful calls per period to the given number using a
        fixed-window rate limiting algorithm.

        :param key: unique identifier, usually some combination of user/IP/route
        :param limit: maximum number of calls that can succeed per period
        :param period: duration of window before more calls can succeed
        """
        count = await self._limit(f"{self.prefix}limit:{key}", _ttl_seconds(period))
        return count <= limit

    def _serialize(self, data: Any) -> str | bytes:
        try:
            serialized = pickle.dumps(data)
        except Exception as e:
            raise RuntimeError(f"Failed to serialize data: {data}") from e
        if self.signing_secret:
            serialized += hmac.digest(self.signing_secret, serialized, "sha256")
        return serialized

    def _deserialize(self, data: Any) -> Any:
        if self.signing_secret:
            data_bytes, signature = data[:-32], data[-32:]
            verify = hmac.digest(self.signing_secret, data_bytes, "sha256")
            if not hmac.compare_digest(signature, verify):
                raise RuntimeError("Invalid signature for task data!")
            data = data_bytes
        try:
            return pickle.loads(data)
        except Exception as e:
            raise RuntimeError(f"Failed to deserialize data: {data}") from e

    async def _try_read(self, key: str) -> _Ok | None:
        raw = await self._client.get(key)
        if raw is not None:
            val = self._deserialize(raw)
            if isinstance(val, _Err):
                raise val.exc
            if isinstance(val, _Ok):
                return val

    @overload
    def cached(self, fn: Callable[P, Awaitable[R]], /) -> Callable[P, Awaitable[R]]: ...

    @overload
    def cached(
        self,
        *,
        ttl: timedelta | int | None = ...,
        error_ttl: timedelta | int | None = ...,
        lock_timeout: timedelta | int = ...,
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...

    def cached(
        self,
        fn: Callable[P, Awaitable[R]] | None = None,
        *,
        ttl: timedelta | int | None = None,
        error_ttl: timedelta | int | None = None,
        lock_timeout: timedelta | int = 60,
    ) -> Any:
        """
        Cache the function's results in Redis. Uses a lock to implement "singleflight",
        protecting against thundering herds.

        :param ttl: duration to cache results, defaults to `cache_ttl`
        :param error_ttl:
            duration to cache errors, defaults to `cache_ttl`, 0 means disabled
        :param lock_timeout: TTL of the stampede protection lock, defaults to 1 minute
        """
        ttl = ttl or self.ttl
        error_ttl = error_ttl if error_ttl is not None else (self.ttl or 0)

        def decorator(_fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            sig = inspect.signature(_fn)

            @wraps(_fn)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                key = _make_cache_key(self.prefix, _fn, sig, args, kwargs)
                # fast path: cache hit
                if res := await self._try_read(key):
                    return res.value
                # slow path: add lock for stampede protection
                lock = self._client.lock(
                    f"{self.prefix}lock:{key}",
                    timeout=_ttl_seconds(lock_timeout),
                    blocking=False,
                )
                if await lock.acquire():
                    try:
                        if res := await self._try_read(key):
                            return res.value
                        value = await _fn(*args, **kwargs)
                        await self._client.set(key, self._serialize(_Ok(value)), ex=ttl)
                        return value
                    except Exception as e:
                        if error_ttl != 0:
                            await self._client.set(
                                key, self._serialize(_Err(exc=e)), ex=error_ttl
                            )
                        raise
                    finally:
                        await lock.release()
                # wait for leader's result by polling the cache
                deadline = time.monotonic() + _ttl_seconds(lock_timeout)
                sleep_time = 0.005  # 5ms initial backoff
                while time.monotonic() < deadline:
                    await sleep(sleep_time)
                    if res := await self._try_read(key):
                        return res.value
                    sleep_time = min(sleep_time * 2, 0.1)  # capped exponential backoff
                # leader didn't complete in time, just run ourselves
                return await _fn(*args, **kwargs)

            return wrapper

        if fn is None:
            return decorator
        return decorator(fn)


__all__ = ["CoredisUtils"]
