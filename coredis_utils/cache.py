from __future__ import annotations

import hmac
import inspect
import pickle
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from functools import update_wrapper
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar

from anyio import Event
from coredis import Redis, RedisCluster

if TYPE_CHECKING:
    from coredis_utils import CoredisUtils

P = ParamSpec("P")
R = TypeVar("R")


# cache envelopes to distinguish a stored value from a stored error
@dataclass(slots=True, frozen=True)
class _Ok:
    value: Any


@dataclass(slots=True, frozen=True)
class _Err:
    type: str
    repr: str

    @classmethod
    def from_exception(cls, exc: Exception) -> _Err:
        return _Err(
            type=f"{type(exc).__module__}.{type(exc).__qualname__}", repr=repr(exc)
        )


@dataclass(slots=True)
class _Flight:
    event: Event = field(default_factory=Event)
    exception: BaseException | None = None
    result: _Ok | _Err | None = None


class CachedError(Exception):
    """
    An error cached in Redis. Doesn't contain traceback or all exception info.
    """

    pass


class CachedFunction(Generic[P, R]):
    __slots__ = (
        "_owner",
        "_client",
        "_fn",
        "_fn_name",
        "_sig",
        "_ttl",
        "_error_ttl",
        "_exclude",
        "_key_fns",
        "_lock_timeout",
        "_flights",
        "__dict__",  # needed for update_wrapper()
        "__signature__",  # used by FastAPI/introspection
    )

    def __init__(
        self,
        owner: CoredisUtils[Any],
        client: Redis[Any] | RedisCluster[Any],
        fn: Callable[P, Awaitable[R]],
        *,
        ttl: timedelta | int | None,
        error_ttl: timedelta | int | None,
        exclude: set[str] | None,
        key_fns: dict[str, Callable[[Any], Any]] | None,
        lock_timeout: int,
    ) -> None:
        self._owner = owner
        self._client = client
        self._fn = fn
        self._fn_name = fn.__module__ + "." + fn.__qualname__
        self._sig = inspect.signature(fn)
        self._ttl = ttl
        self._error_ttl = error_ttl
        self._exclude = exclude or set()
        self._key_fns = key_fns or {}
        self._lock_timeout = lock_timeout
        update_wrapper(self, fn)
        self.__signature__ = self._sig
        self._flights: dict[str, _Flight] = {}

    def build_key(self, *args: P.args, **kwargs: P.kwargs) -> str:
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        key_args = {
            k: self._key_fns[k](v) if k in self._key_fns else v
            for k, v in bound.arguments.items()
            if k not in self._exclude
        }
        canonical = pickle.dumps(key_args, protocol=pickle.HIGHEST_PROTOCOL)
        digest = sha256(canonical).hexdigest()[:16]
        return f"{self._owner.prefix}cache:{self._fn_name}:{digest}"

    def _serialize(self, data: _Ok | _Err) -> str | bytes:
        try:
            serialized = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            raise RuntimeError(f"Failed to serialize data: {data}") from e
        if self._owner.signing_secret:
            serialized += hmac.digest(self._owner.signing_secret, serialized, "sha256")
        return serialized

    def _deserialize(self, data: Any) -> _Ok | _Err:
        if self._owner.signing_secret:
            data_bytes, signature = data[:-32], data[-32:]
            verify = hmac.digest(self._owner.signing_secret, data_bytes, "sha256")
            if not hmac.compare_digest(signature, verify):
                raise RuntimeError("Invalid signature for task data!")
            data = data_bytes
        try:
            return pickle.loads(data)
        except Exception as e:
            raise RuntimeError(f"Failed to deserialize data: {data}") from e

    async def _try_read(self, key: str) -> _Ok | _Err | None:
        raw = await self._client.get(key)
        if raw is not None:
            return self._deserialize(raw)

    def _unwrap(self, res: _Ok | _Err) -> R:
        if isinstance(res, _Err):
            raise CachedError(f"Cached error {res.type}: {res.repr}")
        return res.value

    async def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> bool:
        """
        Invalidate the key built from the given arguments.
        """
        key = self.build_key(*args, **kwargs)
        return await self._client.delete([key]) == 1

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = self.build_key(*args, **kwargs)
        # fast path: cache hit
        if res := await self._try_read(key):
            return self._unwrap(res)
        # slow path: event for stampede protection
        # if flight exists, wait for leader
        if flight := self._flights.get(key):
            await flight.event.wait()
            if flight.exception:
                raise flight.exception
            assert flight.result
            return self._unwrap(flight.result)
        # slow path: leader gets distributed lock for stampede protection
        flight, res = _Flight(), None
        self._flights[key] = flight
        try:
            lock = self._client.lock(
                f"{self._owner.prefix}lock:{key}",
                timeout=self._lock_timeout,
                blocking_timeout=self._lock_timeout,
            )
            try:
                await lock.acquire()
                if not (res := await self._try_read(key)):
                    res = _Ok(await self._fn(*args, **kwargs))
            except Exception as e:
                if not res:
                    flight.exception = e
                    res = _Err.from_exception(e)
            finally:
                await lock.release()
            # set result and notify waiters
            flight.result = res
            flight.event.set()
            if isinstance(res, _Err):
                if self._error_ttl != 0:
                    await self._client.set(
                        key, self._serialize(res), ex=self._error_ttl
                    )
                if flight.exception:
                    raise flight.exception
                raise CachedError(f"Cached error {res.type}: {res.repr}")
            await self._client.set(key, self._serialize(res), ex=self._ttl)
            return res.value
        finally:
            self._flights.pop(key, None)
