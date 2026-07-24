from __future__ import annotations

import hmac
import inspect
import pickle
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from functools import update_wrapper
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar
from weakref import WeakValueDictionary

from anyio import Lock, sleep
from coredis import PureToken, Redis, RedisCluster

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


class CachedError(Exception):
    """
    An error cached in Redis. Doesn't contain traceback or all exception info.
    """

    pass


class CachedFunction(Generic[P, R]):
    __slots__ = (
        "hits",
        "misses",
        "_owner",
        "_client",
        "_fn",
        "_fn_name",
        "_locks",
        "_sig",
        "_ttl",
        "_error_ttl",
        "_exclude",
        "_key_fns",
        "__dict__",  # needed for update_wrapper()
        "__signature__",  # used by FastAPI/introspection
    )

    def __init__(
        self,
        owner: CoredisUtils[Any],
        client: Redis[Any] | RedisCluster[Any],
        fn: Callable[P, Awaitable[R]],
        *,
        ttl: timedelta | int,
        error_ttl: timedelta | int,
        exclude: set[str] | None,
        key_fns: dict[str, Callable[[Any], Any]] | None,
    ) -> None:
        #: number of cache hits
        self.hits = 0
        #: number of cache misses
        self.misses = 0
        self._owner = owner
        self._client = client
        self._fn = fn
        self._fn_name = fn.__module__ + "." + fn.__qualname__
        self._sig = inspect.signature(fn)
        self._ttl = ttl if isinstance(ttl, int) else round(ttl.total_seconds())
        self._error_ttl = (
            error_ttl
            if isinstance(error_ttl, int)
            else round(error_ttl.total_seconds())
        )
        self._exclude = exclude or set()
        self._key_fns = key_fns or {}
        update_wrapper(self, fn)
        self.__signature__ = self._sig
        self._locks = WeakValueDictionary[str, Lock]()

    def build_key(self, *args: P.args, **kwargs: P.kwargs) -> str:
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        key_args = tuple(
            self._key_fns[k](v) if k in self._key_fns else v
            for k, v in bound.arguments.items()
            if k not in self._exclude
        )
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

    async def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> bool:
        """
        Invalidate the key built from the given arguments.
        """
        key = self.build_key(*args, **kwargs)
        return await self._client.delete([key]) == 1

    def _unwrap(self, res: _Ok | _Err) -> R:
        if isinstance(res, _Err):
            raise CachedError(f"Cached error {res.type}: {res.repr}")
        return res.value

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = self.build_key(*args, **kwargs)
        if res := await self._try_read(key):
            self.hits += 1
            return self._unwrap(res)
        # collapse the intra-process herd: many concurrent calls become one Redis waiter
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = Lock()
        async with lock:
            # acquired per-process lock, recheck
            if res := await self._try_read(key):
                self.hits += 1
                return self._unwrap(res)
            # elect a single worker across the fleet to compute
            lock_key, backoff = f"{key}:lock", 0.05
            while not await self._client.set(
                lock_key, 1, condition=PureToken.NX, ex=10
            ):
                # backoff with jitter so waiters don't retry simultaneously
                await sleep(random.uniform(0, backoff))
                if res := await self._try_read(key):
                    self.hits += 1
                    return self._unwrap(res)
                backoff = min(backoff * 2, 1)
            # acquired per-worker lock, recheck
            if res := await self._try_read(key):
                self.hits += 1
                return self._unwrap(res)
            # winner computes once on behalf of every waiter
            self.misses += 1
            try:
                val = await self._fn(*args, **kwargs)
            except Exception as e:
                if self._error_ttl > 0:
                    res = _Err(
                        type=f"{type(e).__module__}.{type(e).__qualname__}",
                        repr=repr(e),
                    )
                    async with self._client.pipeline(transaction=False) as pipe:
                        pipe.set(key, self._serialize(res), ex=self._error_ttl)
                        pipe.delete([lock_key])
                else:
                    await self._client.delete([lock_key])
                raise
            res = _Ok(value=val)
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.set(key, self._serialize(res), ex=self._ttl)
                pipe.delete([lock_key])
            return val
