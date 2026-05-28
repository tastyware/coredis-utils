from __future__ import annotations

import hmac
import inspect
import pickle
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from functools import update_wrapper
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar
from weakref import WeakValueDictionary

from anyio import Lock
from coredis import Redis, RedisCluster

if TYPE_CHECKING:
    from coredis_utils import CoredisUtils

P = ParamSpec("P")
R = TypeVar("R")


# cache envelopes to distinguish a stored value from a stored error
@dataclass(slots=True, frozen=True)
class _Ok:
    value: Any
    expiry: float


@dataclass(slots=True, frozen=True)
class _Err:
    type: str
    repr: str
    expiry: float


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

    def _should_refresh(self, res: _Ok | _Err) -> bool:
        remaining = res.expiry - time.time()
        base_ttl = self._ttl if isinstance(res, _Ok) else self._error_ttl
        window = base_ttl * 0.1  # last 10% of TTL
        if remaining >= window:
            return False
        if remaining <= 0:
            return True
        return random.random() > (remaining / window)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = self.build_key(*args, **kwargs)
        res = await self._try_read(key)
        if res and not self._should_refresh(res):
            return self._unwrap(res)
        # max one computation per process
        lock = self._locks.get(key)
        if lock is None:
            lock = Lock()
            self._locks[key] = lock
        prior_expiry = res.expiry if res else 0
        async with lock:
            latest = await self._try_read(key)
            if latest and latest.expiry > prior_expiry:
                return self._unwrap(latest)
            try:
                val = await self._fn(*args, **kwargs)
            except Exception as e:
                if self._error_ttl > 0:
                    res = _Err(
                        type=f"{type(e).__module__}.{type(e).__qualname__}",
                        repr=repr(e),
                        expiry=time.time() + self._error_ttl,
                    )
                    await self._client.set(
                        key, self._serialize(res), ex=self._error_ttl
                    )
                raise
            res = _Ok(value=val, expiry=time.time() + self._ttl)
            await self._client.set(key, self._serialize(res), ex=self._ttl)
            return val
