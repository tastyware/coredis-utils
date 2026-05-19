from typing import Any

import pytest
from anyio import create_task_group, sleep
from coredis import Redis
from coredis._concurrency import gather

from coredis_utils import CoredisUtils

pytestmark = pytest.mark.anyio


async def test_idempotency_sequential(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    counter = 0
    for _ in range(5):
        if await utils.idempotent("test_idempotency_sequential"):
            counter += 1
    assert counter == 1


async def test_idempotency_parallel(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    counter = 0

    async def contend():
        nonlocal counter
        if await utils.idempotent("test_idempotency_parallel"):
            counter += 1

    async with create_task_group() as tg:
        for _ in range(500):
            tg.start_soon(contend)
    assert counter == 1


async def test_idempotency_ttl(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    counter = 0
    if await utils.idempotent("test_idempotency_ttl", ttl=1):
        counter += 1
    await sleep(1)
    if await utils.idempotent("test_idempotency_ttl", ttl=1):
        counter += 1
    assert counter == 2


async def test_rate_limiter(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    for _ in range(5):
        assert await utils.limit("limiter", 5, 1)  # 5/second
    assert not await utils.limit("limiter", 5, 1)


async def test_rate_limiter_parallel(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    counter = 0

    async def contend():
        nonlocal counter
        if await utils.limit("limiter", 5, 1):
            counter += 1

    async with create_task_group() as tg:
        for _ in range(500):
            tg.start_soon(contend)
    assert counter == 5


async def test_rate_limiter_next_window(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    for _ in range(5):
        assert await utils.limit("limiter", 5, 1)  # 5/second
    assert not await utils.limit("limiter", 5, 1)
    await sleep(1)
    assert await utils.limit("limiter", 5, 1)


async def test_cache_hits_concurrent(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    counter = 0

    @utils.cached(ttl=1)
    async def do_work() -> int:
        nonlocal counter
        await sleep(0)
        counter += 1
        return 22

    results = await gather(*[do_work() for _ in range(1_000)])
    assert sum(results) == 22_000
    assert counter == 1


async def test_cache_hits_sequential(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached
    async def work() -> int:
        nonlocal calls
        calls += 1
        return 42

    assert await work() == 42
    assert await work() == 42
    assert calls == 1


async def test_cache_none_value(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60)
    async def work() -> None:
        nonlocal calls
        calls += 1
        return None

    assert await work() is None
    assert await work() is None
    assert calls == 1


async def test_cache_different_args_different_entries(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60)
    async def work(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert await work(1) == 2
    assert await work(2) == 4
    assert await work(3) == 6
    assert calls == 3
    assert await work(1) == 2
    assert await work(2) == 4
    assert calls == 3


async def test_cache_canonical_args(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60)
    async def work(x: int, y: int = 10) -> int:
        nonlocal calls
        calls += 1
        return x + y

    assert await work(1) == 11
    assert await work(1, 10) == 11
    assert await work(1, y=10) == 11
    assert await work(x=1, y=10) == 11
    assert calls == 1


async def test_cache_complex_args(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60)
    async def work(items: list[int], options: dict[str, int]) -> int:
        nonlocal calls
        calls += 1
        return sum(items) + sum(options.values())

    assert await work([1, 2, 3], {"a": 10, "b": 20}) == 36
    assert await work([1, 2, 3], {"a": 10, "b": 20}) == 36
    assert calls == 1


async def test_cache_errors_are_cached(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60, error_ttl=60)
    async def work() -> int:
        nonlocal calls
        calls += 1
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        await work()
    with pytest.raises(ValueError, match="nope"):
        await work()

    assert calls == 1


async def test_cache_error_ttl_zero_disables(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60, error_ttl=0)
    async def work() -> int:
        nonlocal calls
        calls += 1
        raise ValueError("nope")

    for _ in range(3):
        with pytest.raises(ValueError):
            await work()

    assert calls == 3


async def test_cache_stampede_propagates_error(redis: Redis[Any]):
    """When the leader raises, all concurrent followers see the same error."""
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60, error_ttl=60)
    async def work() -> int:
        nonlocal calls
        await sleep(0.01)  # give followers time to queue up behind us
        calls += 1
        raise ValueError("kaboom")

    async def attempt() -> str:
        try:
            await work()
            return "no error"
        except ValueError as e:
            return str(e)

    results = await gather(*[attempt() for _ in range(1000)])
    assert all(r == "kaboom" for r in results)
    assert calls == 1


async def test_cache_signing_round_trip(redis: Redis[Any]):
    utils = CoredisUtils(redis, signing_secret="testsecret123")
    calls = 0

    @utils.cached(ttl=60)
    async def work() -> int:
        nonlocal calls
        calls += 1
        return 42

    assert await work() == 42
    assert await work() == 42
    assert calls == 1


async def test_cache_signing_rejects_tampered_data(redis: Redis[Any]):
    utils = CoredisUtils(redis, signing_secret="testsecret123")

    @utils.cached(ttl=60)
    async def work() -> int:
        return 42

    await work()
    # Find the cache entry and tamper with its signature
    keys = [k async for k in redis.scan_iter(match="*cache:*")]
    assert len(keys) == 1
    raw = await redis.get(keys[0])
    assert raw
    tampered = raw[:-32] + b"\x00" * 32
    await redis.set(keys[0], tampered)

    with pytest.raises(RuntimeError, match="Invalid signature"):
        await work()


async def test_cache_ttl_expiry(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=1)
    async def work() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await work() == 1
    assert await work() == 1  # cached
    await sleep(1)
    assert await work() == 2  # expired
    assert calls == 2


async def test_cache_error_ttl_expiry(redis: Redis[Any]):
    utils = CoredisUtils(redis)
    calls = 0

    @utils.cached(ttl=60, error_ttl=1)
    async def work() -> int:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise ValueError("first attempt fails")
        return 42

    with pytest.raises(ValueError):
        await work()
    with pytest.raises(ValueError):
        await work()  # cached error
    assert calls == 1
    await sleep(1)

    assert await work() == 42  # error expired, retry succeeds
    assert calls == 2
