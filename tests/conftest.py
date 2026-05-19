from collections.abc import AsyncIterator
from typing import Any

import pytest
from coredis import Redis


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("trio", {}), id="trio"),
    ]
)
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(params=[pytest.param(True, id="str"), pytest.param(False, id="bytes")])
async def redis(request: pytest.FixtureRequest) -> AsyncIterator[Redis[Any]]:
    async with Redis(decode_responses=request.param) as client:
        await client.flushdb()
        yield client
