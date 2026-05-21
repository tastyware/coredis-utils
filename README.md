[![PyPI](https://img.shields.io/pypi/v/coredis-utils)](https://pypi.org/project/coredis-utils)
[![Downloads](https://static.pepy.tech/badge/coredis-utils)](https://pepy.tech/project/coredis-utils)
[![Release](https://img.shields.io/github/v/release/tastyware/coredis-utils?label=release%20notes)](https://github.com/tastyware/coredis-utils/releases)

# coredis-utils

A collection of helpful utilities for [coredis](https://coredis.readthedocs.io/en/latest/).

## Features

- Caching decorator with thundering herd protection and error caching
- Idempotency keys
- Fixed-window rate limiting

## Installation

```console
$ pip install coredis-utils
```

## Getting started

First, create a `CoredisUtils` object wrapping a `coredis.Redis` instance:

```python
from coredis import Redis
from coredis_utils import CoredisUtils

client = Redis(...)
utils = CoredisUtils(client)
```

Caching is implemented with a decorator:

```python
@utils.cached(ttl=60)
async def my_task() -> int: ...
```

Idempotency uses a simple check:

```python
if await utils.idempotent("my-key", ttl=60):
    ...  # code in this block can only run once
```

Rate limiting is similar:

```python
for _ in range(10):
    if await utils.limit("my-ip-addr", 5, 1):  # limit to 5/second
        print("success")
```

```
success
success
success
success
success
```

## Advanced caching

Cache keys are generated using a SHA256 hash of pickled arguments. You can exclude non-serializable arguments from cache key construction:

```python
from sqlalchemy.ext.asyncio import AsyncSession

@utils.cached(ttl=60, exclude={"session"})
async def my_task(session: AsyncSession) -> int: ...
```

You can also customize which parts of arguments get hashed:

```python
@utils.cached(
    ttl=60,
    key_fns={
        # hash just the ID, not the entire model
        "user": lambda u: u.id,
        # hash a couple relevant fields
        "message": lambda m: (m.type, m.timestamp),
    },
)
async def my_task(user: User, message: Message) -> int: ...
```

Errors can be cached and propagated just like normal responses:

```python
@utils.cached(ttl=60, error_ttl=5)
async def my_task() -> int:
    raise Exception("Oh no!")
```

You can easily invalidate keys by passing the same arguments:

```python
@utils.cached(ttl=60)
async def my_task(time: int) -> int: ...

await my_task.invalidate(3)
```
