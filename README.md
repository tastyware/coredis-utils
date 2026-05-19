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
for _ in range(15):
    print(await utils.limit("my-ip-addr", 10, 1))  # limit to 10/second
```

```python
True
True
True
True
True
True
True
True
True
True
False
False
False
False
False
```
