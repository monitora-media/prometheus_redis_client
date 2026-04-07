import os

import pytest
from testcontainers.redis import RedisContainer

os.environ['METRICS_REDIS_URI'] = 'redis://dummy'  # needed to satisfy module-level assert


@pytest.fixture(scope='session')
def redis():
    with RedisContainer() as container:
        yield container.get_client()


@pytest.fixture()
def registry(redis):
    """Create a fresh Registry backed by the testcontainers Redis, flushing between tests."""
    from prometheus_redis_client.registry import Registry

    reg = Registry(redis=redis)
    try:
        yield reg
    finally:
        redis.flushall()
