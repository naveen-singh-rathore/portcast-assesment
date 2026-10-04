import os
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from quota import QuotaClient

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")


class FakeClock:
    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kw: float) -> None:
        self.now += timedelta(**kw)


@pytest_asyncio.fixture
async def redis():
    r = Redis.from_url(REDIS_URL)
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.aclose()


@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 10, 15, 12, 0, tzinfo=UTC))


@pytest_asyncio.fixture
async def quota(redis, clock):
    return QuotaClient(redis, clock=clock, reservation_ttl=timedelta(seconds=30))
