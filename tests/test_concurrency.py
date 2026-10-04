"""Concurrency proof.

Each worker is a separate OS process with its own Redis connection pool,
standing in for a separate service instance. All workers start at the same
moment (multiprocessing.Barrier) and hammer ONE (org, feature) counter.

The control test runs the same harness against a naive read-then-write
implementation and asserts that it DOES over-serve. That shows the harness
creates real contention, so the passing tests are meaningful rather than
lucky.
"""

import asyncio
import multiprocessing as mp
import os
import random
from datetime import UTC, datetime
from typing import Any

from redis import Redis as SyncRedis
from redis.asyncio import Redis

from quota import QuotaClient, Usage
from quota.keys import keys_for

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
ORG, FEAT = "acme", "container-tracking"
NAIVE_KEY = "naive:used"
PROCS = 8  # "instances"
TASKS_PER_PROC = 50  # concurrent requests inside each instance
# Burst storms pin every worker to one instant, so all requests land in one window
# no matter when the test runs.
PINNED = datetime(2026, 10, 15, 12, 0, 0, 500_000, tzinfo=UTC)


def _sizes(seed: int, n: int, mode: str) -> list[int]:
    rnd = random.Random(seed)
    if mode == "unit":
        return [1] * n
    return [rnd.choice([1, 2, 5, 10, 25, 50]) for _ in range(n)]


async def _worker_quota(seed: int, n: int, mode: str, flow: str, limit: int) -> int:
    r = Redis.from_url(REDIS_URL, max_connections=n)
    q = QuotaClient(r, clock=lambda: PINNED) if flow == "burst" else QuotaClient(r)
    rnd = random.Random(seed * 7)

    async def one(units: int) -> int:
        if flow in ("consume", "burst"):
            res = await q.consume(ORG, FEAT, units)
            return units if res.status == "OK" else 0
        # reserve -> (maybe fail) -> commit/release
        res = await q.reserve(ORG, FEAT, units)
        if res.status != "OK" or res.reservation is None:
            return 0
        await asyncio.sleep(rnd.random() * 0.005)  # downstream work
        if rnd.random() < 0.2:  # 20% downstream failure
            await q.release(res.reservation)
            return 0
        status, charged = await q.commit(res.reservation)
        assert status == "COMMITTED"
        return charged

    try:
        granted = await asyncio.gather(*[one(u) for u in _sizes(seed, n, mode)])
    finally:
        await r.aclose()
    return sum(granted)


async def _worker_naive(seed: int, n: int, mode: str, flow: str, limit: int) -> int:
    """Deliberately broken: GET, compare in Python, then INCRBY."""
    r = Redis.from_url(REDIS_URL, max_connections=n)

    async def one(units: int) -> int:
        used = int(await r.get(NAIVE_KEY) or 0)
        if used + units > limit:
            return 0
        await r.incrby(NAIVE_KEY, units)
        return units

    try:
        granted = await asyncio.gather(*[one(u) for u in _sizes(seed, n, mode)])
    finally:
        await r.aclose()
    return sum(granted)


def _proc(kind: str, seed: int, mode: str, flow: str, limit: int, barrier: Any, out: Any) -> None:
    fn = _worker_quota if kind == "quota" else _worker_naive
    barrier.wait()  # every "instance" fires at once
    try:
        out.put(asyncio.run(fn(seed, TASKS_PER_PROC, mode, flow, limit)))
    except BaseException as e:
        # Report the real error to the parent instead of leaving it waiting on the queue.
        out.put(f"worker {seed} failed: {e!r}")
        raise


def run_storm(
    kind: str, limit: int, mode: str = "unit", flow: str = "consume", burst: int | None = None
) -> int:
    with SyncRedis.from_url(REDIS_URL) as sync:
        sync.flushdb()
        sync.set(keys_for(ORG, FEAT, "_").limit, limit)
        if burst is not None:
            sync.set(keys_for(ORG, FEAT, "_").burst, f"{burst}/1000")
    ctx = mp.get_context("spawn")
    barrier, out = ctx.Barrier(PROCS), ctx.Queue()
    procs = [
        ctx.Process(target=_proc, args=(kind, i, mode, flow, limit, barrier, out))
        for i in range(PROCS)
    ]
    for p in procs:
        p.start()
    results = [out.get(timeout=60) for _ in procs]
    errors = [r for r in results if isinstance(r, str)]
    assert not errors, errors
    total: int = sum(results)
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0
    return total


def _usage(pinned: bool = False) -> Usage:
    async def go() -> Usage:
        r = Redis.from_url(REDIS_URL)
        try:
            q = QuotaClient(r, clock=lambda: PINNED) if pinned else QuotaClient(r)
            return await q.usage(ORG, FEAT)
        finally:
            await r.aclose()

    return asyncio.run(go())


def test_unit_requests_fill_exactly_to_zero() -> None:
    # demand = 8 * 50 = 400 requests x 1 unit against a limit of 100
    granted = run_storm("quota", limit=100, mode="unit")
    u = _usage()
    assert granted == 100  # fully used, not under-served
    assert u.used == 100 and u.remaining == 0 and u.reserved == 0


def test_mixed_batches_never_over_serve() -> None:
    granted = run_storm("quota", limit=500, mode="mixed")  # demand ~ 5,000+ units
    u = _usage()
    assert granted <= 500
    assert granted == u.used  # what callers got == what was recorded
    assert u.used + u.remaining == 500 and u.remaining >= 0
    assert u.remaining < 50  # leftover is smaller than the largest batch


def test_exact_total_equal_to_limit_all_granted() -> None:
    # 400 requests x 1 unit against a limit of exactly 400: every one must succeed
    granted = run_storm("quota", limit=400, mode="unit")
    assert granted == 400 and _usage().remaining == 0


def test_reserve_commit_release_under_contention() -> None:
    granted = run_storm("quota", limit=300, mode="mixed", flow="reserve")
    u = _usage()
    assert granted == u.used <= 300
    assert u.reserved == 0  # every hold resolved: no leaked quota


def test_burst_window_fills_exactly_under_contention() -> None:
    # 400 one-unit requests in one 1 s window allowing 60; the month allows 1,000.
    granted = run_storm("quota", limit=1000, mode="unit", flow="burst", burst=60)
    u = _usage(pinned=True)
    assert granted == 60 == u.used  # the window, not the month, was the binding limit
    assert u.burst is not None and u.burst.used == 60 and u.remaining == 940


def test_burst_window_never_over_serves_mixed_batches() -> None:
    granted = run_storm("quota", limit=10_000, mode="mixed", flow="burst", burst=200)
    u = _usage(pinned=True)
    assert granted == u.used <= 200
    assert u.burst is not None and u.burst.used == granted
    assert 200 - granted < 50  # leftover is smaller than the largest batch


def test_control_naive_read_then_write_over_serves() -> None:
    """If this ever stops over-serving, the harness lost its contention."""
    granted = run_storm("naive", limit=500, mode="mixed")
    with SyncRedis.from_url(REDIS_URL) as sync:
        used = int(sync.get(NAIVE_KEY) or 0)
    print(f"naive read-then-write: granted={granted} used={used} on a limit of 500")
    assert used > 500, f"naive impl did not over-serve (used={used}); harness too gentle"
