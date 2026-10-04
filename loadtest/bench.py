"""Library-level benchmark: pure quota-operation latency and throughput.

Spawns PROCS processes (stand-ins for service instances); each runs CONC
concurrent async loops issuing reserve+commit (and some consume) against a
pool of orgs with bursty, skewed traffic. At the end it checks the
invariant for every org touched: granted == used <= limit, reserved == 0.

    python -m loadtest.bench --procs 4 --conc 64 --seconds 15

WARNING: flushes the Redis DB in REDIS_URL (default db 14) before running.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import statistics
import time
from datetime import UTC, datetime
from typing import Any

from redis import Redis as SyncRedis
from redis.asyncio import Redis

from quota import QuotaClient, period_for
from quota.keys import keys_for

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/14")
FEAT = "container-tracking"


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else 0.0


def org_name(i: int) -> str:
    return f"bench-{i:05d}"


async def _run(seed: int, conc: int, seconds: float, orgs: int) -> dict[str, Any]:
    r = Redis.from_url(REDIS_URL, max_connections=conc + 4)
    q = QuotaClient(r)
    rnd = random.Random(seed)
    lat: list[float] = []
    granted: dict[str, int] = {}
    ops = rejected = 0
    deadline = time.perf_counter() + seconds

    async def timed(coro: Any) -> Any:
        nonlocal ops
        t = time.perf_counter()
        result = await coro
        lat.append((time.perf_counter() - t) * 1000)
        ops += 1
        return result

    async def loop() -> None:
        nonlocal rejected
        while time.perf_counter() < deadline:
            # Skewed: 20% of orgs get 80% of traffic, and requests come in bursts.
            org_i = rnd.randrange(orgs // 5) if rnd.random() < 0.8 else rnd.randrange(orgs)
            org = org_name(org_i)
            granted.setdefault(org, 0)  # audit every org touched, even if never granted
            for _ in range(rnd.randint(1, 8)):  # a burst
                units = rnd.choice([1, 1, 1, 5, 10, 50, 100])
                if rnd.random() < 0.3:
                    res = await timed(q.consume(org, FEAT, units))
                    ok = res.status == "OK"
                else:
                    res = await timed(q.reserve(org, FEAT, units))
                    ok = res.status == "OK" and res.reservation is not None
                    if ok:
                        await timed(q.commit(res.reservation))
                if ok:
                    granted[org] += units
                else:
                    rejected += 1

    try:
        await asyncio.gather(*[loop() for _ in range(conc)])
    finally:
        await r.aclose()
    return {"lat": lat, "ops": ops, "rejected": rejected, "granted": granted}


def _proc(seed: int, conc: int, seconds: float, orgs: int, barrier: Any, out: Any) -> None:
    barrier.wait()
    try:
        out.put(asyncio.run(_run(seed, conc, seconds, orgs)))
    except BaseException as e:
        out.put({"error": f"worker {seed} failed: {e!r}"})
        raise


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--conc", type=int, default=64)
    ap.add_argument("--seconds", type=float, default=15)
    ap.add_argument("--orgs", type=int, default=5000)
    ap.add_argument("--limit", type=int, default=20000)
    a = ap.parse_args()

    sync = SyncRedis.from_url(REDIS_URL)
    sync.flushdb()
    pipe = sync.pipeline(transaction=False)
    for i in range(a.orgs):
        pipe.set(keys_for(org_name(i), FEAT, "_").limit, a.limit)
    pipe.execute()

    ctx = mp.get_context("spawn")
    barrier, out = ctx.Barrier(a.procs), ctx.Queue()
    ps = [
        ctx.Process(target=_proc, args=(i, a.conc, a.seconds, a.orgs, barrier, out))
        for i in range(a.procs)
    ]
    t0 = time.perf_counter()
    for p in ps:
        p.start()
    parts = [out.get(timeout=a.seconds + 120) for _ in ps]
    for p in ps:
        p.join()
    wall = time.perf_counter() - t0
    errors = [part["error"] for part in parts if "error" in part]
    if errors:
        raise SystemExit("\n".join(errors))

    lat = [x for part in parts for x in part["lat"]]
    ops = sum(part["ops"] for part in parts)
    granted: dict[str, int] = {}
    for part in parts:
        for org, u in part["granted"].items():
            granted[org] = granted.get(org, 0) + u

    # Invariant check against Redis for every org that saw traffic.
    period = period_for(datetime.now(UTC)).id
    violations = exhausted = 0
    for org, g in granted.items():
        h = sync.hgetall(keys_for(org, FEAT, period).state)
        used, reserved = int(h.get(b"used", 0)), int(h.get(b"reserved", 0))
        if used != g or used > a.limit or reserved != 0:
            violations += 1
        if a.limit - used < 100:
            exhausted += 1
    sync.close()

    report = {
        "procs": a.procs,
        "concurrency_per_proc": a.conc,
        "orgs": a.orgs,
        "duration_s": a.seconds,
        "wall_s": round(wall, 1),
        "quota_ops": ops,
        "throughput_ops_s": round(ops / a.seconds),
        "latency_ms": {
            "p50": round(pct(lat, 0.50), 3),
            "p95": round(pct(lat, 0.95), 3),
            "p99": round(pct(lat, 0.99), 3),
            "max": round(max(lat, default=0.0), 3),
            "mean": round(statistics.mean(lat), 3) if lat else 0.0,
        },
        "rejected_requests": sum(part["rejected"] for part in parts),
        "orgs_touched": len(granted),
        "orgs_near_exhausted": exhausted,
        "invariant_violations": violations,
    }
    print(json.dumps(report, indent=2))
    if violations:
        raise SystemExit("INVARIANT VIOLATED")


if __name__ == "__main__":
    main()
