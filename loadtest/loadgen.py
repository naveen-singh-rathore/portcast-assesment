"""HTTP load generator: drives the metered endpoint through the fleet.

Bursty per org (a few large batches, then quiet), client retries that reuse
the Idempotency-Key, and a final audit against Redis for every org touched:
never over the limit, and the growth in Redis `used` during the run matches what
clients were told. Usage is snapshotted before the run, so earlier runs in the
same month (data left in Redis) do not fail the audit.

    python -m loadtest.loadgen --targets http://localhost:8080 --seconds 30 --rps 500
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import httpx

FEATURE = "container-tracking"


@dataclass
class Outcome:
    """What the client knows about one logical request (one Idempotency-Key)."""

    org: str
    units: int
    succeeded: bool = False  # some attempt got 200 (fresh or replayed)
    unknown: bool = False  # some attempt died in transit: server may have charged it


@dataclass
class Stats:
    e2e_ms: list[float] = field(default_factory=list)
    quota_ms: list[float] = field(default_factory=list)
    codes: Counter[str] = field(default_factory=Counter)
    instances: Counter[str] = field(default_factory=Counter)
    rejections: Counter[str] = field(default_factory=Counter)  # 429 by reason
    retries: int = 0
    replayed: int = 0


def pct(xs: list[float], p: float) -> float | None:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(len(xs) * p))], 3) if xs else None


def audit(
    outcomes: list[Outcome],
    usage: dict[str, tuple[int, int]],
    baseline: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Compare client-side outcomes with Redis. usage: org -> (used, limit) after the run;
    baseline: org -> used before it (missing: 0).

    A request whose every attempt failed in transit has an unknown outcome, so
    the growth in `used` must lie in [known_granted, known_granted + unknown_units].
    `used` itself (including earlier runs) must never exceed the limit.
    """
    baseline = baseline or {}
    known: Counter[str] = Counter()
    unknown: Counter[str] = Counter()
    for o in outcomes:
        if o.succeeded:
            known[o.org] += o.units
        elif o.unknown:
            unknown[o.org] += o.units
    over_limit, mismatches = [], []
    for org, (used, limit) in usage.items():
        if used > limit:
            over_limit.append(org)
        before = baseline.get(org, 0)
        if not known[org] <= used - before <= known[org] + unknown[org]:
            mismatches.append(
                {
                    "org": org,
                    "client_granted": known[org],
                    "unknown": unknown[org],
                    "used_before": before,
                    "used": used,
                }
            )
    return {
        "orgs_audited": len(usage),
        "used_before_run_units": sum(baseline.get(org, 0) for org in usage),
        "over_limit_orgs": len(over_limit),
        "granted_vs_used_mismatches": len(mismatches),
        "mismatch_detail": mismatches[:10],
        "unknown_outcome_units": sum(unknown.values()),
    }


async def wait_for_fleet(c: httpx.AsyncClient, targets: list[str], tries: int = 60) -> None:
    for _ in range(tries):
        try:
            codes = [(await c.get(f"{t}/healthz")).status_code for t in targets]
            if all(code == 200 for code in codes):
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    raise SystemExit("fleet never became healthy")


async def fetch_used(c: httpx.AsyncClient, target: str, orgs: list[str]) -> dict[str, int]:
    async def one(org: str) -> tuple[str, int]:
        resp = await c.get(f"{target}/v1/quota/{org}/{FEATURE}")
        resp.raise_for_status()
        return org, int(resp.json()["used"])

    return dict(await asyncio.gather(*[one(org) for org in orgs]))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", default="http://localhost:8080")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--rps", type=float, default=500, help="target request rate")
    ap.add_argument("--orgs", type=int, default=200)
    ap.add_argument("--conc", type=int, default=128)
    ap.add_argument("--retry-rate", type=float, default=0.05, help="lost-response retries")
    a = ap.parse_args()
    targets = a.targets.split(",")

    stats = Stats()
    outcomes: list[Outcome] = []
    sem = asyncio.Semaphore(a.conc)
    rnd = random.Random(42)

    async with httpx.AsyncClient(timeout=5, limits=httpx.Limits(max_connections=a.conc)) as c:
        await wait_for_fleet(c, targets)
        # This run's orgs may already carry usage this month (e.g. an earlier run).
        orgs = [f"org-{i:04d}" for i in range(1, a.orgs + 1)]
        baseline = await fetch_used(c, targets[0], orgs)

        async def attempt(o: Outcome, key: str) -> bool:
            """One HTTP attempt. Returns False if it died in transit."""
            body = {"org": o.org, "containers": [f"C{i}" for i in range(o.units)]}
            async with sem:
                t = time.perf_counter()
                try:
                    resp = await c.post(
                        f"{rnd.choice(targets)}/v1/containers/track",
                        json=body,
                        headers={"Idempotency-Key": key},
                    )
                except httpx.HTTPError:
                    stats.codes["transport_error"] += 1
                    o.unknown = True
                    return False
            stats.e2e_ms.append((time.perf_counter() - t) * 1000)
            stats.codes[str(resp.status_code)] += 1
            if "X-Quota-Ms" in resp.headers:
                stats.quota_ms.append(float(resp.headers["X-Quota-Ms"]))
            if "X-Instance" in resp.headers:
                stats.instances[resp.headers["X-Instance"]] += 1
            if resp.status_code == 429:
                stats.rejections[resp.json().get("error", "unknown")] += 1
            if resp.status_code == 200:
                o.succeeded = True
                if resp.json().get("replayed"):
                    stats.replayed += 1
            return True

        async def send(o: Outcome) -> None:
            key = uuid.uuid4().hex
            delivered = await attempt(o, key)
            # Retry with the same key when the response was lost, plus a share of
            # requests whose response "got lost" on the way back to the client.
            if not delivered or rnd.random() < a.retry_rate:
                stats.retries += 1
                await attempt(o, key)

        tasks, start = [], time.perf_counter()
        interval = 1.0 / a.rps
        while time.perf_counter() - start < a.seconds:
            org = f"org-{rnd.randint(1, a.orgs):04d}"
            for _ in range(rnd.randint(1, 6)):  # burst for this org
                o = Outcome(org, rnd.choice([1, 1, 5, 20, 50, 100]))
                outcomes.append(o)
                tasks.append(asyncio.create_task(send(o)))
                await asyncio.sleep(interval)
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

        usage: dict[str, tuple[int, int]] = {}
        for org in sorted({o.org for o in outcomes}):
            u = (await c.get(f"{targets[0]}/v1/quota/{org}/{FEATURE}")).json()
            usage[org] = (int(u["used"]), int(u["limit"]))

    total = sum(stats.codes.values())
    result = audit(outcomes, usage, baseline)
    report = {
        "requests": total,
        "elapsed_s": round(elapsed, 1),
        "achieved_rps": round(total / elapsed),
        "status_codes": dict(stats.codes),
        "rejections_429": dict(stats.rejections),
        "client_retries": stats.retries,
        "replayed_by_idempotency": stats.replayed,
        "instances_seen": dict(stats.instances),
        "end_to_end_ms": {p: pct(stats.e2e_ms, q) for p, q in _PCTS},
        "quota_overhead_ms": {p: pct(stats.quota_ms, q) for p, q in _PCTS},
        **result,
    }
    print(json.dumps(report, indent=2))
    if result["over_limit_orgs"] or result["granted_vs_used_mismatches"]:
        raise SystemExit("AUDIT FAILED")


_PCTS = (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))

if __name__ == "__main__":
    asyncio.run(main())
