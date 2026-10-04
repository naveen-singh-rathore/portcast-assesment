"""Demo API service: one consumer of the quota library under load.

Run several replicas behind a load balancer; they share state only through
Redis. Nothing quota-related lives in process memory.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, cast

import yaml
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.exceptions import RedisError

from quota import (
    DuplicateInFlight,
    IdempotencyKeyReused,
    InvalidInput,
    QuotaClient,
    QuotaExceeded,
    QuotaNotConfigured,
    QuotaUnavailable,
)
from quota.keys import keys_for

log = logging.getLogger("service")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
FAIL_OPEN = set(filter(None, os.environ.get("FAIL_OPEN_FEATURES", "").split(",")))
DOWNSTREAM_FAILURE_RATE = float(os.environ.get("DOWNSTREAM_FAILURE_RATE", "0.05"))
# Work must finish well inside the hold, or a slow request could complete after its
# hold expired and its capacity went to someone else (served but not charged).
RESERVATION_TTL_S = 30.0
DOWNSTREAM_TIMEOUT_S = float(os.environ.get("DOWNSTREAM_TIMEOUT_S", "10"))
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")  # empty: admin endpoints are open (demo)
INSTANCE = os.environ.get("HOSTNAME", "local")

TRACKING = "container-tracking"
SCHEDULES = "sailing-schedule"
MAX_TRACK_BATCH = 1000  # largest batch one /track call may meter


# ---- limit seeding ----------------------------------------------------------
def load_seed(path: str | Path) -> list[tuple[str, str, int]]:
    """Parse quotas.yaml into (org, feature, limit) rows. Fails fast on bad config."""
    cfg = yaml.safe_load(Path(path).read_text())
    defaults: dict[str, int] = cfg["default"]
    orgs = cfg["orgs"]
    overrides: dict[str, dict[str, int]] = orgs.get("overrides") or {}
    rows = []
    for i in range(1, int(orgs["count"]) + 1):
        org = f"org-{i:04d}"
        for feature, limit in {**defaults, **overrides.get(org, {})}.items():
            if not isinstance(limit, int) or limit < 0:
                raise ValueError(f"{path}: limit for {org}/{feature} must be an int >= 0")
            rows.append((org, feature, limit))
    return rows


async def seed_limits(r: Redis, rows: list[tuple[str, str, int]]) -> None:
    """SET NX: seeding never overwrites a limit an admin has changed since."""
    pipe = r.pipeline(transaction=False)
    for org, feature, limit in rows:
        pipe.set(keys_for(org, feature, "_").limit, limit, nx=True)
    await pipe.execute()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if not 0 < DOWNSTREAM_TIMEOUT_S <= RESERVATION_TTL_S - 5:
        raise ValueError(
            f"DOWNSTREAM_TIMEOUT_S must be > 0 and leave 5 s of the {RESERVATION_TTL_S:.0f} s hold"
        )
    # Short socket timeout: a slow Redis must not stall every request; the
    # caller gets a fast 503 (or fail-open) instead.
    r = Redis.from_url(
        REDIS_URL, max_connections=200, socket_timeout=0.05, socket_connect_timeout=0.5
    )
    app.state.redis = r
    app.state.quota = QuotaClient(r, reservation_ttl=timedelta(seconds=RESERVATION_TTL_S))
    if seed_file := os.environ.get("SEED_FILE"):
        await seed_limits(r, load_seed(seed_file))
    yield
    await r.aclose()


app = FastAPI(title="quota-metering demo", lifespan=lifespan)


def _quota() -> QuotaClient:
    return cast(QuotaClient, app.state.quota)


# ---- error mapping: one place decides what callers see ----------------------
@app.exception_handler(QuotaExceeded)
async def _exceeded(_: Request, e: QuotaExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={
            "error": "quota_exceeded",
            "feature": e.feature,
            "requested": e.requested,
            "remaining": e.remaining,
        },
    )


@app.exception_handler(QuotaNotConfigured)
async def _not_configured(_: Request, e: QuotaNotConfigured) -> JSONResponse:
    return JSONResponse(status_code=403, content={"error": "feature_not_enabled"})


@app.exception_handler(DuplicateInFlight)
async def _in_flight(_: Request, e: DuplicateInFlight) -> JSONResponse:
    # The first attempt either finishes (a retry then replays it) or its hold expires.
    return JSONResponse(
        status_code=409, content={"error": "request_in_progress"}, headers={"Retry-After": "1"}
    )


@app.exception_handler(IdempotencyKeyReused)
async def _key_reused(_: Request, e: IdempotencyKeyReused) -> JSONResponse:
    return JSONResponse(status_code=422, content={"error": "idempotency_key_reused"})


@app.exception_handler(QuotaUnavailable)
async def _unavailable(_: Request, e: QuotaUnavailable) -> JSONResponse:
    return JSONResponse(status_code=503, content={"error": "quota_unavailable"})


@app.exception_handler(InvalidInput)
async def _invalid(_: Request, e: InvalidInput) -> JSONResponse:
    # Raised by the library for bad identifiers / units (e.g. an org id containing ":").
    return JSONResponse(status_code=400, content={"error": "invalid_request", "detail": str(e)})


# ---- the consumer: a metered endpoint ---------------------------------------
class TrackRequest(BaseModel):
    org: str
    containers: list[str] = Field(min_length=1, max_length=MAX_TRACK_BATCH)


async def track_downstream(containers: list[str]) -> None:
    """Stand-in for the real work (DB writes, carrier API calls...)."""
    await asyncio.sleep(random.uniform(0.002, 0.010))
    if random.random() < DOWNSTREAM_FAILURE_RATE:
        raise RuntimeError("carrier API timeout")


def fingerprint(payload: dict[str, Any]) -> str:
    """Identifies a request body, so one Idempotency-Key cannot be reused for another."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


async def _run_downstream(containers: list[str]) -> None:
    await asyncio.wait_for(track_downstream(containers), DOWNSTREAM_TIMEOUT_S)


def _timed(body: dict[str, Any], quota_ms: float, status: int = 200) -> JSONResponse:
    headers = {"X-Quota-Ms": f"{quota_ms:.3f}", "X-Instance": INSTANCE}
    return JSONResponse(body, status_code=status, headers=headers)


@app.post("/v1/containers/track")
async def track_containers(
    body: TrackRequest,
    idempotency_key: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    q = _quota()
    units = len(body.containers)
    t0 = time.perf_counter()
    try:
        res = await q.reserve(
            body.org, TRACKING, units, idempotency_key, fingerprint(body.model_dump())
        )
    except QuotaUnavailable:
        if TRACKING not in FAIL_OPEN:
            raise  # fail closed -> 503
        log.warning("quota store down, failing open for %s", TRACKING)
        try:
            await _run_downstream(body.containers)
        except Exception:
            return _timed({"error": "downstream_failed"}, 0.0, status=502)
        return _timed({"tracked": units, "metered": False}, 0.0)
    quota_ms = (time.perf_counter() - t0) * 1000

    if res.status == "REJECTED":
        raise QuotaExceeded(body.org, TRACKING, units, res.remaining)
    if res.status == "DUPLICATE":
        if res.state == "held":
            raise DuplicateInFlight(idempotency_key or "")
        return _timed({"tracked": units, "replayed": True, "remaining": res.remaining}, quota_ms)
    if res.reservation is None:  # OK always carries a reservation
        raise RuntimeError(f"unexpected reserve result: {res}")

    try:
        await _run_downstream(body.containers)  # timeout counts as a failure
    except Exception:
        t1 = time.perf_counter()
        await q.release(res.reservation)  # give the quota back
        quota_ms += (time.perf_counter() - t1) * 1000
        return _timed({"error": "downstream_failed"}, quota_ms, status=502)

    # commit is idempotent and retried once inside the library. If Redis is still
    # unreachable the caller gets 503 and the hold expires; a retry with the same
    # Idempotency-Key replays if the commit did land.
    t1 = time.perf_counter()
    status, _ = await q.commit(res.reservation)
    quota_ms += (time.perf_counter() - t1) * 1000
    if status == "EXPIRED_UNCHARGED":
        # Only possible if the work overran its hold (process pause, clock jump).
        log.error("work for %s finished after its hold expired; not charged", body.org)
        return _timed({"tracked": units, "metered": False}, quota_ms)
    return _timed({"tracked": units, "remaining": res.remaining}, quota_ms)


@app.get("/v1/schedules/search")
async def search_schedules(
    org: str,
    q: str = "",
    idempotency_key: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    """A lookup that cannot fail after the check, so it uses one-shot consume()."""
    res = await _quota().consume(
        org, SCHEDULES, 1, idempotency_key, fingerprint({"org": org, "q": q})
    )
    if res.status == "REJECTED":
        raise QuotaExceeded(org, SCHEDULES, 1, res.remaining)
    return {"results": [], "remaining": res.remaining}


# ---- reporting + admin -------------------------------------------------------
@app.get("/v1/quota/{org}/{feature}")
async def get_usage(org: str, feature: str) -> dict[str, str | int]:
    return (await _quota().usage(org, feature)).as_dict()


class LimitBody(BaseModel):
    limit: int = Field(ge=0)


@app.put("/v1/quota/{org}/{feature}", response_model=None)
async def set_limit(
    org: str,
    feature: str,
    body: LimitBody,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str | int] | JSONResponse:
    if ADMIN_TOKEN and not hmac.compare_digest(authorization or "", f"Bearer {ADMIN_TOKEN}"):
        return JSONResponse(status_code=401, content={"error": "unauthorized"})
    await _quota().set_limit(org, feature, body.limit)
    return (await _quota().usage(org, feature)).as_dict()


@app.get("/healthz")
async def healthz() -> JSONResponse:
    try:
        await app.state.redis.ping()
    except RedisError:
        return JSONResponse({"ok": False, "instance": INSTANCE}, status_code=503)
    return JSONResponse({"ok": True, "instance": INSTANCE})
