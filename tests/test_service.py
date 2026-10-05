"""HTTP-level tests: status codes, bodies and quota side effects of each path."""

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from quota import QuotaClient
from service import app as svc

ORG = "org-0001"
TRACK = "/v1/containers/track"


@pytest_asyncio.fixture
async def client(redis: Redis, quota: QuotaClient) -> AsyncIterator[httpx.AsyncClient]:
    # ASGITransport does not run the lifespan, so wire the app state directly.
    svc.app.state.redis = redis
    svc.app.state.quota = quota
    await quota.set_limit(ORG, svc.TRACKING, 10)
    await quota.set_limit(ORG, svc.SCHEDULES, 2)
    transport = httpx.ASGITransport(app=svc.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True)
def downstream_ok(monkeypatch):
    async def ok(containers):
        return None

    monkeypatch.setattr(svc, "track_downstream", ok)


def track(n: int) -> dict[str, object]:
    return {"org": ORG, "containers": [f"C{i}" for i in range(n)]}


async def used(c: httpx.AsyncClient) -> int:
    return int((await c.get(f"/v1/quota/{ORG}/{svc.TRACKING}")).json()["used"])


async def test_track_charges_units(client):
    r = await client.post(TRACK, json=track(3))
    assert r.status_code == 200
    assert r.json() == {"tracked": 3, "remaining": 7}
    assert "X-Quota-Ms" in r.headers
    assert await used(client) == 3


async def test_over_limit_is_429_and_charges_nothing(client):
    r = await client.post(TRACK, json=track(11))
    assert r.status_code == 429
    assert r.json() == {
        "error": "quota_exceeded",
        "feature": svc.TRACKING,
        "requested": 11,
        "remaining": 10,
    }
    assert await used(client) == 0


async def test_unconfigured_feature_is_403(client):
    r = await client.post(TRACK, json={"org": "org-9999", "containers": ["C1"]})
    assert r.status_code == 403
    assert r.json() == {"error": "feature_not_enabled"}


async def test_invalid_org_is_400(client):
    r = await client.post(TRACK, json={"org": "bad:org", "containers": ["C1"]})
    assert r.status_code == 400


async def test_batch_above_max_is_422(client):
    r = await client.post(TRACK, json=track(svc.MAX_TRACK_BATCH + 1))
    assert r.status_code == 422


async def test_downstream_failure_is_502_and_quota_returned(client, monkeypatch):
    async def boom(containers):
        raise RuntimeError("carrier API timeout")

    monkeypatch.setattr(svc, "track_downstream", boom)
    r = await client.post(TRACK, json=track(4))
    assert r.status_code == 502
    u = (await client.get(f"/v1/quota/{ORG}/{svc.TRACKING}")).json()
    assert (u["used"], u["reserved"]) == (0, 0)


async def test_retry_with_idempotency_key_is_replayed_not_charged(client):
    h = {"Idempotency-Key": "req-1"}
    first = await client.post(TRACK, json=track(5), headers=h)
    retry = await client.post(TRACK, json=track(5), headers=h)
    assert first.status_code == retry.status_code == 200
    assert retry.json()["replayed"] is True
    assert await used(client) == 5


async def test_retry_while_in_flight_is_409(client, quota):
    # Same key and same body as an attempt that is still held.
    await quota.reserve(ORG, svc.TRACKING, 1, "req-2", svc.fingerprint(track(1)))
    r = await client.post(TRACK, json=track(1), headers={"Idempotency-Key": "req-2"})
    assert r.status_code == 409
    assert r.json() == {"error": "request_in_progress"}
    assert r.headers["Retry-After"] == "1"


async def test_same_key_different_body_is_422(client):
    h = {"Idempotency-Key": "req-3"}
    assert (await client.post(TRACK, json=track(2), headers=h)).status_code == 200
    r = await client.post(TRACK, json=track(5), headers=h)
    assert r.status_code == 422
    assert r.json() == {"error": "idempotency_key_reused"}
    assert await used(client) == 2


async def test_slow_downstream_times_out_and_releases(client, monkeypatch):
    async def slow(containers):
        await asyncio.sleep(1)

    monkeypatch.setattr(svc, "track_downstream", slow)
    monkeypatch.setattr(svc, "DOWNSTREAM_TIMEOUT_S", 0.05)
    r = await client.post(TRACK, json=track(4))
    assert r.status_code == 502
    u = (await client.get(f"/v1/quota/{ORG}/{svc.TRACKING}")).json()
    assert (u["used"], u["reserved"]) == (0, 0)


async def test_commit_after_expired_hold_is_reported_unmetered(client, quota, clock, monkeypatch):
    # The work overruns its hold (e.g. a long GC pause) and the capacity is taken.
    async def overrun(containers):
        clock.advance(seconds=31)
        await quota.consume(ORG, svc.TRACKING, 10)

    monkeypatch.setattr(svc, "track_downstream", overrun)
    r = await client.post(TRACK, json=track(5))
    assert r.status_code == 200
    assert r.json() == {"tracked": 5, "metered": False}
    assert await used(client) == 10  # never above the limit


async def test_admin_token_required_when_set(client, monkeypatch):
    monkeypatch.setattr(svc, "ADMIN_TOKEN", "s3cret")
    url = f"/v1/quota/{ORG}/{svc.TRACKING}"
    assert (await client.put(url, json={"limit": 50})).status_code == 401
    bad = {"Authorization": "Bearer nope"}
    assert (await client.put(url, json={"limit": 50}, headers=bad)).status_code == 401
    ok = {"Authorization": "Bearer s3cret"}
    assert (await client.put(url, json={"limit": 50}, headers=ok)).status_code == 200


async def test_redis_down_fails_closed_with_503(client):
    svc.app.state.quota = QuotaClient(Redis(port=1, socket_connect_timeout=0.1))
    r = await client.post(TRACK, json=track(1))
    assert r.status_code == 503
    assert r.json() == {"error": "quota_unavailable"}


async def test_redis_down_fails_open_when_configured(client, monkeypatch):
    monkeypatch.setattr(svc, "FAIL_OPEN", {svc.TRACKING})
    svc.app.state.quota = QuotaClient(Redis(port=1, socket_connect_timeout=0.1))
    r = await client.post(TRACK, json=track(1))
    assert r.status_code == 200
    assert r.json() == {"tracked": 1, "metered": False}


async def test_schedule_search_consumes_one_unit(client):
    for remaining in (1, 0):
        r = await client.get("/v1/schedules/search", params={"org": ORG})
        assert r.status_code == 200 and r.json()["remaining"] == remaining
    r = await client.get("/v1/schedules/search", params={"org": ORG})
    assert r.status_code == 429


async def test_admin_put_limit_and_usage_report(client):
    r = await client.put(f"/v1/quota/{ORG}/{svc.TRACKING}", json={"limit": 50})
    assert r.status_code == 200
    body = r.json()
    assert (body["limit"], body["used"], body["remaining"]) == (50, 0, 50)
    assert body["period"] == "2026-10"
    assert body["resets_at"] == "2026-11-01T00:00:00+00:00"


async def test_burst_exceeded_is_429_rate_limited_with_retry_after(client, clock):
    r = await client.put(f"/v1/quota/{ORG}/{svc.TRACKING}/burst", json={"units": 4, "window_s": 1})
    assert r.status_code == 200
    assert r.json()["burst"]["limit"] == 4
    assert (await client.post(TRACK, json=track(3))).status_code == 200
    clock.advance(milliseconds=200)
    r = await client.post(TRACK, json=track(2))
    assert r.status_code == 429
    assert r.json() == {
        "error": "rate_limited",
        "feature": svc.TRACKING,
        "requested": 2,
        "burst_limit": 4,
        "retry_after_s": 0.8,
    }
    assert r.headers["Retry-After"] == "1"
    assert await used(client) == 3  # nothing taken from the month
    clock.advance(milliseconds=800)
    assert (await client.post(TRACK, json=track(2))).status_code == 200


async def test_batch_larger_than_burst_window_has_no_retry_after(client):
    await client.put(f"/v1/quota/{ORG}/{svc.TRACKING}/burst", json={"units": 4, "window_s": 1})
    r = await client.post(TRACK, json=track(5))
    assert r.status_code == 429 and r.json()["retry_after_s"] is None
    assert "Retry-After" not in r.headers


async def test_schedule_search_is_burst_limited(client):
    await client.put(f"/v1/quota/{ORG}/{svc.SCHEDULES}/burst", json={"units": 1, "window_s": 60})
    assert (await client.get("/v1/schedules/search", params={"org": ORG})).status_code == 200
    r = await client.get("/v1/schedules/search", params={"org": ORG})
    assert r.status_code == 429 and r.json()["error"] == "rate_limited"


async def test_delete_burst_limit(client):
    url = f"/v1/quota/{ORG}/{svc.TRACKING}/burst"
    await client.put(url, json={"units": 0, "window_s": 1})
    assert (await client.post(TRACK, json=track(1))).status_code == 429
    r = await client.delete(url)
    assert r.status_code == 200 and r.json()["burst"] is None
    assert (await client.post(TRACK, json=track(1))).status_code == 200


async def test_burst_admin_requires_token_when_set(client, monkeypatch):
    monkeypatch.setattr(svc, "ADMIN_TOKEN", "s3cret")
    url = f"/v1/quota/{ORG}/{svc.TRACKING}/burst"
    assert (await client.put(url, json={"units": 5, "window_s": 1})).status_code == 401
    assert (await client.delete(url)).status_code == 401


async def test_admin_rejects_bad_burst(client):
    url = f"/v1/quota/{ORG}/{svc.TRACKING}/burst"
    assert (await client.put(url, json={"units": -1, "window_s": 1})).status_code == 422
    assert (await client.put(url, json={"units": 5, "window_s": 0})).status_code == 422


async def test_admin_rejects_negative_limit(client):
    r = await client.put(f"/v1/quota/{ORG}/{svc.TRACKING}", json={"limit": -1})
    assert r.status_code == 422


async def test_healthz(client):
    r = await client.get("/healthz")
    assert r.status_code == 200 and r.json()["ok"] is True


# ---- seeding ----------------------------------------------------------------
SEED = """
default:
  container-tracking: 5000
  sailing-schedule: 10000
orgs:
  count: 2
  overrides:
    org-0001:
      container-tracking: 500
"""


def test_load_seed_applies_defaults_and_overrides(tmp_path: Path) -> None:
    p = tmp_path / "quotas.yaml"
    p.write_text(SEED)
    assert sorted(svc.load_seed(p)) == [
        ("org-0001", "container-tracking", 500),
        ("org-0001", "sailing-schedule", 10000),
        ("org-0002", "container-tracking", 5000),
        ("org-0002", "sailing-schedule", 10000),
    ]


BURST_SEED = """
burst:
  default:
    container-tracking: {units: 300, window_s: 1}
  overrides:
    org-0002:
      container-tracking: {units: 50, window_s: 0.5}
      sailing-schedule: {units: 10, window_s: 60}
"""


def test_load_burst_seed_applies_defaults_and_overrides(tmp_path: Path) -> None:
    p = tmp_path / "quotas.yaml"
    p.write_text(SEED + BURST_SEED)
    assert sorted(svc.load_burst_seed(p)) == [
        ("org-0001", "container-tracking", 300, 1000),
        ("org-0002", "container-tracking", 50, 500),
        ("org-0002", "sailing-schedule", 10, 60000),
    ]


def test_load_burst_seed_is_optional(tmp_path: Path) -> None:
    p = tmp_path / "quotas.yaml"
    p.write_text(SEED)
    assert svc.load_burst_seed(p) == []


@pytest.mark.parametrize(
    "bad", ["{units: -1, window_s: 1}", "{units: 5, window_s: 0}", "{units: 5}"]
)
def test_load_burst_seed_rejects_bad_values(tmp_path: Path, bad: str) -> None:
    p = tmp_path / "quotas.yaml"
    p.write_text(SEED + f"burst:\n  default:\n    container-tracking: {bad}\n")
    with pytest.raises(ValueError):
        svc.load_burst_seed(p)


async def test_burst_seeding_never_overwrites_admin_changes(redis, quota):
    await quota.set_limit("org-0001", "f", 100)
    await quota.set_burst_limit("org-0001", "f", 7, timedelta(seconds=2))  # admin change
    await svc.seed_bursts(redis, [("org-0001", "f", 300, 1000)])
    b = (await quota.usage("org-0001", "f")).burst
    assert b is not None and (b.limit, b.window) == (7, timedelta(seconds=2))


def test_load_seed_rejects_bad_limit(tmp_path: Path) -> None:
    p = tmp_path / "quotas.yaml"
    p.write_text(SEED.replace("500\n", "-5\n"))
    with pytest.raises(ValueError):
        svc.load_seed(p)


async def test_seeding_never_overwrites_admin_changes(redis, quota):
    await quota.set_limit("org-0001", "container-tracking", 42)  # admin change
    await svc.seed_limits(redis, [("org-0001", "container-tracking", 500), ("org-0002", "f", 7)])
    assert (await quota.usage("org-0001", "container-tracking")).limit == 42
    assert (await quota.usage("org-0002", "f")).limit == 7
