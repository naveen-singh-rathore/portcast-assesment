import asyncio

import pytest

from quota import DuplicateInFlight, QuotaExceeded, QuotaNotConfigured

ORG, FEAT = "acme", "container-tracking"


async def test_unconfigured_feature_is_denied(quota):
    with pytest.raises(QuotaNotConfigured):
        await quota.consume(ORG, FEAT, 1)


async def test_consume_and_usage(quota):
    await quota.set_limit(ORG, FEAT, 500)
    r = await quota.consume(ORG, FEAT, 100)
    assert r.status == "OK" and r.remaining == 400
    u = await quota.usage(ORG, FEAT)
    assert (u.limit, u.used, u.reserved, u.remaining) == (500, 100, 0, 400)
    assert u.resets_at.isoformat() == "2026-11-01T00:00:00+00:00"


async def test_batch_is_all_or_nothing(quota):
    await quota.set_limit(ORG, FEAT, 500)
    await quota.consume(ORG, FEAT, 450)
    r = await quota.consume(ORG, FEAT, 100)  # needs 100, only 50 left
    assert r.status == "REJECTED" and r.remaining == 50
    assert (await quota.usage(ORG, FEAT)).used == 450  # nothing partially taken


async def test_exact_fill_reaches_zero(quota):
    await quota.set_limit(ORG, FEAT, 500)
    assert (await quota.consume(ORG, FEAT, 500)).remaining == 0
    assert (await quota.consume(ORG, FEAT, 1)).status == "REJECTED"


async def test_reserve_commit_and_partial_commit(quota):
    await quota.set_limit(ORG, FEAT, 500)
    r = await quota.reserve(ORG, FEAT, 100)
    u = await quota.usage(ORG, FEAT)
    assert (u.used, u.reserved, u.remaining) == (0, 100, 400)
    assert await quota.commit(r.reservation, actual_units=97) == ("COMMITTED", 97)
    u = await quota.usage(ORG, FEAT)
    assert (u.used, u.reserved, u.remaining) == (97, 0, 403)


async def test_release_returns_units_and_is_idempotent(quota):
    await quota.set_limit(ORG, FEAT, 500)
    r = await quota.reserve(ORG, FEAT, 200)
    assert await quota.release(r.reservation) == ("RELEASED", 200)
    assert await quota.release(r.reservation) == ("ALREADY_RELEASED", 0)
    assert await quota.commit(r.reservation) == ("ALREADY_RELEASED", 0)
    assert (await quota.usage(ORG, FEAT)).remaining == 500


async def test_commit_is_idempotent(quota):
    await quota.set_limit(ORG, FEAT, 500)
    r = await quota.reserve(ORG, FEAT, 50)
    assert await quota.commit(r.reservation) == ("COMMITTED", 50)
    assert await quota.commit(r.reservation) == ("ALREADY_COMMITTED", 0)
    assert (await quota.usage(ORG, FEAT)).used == 50


async def test_hold_commits_on_success_releases_on_failure(quota):
    await quota.set_limit(ORG, FEAT, 500)
    async with quota.hold(ORG, FEAT, 100):
        pass
    with pytest.raises(RuntimeError):
        async with quota.hold(ORG, FEAT, 100):
            raise RuntimeError("downstream failed")
    u = await quota.usage(ORG, FEAT)
    assert (u.used, u.reserved) == (100, 0)


async def test_hold_raises_when_exceeded(quota):
    await quota.set_limit(ORG, FEAT, 10)
    with pytest.raises(QuotaExceeded) as e:
        async with quota.hold(ORG, FEAT, 11):
            pass
    assert e.value.remaining == 10


# ---------- idempotency ----------


async def test_retry_with_same_key_is_not_double_charged(quota):
    await quota.set_limit(ORG, FEAT, 500)
    first = await quota.consume(ORG, FEAT, 100, idempotency_key="req-1")
    retry = await quota.consume(ORG, FEAT, 100, idempotency_key="req-1")
    assert first.status == "OK" and retry.status == "DUPLICATE"
    assert retry.reservation.id == first.reservation.id
    assert (await quota.usage(ORG, FEAT)).used == 100


async def test_concurrent_duplicates_charge_once(quota):
    await quota.set_limit(ORG, FEAT, 500)
    results = await asyncio.gather(
        *[quota.consume(ORG, FEAT, 100, idempotency_key="burst") for _ in range(50)]
    )
    assert sum(r.status == "OK" for r in results) == 1
    assert sum(r.status == "DUPLICATE" for r in results) == 49
    assert (await quota.usage(ORG, FEAT)).used == 100


async def test_retry_while_in_flight_is_flagged(quota):
    await quota.set_limit(ORG, FEAT, 500)
    await quota.reserve(ORG, FEAT, 100, idempotency_key="k")
    with pytest.raises(DuplicateInFlight):
        async with quota.hold(ORG, FEAT, 100, idempotency_key="k"):
            pass


async def test_retry_after_failure_is_allowed(quota):
    await quota.set_limit(ORG, FEAT, 500)
    with pytest.raises(RuntimeError):
        async with quota.hold(ORG, FEAT, 100, idempotency_key="k"):
            raise RuntimeError("boom")
    async with quota.hold(ORG, FEAT, 100, idempotency_key="k") as r:
        assert r.status == "OK"
    assert (await quota.usage(ORG, FEAT)).used == 100


# ---------- expiry (instance died mid-request) ----------


async def test_abandoned_reservation_expires(quota, clock):
    await quota.set_limit(ORG, FEAT, 500)
    await quota.reserve(ORG, FEAT, 300)  # instance "crashes": never commits
    assert (await quota.usage(ORG, FEAT)).remaining == 200
    clock.advance(seconds=31)
    u = await quota.usage(ORG, FEAT)
    assert (u.used, u.reserved, u.remaining) == (0, 0, 500)


async def test_late_commit_charges_only_if_capacity_free(quota, clock):
    await quota.set_limit(ORG, FEAT, 500)
    slow = await quota.reserve(ORG, FEAT, 300)
    clock.advance(seconds=31)
    assert await quota.commit(slow.reservation) == ("LATE_COMMITTED", 300)

    slow2 = await quota.reserve(ORG, FEAT, 200)
    clock.advance(seconds=31)  # expires, capacity taken by others
    await quota.consume(ORG, FEAT, 200)
    assert await quota.commit(slow2.reservation) == ("EXPIRED_UNCHARGED", 0)
    assert (await quota.usage(ORG, FEAT)).used == 500  # never over 500


# ---------- monthly reset ----------


async def test_month_rollover_resets(quota, clock):
    await quota.set_limit(ORG, FEAT, 500)
    clock.now = clock.now.replace(day=31, hour=23, minute=59, second=59)
    await quota.consume(ORG, FEAT, 500)
    assert (await quota.usage(ORG, FEAT)).remaining == 0
    clock.advance(seconds=2)  # 1 Nov 00:00:01 UTC
    u = await quota.usage(ORG, FEAT)
    assert u.period == "2026-11" and u.used == 0 and u.remaining == 500
    assert u.resets_at.isoformat() == "2026-12-01T00:00:00+00:00"


async def test_reservation_across_boundary_charges_its_own_period(quota, clock, redis):
    await quota.set_limit(ORG, FEAT, 500)
    clock.now = clock.now.replace(day=31, hour=23, minute=59, second=50)
    r = await quota.reserve(ORG, FEAT, 100)
    clock.advance(seconds=15)  # now November, reservation still live
    assert await quota.commit(r.reservation) == ("COMMITTED", 100)
    assert (await quota.usage(ORG, FEAT)).used == 0  # November untouched
    assert int(await redis.hget("q:{acme:container-tracking}:2026-10", "used")) == 100


async def test_lowering_limit_below_usage_never_goes_negative(quota):
    await quota.set_limit(ORG, FEAT, 500)
    await quota.consume(ORG, FEAT, 400)
    await quota.set_limit(ORG, FEAT, 300)
    u = await quota.usage(ORG, FEAT)
    assert u.remaining == 0
    assert (await quota.consume(ORG, FEAT, 1)).status == "REJECTED"
