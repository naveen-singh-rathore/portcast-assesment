"""Fixed-window burst limit: at most N units per window, on top of the monthly limit."""

from datetime import timedelta

import pytest

from quota import InvalidInput, QuotaExceeded, RateLimited

ORG, FEAT = "acme", "container-tracking"
SECOND = timedelta(seconds=1)


@pytest.fixture
async def burst(quota):
    # The test clock starts at 12:00:00.000, the start of a 1 s window.
    await quota.set_limit(ORG, FEAT, 1000)
    await quota.set_burst_limit(ORG, FEAT, 10, SECOND)
    return quota


async def test_no_burst_limit_by_default(quota):
    await quota.set_limit(ORG, FEAT, 1000)
    assert (await quota.consume(ORG, FEAT, 500)).status == "OK"
    assert (await quota.usage(ORG, FEAT)).burst is None


async def test_window_fills_then_rejects_all_or_nothing(burst, clock):
    assert (await burst.consume(ORG, FEAT, 6)).status == "OK"
    clock.advance(milliseconds=250)
    r = await burst.consume(ORG, FEAT, 5)  # 6 + 5 > 10
    assert r.status == "RATE_LIMITED"
    assert (r.burst_limit, r.retry_after_ms, r.remaining) == (10, 750, 994)
    assert r.reservation is None
    assert (await burst.consume(ORG, FEAT, 4)).status == "OK"  # exactly fills the window
    u = await burst.usage(ORG, FEAT)
    assert u.used == 10  # the rejected batch took nothing from the month either
    assert u.burst is not None and (u.burst.limit, u.burst.used) == (10, 10)


async def test_next_window_starts_fresh(burst, clock):
    assert (await burst.consume(ORG, FEAT, 10)).status == "OK"
    assert (await burst.consume(ORG, FEAT, 1)).status == "RATE_LIMITED"
    clock.advance(milliseconds=999)
    assert (await burst.consume(ORG, FEAT, 1)).status == "RATE_LIMITED"
    clock.advance(milliseconds=1)  # 12:00:01.000: new window
    assert (await burst.consume(ORG, FEAT, 10)).status == "OK"
    assert (await burst.usage(ORG, FEAT)).used == 20


async def test_batch_larger_than_window_never_fits(burst):
    r = await burst.reserve(ORG, FEAT, 11)
    assert r.status == "RATE_LIMITED" and r.retry_after_ms == -1
    with pytest.raises(RateLimited) as e:
        async with burst.hold(ORG, FEAT, 11):
            pass
    assert e.value.retry_after_s is None and e.value.burst_limit == 10


async def test_hold_raises_rate_limited_with_retry_after(burst, clock):
    await burst.consume(ORG, FEAT, 10)
    clock.advance(milliseconds=400)
    with pytest.raises(RateLimited) as e:
        async with burst.hold(ORG, FEAT, 1):
            pass
    assert e.value.retry_after_s == 0.6


async def test_monthly_exhaustion_wins_over_burst(burst):
    await burst.set_limit(ORG, FEAT, 5)
    r = await burst.consume(ORG, FEAT, 8)  # fails both checks
    assert r.status == "REJECTED"  # the month is the lasting reason, so it is reported
    with pytest.raises(QuotaExceeded):
        async with burst.hold(ORG, FEAT, 8):
            pass


async def test_release_does_not_refund_the_window(burst):
    r = await burst.reserve(ORG, FEAT, 8)
    await burst.release(r.reservation)
    u = await burst.usage(ORG, FEAT)
    assert u.remaining == 1000  # the month gets its units back
    assert u.burst is not None and u.burst.used == 8  # the window does not
    assert (await burst.consume(ORG, FEAT, 3)).status == "RATE_LIMITED"


async def test_idempotent_replay_does_not_use_the_window(burst):
    first = await burst.consume(ORG, FEAT, 10, idempotency_key="k1")
    assert first.status == "OK"
    replay = await burst.consume(ORG, FEAT, 10, idempotency_key="k1")
    assert replay.status == "DUPLICATE"  # not RATE_LIMITED, though the window is full


async def test_usage_reports_window_reset(burst, clock):
    clock.advance(milliseconds=300)
    await burst.consume(ORG, FEAT, 4)
    b = (await burst.usage(ORG, FEAT)).burst
    assert b is not None
    assert b.as_dict() == {
        "limit": 10,
        "window_s": 1.0,
        "used": 4,
        "remaining": 6,
        "resets_at": "2026-10-15T12:00:01+00:00",
    }


async def test_changing_window_length_takes_effect(burst, clock):
    await burst.consume(ORG, FEAT, 10)
    await burst.set_burst_limit(ORG, FEAT, 10, timedelta(minutes=1))
    # 12:00:00 is also the start of the 1-minute window, so the count carries over.
    assert (await burst.consume(ORG, FEAT, 1)).status == "RATE_LIMITED"
    clock.advance(seconds=30)
    assert (await burst.consume(ORG, FEAT, 1)).status == "RATE_LIMITED"
    clock.advance(seconds=30)
    assert (await burst.consume(ORG, FEAT, 10)).status == "OK"


async def test_clear_burst_limit(burst):
    await burst.consume(ORG, FEAT, 10)
    await burst.clear_burst_limit(ORG, FEAT)
    assert (await burst.consume(ORG, FEAT, 100)).status == "OK"
    assert (await burst.usage(ORG, FEAT)).burst is None


async def test_zero_burst_blocks_everything(burst):
    await burst.set_burst_limit(ORG, FEAT, 0, SECOND)
    assert (await burst.consume(ORG, FEAT, 1)).status == "RATE_LIMITED"


@pytest.mark.parametrize(("units", "window"), [(-1, SECOND), (10, timedelta(0))])
async def test_invalid_burst_limit_rejected(quota, units, window):
    with pytest.raises(InvalidInput):
        await quota.set_burst_limit(ORG, FEAT, units, window)
