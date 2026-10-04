from datetime import UTC, datetime, timedelta, timezone

import pytest

from quota.periods import period_for


def test_mid_month() -> None:
    p = period_for(datetime(2026, 10, 15, 12, tzinfo=UTC))
    assert p.id == "2026-10"
    assert p.start == datetime(2026, 10, 1, tzinfo=UTC)
    assert p.end == datetime(2026, 11, 1, tzinfo=UTC)


def test_december_rolls_year() -> None:
    p = period_for(datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC))
    assert p.id == "2026-12"
    assert p.end == datetime(2027, 1, 1, tzinfo=UTC)


def test_exact_boundary_belongs_to_new_month() -> None:
    assert period_for(datetime(2026, 11, 1, 0, 0, tzinfo=UTC)).id == "2026-11"


def test_non_utc_input_is_normalised() -> None:
    # 1 Nov 03:00 in IST (UTC+5:30) is still 31 Oct in UTC
    ist = timezone(timedelta(hours=5, minutes=30))
    assert period_for(datetime(2026, 11, 1, 3, 0, tzinfo=ist)).id == "2026-10"


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValueError):
        period_for(datetime(2026, 10, 1))


def test_end_ms_is_epoch_millis_of_reset() -> None:
    p = period_for(datetime(2026, 10, 15, tzinfo=UTC))
    assert p.end_ms == int(datetime(2026, 11, 1, tzinfo=UTC).timestamp() * 1000)
