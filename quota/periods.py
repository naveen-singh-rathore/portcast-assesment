"""Billing periods: calendar months in UTC.

A period is identified by "YYYY-MM". It starts at 00:00:00 UTC on the 1st
and ends (exclusive) at 00:00:00 UTC on the 1st of the next month. Because
the period id is part of every Redis key, a new month simply starts writing
to fresh keys: there is no reset job.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class Period:
    id: str  # "2026-10"
    start: datetime  # inclusive, UTC
    end: datetime  # exclusive, UTC == next reset

    @property
    def end_ms(self) -> int:
        return int(self.end.timestamp() * 1000)


def period_for(now: datetime) -> Period:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(UTC)
    start = datetime(now.year, now.month, 1, tzinfo=UTC)
    if now.month == 12:
        end = datetime(now.year + 1, 1, 1, tzinfo=UTC)
    else:
        end = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
    return Period(id=f"{now.year:04d}-{now.month:02d}", start=start, end=end)
