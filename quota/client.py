"""Async quota client: the library every service instance embeds.

Integration shape: an in-process library talking directly to Redis. There is
no separate quota microservice, so each quota operation costs exactly one
Redis round trip (one EVALSHA).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import RedisError

from . import scripts
from .keys import keys_for
from .periods import Period, period_for

# Keep a period's keys around after it ends so last month stays reportable.
RETENTION_AFTER_PERIOD = timedelta(days=35)


class QuotaError(Exception):
    """Base error."""


class QuotaUnavailable(QuotaError):
    """The quota store could not be reached; caller applies its fail policy."""


class QuotaExceeded(QuotaError):
    def __init__(self, org: str, feature: str, requested: int, remaining: int) -> None:
        super().__init__(f"{org}/{feature}: requested {requested}, remaining {remaining}")
        self.org, self.feature = org, feature
        self.requested, self.remaining = requested, remaining


class QuotaNotConfigured(QuotaError):
    pass


class DuplicateInFlight(QuotaError):
    """Same idempotency key is still being processed by another request."""


@dataclass(frozen=True)
class Reservation:
    org: str
    feature: str
    period_id: str
    id: str
    units: int

    def token(self) -> str:
        return "|".join([self.org, self.feature, self.period_id, self.id, str(self.units)])

    @classmethod
    def from_token(cls, token: str) -> Reservation:
        org, feature, period_id, rid, units = token.split("|")
        return cls(org, feature, period_id, rid, int(units))


@dataclass(frozen=True)
class ReserveResult:
    status: str  # OK | DUPLICATE | REJECTED | NO_LIMIT
    remaining: int
    reservation: Reservation | None
    state: str  # held | c | '' (for DUPLICATE: state of the original)

    @property
    def granted(self) -> bool:
        return self.status in ("OK", "DUPLICATE")


@dataclass(frozen=True)
class Usage:
    org: str
    feature: str
    period: str
    limit: int
    used: int
    reserved: int
    remaining: int
    period_start: datetime
    resets_at: datetime

    def as_dict(self) -> dict[str, str | int]:
        return {
            "org": self.org,
            "feature": self.feature,
            "period": self.period,
            "limit": self.limit,
            "used": self.used,
            "reserved": self.reserved,
            "remaining": self.remaining,
            "period_start": self.period_start.isoformat(),
            "resets_at": self.resets_at.isoformat(),
        }


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _s(v: object) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _period_by_id(period_id: str) -> Period:
    year, month = (int(p) for p in period_id.split("-"))
    return period_for(datetime(year, month, 1, tzinfo=UTC))


class QuotaClient:
    def __init__(
        self,
        redis: Redis,
        *,
        reservation_ttl: timedelta = timedelta(seconds=30),
        idempotency_ttl: timedelta = timedelta(hours=1),
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._r = redis
        self._ttl_ms = int(reservation_ttl.total_seconds() * 1000)
        self._idem_ttl_s = int(idempotency_ttl.total_seconds())
        self._clock = clock
        self._reserve = redis.register_script(scripts.RESERVE)
        self._commit = redis.register_script(scripts.COMMIT)
        self._release = redis.register_script(scripts.RELEASE)
        self._usage = redis.register_script(scripts.USAGE)

    # -- helpers ----------------------------------------------------------
    def _now(self) -> tuple[int, Period]:
        now = self._clock()
        return int(now.timestamp() * 1000), period_for(now)

    @staticmethod
    def _keep_until_ms(period: Period) -> int:
        return int((period.end + RETENTION_AFTER_PERIOD).timestamp() * 1000)

    async def _run(
        self, script: AsyncScript, keys: list[str], args: Sequence[str | int]
    ) -> list[Any]:
        try:
            result: list[Any] = await script(keys=keys, args=list(args))
        except RedisError as e:
            raise QuotaUnavailable(str(e)) from e
        return result

    # -- config -----------------------------------------------------------
    async def set_limit(self, org: str, feature: str, limit: int) -> None:
        if limit < 0:
            raise ValueError("limit must be >= 0")
        k = keys_for(org, feature, "_")
        try:
            await self._r.set(k.limit, limit)
        except RedisError as e:
            raise QuotaUnavailable(str(e)) from e

    # -- operations -------------------------------------------------------
    async def _acquire(
        self, mode: str, org: str, feature: str, units: int, idempotency_key: str | None
    ) -> ReserveResult:
        if units <= 0:
            raise ValueError("units must be > 0")
        now_ms, period = self._now()
        k = keys_for(org, feature, period.id)
        rid = uuid.uuid4().hex
        raw = await self._run(
            self._reserve,
            k.all_for_script(idempotency_key or ""),
            [
                units,
                now_ms,
                self._ttl_ms,
                rid,
                mode,
                self._keep_until_ms(period),
                self._idem_ttl_s,
                "1" if idempotency_key else "0",
            ],
        )
        status, remaining, res_id, state = _s(raw[0]), int(raw[1]), _s(raw[2]), _s(raw[3])
        if status == "NO_LIMIT":
            raise QuotaNotConfigured(f"no quota configured for {org}/{feature}")
        res = Reservation(org, feature, period.id, res_id, units) if res_id else None
        return ReserveResult(status, remaining, res, state)

    async def reserve(
        self, org: str, feature: str, units: int, idempotency_key: str | None = None
    ) -> ReserveResult:
        """Hold `units` until commit/release or TTL expiry. All-or-nothing."""
        return await self._acquire("reserve", org, feature, units, idempotency_key)

    async def consume(
        self, org: str, feature: str, units: int, idempotency_key: str | None = None
    ) -> ReserveResult:
        """Reserve + commit in one atomic call, for work that cannot fail after the check."""
        return await self._acquire("consume", org, feature, units, idempotency_key)

    async def commit(self, r: Reservation, actual_units: int | None = None) -> tuple[str, int]:
        """Turn a hold into usage. actual_units < held returns the remainder."""
        charge = r.units if actual_units is None else actual_units
        if charge < 0:
            raise ValueError("actual_units must be >= 0")
        now_ms, _ = self._now()
        # Charged to the period the reservation was made in, even after midnight.
        period = _period_by_id(r.period_id)
        k = keys_for(r.org, r.feature, r.period_id)
        raw = await self._run(
            self._commit, k.all_for_script(), [r.id, charge, now_ms, self._keep_until_ms(period)]
        )
        return _s(raw[0]), int(raw[1])

    async def release(self, r: Reservation) -> tuple[str, int]:
        """Give held units back (downstream work failed)."""
        now_ms, _ = self._now()
        period = _period_by_id(r.period_id)
        k = keys_for(r.org, r.feature, r.period_id)
        raw = await self._run(
            self._release, k.all_for_script(), [r.id, now_ms, self._keep_until_ms(period)]
        )
        return _s(raw[0]), int(raw[1])

    async def usage(self, org: str, feature: str) -> Usage:
        now_ms, period = self._now()
        k = keys_for(org, feature, period.id)
        raw = await self._run(self._usage, k.all_for_script(), [now_ms])
        limit, used, reserved = int(raw[0]), int(raw[1]), int(raw[2])
        if limit < 0:
            raise QuotaNotConfigured(f"no quota configured for {org}/{feature}")
        return Usage(
            org,
            feature,
            period.id,
            limit,
            used,
            reserved,
            max(limit - used - reserved, 0),
            period.start,
            period.end,
        )

    @asynccontextmanager
    async def hold(
        self, org: str, feature: str, units: int, idempotency_key: str | None = None
    ) -> AsyncIterator[ReserveResult]:
        """Reserve, run the block, commit on success, release on exception.

        async with quota.hold("acme", "container-tracking", 100, key) as r:
            await do_the_work()
        """
        res = await self.reserve(org, feature, units, idempotency_key)
        if res.status == "REJECTED":
            raise QuotaExceeded(org, feature, units, res.remaining)
        if res.status == "DUPLICATE":
            if res.state == "held":
                raise DuplicateInFlight(idempotency_key or "")
            yield res  # already committed earlier: caller replays its result
            return
        if res.reservation is None:  # OK always carries a reservation
            raise RuntimeError(f"unexpected reserve result: {res}")
        try:
            yield res
        except BaseException:
            await self.release(res.reservation)
            raise
        else:
            await self.commit(res.reservation)
