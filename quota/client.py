"""Async quota client: the library every service instance embeds.

Integration shape: an in-process library talking directly to Redis. There is
no separate quota microservice, so each quota operation costs exactly one
Redis round trip (one EVALSHA, plus a one-off SCRIPT LOAD per connection).
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from . import scripts
from .errors import (
    DuplicateInFlight,
    HoldExpired,
    IdempotencyKeyReused,
    InvalidInput,
    QuotaError,
    QuotaExceeded,
    QuotaNotConfigured,
    QuotaUnavailable,
)
from .keys import QuotaKeys, keys_for
from .periods import Period, period_for

__all__ = [
    "DuplicateInFlight",
    "HoldExpired",
    "IdempotencyKeyReused",
    "InvalidInput",
    "QuotaClient",
    "QuotaError",
    "QuotaExceeded",
    "QuotaNotConfigured",
    "QuotaUnavailable",
    "Reservation",
    "ReserveResult",
    "Usage",
]

log = logging.getLogger("quota")

# Keep a period's keys around after it ends so last month stays reportable.
RETENTION_AFTER_PERIOD = timedelta(days=35)


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
    status: str  # OK | DUPLICATE | REJECTED
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


def _period_at_ms(ms: int) -> Period:
    return period_for(datetime.fromtimestamp(ms / 1000, UTC))


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class QuotaClient:
    def __init__(
        self,
        redis: Redis,
        *,
        reservation_ttl: timedelta = timedelta(seconds=30),
        idempotency_ttl: timedelta = timedelta(hours=1),
        clock: Callable[[], datetime] | None = None,
        server_time: bool | None = None,
    ) -> None:
        """clock=None (the default) uses the Redis server clock inside every script.

        Passing a clock makes the scripts use it instead (tests move it by hand).
        server_time=True with a clock uses the clock only to guess the month and
        still trusts Redis time: that is how a skewed instance is simulated.
        """
        self._r = redis
        self._ttl_ms = int(reservation_ttl.total_seconds() * 1000)
        self._idem_ttl_s = int(idempotency_ttl.total_seconds())
        self._clock = clock or _utcnow
        self._server_time = (clock is None) if server_time is None else server_time
        self._reserve = redis.register_script(scripts.RESERVE)
        self._commit = redis.register_script(scripts.COMMIT)
        self._release = redis.register_script(scripts.RELEASE)
        self._extend = redis.register_script(scripts.EXTEND)
        self._usage = redis.register_script(scripts.USAGE)

    # -- helpers ----------------------------------------------------------
    def _now(self) -> tuple[str | int, Period]:
        """(time argument for the scripts, the month this instance thinks it is)."""
        now = self._clock()
        return ("" if self._server_time else _ms(now)), period_for(now)

    @staticmethod
    def _keep_until_ms(period: Period) -> int:
        return _ms(period.end + RETENTION_AFTER_PERIOD)

    async def _run(
        self, script: AsyncScript, keys: list[str], args: Sequence[str | int], retries: int = 0
    ) -> list[Any]:
        """One script call. `retries` is only for scripts that are safe to repeat."""
        for attempt in range(retries + 1):
            try:
                result: list[Any] = await script(keys=keys, args=list(args))
                return result
            except RedisError as e:
                if attempt == retries:
                    raise QuotaUnavailable(str(e)) from e
                log.warning("quota script failed, retrying once: %r", e)
        raise AssertionError("unreachable")

    # -- config -----------------------------------------------------------
    async def set_limit(self, org: str, feature: str, limit: int) -> None:
        if limit < 0:
            raise InvalidInput("limit must be >= 0")
        k = keys_for(org, feature, "_")
        try:
            await self._r.set(k.limit, limit)
        except RedisError as e:
            raise QuotaUnavailable(str(e)) from e

    # -- operations -------------------------------------------------------
    async def _acquire(
        self,
        mode: str,
        org: str,
        feature: str,
        units: int,
        idempotency_key: str | None,
        fingerprint: str | None,
    ) -> ReserveResult:
        if units <= 0:
            raise InvalidInput("units must be > 0")
        now_arg, period = self._now()
        rid = uuid.uuid4().hex
        fp = fingerprint if fingerprint is not None else f"units={units}"
        for _ in range(2):  # second pass only if Redis says it is another month
            k = keys_for(org, feature, period.id)
            args: list[str | int] = [
                units,
                now_arg,
                self._ttl_ms,
                rid,
                mode,
                self._keep_until_ms(period),
                self._idem_ttl_s,
                "1" if idempotency_key else "0",
                fp,
                _ms(period.start),
                _ms(period.end),
            ]
            try:
                raw = await self._run(self._reserve, k.all_for_script(idempotency_key or ""), args)
            except QuotaUnavailable as e:
                if mode == "reserve" and isinstance(e.__cause__, RedisTimeoutError):
                    # The script may have run and only the reply was lost. Release the
                    # hold by its id so a retry is not blocked until it expires.
                    # (A refused connection means the script never ran: nothing to undo.)
                    await self._release_quietly(k, rid, period)
                raise
            status = _s(raw[0])
            if status != "WRONG_PERIOD":
                break
            server_ms = int(raw[1])
            log.warning("instance clock disagrees with Redis about the month; using Redis time")
            period = _period_at_ms(server_ms)
        else:
            raise QuotaUnavailable("could not agree on the billing month with Redis")

        remaining, res_id, state, held_units = int(raw[1]), _s(raw[2]), _s(raw[3]), int(raw[4])
        if status == "NO_LIMIT":
            raise QuotaNotConfigured(f"no quota configured for {org}/{feature}")
        if status == "MISMATCH":
            raise IdempotencyKeyReused(idempotency_key or "")
        res = Reservation(org, feature, period.id, res_id, held_units) if res_id else None
        return ReserveResult(status, remaining, res, state)

    async def _release_quietly(self, k: QuotaKeys, rid: str, period: Period) -> None:
        now_arg, _ = self._now()
        # If Redis really is down, the hold (if any) expires on its own.
        with contextlib.suppress(QuotaUnavailable):
            await self._run(
                self._release, k.all_for_script(), [rid, now_arg, self._keep_until_ms(period)]
            )

    async def reserve(
        self,
        org: str,
        feature: str,
        units: int,
        idempotency_key: str | None = None,
        fingerprint: str | None = None,
    ) -> ReserveResult:
        """Hold `units` until commit/release or TTL expiry. All-or-nothing.

        `fingerprint` identifies the request payload for idempotency; reusing a key
        with a different fingerprint raises IdempotencyKeyReused. Default: the units.
        """
        return await self._acquire("reserve", org, feature, units, idempotency_key, fingerprint)

    async def consume(
        self,
        org: str,
        feature: str,
        units: int,
        idempotency_key: str | None = None,
        fingerprint: str | None = None,
    ) -> ReserveResult:
        """Reserve + commit in one atomic call, for work that cannot fail after the check."""
        return await self._acquire("consume", org, feature, units, idempotency_key, fingerprint)

    async def commit(self, r: Reservation, actual_units: int | None = None) -> tuple[str, int]:
        """Turn a hold into usage. actual_units < held returns the remainder.

        Statuses: COMMITTED, LATE_COMMITTED (hold had expired, capacity was still free),
        EXPIRED_UNCHARGED (hold had expired and the capacity was gone), ALREADY_COMMITTED,
        ALREADY_RELEASED, UNKNOWN (not a reservation of this counter). Safe to retry.
        """
        charge = r.units if actual_units is None else actual_units
        if charge < 0:
            raise InvalidInput("actual_units must be >= 0")
        now_arg, _ = self._now()
        # Charged to the period the reservation was made in, even after midnight.
        period = _period_by_id(r.period_id)
        k = keys_for(r.org, r.feature, r.period_id)
        raw = await self._run(
            self._commit,
            k.all_for_script(),
            [r.id, charge, now_arg, self._keep_until_ms(period)],
            retries=1,
        )
        return _s(raw[0]), int(raw[1])

    async def release(self, r: Reservation) -> tuple[str, int]:
        """Give held units back (downstream work failed). Safe to retry."""
        now_arg, _ = self._now()
        period = _period_by_id(r.period_id)
        k = keys_for(r.org, r.feature, r.period_id)
        raw = await self._run(
            self._release,
            k.all_for_script(),
            [r.id, now_arg, self._keep_until_ms(period)],
            retries=1,
        )
        return _s(raw[0]), int(raw[1])

    async def extend(self, r: Reservation) -> str:
        """Renew a live hold for another TTL, for work that may outlast it.

        Returns EXTENDED, NOT_HELD (already committed, released or expired) or UNKNOWN.
        """
        now_arg, _ = self._now()
        period = _period_by_id(r.period_id)
        k = keys_for(r.org, r.feature, r.period_id)
        raw = await self._run(
            self._extend,
            k.all_for_script(),
            [r.id, now_arg, self._ttl_ms, self._keep_until_ms(period)],
            retries=1,
        )
        return _s(raw[0])

    async def usage(self, org: str, feature: str) -> Usage:
        now_arg, period = self._now()
        for _ in range(2):
            k = keys_for(org, feature, period.id)
            raw = await self._run(
                self._usage, k.all_for_script(), [now_arg, _ms(period.start), _ms(period.end)]
            )
            if _s(raw[0]) != "WRONG_PERIOD":
                break
            period = _period_at_ms(int(raw[1]))
        else:
            raise QuotaUnavailable("could not agree on the billing month with Redis")
        limit, used, reserved = int(raw[1]), int(raw[2]), int(raw[3])
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
        self,
        org: str,
        feature: str,
        units: int,
        idempotency_key: str | None = None,
        fingerprint: str | None = None,
    ) -> AsyncIterator[ReserveResult]:
        """Reserve, run the block, commit on success, release on exception.

        async with quota.hold("acme", "container-tracking", 100, key) as r:
            await do_the_work()          # call quota.extend(r.reservation) if it may take > TTL

        Raises HoldExpired after the block if the work outlived its hold and the
        capacity had been given to others, so the caller knows it was not charged.
        """
        res = await self.reserve(org, feature, units, idempotency_key, fingerprint)
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
            try:
                await self.release(res.reservation)
            except QuotaUnavailable as e:
                # Keep the caller's original error; the hold expires on its own.
                log.warning("could not release reservation %s: %r", res.reservation.id, e)
            raise
        status, _ = await self.commit(res.reservation)
        if status == "EXPIRED_UNCHARGED":
            raise HoldExpired(f"reservation {res.reservation.id} expired before commit")
