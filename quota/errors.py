"""Error types raised by the quota library. Callers handle errors by type."""

from __future__ import annotations


class QuotaError(Exception):
    """Base error."""


class InvalidInput(QuotaError, ValueError):
    """Bad identifier, units or limit. Subclasses ValueError for plain-Python callers."""


class QuotaUnavailable(QuotaError):
    """The quota store could not be reached; caller applies its fail policy."""


class QuotaExceeded(QuotaError):
    def __init__(self, org: str, feature: str, requested: int, remaining: int) -> None:
        super().__init__(f"{org}/{feature}: requested {requested}, remaining {remaining}")
        self.org, self.feature = org, feature
        self.requested, self.remaining = requested, remaining


class RateLimited(QuotaError):
    """The request does not fit the current burst window (the monthly quota may still).

    retry_after_s is when the window resets, or None if the batch is larger than a
    whole window allows and can never fit: split it instead of retrying.
    """

    def __init__(
        self, org: str, feature: str, requested: int, burst_limit: int, retry_after_s: float | None
    ) -> None:
        super().__init__(
            f"{org}/{feature}: requested {requested}, burst limit {burst_limit} per window"
        )
        self.org, self.feature = org, feature
        self.requested, self.burst_limit, self.retry_after_s = requested, burst_limit, retry_after_s


class QuotaNotConfigured(QuotaError):
    pass


class DuplicateInFlight(QuotaError):
    """Same idempotency key is still being processed by another request."""


class IdempotencyKeyReused(QuotaError):
    """The idempotency key was already used for a request with a different payload."""


class HoldExpired(QuotaError):
    """The work finished after its hold expired and the capacity was gone: not charged."""
