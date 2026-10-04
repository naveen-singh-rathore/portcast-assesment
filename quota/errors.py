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


class QuotaNotConfigured(QuotaError):
    pass


class DuplicateInFlight(QuotaError):
    """Same idempotency key is still being processed by another request."""


class IdempotencyKeyReused(QuotaError):
    """The idempotency key was already used for a request with a different payload."""


class HoldExpired(QuotaError):
    """The work finished after its hold expired and the capacity was gone: not charged."""
