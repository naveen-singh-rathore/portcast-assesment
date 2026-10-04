"""Redis key layout.

Every key for one (org, feature) shares the hash tag {org:feature}, so in
Redis Cluster they land on the same slot and one Lua script can touch them
all atomically.

  q:{org:feat}:limit              STRING  monthly limit (config, not per period)
  q:{org:feat}:burst              STRING  "units/window_ms" burst limit (config, optional)
  q:{org:feat}:win                HASH    start, n: units admitted in the current window
  q:{org:feat}:<period>           HASH    used, reserved
  q:{org:feat}:<period>:res       ZSET    reservation_id -> expires_at_ms
  q:{org:feat}:<period>:resunits  HASH    reservation_id -> units
  q:{org:feat}:<period>:done      HASH    reservation_id -> c | r | x | u
  q:{org:feat}:idem:<key>         STRING  "reservation_id|units|fingerprint" (1 h TTL)

Identifiers may not contain "{", "}" (would break the hash tag), ":" (the
separator inside the tag), "|" (the reservation token separator) or spaces.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import InvalidInput

_FORBIDDEN = frozenset("{}:| ")


def _tag(org: str, feature: str) -> str:
    for part in (org, feature):
        if not part or any(c in _FORBIDDEN for c in part):
            raise InvalidInput(f"invalid identifier: {part!r}")
    return f"q:{{{org}:{feature}}}"


@dataclass(frozen=True)
class QuotaKeys:
    limit: str
    burst: str
    window: str
    state: str
    res: str
    resunits: str
    done: str
    idem_prefix: str

    def idem(self, key: str) -> str:
        return f"{self.idem_prefix}{key}"

    def all_for_script(self, idem_key: str = "") -> list[str]:
        # KEYS order expected by every Lua script. The idempotency slot is always
        # filled (with a placeholder when unused) so KEYS has a fixed length.
        return [
            self.limit,
            self.state,
            self.res,
            self.resunits,
            self.done,
            self.idem(idem_key) if idem_key else self.idem("_none"),
            self.burst,
            self.window,
        ]


def keys_for(org: str, feature: str, period_id: str) -> QuotaKeys:
    t = _tag(org, feature)
    base = f"{t}:{period_id}"
    return QuotaKeys(
        limit=f"{t}:limit",
        burst=f"{t}:burst",
        window=f"{t}:win",
        state=base,
        res=f"{base}:res",
        resunits=f"{base}:resunits",
        done=f"{base}:done",
        idem_prefix=f"{t}:idem:",
    )
