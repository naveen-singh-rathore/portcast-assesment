"""Lua scripts executed atomically inside Redis.

Redis runs one script at a time, start to finish, so every check-and-write
below is indivisible: no other client can observe or modify the counters
between the read and the write. This is the whole concurrency story.

Every script receives the same KEYS layout (see keys.py):
  KEYS[1] limit   KEYS[2] state   KEYS[3] res   KEYS[4] resunits
  KEYS[5] done    KEYS[6] idem

Time comes from the Redis server clock (TIME) unless the caller passes one
(tests do), so hold expiry and the billing month never depend on how well an
instance's clock is synchronised.

done markers: c committed, r released, x expired (units still recorded in
resunits so a late commit can be capped), u expired and could not be charged.
"""

# Shared prelude: key names, server time and lazy expiry of stale reservations.
# Expired reservations are swept by whichever call touches the counter
# next, so there is no background job that can fall behind or die.
_PRELUDE = r"""
local K_LIMIT, K_STATE, K_RES, K_RESUNITS, K_DONE, K_IDEM =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]

local function num(v) return tonumber(v or '0') or 0 end

-- Milliseconds from the Redis server clock, or the caller's value if given.
local function now_ms(arg)
  if arg == nil or arg == '' then
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  end
  return tonumber(arg)
end

-- Expire one hold: return its units to remaining. Its unit count stays in
-- resunits so a late commit can still be capped at what was held.
local function expire(id)
  local u = num(redis.call('HGET', K_RESUNITS, id))
  redis.call('HINCRBY', K_STATE, 'reserved', -u)
  redis.call('ZREM', K_RES, id)
  redis.call('HSET', K_DONE, id, 'x')
end

-- Return held units of reservations whose expiry <= now. Bounded per call
-- so one call never does unbounded work.
local function sweep(now)
  local expired = redis.call('ZRANGEBYSCORE', K_RES, '-inf', now, 'LIMIT', 0, 100)
  for _, id in ipairs(expired) do expire(id) end
end

local function touch(keep_until_ms)
  for _, k in ipairs({K_STATE, K_RES, K_RESUNITS, K_DONE}) do
    redis.call('PEXPIREAT', k, keep_until_ms)
  end
end

-- 'held' | 'c' | 'r' | 'x' | 'u' | false
local function state_of(id)
  local d = redis.call('HGET', K_DONE, id)
  if d then return d end
  if redis.call('ZSCORE', K_RES, id) then return 'held' end
  return false
end

-- Idempotency value: "reservation_id|units|fingerprint"
local function parse_idem(v)
  local id, u, fp = string.match(v, '^([^|]*)|([^|]*)|(.*)$')
  return id, tonumber(u) or 0, fp or ''
end
"""

# ARGV: units, now_ms ('' = server time), ttl_ms, res_id, mode('reserve'|'consume'),
#       keep_until_ms, idem_ttl_s, has_idem('1'|'0'), fingerprint,
#       period_start_ms, period_end_ms
# Returns {status, remaining | server_now, res_id, state, units}
RESERVE = _PRELUDE + r"""
local units   = tonumber(ARGV[1])
local now     = now_ms(ARGV[2])
local ttl     = tonumber(ARGV[3])
local res_id  = ARGV[4]
local mode    = ARGV[5]
local keep    = tonumber(ARGV[6])
local idemttl = tonumber(ARGV[7])
local hasidem = ARGV[8] == '1'
local fp      = ARGV[9]
local pstart, pend = tonumber(ARGV[10]), tonumber(ARGV[11])

-- The caller picked these keys from its own clock. If the server clock says
-- another month, write nothing and tell the caller which time it is.
if now < pstart or now >= pend then return {'WRONG_PERIOD', now, '', '', 0} end

sweep(now)

local limit = redis.call('GET', K_LIMIT)
if not limit then return {'NO_LIMIT', 0, '', '', 0} end
limit = tonumber(limit)
local used, reserved = num(redis.call('HGET', K_STATE, 'used')),
                       num(redis.call('HGET', K_STATE, 'reserved'))
local remaining = limit - used - reserved

-- Idempotency: a retry of a request that is still held or already
-- committed gets the original reservation back, never a second charge.
-- The same key with a different payload is refused. Released / expired /
-- unknown -> the earlier attempt left no charge, so this retry may try again.
if hasidem then
  local prior = redis.call('GET', K_IDEM)
  if prior then
    local pid, punits, pfp = parse_idem(prior)
    local st = state_of(pid)
    if st == 'held' or st == 'c' then
      local status = (pfp == fp) and 'DUPLICATE' or 'MISMATCH'
      return {status, math.max(remaining, 0), pid, st, punits}
    end
  end
end

-- The atomic check. Nothing can run between this comparison and the
-- writes below. All-or-nothing: never grant part of a batch.
if units > remaining then
  return {'REJECTED', math.max(remaining, 0), '', '', 0}
end

if mode == 'reserve' then
  redis.call('HINCRBY', K_STATE, 'reserved', units)
  redis.call('ZADD', K_RES, now + ttl, res_id)
  redis.call('HSET', K_RESUNITS, res_id, units)
else
  redis.call('HINCRBY', K_STATE, 'used', units)
  redis.call('HSET', K_DONE, res_id, 'c')
end
if hasidem then
  redis.call('SET', K_IDEM, res_id .. '|' .. units .. '|' .. fp, 'EX', idemttl)
end
touch(keep)
return {'OK', remaining - units, res_id, mode == 'reserve' and 'held' or 'c', units}
"""

# ARGV: res_id, charge_units, now_ms ('' = server time), keep_until_ms
# Returns {status, charged}
COMMIT = _PRELUDE + r"""
local res_id = ARGV[1]
local charge = tonumber(ARGV[2])
local now    = now_ms(ARGV[3])
local keep   = tonumber(ARGV[4])

sweep(now)

local d = redis.call('HGET', K_DONE, res_id)
if d == 'c' then return {'ALREADY_COMMITTED', 0} end
if d == 'r' then return {'ALREADY_RELEASED', 0} end
if d == 'u' then return {'EXPIRED_UNCHARGED', 0} end

local held = redis.call('HGET', K_RESUNITS, res_id)
if not d and held then
  held = tonumber(held)
  if charge > held then charge = held end   -- never charge more than held
  redis.call('HINCRBY', K_STATE, 'reserved', -held)
  redis.call('HINCRBY', K_STATE, 'used', charge)  -- unused remainder returns
  redis.call('ZREM', K_RES, res_id)
  redis.call('HDEL', K_RESUNITS, res_id)
  redis.call('HSET', K_DONE, res_id, 'c')
  touch(keep)
  return {'COMMITTED', charge}
end

-- Never seen: not a reservation of this counter. Charge nothing.
if d ~= 'x' then return {'UNKNOWN', 0} end

-- Reservation expired (work outlived its TTL). Charge only if the capacity
-- is still free: we prefer under-charging to over-serving.
local cap = num(held)
if charge > cap then charge = cap end
redis.call('HDEL', K_RESUNITS, res_id)
local limit = num(redis.call('GET', K_LIMIT))
local used, reserved = num(redis.call('HGET', K_STATE, 'used')),
                       num(redis.call('HGET', K_STATE, 'reserved'))
touch(keep)
if used + reserved + charge <= limit then
  redis.call('HINCRBY', K_STATE, 'used', charge)
  redis.call('HSET', K_DONE, res_id, 'c')
  return {'LATE_COMMITTED', charge}
end
redis.call('HSET', K_DONE, res_id, 'u')
return {'EXPIRED_UNCHARGED', 0}
"""

# ARGV: res_id, now_ms ('' = server time), keep_until_ms
# Returns {status, released_units}
RELEASE = _PRELUDE + r"""
local res_id = ARGV[1]
local now    = now_ms(ARGV[2])
local keep   = tonumber(ARGV[3])

sweep(now)

local d = redis.call('HGET', K_DONE, res_id)
if d == 'c' then return {'ALREADY_COMMITTED', 0} end
if d == 'r' then return {'ALREADY_RELEASED', 0} end
if d == 'x' or d == 'u' then
  redis.call('HDEL', K_RESUNITS, res_id)
  return {'EXPIRED', 0}
end

local held = redis.call('HGET', K_RESUNITS, res_id)
if not held then return {'UNKNOWN', 0} end
held = tonumber(held)
redis.call('HINCRBY', K_STATE, 'reserved', -held)
redis.call('ZREM', K_RES, res_id)
redis.call('HDEL', K_RESUNITS, res_id)
redis.call('HSET', K_DONE, res_id, 'r')
touch(keep)
return {'RELEASED', held}
"""

# ARGV: res_id, now_ms ('' = server time), ttl_ms, keep_until_ms
# Returns {status, done_marker}: EXTENDED | NOT_HELD (+ marker) | UNKNOWN
EXTEND = _PRELUDE + r"""
local res_id = ARGV[1]
local now    = now_ms(ARGV[2])
local ttl    = tonumber(ARGV[3])
local keep   = tonumber(ARGV[4])

sweep(now)

local d = redis.call('HGET', K_DONE, res_id)
if d then return {'NOT_HELD', d} end
local score = redis.call('ZSCORE', K_RES, res_id)
if not score then return {'UNKNOWN', ''} end
if tonumber(score) <= now then   -- expired but not yet swept (sweep is bounded)
  expire(res_id)
  return {'NOT_HELD', 'x'}
end
redis.call('ZADD', K_RES, 'XX', now + ttl, res_id)
touch(keep)
return {'EXTENDED', ''}
"""

# ARGV: now_ms ('' = server time), period_start_ms, period_end_ms
# Returns {'OK', limit(-1 if unset), used, reserved} or {'WRONG_PERIOD', server_now, 0, 0}
USAGE = _PRELUDE + r"""
local now = now_ms(ARGV[1])
if now < tonumber(ARGV[2]) or now >= tonumber(ARGV[3]) then
  return {'WRONG_PERIOD', now, 0, 0}
end
sweep(now)
local limit = redis.call('GET', K_LIMIT)
return {'OK', limit and tonumber(limit) or -1,
        num(redis.call('HGET', K_STATE, 'used')),
        num(redis.call('HGET', K_STATE, 'reserved'))}
"""
