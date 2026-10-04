"""Lua scripts executed atomically inside Redis.

Redis runs one script at a time, start to finish, so every check-and-write
below is indivisible: no other client can observe or modify the counters
between the read and the write. This is the whole concurrency story.

Every script receives the same KEYS layout (see keys.py):
  KEYS[1] limit   KEYS[2] state   KEYS[3] res   KEYS[4] resunits
  KEYS[5] done    KEYS[6] idem
"""

# Shared prelude: key names + lazy expiry of stale reservations.
# Expired reservations are swept by whichever call touches the counter
# next, so there is no background job that can fall behind or die.
_PRELUDE = r"""
local K_LIMIT, K_STATE, K_RES, K_RESUNITS, K_DONE, K_IDEM =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]

local function num(v) return tonumber(v or '0') or 0 end

-- Return held units of reservations whose expiry <= now. Bounded per call
-- so one call never does unbounded work.
local function sweep(now)
  local expired = redis.call('ZRANGEBYSCORE', K_RES, '-inf', now, 'LIMIT', 0, 100)
  for _, id in ipairs(expired) do
    local u = num(redis.call('HGET', K_RESUNITS, id))
    redis.call('HINCRBY', K_STATE, 'reserved', -u)
    redis.call('ZREM', K_RES, id)
    redis.call('HDEL', K_RESUNITS, id)
    redis.call('HSET', K_DONE, id, 'x')
  end
end

local function touch(keep_until_ms)
  for _, k in ipairs({K_STATE, K_RES, K_RESUNITS, K_DONE}) do
    redis.call('PEXPIREAT', k, keep_until_ms)
  end
end

-- 'held' | 'c' (committed) | 'r' (released) | 'x' (expired) | false
local function state_of(id)
  local d = redis.call('HGET', K_DONE, id)
  if d then return d end
  if redis.call('ZSCORE', K_RES, id) then return 'held' end
  return false
end
"""

# ARGV: units, now_ms, ttl_ms, res_id, mode('reserve'|'consume'),
#       keep_until_ms, idem_ttl_s, has_idem('1'|'0')
# Returns {status, remaining, res_id, state}
RESERVE = _PRELUDE + r"""
local units   = tonumber(ARGV[1])
local now     = tonumber(ARGV[2])
local ttl     = tonumber(ARGV[3])
local res_id  = ARGV[4]
local mode    = ARGV[5]
local keep    = tonumber(ARGV[6])
local idemttl = tonumber(ARGV[7])
local hasidem = ARGV[8] == '1'

sweep(now)

local limit = redis.call('GET', K_LIMIT)
if not limit then return {'NO_LIMIT', 0, '', ''} end
limit = tonumber(limit)
local used, reserved = num(redis.call('HGET', K_STATE, 'used')),
                       num(redis.call('HGET', K_STATE, 'reserved'))
local remaining = limit - used - reserved

-- Idempotency: a retry of a request that is still held or already
-- committed gets the original reservation back, never a second charge.
-- Released / expired / unknown -> the earlier attempt left no charge, so
-- this retry is allowed to try again.
if hasidem then
  local prior = redis.call('GET', K_IDEM)
  if prior then
    local st = state_of(prior)
    if st == 'held' or st == 'c' then
      return {'DUPLICATE', math.max(remaining, 0), prior, st}
    end
  end
end

-- The atomic check. Nothing can run between this comparison and the
-- writes below. All-or-nothing: never grant part of a batch.
if units > remaining then
  return {'REJECTED', math.max(remaining, 0), '', ''}
end

if mode == 'reserve' then
  redis.call('HINCRBY', K_STATE, 'reserved', units)
  redis.call('ZADD', K_RES, now + ttl, res_id)
  redis.call('HSET', K_RESUNITS, res_id, units)
else
  redis.call('HINCRBY', K_STATE, 'used', units)
  redis.call('HSET', K_DONE, res_id, 'c')
end
if hasidem then redis.call('SET', K_IDEM, res_id, 'EX', idemttl) end
touch(keep)
return {'OK', remaining - units, res_id, mode == 'reserve' and 'held' or 'c'}
"""

# ARGV: res_id, charge_units, now_ms, keep_until_ms
# Returns {status, charged}
COMMIT = _PRELUDE + r"""
local res_id = ARGV[1]
local charge = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local keep   = tonumber(ARGV[4])

sweep(now)

local d = redis.call('HGET', K_DONE, res_id)
if d == 'c' then return {'ALREADY_COMMITTED', 0} end
if d == 'r' then return {'ALREADY_RELEASED', 0} end

local held = redis.call('HGET', K_RESUNITS, res_id)
if held then
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

-- Reservation already expired (work outlived its TTL). Charge only if the
-- capacity is still free: we prefer under-charging to over-serving.
local limit = num(redis.call('GET', K_LIMIT))
local used, reserved = num(redis.call('HGET', K_STATE, 'used')),
                       num(redis.call('HGET', K_STATE, 'reserved'))
redis.call('HSET', K_DONE, res_id, 'c')
touch(keep)
if used + reserved + charge <= limit then
  redis.call('HINCRBY', K_STATE, 'used', charge)
  return {'LATE_COMMITTED', charge}
end
return {'EXPIRED_UNCHARGED', 0}
"""

# ARGV: res_id, now_ms, keep_until_ms
# Returns {status, released_units}
RELEASE = _PRELUDE + r"""
local res_id = ARGV[1]
local now    = tonumber(ARGV[2])
local keep   = tonumber(ARGV[3])

sweep(now)

local d = redis.call('HGET', K_DONE, res_id)
if d == 'c' then return {'ALREADY_COMMITTED', 0} end
if d == 'r' then return {'ALREADY_RELEASED', 0} end
if d == 'x' then return {'EXPIRED', 0} end

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

# ARGV: now_ms. Returns {limit(-1 if unset), used, reserved}
USAGE = _PRELUDE + r"""
sweep(tonumber(ARGV[1]))
local limit = redis.call('GET', K_LIMIT)
return {limit and tonumber(limit) or -1,
        num(redis.call('HGET', K_STATE, 'used')),
        num(redis.call('HGET', K_STATE, 'reserved'))}
"""
