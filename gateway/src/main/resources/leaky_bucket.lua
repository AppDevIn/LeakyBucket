-- Leaky bucket, evaluated atomically inside Redis.
--
-- KEYS[1] = bucket key
-- ARGV[1] = capacity          (max units the bucket holds)
-- ARGV[2] = leak_rate         (units drained per second)
--
-- Returns {allowed, level} where allowed is 1 or 0 and level is rounded up, so
-- capacity - level is the number of whole requests that would still be admitted.

local bucket_key = KEYS[1]
local capacity = tonumber(ARGV[1])
local leak_rate = tonumber(ARGV[2])

-- Use Redis's clock, not the caller's: gateway instances with skewed clocks would
-- otherwise see negative elapsed time and refill each other's buckets.
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local data = redis.call("HMGET", bucket_key, "level", "last_leak")
local level = tonumber(data[1]) or 0
local last_leak = tonumber(data[2]) or now

-- Drain whatever leaked out since we last looked.
-- Clamped so a last_leak in the future (e.g. after failover to a replica whose clock
-- is behind) can never add to the level.
local elapsed_sec = math.max(0, now - last_leak) / 1000.0
local leaked = elapsed_sec * leak_rate
level = math.max(0, level - leaked)

local allowed = 0
if level + 1 <= capacity then
    level = level + 1
    allowed = 1
end

redis.call("HMSET", bucket_key, "level", level, "last_leak", now)
-- Key can be dropped once a full bucket would have drained.
redis.call("EXPIRE", bucket_key, math.ceil(capacity / leak_rate) + 1)

return {allowed, math.ceil(level)}
