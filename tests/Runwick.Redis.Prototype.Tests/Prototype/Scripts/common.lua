-- Shared prelude for SP-018 mutation scripts. KEYS[1] is the application's meta hash "rw:{app}:meta";
-- every other key is "rw:{app}:<suffix>" so the whole application lives in one hash slot.
local META = KEYS[1]
local P = string.sub(META, 1, #META - 4)

-- Failure injection: after exactly failAfter writes the script raises an error. Redis does not roll
-- back earlier writes of a failed script, so this reproduces a real interrupted write prefix.
local failAfter = -1
local writes = 0
local function w(...)
  if failAfter >= 0 and writes >= failAfter then
    error({ err = 'INJECTED failure after write ' .. writes })
  end
  writes = writes + 1
  return redis.call(...)
end
local function injectAtEnd()
  if failAfter >= 0 and writes >= failAfter then
    error({ err = 'INJECTED reply lost after write ' .. writes })
  end
end

local function S(n) return string.format('%d', n) end

-- Exact add of +1/-1 on non-negative int64 decimal strings (Lua numbers are doubles: exact only to 2^53).
local MAXI = '9223372036854775807'
local function dadd(s, d)
  s = s or '0'
  local out, i = '', #s
  if d == 1 then
    if s == MAXI then return nil end
    while i >= 1 do
      local c = string.byte(s, i) - 48
      if c < 9 then return string.sub(s, 1, i - 1) .. (c + 1) .. out end
      out = '0' .. out
      i = i - 1
    end
    return '1' .. out
  end
  if s == '0' then return nil end
  while i >= 1 do
    local c = string.byte(s, i) - 48
    if c > 0 then
      local r = (string.gsub(string.sub(s, 1, i - 1) .. (c - 1) .. out, '^0+(%d)', '%1'))
      return r
    end
    out = '9' .. out
    i = i - 1
  end
end

local function hgetall(k)
  local r = redis.call('HGETALL', k)
  local t = {}
  for i = 1, #r, 2 do t[r[i]] = r[i + 1] end
  return t
end

local function flat(t)
  local keys = {}
  for k in pairs(t) do keys[#keys + 1] = k end
  table.sort(keys)
  local a = {}
  for _, k in ipairs(keys) do
    a[#a + 1] = k
    a[#a + 1] = tostring(t[k])
  end
  return a
end

-- Idempotent after-image mutations. Reapplying any prefix or the whole list gives the same state.
local function apply(op)
  local k = P .. op[2]
  if op[1] == 'h' then
    w('DEL', k)
    w('HSET', k, unpack(op[3]))
  elseif op[1] == 'z+' then
    w('ZADD', k, op[3], op[4])
  elseif op[1] == 'z-' then
    w('ZREM', k, op[3])
  else
    error({ err = 'INVARIANT unknown_op' })
  end
end

local function publish(rev)
  w('HSET', META, 'rev', rev, 'guard', '', 'intent', '', 'intentsha', '')
end
