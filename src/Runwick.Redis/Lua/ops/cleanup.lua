-- CleanupBatch/1 (17 §4.1, 22 §7.3): one intent per batch, no receipt, explicit deletions chosen from the retention indexes.
-- ARGS: 1 record class, 2 mode ('batch' or 'fail'), 3 row limit, 4 detail byte budget, 5 error code (fail), 6 default policy hours JSON.
-- The `retention` hash holds `policy` ({rev, hs = hours JSON, at, by}) and `p:<class>` (progress). A batch stops adding work when
-- its intent would write more than 230 distinct keys (22 §4.3); the pass then simply continues in the next batch.
local M = {}
local CLASS = ARGS(1)
local DAY = 86400000
local KEY_BUDGET = 230

local function today() return math.floor(NOW / DAY) end

-- Hinnant's civil_from_days: days since 1970-01-01 as YYYY-MM-DD (Lua has no os.date here).
local function ymd(days)
  local z = days + 719468
  local era = math.floor(z / 146097)
  local doe = z - era * 146097
  local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524) - math.floor(doe / 146096)) / 365)
  local y = yoe + era * 400
  local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
  local mp = math.floor((5 * doy + 2) / 153)
  local d = doy - math.floor((153 * mp + 2) / 5) + 1
  local m = mp < 10 and mp + 3 or mp - 9
  if m <= 2 then y = y + 1 end
  return string.format('%04d-%02d-%02d', y, m, d)
end

local HORIZON = { job_detail_completed = 1, job_detail_failed = 2, job_history = 3, job_events = 3, management_audit = 4, schedule_audit = 4,
  scaling_events = 5, schedule_occurrences = 6, worker_presence = 7 }

local function full() return #WO >= KEY_BUDGET end

-- Members of a score index below an exclusive score bound, oldest first.
local function below(index, bound, limit)
  return redis.call('ZRANGE', P .. index, '-inf', '(' .. U.S(bound), 'BYSCORE', 'LIMIT', 0, limit)
end

-- Deletes members (and their data) of a time-ordered index older than the bound; returns deleted, stoppedForBudget.
local function age_delete(index, bound, rows, remove)
  local deleted = 0
  for _, m in ipairs(below(index, bound, rows)) do
    if full() then return deleted, true end
    remove(m)
    U.zrem(index, m)
    deleted = deleted + 1
  end
  return deleted, false
end

-- Identity-ordered rows (events, audit): the head of the index while rows are older than the cutoff, stopping at the first that is not.
local function head_delete(index, data, field, cutoff, rows)
  local deleted = 0
  for _, id in ipairs(redis.call('ZRANGE', P .. index, 0, rows - 1)) do
    local row = U.hget(data, id)
    local at = row and tonumber(string.match(row, '"' .. field .. '":(%d+)'))
    if at and at >= cutoff then break end
    if full() then return deleted, true end
    U.hset(data, id, nil)
    U.zrem(index, id)
    deleted = deleted + 1
  end
  return deleted, false
end

-- Walks idx:created:s:<state> for the given states from the stored cursor, offering each terminal job to take(id, fields).
local function walk(p, states, rows, take)
  local si, ca, ck = p.cs or 1, p.ca, p.ck
  local out, examined = {}, 0
  while si <= #states do
    local key = P .. 'idx:created:s:' .. states[si]
    local min, from, after, offset = ca and U.S(ca) or '-inf', ca, ck, 0
    while true do
      local page = redis.call('ZRANGE', key, min, '+inf', 'BYSCORE', 'LIMIT', offset, 100, 'WITHSCORES')
      if #page == 0 then break end
      offset = offset + #page / 2
      for i = 1, #page, 2 do
        local id, score = page[i], tonumber(page[i + 1])
        if not (from and score == from and not U.blt(after, id)) then
          local t = take(id)
          if t == 'stop' then return out, false, si, ca, ck end
          examined = examined + 1
          if t then out[#out + 1] = id end
          ca, ck = score, id
          if #out >= rows or examined >= 5000 then return out, false, si, ca, ck end
        end
      end
    end
    si, ca, ck = si + 1, nil, nil
  end
  return out, true, nil, nil, nil
end

local function terminal_before(id, cutoff)
  local f = redis.call('HMGET', P .. 'job:' .. id, 'terminalat', 'updated', 'detailexp')
  local at = tonumber(f[1]) or tonumber(f[2])
  return at and at < cutoff, f[3] ~= false and f[3] ~= nil
end

-- 17 §3.1: payload and error text go; identity, state, history and error codes stay.
local function expire_detail(p, states, cutoff, rows, bytes)
  local total = 0
  local picked = {}
  local out, exhausted, cs, ca, ck = walk(p, states, rows, function(id)
    local old, expired = terminal_before(id, cutoff)
    if not old or expired then return false end
    local size = redis.call('STRLEN', P .. 'pay:' .. id)
    local j = redis.call('HGETALL', P .. 'job:' .. id)
    for i = 1, #j, 2 do
      if string.match(j[i], '^a:[0-9a-f]+:error$') then size = size + #j[i + 1] end
    end
    if #picked > 0 and total + size > bytes then return 'stop' end
    total = total + size
    picked[#picked + 1] = id
    return true
  end)
  for _, id in ipairs(out) do
    U.del('pay:' .. id)
    local j = redis.call('HGETALL', P .. 'job:' .. id)
    for i = 1, #j, 2 do
      if string.match(j[i], '^a:[0-9a-f]+:error$') then
        local ok, e = pcall(cjson.decode, j[i + 1])
        local code = ok and type(e) == 'table' and U.nn(e.code) or nil
        U.hset('job:' .. id, j[i], U.enc({ code = code or NULL, expired = true }))
      end
    end
    U.hset('job:' .. id, 'detailexp', NOWS)
  end
  return #out, exhausted, cs, ca, ck
end

-- 17 §3.2: the job and its cascade, only after detail expiry and with no live binding or permit.
local function live(id)
  local j = U.job(id)
  if not j then return false end
  if j.akey and U.hget('keys:active', j.name .. '\0' .. j.akey) == id then return true end
  for f in pairs(j) do
    if string.sub(f, 1, 2) == 'w:' then
      local w = U.jget('keys:window', string.sub(f, 3))
      if w and w.job == id and w.exp > NOW then return true end
    end
  end
  for _, sid in ipairs(U.list(j.scopes)) do
    if U.hget('permits', sid .. ':' .. id) then return true end
  end
  return false
end

local function purge_jobs(p, cutoff, rows)
  local out, exhausted, cs, ca, ck = walk(p, { 'succeeded', 'failed', 'cancelled' }, rows, function(id)
    local old, expired = terminal_before(id, cutoff)
    return old and expired and not live(id)
  end)
  for _, id in ipairs(out) do
    local f = redis.call('HMGET', P .. 'job:' .. id, 'queue', 'state', 'name', 'created')
    U.del('job:' .. id)
    U.zrem('idx:created', id)
    U.zrem('idx:created:q:' .. f[1], id)
    U.zrem('idx:created:s:' .. f[2], id)
    U.zrem('idx:terminal', id)
    if U.counted(tonumber(f[4]), id) then U.jobtype(f[1], f[3], -1) end
  end
  return #out, exhausted, cs, ca, ck
end

-- 17 §3.4: one scope per batch, keyset by scope ID. Logical time never goes backward, so no later window contains a pruned grant.
local function prune_grants(p, rows)
  local ids = {}
  for _, f in ipairs(redis.call('HKEYS', P .. 'ratescopes')) do
    if #f == 32 and not string.find(f, '[^0-9a-f]') and (not p.ck or U.blt(p.ck, f)) then ids[#ids + 1] = f end
  end
  table.sort(ids, U.blt)
  local id = ids[1]
  if not id then return 0, true end
  local s = U.jget('ratescopes', id)
  local deleted = 0
  if s and U.nn(s.last) then
    local bound = s.last - s.window
    for _, m in ipairs(redis.call('ZRANGE', P .. 'grants:' .. id, '-inf', U.S(bound), 'BYSCORE', 'LIMIT', 0, rows)) do
      if full() then break end
      U.zrem('grants:' .. id, m)
      deleted = deleted + 1
    end
  end
  return deleted, false, nil, nil, id
end

local function policy_hours()
  local row = U.jget('retention', 'policy')
  return cjson.decode(row and row.hs or ARGS(6))
end

local function sweep(p, rows)
  local c = CLASS
  local hi = HORIZON[c]
  local cutoff = NOW
  if hi then
    local h = U.nn(policy_hours()[hi])
    if h == nil then return 0, true end
    cutoff = NOW - h * 3600000
  end
  local function ended(deleted, stopped) return deleted, (not stopped) and deleted < rows end
  if c == 'job_detail_completed' then return expire_detail(p, { 'succeeded', 'cancelled' }, cutoff, rows, tonumber(ARGS(4))) end
  if c == 'job_detail_failed' then return expire_detail(p, { 'failed' }, cutoff, rows, tonumber(ARGS(4))) end
  if c == 'job_history' then return purge_jobs(p, cutoff, rows) end
  if c == 'job_events' then return ended(head_delete('events', 'events:data', 'occurredAt', cutoff, rows)) end
  if c == 'management_audit' then return ended(head_delete('audit', 'audit:data', 'at', cutoff, rows)) end
  if c == 'schedule_audit' then
    return ended(age_delete('sched:ar', cutoff, rows, function(m) U.hset('sched', 'ar:' .. m, nil) end))
  end
  if c == 'scaling_events' then
    return ended(age_delete('scaling', cutoff, rows, function(m) U.hset('scaling:data', m, nil) end))
  end
  if c == 'schedule_occurrences' then
    return ended(age_delete('occ:idx', cutoff, rows, function(m) U.hset('occ:' .. string.sub(m, 1, 32), string.sub(m, 34), nil) end))
  end
  if c == 'worker_presence' then
    return ended(age_delete('workers:exp', cutoff, rows, function(m) U.hset('workers', m, nil) end))
  end
  if c == 'receipts_ownership' or c == 'receipts_durable' then
    local cls = c == 'receipts_ownership' and 'ownership' or 'durable'
    return ended(age_delete('rcpt:exp:' .. cls, NOW, rows, function(m) U.del('rcpt:' .. m) end))
  end
  if c == 'dispatch_keys' then
    -- Expired window keys; active keys are released when their job ends, so no unbound rows exist to age out.
    return ended(age_delete('keys:window:exp', NOW, rows, function(m) U.hset('keys:window', m, nil) end))
  end
  if c == 'rate_grants' then return prune_grants(p, rows) end
  return ended(age_delete('outbox:exp', NOW, rows, function(m) U.hset('outbox', m, nil) end))
end

function M.decide()
  local p = U.jget('retention', 'p:' .. CLASS) or {}
  local td = today()
  if ARGS(2) == 'fail' then
    p.err, p.erra, p.df = string.sub(ARGS(5), 1, 64), NOW, (p.df or 0) + 1
    U.jset('retention', 'p:' .. CLASS, p)
    return U.applied({ status = 'failed', rows = 0, complete = false, caughtUp = false })
  end
  if p.day and p.day < td then
    U.audit('RetentionSummary', { subject = 'runwick:retention', scheme = 'system' }, { targetKind = 'application', targetId = CLASS,
      details = { batches = p.db or 0, day = ymd(p.day), failures = p.df or 0, rows = p.dr or 0 } })
  end
  local rows = math.min(tonumber(ARGS(3)), 100)
  local deleted, complete, cs, ca, ck = sweep(p, rows)
  local caught = complete and (p.pr or 0) + deleted == 0
  if caught then p.cu = NOW end
  p.cs, p.ca, p.ck = cs, ca, ck
  p.ps = (not complete) and (p.ps or NOW) or nil
  p.pr = complete and 0 or (p.pr or 0) + deleted
  p.lb, p.lr, p.tr = NOW, deleted, (p.tr or 0) + deleted
  local same = p.day == td
  p.dr, p.db, p.df, p.day = (same and p.dr or 0) + deleted, (same and p.db or 0) + 1, same and p.df or 0, td
  U.jset('retention', 'p:' .. CLASS, p)
  return U.applied({ status = 'done', rows = deleted, complete = complete, caughtUp = caught })
end
return U.run(M)
