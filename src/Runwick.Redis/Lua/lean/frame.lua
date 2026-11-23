-- Lean worker paths (22 §4.1, REDIS-22): claim, finish and renew written by hand for their usual case. The script holds the whole
-- generic frame as GENERIC() (prelude, jobs.lua and the operation body, defined but not run); the lean path only reads until it
-- knows it can decide the command itself, and calls GENERIC() whenever it cannot: a gate or admission refusal, a receipt to replay,
-- an expired or far deadline, a job with concurrency or rate scopes, a cancel request, an exhausted budget, a refused ownership,
-- duplicate items or a counter at its limit. Nothing is written before that choice, so GENERIC() starts from the same store.
-- What the lean path writes is exactly the after-image the generic path writes (the same keys, fields, members and texts; diffcheck.py
-- and REDIS-22 compare them), applied as one direct projection with the same crash rule: the intent is stored only if a write fails.
local META = KEYS[1]
if not string.match(META, '^rw:{[0-9a-f][0-9a-f]+}:meta$') or #META > 266 then return GENERIC() end
local P = string.sub(META, 1, #META - 4)
local FAIL = tonumber(ARGV[1]) or 0
local STORE, OP, CMD, FP = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local NOT_AFTER, CAP, FLOOR, CLS, GATE = tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8]), ARGV[9], ARGV[10]
local function ARGS(i) return ARGV[11 + i] end
local sformat, sfind, sgsub, ssub, byte, concat = string.format, string.find, string.gsub, string.sub, string.byte, table.concat
local t0 = redis.call('TIME')
local NOW = tonumber(t0[1]) * 1000 + math.floor(tonumber(t0[2]) / 1000)
local NOWS = sformat('%d', NOW)
local MAX_SCORE = 9007199254740991
local L = {}

function L.S(n)
  if type(n) == 'string' then return n end
  return sformat('%d', n)
end

-- Exact +1 on a non-negative int64 decimal string; nil past the int64 maximum (U.inc).
function L.inc(s)
  s = s or '0'
  if s == '9223372036854775807' then return nil end
  local out, i = '', #s
  while i >= 1 do
    local c = byte(s, i) - 48
    if c < 9 then return ssub(s, 1, i - 1) .. (c + 1) .. out end
    out = '0' .. out
    i = i - 1
  end
  return '1' .. out
end

-- Bytewise a < b (U.blt).
function L.blt(a, b)
  local n, m = #a, #b
  if m < n then n = m end
  for i = 1, n do
    local x, y = byte(a, i), byte(b, i)
    if x ~= y then return x < y end
  end
  return #a < #b
end

local esc = { ['"'] = '\\"', ['\\'] = '\\\\', ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' }
local function escape(c) return esc[c] or sformat('\\u%04x', byte(c)) end
local QS = {}
-- A JSON string (U.str).
function L.str(s)
  local q = QS[s]
  if q then return q end
  if not sfind(s, '[%c"\\]') then q = '"' .. s .. '"' else q = '"' .. sgsub(s, '[%c"\\]', escape) .. '"' end
  QS[s] = q
  return q
end
local q = L.str

-- The gates, the receipt check and the deadline checks of U.run; meta when the lean path may decide, nil to hand over to GENERIC().
function L.frame()
  local mr = redis.call('HGETALL', META)
  local m = {}
  for i = 1, #mr, 2 do m[mr[i]] = mr[i + 1] end
  if m.store ~= STORE or (m.guard and m.guard ~= '') or m.mode ~= 'normal' or m.layout ~= '2' or m.epoch ~= '1' then return nil end
  if CMD == '' or sfind(CMD, '[|\n]') then return nil end -- not packable as an event pack's command id (U.flush_events writes rows)
  if GATE == 'c' then
    local re = tonumber(m.reqepoch or '1')
    if not re or re > 1 then return nil end
    local known = {}
    for name, v in string.gmatch(ARGV[11] or '', '([^=;]+)=(%d+)') do known[name] = tonumber(v) end
    for name, need in pairs(cjson.decode(m.caps or '{}')) do
      if not known[name] or tonumber(need) > known[name] then return nil end
    end
  end
  if not (m.admission == 'open' or (m.admission == 'drain' and GATE == 'd')) then return nil end
  if FP ~= '' and redis.call('HEXISTS', P .. 'rcpt:' .. CMD, 'cls') == 1 then return nil end
  if not NOT_AFTER or NOW >= NOT_AFTER then return nil end
  if FP ~= '' and not (CAP and NOT_AFTER - NOW <= CAP) then return nil end
  return m
end

-- rw_ownership_v1 (U.ownership): nil when the lease owns the job in one of the states, else status, code.
function L.ownership(j, lease, states)
  if not j.state then return 'NotFound', 'job_not_found' end
  if j.attempt ~= lease.attempt then
    if j.state == 'succeeded' or j.state == 'failed' or j.state == 'cancelled' then return 'InvalidState', 'job_terminal' end
    return 'StaleLease', 'stale_lease'
  end
  if j.token ~= lease.token or j.worker ~= lease.worker or j.gen ~= lease.gen then return 'StaleLease', 'ownership_mismatch' end
  if not (tonumber(j.lease) > NOW) then return 'StaleLease', 'lease_expired' end
  if not states[j.state] then return 'InvalidState', 'invalid_state' end
  return nil
end

-- A lease from its JSON object (U.lease_of); nil when a field is not the text it should be.
function L.lease_of(t)
  if type(t) ~= 'table' then return nil end
  local g = t.generation
  local l = { job = t.jobId, attempt = t.attemptId, worker = t.workerInstanceId, token = t.ownershipToken,
    gen = (type(g) == 'number' or type(g) == 'string') and L.S(g) or nil }
  if type(l.job) ~= 'string' or type(l.attempt) ~= 'string' or type(l.worker) ~= 'string' or type(l.token) ~= 'string' or not l.gen then
    return nil
  end
  return l
end

function L.job(id)
  local a = redis.call('HGETALL', P .. 'job:' .. id)
  local t = {}
  for i = 1, #a, 2 do t[a[i]] = a[i + 1] end
  t.id = id
  return t
end

-- ------------------------------------------------------------------ the after-image, in the intent's op form

-- Per key: a deletion op and a set op ({ 'HD'|'ZR', key, ... }, { 'H'|'Z', key, ... }), in first-touch order, as build_ops lists them.
-- The lean paths touch each (key, field) and (key, member) at most once, so no op needs the overlay's bookkeeping.
local REC, KO = {}, {}
local function rec(k)
  local r = REC[k]
  if not r then
    r = {}
    REC[k] = r
    KO[#KO + 1] = k
  end
  return r
end
function L.hset(k, f, v)
  local r = rec(k)
  local op = r.set
  if not op then
    op = { 'H', k }
    r.set = op
  end
  op[#op + 1] = f
  op[#op + 1] = v
end
function L.hdel(k, f)
  local r = rec(k)
  local op = r.del
  if not op then
    op = { 'HD', k }
    r.del = op
  end
  op[#op + 1] = f
end
function L.zadd(k, s, m)
  local r = rec(k)
  local op = r.set
  if not op then
    op = { 'Z', k }
    r.set = op
  end
  op[#op + 1] = s
  op[#op + 1] = m
end
function L.zrem(k, m)
  local r = rec(k)
  local op = r.del
  if not op then
    op = { 'ZR', k }
    r.del = op
  end
  op[#op + 1] = m
end

-- A job field as U.jw writes it, given its final value in this command: nothing when unchanged, a deletion when cleared.
local JR = {}
function L.jw(j, f, v)
  if j[f] == v then return end
  j[f] = v
  local r = JR[j] -- the job's record, found once per job rather than by building its key for every field
  if not r then
    r = rec('job:' .. j.id)
    JR[j] = r
  end
  if v == nil then
    local op = r.del
    if not op then
      op = { 'HD', 'job:' .. j.id }
      r.del = op
    end
    op[#op + 1] = f
  else
    local op = r.set
    if not op then
      op = { 'H', 'job:' .. j.id }
      r.set = op
    end
    op[#op + 1] = f
    op[#op + 1] = v
  end
end

-- Event rows, written at commit as flush_events writes them.
local EV = {}
function L.event(kind, job, attempt, details)
  EV[#EV + 1] = { kind, job, attempt, details }
end

-- ------------------------------------------------------------------ texts shared with jobs.lua (same functions, same bytes)

function L.claim_text(ordinal, v, b)
  local job, att, worker, token = q(v.job), q(v.att), q(v.worker), q(v.token)
  local gen, lease = v.gen or '{}', v.lease or '{}'
  local begin = '{"begin":{"attemptId":' .. att .. ',"authorizedAt":' .. (b.auth or '{}') .. ',"executionNumber":' .. (b.xnum or '{}')
    .. ',"generation":' .. gen .. ',"jobId":' .. job .. ',"leaseExpiresAt":' .. lease .. ',"ownershipToken":' .. token
    .. ',"revision":' .. (b.brev or '{}') .. ',"state":"running","totalExecutionNumber":' .. (b.txnum or '{}')
    .. ',"workerInstanceId":' .. worker .. '},"code":null,"jobId":' .. job .. ',"ordinal":' .. sformat('%d', ordinal)
  local reserve = ',"reserve":{"attemptId":' .. att .. ',"contract":{"name":' .. q(v.name) .. ',"payloadVersion":' .. (v.pver or '{}')
    .. ',"serializerProfileId":' .. q(v.profile) .. ',"serializerProfileVersion":' .. (v.profver or '{}') .. '},"cursor":{"eligibleAt":'
    .. (v.elig or '{}') .. ',"jobId":' .. job .. '},"fences":[],"generation":' .. gen .. ',"jobId":' .. job .. ',"leaseExpiresAt":' .. lease
    .. ',"ownershipToken":' .. token
  local policy = ',"policy":{"executionTimeout":' .. (v.timeout or 'null') .. ',"fingerprintVersion":1,"maxConsecutiveUnstartedAbandonments":'
    .. (v.maxun or '{}') .. ',"maxExecutions":' .. (v.max or '{}') .. ',"retry":' .. (v.retry or '{}') .. ',"unstartedRetry":' .. (v.uretry or '{}')
    .. '},"priority":' .. q(v.priority) .. ',"queue":' .. q(v.queue) .. ',"reservationNumber":' .. (v.rnum or '{}') .. ',"revision":'
    .. (v.rrev or '{}') .. ',"state":"reserved","workerInstanceId":' .. worker .. '},"status":"Applied"}'
  return begin .. reserve .. policy
end

function L.finish_text(v)
  local state = q(v.state)
  return '{"attemptId":' .. q(v.att) .. ',"attemptOutcome":' .. q(v.outcome) .. ',"disposition":' .. state .. ',"eligibleAt":' .. (v.elig or 'null')
    .. ',"generation":' .. (v.gen or '{}') .. ',"jobId":' .. q(v.job) .. ',"retryDelay":' .. (v.delay or 'null') .. ',"revision":' .. (v.rev or '{}')
    .. ',"state":' .. state .. ',"terminalReason":' .. (v.reason and q(v.reason) or 'null') .. '}'
end

-- Codec 1 lists (22 §3.2): the key lists of jobs.lua (U.RKEYS, U.BKEYS, U.FKEYS) and its texts().
L.RKEYS = { 'job', 'gen', 'rrev', 'att', 'worker', 'token', 'lease', 'rnum', 'name', 'pver', 'profile', 'profver', 'queue', 'priority',
  'elig', 'timeout', 'maxun', 'max', 'retry', 'uretry' }
L.BKEYS = { 'brev', 'xnum', 'txnum', 'auth' }
L.FKEYS = { 'att', 'outcome', 'state', 'elig', 'gen', 'job', 'delay', 'rev', 'reason' }
function L.texts(v, keys, prev)
  local out = {}
  for i = 1, #keys do
    local k = keys[i]
    local x = v[k]
    out[i] = (x == nil and 'null') or (prev and prev[k] == x and '0') or q(x)
  end
  return '[' .. concat(out, ',') .. ']'
end

-- The receipt row of an applied command (U.hset_sorted on rcpt:<command>, compact op: no code or detail field).
function L.receipt(cmd, cls, op, fp, na, floor, result)
  local k = 'rcpt:' .. cmd
  L.hset(k, 'cls', cls)
  L.hset(k, 'op', op)
  L.hset(k, 'ver', '1')
  L.hset(k, 'fp', fp)
  L.hset(k, 'notafter', sformat('%d', na))
  L.hset(k, 'retain', sformat('%d', na + floor))
  L.hset(k, 'status', 'Applied')
  L.hset(k, 'decided', NOWS)
  L.hset(k, 'result', result)
  L.zadd('rcpt:exp:' .. cls, sformat('%d', na + floor), cmd)
end

-- ------------------------------------------------------------------ commit: one direct projection (22 §4.1)

function L.reply(text)
  return { 'R', 'Applied', 'A', '0', '', '', NOWS, text, '', '', '', '' }
end

local writes = 0
local function w(...)
  if FAIL > 0 and writes >= FAIL then error('INJECTED failure after write ' .. writes) end
  writes = writes + 1
  return redis.call(...)
end

local function apply(op)
  local c, k = op[1], P .. op[2]
  local cmd = (c == 'H' and 'HSET') or (c == 'HD' and 'HDEL') or (c == 'Z' and 'ZADD') or (c == 'ZR' and 'ZREM')
  local i = 3
  while i <= #op do
    local last = math.min(#op, i + 999)
    w(cmd, k, unpack(op, i, last))
    i = last + 1
  end
end

-- U.wake: a hint per queue that just received a due job or a new earliest not-yet-due job.
local function wake(ops)
  local seen = {}
  for i = 1, #ops do
    local op = ops[i]
    local queue = op[1] == 'Z' and ssub(op[2], 1, 6) == 'ready:' and string.match(op[2], '^ready:(.*):[^:]*$')
    if queue and not seen[queue] then
      local first
      for k = 3, #op, 2 do
        if tonumber(op[k]) > NOW then
          first = first or redis.call('ZRANGE', P .. op[2], '(' .. NOWS, '+inf', 'BYSCORE', 'LIMIT', 0, 1)[1] or ''
        end
        if tonumber(op[k]) <= NOW or op[k + 1] == first then
          seen[queue] = true
          redis.call('PUBLISH', P .. 'wake', queue)
          break
        end
      end
    end
  end
end

-- Whether the counters this commit advances are below their limits (else GENERIC() gives the counter_limit refusal).
function L.can_commit(m)
  return L.inc(m.rev or '0') ~= nil and tonumber(m.nextevent or '1') + #EV - 1 <= MAX_SCORE
end

-- Event rows, the op list, the writes, the revision and the wake hint, with the crash rule of U.run's direct projection: if a write
-- fails partway the intent (the same table the generic path would pack) is stored after the fact with ipos 1, and the error goes on.
function L.commit(m, reply)
  if #EV > 0 then
    -- One event pack per call (layout 2, U.flush_events): no actor and the call's own command id on every lean event.
    local first = tonumber(m.nextevent or '1')
    local parts, i = { '~1|' .. NOWS .. '|' .. CMD }, 1
    while i <= #EV do
      local e, f = EV[i], EV[i + 1]
      local g, n, x, g2
      if e[1] == 'attempt_reserved' and f and f[1] == 'attempt_started' and f[2] == e[2] and f[3] == e[3] then
        g, n = string.match(e[4], '^{"generation":(%d+),"reservationNumber":(%d+)}$')
        x, g2 = string.match(f[4], '^{"executionNumber":(%d+),"generation":(%d+)}$')
      end
      if g and x and g == g2 then
        parts[#parts + 1] = '~rs|' .. e[2] .. '|' .. e[3] .. '||0|' .. g .. ',' .. n .. ',' .. x
        i = i + 2
      else
        parts[#parts + 1] = e[1] .. '|' .. e[2] .. '|' .. e[3] .. '||0|' .. e[4]
        i = i + 1
      end
    end
    local fs = sformat('%d', first)
    L.zadd('events', fs, fs)
    L.hset('events:data', fs, concat(parts, '\n'))
    L.hset('meta', 'nextevent', sformat('%d', first + #EV))
  end
  local ops = {}
  for _, k in ipairs(KO) do
    local r = REC[k]
    if r.del then ops[#ops + 1] = r.del end
    if r.set then ops[#ops + 1] = r.set end
  end
  local intent = { id = CMD, op = OP, rev = L.inc(m.rev or '0'), decided = NOWS, ops = ops, reply = reply }
  writes = writes + 1 -- the guard's place, as in U.run
  local ok, err = pcall(function()
    for i = 1, #ops do apply(ops[i]) end
    w('HSET', META, 'rev', intent.rev)
    wake(ops)
  end)
  if not ok then
    local bytes = cmsgpack.pack(intent)
    redis.pcall('HSET', META, 'guard', CMD, 'intent', bytes, 'intentsha', redis.sha1hex(bytes), 'ipos', '1')
    error(err, 0)
  end
  if FAIL > 0 and writes >= FAIL then error('INJECTED reply lost after write ' .. writes) end
end

-- The reply with its payload rows (with_payloads): known[id] = { headers, created } read by the claim.
function L.with_payloads(reply, ids, known)
  local a = {}
  for i = 1, #reply do a[i] = reply[i] end
  local n, tail = 0, {}
  for _, id in ipairs(ids or {}) do
    local j = known[id]
    local p = redis.call('GET', P .. 'pay:' .. id)
    if j[1] and p then
      n = n + 1
      tail[#tail + 1] = id
      tail[#tail + 1] = p
      tail[#tail + 1] = j[1]
      tail[#tail + 1] = j[2]
    end
  end
  a[#a + 1] = sformat('%d', n)
  for _, v in ipairs(tail) do a[#a + 1] = v end
  return a
end

-- ------------------------------------------------------------------ finish (U.finish_job and U.close), in two halves

local RUNNING = { running = true }
local TERMINAL = { succeeded = true, failed = true, cancelled = true }

-- The decision for one finish body (reads only): a plan, or nil to hand the command over to GENERIC().
function L.finish_plan(body, draw)
  if type(body) ~= 'table' then return nil end
  local lease = L.lease_of(body.lease)
  local outcome, elapsed = body.outcome, body.elapsed
  if not lease or type(outcome) ~= 'string' or type(elapsed) ~= 'number' then return nil end
  local err, usage, requested = body.error, body.usage, body.requested
  if err == cjson.null then err = nil end
  if usage == cjson.null then usage = nil end
  if requested == cjson.null then requested = nil end
  if (err ~= nil and type(err) ~= 'string') or (usage ~= nil and type(usage) ~= 'string') then return nil end
  local j = L.job(lease.job)
  -- An attempt reserved under layout 1 (a:<attempt>:res) is closed by the generic path in its own form.
  if L.ownership(j, lease, RUNNING) or (j.scopes and j.scopes ~= '') or j['a:' .. lease.attempt .. ':res'] or j['a:' .. lease.attempt]
    or not j.created or not j.ares then return nil end
  local started, max = tonumber(j['g:' .. j.gen .. ':started']), tonumber(j['g:' .. j.gen .. ':max'])
  local rev = L.inc(j.rev)
  if not started or not max or not rev or not tonumber(j.gen) then return nil end
  -- finish_plan
  local state, reason
  if outcome == 'succeeded' then state, reason = 'succeeded', 'succeeded'
  elseif outcome == 'user_cancelled' then state, reason = 'cancelled', 'user_cancelled'
  elseif j.cancelat ~= nil then state, reason = 'cancelled', 'cancellation_requested'
  elseif outcome == 'permanent_failure' then state, reason = 'failed', 'permanent_failure'
  elseif started < max then state = 'retrying'
  else state, reason = 'failed', 'execution_budget_exhausted' end
  local delay, eligible
  if state == 'retrying' then
    local rt = j['g:' .. j.gen .. ':retry']
    if not rt then return nil end
    local retry = cjson.decode(rt)
    if outcome == 'retryable_failure' and requested then
      delay = math.min(math.max(1, requested), retry.maximumDelay)
    else
      -- rw_retry_delay_v1 (U.retry_delay)
      if started < 1 then return nil end
      local initial, maxd = retry.initialDelay, retry.maximumDelay
      local b
      if started - 1 >= 52 or initial * (2 ^ (started - 1)) > maxd then b = maxd else b = math.min(maxd, initial * (2 ^ (started - 1))) end
      local low = math.floor((b + 1) / 2)
      delay = low + math.floor(draw * (b - low + 1))
    end
    eligible = NOW + delay
  end
  return { j = j, lease = lease, outcome = outcome, state = state, reason = reason, delay = delay, eligible = eligible, elapsed = elapsed,
    err = err, usage = usage, rev = rev }
end

-- U.attempt_record: the closed attempt's packed record (layout 2).
local ATT_KEYS = { 'gen', 'res', 'exec', 'worker', 'reserved', 'auth', 'fin', 'outcome', 'disp', 'reason', 'elapsed', 'delay', 'next' }
local ATT_TEXT = { worker = true, outcome = true, disp = true, reason = true }
function L.attempt_record(j, outcome, state, reason, elapsed, delay, eligible)
  local v = { gen = j.gen, res = j.rcount, exec = j.aauth and j['g:' .. j.gen .. ':started'] or nil, worker = j.worker, reserved = j.ares,
    auth = j.aauth, fin = NOWS, outcome = outcome, disp = state, reason = reason, elapsed = elapsed and L.S(elapsed),
    delay = delay and L.S(delay), next = eligible and L.S(eligible) }
  local parts = {}
  for i, k in ipairs(ATT_KEYS) do
    local x = v[k]
    parts[i] = x == nil and 'null' or (ATT_TEXT[k] and q(x) or x)
  end
  return '[' .. concat(parts, ',') .. ']'
end

-- The after-image of one planned finish; returns the result's values (U.finish_job's v).
function L.finish_write(p)
  local j, lease, state, reason, eligible = p.j, p.lease, p.state, p.reason, p.eligible
  local id, att, old = j.id, lease.attempt, j.state
  local a = 'a:' .. att .. ':'
  local terminal = TERMINAL[state]
  L.jw(j, 'a:' .. att, L.attempt_record(j, p.outcome, state, reason, p.elapsed, p.delay, eligible))
  L.jw(j, 'ares', nil)
  L.jw(j, 'aauth', nil)
  L.jw(j, a .. 'error', p.err)
  if eligible then L.jw(j, 'eligible', L.S(eligible)) end
  L.jw(j, 'attempt', nil)
  L.jw(j, 'worker', nil)
  L.jw(j, 'token', nil)
  L.jw(j, 'lease', nil)
  L.jw(j, 'terminalat', terminal and NOWS or nil)
  L.jw(j, 'reason', reason)
  L.jw(j, 'rev', p.rev)
  L.jw(j, 'updated', NOWS)
  L.jw(j, 'state', state)
  -- U.set_state from running
  L.zrem('idx:created:s:' .. old, id)
  L.zadd('idx:created:s:' .. state, j.created, id)
  if state == 'retrying' then L.zadd('ready:' .. j.queue .. ':' .. j.priority, j.eligible, id) else L.zrem('ready:' .. j.queue .. ':' .. j.priority, id) end
  L.zrem('leases', id)
  if terminal then L.zadd('idx:terminal', NOWS, id) else L.zrem('idx:terminal', id) end
  if terminal and j.akey then
    local f = j.name .. '\0' .. j.akey
    if redis.call('HGET', P .. 'keys:active', f) == id then L.hdel('keys:active', f) end
  end
  local g = tonumber(j.gen)
  L.event('attempt_finished', id, att, '{"disposition":' .. q(state) .. (eligible and ',"eligibleAt":' .. sformat('%d', eligible) or '')
    .. ',"generation":' .. sformat('%d', g) .. ',"outcome":' .. q(p.outcome) .. (reason and ',"terminalReason":' .. q(reason) or '') .. '}')
  if p.usage then L.jw(j, a .. 'usage', p.usage) end
  return { att = att, outcome = p.outcome, state = state, elig = eligible and L.S(eligible), gen = j.gen, job = id,
    delay = p.delay and L.S(p.delay), rev = p.rev, reason = reason }
end

function L.finish_compact(v) return '{"~":1,"f":' .. L.texts(v, L.FKEYS) .. '}' end
