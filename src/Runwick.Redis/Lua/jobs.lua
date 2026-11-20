-- Shared job steps: the bodies of rw_reserve_job_v1, rw_begin_job_v1, rw_finish_job_v1, rw_close_v1 and rw_dispatch_core_v3
-- (04 §7) on the overlay. Every check runs before the first overlay write of its step; a failed invariant aborts the
-- whole command with no effect (the decision has not been stored yet).

local TERMINAL = { succeeded = true, failed = true, cancelled = true }
U.TERMINAL = TERMINAL
local q, sformat = U.str, string.format

-- A UUIDv7 at the decision time from 20 hex characters of client entropy (RFC 9562 layout).
function U.uuid7(r)
  local v = (tonumber(string.sub(r, 4, 4), 16) % 4) + 8
  return string.format('%012x', NOW) .. '7' .. string.sub(r, 1, 3) .. string.format('%x', v) .. string.sub(r, 5, 19)
end

function U.expect(ok)
  if not ok then U.abort('InvariantFault', 'invalid_transition') end
end

-- rw_ownership_v1: nil when the lease owns the job in one of the states.
function U.ownership(j, lease, states)
  if not j then return U.noeffect('NotFound', 'job_not_found') end
  if j.attempt ~= lease.attempt then
    if TERMINAL[j.state] then return U.noeffect('InvalidState', 'job_terminal') end
    return U.noeffect('StaleLease', 'stale_lease')
  end
  if j.token ~= lease.token or j.worker ~= lease.worker or j.gen ~= lease.gen then
    return U.noeffect('StaleLease', 'ownership_mismatch')
  end
  if not (tonumber(j.lease) > NOW) then return U.noeffect('StaleLease', 'lease_expired') end
  if not states[j.state] then return U.noeffect('InvalidState', 'invalid_state') end
  return nil
end

-- rw_retry_delay_v1: exponential-v1 with equal jitter from a client draw in [0, 1).
function U.retry_delay(n, initial, max, draw)
  U.expect(n >= 1)
  local b
  if n - 1 >= 52 or initial * (2 ^ (n - 1)) > max then b = max else b = math.min(max, initial * (2 ^ (n - 1))) end
  local low = math.floor((b + 1) / 2)
  return low + math.floor(draw * (b - low + 1))
end

function U.permit_scopes(j) return U.list(j.scopes) end

-- Event details of the hot steps, written directly when the values have their usual types (what U.enc gives for the table built
-- otherwise; SC-01 checks both). An attempt's closing event: details plus generation, outcome, disposition, terminalReason, eligibleAt.
function U.close_details(details, g, outcome, state, reason, eligible)
  if details == nil and g and type(outcome) == 'string' and type(state) == 'string' and (reason == nil or type(reason) == 'string')
    and (eligible == nil or type(eligible) == 'number') then
    return U.raw('{"disposition":' .. q(state) .. (eligible and ',"eligibleAt":' .. sformat('%d', eligible) or '') .. ',"generation":'
      .. sformat('%d', g) .. ',"outcome":' .. q(outcome) .. (reason and ',"terminalReason":' .. q(reason) or '') .. '}')
  end
  local d = details or {}
  d.generation = g
  d.outcome = outcome
  d.disposition = state
  if reason then d.terminalReason = reason end
  if eligible then d.eligibleAt = eligible end
  return d
end

function U.reserved_details(g, number)
  if g and type(number) == 'string' then return U.raw('{"generation":' .. sformat('%d', g) .. ',"reservationNumber":' .. number .. '}') end
  return { generation = g, reservationNumber = U.raw(number) }
end

function U.started_details(g, x)
  if g and x then return U.raw('{"executionNumber":' .. sformat('%d', x) .. ',"generation":' .. sformat('%d', g) .. '}') end
  return { generation = g, executionNumber = x }
end

-- rw_close_v1: attempt row, permits, job, active key, event. Returns the job's new revision.
function U.close(j, attempt, token, outcome, state, reason, kind, elapsed, delay, eligible, err, details)
  U.expect(U.att(j, attempt, 'fin') == nil and j.attempt == attempt and j.token == token)
  U.attw(j, attempt, 'fin', NOWS)
  U.attw(j, attempt, 'outcome', outcome)
  U.attw(j, attempt, 'disp', state)
  U.attw(j, attempt, 'reason', reason)
  U.attw(j, attempt, 'elapsed', elapsed and U.S(elapsed) or nil)
  U.attw(j, attempt, 'delay', delay and U.S(delay) or nil)
  U.attw(j, attempt, 'next', eligible and U.S(eligible) or nil)
  U.attw(j, attempt, 'error', err)
  U.attw(j, attempt, 'closedby', CMD)
  for _, sid in ipairs(U.permit_scopes(j)) do
    local p = U.jget('permits', sid .. ':' .. j.id)
    if p and p.attempt == attempt and p.token == token then
      U.hset('permits', sid .. ':' .. j.id, nil)
      U.zrem('permits:' .. sid, j.id)
    end
  end
  if eligible then U.jw(j, 'eligible', U.S(eligible)) end
  U.jw(j, 'attempt', nil)
  U.jw(j, 'worker', nil)
  U.jw(j, 'token', nil)
  U.jw(j, 'lease', nil)
  U.jw(j, 'terminalat', TERMINAL[state] and NOWS or nil)
  U.jw(j, 'reason', reason)
  U.jw(j, 'rev', U.must(U.inc(j.rev)))
  U.jw(j, 'updated', NOWS)
  U.set_state(j, state)
  if TERMINAL[state] and j.akey then
    local f = j.name .. '\0' .. j.akey
    if U.hget('keys:active', f) == j.id then U.hset('keys:active', f, nil) end
  end
  U.event(kind, j.id, attempt, U.close_details(details, tonumber(j.gen), outcome, state, reason, eligible))
  return j.rev
end

-- ------------------------------------------------------------------ Reserve (rw_reserve_job_v1)

-- Due candidates of the lane with a supported tuple, in (eligible_at, job_id) order after the cursor.
function U.candidates(queue, priority, afterAt, afterJob, contracts, limit)
  local key = P .. 'ready:' .. queue .. ':' .. priority
  local out, offset, examined = {}, 0, 0
  local min = afterAt and U.S(afterAt) or '-inf'
  while #out < limit and examined < 5000 do
    -- The first page asks for a few more than wanted (a claim of 8 read and converted 100 entries); later pages read 100. The
    -- same entries are examined in the same order either way.
    local page = redis.call('ZRANGE', key, min, NOWS, 'BYSCORE', 'LIMIT', offset, offset == 0 and math.min(100, limit + 4) or 100, 'WITHSCORES')
    if #page == 0 then break end
    offset = offset + #page / 2
    for i = 1, #page, 2 do
      local id, score = page[i], tonumber(page[i + 1])
      examined = examined + 1
      if not (afterAt and score == afterAt and not U.blt(afterJob, id)) then
        local c = redis.call('HMGET', P .. 'job:' .. id, 'name', 'pver', 'profile', 'profver', 'state')
        if (c[5] == 'pending' or c[5] == 'retrying') and contracts[c[1] .. '\0' .. c[2] .. '\0' .. c[3] .. '\0' .. c[4]] then
          out[#out + 1] = { id = id, at = score }
          if #out >= limit then break end
        end
      end
    end
  end
  return out
end

function U.contract_set(json)
  local set = {}
  for _, c in ipairs(cjson.decode(json)) do set[c[1] .. '\0' .. U.S(c[2]) .. '\0' .. c[3] .. '\0' .. U.S(c[4])] = true end
  return set
end

function U.queue_gate(queue)
  local q = U.jget('queues', queue)
  if not q then return U.noeffect('NoWork', 'queue_not_registered') end
  if q.paused then return U.noeffect('QueuePaused', 'queue_paused') end
  return nil
end

-- ------------------------------------------------------------------ claim and finish results, and their compact receipts

-- The results of a reserve, a begin and a finish are built from short lists of texts (v) by these functions. A claim or finish
-- receipt (22 §3.2, codec 1) stores only the lists, {"~":1,"c":[[ordinal,[reserve texts],[begin texts]],...]} or {"~":1,"f":[...]}
-- (in a claim, 0 stands for the previous item's text in the same place),
-- and a replay rebuilds the result with the same functions, so it is the replied text byte for byte. A claim whose items are not all
-- plain Applied ones (blocked, deferred, closed, fenced) keeps its text as replied. The key lists are part of codec 1.
U.RKEYS = { 'job', 'gen', 'rrev', 'att', 'worker', 'token', 'lease', 'rnum', 'name', 'pver', 'profile', 'profver', 'queue', 'priority',
  'elig', 'timeout', 'maxun', 'max', 'retry', 'uretry' }
U.BKEYS = { 'brev', 'xnum', 'txnum', 'auth' }
U.FKEYS = { 'att', 'outcome', 'state', 'elig', 'gen', 'job', 'delay', 'rev', 'reason' }
U.COMPACT = true

function U.reserve_result(v, fences)
  return {
    jobId = v.job, generation = U.raw(v.gen), revision = U.raw(v.rrev), state = 'reserved', attemptId = v.att,
    workerInstanceId = v.worker, ownershipToken = v.token, leaseExpiresAt = U.raw(v.lease), reservationNumber = U.raw(v.rnum),
    contract = { name = v.name, payloadVersion = U.raw(v.pver), serializerProfileId = v.profile, serializerProfileVersion = U.raw(v.profver) },
    queue = v.queue, priority = v.priority,
    cursor = { eligibleAt = U.raw(v.elig), jobId = v.job },
    policy = { executionTimeout = v.timeout and U.raw(v.timeout) or NULL, fingerprintVersion = 1,
      maxConsecutiveUnstartedAbandonments = U.raw(v.maxun), maxExecutions = U.raw(v.max), retry = U.raw(v.retry),
      unstartedRetry = U.raw(v.uretry) },
    fences = fences or U.arr(),
  }
end

function U.begin_result(v, b)
  return { jobId = v.job, generation = U.raw(v.gen), revision = U.raw(b.brev), state = 'running', attemptId = v.att,
    workerInstanceId = v.worker, ownershipToken = v.token, leaseExpiresAt = U.raw(v.lease),
    executionNumber = U.raw(b.xnum), totalExecutionNumber = U.raw(b.txnum), authorizedAt = U.raw(b.auth) }
end

function U.claim_item(ordinal, v, b, reserve, begin)
  return { ordinal = ordinal, jobId = v.job, status = 'Applied', code = NULL, reserve = reserve or U.reserve_result(v),
    begin = begin or U.begin_result(v, b) }
end

function U.finish_result(v)
  return { jobId = v.job, generation = U.raw(v.gen), revision = U.raw(v.rev), state = v.state, attemptId = v.att,
    attemptOutcome = v.outcome, disposition = v.state, terminalReason = v.reason or NULL, eligibleAt = v.elig and U.raw(v.elig) or NULL,
    retryDelay = v.delay and U.raw(v.delay) or NULL }
end

-- The texts U.enc gives for a plain Applied claim item and a finish result, written directly (keys in byte order, U.raw values as
-- they are, a nil U.raw value as {} the way U.enc writes it). The generic writer walks and sorts every object of the result, which was
-- the largest single cost of a claim. Values of any other type than the results normally hold go to U.enc. RC-05 checks both against U.enc.
local function claim_text(ordinal, v, b)
  local job, att, worker, token = v.job, v.att, v.worker, v.token
  if type(job) ~= 'string' or type(att) ~= 'string' or type(worker) ~= 'string' or type(token) ~= 'string' or type(v.name) ~= 'string'
    or type(v.profile) ~= 'string' or type(v.queue) ~= 'string' or type(v.priority) ~= 'string' or type(ordinal) ~= 'number' then
    return U.enc(U.claim_item(ordinal, v, b))
  end
  job, att, worker, token = q(job), q(att), q(worker), q(token)
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
U.claim_text = claim_text

function U.finish_text(v)
  local job, att, outcome, state, reason = v.job, v.att, v.outcome, v.state, v.reason
  if type(job) ~= 'string' or type(att) ~= 'string' or type(outcome) ~= 'string' or type(state) ~= 'string'
    or (reason ~= nil and type(reason) ~= 'string') then
    return U.enc(U.finish_result(v))
  end
  state = q(state)
  return '{"attemptId":' .. q(att) .. ',"attemptOutcome":' .. q(outcome) .. ',"disposition":' .. state .. ',"eligibleAt":' .. (v.elig or 'null')
    .. ',"generation":' .. (v.gen or '{}') .. ',"jobId":' .. q(job) .. ',"retryDelay":' .. (v.delay or 'null') .. ',"revision":' .. (v.rev or '{}')
    .. ',"state":' .. state .. ',"terminalReason":' .. (reason and q(reason) or 'null') .. '}'
end

-- A list as a JSON array of texts and nulls, with 0 for a text equal to the previous item's (prev); nil when v holds a key outside
-- the list or a value that is not text (codec 1 cannot keep it, so the receipt keeps the replied text).
local function texts(v, keys, set, prev)
  for k, x in pairs(v) do
    if not set[k] or type(x) ~= 'string' then return nil end
  end
  local out = {}
  for i = 1, #keys do
    local x = v[keys[i]]
    out[i] = (x == nil and 'null') or (prev and prev[keys[i]] == x and '0') or q(x)
  end
  return '[' .. table.concat(out, ',') .. ']'
end
local function set_of(keys)
  local t = {}
  for _, k in ipairs(keys) do t[k] = true end
  return t
end
local RSET, BSET, FSET = set_of(U.RKEYS), set_of(U.BKEYS), set_of(U.FKEYS)

-- The compact receipt result of a decision (r.packed: a claim's items; r.fv: a finish), or nil to store the replied text.
function U.cenc(r)
  if not U.COMPACT then return nil end
  if r.fv then
    local f = texts(r.fv, U.FKEYS, FSET)
    return f and '{"~":1,"f":' .. f .. '}'
  end
  if not r.packed then return nil end
  local out = {}
  for i, p in ipairs(r.packed) do
    local pv = r.packed[i - 1]
    local a, b = texts(p[2], U.RKEYS, RSET, pv and pv[2]), texts(p[3], U.BKEYS, BSET, pv and pv[3])
    if not (a and b) then return nil end
    out[i] = '[' .. p[1] .. ',' .. a .. ',' .. b .. ']'
  end
  return '{"~":1,"c":[' .. table.concat(out, ',') .. ']}'
end

local function unlist(a, keys, prev)
  local v = {}
  for i = 1, #keys do
    local x = a[i]
    if x == 0 then x = prev[keys[i]] end
    if x ~= cjson.null then v[keys[i]] = x end
  end
  return v
end

-- A stored receipt result as replied: plain text as it is, a compact one rebuilt; nil for a codec this binary does not know.
function U.cdec(text)
  if string.sub(text, 1, 5) ~= '{"~":' then return text end
  local d = cjson.decode(text)
  if d['~'] ~= 1 then return nil end
  if d.f then return U.finish_text(unlist(d.f, U.FKEYS)) end
  if not d.c then return nil end
  local items, v, b = {}, nil, nil
  for i, e in ipairs(d.c) do
    v, b = unlist(e[2], U.RKEYS, v), unlist(e[3], U.BKEYS, b)
    items[i] = claim_text(e[1], v, b)
  end
  return '{"items":[' .. table.concat(items, ',') .. ']}'
end

-- Returns the Applied reply (result table) or a no-effect reply (CapacityBlocked carries the cursor).
-- lazy: leave r.result out (a claim writes its item text from r.v; U.reserve_result(r.v, r.fences) builds it when needed).
function U.reserve_job(id, leaseMs, worker, token, entropy, lazy)
  local j = U.job(id)
  if not j or not (j.state == 'pending' or j.state == 'retrying') then return U.noeffect('InvariantFault', 'invalid_transition') end
  local scopes = {}
  for _, sid in ipairs(U.permit_scopes(j)) do
    local s = U.jget('scopes', sid)
    U.expect(s ~= nil)
    scopes[#scopes + 1] = { id = sid, row = s }
    if U.zcount_after('permits:' .. sid, NOW) >= s.capacity then
      return U.noeffect('CapacityBlocked', 'capacity_blocked', { blocked = sid, resumeAt = tonumber(j.eligible), resumeJob = id })
    end
  end
  local expiry = NOW + leaseMs
  local attempt = U.uuid7(entropy)
  local fences = U.arr()
  local fenceOf = {}
  for _, s in ipairs(scopes) do
    if s.row.fencing then
      local v = U.must(U.inc(s.row.fence or '0'))
      s.row.fence = v
      s.row.updated = NOW
      U.jset('scopes', s.id, s.row)
      fenceOf[s.id] = v
      fences[#fences + 1] = { scopeId = s.id, value = U.raw(v) }
    end
  end
  local number = U.must(U.inc(j.rcount))
  U.attw(j, attempt, 'gen', j.gen)
  U.attw(j, attempt, 'res', number)
  U.hset('attempts', attempt, id) -- attempt -> job, for management reads and RevealError (22 §7.2)
  U.attw(j, attempt, 'worker', worker)
  U.attw(j, attempt, 'token', token)
  U.attw(j, attempt, 'reserved', NOWS)
  U.jw(j, 'rcount', number)
  U.jw(j, 'rev', U.must(U.inc(j.rev)))
  U.jw(j, 'attempt', attempt)
  U.jw(j, 'worker', worker)
  U.jw(j, 'token', token)
  U.jw(j, 'lease', U.S(expiry))
  U.jw(j, 'updated', NOWS)
  U.set_state(j, 'reserved')
  for _, s in ipairs(scopes) do
    U.jset('permits', s.id .. ':' .. id, { attempt = attempt, token = token, lease = expiry, fence = fenceOf[s.id] or NULL })
    U.zadd('permits:' .. s.id, expiry, id)
  end
  U.event('attempt_reserved', id, attempt, U.reserved_details(tonumber(j.gen), number))
  local v = { job = id, gen = j.gen, rrev = j.rev, att = attempt, worker = worker, token = token, lease = U.S(expiry), rnum = number,
    name = j.name, pver = j.pver, profile = j.profile, profver = j.profver, queue = j.queue, priority = j.priority, elig = j.eligible,
    timeout = U.gen(j, 'timeout'), maxun = U.gen(j, 'maxun'), max = U.gen(j, 'max'), retry = U.gen(j, 'retry'), uretry = U.gen(j, 'uretry') }
  -- j: the job as this command left it, for a Begin that follows in the same command; v: the values a compact receipt keeps.
  return U.applied(not lazy and U.reserve_result(v, fences) or nil, nil, nil, { j = j, v = v, fenced = #fences > 0, fences = fences })
end

-- ------------------------------------------------------------------ Begin (rw_begin_job_v1, 10 §4)

local MIN_DEFER, MAX_JITTER = 250, 100

-- The rate step: grants all-or-none at logical time t, or the T20 deferral (returned).
local function rate_step(j, lease, jitter)
  local ids = U.list(j.rates)
  if #ids == 0 then return nil end
  table.sort(ids, U.blt)
  local scopes, t = {}, NOW
  for _, id in ipairs(ids) do
    local s = U.jget('ratescopes', id)
    U.expect(s ~= nil)
    scopes[#scopes + 1] = { id = id, row = s }
    if U.nn(s.last) and s.last > t then t = s.last end
  end
  local counts, blocked, isBlocked = {}, U.arr(), {}
  for i, s in ipairs(scopes) do
    counts[i] = U.zcount_after('grants:' .. s.id, t - s.row.window)
    if counts[i] >= s.row.limit or (U.nn(s.row.notBefore) and t < s.row.notBefore) then
      blocked[#blocked + 1] = s.id
      isBlocked[s.id] = true
    end
  end
  for _, s in ipairs(scopes) do
    if not U.nn(s.row.last) or s.row.last < t then
      s.row.last = t
      U.jset('ratescopes', s.id, s.row)
    end
  end
  if #blocked == 0 then
    for _, s in ipairs(scopes) do U.zadd('grants:' .. s.id, t, lease.attempt .. ':' .. j.id .. ':' .. U.S(s.row.prev)) end
    return nil
  end
  local hint = t
  for i, s in ipairs(scopes) do
    if isBlocked[s.id] then
      if counts[i] >= s.row.limit then
        local k = P .. 'grants:' .. s.id
        local o = redis.call('ZRANGE', k, '(' .. U.S(t - s.row.window), '+inf', 'BYSCORE', 'LIMIT', counts[i] - s.row.limit, 1, 'WITHSCORES')
        if #o == 2 and tonumber(o[2]) + s.row.window > hint then hint = tonumber(o[2]) + s.row.window end
      end
      if U.nn(s.row.notBefore) and s.row.notBefore > hint then hint = s.row.notBefore end
    end
  end
  local logical = math.max(hint, t + MIN_DEFER)
  local eligible = NOW + (logical - t) + jitter
  local state = U.gen(j, 'started') == '0' and 'pending' or 'retrying'
  local rev = U.close(j, lease.attempt, lease.token, 'admission_deferred', state, nil, 'attempt_admission_deferred', nil,
    math.max(1, eligible - NOW), eligible, nil, { blockingScopes = blocked, rateTime = t })
  return U.applied({ jobId = j.id, generation = U.raw(j.gen), revision = U.raw(rev), state = state, deferredUntil = eligible,
    blockingScopes = blocked }, 'RateLimited', 'rate_limited', { detail = U.S(eligible) })
end

-- lazy: an authorized begin leaves r.result out (U.begin_result(r.v, r.bv) builds it).
function U.begin_job(lease, jitter, job, lazy)
  local j = job or U.job(lease.job)
  local plan = 'authorize'
  if j then
    if j.cancelat then plan = 'cancel'
    elseif tonumber(U.gen(j, 'started')) >= tonumber(U.gen(j, 'max')) then plan = 'exhausted' end
  end
  local refused = U.ownership(j, lease, { reserved = true })
  if refused then return refused end
  if plan ~= 'authorize' then
    local cancel = plan == 'cancel'
    local outcome = cancel and 'cancelled_before_start' or 'abandoned_before_start'
    local reason = cancel and 'cancelled_before_start' or 'execution_budget_exhausted'
    local state = cancel and 'cancelled' or 'failed'
    local rev = U.close(j, lease.attempt, lease.token, outcome, state, reason,
      cancel and 'attempt_cancelled_before_start' or 'attempt_finished')
    return U.applied({ jobId = j.id, generation = U.raw(j.gen), revision = U.raw(rev), state = state, attemptOutcome = outcome,
      terminalReason = reason, errorCode = reason }, cancel and 'InvalidState' or 'BudgetExhausted', reason)
  end
  local deferred = rate_step(j, lease, jitter)
  if deferred then return deferred end
  local number = U.must(U.inc(U.gen(j, 'started')))
  U.expect(tonumber(number) <= tonumber(U.gen(j, 'max')))
  U.genw(j, 'started', number)
  U.genw(j, 'unstarted', '0')
  U.jw(j, 'xcount', U.must(U.inc(j.xcount)))
  U.jw(j, 'rev', U.must(U.inc(j.rev)))
  U.jw(j, 'updated', NOWS)
  U.expect(U.att(j, lease.attempt, 'fin') == nil and U.att(j, lease.attempt, 'exec') == nil)
  U.attw(j, lease.attempt, 'exec', number)
  U.attw(j, lease.attempt, 'auth', NOWS)
  U.set_state(j, 'running')
  U.event('attempt_started', j.id, lease.attempt, U.started_details(tonumber(j.gen), tonumber(number)))
  local v = { job = j.id, gen = j.gen, att = lease.attempt, worker = lease.worker, token = lease.token, lease = j.lease }
  local b = { brev = j.rev, xnum = number, txnum = j.xcount, auth = NOWS }
  return U.applied(not lazy and U.begin_result(v, b) or nil, nil, nil, { v = v, bv = b })
end

function U.lease_of(t)
  return { job = t.jobId, attempt = t.attemptId, worker = t.workerInstanceId, token = t.ownershipToken, gen = U.S(t.generation) }
end

-- ------------------------------------------------------------------ Finish (rw_finish_job_v1)

local function finish_plan(outcome, cancel, started, max)
  if outcome == 'succeeded' then return 'succeeded', 'succeeded' end
  if outcome == 'user_cancelled' then return 'cancelled', 'user_cancelled' end
  if cancel then return 'cancelled', 'cancellation_requested' end
  if outcome == 'permanent_failure' then return 'failed', 'permanent_failure' end
  if started < max then return 'retrying', nil end
  return 'failed', 'execution_budget_exhausted'
end

-- body: { lease = {...}, outcome, elapsed, requested (ms or nil), error (JSON text or nil), usage (JSON text or nil, 16 §3) }
function U.finish_job(body, draw)
  local lease = U.lease_of(body.lease)
  local j = U.job(lease.job)
  local refused = U.ownership(j, lease, { running = true })
  if refused then return refused end
  local started, max = tonumber(U.gen(j, 'started')), tonumber(U.gen(j, 'max'))
  local state, reason = finish_plan(body.outcome, j.cancelat ~= nil, started, max)
  local delay, eligible
  if state == 'retrying' then
    local retry = cjson.decode(U.gen(j, 'retry'))
    local requested = U.nn(body.requested)
    if body.outcome == 'retryable_failure' and requested then
      delay = math.min(math.max(1, requested), retry.maximumDelay)
    else
      delay = U.retry_delay(started, retry.initialDelay, retry.maximumDelay, draw)
    end
    eligible = NOW + delay
  end
  local rev = U.close(j, lease.attempt, lease.token, body.outcome, state, reason, 'attempt_finished', body.elapsed, delay, eligible,
    U.nn(body.error))
  if U.nn(body.usage) then U.attw(j, lease.attempt, 'usage', body.usage) end
  local v = { att = lease.attempt, outcome = body.outcome, state = state, elig = eligible and U.S(eligible), gen = j.gen, job = j.id,
    delay = delay and U.S(delay), rev = rev, reason = reason }
  local text = U.finish_text(v)
  return U.applied(U.raw(text), nil, nil, { fv = v, rtext = text })
end

-- ------------------------------------------------------------------ scopes (04 §7.1, §7.2 step 8)

local function scope_field(kind, key) return 'k:' .. kind .. '\0' .. key end
U.scope_field = scope_field

-- Seed-if-absent; returns id, row, created.
function U.seed_scope(hash, kind, key, newId, row)
  local f = scope_field(kind, key)
  local id = U.hget(hash, f)
  if id then return id, U.jget(hash, id), false end
  U.hset(hash, f, newId)
  U.jset(hash, newId, row)
  return newId, row, true
end

function U.new_concurrency_scope(kind, key, capacity)
  return { kind = kind, key = key, capacity = capacity, fencing = (kind == 'resource' and capacity == 1), fence = '0', prev = 1,
    created = NOW, updated = NOW }
end

-- ------------------------------------------------------------------ dispatch core (rw_dispatch_core_v3)

-- items: decoded item tables with .payload; returns nil, results (array of result tables) or a no-effect reply.
-- The fields a created job's hash starts with, in bytewise order (what sorting them gave); a test checks the order.
U.JOB_FIELDS = { 'akey', 'created', 'eligible', 'g:1:created', 'g:1:max', 'g:1:maxun', 'g:1:reason', 'g:1:retry', 'g:1:started',
  'g:1:timeout', 'g:1:unstarted', 'g:1:uretry', 'gen', 'headers', 'name', 'penc', 'priority', 'profile', 'profver', 'pver', 'queue',
  'rates', 'rcount', 'rev', 'scopes', 'state', 'updated', 'xcount' }

function U.dispatch_core(items, continueOnConflict, createdReason, actor)
  local hasRates = false
  for _, it in ipairs(items) do if #it.rates > 0 then hasRates = true end end
  if hasRates then
    local caps = cjson.decode(U.meta.caps or '{}')
    if not caps['rate_admission'] then return U.noeffect('UnsupportedVersion', 'rate_admission_not_active') end
  end
  for _, it in ipairs(items) do
    if not U.hget('queues', it.queue) then return U.noeffect('InvalidRequest', 'queue_not_registered') end
  end
  local keys = {}
  local function key(kind, name, k)
    local f = name .. '\0' .. k
    local id = kind .. '\0' .. f
    local s = keys[id]
    if not s then
      s = { kind = kind, f = f }
      if kind == 'active' then
        s.bound = U.hget('keys:active', f)
      else
        local w = U.jget('keys:window', f)
        if w then s.bound, s.biz, s.pol, s.ver, s.exp = w.job, w.biz, w.pol, w.ver, w.exp end
      end
      keys[id] = s
    end
    return s
  end
  local function bind_window(wk, job, it)
    wk.bound, wk.biz, wk.pol, wk.ver, wk.exp, wk.dirty = job, it.biz, it.pol, 1, NOW + it.wms, true
    U.hset('job:' .. job, 'w:' .. wk.f, '1') -- reverse link for retention (17 §3.2): a live window binding keeps its job
  end
  local results, created, drifts, conflicts = U.arr(), {}, {}, {}
  for i, it in ipairs(items) do
    local ord = i - 1
    local ak = U.nn(it.akey) and key('active', it.name, it.akey) or nil
    local wk = U.nn(it.wkey) and key('window', it.name, it.wkey) or nil
    if wk and wk.bound and wk.exp > NOW then
      if wk.ver ~= 1 then return U.noeffect('UnsupportedVersion', 'fingerprint_version_unsupported') end
      if it.biz ~= wk.biz then
        conflicts[#conflicts + 1] = { ord = ord, code = 'window_fingerprint_mismatch', job = wk.bound }
        results[#results + 1] = { ordinal = ord, failureCode = 'uniqueness_conflict', conflictingJobId = wk.bound }
      elseif ak and ak.bound and ak.bound ~= wk.bound then
        conflicts[#conflicts + 1] = { ord = ord, code = 'uniqueness_composition_conflict', job = ak.bound }
        results[#results + 1] = { ordinal = ord, failureCode = 'uniqueness_composition_conflict', conflictingJobId = ak.bound }
      else
        local match = (ak and ak.bound and ak.bound == wk.bound) and 'ActiveAndWindow' or 'Window'
        local drift = it.pol ~= wk.pol
        results[#results + 1] = { ordinal = ord, jobId = wk.bound, disposition = 'Reused', match = match, policyDrift = drift }
        if drift then drifts[#drifts + 1] = { ord = ord, job = wk.bound, match = match } end
      end
    elseif ak and ak.bound then
      if wk then bind_window(wk, ak.bound, it) end
      results[#results + 1] = { ordinal = ord, jobId = ak.bound, disposition = 'Reused', match = 'Active', policyDrift = false }
    else
      local job = it.jobId
      if ak then
        ak.bound = job
        ak.dirty = true
      end
      if wk then bind_window(wk, job, it) end
      created[#created + 1] = i
      results[#results + 1] = { ordinal = ord, jobId = job, disposition = 'Created', match = 'None', policyDrift = false }
    end
  end
  if #conflicts > 0 and not continueOnConflict then
    local detail
    if #items > 1 then
      local a = U.arr()
      for _, c in ipairs(conflicts) do
        a[#a + 1] = { ordinal = c.ord, code = c.code == 'window_fingerprint_mismatch' and 'uniqueness_conflict' or c.code,
          conflictingJobId = c.job or NULL }
      end
      detail = U.enc(a)
    end
    return U.noeffect('UniquenessConflict', conflicts[1].code, { detail = detail, conflict = conflicts[1].job })
  end
  -- Key writes in sorted order.
  for _, id in ipairs(U.sorted(keys)) do
    local s = keys[id]
    if s.dirty then
      if s.kind == 'active' then
        U.hset('keys:active', s.f, s.bound)
      else
        U.jset('keys:window', s.f, { job = s.bound, biz = s.biz, pol = s.pol, ver = s.ver, exp = s.exp })
        U.zadd('keys:window:exp', s.exp, s.f)
      end
    end
  end
  -- Jobs in ascending job id; scopes seeded in (kind, key) order, the first item in job order wins.
  local byJob = {}
  for _, i in ipairs(created) do byJob[#byJob + 1] = items[i] end
  table.sort(byJob, function(a, b) return U.blt(a.jobId, b.jobId) end)
  local cseeds, rseeds = {}, {}
  for _, it in ipairs(byJob) do
    for _, s in ipairs(it.scopes) do
      local f = s.kind .. '\0' .. s.key
      if not cseeds[f] then cseeds[f] = s end
    end
    for _, s in ipairs(it.rates) do
      local f = s.kind .. '\0' .. s.key
      if not rseeds[f] then rseeds[f] = s end
    end
  end
  local cid, rid = {}, {}
  for _, f in ipairs(U.sorted(cseeds)) do
    local s = cseeds[f]
    cid[f] = U.seed_scope('scopes', s.kind, s.key, s.id, U.new_concurrency_scope(s.kind, s.key, s.capacity))
  end
  for _, f in ipairs(U.sorted(rseeds)) do
    local s = rseeds[f]
    rid[f] = U.seed_scope('ratescopes', s.kind, s.key, s.id, { kind = s.kind, key = s.key, limit = s.limit, window = s.window,
      prev = 1, last = NULL, notBefore = NULL, created = NOW, updated = NOW })
  end
  for _, it in ipairs(byJob) do
    local id = it.jobId
    if redis.call('EXISTS', P .. 'job:' .. id) == 1 then U.abort('InvariantFault', 'id_collision') end
    local eligible = NOW
    if U.nn(it.notBefore) and it.notBefore > NOW then eligible = it.notBefore end
    local function ids(list, map)
      local seen, out = {}, {}
      for _, s in ipairs(list) do
        local x = map[s.kind .. '\0' .. s.key]
        if not seen[x] then
          seen[x] = true
          out[#out + 1] = x
        end
      end
      table.sort(out, U.blt)
      return table.concat(out, ',')
    end
    local j = { id = id, state = 'pending', queue = it.queue, priority = it.priority, eligible = U.S(eligible), created = NOWS }
    local p = it.policy
    local fields = {
      state = 'pending', name = it.name, pver = U.S(it.pver), profile = it.profile, profver = U.S(it.profver), queue = it.queue, priority = it.priority,
      eligible = U.S(eligible), created = NOWS, updated = NOWS, akey = U.nn(it.akey), gen = '1', rev = '1', rcount = '0', xcount = '0',
      headers = it.headers, penc = '1', scopes = ids(it.scopes, cid), rates = ids(it.rates, rid),
      ['g:1:created'] = NOWS, ['g:1:reason'] = createdReason, ['g:1:max'] = U.S(p.maxExec), ['g:1:maxun'] = U.S(p.maxUn),
      ['g:1:timeout'] = U.nn(p.timeout) and U.S(p.timeout) or nil, ['g:1:retry'] = p.retry, ['g:1:uretry'] = p.uretry,
      ['g:1:started'] = '0', ['g:1:unstarted'] = '0',
    }
    U.hset_sorted('job:' .. id, U.JOB_FIELDS, fields, true)
    U.sset('pay:' .. id, it.payload)
    U.zadd('idx:created', NOW, id)
    U.zadd('idx:created:q:' .. it.queue, NOW, id)
    U.zadd('idx:created:s:pending', NOW, id)
    if U.counted(NOW, id) then U.jobtype(it.queue, it.name, 1) end
    U.zadd(U.ready_key(j), eligible, id)
  end
  for _, i in ipairs(created) do
    local it = items[i]
    U.event('dispatched', it.jobId, nil, { generation = 1, queue = it.queue, priority = it.priority, ordinal = i - 1 }, actor)
  end
  for _, d in ipairs(drifts) do U.event('policy_drift', d.job, nil, { ordinal = d.ord, match = d.match }, actor) end
  return nil, results
end

-- ------------------------------------------------------------------ management audit (16, 19 §6.2)

-- actor: { subject, display, scheme, reason }; fields: extra row columns.
function U.audit(action, actor, fields)
  local id = tonumber(U.hget('meta', 'nextaudit') or '1')
  if id > MAX_SCORE then U.abort('InvariantFault', 'counter_limit') end
  local row = fields or {}
  row.auditId = id
  row.action = action
  row.subject = actor.subject
  row.display = U.nn(actor.display) or NULL
  row.scheme = actor.scheme
  row.reason = U.nn(actor.reason) or NULL
  row.commandId = CMD
  row.at = NOW
  U.zadd('audit', id, U.S(id))
  U.hset('audit:data', U.S(id), U.enc(row))
  U.hset('meta', 'nextaudit', U.S(id + 1))
  return id
end

-- Dashed UUID text (8-4-4-4-12) of a 32-hex ID, as management audit target IDs are stored on PostgreSQL.
function U.uuid_text(hex)
  return string.sub(hex, 1, 8) .. '-' .. string.sub(hex, 9, 12) .. '-' .. string.sub(hex, 13, 16) .. '-' .. string.sub(hex, 17, 20) .. '-' .. string.sub(hex, 21, 32)
end

-- ------------------------------------------------------------------ shared decisions (FinishMany/1, ReserveAndBegin/1, FinishAndClaim)
-- Each takes the position of its first argument, so one script call can run both (FinishAndClaim: one intent, one projection).

U.DURABLE_FLOOR = 86400000
-- The item as U.raw text written directly (what U.enc gives for the table below), when its values have the usual types.
function U.finish_item_json(cmd, r, replayed)
  local res, code, d, st = r.result, r.code, r.decided, r.status
  if type(cmd) == 'string' and type(st) == 'string' and (code == nil or type(code) == 'string') and type(replayed) == 'boolean' then
    local rt
    if res == nil then
      rt = 'null'
    elseif type(res) == 'string' then
      rt = res
    elseif type(res) == 'table' and type(res.__raw) == 'string' then
      rt = res.__raw
    end
    if rt then
      return U.raw('{"code":' .. (code and q(code) or 'null') .. ',"commandId":' .. q(cmd) .. ',"decidedAt":' .. (d and d ~= '' and d or 'null')
        .. ',"effect":' .. (r.effect == 'A' and '"Applied"' or '"ObservedNoEffect"') .. ',"replayed":' .. (replayed and 'true' or 'false')
        .. ',"result":' .. rt .. ',"status":' .. q(st) .. '}')
    end
  end
  return { commandId = cmd, status = r.status, effect = r.effect == 'A' and 'Applied' or 'ObservedNoEffect', replayed = replayed,
    code = r.code or NULL, decidedAt = r.decided and r.decided ~= '' and U.raw(r.decided) or NULL,
    result = r.result and (type(r.result) == 'table' and r.result or U.raw(r.result)) or NULL }
end

-- ARGS(base) n, then per item: command id, fingerprint, NotAfter ms, body JSON, retry draw. Returns the item replies and the applied count.
function U.finish_items(base)
  local n = tonumber(ARGS(base))
  local items, done, confirm = {}, {}, {}
  for i = 1, n do
    local b = base + 1 + (i - 1) * 5
    items[i] = { cmd = ARGS(b), fp = ARGS(b + 1), na = tonumber(ARGS(b + 2)), body = cjson.decode(ARGS(b + 3)), draw = tonumber(ARGS(b + 4)) }
  end
  for k, it in ipairs(items) do
    local rc = redis.call('HMGET', P .. 'rcpt:' .. it.cmd, 'cls', 'op', 'ver', 'fp', 'status', 'code', 'decided', 'result')
    local text = rc[1] and rc[8] and U.cdec(rc[8])
    if rc[1] then
      if rc[1] ~= 'durable' or rc[2] ~= 'Finish' or rc[3] ~= '1' or rc[4] ~= it.fp then
        done[k] = U.finish_item_json(it.cmd, U.noeffect('CommandConflict', 'command_conflict'), false)
      elseif not text then
        done[k] = U.finish_item_json(it.cmd, U.noeffect('UnsupportedVersion', 'receipt_codec_unsupported'), false)
      else
        confirm[#confirm + 1] = k
        done[k] = U.finish_item_json(it.cmd, { status = rc[5], effect = 'A', code = rc[6] ~= '' and rc[6] or nil, decided = rc[7], result = text }, true)
      end
    elseif NOW >= it.na then
      done[k] = U.finish_item_json(it.cmd, U.noeffect('CommandExpired', 'command_expired', { decided = NOWS }), false)
    elseif it.na - NOW > 60000 then
      done[k] = U.finish_item_json(it.cmd, U.noeffect('InvalidRequest', 'deadline_too_far', { decided = NOWS }), false)
    end
  end
  -- 22 §4.5 markers for the replayed items: fresh writes, not part of any intent, in command-id order. (Only these are sorted: the
  -- checks above are reads, so their order changes nothing.)
  if #confirm > 1 then table.sort(confirm, function(a, b) return U.blt(items[a].cmd, items[b].cmd) end) end
  for _, k in ipairs(confirm) do redis.call('HSET', P .. 'rcpt:' .. items[k].cmd, 'confirmed', NOWS) end
  local applied = 0
  for k, it in ipairs(items) do
    if not done[k] then
      local r = U.finish_job(it.body, it.draw)
      r.decided = NOWS
      if r.effect == 'A' then
        applied = applied + 1
        local key = 'rcpt:' .. it.cmd
        -- Encoded once: the receipt row and the reply item carry the same text.
        local encoded, stored = r.rtext or U.enc(r.result), U.cenc(r)
        r.result = U.raw(encoded)
        U.hset_sorted(key, RECEIPT_FIELDS, { cls = 'durable', op = 'Finish', ver = '1', fp = it.fp, notafter = U.S(it.na),
          retain = U.S(it.na + U.DURABLE_FLOOR), status = 'Applied', decided = NOWS, result = stored or encoded }, true)
        U.zadd('rcpt:exp:durable', it.na + U.DURABLE_FLOOR, it.cmd)
      end
      done[k] = U.finish_item_json(it.cmd, r, false)
    end
  end
  local out = U.arr()
  for k = 1, n do out[k] = done[k] end
  return out, applied
end

-- ARGS(base..base+10): worker, queue, priority, lease ms, tokens JSON, after eligible, after job, contracts JSON, max jobs, entropies JSON,
-- jitters JSON. Returns the Applied reply (result { items }, payloads) or a no-effect reply.
function U.reserve_and_begin(base)
  local function a(k) return ARGS(base + k - 1) end
  local g = U.queue_gate(a(2))
  if g then return g end
  local afterAt = a(6) ~= '' and tonumber(a(6)) or nil
  local c = U.candidates(a(2), a(3), afterAt, a(7), U.contract_set(a(8)), tonumber(a(9)))
  if #c == 0 then return U.noeffect('NoWork', 'no_work') end
  local tokens, entropy, jitters = cjson.decode(a(5)), cjson.decode(a(10)), cjson.decode(a(11))
  local items, begun, firstBlocked, last, reserved = U.arr(), {}, nil, nil, false
  local packed, plain, text, known = {}, false, {}, {}
  for i, cand in ipairs(c) do
    local r = U.reserve_job(cand.id, tonumber(a(4)), a(1), tokens[i], entropy[i], true)
    local item
    if r.status == 'CapacityBlocked' then
      firstBlocked = firstBlocked or r.blocked
      plain = true
      last = cand
      item = { ordinal = i - 1, jobId = cand.id, status = 'CapacityBlocked', code = 'capacity_blocked', blockedScopeId = r.blocked,
        cursor = { eligibleAt = cand.at, jobId = cand.id } }
    else
      if r.effect ~= 'A' then U.abort(r.status, r.code) end
      reserved = true
      local v = r.v
      -- U.lease_of(reserve result), from the values that result is built from.
      local b = U.begin_job({ job = v.job, attempt = v.att, worker = v.worker, token = v.token, gen = v.gen }, jitters[i], r.j, true)
      if b.effect ~= 'A' then U.abort(b.status, b.code) end
      local bv = b.bv
      if b.status == 'Applied' and not b.code and not r.fenced and bv and b.v.job == v.job and b.v.gen == v.gen and b.v.att == v.att
        and b.v.worker == v.worker and b.v.token == v.token and b.v.lease == v.lease then
        -- Written by the function a replay of the compact receipt uses.
        item = U.raw(claim_text(i - 1, v, bv))
        packed[#packed + 1] = { i - 1, v, bv }
      else
        plain = true
        item = { ordinal = i - 1, jobId = cand.id, status = b.status, code = b.code or NULL, reserve = U.reserve_result(v, r.fences),
          begin = b.result or U.begin_result(b.v, bv) }
      end
      if b.status == 'Applied' then
        begun[#begun + 1] = cand.id
        -- The claim does not change headers or created, so the reply's payload rows take them from the job as read here.
        known[cand.id] = { r.j.headers or false, r.j.created or false }
      end
    end
    items[#items + 1] = item
    text[#text + 1] = item.__raw or U.enc(item)
  end
  if not reserved then
    return U.noeffect('CapacityBlocked', 'capacity_blocked', { blocked = firstBlocked, resumeAt = last.at, resumeJob = last.id })
  end
  -- rtext is U.enc(r.result): the items' texts in order.
  local r = U.applied({ items = items }, nil, nil, { rtext = '{"items":[' .. table.concat(text, ',') .. ']}' })
  r.payloads = begun
  r.known = known
  if not plain then r.packed = packed end
  return r
end
