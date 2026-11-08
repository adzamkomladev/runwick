-- Shared job steps: the bodies of rw_reserve_job_v1, rw_begin_job_v1, rw_finish_job_v1, rw_close_v1 and rw_dispatch_core_v3
-- (04 §7) on the overlay. Every check runs before the first overlay write of its step; a failed invariant aborts the
-- whole command with no effect (the decision has not been stored yet).

local TERMINAL = { succeeded = true, failed = true, cancelled = true }
U.TERMINAL = TERMINAL

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
  local d = details or {}
  d.generation = tonumber(j.gen)
  d.outcome = outcome
  d.disposition = state
  if reason then d.terminalReason = reason end
  if eligible then d.eligibleAt = eligible end
  U.event(kind, j.id, attempt, d)
  return j.rev
end

-- ------------------------------------------------------------------ Reserve (rw_reserve_job_v1)

-- Due candidates of the lane with a supported tuple, in (eligible_at, job_id) order after the cursor.
function U.candidates(queue, priority, afterAt, afterJob, contracts, limit)
  local key = P .. 'ready:' .. queue .. ':' .. priority
  local out, offset, examined = {}, 0, 0
  local min = afterAt and U.S(afterAt) or '-inf'
  while #out < limit and examined < 5000 do
    local page = redis.call('ZRANGE', key, min, NOWS, 'BYSCORE', 'LIMIT', offset, 100, 'WITHSCORES')
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

function U.policy_json(j)
  return {
    executionTimeout = U.gen(j, 'timeout') and U.raw(U.gen(j, 'timeout')) or NULL,
    fingerprintVersion = 1,
    maxConsecutiveUnstartedAbandonments = U.raw(U.gen(j, 'maxun')),
    maxExecutions = U.raw(U.gen(j, 'max')),
    retry = U.raw(U.gen(j, 'retry')),
    unstartedRetry = U.raw(U.gen(j, 'uretry')),
  }
end

-- Returns the Applied reply (result table) or a no-effect reply (CapacityBlocked carries the cursor).
function U.reserve_job(id, leaseMs, worker, token, entropy)
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
  U.event('attempt_reserved', id, attempt, { generation = tonumber(j.gen), reservationNumber = U.raw(number) })
  return U.applied({
    jobId = id, generation = U.raw(j.gen), revision = U.raw(j.rev), state = 'reserved', attemptId = attempt,
    workerInstanceId = worker, ownershipToken = token, leaseExpiresAt = expiry, reservationNumber = U.raw(number),
    contract = { name = j.name, payloadVersion = U.raw(j.pver), serializerProfileId = j.profile, serializerProfileVersion = U.raw(j.profver) },
    queue = j.queue, priority = j.priority,
    cursor = { eligibleAt = U.raw(j.eligible), jobId = id },
    policy = U.policy_json(j),
    fences = fences,
  })
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

function U.begin_job(lease, jitter)
  local j = U.job(lease.job)
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
  U.event('attempt_started', j.id, lease.attempt, { generation = tonumber(j.gen), executionNumber = tonumber(number) })
  return U.applied({ jobId = j.id, generation = U.raw(j.gen), revision = U.raw(j.rev), state = 'running', attemptId = lease.attempt,
    workerInstanceId = lease.worker, ownershipToken = lease.token, leaseExpiresAt = U.raw(j.lease),
    executionNumber = U.raw(number), totalExecutionNumber = U.raw(j.xcount), authorizedAt = NOW })
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

-- body: { lease = {...}, outcome, elapsed, requested (ms or nil), error (JSON text or nil) }
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
  return U.applied({ jobId = j.id, generation = U.raw(j.gen), revision = U.raw(rev), state = state, attemptId = lease.attempt,
    attemptOutcome = body.outcome, disposition = state, terminalReason = reason or NULL, eligibleAt = eligible or NULL,
    retryDelay = delay or NULL })
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
    for f, v in pairs(fields) do if v ~= nil then U.hset('job:' .. id, f, v) end end
    U.sset('pay:' .. id, it.payload)
    U.zadd('idx:created', NOW, id)
    U.zadd('idx:created:q:' .. it.queue, NOW, id)
    U.zadd('idx:created:s:pending', NOW, id)
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
