-- Decide one command, record it as a complete intent (guard + after-images) in one write, then
-- (for small intents) apply the after-images and publish. Large intents are projected by recover.lua.
-- ARGV: 1 clock override ('' = server TIME), 2 failAfter, 3 store id, 4 op, 5 command id,
--       6 fingerprint, 7 NotAfter (epoch ms), 8.. operation arguments.
failAfter = tonumber(ARGV[2])
local storeId, opName, cmdId, fp, notAfter = ARGV[3], ARGV[4], ARGV[5], ARGV[6], tonumber(ARGV[7])
local A = 8
local INLINE_OPS, MAX_OPS, MAX_KEYS, MAX_BYTES = 100, 1024, 256, 4194304
local DURABLE_CAP = 600000

local meta = redis.call('HMGET', META, 'store', 'guard', 'rev')
if not meta[1] then return { 'NOEFFECT', 'AdmissionClosed', 'application_not_registered', '', '' } end
if meta[1] ~= storeId then return { 'NOEFFECT', 'UnsupportedVersion', 'store_identity_mismatch', '', '' } end
if meta[2] and meta[2] ~= '' then return { 'RECOVERING', meta[2] } end

local now
if ARGV[1] ~= '' then now = tonumber(ARGV[1]) else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function noeffect(status, code) return { 'NOEFFECT', status, code, '', S(now) } end

local ops = {}
local function H(key, t) ops[#ops + 1] = { 'h', key, flat(t) } end
local function Z(key, score, member) ops[#ops + 1] = { 'z+', key, score, member } end
local function ZR(key, member) ops[#ops + 1] = { 'z-', key, member } end

local counters
local counterOk = true
local function ctr(name, d)
  counters = counters or hgetall(P .. 'counters')
  local v = dadd(counters[name] or '0', d)
  if not v then counterOk = false return end
  counters[name] = v
end

local receiptBearing = opName ~= 'renew'
if receiptBearing then
  local r = redis.call('HMGET', P .. 'rcpt:' .. cmdId, 'op', 'fp', 'status', 'result', 'decidedAt')
  if r[1] then
    if r[1] ~= opName or r[2] ~= fp then return noeffect('CommandConflict', 'command_conflict') end
    return { 'REPLAYED', r[3], '', r[4], r[5] }
  end
end
if now >= notAfter then return noeffect('CommandExpired', 'command_expired') end
if notAfter - now > DURABLE_CAP then return noeffect('InvalidRequest', 'deadline_too_far') end

local function owned(jobId, attempt, worker, token, gen)
  local j = hgetall(P .. 'job:' .. jobId)
  if not j.state then return nil, noeffect('NotFound', 'job_not_found') end
  if j.attempt ~= attempt then return nil, noeffect('StaleLease', 'stale_lease') end
  if j.worker ~= worker or j.token ~= token or j.gen ~= gen then return nil, noeffect('StaleLease', 'ownership_mismatch') end
  if tonumber(j.leaseExp) <= now then return nil, noeffect('StaleLease', 'lease_expired') end
  return j
end
local function clearOwnership(j)
  j.attempt, j.worker, j.token, j.leaseExp = nil, nil, nil, nil
end
local function newJob(jobId, queue, payload, maxExec, origin)
  if redis.call('EXISTS', P .. 'job:' .. jobId) == 1 then return false end
  local j = { state = 'pending', queue = queue, payload = payload, gen = '1', rev = '1', started = '0', maxExec = maxExec, eligibleAt = S(now) }
  if origin ~= '' then j.origin = origin end
  H('job:' .. jobId, j)
  Z('pending', S(now), jobId)
  ctr('pending', 1)
  ctr('created', 1)
  return true
end
local function audit(kind, fields)
  ctr('audit', 1)
  fields.kind, fields.at, fields.command = kind, S(now), cmdId
  H('audit:' .. cmdId, fields)
  Z('auditIdx', counters.audit, cmdId)
end

local result
if opName == 'dispatch' then
  local jobId = ARGV[A]
  if not newJob(jobId, ARGV[A + 1], ARGV[A + 2], ARGV[A + 3], ARGV[A + 4]) then return noeffect('InvariantFault', 'id_collision') end
  result = { jobId = jobId, disposition = 'Created' }

elseif opName == 'dispatchMany' then
  local queue, maxExec, items = ARGV[A], ARGV[A + 1], {}
  for i = A + 2, #ARGV, 2 do
    if not newJob(ARGV[i], queue, ARGV[i + 1], maxExec, '') then return noeffect('InvariantFault', 'id_collision') end
    items[#items + 1] = ARGV[i]
  end
  result = { items = items }

elseif opName == 'reserve' then
  local worker, token, attempt, leaseMs = ARGV[A], ARGV[A + 1], ARGV[A + 2], tonumber(ARGV[A + 3])
  local ids = redis.call('ZRANGEBYSCORE', P .. 'pending', '-inf', now, 'LIMIT', 0, 1)
  if #ids == 0 then return noeffect('NoWork', 'no_work') end
  local id = ids[1]
  local j = hgetall(P .. 'job:' .. id)
  if j.state ~= 'pending' and j.state ~= 'retrying' then return noeffect('InvariantFault', 'invalid_transition') end
  ctr(j.state, -1)
  ctr('reserved', 1)
  j.state, j.attempt, j.worker, j.token, j.leaseExp, j.rev = 'reserved', attempt, worker, token, S(now + leaseMs), dadd(j.rev, 1)
  H('job:' .. id, j)
  ZR('pending', id)
  Z('leases', j.leaseExp, id)
  result = { jobId = id, attemptId = attempt, workerInstanceId = worker, ownershipToken = token, generation = j.gen, revision = j.rev, leaseExpiresAt = j.leaseExp }

elseif opName == 'begin' or opName == 'renew' or opName == 'finish' then
  local id = ARGV[A]
  local j, reply = owned(id, ARGV[A + 1], ARGV[A + 2], ARGV[A + 3], ARGV[A + 4])
  if not j then return reply end
  if opName == 'begin' then
    if j.state ~= 'reserved' then return noeffect('InvalidState', 'invalid_state') end
    if tonumber(j.started) >= tonumber(j.maxExec) then return noeffect('InvalidState', 'execution_budget_exhausted') end
    ctr('reserved', -1)
    ctr('running', 1)
    j.state, j.started, j.rev = 'running', S(tonumber(j.started) + 1), dadd(j.rev, 1)
    H('job:' .. id, j)
    result = { jobId = id, attemptId = j.attempt, executionNumber = j.started, revision = j.rev, leaseExpiresAt = j.leaseExp }
  elseif opName == 'renew' then
    j.leaseExp = S(now + tonumber(ARGV[A + 5]))
    H('job:' .. id, j)
    Z('leases', j.leaseExp, id)
    result = { jobId = id, leaseExpiresAt = j.leaseExp }
  else
    local outcome = ARGV[A + 5]
    if outcome ~= 'succeeded' and outcome ~= 'failed' then return noeffect('InvalidRequest', 'invalid_request') end
    if j.state ~= 'running' then return noeffect('InvalidState', 'invalid_state') end
    ctr('running', -1)
    ctr(outcome, 1)
    local attempt = j.attempt
    clearOwnership(j)
    j.state, j.terminalAt, j.rev = outcome, S(now), dadd(j.rev, 1)
    H('job:' .. id, j)
    ZR('leases', id)
    result = { jobId = id, attemptId = attempt, state = outcome, revision = j.rev }
  end

elseif opName == 'recover' then
  local ids = redis.call('ZRANGEBYSCORE', P .. 'leases', '-inf', now, 'LIMIT', 0, 1)
  if #ids == 0 then return noeffect('NoWork', 'no_work') end
  local id = ids[1]
  local j = hgetall(P .. 'job:' .. id)
  local to
  if j.state == 'reserved' then to = (j.started == '0') and 'pending' or 'retrying'
  elseif j.state == 'running' then to = (tonumber(j.started) < tonumber(j.maxExec)) and 'retrying' or 'failed'
  else return noeffect('InvariantFault', 'invalid_transition') end
  ctr(j.state, -1)
  ctr(to, 1)
  local attempt = j.attempt
  clearOwnership(j)
  j.state, j.rev, j.eligibleAt = to, dadd(j.rev, 1), S(now)
  H('job:' .. id, j)
  ZR('leases', id)
  if to ~= 'failed' then Z('pending', S(now), id) end
  result = { jobId = id, attemptId = attempt, state = to, revision = j.rev }

elseif opName == 'scheduleFire' then
  local name, slot, jobId, payload = ARGV[A], tonumber(ARGV[A + 1]), ARGV[A + 2], ARGV[A + 3]
  local s = hgetall(P .. 'sched:' .. name)
  if slot <= tonumber(s.lastSlot or '-1') then return noeffect('InvalidState', 'occurrence_exists') end
  if not newJob(jobId, 'default', payload, '3', 'sched:' .. name .. ':' .. S(slot)) then return noeffect('InvariantFault', 'id_collision') end
  H('sched:' .. name, { lastSlot = S(slot), fired = dadd(s.fired, 1) })
  ctr('outbox', 1)
  H('outbox:' .. jobId, { kind = 'job_created', jobId = jobId, at = S(now) })
  Z('outboxIdx', S(now), jobId)
  audit('schedule_fire', { schedule = name, slot = S(slot), jobId = jobId })
  result = { jobId = jobId, slot = slot }

elseif opName == 'outboxAppend' then
  local msgId, body = ARGV[A], ARGV[A + 1]
  if redis.call('EXISTS', P .. 'outbox:' .. msgId) == 1 then return noeffect('InvalidState', 'message_exists') end
  ctr('outbox', 1)
  H('outbox:' .. msgId, { kind = 'message', body = body, at = S(now) })
  Z('outboxIdx', S(now), msgId)
  audit('outbox_append', { message = msgId })
  result = { messageId = msgId }

else
  return noeffect('UnsupportedVersion', 'operation_not_supported')
end

if not counterOk then return noeffect('InvariantFault', 'counter_limit') end
if counters then H('counters', counters) end
local resultJson = cjson.encode(result)
if receiptBearing then
  H('rcpt:' .. cmdId, { op = opName, fp = fp, status = 'Applied', decidedAt = S(now), result = resultJson, notAfter = S(notAfter) })
end

-- Bounds are checked before anything is written: an oversized command never leaves an intent behind.
if #ops > MAX_OPS then return noeffect('InvalidRequest', 'intent_too_many_mutations') end
local keys, nkeys = {}, 0
for _, op in ipairs(ops) do
  if not keys[op[2]] then keys[op[2]] = true nkeys = nkeys + 1 end
end
if nkeys > MAX_KEYS then return noeffect('InvalidRequest', 'intent_too_many_keys') end

local newRev = dadd(meta[3] or '0', 1)
local reply = { 'APPLIED', 'Applied', '', resultJson, S(now) }
local intent = cjson.encode({ id = cmdId, rev = newRev, ops = ops, reply = reply })
if #intent > MAX_BYTES then return noeffect('InvalidRequest', 'intent_too_large') end

-- Write 1: the whole intent and the guard, in one command.
w('HSET', META, 'guard', cmdId, 'intent', intent, 'intentsha', redis.sha1hex(intent))
if #ops > INLINE_OPS then
  injectAtEnd()
  return { 'ACCEPTED', cmdId }
end
for _, op in ipairs(ops) do apply(op) end
publish(newRev)
injectAtEnd()
return reply
