-- ReserveAndBegin/1, lean path: a claim whose candidates have no concurrency or rate scope, no cancel request and execution budget
-- left (U.reserve_and_begin with U.reserve_job and U.begin_job, every item a plain Applied one). NoWork is answered here too.
local m = L.frame()
if not m then return GENERIC() end
local leaseMs = tonumber(ARGS(4))
if not leaseMs or leaseMs <= 0 then return GENERIC() end
local queue, priority = ARGS(2), ARGS(3)
local qrow = redis.call('HGET', P .. 'queues', queue)
if not qrow or cjson.decode(qrow).paused then return GENERIC() end

-- U.candidates: due entries of the lane with a supported contract, in (eligible_at, job_id) order after the cursor.
local contracts = {}
for _, c in ipairs(cjson.decode(ARGS(8))) do contracts[c[1] .. '\0' .. L.S(c[2]) .. '\0' .. c[3] .. '\0' .. L.S(c[4])] = true end
local limit = tonumber(ARGS(9))
local afterAt = ARGS(6) ~= '' and tonumber(ARGS(6)) or nil
local afterJob = ARGS(7)
local key = P .. 'ready:' .. queue .. ':' .. priority
local cands, jobs, offset, examined = {}, {}, 0, 0
local min = afterAt and L.S(afterAt) or '-inf'
while #cands < limit and examined < 5000 do
  local page = redis.call('ZRANGE', key, min, NOWS, 'BYSCORE', 'LIMIT', offset, offset == 0 and math.min(100, limit + 4) or 100, 'WITHSCORES')
  if #page == 0 then break end
  offset = offset + #page / 2
  for i = 1, #page, 2 do
    local id, score = page[i], tonumber(page[i + 1])
    examined = examined + 1
    if not (afterAt and score == afterAt and not L.blt(afterJob, id)) then
      -- The whole job is read here (U.candidates reads five fields, then U.reserve_job the job): most candidates are claimed.
      local j = L.job(id)
      if (j.state == 'pending' or j.state == 'retrying')
        and contracts[(j.name or '') .. '\0' .. (j.pver or '') .. '\0' .. (j.profile or '') .. '\0' .. (j.profver or '')] then
        cands[#cands + 1] = id
        jobs[#jobs + 1] = j
        if #cands >= limit then break end
      end
    end
  end
end
if #cands == 0 then return { 'R', 'NoWork', 'N', '0', 'no_work', '', NOWS, '', '', '', '', '' } end
if #cands > 64 then return GENERIC() end

local tokens, entropy = cjson.decode(ARGS(5)), cjson.decode(ARGS(10))
local worker = ARGS(1)
local expiry = NOW + leaseMs
local lease = L.S(expiry)
local vs, bs = {}, {}
for i, id in ipairs(cands) do
  local j = jobs[i]
  local gen = j.gen
  local token, ent = tokens[i], entropy[i]
  local started, max = tonumber(j['g:' .. (gen or '') .. ':started']), tonumber(j['g:' .. (gen or '') .. ':max'])
  if not (j.state == 'pending' or j.state == 'retrying') or (j.scopes and j.scopes ~= '') or (j.rates and j.rates ~= '') or j.cancelat
    or not gen or not started or not max or started >= max or type(token) ~= 'string' or type(ent) ~= 'string'
    or not string.match(ent, '^%x%x%x%x') or not j.created
    or type(j.name) ~= 'string' or type(j.profile) ~= 'string' or type(j.queue) ~= 'string' or type(j.priority) ~= 'string' then
    return GENERIC()
  end
  -- U.uuid7: a UUIDv7 at the decision time from the client's entropy.
  local att = sformat('%012x', NOW) .. '7' .. ssub(ent, 1, 3) .. sformat('%x', (tonumber(ssub(ent, 4, 4), 16) % 4) + 8) .. ssub(ent, 5, 19)
  local number, rrev = L.inc(j.rcount), L.inc(j.rev)
  local brev, xnum, txnum = rrev and L.inc(rrev), L.inc(j['g:' .. gen .. ':started']), L.inc(j.xcount)
  if not (number and brev and xnum and txnum) or j['a:' .. att .. ':fin'] or j['a:' .. att .. ':exec'] then return GENERIC() end
  vs[i] = { job = id, gen = gen, rrev = rrev, att = att, worker = worker, token = token, lease = lease, rnum = number, name = j.name,
    pver = j.pver, profile = j.profile, profver = j.profver, queue = j.queue, priority = j.priority, elig = j.eligible,
    timeout = j['g:' .. gen .. ':timeout'], maxun = j['g:' .. gen .. ':maxun'], max = j['g:' .. gen .. ':max'], retry = j['g:' .. gen .. ':retry'],
    uretry = j['g:' .. gen .. ':uretry'] }
  bs[i] = { brev = brev, xnum = xnum, txnum = txnum, auth = NOWS }
end
if not L.can_commit(m) then return GENERIC() end

-- Decided: the after-image of reserve then begin for each job (fields written once with their final values).
local items, packed, known = {}, {}, {}
for i, j in ipairs(jobs) do
  local v, b, id, gen = vs[i], bs[i], j.id, j.gen
  local a = 'a:' .. v.att .. ':'
  local old = j.state
  L.jw(j, a .. 'gen', gen)
  L.jw(j, a .. 'res', v.rnum)
  L.hset('attempts', v.att, id)
  L.jw(j, a .. 'worker', worker)
  L.jw(j, a .. 'token', v.token)
  L.jw(j, a .. 'reserved', NOWS)
  L.jw(j, 'rcount', v.rnum)
  L.jw(j, 'rev', b.brev)
  L.jw(j, 'attempt', v.att)
  L.jw(j, 'worker', worker)
  L.jw(j, 'token', v.token)
  L.jw(j, 'lease', lease)
  L.jw(j, 'updated', NOWS)
  L.jw(j, 'state', 'running')
  L.jw(j, 'g:' .. gen .. ':started', b.xnum)
  L.jw(j, 'g:' .. gen .. ':unstarted', '0')
  L.jw(j, 'xcount', b.txnum)
  L.jw(j, a .. 'exec', b.xnum)
  L.jw(j, a .. 'auth', NOWS)
  -- U.set_state(reserved) then U.set_state(running): the net index changes.
  L.zrem('idx:created:s:' .. old, id)
  L.zrem('idx:created:s:reserved', id)
  L.zadd('idx:created:s:running', j.created, id)
  L.zrem('ready:' .. j.queue .. ':' .. j.priority, id)
  L.zadd('leases', lease, id)
  L.zrem('idx:terminal', id)
  L.event('attempt_reserved', id, v.att, '{"generation":' .. sformat('%d', tonumber(gen)) .. ',"reservationNumber":' .. v.rnum .. '}')
  L.event('attempt_started', id, v.att, '{"executionNumber":' .. sformat('%d', tonumber(b.xnum)) .. ',"generation":' .. sformat('%d', tonumber(gen)) .. '}')
  items[i] = L.claim_text(i - 1, v, b)
  local pv, pb = vs[i - 1], bs[i - 1]
  packed[i] = '[' .. (i - 1) .. ',' .. L.texts(v, L.RKEYS, pv) .. ',' .. L.texts(b, L.BKEYS, pb) .. ']'
  known[id] = { j.headers or false, j.created or false }
end
if FP ~= '' then L.receipt(CMD, CLS, OP, FP, NOT_AFTER, FLOOR, '{"~":1,"c":[' .. concat(packed, ',') .. ']}') end
local reply = L.reply('{"items":[' .. concat(items, ',') .. ']}')
L.commit(m, reply)
return L.with_payloads(reply, cands, known)
