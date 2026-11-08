-- Runwick Redis write prelude (22 §4.1). KEYS[1] is the application's meta hash rw:{<hex>}:meta; every other key is
-- built from its prefix. One script call: gates, server time, receipt, decision, intent, projection, publication.
-- ARGV: 1 failAfter (0 = off), 2 store id, 3 operation, 4 command id, 5 fingerprint hex ('' = no receipt), 6 NotAfter ms,
--       7 deadline cap ms, 8 retention floor ms, 9 retention class, 10 gate kind, 11 known capabilities, 12.. operation arguments.
local META = KEYS[1]
if not string.match(META, '^rw:{[0-9a-f][0-9a-f]+}:meta$') or #META > 266 then
  return { 'R', 'InvariantFault', 'N', '0', 'invalid_transition', 'key prefix', '' }
end
local P = string.sub(META, 1, #META - 4)
local A = 12
local FAIL = tonumber(ARGV[1]) or 0
local STORE, OP, CMD, FP = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local NOT_AFTER, CAP, FLOOR, CLS, GATE = tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8]), ARGV[9], ARGV[10]
local INLINE, MAX_OPS, MAX_KEYS, MAX_BYTES, MAX_SCORE = 100, 1024, 256, 4194304, 9007199254740991

local U = {}   -- helpers namespace (keeps the chunk under Lua's local-variable limit)

-- ------------------------------------------------------------------ numbers, text, JSON

function U.S(n)
  if type(n) == 'table' then return n.__raw end
  if type(n) == 'string' then return n end
  return string.format('%d', n)
end

local t0 = redis.call('TIME')
local NOW = tonumber(t0[1]) * 1000 + math.floor(tonumber(t0[2]) / 1000)
local NOWS = U.S(NOW)

-- Exact +1 on a non-negative int64 decimal string; nil past the int64 maximum (counter_limit).
local MAXI = '9223372036854775807'
function U.inc(s)
  s = s or '0'
  if s == MAXI then return nil end
  local out, i = '', #s
  while i >= 1 do
    local c = string.byte(s, i) - 48
    if c < 9 then return string.sub(s, 1, i - 1) .. (c + 1) .. out end
    out = '0' .. out
    i = i - 1
  end
  return '1' .. out
end

-- Bytewise a < b (never Lua '<' on strings: it follows the server's LC_COLLATE).
function U.blt(a, b)
  local n = math.min(#a, #b)
  for i = 1, n do
    local x, y = string.byte(a, i), string.byte(b, i)
    if x ~= y then return x < y end
  end
  return #a < #b
end

function U.sorted(t)
  local k = {}
  for key in pairs(t) do k[#k + 1] = key end
  table.sort(k, U.blt)
  return k
end

local NULL = setmetatable({}, { __tostring = function() return 'null' end })
local ARR = {}
U.NULL = NULL
function U.arr(t) return setmetatable(t or {}, ARR) end
function U.raw(s) return { __raw = s } end
function U.nn(v) if v == nil or v == cjson.null or v == NULL then return nil end return v end

local esc = { ['"'] = '\\"', ['\\'] = '\\\\', ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' }
function U.str(s)
  return '"' .. string.gsub(s, '[%c"\\]', function(c) return esc[c] or string.format('\\u%04x', string.byte(c)) end) .. '"'
end

-- JSON with integers only (no float formatting), object keys in byte order, U.raw for preformatted values.
function U.enc(v)
  if v == nil or v == NULL or v == cjson.null then return 'null' end
  local t = type(v)
  if t == 'string' then return U.str(v) end
  if t == 'number' then return string.format('%d', v) end
  if t == 'boolean' then return v and 'true' or 'false' end
  if v.__raw then return v.__raw end
  local parts = {}
  if getmetatable(v) == ARR then
    for i = 1, (v.n or #v) do parts[i] = U.enc(v[i]) end
    return '[' .. table.concat(parts, ',') .. ']'
  end
  for _, k in ipairs(U.sorted(v)) do parts[#parts + 1] = U.str(k) .. ':' .. U.enc(v[k]) end
  return '{' .. table.concat(parts, ',') .. '}'
end

-- ------------------------------------------------------------------ refusals inside the decision (before any write)

local ABORT = {}
function U.abort(status, code, extra)
  local r = extra or {}
  r.status, r.code, r.effect = status, code, 'N'
  error({ [ABORT] = r })
end

function U.noeffect(status, code, extra)
  local r = extra or {}
  r.status, r.code, r.effect = status, code, 'N'
  return r
end

function U.applied(result, status, code, extra)
  local r = extra or {}
  r.status, r.code, r.effect, r.result = status or 'Applied', code, 'A', result
  return r
end

function U.must(v, code)
  if v == nil then U.abort('InvariantFault', code or 'counter_limit') end
  return v
end

-- ------------------------------------------------------------------ overlay: reads see this command's own writes

local W, WO = {}, {}
local function rec(k, t)
  local r = W[k]
  if not r then
    r = { t = t, set = {}, del = {}, kill = false }
    W[k] = r
    WO[#WO + 1] = k
  end
  return r
end

function U.hget(k, f)
  local r = W[P .. k]
  if r then
    local v = r.set[f]
    if v ~= nil then return v end
    if r.kill or r.del[f] then return nil end
  end
  local v = redis.call('HGET', P .. k, f)
  if v == false then return nil end
  return v
end

function U.hgetall(k)
  local t = {}
  local r = W[P .. k]
  if not (r and r.kill) then
    local a = redis.call('HGETALL', P .. k)
    for i = 1, #a, 2 do t[a[i]] = a[i + 1] end
  end
  if r then
    for f in pairs(r.del) do t[f] = nil end
    for f, v in pairs(r.set) do t[f] = v end
  end
  return t
end

function U.hset(k, f, v)
  local r = rec(P .. k, 'h')
  if v == nil then
    r.set[f] = nil
    r.del[f] = true
  else
    r.set[f] = tostring(v)
    r.del[f] = nil
  end
end

function U.zscore(k, m)
  local r = W[P .. k]
  if r then
    local v = r.set[m]
    if v ~= nil then return tonumber(v) end
    if r.kill or r.del[m] then return nil end
  end
  local v = redis.call('ZSCORE', P .. k, m)
  if v == false then return nil end
  return tonumber(v)
end

function U.zadd(k, score, m)
  local r = rec(P .. k, 'z')
  r.set[m] = U.S(score)
  r.del[m] = nil
end

function U.zrem(k, m)
  local r = rec(P .. k, 'z')
  r.set[m] = nil
  r.del[m] = true
end

-- Members with score > min (exclusive), counting this command's own changes.
function U.zcount_after(k, min)
  local n = redis.call('ZCOUNT', P .. k, '(' .. U.S(min), '+inf')
  local r = W[P .. k]
  if r then
    if r.kill then n = 0 end
    for m in pairs(r.del) do
      local o = redis.call('ZSCORE', P .. k, m)
      if not r.kill and o and tonumber(o) > min then n = n - 1 end
    end
    for m, s in pairs(r.set) do
      local o = (not r.kill) and redis.call('ZSCORE', P .. k, m) or false
      if o and tonumber(o) > min then n = n - 1 end
      if tonumber(s) > min then n = n + 1 end
    end
  end
  return n
end

function U.sget(k)
  local r = W[P .. k]
  if r then
    if r.val ~= nil then return r.val end
    if r.kill then return nil end
  end
  local v = redis.call('GET', P .. k)
  if v == false then return nil end
  return v
end

function U.sset(k, v)
  local r = rec(P .. k, 's')
  r.val = v
end

function U.del(k)
  local r = rec(P .. k, W[P .. k] and W[P .. k].t or 'h')
  r.kill, r.set, r.del, r.val = true, {}, {}, nil
end

-- ------------------------------------------------------------------ events

local EV = {}
function U.event(kind, job, attempt, details, actor)
  EV[#EV + 1] = { kind = kind, job = job, attempt = attempt, details = details or {}, actor = actor }
end

-- ------------------------------------------------------------------ JSON rows in aggregate hashes

function U.jget(k, f)
  local v = U.hget(k, f)
  if not v then return nil end
  return cjson.decode(v)
end

function U.jset(k, f, t) U.hset(k, f, U.enc(t)) end

-- ------------------------------------------------------------------ jobs: one hash per job, plus the indexes

function U.job(id)
  local t = U.hgetall('job:' .. id)
  if not t.state then return nil end
  t.id = id
  return t
end

function U.jw(j, f, v)
  j[f] = v ~= nil and tostring(v) or nil
  U.hset('job:' .. j.id, f, v)
end

function U.ready_key(j) return 'ready:' .. j.queue .. ':' .. j.priority end

-- State change: keeps the ready, lease and created-by-state indexes in step (22 §3.1, §7.2).
function U.set_state(j, state)
  local old = j.state
  if old ~= state then
    U.zrem('idx:created:s:' .. old, j.id)
    U.zadd('idx:created:s:' .. state, tonumber(j.created), j.id)
  end
  U.jw(j, 'state', state)
  if state == 'pending' or state == 'retrying' then
    U.zadd(U.ready_key(j), tonumber(j.eligible), j.id)
  else
    U.zrem(U.ready_key(j), j.id)
  end
  if state == 'reserved' or state == 'running' then
    U.zadd('leases', tonumber(j.lease), j.id)
  else
    U.zrem('leases', j.id)
  end
end

function U.gen(j, f) return j['g:' .. j.gen .. ':' .. f] end
function U.genw(j, f, v) U.jw(j, 'g:' .. j.gen .. ':' .. f, v) end
function U.att(j, a, f) return j['a:' .. a .. ':' .. f] end
function U.attw(j, a, f, v) U.jw(j, 'a:' .. a .. ':' .. f, v) end

function U.list(s)
  local t = {}
  if s and s ~= '' then for x in string.gmatch(s, '[^,]+') do t[#t + 1] = x end end
  return t
end

-- ------------------------------------------------------------------ gates (03 §10.1) and receipt (03 §8.2)

local function reply_array(r, replayed)
  local a = { 'R', r.status, r.effect, replayed and '1' or '0', r.code or '', r.detail or '', r.decided or '',
    type(r.result) == 'table' and U.enc(r.result) or (r.result or ''),
    r.conflict or '', r.blocked or '', r.resumeAt and U.S(r.resumeAt) or '', r.resumeJob or '' }
  return a
end

local function with_payloads(a, ids)
  local n = 0
  local tail = {}
  for _, id in ipairs(ids or {}) do
    local j = redis.call('HMGET', P .. 'job:' .. id, 'headers', 'created')
    local p = redis.call('GET', P .. 'pay:' .. id)
    if j[1] and p then
      n = n + 1
      tail[#tail + 1] = id
      tail[#tail + 1] = p
      tail[#tail + 1] = j[1]
      tail[#tail + 1] = j[2]
    end
  end
  a[#a + 1] = U.S(n)
  for _, v in ipairs(tail) do a[#a + 1] = v end
  return a
end

function U.known_caps()
  local t = {}
  for name, v in string.gmatch(ARGV[11] or '', '([^=;]+)=(%d+)') do t[name] = tonumber(v) end
  return t
end

local function gates(m)
  if m.store ~= STORE then return U.noeffect('UnsupportedVersion', 'store_identity_mismatch') end
  if m.mode ~= 'normal' then return U.noeffect('AdmissionClosed', 'store_maintenance') end
  if m.layout ~= '1' or m.epoch ~= '1' then return U.noeffect('UnsupportedVersion', 'schema_incompatible') end
  if GATE == 'c' then
    if tonumber(m.reqepoch or '1') > 1 then return U.noeffect('UnsupportedVersion', 'capability_unsupported') end
    local known = U.known_caps()
    for name, need in pairs(cjson.decode(m.caps or '{}')) do
      if not known[name] or tonumber(need) > known[name] then return U.noeffect('UnsupportedVersion', 'capability_unsupported') end
    end
  end
  local mode = m.admission
  if mode == 'open' then return nil end
  if mode == 'drain' then
    if GATE == 'd' or GATE == 'a' or GATE == 'r' then return nil end
    return U.noeffect('AdmissionClosed', 'application_draining')
  end
  if GATE == 'r' or GATE == 'a' then return nil end
  return U.noeffect('AdmissionClosed', 'application_maintenance')
end

-- ------------------------------------------------------------------ intent: store, project, publish (22 §4.1 steps 5–7)

local writes = 0
local function w(...)
  if FAIL > 0 and writes >= FAIL then error('INJECTED failure after write ' .. writes) end
  writes = writes + 1
  return redis.call(...)
end

local function apply(op)
  local c, k = op[1], P .. op[2]
  if c == 'D' then
    w('DEL', k)
  elseif c == 'S' then
    w('SET', k, op[3])
  else
    local cmd = (c == 'H' and 'HSET') or (c == 'HD' and 'HDEL') or (c == 'Z' and 'ZADD') or (c == 'ZR' and 'ZREM')
    local i = 3
    while i <= #op do
      local last = math.min(#op, i + 999)
      w(cmd, k, unpack(op, i, last))
      i = last + 1
    end
  end
end
U.apply = apply

local function flush_events(m)
  if #EV == 0 then return end
  local id = tonumber(m.nextevent or '1')
  for _, e in ipairs(EV) do
    if id > MAX_SCORE then U.abort('InvariantFault', 'counter_limit') end
    local row = { eventId = id, jobId = e.job or NULL, attemptId = e.attempt or NULL, commandId = CMD, kind = e.kind,
      occurredAt = NOW, details = e.details, actorId = e.actor or NULL }
    U.zadd('events', id, U.S(id))
    U.hset('events:data', U.S(id), U.enc(row))
    id = id + 1
  end
  U.hset('meta', 'nextevent', U.S(id))
end

local function build_ops()
  local ops, keys = {}, 0
  for _, k in ipairs(WO) do
    local r = W[k]
    local s = string.sub(k, #P + 1)
    if s ~= 'meta' then keys = keys + 1 end
    if r.kill then ops[#ops + 1] = { 'D', s } end
    if r.t == 's' then
      if r.val ~= nil then ops[#ops + 1] = { 'S', s, r.val } end
    else
      local d = U.sorted(r.del)
      if #d > 0 and not r.kill then
        local op = { r.t == 'h' and 'HD' or 'ZR', s }
        for _, f in ipairs(d) do op[#op + 1] = f end
        ops[#ops + 1] = op
      end
      local fs = U.sorted(r.set)
      if #fs > 0 then
        local op = { r.t == 'h' and 'H' or 'Z', s }
        for _, f in ipairs(fs) do
          if r.t == 'h' then
            op[#op + 1] = f
            op[#op + 1] = r.set[f]
          else
            op[#op + 1] = r.set[f]
            op[#op + 1] = f
          end
        end
        ops[#ops + 1] = op
      end
    end
  end
  return ops, keys
end

-- Publishes intent batches from position pos; returns the next position or nil when published.
function U.project(intent, pos, budget)
  local last = math.min(#intent.ops, pos + budget - 1)
  for i = pos, last do apply(intent.ops[i]) end
  if last < #intent.ops then
    w('HSET', META, 'ipos', U.S(last + 1))
    return last + 1
  end
  w('HSET', META, 'rev', intent.rev, 'guard', '', 'intent', '', 'intentsha', '', 'ipos', '')
  return nil
end

local function commit(m, r)
  flush_events(m)
  local ops, keys = build_ops()
  if keys > MAX_KEYS then return U.noeffect('InvalidRequest', 'intent_too_many_keys') end
  if #ops > MAX_OPS then return U.noeffect('InvalidRequest', 'intent_too_many_mutations') end
  local nextrev = U.inc(m.rev or '0')
  if not nextrev then return U.noeffect('InvariantFault', 'counter_limit') end
  local reply = reply_array(r, false)
  local intent = { id = CMD, op = OP, rev = nextrev, decided = NOWS, ops = ops, reply = reply }
  local bytes = cmsgpack.pack(intent)
  if #bytes > MAX_BYTES then return U.noeffect('InvalidRequest', 'intent_too_large') end
  w('HSET', META, 'guard', CMD, 'intent', bytes, 'intentsha', redis.sha1hex(bytes), 'ipos', '1')
  local more = U.project(intent, 1, INLINE)
  if FAIL > 0 and writes >= FAIL then error('INJECTED reply lost after write ' .. writes) end
  if more then return { pending = true } end
  return nil
end

-- ------------------------------------------------------------------ the frame

function U.run(M)
  local mr = redis.call('HGETALL', META)
  local m = {}
  for i = 1, #mr, 2 do m[mr[i]] = mr[i + 1] end
  if not m.store then
    if GATE == 'a' and M.seed then
      m = M.seed()
    else
      return reply_array(U.noeffect('AdmissionClosed', 'application_not_registered'), false)
    end
  end
  if m.store ~= STORE then return reply_array(U.noeffect('UnsupportedVersion', 'store_identity_mismatch'), false) end
  if m.guard and m.guard ~= '' then return { 'RECOVERING', m.guard } end
  local g = gates(m)
  if g then return reply_array(g, false) end
  U.meta = m

  if FP ~= '' then
    local rc = redis.call('HMGET', P .. 'rcpt:' .. CMD, 'cls', 'op', 'ver', 'fp', 'status', 'code', 'detail', 'decided', 'result')
    if rc[1] then
      if rc[1] ~= CLS or rc[2] ~= OP or rc[3] ~= '1' or rc[4] ~= FP then
        return reply_array(U.noeffect('CommandConflict', 'command_conflict'), false)
      end
      if not rc[9] or rc[9] == '' then return reply_array(U.noeffect('InvariantFault', 'receipt_incomplete'), false) end
      -- 22 §4.5: the confirmation marker is a fresh write before the replayed reply (not an intent, no rev change).
      redis.call('HSET', P .. 'rcpt:' .. CMD, 'confirmed', NOWS)
      local r = { status = rc[5], effect = 'A', code = rc[6] ~= '' and rc[6] or nil, detail = rc[7] ~= '' and rc[7] or nil,
        decided = rc[8], result = rc[9] }
      local a = reply_array(r, true)
      return with_payloads(a, M.replay_ids and M.replay_ids(cjson.decode(rc[9])) or {})
    end
  end

  if NOW >= NOT_AFTER then return reply_array(U.noeffect('CommandExpired', 'command_expired', { decided = NOWS }), false) end
  if FP ~= '' and NOT_AFTER - NOW > CAP then
    return reply_array(U.noeffect('InvalidRequest', 'deadline_too_far', { decided = NOWS }), false)
  end
  if M.seeded then M.seeded(m) end

  local ok, r = pcall(M.decide)
  if not ok then
    if type(r) == 'table' and r[ABORT] then
      r = r[ABORT]
    else
      return { 'E', tostring(r) }
    end
  end
  r.decided = r.decided or NOWS
  if r.effect ~= 'A' then return reply_array(r, false) end
  if r.nocommit then return with_payloads(reply_array(r, false), r.payloads) end

  if FP ~= '' then
    local na = NOT_AFTER
    U.hset('rcpt:' .. CMD, 'cls', CLS)
    U.hset('rcpt:' .. CMD, 'op', OP)
    U.hset('rcpt:' .. CMD, 'ver', '1')
    U.hset('rcpt:' .. CMD, 'fp', FP)
    U.hset('rcpt:' .. CMD, 'notafter', U.S(na))
    U.hset('rcpt:' .. CMD, 'retain', U.S(na + FLOOR))
    U.hset('rcpt:' .. CMD, 'status', r.status)
    U.hset('rcpt:' .. CMD, 'code', r.code or '')
    U.hset('rcpt:' .. CMD, 'detail', r.detail or '')
    U.hset('rcpt:' .. CMD, 'decided', r.decided)
    U.hset('rcpt:' .. CMD, 'result', type(r.result) == 'table' and U.enc(r.result) or r.result)
    U.zadd('rcpt:exp:' .. CLS, na + FLOOR, CMD)
  end

  local ok2, c = pcall(commit, m, r)
  if not ok2 then
    if type(c) == 'table' and c[ABORT] then return reply_array(c[ABORT], false) end
    error(c)
  end
  if c and c.effect then return reply_array(c, false) end
  if c and c.pending then return { 'P', CMD } end
  return with_payloads(reply_array(r, false), r.payloads)
end

local ARGS = function(i) return ARGV[A + i - 1] end

-- Read path (22 §4.6): nil, meta when open; otherwise the reply to return (RECOVERING, NONE code, or CLOSED).
function U.read_gate()
  local mr = redis.call('HGETALL', META)
  local m = {}
  for i = 1, #mr, 2 do m[mr[i]] = mr[i + 1] end
  if not m.store then return { 'NONE', 'application_not_registered' } end
  if STORE ~= '' and m.store ~= STORE then return { 'NONE', 'store_identity_mismatch' } end
  if m.guard and m.guard ~= '' then return { 'RECOVERING', m.guard } end
  if m.layout ~= '1' or m.epoch ~= '1' then return { 'NONE', 'schema_incompatible' } end
  if m.mode ~= 'normal' then return { 'CLOSED', m.rev or '0' } end
  U.meta = m
  return nil, m
end
