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
local BATCH_INLINE_OPS, BATCH_INLINE_KEYS, BATCH_INLINE_BYTES, BATCH_INLINE_ARGS = 256, 230, 524288, 8192

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
local byte = string.byte
function U.blt(a, b)
  -- Most keys differ at the first byte, so settle that before any loop.
  local x, y = byte(a, 1), byte(b, 1)
  if x ~= y then return (x or -1) < (y or -1) end
  -- Job hash fields share long prefixes (a:<attempt id>:...), so the rest is compared eight bytes per string.byte call.
  -- A missing byte (past the end) is -1, so a proper prefix orders first.
  local n, m = #a, #b
  if m < n then n = m end
  local i = 2
  while i <= n do
    local a1, a2, a3, a4, a5, a6, a7, a8 = byte(a, i, i + 7)
    local b1, b2, b3, b4, b5, b6, b7, b8 = byte(b, i, i + 7)
    if a1 ~= b1 then return (a1 or -1) < (b1 or -1) end
    if a2 ~= b2 then return (a2 or -1) < (b2 or -1) end
    if a3 ~= b3 then return (a3 or -1) < (b3 or -1) end
    if a4 ~= b4 then return (a4 or -1) < (b4 or -1) end
    if a5 ~= b5 then return (a5 or -1) < (b5 or -1) end
    if a6 ~= b6 then return (a6 or -1) < (b6 or -1) end
    if a7 ~= b7 then return (a7 or -1) < (b7 or -1) end
    if a8 ~= b8 then return (a8 or -1) < (b8 or -1) end
    i = i + 8
  end
  return #a < #b
end

function U.sorted(t)
  local k = {}
  for key in pairs(t) do k[#k + 1] = key end
  if #k > 1 then table.sort(k, U.blt) end
  return k
end

local NULL = setmetatable({}, { __tostring = function() return 'null' end })
local ARR = {}
U.NULL = NULL
function U.arr(t) return setmetatable(t or {}, ARR) end
function U.raw(s) return { __raw = s } end
function U.nn(v) if v == nil or v == cjson.null or v == NULL then return nil end return v end

local esc = { ['"'] = '\\"', ['\\'] = '\\\\', ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' }
local sfind, sformat, sgsub, concat = string.find, string.format, string.gsub, table.concat
local function escape(c) return esc[c] or sformat('\\u%04x', string.byte(c)) end
-- Quoted texts are kept for the call: a claim's ids, states and names repeat, and its compact receipt quotes them again.
local QS = {}
function U.str(s)
  local q = QS[s]
  if q then return q end
  if not sfind(s, '[%c"\\]') then q = '"' .. s .. '"' -- ids, names and hex need no escaping
  else q = '"' .. sgsub(s, '[%c"\\]', escape) .. '"' end
  QS[s] = q
  return q
end

-- Objects of one shape (a claim's items, events' details) repeat within a call, so each key set is sorted once per call: a trie
-- walked by the keys in pairs order ends at a node holding the sorted list (END, a table, cannot clash with a key). Exact for any
-- keys; a different pairs order for the same set only costs another sort. Script locals live for one call, so nothing is shared.
local KT, END, KS = {}, {}, {}
local function sorted_keys(v)
  local node = KT
  for key in pairs(v) do
    local nx = node[key]
    if not nx then
      nx = {}
      node[key] = nx
    end
    node = nx
  end
  local s = node[END]
  if not s then
    -- A new key set: list it (a second walk of the same table lists the same keys) and sort it once.
    s = {}
    for key in pairs(v) do s[#s + 1] = key end
    if #s > 1 then table.sort(s, U.blt) end
    node[END] = s
  end
  return s
end

-- JSON with integers only (no float formatting), object keys in byte order, U.raw for preformatted values. The pieces go into one
-- buffer joined once at the end, instead of building a string per value and per object.
local CJNULL = cjson.null
local function put(v, b, n)
  local t = type(v)
  n = n + 1
  if t == 'string' then
    b[n] = U.str(v)
    return n
  end
  if t == 'number' then
    b[n] = sformat('%d', v)
    return n
  end
  if v == nil or v == NULL or v == CJNULL then
    b[n] = 'null'
    return n
  end
  if t == 'boolean' then
    b[n] = v and 'true' or 'false'
    return n
  end
  if v.__raw then
    b[n] = v.__raw
    return n
  end
  if getmetatable(v) == ARR then
    b[n] = '['
    for i = 1, (v.n or #v) do
      if i > 1 then
        n = n + 1
        b[n] = ','
      end
      n = put(v[i], b, n)
    end
    n = n + 1
    b[n] = ']'
    return n
  end
  local keys = sorted_keys(v)
  if #keys == 0 then
    b[n] = '{}'
    return n
  end
  for i = 1, #keys do
    local k = keys[i]
    local ks = KS[k]
    if not ks then
      ks = U.str(k) .. ':'
      KS[k] = ks
    end
    if i > 1 then n = n + 1 end
    b[n] = (i == 1 and '{' or ',') .. ks
    n = put(v[k], b, n)
  end
  n = n + 1
  b[n] = '}'
  return n
end

function U.enc(v)
  local b = {}
  return concat(b, '', 1, put(v, b, 0))
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

-- Writes several fields of a hash; on a hash with no other write yet in this command it skips the per-field bookkeeping. A nil
-- value is skipped when skipnil is set; otherwise it takes the plain path, which records a field deletion. (The lists are kept in
-- bytewise order, which a test checks, though writes are no longer sorted.)
function U.hset_sorted(k, names, vals, skipnil)
  local r = rec(P .. k, 'h')
  local fresh = next(r.set) == nil and next(r.del) == nil and not r.kill
  for i = 1, #names do
    local f = names[i]
    local v = vals[f]
    if v == nil then
      if not skipnil then
        fresh = false
        U.hset(k, f, nil)
      end
    elseif fresh then
      r.set[f] = tostring(v)
    else
      U.hset(k, f, v)
    end
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

local JREC = {}
function U.jw(j, f, v)
  -- j is the job as this command sees it (stored fields plus its own writes). A field it lacks has nothing to delete, and a field that already holds the value is not written again.
  local s = v ~= nil and tostring(v) or nil
  if j[f] == s then return end
  j[f] = s
  -- U.hset('job:' .. id, f, v), with the job's overlay record found once per job rather than by building its key for every field.
  local r = JREC[j.id]
  if not r then
    r = rec(P .. 'job:' .. j.id, 'h')
    JREC[j.id] = r
  end
  if s == nil then
    r.set[f] = nil
    r.del[f] = true
  else
    r.set[f] = s
    r.del[f] = nil
  end
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
  -- idx:terminal orders ended jobs by end time for the job stats read (16 §2.4); callers write terminalat first.
  if U.TERMINAL[state] and j.terminalat then
    U.zadd('idx:terminal', tonumber(j.terminalat), j.id)
  else
    U.zrem('idx:terminal', j.id)
  end
end

-- Whether a job is already in the jobtypes counts: meta.bf is the StatsIndex walk's (created, id) cursor over idx:created, or
-- 'done'. A job the walk has not reached yet is counted by the walk itself, so writers leave it alone (16 §2.6).
function U.counted(created, id)
  local bf = U.meta and U.meta.bf
  if bf == 'done' then return true end
  if not bf then return false end
  local s, k = string.match(bf, '^(%d+):(.*)$')
  s = tonumber(s)
  return created < s or (created == s and not U.blt(k, id))
end

-- jobtypes: stored jobs per (queue, job name), so ListJobTypes reads the distinct pairs without a scan.
function U.jobtype(queue, name, delta)
  local f = queue .. '\0' .. name
  local n = tonumber(U.hget('jobtypes', f) or '0') + delta
  U.hset('jobtypes', f, n > 0 and U.S(n) or nil)
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

-- The result as text. The receipt, the intent's reply and the final reply all need it, so a decided result is encoded once.
local function result_text(r)
  if type(r.result) ~= 'table' then return r.result end
  local t = r.rtext
  if not t then
    t = U.enc(r.result)
    r.rtext = t
  end
  return t
end

local function reply_array(r, replayed)
  local a = { 'R', r.status, r.effect, replayed and '1' or '0', r.code or '', r.detail or '', r.decided or '',
    result_text(r) or '',
    r.conflict or '', r.blocked or '', r.resumeAt and U.S(r.resumeAt) or '', r.resumeJob or '' }
  return a
end

-- known: id -> { headers, created } this command already holds for a job it claimed (the claim does not change either field).
local function with_payloads(a, ids, known)
  local n = 0
  local tail = {}
  for _, id in ipairs(ids or {}) do
    local j = known and known[id] or redis.call('HMGET', P .. 'job:' .. id, 'headers', 'created')
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
  -- U.zadd('events', id, ids) and U.hset('events:data', ids, row) on records found once (created in that order, as before).
  local ze, he = rec(P .. 'events', 'z'), rec(P .. 'events:data', 'h')
  for _, e in ipairs(EV) do
    if id > MAX_SCORE then U.abort('InvariantFault', 'counter_limit') end
    -- The row's keys are fixed, so they are written in bytewise order instead of being sorted.
    local ids = U.S(id)
    local row = '{"actorId":' .. (e.actor and U.str(e.actor) or 'null') .. ',"attemptId":' .. (e.attempt and U.str(e.attempt) or 'null')
      .. ',"commandId":' .. U.str(e.commandId or CMD) .. ',"details":' .. (e.details.__raw or U.enc(e.details)) .. ',"eventId":' .. ids
      .. ',"jobId":' .. (e.job and U.str(e.job) or 'null') .. ',"kind":' .. U.str(e.kind) .. ',"occurredAt":' .. NOWS .. '}'
    ze.set[ids], ze.del[ids] = ids, nil
    he.set[ids], he.del[ids] = row, nil
    id = id + 1
  end
  U.hset('meta', 'nextevent', U.S(id))
end

-- OPBYTES: the ops' part of packed_bound, summed by build_ops as it builds them (5 per list, then length plus 9 per text, 18 per
-- other value), so the bound needs no second pass over every op.
local OPBYTES = 0
local function vb(x) return type(x) == 'string' and #x + 9 or 18 end
local function build_ops()
  local ops, keys, b = {}, 0, 0
  for _, k in ipairs(WO) do
    local r = W[k]
    local s = string.sub(k, #P + 1)
    local sb = #s + 9
    if s ~= 'meta' then keys = keys + 1 end
    if r.kill then
      ops[#ops + 1] = { 'D', s }
      b = b + 15 + sb
    end
    if r.t == 's' then
      if r.val ~= nil then
        ops[#ops + 1] = { 'S', s, r.val }
        b = b + 15 + sb + vb(r.val)
      end
    else
      -- The order of fields inside one write does not change what is stored, so nothing is sorted here.
      local h = r.t == 'h'
      if next(r.del) ~= nil and not r.kill then
        local op = { h and 'HD' or 'ZR', s }
        b = b + 16 + sb
        for f in pairs(r.del) do
          op[#op + 1] = f
          b = b + (type(f) == 'string' and #f + 9 or 18)
        end
        ops[#ops + 1] = op
      end
      local op, n, ob = { h and 'H' or 'Z', s }, 2, 0
      for f, v in pairs(r.set) do
        if h then op[n + 1], op[n + 2] = f, v else op[n + 1], op[n + 2] = v, f end
        n = n + 2
        ob = ob + (type(f) == 'string' and #f + 9 or 18) + (type(v) == 'string' and #v + 9 or 18)
      end
      if n > 2 then
        ops[#ops + 1] = op
        b = b + 15 + sb + ob
      end
    end
  end
  OPBYTES = b
  return ops, keys
end

-- Tells listening workers which queues just received a job that is due now, or a job that is now the earliest not-yet-due job of
-- its lane (an idle worker's last probe could not name it, so without the hint it would sleep its whole back-off past the due
-- time). A hint published once the intent is whole (never required; workers keep polling). Sent with redis.call directly, so it is
-- not counted as a write of the intent.
function U.wake(ops)
  local seen = {}
  for i = 1, #ops do
    local op = ops[i]
    local queue = op[1] == 'Z' and string.sub(op[2], 1, 6) == 'ready:' and string.match(op[2], '^ready:(.*):[^:]*$')
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

-- Publishes intent batches from position pos; returns the next position or nil when published. unstored: the intent was never
-- stored (a direct projection, 22 §4.1), so publication only moves rev.
function U.project(intent, pos, budget, unstored)
  local last = math.min(#intent.ops, pos + budget - 1)
  for i = pos, last do apply(intent.ops[i]) end
  if last < #intent.ops then
    w('HSET', META, 'ipos', U.S(last + 1))
    return last + 1
  end
  if unstored then
    w('HSET', META, 'rev', intent.rev)
  else
    w('HSET', META, 'rev', intent.rev, 'guard', '', 'intent', '', 'intentsha', '', 'ipos', '')
  end
  U.wake(intent.ops)
  return nil
end

local function inline_budget(ops, keys, bytes)
  if #ops <= INLINE or #ops > BATCH_INLINE_OPS or keys > BATCH_INLINE_KEYS or bytes > BATCH_INLINE_BYTES then
    return INLINE
  end
  if OP ~= 'DispatchMany' and OP ~= 'FinishMany' and not U.batchy then return INLINE end

  -- A single after-image can carry hundreds of hash fields or sorted-set members.
  local arguments = 0
  for _, op in ipairs(ops) do
    arguments = arguments + #op - 2 -- exclude the operation and key
    if arguments > BATCH_INLINE_ARGS then return INLINE end
  end
  return #ops
end

-- An upper bound of #cmsgpack.pack(intent), far cheaper than packing: a header is at most 5 bytes and a number packs to at most 9
-- bytes, so length plus 9 bounds a text value and 18 any other.
local function packed_bound(intent)
  local n = 128
  local function list(t)
    n = n + 5
    for i = 1, #t do
      local v = t[i]
      n = n + (type(v) == 'string' and #v + 9 or 18)
    end
  end
  list({ intent.id, intent.op, intent.rev, intent.decided })
  list(intent.reply)
  return n + OPBYTES -- the ops, summed by build_ops
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
  -- Packed only when it is stored or when the bound cannot settle a size limit; the limits below see the same answer either way.
  local bound, bytes = packed_bound(intent), nil
  local function size(limit)
    if bound <= limit then return bound end
    bytes = bytes or cmsgpack.pack(intent)
    return #bytes
  end
  if size(MAX_BYTES) > MAX_BYTES then return U.noeffect('InvalidRequest', 'intent_too_large') end
  local budget = inline_budget(ops, keys, size(BATCH_INLINE_BYTES))
  if OP == 'DispatchGroup' then
    -- A grouped dispatch publishes inline only within measured shape limits; larger or slower shapes are declined
    -- before the first write so the client sends each command alone with its own identity.
    local args = 0
    for _, op in ipairs(ops) do args = args + #op - 2 end
    local t = redis.call('TIME')
    local elapsed = (tonumber(t[1]) - tonumber(t0[1])) * 1000000 + tonumber(t[2]) - tonumber(t0[2])
    if #ops > 80 or keys > 72 or size(262144) > 262144 or args > 4096 or elapsed > 5000 then
      return U.noeffect('InvalidRequest', 'group_fallback')
    end
    budget = #ops
  end
  local more
  if budget >= #ops then
    -- Direct projection (22 §4.1): one script call publishes the whole command, and Redis persists and replicates a script's
    -- writes as one MULTI/EXEC, so the intent is only needed if a write fails partway (wrong key type, a key that left a cluster
    -- node, an injected fault). Then it is stored after the fact, exactly as the stored-first order would have left it (guard,
    -- intent, ipos 1, the writes made so far), and the error goes on as before. The guard counts as write 1 for FailAfter.
    writes = writes + 1
    local ok, err = pcall(U.project, intent, 1, budget, true)
    if not ok then
      bytes = bytes or cmsgpack.pack(intent)
      redis.pcall('HSET', META, 'guard', CMD, 'intent', bytes, 'intentsha', redis.sha1hex(bytes), 'ipos', '1')
      error(err, 0)
    end
  else
    bytes = bytes or cmsgpack.pack(intent)
    w('HSET', META, 'guard', CMD, 'intent', bytes, 'intentsha', redis.sha1hex(bytes), 'ipos', '1')
    more = U.project(intent, 1, budget)
  end
  if FAIL > 0 and writes >= FAIL then error('INJECTED reply lost after write ' .. writes) end
  if more then return { pending = true } end
  return nil
end

-- ------------------------------------------------------------------ the frame

-- Receipt hash fields in bytewise order, the order U.sorted gave them.
local RECEIPT_FIELDS = { 'cls', 'code', 'decided', 'detail', 'fp', 'notafter', 'op', 'result', 'retain', 'status', 'ver' }
local COMPACT_OPS = { ReserveAndBegin = true, Finish = true }

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
      local text = U.cdec(rc[9])
      if not text then return reply_array(U.noeffect('UnsupportedVersion', 'receipt_codec_unsupported'), false) end
      local r = { status = rc[5], effect = 'A', code = rc[6] ~= '' and rc[6] or nil, detail = rc[7] ~= '' and rc[7] or nil,
        decided = rc[8], result = text }
      local a = reply_array(r, true)
      return with_payloads(a, M.replay_ids and M.replay_ids(cjson.decode(text)) or {})
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
  if r.nocommit then
    -- A replay-only group confirms its receipts like Dispatch replays do: a fresh write, no intent, no revision change.
    for _, key in ipairs(r.confirmations or {}) do w('HSET', P .. key, 'confirmed', NOWS) end
    return with_payloads(reply_array(r, false), r.payloads, r.known)
  end

  if FP ~= '' then
    local na = NOT_AFTER
    -- Claim and finish receipts store a compact result and leave out empty code and detail fields (a missing field reads as empty).
    local compact = COMPACT_OPS[OP] and U.cenc(r)
    U.hset_sorted('rcpt:' .. CMD, RECEIPT_FIELDS, { cls = CLS, op = OP, ver = '1', fp = FP, notafter = U.S(na), retain = U.S(na + FLOOR),
      status = r.status, code = r.code or (not COMPACT_OPS[OP] and '' or nil), detail = r.detail or (not COMPACT_OPS[OP] and '' or nil),
      decided = r.decided, result = compact or r.rcpt_text or result_text(r) }, true)
    U.zadd('rcpt:exp:' .. CLS, na + FLOOR, CMD)
  end

  local ok2, c = pcall(commit, m, r)
  if not ok2 then
    if type(c) == 'table' and c[ABORT] then return reply_array(c[ABORT], false) end
    error(c)
  end
  if c and c.effect then return reply_array(c, false) end
  if c and c.pending then return { 'P', CMD } end
  return with_payloads(reply_array(r, false), r.payloads, r.known)
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
