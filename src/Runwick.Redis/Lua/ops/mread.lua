-- Management reads (16 §2, §6; 22 §7.2) on the read path. ARGS: 1 sub-command, then its arguments. Every list examines at most
-- BUDGET index entries; a page that would need more answers BUDGET (the caller's query_budget_exceeded), never a partial page.
-- Reply: OK, rev, store time, then one JSON text per row (rows are the stored JSON, wrapped where a read adds a value).
local g, m = U.read_gate()
if g then return g end
if m.admission == 'maintenance' then return { 'NONE', 'application_maintenance' } end
local BUDGET = 5000
local SUB = ARGS(1)
local out = { 'OK', m.rev or '0', NOWS }
local function add(s) out[#out + 1] = s end
local function set_of(json)
  if json == '' then return nil end
  local t = {}
  for _, q in ipairs(cjson.decode(json)) do t[q] = true end
  return t
end
local function num(v) if v == nil or v == false then return NULL end return U.raw(v) end
local function str(v) if v == nil or v == false then return NULL end return v end

local JOB_FIELDS = { 'name', 'pver', 'queue', 'priority', 'state', 'gen', 'rev', 'created', 'updated', 'eligible', 'terminalat', 'reason',
  'cancelat', 'xcount', 'detailexp' }
local function job_summary(id, v)
  return { id = id, name = v[1], pver = num(v[2]), queue = v[3], priority = v[4], state = v[5], gen = num(v[6]), rev = num(v[7]),
    created = num(v[8]), updated = num(v[9]), eligible = num(v[10]), terminalAt = num(v[11]), reason = str(v[12]),
    cancel = v[13] ~= nil and v[13] ~= false, xcount = num(v[14]), detailExpiredAt = num(v[15]) }
end

if SUB == 'jobs' then
  -- 2 allowed queues ('' = all), 3 queue, 4 state, 5 job name, 6 from ms, 7 to ms, 8 after created ms ('' = none), 9 after id, 10 limit
  local allowed, fq, fs, fn = set_of(ARGS(2)), ARGS(3), ARGS(4), ARGS(5)
  local from, to, limit = tonumber(ARGS(6)), tonumber(ARGS(7)), tonumber(ARGS(10))
  local afterAt, afterId = ARGS(8), ARGS(9)
  local key = 'idx:created'
  if fq ~= '' then key = 'idx:created:q:' .. fq elseif fs ~= '' then key = 'idx:created:s:' .. fs end
  if allowed and fq ~= '' and not allowed[fq] then return out end
  local top = to
  if afterAt ~= '' and tonumber(afterAt) < top then top = tonumber(afterAt) end
  local rows, examined, offset = 0, 0, 0
  while rows < limit and top >= from do
    local page = redis.call('ZRANGE', P .. key, U.S(top), U.S(from), 'BYSCORE', 'REV', 'LIMIT', offset, 100, 'WITHSCORES')
    if #page == 0 then break end
    offset = offset + #page / 2
    for i = 1, #page, 2 do
      local id, score = page[i], page[i + 1]
      examined = examined + 1
      if examined > BUDGET then return { 'BUDGET' } end
      if not (afterAt ~= '' and score == afterAt and not U.blt(id, afterId)) then
        local v = redis.call('HMGET', P .. 'job:' .. id, unpack(JOB_FIELDS))
        if v[1] and (fq == '' or v[3] == fq) and (fs == '' or v[5] == fs) and (fn == '' or v[1] == fn) and (not allowed or allowed[v[3]]) then
          add(U.enc(job_summary(id, v)))
          rows = rows + 1
          if rows >= limit then break end
        end
      end
    end
  end
elseif SUB == 'job' then
  -- 2 job id
  local j = U.job(ARGS(2))
  if j then
    local v = {}
    for i, f in ipairs(JOB_FIELDS) do v[i] = j[f] end
    local summary = job_summary(j.id, v)
    local gens, atts = {}, {}
    for f, val in pairs(j) do
      local n = string.match(f, '^g:(%d+):created$')
      if n then
        gens[#gens + 1] = { n = tonumber(n), row = { generation = U.raw(n), reason = j['g:' .. n .. ':reason'], by = str(j['g:' .. n .. ':by']),
          created = num(val), started = num(j['g:' .. n .. ':started']), max = num(j['g:' .. n .. ':max']) } }
      end
      local a = string.match(f, '^a:([0-9a-f]+):res$')
      if a then
        local function af(x) return j['a:' .. a .. ':' .. x] end
        local code = NULL
        if af('error') then
          local ok, e = pcall(cjson.decode, af('error'))
          if ok and type(e) == 'table' and U.nn(e.code) then code = e.code end
        end
        atts[#atts + 1] = { n = tonumber(val), row = { id = a, generation = num(af('gen')), reservation = num(val), execution = num(af('exec')),
          worker = af('worker'), reserved = num(af('reserved')), authorized = num(af('auth')), finished = num(af('fin')), outcome = str(af('outcome')),
          disposition = str(af('disp')), reason = str(af('reason')), errorCode = code, usage = str(af('usage')) } }
      end
    end
    table.sort(gens, function(x, y) return x.n < y.n end)
    table.sort(atts, function(x, y) return x.n < y.n end)
    local ga, aa = U.arr(), U.arr()
    for _, x in ipairs(gens) do ga[#ga + 1] = x.row end
    for _, x in ipairs(atts) do aa[#aa + 1] = x.row end
    add(U.enc({ job = summary, generations = ga, attempts = aa }))
  end
elseif SUB == 'queueof' then
  -- 2 job id, 3 attempt id ('' = none)
  local id = ARGS(2)
  if id == '' then id = redis.call('HGET', P .. 'attempts', ARGS(3)) or '' end
  local q = id ~= '' and redis.call('HGET', P .. 'job:' .. id, 'queue') or false
  if q then add(q) end
elseif SUB == 'notafter' then
  local v = redis.call('HGET', P .. 'rcpt:' .. ARGS(2), 'notafter')
  if v then add(v) end
elseif SUB == 'queues' then
  -- 2 allowed queues, 3 after name ('' = none), 4 limit
  if redis.call('HLEN', P .. 'queues') > BUDGET then return { 'BUDGET' } end
  local allowed, after, limit = set_of(ARGS(2)), ARGS(3), tonumber(ARGS(4))
  local names = redis.call('HKEYS', P .. 'queues')
  table.sort(names, U.blt)
  for _, q in ipairs(names) do
    if (after == '' or U.blt(after, q)) and (not allowed or allowed[q]) then
      add('{"name":' .. U.str(q) .. ',"row":' .. redis.call('HGET', P .. 'queues', q) .. '}')
      if #out - 3 >= limit then break end
    end
  end
elseif SUB == 'workers' then
  -- 2 include expired ('1'), 3 after heartbeat ('' = none), 4 after id, 5 limit
  local ids
  if ARGS(2) == '1' then ids = redis.call('HKEYS', P .. 'workers') else ids = redis.call('ZRANGE', P .. 'workers:exp', '(' .. NOWS, '+inf', 'BYSCORE') end
  if #ids > BUDGET then return { 'BUDGET' } end
  local afterHb, afterId, limit = ARGS(3), ARGS(4), tonumber(ARGS(5))
  local rows = {}
  for _, id in ipairs(ids) do
    local raw = redis.call('HGET', P .. 'workers', id)
    if raw then
      local hb = cjson.decode(raw).heartbeat
      if afterHb == '' or hb < tonumber(afterHb) or (hb == tonumber(afterHb) and U.blt(id, afterId)) then
        rows[#rows + 1] = { hb = hb, id = id, raw = raw }
      end
    end
  end
  table.sort(rows, function(x, y) if x.hb ~= y.hb then return x.hb > y.hb end return U.blt(y.id, x.id) end)
  for i = 1, math.min(limit, #rows) do add('{"id":' .. U.str(rows[i].id) .. ',"row":' .. rows[i].raw .. '}') end
elseif SUB == 'scopes' then
  -- 2 kind ('' = all), 3 after kind ('' = none), 4 after key, 5 limit
  if redis.call('HLEN', P .. 'scopes') > 2 * BUDGET then return { 'BUDGET' } end
  local kind, after, limit = ARGS(2), nil, tonumber(ARGS(5))
  if ARGS(3) ~= '' then after = ARGS(3) .. '\0' .. ARGS(4) end
  local fields = {}
  for _, f in ipairs(redis.call('HKEYS', P .. 'scopes')) do
    if string.sub(f, 1, 2) == 'k:' then fields[#fields + 1] = string.sub(f, 3) end
  end
  table.sort(fields, U.blt)
  for _, f in ipairs(fields) do
    local k = string.match(f, '^([^%z]*)')
    if (kind == '' or k == kind) and (not after or U.blt(after, f)) then
      local sid = redis.call('HGET', P .. 'scopes', 'k:' .. f)
      local live = redis.call('ZCOUNT', P .. 'permits:' .. sid, '(' .. NOWS, '+inf')
      add('{"row":' .. redis.call('HGET', P .. 'scopes', sid) .. ',"live":' .. live .. '}')
      if #out - 3 >= limit then break end
    end
  end
elseif SUB == 'schedules' then
  -- 2 after key ('' = none), 3 limit. The hash also holds key indexes (k:), definitions (d:) and audit ranges (ar:); a schedule
  -- row's field is its colon-free ID. Listed by schedule key; the last materialization is the newest of its occurrences.
  if redis.call('HLEN', P .. 'sched') > BUDGET then return { 'BUDGET' } end
  local after, limit = ARGS(2), tonumber(ARGS(3))
  local rows = {}
  for _, id in ipairs(redis.call('HKEYS', P .. 'sched')) do
    if not string.find(id, ':', 1, true) then rows[#rows + 1] = U.jget('sched', id) end
  end
  table.sort(rows, function(a, b) return U.blt(a.key, b.key) end)
  for _, r in ipairs(rows) do
    if after == '' or U.blt(after, r.key) then
      local mat = NULL
      local occ = redis.call('HVALS', P .. 'occ:' .. r.id)
      if #occ > BUDGET then return { 'BUDGET' } end
      for _, o in ipairs(occ) do
        local m = cjson.decode(o).mat
        if m and (mat == NULL or m > mat) then mat = m end
      end
      add(U.enc({ key = r.key, paused = r.paused and true or false, review = r.review and true or false, drev = r.drev, crev = r.crev,
        next = U.nn(r.next) or NULL, mat = mat }))
      if #out - 3 >= limit then break end
    end
  end
elseif SUB == 'jscan' then
  -- 16 §2.4 job stats: one row per job of an index range in score order. 2 index key, 3 min score, 4 max score ('(' = exclusive),
  -- 5 after score ('' = none), 6 after id, 7 worker id ('' = any), 8 error range from ms ('' = no error codes), 9 to ms (exclusive).
  -- A row: id, queue, name, state, created, terminal and eligible times, executions, the current generation's first start
  -- (earliest authorization, else earliest reservation), whether worker 7 ran an attempt, and the error codes of attempts
  -- (by worker 7 when given) that finished in [8, 9). After BUDGET entries the last row is the cursor { more, sc, id }.
  local from, after, worker = tonumber(ARGS(5)), ARGS(6), ARGS(7)
  local ef, et = tonumber(ARGS(8)), tonumber(ARGS(9))
  local min = ARGS(5) ~= '' and ARGS(5) or ARGS(3)
  local offset, examined = 0, 0
  while true do
    local page = redis.call('ZRANGE', P .. ARGS(2), min, ARGS(4), 'BYSCORE', 'LIMIT', offset, 200, 'WITHSCORES')
    if #page == 0 then return out end
    offset = offset + #page / 2
    for i = 1, #page, 2 do
      local id, score = page[i], page[i + 1]
      if not (from and tonumber(score) == from and not U.blt(after, id)) then
        if examined >= BUDGET then
          add(U.enc({ more = true, sc = U.raw(score), id = id }))
          return out
        end
        examined = examined + 1
        local j = U.hgetall('job:' .. id)
        if j.state then
          local auth, res, ran, errs = nil, nil, false, U.arr()
          for f in pairs(j) do
            local a = string.match(f, '^a:([0-9a-f]+):res$')
            if a then
              local p = 'a:' .. a .. ':'
              local by = j[p .. 'worker'] == worker
              if by then ran = true end
              if j[p .. 'gen'] == j.gen then
                local x, r = tonumber(j[p .. 'auth']), tonumber(j[p .. 'reserved'])
                if x and (not auth or x < auth) then auth = x end
                if r and (not res or r < res) then res = r end
              end
              local fin = tonumber(j[p .. 'fin'])
              if ef and fin and fin >= ef and fin < et and j[p .. 'error'] and (worker == '' or by) then
                local ok, e = pcall(cjson.decode, j[p .. 'error'])
                if ok and type(e) == 'table' and type(U.nn(e.code)) == 'string' then errs[#errs + 1] = e.code end
              end
            end
          end
          local st = auth or res
          add(U.enc({ id = id, sc = U.raw(score), q = j.queue, n = j.name, s = j.state, c = num(j.created), t = num(j.terminalat),
            e = num(j.eligible), x = num(j.xcount), st = st and U.raw(U.S(st)) or NULL, w = ran, err = errs }))
        end
      end
    end
  end
elseif SUB == 'jobnames' then
  -- 16 §2.6: the stored (queue, job name) pairs, then each schedule's key with its current definition's job name and queue.
  local t = redis.call('HKEYS', P .. 'jobtypes')
  if #t > BUDGET then return { 'BUDGET' } end
  local names, schedules = U.arr(), U.arr()
  for _, f in ipairs(t) do
    local q, n = string.match(f, '^([^%z]*)%z(.*)$')
    names[#names + 1] = U.arr({ q, n })
  end
  if redis.call('HLEN', P .. 'sched') > BUDGET then return { 'BUDGET' } end
  for _, id in ipairs(redis.call('HKEYS', P .. 'sched')) do
    if not string.find(id, ':', 1, true) then
      local r = U.jget('sched', id)
      local d = cjson.decode(redis.call('HGET', P .. 'sched', 'd:' .. id .. ':' .. U.S(r.drev)))
      schedules[#schedules + 1] = U.arr({ r.key, d.template.jobName, d.template.queue })
    end
  end
  add(U.enc({ names = names, schedules = schedules }))
elseif SUB == 'sstats' then
  -- 16 §2.7: 2 from ms, 3 to ms (exclusive). Schedules now; occurrences (occ:idx) and misfire ranges (sched:ar) in the range.
  local from, to = ARGS(2), '(' .. ARGS(3)
  if redis.call('HLEN', P .. 'sched') > BUDGET then return { 'BUDGET' } end
  local total, paused, review, nextDue = 0, 0, 0, NULL
  for _, id in ipairs(redis.call('HKEYS', P .. 'sched')) do
    if not string.find(id, ':', 1, true) then
      local r = U.jget('sched', id)
      total = total + 1
      if r.paused then paused = paused + 1 end
      if r.review then review = review + 1 end
      local n = U.nn(r.next)
      if not r.paused and n and (nextDue == NULL or n < nextDue) then nextDue = n end
    end
  end
  if redis.call('ZCOUNT', P .. 'occ:idx', from, to) > BUDGET then return { 'BUDGET' } end
  local occ = {}
  for _, m in ipairs(redis.call('ZRANGE', P .. 'occ:idx', from, to, 'BYSCORE')) do
    local raw = redis.call('HGET', P .. 'occ:' .. string.sub(m, 1, 32), string.sub(m, 34))
    if raw then
      local d = cjson.decode(raw).disp
      occ[d] = (occ[d] or 0) + 1
    end
  end
  if redis.call('ZCOUNT', P .. 'sched:ar', from, to) > BUDGET then return { 'BUDGET' } end
  local misfires = 0
  for _, m in ipairs(redis.call('ZRANGE', P .. 'sched:ar', from, to, 'BYSCORE')) do
    local raw = redis.call('HGET', P .. 'sched', 'ar:' .. m)
    if raw then
      local reason = cjson.decode(raw).reason
      if reason == 'misfire_skip' or reason == 'run_once_covered' or reason == 'catch_up_skipped' then misfires = misfires + 1 end
    end
  end
  add(U.enc({ total = total, paused = paused, review = review, next = nextDue, occ = occ, misfires = misfires }))
elseif SUB == 'astats' then
  -- 16 §2.7: 2 from ms, 3 to ms (exclusive), 4 before audit id ('' = newest). Counts by operation and actor of the audit rows
  -- in the range, newest first; after BUDGET rows the reply carries the next audit id to continue from.
  local from, to, top = tonumber(ARGS(2)), tonumber(ARGS(3)), ARGS(4) ~= '' and '(' .. ARGS(4) or '+inf'
  local ops, actors, examined, offset = {}, {}, 0, 0
  while true do
    local ids = redis.call('ZRANGE', P .. 'audit', top, '-inf', 'BYSCORE', 'REV', 'LIMIT', offset, 100)
    if #ids == 0 then break end
    offset = offset + #ids
    local rows = redis.call('HMGET', P .. 'audit:data', unpack(ids))
    for i, raw in ipairs(rows) do
      if examined >= BUDGET then
        add(U.enc({ ops = ops, actors = actors, next = ids[i] }))
        return out
      end
      examined = examined + 1
      if raw then
        local a = cjson.decode(raw)
        -- Audit IDs grow with store time, which never goes backward: the first row before the range ends the walk.
        if a.at < from then
          add(U.enc({ ops = ops, actors = actors }))
          return out
        end
        if a.at < to then
          ops[a.action] = (ops[a.action] or 0) + 1
          actors[a.subject] = (actors[a.subject] or 0) + 1
        end
      end
    end
  end
  add(U.enc({ ops = ops, actors = actors }))
elseif SUB == 'schedule' then
  -- 2 schedule key, 3 limit. The summary, the current definition as stored, then the newest occurrences (by materialization
  -- time, then scheduled time with manual ones first, then key; as the SQL stores) with each job's current state.
  local id = redis.call('HGET', P .. 'sched', 'k:' .. ARGS(2))
  if not id then return out end
  local r = cjson.decode(redis.call('HGET', P .. 'sched', id))
  local occ = redis.call('HGETALL', P .. 'occ:' .. id)
  if #occ > 2 * BUDGET then return { 'BUDGET' } end
  local rows, mat = {}, NULL
  for i = 1, #occ, 2 do
    local o = cjson.decode(occ[i + 1])
    o.f = occ[i]
    rows[#rows + 1] = o
    if mat == NULL or o.mat > mat then mat = o.mat end
  end
  local function at(o) return U.nn(o.at) or math.huge end
  table.sort(rows, function(a, b)
    if a.mat ~= b.mat then return a.mat > b.mat end
    if at(a) ~= at(b) then return at(a) > at(b) end
    return U.blt(b.f, a.f)
  end)
  add(U.enc({ key = r.key, paused = r.paused and true or false, review = r.review and true or false, drev = r.drev, crev = r.crev,
    next = U.nn(r.next) or NULL, mat = mat }))
  add(redis.call('HGET', P .. 'sched', 'd:' .. id .. ':' .. U.S(r.drev)))
  for i = 1, math.min(tonumber(ARGS(3)), #rows) do
    local o = rows[i]
    local kind, key = string.match(o.f, '^([^:]*):(.*)$')
    local job = U.nn(o.job)
    local state = job and redis.call('HGET', P .. 'job:' .. job, 'state') or NULL
    add(U.enc({ kind = kind, key = key, at = U.nn(o.at) or NULL, mat = o.mat, disp = o.disp, coal = o.coal and true or false, job = job or NULL,
      state = state }))
  end
elseif SUB == 'audit' then
  -- 2 target kind, 3 target id, 4 from ms, 5 to ms ('' = none), 6 after audit id ('' = none), 7 limit
  local kind, target, limit = ARGS(2), ARGS(3), tonumber(ARGS(7))
  local from, to = tonumber(ARGS(4)), tonumber(ARGS(5))
  local top = ARGS(6) ~= '' and '(' .. ARGS(6) or '+inf'
  local examined, offset = 0, 0
  while #out - 3 < limit do
    local ids = redis.call('ZRANGE', P .. 'audit', top, '-inf', 'BYSCORE', 'REV', 'LIMIT', offset, 100)
    if #ids == 0 then break end
    offset = offset + #ids
    local rows = redis.call('HMGET', P .. 'audit:data', unpack(ids))
    for i, raw in ipairs(rows) do
      examined = examined + 1
      if examined > BUDGET then return { 'BUDGET' } end
      if raw then
        local a = cjson.decode(raw)
        if (kind == '' or a.targetKind == kind) and (target == '' or a.targetId == target) and (not from or a.at >= from) and (not to or a.at <= to) then
          add(raw)
          if #out - 3 >= limit then break end
        end
      end
    end
  end
elseif SUB == 'scaling' then
  -- 2 instance id ('' = any), 3 pool ('' = any), 4 from ms, 5 to ms, 6 after created ms ('' = none), 7 after id, 8 limit
  local inst, pool, limit = ARGS(2), ARGS(3), tonumber(ARGS(8))
  local from, to, afterAt, afterId = tonumber(ARGS(4)), tonumber(ARGS(5)), ARGS(6), ARGS(7)
  local top = to
  if afterAt ~= '' and tonumber(afterAt) < top then top = tonumber(afterAt) end
  local examined, offset = 0, 0
  while #out - 3 < limit and top >= from do
    local page = redis.call('ZRANGE', P .. 'scaling', U.S(top), U.S(from), 'BYSCORE', 'REV', 'LIMIT', offset, 100, 'WITHSCORES')
    if #page == 0 then break end
    offset = offset + #page / 2
    for i = 1, #page, 2 do
      local id, score = page[i], page[i + 1]
      examined = examined + 1
      if examined > BUDGET then return { 'BUDGET' } end
      if not (afterAt ~= '' and score == afterAt and not U.blt(id, afterId)) then
        local raw = redis.call('HGET', P .. 'scaling:data', id)
        if raw then
          local e = cjson.decode(raw)
          if (inst == '' or e.instanceId == inst) and (pool == '' or e.pool == pool) then
            add(raw)
            if #out - 3 >= limit then break end
          end
        end
      end
    end
  end
else
  return { 'NONE', 'invalid_request' }
end
return out
