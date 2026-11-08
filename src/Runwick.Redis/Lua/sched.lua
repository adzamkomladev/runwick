-- Schedule helpers shared by the sched_* scripts (13, 22 §3.1). In the `sched` hash: `k:<key>` -> schedule id, `<id>` -> the
-- row (cursor and control state, canonical JSON), `d:<id>:<revision>` -> the definition exactly as the client sent it,
-- `ar:<commandId>:<n>` -> an audit range. `sched:due` holds due schedules by scan_after_at; `sched:ar` the audit ranges by
-- creation time. `occ:<id>` holds a schedule's occurrences (field <kind>:<key>); `occ:idx` indexes them by materialization time.
local S = {}
S.ACTOR = 'svc:scheduler'
S.MAX_PLAN = 2097152
local NULLABLE = { 'reason', 'next', 'through', 'ep', 'rpol', 'rcut' }

function S.active()
  return cjson.decode(U.meta.caps or '{}')['scheduler'] ~= nil
end

-- The row with JSON nulls turned into nil.
function S.get(id)
  local r = U.jget('sched', id)
  if not r then return nil end
  for _, f in ipairs(NULLABLE) do r[f] = U.nn(r[f]) end
  return r
end

function S.by_key(key)
  local id = U.hget('sched', 'k:' .. key)
  if not id then return nil end
  return S.get(id)
end

-- Saves the row and keeps the due index in step (due = enabled, not paused, not in review, with a next occurrence).
function S.put(r)
  local o = {}
  for k, v in pairs(r) do o[k] = v end
  for _, f in ipairs(NULLABLE) do if o[f] == nil then o[f] = NULL end end
  U.jset('sched', r.id, o)
  if r.enabled and not r.paused and not r.review and r.next ~= nil then
    U.zadd('sched:due', r.scan, r.id)
  else
    U.zrem('sched:due', r.id)
  end
end

function S.dash(h)
  return string.sub(h, 1, 8) .. '-' .. string.sub(h, 9, 12) .. '-' .. string.sub(h, 13, 16) .. '-' .. string.sub(h, 17, 20) .. '-' .. string.sub(h, 21, 32)
end

function S.nz(v)
  if v == nil then return NULL end
  return v
end

function S.state(r)
  return { key = r.key, paused = r.paused, needsReview = r.review, definitionRevision = r.drev, controlRevision = r.crev, cursorRevision = r.curev }
end

function S.event(kind, r, actor, details)
  details.scheduleId = r.id
  details.scheduleKey = r.key
  U.event(kind, nil, nil, details, actor)
end

-- One occurrence row; false when it already exists.
function S.occ_put(r, kind, key, job, disp, at, coalesced, from, through)
  local f = kind .. ':' .. key
  if U.hget('occ:' .. r.id, f) then return false end
  U.jset('occ:' .. r.id, f, { job = job, disp = disp, at = S.nz(at), drev = r.drev, coal = coalesced and true or false, from = S.nz(from),
    through = S.nz(through), mat = NOW, cmd = CMD })
  U.zadd('occ:idx', NOW, r.id .. ':' .. f)
  return true
end

function S.range(r, drev, i, from, through, reason)
  local m = CMD .. ':' .. i
  U.jset('sched', 'ar:' .. m, { sid = r.id, drev = drev, from = S.nz(from), through = through, reason = reason, at = NOW })
  U.zadd('sched:ar', NOW, m)
end

function S.disp(d) return d == 'Created' and 'created' or 'linked_existing' end

-- Items start at ARGS(first): item JSON, payload bytes, repeated n times. A manual trigger's header time is the decision time.
function S.items(first, n)
  local items = {}
  for i = 1, n do
    local it = cjson.decode(ARGS(first + 2 * (i - 1)))
    it.payload = ARGS(first + 2 * (i - 1) + 1)
    it.headers = (string.gsub(it.headers, '"__now__"', NOWS))
    items[i] = it
  end
  return items
end

-- The jobs of one command through the shared dispatch core (created reason `scheduled`, 13 C-10).
function S.dispatch(items)
  if #items == 0 then return nil, {} end
  return U.dispatch_core(items, false, 'scheduled', nil)
end

function S.conflict(detail) return U.noeffect('VersionConflict', 'revision_conflict', { detail = detail }) end

function S.audit(r, actor, expected, resulting, details)
  U.audit(OP, actor, { targetKind = 'schedule', targetId = r.key, expectedRevision = expected, resultingRevision = resulting, details = details })
end
