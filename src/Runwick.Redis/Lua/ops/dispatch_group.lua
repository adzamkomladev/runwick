-- DispatchGroup (client-internal, not a manifest operation). ARGS: count, then command ID, fingerprint, deadline, item JSON, payload per item.
-- The outer command ID is the group's recoverable intent and has no receipt; each item keeps its own ordinary Dispatch/1 receipt and event.
-- Declines with group_fallback (before any write) for shapes it does not group; the client then sends each command alone.
local M = {}
local MAX_COUNT, MAX_REQUEST_BYTES = 24, 196608

local function put_receipt(it, r)
  local key = 'rcpt:' .. it.cmd
  local row = {
    cls = 'durable', op = 'Dispatch', ver = '1', fp = it.fp,
    notafter = U.S(it.na), retain = U.S(it.na + FLOOR),
    status = r.status, code = r.code or '', detail = r.detail or '',
    decided = r.decided, result = U.enc(r.result),
  }
  for f, v in pairs(row) do U.hset(key, f, v) end
  U.zadd('rcpt:exp:durable', it.na + FLOOR, it.cmd)
end

local function read_items()
  local n = tonumber(ARGS(1))
  if not n or n % 1 ~= 0 or n < 2 or n > MAX_COUNT or #ARGV ~= A + 5 * n then return nil end
  local request_bytes, items, seen = 0, {}, {}
  for _, value in ipairs(ARGV) do
    request_bytes = request_bytes + #value
    if request_bytes > MAX_REQUEST_BYTES then return nil end
  end
  for i = 1, n do
    local b = 2 + (i - 1) * 5
    local cmd, fp, na = ARGS(b), ARGS(b + 1), tonumber(ARGS(b + 2))
    if #cmd ~= 32 or not cmd:match('^[0-9a-f]+$') or seen[cmd] or not na or na % 1 ~= 0 or #fp ~= 64 or
        not fp:match('^[0-9a-f]+$') then return nil end
    seen[cmd] = true
    items[i] = { cmd = cmd, fp = fp, na = na, json = ARGS(b + 3), payload = ARGS(b + 4) }
  end
  return items
end

-- Payload and item JSON come from DispatchCallAsync after its validation. Check the
-- narrow wire shape before staging any after-images, including malformed JSON.
local function body(it)
  local ok, item = pcall(cjson.decode, it.json)
  if not ok or type(item) ~= 'table' or type(item.jobId) ~= 'string' or #item.jobId ~= 32 or
      not item.jobId:match('^[0-9a-f]+$') or type(item.priority) ~= 'string' or
      type(item.pver) ~= 'number' or type(item.profver) ~= 'number' or type(item.profile) ~= 'string' or
      type(item.name) ~= 'string' or type(item.queue) ~= 'string' or type(item.headers) ~= 'string' or
      type(item.scopes) ~= 'table' or type(item.rates) ~= 'table' or type(item.policy) ~= 'table' then return nil end
  local p = item.policy
  if type(p.maxExec) ~= 'number' or type(p.maxUn) ~= 'number' or
      type(p.retry) ~= 'string' or type(p.uretry) ~= 'string' or
      (U.nn(p.timeout) and type(p.timeout) ~= 'number') then return nil end
  item.payload = it.payload
  return item
end

function M.decide()
  local items = read_items()
  if not items then return U.noeffect('InvalidRequest', 'group_fallback') end
  local results, confirmations, jobs = U.arr(), {}, {}
  local applied = 0
  for i, it in ipairs(items) do
    local key = 'rcpt:' .. it.cmd
    local rc = U.hgetall(key)
    local r, replayed
    if rc.cls then
      if rc.cls ~= 'durable' or rc.op ~= 'Dispatch' or rc.ver ~= '1' or rc.fp ~= it.fp then
        r = U.noeffect('CommandConflict', 'command_conflict')
      elseif not rc.result or rc.result == '' then
        r = U.noeffect('InvariantFault', 'receipt_incomplete')
      else
        r = U.applied(rc.result, rc.status, rc.code, { detail = rc.detail, decided = rc.decided })
        replayed = true
        confirmations[#confirmations + 1] = key
      end
    elseif NOW >= it.na then
      r = U.noeffect('CommandExpired', 'command_expired', { decided = NOWS })
    elseif it.na - NOW > CAP then
      r = U.noeffect('InvalidRequest', 'deadline_too_far', { decided = NOWS })
    else
      local item = body(it)
      if not item then
        r = U.noeffect('InvalidRequest', 'invalid_request', { decided = NOWS })
      elseif U.nn(item.akey) or U.nn(item.wkey) or next(item.scopes) or next(item.rates) then
        return U.noeffect('InvalidRequest', 'group_fallback')
      elseif not U.hget('queues', item.queue) then
        r = U.noeffect('InvalidRequest', 'queue_not_registered', { decided = NOWS })
      elseif jobs[item.jobId] or redis.call('EXISTS', P .. 'job:' .. item.jobId) == 1 then
        r = U.noeffect('InvariantFault', 'id_collision', { decided = NOWS })
      else
        local first = #EV + 1
        local refused, dispatched = U.dispatch_core({ item }, false, 'initial', nil)
        if refused then
          r = refused
          r.decided = NOWS
        else
          local result = dispatched[1]
          result.ordinal = nil
          r = U.applied(result, nil, nil, { decided = NOWS })
          put_receipt(it, r)
          for e = first, #EV do EV[e].commandId = it.cmd end
          jobs[item.jobId] = true
          applied = applied + 1
        end
      end
    end
    results[i] = U.arr(reply_array(r, replayed))
  end

  -- Mixed group confirmation is included in its intent. Replay-only confirmation
  -- writes have the same semantics as Dispatch replay and do not advance the revision.
  for _, key in ipairs(confirmations) do
    if applied > 0 then
      U.hset(key, 'confirmed', NOWS)
    end
  end
  return U.applied({ items = results }, nil, nil, { nocommit = applied == 0, confirmations = confirmations })
end

return U.run(M)
