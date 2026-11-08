-- FinishMany/1 (PF-06): each item is a Finish/1 command with its own receipt. ARGS: 1 n, then per item:
-- command id, fingerprint, NotAfter ms, body JSON, retry draw.
local M = {}
local DURABLE_FLOOR = 86400000
local function item_json(cmd, r, replayed)
  return { commandId = cmd, status = r.status, effect = r.effect == 'A' and 'Applied' or 'ObservedNoEffect', replayed = replayed,
    code = r.code or NULL, decidedAt = r.decided and r.decided ~= '' and U.raw(r.decided) or NULL,
    result = r.result and (type(r.result) == 'table' and r.result or U.raw(r.result)) or NULL }
end
function M.decide()
  local n = tonumber(ARGS(1))
  local items, done, order = {}, {}, {}
  for i = 1, n do
    local b = 2 + (i - 1) * 5
    items[i] = { cmd = ARGS(b), fp = ARGS(b + 1), na = tonumber(ARGS(b + 2)), body = cjson.decode(ARGS(b + 3)), draw = tonumber(ARGS(b + 4)) }
    order[i] = i
  end
  table.sort(order, function(a, b) return U.blt(items[a].cmd, items[b].cmd) end)
  for _, k in ipairs(order) do
    local it = items[k]
    local rc = redis.call('HMGET', P .. 'rcpt:' .. it.cmd, 'cls', 'op', 'ver', 'fp', 'status', 'code', 'decided', 'result')
    if rc[1] then
      if rc[1] ~= 'durable' or rc[2] ~= 'Finish' or rc[3] ~= '1' or rc[4] ~= it.fp then
        done[k] = item_json(it.cmd, U.noeffect('CommandConflict', 'command_conflict'), false)
      else
        -- 22 §4.5 marker for a replayed item: a fresh write, not part of any intent.
        redis.call('HSET', P .. 'rcpt:' .. it.cmd, 'confirmed', NOWS)
        done[k] = item_json(it.cmd, { status = rc[5], effect = 'A', code = rc[6] ~= '' and rc[6] or nil, decided = rc[7], result = rc[8] }, true)
      end
    elseif NOW >= it.na then
      done[k] = item_json(it.cmd, U.noeffect('CommandExpired', 'command_expired', { decided = NOWS }), false)
    elseif it.na - NOW > 60000 then
      done[k] = item_json(it.cmd, U.noeffect('InvalidRequest', 'deadline_too_far', { decided = NOWS }), false)
    end
  end
  local applied = 0
  for k, it in ipairs(items) do
    if not done[k] then
      local r = U.finish_job(it.body, it.draw)
      r.decided = NOWS
      if r.effect == 'A' then
        applied = applied + 1
        local key = 'rcpt:' .. it.cmd
        local row = { cls = 'durable', op = 'Finish', ver = '1', fp = it.fp, notafter = U.S(it.na), retain = U.S(it.na + DURABLE_FLOOR),
          status = 'Applied', code = '', detail = '', decided = NOWS, result = U.enc(r.result) }
        for f, v in pairs(row) do U.hset(key, f, v) end
        U.zadd('rcpt:exp:durable', it.na + DURABLE_FLOOR, it.cmd)
      end
      done[k] = item_json(it.cmd, r, false)
    end
  end
  local out = U.arr()
  for k = 1, n do out[k] = done[k] end
  local r = U.applied({ items = out })
  r.nocommit = applied == 0
  return r
end
return U.run(M)
