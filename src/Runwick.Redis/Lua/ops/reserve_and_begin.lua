-- ReserveAndBegin/1 (PF-06). ARGS: 1 worker, 2 queue, 3 priority, 4 lease ms, 5 tokens JSON, 6 after eligible, 7 after job,
-- 8 contracts JSON, 9 max jobs, 10 entropies JSON, 11 jitters JSON.
local M = {}
function M.replay_ids(r)
  local ids = {}
  for _, i in ipairs(r.items) do if i.status == 'Applied' then ids[#ids + 1] = i.jobId end end
  return ids
end
function M.decide()
  local g = U.queue_gate(ARGS(2))
  if g then return g end
  local afterAt = ARGS(6) ~= '' and tonumber(ARGS(6)) or nil
  local c = U.candidates(ARGS(2), ARGS(3), afterAt, ARGS(7), U.contract_set(ARGS(8)), tonumber(ARGS(9)))
  if #c == 0 then return U.noeffect('NoWork', 'no_work') end
  local tokens, entropy, jitters = cjson.decode(ARGS(5)), cjson.decode(ARGS(10)), cjson.decode(ARGS(11))
  local items, begun, firstBlocked, last, reserved = U.arr(), {}, nil, nil, false
  for i, cand in ipairs(c) do
    local r = U.reserve_job(cand.id, tonumber(ARGS(4)), ARGS(1), tokens[i], entropy[i])
    if r.status == 'CapacityBlocked' then
      firstBlocked = firstBlocked or r.blocked
      last = cand
      items[#items + 1] = { ordinal = i - 1, jobId = cand.id, status = 'CapacityBlocked', code = 'capacity_blocked', blockedScopeId = r.blocked,
        cursor = { eligibleAt = cand.at, jobId = cand.id } }
    else
      if r.effect ~= 'A' then U.abort(r.status, r.code) end
      reserved = true
      local b = U.begin_job(U.lease_of(r.result), jitters[i])
      if b.effect ~= 'A' then U.abort(b.status, b.code) end
      if b.status == 'Applied' then begun[#begun + 1] = cand.id end
      items[#items + 1] = { ordinal = i - 1, jobId = cand.id, status = b.status, code = b.code or NULL, reserve = r.result, begin = b.result }
    end
  end
  if not reserved then
    return U.noeffect('CapacityBlocked', 'capacity_blocked', { blocked = firstBlocked, resumeAt = last.at, resumeJob = last.id })
  end
  local r = U.applied({ items = items })
  r.payloads = begun
  return r
end
return U.run(M)
