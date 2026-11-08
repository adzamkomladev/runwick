-- Reserve/1 (04 §7.3). ARGS: 1 worker, 2 queue, 3 priority, 4 lease ms, 5 token, 6 after eligible, 7 after job, 8 contracts JSON,
-- 9 attempt entropy.
local M = {}
function M.replay_ids(r) return { r.jobId } end
function M.decide()
  local g = U.queue_gate(ARGS(2))
  if g then return g end
  local afterAt = ARGS(6) ~= '' and tonumber(ARGS(6)) or nil
  local c = U.candidates(ARGS(2), ARGS(3), afterAt, ARGS(7), U.contract_set(ARGS(8)), 1)
  if #c == 0 then return U.noeffect('NoWork', 'no_work') end
  local r = U.reserve_job(c[1].id, tonumber(ARGS(4)), ARGS(1), ARGS(5), ARGS(9))
  r.payloads = { c[1].id }
  return r
end
return U.run(M)
