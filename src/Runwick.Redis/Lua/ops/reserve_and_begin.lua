-- ReserveAndBegin/1 (PF-06). ARGS: 1 worker, 2 queue, 3 priority, 4 lease ms, 5 tokens JSON, 6 after eligible, 7 after job,
-- 8 contracts JSON, 9 max jobs, 10 entropies JSON, 11 jitters JSON. The decision is U.reserve_and_begin (jobs.lua), shared with FinishAndClaim.
local M = {}
function M.replay_ids(r)
  local ids = {}
  for _, i in ipairs(r.items) do if i.status == 'Applied' then ids[#ids + 1] = i.jobId end end
  return ids
end
function M.decide()
  return U.reserve_and_begin(1)
end
return U.run(M)
