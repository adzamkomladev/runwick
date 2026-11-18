-- FinishMany/1 (PF-06): each item is a Finish/1 command with its own receipt. ARGS: 1 n, then per item:
-- command id, fingerprint, NotAfter ms, body JSON, retry draw. The decision is U.finish_items (jobs.lua), shared with FinishAndClaim.
local M = {}
function M.decide()
  local out, applied = U.finish_items(1)
  local r = U.applied({ items = out })
  r.nocommit = applied == 0
  return r
end
return U.run(M)
