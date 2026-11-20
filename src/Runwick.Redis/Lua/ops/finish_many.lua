-- FinishMany/1 (PF-06): each item is a Finish/1 command with its own receipt, kept in the call's rcpt:<call>:f. ARGS: 1 n, then per
-- item: command id, fingerprint, NotAfter ms, body JSON, retry draw, hint (the call a resent item was first sent in, '' otherwise). The decision is U.finish_items (jobs.lua), shared with FinishAndClaim.
local M = {}
function M.decide()
  local out, applied = U.finish_items(1)
  local r = U.applied({ items = out })
  r.nocommit = applied == 0
  return r
end
return U.run(M)
