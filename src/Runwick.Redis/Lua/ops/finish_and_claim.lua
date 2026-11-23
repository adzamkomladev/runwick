-- FinishAndClaim (opt-in, one script call): FinishMany/1's items and one ReserveAndBegin/1 claim decided together, one intent, so a
-- worker that finishes jobs gets its next jobs in the same round trip. The call runs as the claim command: its receipt (command id,
-- fingerprint, ownership class) is the claim's own and holds only the claim result, so a plain ReserveAndBegin resend of the same
-- command replays it. The finish items' durable receipts are rcpt:<call>:f, apart from the claim's. When no job is claimed the finishes are still decided, and the
-- claim's status travels in the reply (no claim receipt, as for any no-effect claim).
-- ARGS: 1 to 11 as ReserveAndBegin/1, 12 n, then per item as FinishMany/1.
local M = {}
function M.replay_ids(r)
  local ids = {}
  for _, i in ipairs(r.items or {}) do if i.status == 'Applied' then ids[#ids + 1] = i.jobId end end
  return ids
end
function M.decide()
  U.batchy = true
  local finished, applied = U.finish_items(12)
  local c = U.reserve_and_begin(1)
  if c.effect == 'A' then
    c.rcpt_text = c.rtext -- the claim's text, U.enc(c.result) before "finished" is added
    c.rtext = nil
    c.result.finished = finished
    -- The reply is the receipt's object plus "finished", which orders before "items" (its only key): splice it in rather than
    -- encode the claim a second time (that second encode was most of this call's extra cost over FinishMany plus a claim).
    local only = true
    for k in pairs(c.result) do if k ~= 'items' and k ~= 'finished' then only = false end end
    if only then c.rtext = '{"finished":' .. U.enc(finished) .. ',' .. string.sub(c.rcpt_text, 2) end
    return c
  end
  FP = '' -- no claim receipt
  local r = U.applied({ finished = finished, claim = { status = c.status, code = c.code or NULL, blocked = c.blocked or NULL,
    resumeAt = c.resumeAt or NULL, resumeJob = c.resumeJob or NULL } })
  r.nocommit = applied == 0
  return r
end
return U.run(M)
