-- ReceiveOutbox/1 (14 §5, receive_outbox_v3). The delivery mapping lives in the outbox hash, field <sourceHex>:<messageHex>,
-- indexed by retain_until in outbox:exp. ARGS: 1 field, 2 delivery fingerprint hex, 3 acceptUntil ms, 4 retainUntil ms,
-- 5 confirmed profile, 6 item JSON, 7 payload bytes.
local M = {}

local function delivery(row, first)
  return { jobId = row.jobId, disposition = row.disposition, match = row.match, policyDrift = row.policyDrift, firstDelivery = first,
    deliveredAt = row.deliveredAt, acceptUntil = row.acceptUntil, confirmedProfile = row.profile }
end

function M.decide()
  local field, fp, accept, retain = ARGS(1), ARGS(2), tonumber(ARGS(3)), tonumber(ARGS(4))
  -- A retained mapping answers every later transport command for the identity, even after AcceptUntil.
  local row = U.jget('outbox', field)
  if row then
    if row.fp ~= fp then return U.noeffect('CommandConflict', 'delivery_conflict', { conflict = row.jobId }) end
    return U.applied(delivery(row, false))
  end
  if NOW >= accept then return U.noeffect('CommandExpired', 'delivery_horizon_passed') end
  local it = cjson.decode(ARGS(6))
  it.payload = ARGS(7)
  local refused, results = U.dispatch_core({ it }, false, 'initial', nil)
  if refused then return refused end
  local r = results[1]
  row = { jobId = r.jobId, disposition = r.disposition, match = r.match, policyDrift = r.policyDrift, fp = fp, fpver = 1,
    acceptUntil = accept, retainUntil = retain, profile = ARGS(5), firstCommand = CMD, deliveredAt = NOW }
  U.jset('outbox', field, row)
  U.zadd('outbox:exp', retain, field)
  return U.applied(delivery(row, true))
end
return U.run(M)
