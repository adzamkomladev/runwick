-- PublishPresence/1 (04 §7.12): no receipt. ARGS: 1 instance id, 2 record JSON (without times), 3 expiry ms.
local M = {}
function M.decide()
  local id = ARGS(1)
  local rec = cjson.decode(ARGS(2))
  local old = U.jget('workers', id)
  if old and old.seq >= rec.seq then return U.noeffect('VersionConflict', 'presence_sequence_stale', { decided = NOWS }) end
  local expires = NOW + tonumber(ARGS(3))
  rec.started = old and old.started or NOW
  rec.heartbeat = NOW
  rec.expires = expires
  rec.subscriptions = U.arr(rec.subscriptions)
  rec.caps = U.arr(rec.caps)
  for i, c in ipairs(rec.caps) do rec.caps[i] = U.arr(c) end
  U.jset('workers', id, rec)
  U.zadd('workers:exp', expires, id)
  return U.applied({ expiresAt = expires })
end
return U.run(M)
