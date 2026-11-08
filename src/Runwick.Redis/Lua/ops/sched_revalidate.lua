-- RevalidateSchedule/1 (13 §6.8). ARGS: 1 key, 2 expected definition revision, 3 actor JSON.
local M = {}
function M.decide()
  local r = S.by_key(ARGS(1))
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  if r.drev ~= tonumber(ARGS(2)) then return S.conflict('actual=' .. U.S(r.drev)) end
  if r.ep then return U.noeffect('InvalidState', 'episode_unresolved') end
  if not r.review then return U.noeffect('InvalidState', 'no_change') end
  local actor = cjson.decode(ARGS(3))
  local previous = r.reason
  r.review, r.reason, r.scan, r.updated, r.by = false, nil, NOW, NOW, actor.subject
  S.put(r)
  S.event('schedule_revalidated', r, actor.subject, { previousReason = S.nz(previous) })
  S.audit(r, actor, r.drev, r.drev, { previousReason = S.nz(previous) })
  return U.applied(S.state(r))
end
return U.run(M)
