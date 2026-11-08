-- EditRatePolicy/1 with the edit freeze (10 §6.2–§6.3). ARGS: 1 scope kind, 2 scope key, 3 limit, 4 window ms,
-- 5 expected policy revision, 6 actor JSON.
local M = {}
function M.decide()
  local kind, key, limit, window, expected = ARGS(1), ARGS(2), tonumber(ARGS(3)), tonumber(ARGS(4)), tonumber(ARGS(5))
  local id = U.hget('ratescopes', U.scope_field(kind, key))
  if not id then return U.noeffect('NotFound', 'scope_not_registered') end
  local s = U.jget('ratescopes', id)
  if s.prev ~= expected then return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. U.S(s.prev) }) end
  -- An identical policy never restarts the freeze (capacity-defaults §3).
  if s.limit == limit and s.window == window then return U.noeffect('InvalidState', 'no_change') end
  -- Freeze new authorizations for one full new window in the scope's logical time.
  local t = NOW
  if U.nn(s.last) and s.last > t then t = s.last end
  local notBefore = t + window
  local actor = cjson.decode(ARGS(6))
  local before, oldLimit, oldWindow = s.prev, s.limit, s.window
  s.limit, s.window, s.prev, s.last, s.notBefore, s.updated, s.updatedBy = limit, window, before + 1, t, notBefore, NOW, actor.subject
  U.jset('ratescopes', id, s)
  local retained = U.zcount_after('grants:' .. id, t - window)
  U.event('rate_policy_edited', nil, nil, { scopeKind = kind, scopeKey = key, limit = limit, windowMs = window, policyRevision = s.prev,
    notBefore = notBefore }, actor.subject)
  U.audit(OP, actor, { targetKind = 'rate_scope', targetId = kind .. '/' .. key, expectedRevision = before, resultingRevision = s.prev,
    details = { limit = limit, windowMs = window, previousLimit = oldLimit, previousWindowMs = oldWindow } })
  return U.applied({ rateScopeId = id, scopeKind = kind, scopeKey = key, limit = limit, windowMs = window, policyRevision = s.prev,
    notBefore = notBefore, retainedGrants = retained })
end
return U.run(M)
