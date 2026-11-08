-- EditScopePolicy/1 (08 §3.5). ARGS: 1 kind, 2 key, 3 capacity, 4 expected policy revision, 5 actor JSON.
local M = {}
function M.decide()
  local kind, key, capacity, expected, actor = ARGS(1), ARGS(2), tonumber(ARGS(3)), tonumber(ARGS(4)), cjson.decode(ARGS(5))
  local sid = U.hget('scopes', U.scope_field(kind, key))
  if not sid then return U.noeffect('NotFound', 'scope_not_registered') end
  local s = U.jget('scopes', sid)
  if s.prev ~= expected then return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. s.prev }) end
  if s.capacity == capacity then return U.noeffect('InvalidState', 'no_change') end
  local previous = s.capacity
  s.capacity, s.prev, s.updated = capacity, s.prev + 1, NOW
  U.jset('scopes', sid, s)
  -- A decrease never revokes a live permit (CTL-01): report the live count.
  local live = U.zcount_after('permits:' .. sid, NOW)
  U.event('scope_policy_edited', nil, nil, { scope = U.uuid_text(sid), kind = kind, key = key, capacity = capacity, previous = previous,
    policyRevision = s.prev }, actor.subject)
  U.audit(OP, actor, { targetKind = 'scope', targetId = kind .. '/' .. key, expectedRevision = expected, resultingRevision = s.prev,
    details = { capacity = capacity, previous = previous, livePermits = live } })
  return U.applied({ scopeId = sid, kind = kind, key = key, capacity = capacity, policyRevision = s.prev, livePermits = live })
end
return U.run(M)
