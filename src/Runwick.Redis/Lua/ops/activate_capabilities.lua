-- ActivateCapabilities/1 (10 §6.1): raises the application's required capabilities in meta; never lowers one.
-- ARGS: 1 capabilities JSON object (name -> version), 2 actor JSON, 3 application name (audit target).
local M = {}
function M.decide()
  local m = U.meta
  local merged, changed = cjson.decode(m.caps or '{}'), false
  for name, v in pairs(cjson.decode(ARGS(1))) do
    if not merged[name] or v > merged[name] then
      merged[name] = v
      changed = true
    end
  end
  if not changed then return U.noeffect('InvalidState', 'no_change') end
  local caps = U.enc(merged)
  local before = m.gaterev or '1'
  local rev = U.must(U.inc(before))
  U.hset('meta', 'caps', caps)
  U.hset('meta', 'gaterev', rev)
  local actor = cjson.decode(ARGS(2))
  U.event('capabilities_activated', nil, nil, { requiredCapabilities = U.raw(caps), revision = U.raw(rev) }, actor.subject)
  U.audit(OP, actor, { targetKind = 'application', targetId = ARGS(3), expectedRevision = U.raw(before), resultingRevision = U.raw(rev),
    details = { requiredCapabilities = U.raw(caps) } })
  return U.applied({ revision = U.raw(rev), requiredCapabilities = U.raw(caps) })
end
return U.run(M)
