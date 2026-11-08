-- SetPoolOverride/1 (11 §5): one revisioned row per (instance, pool) in the pools hash, field <instanceHex>:<pool>.
-- ARGS: 1 instance hex, 2 instance id (D form, for events and audit), 3 pool, 4 target ('' = none), 5 expires ms ('' = none),
-- 6 expected revision, 7 actor JSON.
local M = {}
function M.decide()
  local field, pool = ARGS(1) .. ':' .. ARGS(3), ARGS(3)
  local target, expires, expected = tonumber(ARGS(4)), tonumber(ARGS(5)), tonumber(ARGS(6))
  local row = U.jget('pools', field)
  local actual = row and row.revision or 0
  if actual ~= expected then return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. U.S(actual) }) end
  -- An expired override reads as absent: setting the same target again renews it rather than no_change.
  local curT, curE
  if row and not (U.nn(row.expiresAt) and row.expiresAt <= NOW) then curT, curE = U.nn(row.target), U.nn(row.expiresAt) end
  if curT == target and curE == expires then return U.noeffect('InvalidState', 'no_change') end
  local actor = cjson.decode(ARGS(7))
  local revision = actual + 1
  U.jset('pools', field, { instanceId = ARGS(1), pool = pool, target = target or NULL, revision = revision, expiresAt = expires or NULL,
    reason = U.nn(actor.reason) or NULL, updated = NOW, updatedBy = actor.subject })
  local details = { instanceId = ARGS(2), pool = pool, target = target or NULL, expiresAt = expires or NULL, revision = revision }
  U.event('pool_override_set', nil, nil, details, actor.subject)
  U.audit(OP, actor, { targetKind = 'pool', targetId = ARGS(2) .. '/' .. pool, expectedRevision = actual, resultingRevision = revision,
    details = details })
  return U.applied({ instanceId = ARGS(1), pool = pool, target = target or NULL, revision = revision, expiresAt = expires or NULL })
end
return U.run(M)
