-- EditRetentionPolicy/1 (17 §2.2). ARGS: 1 expected revision, 2 hours JSON (7 entries, null = forever), 3 actor JSON,
-- 4 default hours JSON (the policy in force at revision 0), 5 application name.
local M = {}
local KEYS = { 'completedDetailHours', 'failedDetailHours', 'jobHistoryHours', 'auditHours', 'scalingEventsHours', 'scheduleOccurrencesHours',
  'workerPresenceHours' }
local function named(json)
  local t, h = {}, cjson.decode(json)
  for i, k in ipairs(KEYS) do t[k] = U.nn(h[i]) or NULL end
  return t
end
function M.decide()
  local cur = U.jget('retention', 'policy')
  local rev = cur and cur.rev or 0
  if rev ~= tonumber(ARGS(1)) then return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. U.S(rev) }) end
  local before = cur and cur.hs or ARGS(4)
  if before == ARGS(2) then return U.noeffect('InvalidState', 'no_change') end
  local actor = cjson.decode(ARGS(3))
  U.jset('retention', 'policy', { rev = rev + 1, hs = ARGS(2), at = NOW, by = actor.subject })
  U.audit(OP, actor, { targetKind = 'application', targetId = ARGS(5), expectedRevision = rev, resultingRevision = rev + 1,
    details = { after = named(ARGS(2)), before = named(before) } })
  return U.applied({ revision = rev + 1, policy = U.raw(ARGS(2)) })
end
return U.run(M)
