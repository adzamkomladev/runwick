-- SetQueueState/1 public path (08 §3.4). ARGS: 1 queue, 2 paused ('1'/'0'), 3 expected revision, 4 actor JSON.
local M = {}
function M.decide()
  local queue, paused, actor = ARGS(1), ARGS(2) == '1', cjson.decode(ARGS(4))
  local row = U.jget('queues', queue)
  if not row then return U.noeffect('NotFound', 'queue_not_registered') end
  local expected = tonumber(ARGS(3))
  if row.revision ~= expected then
    return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. row.revision })
  end
  if row.paused == paused then return U.noeffect('InvalidState', 'no_change') end
  row.paused, row.revision, row.updated, row.updatedBy = paused, row.revision + 1, NOW, actor.subject
  U.jset('queues', queue, row)
  U.event(paused and 'queue_paused' or 'queue_resumed', nil, nil, { queue = queue, revision = row.revision }, actor.subject)
  U.audit(OP, actor, { targetKind = 'queue', targetId = queue, expectedRevision = expected, resultingRevision = row.revision,
    details = { paused = paused } })
  return U.applied({ queue = queue, paused = paused, revision = row.revision })
end
return U.run(M)
