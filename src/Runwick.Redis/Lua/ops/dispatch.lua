-- Dispatch/1, DispatchMany/1, ManagementDispatch/1 (04 §7.2, 19 §6.2). ARGS: 1 continueOnConflict, 2 many, 3 created reason,
-- 4 actor JSON ('' = none), 5 n, then per item: item JSON, payload bytes.
local M = {}
function M.decide()
  local n = tonumber(ARGS(5))
  local items = {}
  for i = 1, n do
    local it = cjson.decode(ARGS(4 + 2 * i))
    it.payload = ARGS(5 + 2 * i)
    items[i] = it
  end
  local actor = ARGS(4) ~= '' and cjson.decode(ARGS(4)) or nil
  local refused, results = U.dispatch_core(items, ARGS(1) == '1', ARGS(3), actor and actor.subject or nil)
  if refused then return refused end
  if ARGS(2) == '1' then return U.applied({ items = results }) end
  local r = results[1]
  r.ordinal = nil
  if actor then
    local j = U.job(r.jobId)
    U.audit('Dispatch', actor, { targetKind = 'job', targetId = U.uuid_text(r.jobId), resultingRevision = U.raw(j.rev),
      details = { jobName = j.name, queue = j.queue, disposition = string.lower(r.disposition) } })
    return U.applied({ dispatch = r, generation = U.raw(j.gen), revision = U.raw(j.rev), queue = j.queue })
  end
  return U.applied(r)
end
return U.run(M)
