-- TriggerSchedule/1 (13 §6.5). ARGS: 1 key, 2 expected definition revision, 3 trigger key, 4 actor JSON, 5 item count, then (item, payload).
local M = {}
function M.decide()
  if not S.active() then return U.noeffect('UnsupportedVersion', 'scheduler_not_active') end
  local r = S.by_key(ARGS(1))
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  if not r.enabled then return U.noeffect('InvalidState', 'schedule_disabled') end
  if r.drev ~= tonumber(ARGS(2)) then return S.conflict('actual=' .. U.S(r.drev)) end
  -- The same trigger ID returns the existing occurrence, never new work (13 §6.5).
  local key = ARGS(3)
  local prior = U.jget('occ:' .. r.id, 'manual:' .. key)
  if prior then
    return U.applied({ scheduleId = r.id, cursorRevision = r.curev, occurrences = U.arr({ { kind = 'manual', key = key, jobId = prior.job, disposition = prior.disp } }) })
  end
  local actor = cjson.decode(ARGS(4))
  local items = S.items(6, tonumber(ARGS(5)))
  if #items ~= 1 then return S.conflict('items') end
  local refused, results = S.dispatch(items)
  if refused then return refused end
  local res = results[1]
  local disp = S.disp(res.disposition)
  if not S.occ_put(r, 'manual', key, res.jobId, disp, nil, false, nil, nil) then return U.noeffect('InvariantFault', 'occurrence_exists') end
  S.event('schedule_triggered', r, actor.subject, { triggerId = key, jobId = S.dash(res.jobId), disposition = disp, paused = r.paused })
  S.audit(r, actor, r.drev, r.drev, { triggerId = key, jobId = S.dash(res.jobId) })
  return U.applied({ scheduleId = r.id, cursorRevision = r.curev, occurrences = U.arr({ { kind = 'manual', key = key, jobId = res.jobId, disposition = disp } }) })
end
return U.run(M)
