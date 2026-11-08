-- ResolveCatchUp/1 (13 §6.6). ARGS: 1 key, 2 episode id, 3 resolution, 4 additional jobs, 5 next due after the cutoff ms or '', 6 actor JSON,
-- 7 the next-due ms the client built the run-once item for or '', 8 item count, then (item, payload).
local M = {}
function M.decide()
  local r = S.by_key(ARGS(1))
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  local ep = r.ep
  if not ep or ep.id ~= ARGS(2) then return S.conflict('episode=' .. (ep and S.dash(ep.id) or '')) end
  local resolution, add, after = ARGS(3), tonumber(ARGS(4)), tonumber(ARGS(5))
  if resolution == 'extend' and ep.limit + add > 10000 then return U.noeffect('InvalidRequest', 'invalid_request', { detail = 'additionalJobs' }) end
  if after and after <= ep.cutoff then return U.noeffect('InvalidRequest', 'invalid_request', { detail = 'nextDueAfterCutoff' }) end
  local runOnce = resolution == 'run_once' and r.next ~= nil
  if runOnce and tonumber(ARGS(7)) ~= r.next then return S.conflict('next') end
  local actor = cjson.decode(ARGS(6))
  local items = S.items(9, tonumber(ARGS(8)))
  if #items ~= (runOnce and 1 or 0) then return S.conflict('items') end
  local refused, results = S.dispatch(items)
  if refused then return refused end
  local occurrences = U.arr()
  if runOnce then
    local key, disp = U.S(r.next), S.disp(results[1].disposition)
    if not S.occ_put(r, 'scheduled', key, results[1].jobId, disp, r.next, false, r.next, ep.cutoff) then
      return U.noeffect('InvariantFault', 'occurrence_exists')
    end
    occurrences[1] = { kind = 'scheduled', key = key, jobId = results[1].jobId, disposition = disp }
  end
  local before = r.curev
  if resolution == 'extend' then
    r.ep = { id = ep.id, cutoff = ep.cutoff, emitted = ep.emitted, limit = ep.limit + add }
    r.review, r.reason, r.scan = false, nil, NOW
  else
    if r.next and r.next <= ep.cutoff then S.range(r, r.drev, 1, r.through, ep.cutoff, runOnce and 'run_once_covered' or 'catch_up_skipped') end
    r.through = (r.through and r.through > ep.cutoff) and r.through or ep.cutoff
    r.next, r.scan = after, after or NOW
    r.ep, r.review, r.reason = nil, false, nil
  end
  r.curev, r.updated, r.by = before + 1, NOW, actor.subject
  S.put(r)
  S.event('catch_up_resolved', r, actor.subject, { episodeId = S.dash(ep.id), resolution = resolution, additionalJobs = add, jobs = #occurrences })
  S.audit(r, actor, before, r.curev, { episodeId = S.dash(ep.id), resolution = resolution })
  return U.applied({ scheduleId = r.id, cursorRevision = r.curev, occurrences = occurrences })
end
return U.run(M)
