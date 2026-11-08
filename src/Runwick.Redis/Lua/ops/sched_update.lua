-- UpdateSchedule/1 (13 §6.4). ARGS: 1 key, 2 expected definition revision, 3 cutoff ms, 4 first due ms or '', 5 fingerprint hex,
-- 6 definition JSON, 7 payload length, 8 actor JSON.
local M = {}
function M.decide()
  local r = S.by_key(ARGS(1))
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  if r.drev ~= tonumber(ARGS(2)) then return S.conflict('actual=' .. U.S(r.drev)) end
  if r.ep then return U.noeffect('InvalidState', 'episode_unresolved') end
  local fp, cutoff, first = ARGS(5), tonumber(ARGS(3)), tonumber(ARGS(4))
  if r.fp == fp then return U.noeffect('InvalidState', 'no_change') end
  if NOW < cutoff then return U.noeffect('InvalidState', 'cutoff_in_future') end
  local actor = cjson.decode(ARGS(8))
  -- Forward cutover: unmaterialized old occurrences through the cutoff are skipped by edit; the high-water mark never moves back.
  local old, previous = r.drev, r.fp
  local rev = old + 1
  U.hset('sched', 'd:' .. r.id .. ':' .. U.S(rev), ARGS(6))
  local through = (r.through and r.through > cutoff) and r.through or cutoff
  if r.next and r.next <= cutoff then S.range(r, old, 1, r.through, cutoff, 'skipped_by_edit') end
  r.drev, r.through, r.next, r.scan = rev, through, first, first or NOW
  r.review, r.reason, r.rpol, r.rcut = false, nil, nil, nil
  r.curev, r.updated, r.by, r.fp, r.plen = r.curev + 1, NOW, actor.subject, fp, tonumber(ARGS(7))
  S.put(r)
  S.event('schedule_updated', r, actor.subject, { definitionRevision = rev, cutoff = cutoff, definitionFingerprint = fp })
  S.audit(r, actor, old, rev, { previousFingerprint = previous, definitionFingerprint = fp })
  return U.applied({ scheduleId = r.id, outcome = 'updated', definitionRevision = rev, storedFingerprint = fp })
end
return U.run(M)
