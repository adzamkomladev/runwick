-- MaterializeOccurrences/1 (13 §4.2, materialize_occurrences_v3). ARGS: 1 plan JSON, 2 item count, then (item JSON, payload) per item.
-- Plan: { id, drev, crev, curev, ep, cutoff, occ = [{at, from, through, coal}], after = {through, next, scan, ep, review}, ranges, clear }.
local M = {}
function M.decide()
  if not S.active() then return U.noeffect('UnsupportedVersion', 'scheduler_not_active') end
  local b = cjson.decode(ARGS(1))
  local r = S.get(b.id)
  -- Plan checks precede the dispatch core, so a stale plan is always reported as stale (13 §4.2).
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  if not r.enabled then return U.noeffect('InvalidState', 'schedule_disabled') end
  if r.paused then return U.noeffect('InvalidState', 'schedule_paused') end
  if r.review then return U.noeffect('InvalidState', 'schedule_needs_review') end
  local want = U.nn(b.ep)
  if r.drev ~= b.drev or r.crev ~= b.crev or r.curev ~= b.curev or (r.ep and r.ep.id or nil) ~= want then
    return S.conflict('actual=' .. U.S(r.drev) .. '/' .. U.S(r.crev) .. '/' .. U.S(r.curev))
  end
  local n, a = #b.occ, b.after
  if n * r.plen > S.MAX_PLAN then return U.noeffect('InvalidRequest', 'plan_too_large') end
  if r.through then
    for _, o in ipairs(b.occ) do
      if o.at <= r.through then return U.noeffect('InvariantFault', 'occurrence_below_high_water') end
    end
    local t = U.nn(a.through)
    if not t or t < r.through then return U.noeffect('InvariantFault', 'cursor_rewind') end
  end
  local items = S.items(3, tonumber(ARGS(2)))
  if #items ~= n then return S.conflict('items') end
  local refused, results = S.dispatch(items)
  if refused then return refused end
  -- The clock went backward after planning: reject and replan.
  if NOW < b.cutoff then return U.noeffect('InvalidState', 'cutoff_in_future') end
  local occurrences = U.arr()
  for i, o in ipairs(b.occ) do
    local key, res = U.S(o.at), results[i]
    local disp = S.disp(res.disposition)
    if not S.occ_put(r, 'scheduled', key, res.jobId, disp, o.at, o.coal, U.nn(o.from), U.nn(o.through)) then
      return U.noeffect('InvariantFault', 'occurrence_exists')
    end
    occurrences[i] = { kind = 'scheduled', key = key, jobId = res.jobId, disposition = disp }
  end
  local reason = U.nn(a.review)
  r.through, r.next, r.scan, r.ep = U.nn(a.through), U.nn(a.next), a.scan, U.nn(a.ep)
  r.review, r.reason = reason ~= nil, reason
  if b.clear then r.rpol, r.rcut = nil, nil end
  local drev = r.drev
  r.curev, r.updated = r.curev + 1, NOW
  S.put(r)
  for i, g in ipairs(b.ranges) do S.range(r, drev, i, U.nn(g.from), g.through, g.reason) end
  S.event('schedule_materialized', r, S.ACTOR, { cursorRevision = r.curev, cutoff = b.cutoff, jobs = n, skippedRanges = #b.ranges,
    episodeId = r.ep and S.dash(r.ep.id) or NULL, needsReview = S.nz(reason) })
  if reason then S.event('schedule_needs_review', r, S.ACTOR, { reason = reason }) end
  return U.applied({ scheduleId = r.id, cursorRevision = r.curev, occurrences = occurrences })
end
return U.run(M)
