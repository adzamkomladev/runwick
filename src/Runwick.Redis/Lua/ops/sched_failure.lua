-- RecordScheduleEvaluationFailure/1 (13 §6.7). ARGS: 1 body JSON { id, drev, code, detail }, 2 actor JSON.
local M = {}
function M.decide()
  local b = cjson.decode(ARGS(1))
  local r = S.get(b.id)
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  if r.drev ~= b.drev then return S.conflict('actual=' .. U.S(r.drev)) end
  if r.review then return U.noeffect('InvalidState', 'no_change') end
  local actor = cjson.decode(ARGS(2))
  -- The cursor never moves on a failure (13 §6.7).
  r.review, r.reason, r.updated = true, b.code, NOW
  S.put(r)
  local detail = U.nn(b.detail)
  S.event('schedule_needs_review', r, actor.subject, { reason = b.code, detail = S.nz(detail) })
  S.audit(r, actor, r.drev, r.drev, { code = b.code, detail = S.nz(detail) })
  return U.applied(S.state(r))
end
return U.run(M)
