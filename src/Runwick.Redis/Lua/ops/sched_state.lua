-- SetScheduleState/1 (13 §6.3). ARGS: 1 key, 2 paused ('1'/'0'), 3 resume policy or '', 4 expected control revision, 5 actor JSON.
local M = {}
function M.decide()
  local r = S.by_key(ARGS(1))
  if not r then return U.noeffect('NotFound', 'schedule_not_found') end
  if r.crev ~= tonumber(ARGS(4)) then return S.conflict('actual=' .. U.S(r.crev)) end
  local paused, policy = ARGS(2) == '1', ARGS(3) ~= '' and ARGS(3) or nil
  if r.paused == paused then return U.noeffect('InvalidState', 'no_change') end
  if not paused and r.ep then return U.noeffect('InvalidState', 'episode_unresolved') end
  local actor = cjson.decode(ARGS(5))
  local before = r.crev
  -- Pause cancels nothing. Resume stores its policy and cutoff; the next plan applies it (13 §5.4).
  r.paused, r.crev, r.updated, r.by = paused, before + 1, NOW, actor.subject
  if not paused then r.rpol, r.rcut, r.scan = policy, NOW, NOW end
  S.put(r)
  S.event(paused and 'schedule_paused' or 'schedule_resumed', r, actor.subject, { controlRevision = r.crev, resumePolicy = S.nz(paused and nil or policy) })
  S.audit(r, actor, before, r.crev, { paused = paused, resumePolicy = S.nz(paused and nil or policy) })
  return U.applied(S.state(r))
end
return U.run(M)
