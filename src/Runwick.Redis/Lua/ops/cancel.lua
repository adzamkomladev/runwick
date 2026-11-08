-- Cancel/1 (08 §3.2). ARGS: 1 job id, 2 expected generation, 3 expected revision, 4 actor JSON.
local M = {}
function M.decide()
  local id, actor = ARGS(1), cjson.decode(ARGS(4))
  local j = U.job(id)
  if not j then return U.noeffect('NotFound', 'job_not_found') end
  if j.gen ~= ARGS(2) or j.rev ~= ARGS(3) then
    return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. j.gen .. '/' .. j.rev })
  end
  if U.TERMINAL[j.state] then return U.noeffect('InvalidState', 'job_terminal') end
  local prev, attempt = j.state, j.attempt
  local waiting = prev == 'pending' or prev == 'retrying'
  if not waiting and j.cancelat then return U.noeffect('InvalidState', 'cancel_already_requested') end
  local rev = U.must(U.inc(j.rev))
  U.jw(j, 'cancelat', NOWS)
  U.jw(j, 'cancelreason', U.nn(actor.reason))
  U.jw(j, 'cancelby', actor.subject)
  U.jw(j, 'rev', rev)
  U.jw(j, 'updated', NOWS)
  local state, reason, effect = prev, nil, 'cancel_requested'
  if waiting then
    -- T17: close the waiting job; the active binding is released only if it still names this job (03 §5.6).
    state, reason, effect = 'cancelled', 'cancelled_while_waiting', 'cancelled'
    U.jw(j, 'terminalat', NOWS)
    U.jw(j, 'reason', reason)
    U.set_state(j, state)
    if j.akey then
      local f = j.name .. '\0' .. j.akey
      if U.hget('keys:active', f) == id then U.hset('keys:active', f, nil) end
    end
  end
  U.event(waiting and 'job_cancelled' or 'job_cancel_requested', id, attempt,
    { generation = tonumber(j.gen), revision = U.raw(rev), previousState = prev }, actor.subject)
  U.audit(OP, actor, { targetKind = 'job', targetId = U.uuid_text(id), expectedRevision = U.raw(ARGS(3)), resultingRevision = U.raw(rev),
    details = { effect = effect, previousState = prev } })
  return U.applied({ jobId = id, generation = U.raw(j.gen), revision = U.raw(rev), state = state, effect = effect,
    terminalReason = reason or NULL, cancelRequestedAt = NOW })
end
return U.run(M)
