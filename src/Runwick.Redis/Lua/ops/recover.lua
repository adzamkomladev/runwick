-- Recover/1 (04 §7.8). ARGS: 1 job, 2 expected attempt, 3 expected token, 4 retry draw.
local M = {}
function M.decide()
  local j = U.job(ARGS(1))
  if not j then return U.noeffect('NotFound', 'job_not_found') end
  local attempt, token = ARGS(2), ARGS(3)
  if j.attempt ~= attempt or j.token ~= token or not (j.state == 'reserved' or j.state == 'running') then
    return U.noeffect('StaleLease', 'stale_lease')
  end
  local reservedState, cancel = j.state == 'reserved', j.cancelat ~= nil
  local started, max = tonumber(U.gen(j, 'started')), tonumber(U.gen(j, 'max'))
  local k = tonumber(U.gen(j, 'unstarted')) + 1
  local state, reason
  if reservedState and cancel then state, reason = 'cancelled', 'cancelled_before_start'
  elseif reservedState and k >= tonumber(U.gen(j, 'maxun')) then state, reason = 'failed', 'reservation_abandonment_exhausted'
  elseif reservedState then state = started == 0 and 'pending' or 'retrying'
  elseif cancel then state, reason = 'cancelled', 'cancellation_requested'
  elseif started < max then state = 'retrying'
  else state, reason = 'failed', 'execution_budget_exhausted' end
  if tonumber(j.lease) > NOW then return U.noeffect('StaleLease', 'lease_renewed', { decided = NOWS }) end
  if reservedState and not cancel then U.genw(j, 'unstarted', U.must(U.inc(U.gen(j, 'unstarted')))) end
  local delay, eligible
  if not U.TERMINAL[state] then
    local policy = cjson.decode(reservedState and U.gen(j, 'uretry') or U.gen(j, 'retry'))
    delay = U.retry_delay(reservedState and k or started, policy.initialDelay, policy.maximumDelay, tonumber(ARGS(4)))
    eligible = NOW + delay
  end
  local outcome = reservedState and 'abandoned_before_start' or 'abandoned_running'
  local rev = U.close(j, attempt, token, outcome, state, reason, 'attempt_recovered', nil, delay, eligible)
  return U.applied({ jobId = j.id, generation = U.raw(j.gen), revision = U.raw(rev), state = state, attemptId = attempt,
    attemptOutcome = outcome, disposition = state, terminalReason = reason or NULL, eligibleAt = eligible or NULL, retryDelay = delay or NULL })
end
return U.run(M)
