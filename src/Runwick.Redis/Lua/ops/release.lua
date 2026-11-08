-- ReleaseReservation/1 (04 §7.9). ARGS: 1 lease JSON.
local M = {}
function M.decide()
  local lease = U.lease_of(cjson.decode(ARGS(1)))
  local j = U.job(lease.job)
  local refused = U.ownership(j, lease, { reserved = true })
  if refused then return refused end
  local cancel = j.cancelat ~= nil
  local state = cancel and 'cancelled' or (U.gen(j, 'started') == '0' and 'pending' or 'retrying')
  local eligible = j.eligible
  local rev = U.close(j, lease.attempt, lease.token, 'released', state, cancel and 'cancelled_before_start' or nil, 'attempt_released')
  return U.applied({ jobId = j.id, generation = U.raw(j.gen), revision = U.raw(rev), state = state, attemptId = lease.attempt,
    disposition = state, eligibleAt = cancel and NULL or U.raw(eligible) })
end
return U.run(M)
