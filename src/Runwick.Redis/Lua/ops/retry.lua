-- ManualRetry/1 and RerunSucceeded/1 (08 §3.3): a new generation. ARGS: 1 job id, 2 expected generation, 3 expected revision,
-- 4 actor JSON, 5 '1' for RerunSucceeded.
local M = {}
function M.decide()
  local id, actor, rerun = ARGS(1), cjson.decode(ARGS(4)), ARGS(5) == '1'
  local j = U.job(id)
  if not j then return U.noeffect('NotFound', 'job_not_found') end
  if j.gen ~= ARGS(2) or j.rev ~= ARGS(3) then
    return U.noeffect('VersionConflict', 'revision_conflict', { detail = 'actual=' .. j.gen .. '/' .. j.rev })
  end
  if rerun then
    if j.state ~= 'succeeded' then return U.noeffect('InvalidState', 'invalid_state') end
  elseif j.state ~= 'failed' and j.state ~= 'cancelled' then
    return U.noeffect('InvalidState', 'invalid_state')
  end
  if redis.call('EXISTS', P .. 'pay:' .. id) == 0 or j.detailexp then return U.noeffect('HistoryUnavailable', 'payload_unavailable') end
  if not U.hget('queues', j.queue) then return U.noeffect('InvalidRequest', 'queue_not_registered') end
  local keyf, holder = nil, nil
  if j.akey then
    keyf = j.name .. '\0' .. j.akey
    holder = U.hget('keys:active', keyf)
    if holder and holder ~= id then return U.noeffect('UniquenessConflict', 'uniqueness_conflict', { conflict = holder }) end
  end
  local prev, prevgen = j.state, j.gen
  local gen = U.must(U.inc(prevgen))
  local rev = U.must(U.inc(j.rev))
  local reason = rerun and 'rerun_succeeded' or 'manual_retry'
  local g = 'g:' .. gen .. ':'
  U.jw(j, g .. 'created', NOWS)
  U.jw(j, g .. 'by', actor.subject)
  U.jw(j, g .. 'reason', reason)
  for _, f in ipairs({ 'max', 'maxun', 'timeout', 'retry', 'uretry' }) do U.jw(j, g .. f, j['g:' .. prevgen .. ':' .. f]) end
  U.jw(j, g .. 'started', '0')
  U.jw(j, g .. 'unstarted', '0')
  U.jw(j, 'gen', gen)
  U.jw(j, 'rev', rev)
  U.jw(j, 'eligible', NOWS)
  U.jw(j, 'updated', NOWS)
  for _, f in ipairs({ 'terminalat', 'reason', 'cancelat', 'cancelreason', 'cancelby' }) do U.jw(j, f, nil) end
  U.set_state(j, 'pending')
  if keyf and not holder then U.hset('keys:active', keyf, id) end
  local effect = rerun and 'rerun' or 'retried'
  U.event(rerun and 'job_rerun' or 'job_retried', id, nil, { generation = tonumber(gen), previousState = prev, reason = reason }, actor.subject)
  U.audit(OP, actor, { targetKind = 'job', targetId = U.uuid_text(id), expectedRevision = U.raw(ARGS(3)), resultingRevision = U.raw(rev),
    details = { effect = effect, previousState = prev, generation = tonumber(gen) } })
  return U.applied({ jobId = id, generation = U.raw(gen), revision = U.raw(rev), state = 'pending', effect = effect,
    terminalReason = NULL, cancelRequestedAt = NULL })
end
return U.run(M)
