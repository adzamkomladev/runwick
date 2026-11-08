-- RecordDisclosure/1 (16 §3): the audit row and the read in one script; data leaves only after the intent is stored.
-- ARGS: 1 kind (RevealPayload | RevealError), 2 target id, 3 allowed queues JSON ('' = all), 4 actor JSON.
local M = {}
function M.decide()
  local payload, id, actor = ARGS(1) == 'RevealPayload', ARGS(2), cjson.decode(ARGS(4))
  local allowed
  if ARGS(3) ~= '' then
    allowed = {}
    for _, q in ipairs(cjson.decode(ARGS(3))) do allowed[q] = true end
  end
  local jobId, attempt = id, nil
  if not payload then
    jobId = U.hget('attempts', id)
    attempt = id
  end
  local j = jobId and U.job(jobId) or nil
  -- Nothing outside the caller's queues is confirmed; neither outcome is audited.
  if not j or (allowed and not allowed[j.queue]) then
    local r = U.applied({ status = 'not_found' })
    r.nocommit = true
    return r
  end
  local text = payload and U.sget('pay:' .. jobId) or nil
  if j.detailexp or (payload and not text) then
    local r = U.applied({ status = 'detail_expired', jobId = jobId })
    r.nocommit = true
    return r
  end
  U.audit(ARGS(1), actor, { targetKind = payload and 'job' or 'attempt', targetId = U.uuid_text(id), details = {} })
  return U.applied({ status = 'disclosed', jobId = jobId, payload = text or NULL, headers = payload and j.headers or NULL,
    error = (not payload) and U.nn(j['a:' .. attempt .. ':error']) or NULL })
end
return U.run(M)
