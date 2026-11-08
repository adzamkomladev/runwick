-- The telemetry sample (18 §4) on the read path. ARGS: 1 queues JSON array, 2 due cap per queue.
-- Reply: OK rev now queues(JSON) needsReview oldestOverdue('' = none) | CLOSED rev | NONE code | RECOVERING guard.
local g, m = U.read_gate()
if g then return g end
local cap = tonumber(ARGS(2))
local out = U.arr()
for _, q in ipairs(cjson.decode(ARGS(1))) do
  local due, oldest = 0, nil
  for _, pr in ipairs({ 'high', 'medium', 'low' }) do
    local k = P .. 'ready:' .. q .. ':' .. pr
    if due < cap then due = math.min(cap, due + redis.call('ZCOUNT', k, '-inf', NOWS)) end
    local first = redis.call('ZRANGE', k, '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, 1, 'WITHSCORES')
    if #first == 2 and (not oldest or tonumber(first[2]) < oldest) then oldest = tonumber(first[2]) end
  end
  out[#out + 1] = { queue = q, due = due, oldest = oldest or NULL }
end
-- Schedules (sched hash, JSON row values): review count and the oldest overdue next-due instant.
-- shortcut: assumes schedule rows carry needsReview/enabled/paused/nextDueAt; align with the schedule store when it lands.
local review, overdue = 0, nil
local all = redis.call('HGETALL', P .. 'sched')
for i = 2, #all, 2 do
  if string.sub(all[i], 1, 1) == '{' then
    local s = cjson.decode(all[i])
    if s.needsReview == true then
      review = review + 1
    elseif s.enabled == true and s.paused ~= true and U.nn(s.nextDueAt) and s.nextDueAt <= NOW and (not overdue or s.nextDueAt < overdue) then
      overdue = s.nextDueAt
    end
  end
end
return { 'OK', m.rev or '0', NOWS, U.enc(out), U.S(review), overdue and U.S(overdue) or '' }
