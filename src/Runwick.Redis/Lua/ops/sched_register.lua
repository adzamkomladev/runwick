-- RegisterSchedule/1 (13 §6.1). ARGS: 1 key, 2 first due ms or '', 3 cutoff ms, 4 cron ('1'/'0'), 5 fingerprint hex, 6 definition JSON,
-- 7 new schedule id, 8 payload length.
local M = {}
function M.decide()
  if not S.active() then return U.noeffect('UnsupportedVersion', 'scheduler_not_active') end
  local key, first, cutoff, fp = ARGS(1), tonumber(ARGS(2)), tonumber(ARGS(3)), ARGS(5)
  if NOW < cutoff then return U.noeffect('InvalidState', 'cutoff_in_future') end
  local old = S.by_key(key)
  if old then
    -- Startup never overwrites: a different fingerprint is a drift diagnostic.
    return U.applied({ scheduleId = old.id, outcome = old.fp == fp and 'same' or 'drift', definitionRevision = old.drev, storedFingerprint = old.fp })
  end
  local id = ARGS(7)
  local r = { id = id, key = key, drev = 1, crev = 1, curev = 1, enabled = true, paused = false, review = false, next = first,
    scan = first or cutoff, through = ARGS(4) == '1' and cutoff or nil, fp = fp, plen = tonumber(ARGS(8)), created = NOW, updated = NOW }
  U.hset('sched', 'k:' .. key, id)
  U.hset('sched', 'd:' .. id .. ':1', ARGS(6))
  S.put(r)
  S.event('schedule_registered', r, S.ACTOR, { firstDueAt = S.nz(first), definitionFingerprint = fp })
  return U.applied({ scheduleId = id, outcome = 'created', definitionRevision = 1, storedFingerprint = fp })
end
return U.run(M)
