-- EstimateDemand/1 (11 §4) on the read path. ARGS: 1 instance hex, 2 queues JSON array, 3 contracts JSON, 4 per-lane count cap.
-- Reply: OK rev now truncated rows(JSON) liveIds(JSON) overrides(JSON) | CLOSED rev | NONE code | RECOVERING guard.
local g, m = U.read_gate()
if g then return g end
local instance, cap = ARGS(1), tonumber(ARGS(4))
local supported = U.contract_set(ARGS(3))
local live = {}
for _, id in ipairs(redis.call('ZRANGE', P .. 'workers:exp', '(' .. NOWS, '+inf', 'BYSCORE')) do
  local w = U.jget('workers', id)
  if w and (w.status == 'starting' or w.status == 'ready' or w.status == 'degraded') then
    local caps, subs = {}, {}
    for _, c in ipairs(w.caps or {}) do caps[c[1] .. '\0' .. U.S(c[2]) .. '\0' .. c[3] .. '\0' .. U.S(c[4])] = true end
    for _, q in ipairs(w.subscriptions or {}) do subs[q] = true end
    live[#live + 1] = { id = id, caps = caps, subs = subs }
  end
end
-- Per lane, up to the cap of due rows in (eligible_at, job_id) order, grouped by supported tuple.
local truncated, rows, seenQ = false, U.arr(), {}
for _, q in ipairs(cjson.decode(ARGS(2))) do
  if not seenQ[q] then
    seenQ[q] = true
    local due, order = {}, {}
    for _, pr in ipairs({ 'high', 'medium', 'low' }) do
      local ids = redis.call('ZRANGE', P .. 'ready:' .. q .. ':' .. pr, '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, cap)
      if #ids == cap then truncated = true end
      for _, id in ipairs(ids) do
        local c = redis.call('HMGET', P .. 'job:' .. id, 'name', 'pver', 'profile', 'profver', 'state')
        if c[5] == 'pending' or c[5] == 'retrying' then
          local t = c[1] .. '\0' .. c[2] .. '\0' .. c[3] .. '\0' .. c[4]
          if supported[t] then
            if not due[t] then
              due[t] = { c = c, n = 0 }
              order[#order + 1] = t
            end
            due[t].n = due[t].n + 1
          end
        end
      end
    end
    for _, t in ipairs(order) do
      local capable = 0
      for _, w in ipairs(live) do if w.caps[t] and w.subs[q] then capable = capable + 1 end end
      local c = due[t].c
      rows[#rows + 1] = { queue = q, name = c[1], pver = tonumber(c[2]), profile = c[3], profver = tonumber(c[4]), due = due[t].n,
        capable = capable }
    end
  end
end
local ids = U.arr()
for _, w in ipairs(live) do ids[#ids + 1] = w.id end
-- This instance's unexpired overrides (pools fields <instanceHex>:<pool>).
local overrides, prefix = {}, instance .. ':'
local all = redis.call('HGETALL', P .. 'pools')
for i = 1, #all, 2 do
  if string.sub(all[i], 1, #prefix) == prefix then
    local o = cjson.decode(all[i + 1])
    if U.nn(o.target) and not (U.nn(o.expiresAt) and o.expiresAt <= NOW) then overrides[o.pool] = o.target end
  end
end
return { 'OK', m.rev or '0', NOWS, truncated and '1' or '0', U.enc(rows), U.enc(ids), U.enc(overrides) }
