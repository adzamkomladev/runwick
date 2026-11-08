-- ReadinessProbe/2 (04 §7.4, 09 §2) on the read path. ARGS: 1 lanes JSON [[queue, priority], ...], 2 contracts JSON.
local g, m = U.read_gate()
if g then return g end
local lanes = cjson.decode(ARGS(1))
local supported = U.contract_set(ARGS(2))
local queues, ready, unsupported, controls, seen = U.arr(), U.arr(), U.arr(), {}, {}
for _, lane in ipairs(lanes) do
  local q = lane[1]
  if not seen[q] then
    seen[q] = true
    local row = U.jget('queues', q)
    if row then
      controls[q] = row
      local sid = U.hget('scopes', U.scope_field('queue', q))
      local cap, live = NULL, NULL
      if sid then
        local s = U.jget('scopes', sid)
        cap = s.capacity
        live = math.min(s.capacity, redis.call('ZCOUNT', P .. 'permits:' .. sid, '(' .. NOWS, '+inf'))
      end
      queues[#queues + 1] = { queue = q, paused = row.paused, revision = row.revision, scopeId = sid or NULL, capacity = cap, live = live }
    end
  end
end
for _, lane in ipairs(lanes) do
  local c = controls[lane[1]]
  if c and not c.paused then
    local key = P .. 'ready:' .. lane[1] .. ':' .. lane[2]
    local head = redis.call('ZRANGE', key, '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, 1)
    if #head == 1 then
      local j = redis.call('HMGET', P .. 'job:' .. head[1], 'name', 'pver', 'profile', 'profver')
      if not supported[j[1] .. '\0' .. j[2] .. '\0' .. j[3] .. '\0' .. j[4]] then unsupported[#unsupported + 1] = lane end
      local due = U.candidates(lane[1], lane[2], nil, nil, supported, 1)
      if #due == 1 then ready[#ready + 1] = { queue = lane[1], priority = lane[2], at = due[1].at } end
    end
  end
end
return { 'OK', m.rev or '0', NOWS, U.enc(queues), U.enc(ready), U.enc(unsupported) }
