-- ListUnsupportedBacklog/1 (09 §5) on the read path. ARGS: 1 sample per lane.
local g, m = U.read_gate()
local closed = g and g[1] == 'CLOSED'
if g and not closed then return g end
local per = tonumber(ARGS(1))
-- Live workers and the tuples each advertises.
local live, liveCount = {}, 0
local wids = redis.call('ZRANGE', P .. 'workers:exp', '(' .. NOWS, '+inf', 'BYSCORE')
for _, id in ipairs(wids) do
  local w = U.jget('workers', id)
  if w and (w.status == 'starting' or w.status == 'ready' or w.status == 'degraded') then
    liveCount = liveCount + 1
    local caps, subs = {}, {}
    for _, c in ipairs(w.caps or {}) do caps[c[1] .. '\0' .. U.S(c[2]) .. '\0' .. c[3] .. '\0' .. U.S(c[4])] = true end
    for _, q in ipairs(w.subscriptions or {}) do subs[q] = true end
    live[#live + 1] = { caps = caps, subs = subs }
  end
end
local entries = U.arr()
if not closed then
  local qs = redis.call('HKEYS', P .. 'queues')
  table.sort(qs, U.blt)
  for _, q in ipairs(qs) do
    for _, pr in ipairs({ 'high', 'medium', 'low' }) do
      local ids = redis.call('ZRANGE', P .. 'ready:' .. q .. ':' .. pr, '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, per, 'WITHSCORES')
      local truncated = #ids / 2 == per
      local groups, order = {}, {}
      for i = 1, #ids, 2 do
        local j = redis.call('HMGET', P .. 'job:' .. ids[i], 'name', 'pver', 'profile', 'profver')
        local t = j[1] .. '\0' .. j[2] .. '\0' .. j[3] .. '\0' .. j[4]
        local at = tonumber(ids[i + 1])
        local gr = groups[t]
        if not gr then
          gr = { c = j, oldest = at, n = 0, t = t }
          groups[t] = gr
          order[#order + 1] = gr
        end
        gr.n = gr.n + 1
        if at < gr.oldest then gr.oldest = at end
      end
      table.sort(order, function(a, b) return a.oldest < b.oldest end)
      for _, gr in ipairs(order) do
        local sub, any = false, false
        for _, w in ipairs(live) do
          if w.caps[gr.t] then
            any = true
            if w.subs[q] then sub = true end
          end
        end
        if not sub then
          entries[#entries + 1] = { queue = q, priority = pr, name = gr.c[1], pver = tonumber(gr.c[2]), profile = gr.c[3],
            profver = tonumber(gr.c[4]), oldest = gr.oldest, n = gr.n, truncated = truncated,
            reason = any and 'advertised_but_not_subscribed' or 'no_live_worker_advertises_tuple' }
        end
      end
    end
  end
end
return { 'OK', m and m.rev or '0', NOWS, U.S(liveCount), U.enc(entries) }
