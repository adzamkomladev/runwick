-- Due read (13 §6.9). ARGS: 1 limit, optional 2 cursor score, 3 cursor id, 4 scan-through ms.
-- Reply: OK, rev, observed ms, then (row, definition) per schedule.
local g, m = U.read_gate()
if g then
  if g[1] == 'CLOSED' then return { 'OK', g[2] } end
  return g
end
local out = { 'OK', m.rev or '0', NOWS }
local key, limit, score = P .. 'sched:due', tonumber(ARGS(1)), ARGS(2)
local ids, through
if score then
  through = math.min(NOW, tonumber(ARGS(4)))
  if tonumber(score) > through then return out end
  -- Seek to the insertion rank within this score's tie group. The anchor may have moved or been removed.
  local first = redis.call('ZCOUNT', key, '-inf', '(' .. score)
  local last = first + redis.call('ZCOUNT', key, score, score)
  local anchor = ARGS(3)
  while first < last do
    local mid = math.floor((first + last) / 2)
    local id = redis.call('ZRANGE', key, mid, mid)[1]
    if U.blt(anchor, id) then last = mid else first = mid + 1 end
  end
  ids = redis.call('ZRANGE', key, first, first + limit - 1, 'WITHSCORES')
else
  through = NOW
  ids = redis.call('ZRANGE', key, '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, limit, 'WITHSCORES')
end
for i = 1, #ids, 2 do
  if tonumber(ids[i + 1]) > through then break end
  local id = ids[i]
  local row = U.hget('sched', id)
  out[#out + 1] = row
  out[#out + 1] = U.hget('sched', 'd:' .. id .. ':' .. U.S(cjson.decode(row).drev))
end
return out
