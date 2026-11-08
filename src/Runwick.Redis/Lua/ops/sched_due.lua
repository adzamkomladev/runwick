-- Due read (13 §6.9) on the read path. ARGS: 1 limit. Reply: OK, rev, observed ms, then (row, definition) per schedule.
local g, m = U.read_gate()
if g then
  if g[1] == 'CLOSED' then return { 'OK', g[2] } end
  return g
end
local out = { 'OK', m.rev or '0', NOWS }
local ids = redis.call('ZRANGE', P .. 'sched:due', '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, tonumber(ARGS(1)))
for _, id in ipairs(ids) do
  local row = U.hget('sched', id)
  out[#out + 1] = row
  out[#out + 1] = U.hget('sched', 'd:' .. id .. ':' .. U.S(cjson.decode(row).drev))
end
return out
