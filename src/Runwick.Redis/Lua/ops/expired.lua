-- ListExpiredLeases/1 (04 §7.11) on the read path. ARGS: 1 limit.
local g, m = U.read_gate()
if g then
  if g[1] == 'CLOSED' then return { 'OK', g[2] } end
  return g
end
local out = { 'OK', m.rev or '0' }
local ids = redis.call('ZRANGE', P .. 'leases', '-inf', NOWS, 'BYSCORE', 'LIMIT', 0, tonumber(ARGS(1)), 'WITHSCORES')
for i = 1, #ids, 2 do
  local j = redis.call('HMGET', P .. 'job:' .. ids[i], 'attempt', 'token', 'state', 'lease')
  if j[1] and (j[3] == 'reserved' or j[3] == 'running') then
    out[#out + 1] = ids[i]
    out[#out + 1] = j[1]
    out[#out + 1] = j[2]
    out[#out + 1] = j[3]
    out[#out + 1] = j[4]
  end
end
return out
