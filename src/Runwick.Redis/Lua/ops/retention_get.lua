-- The retention read (17 §7) on the read path. Reply: OK, rev, observed ms, policy row JSON or '', then (class, progress JSON) pairs.
local g, m = U.read_gate()
if g then
  if g[1] == 'CLOSED' then return { 'OK', g[2] } end
  return g
end
local out = { 'OK', m.rev or '0', NOWS, U.hget('retention', 'policy') or '' }
local all = redis.call('HGETALL', P .. 'retention')
for i = 1, #all, 2 do
  if string.sub(all[i], 1, 2) == 'p:' then
    out[#out + 1] = string.sub(all[i], 3)
    out[#out + 1] = all[i + 1]
  end
end
return out
