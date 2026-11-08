-- EventsSince/1 (08 §7.2, 22 §7.1) on the read path. ARGS: 1 after event id, 2 limit.
local g, m = U.read_gate()
if g then return g end
if m.admission == 'maintenance' then return { 'NONE', 'application_maintenance' } end
local ids = redis.call('ZRANGE', P .. 'events', '(' .. ARGS(1), '+inf', 'BYSCORE', 'LIMIT', 0, tonumber(ARGS(2)))
local out = { 'OK', m.rev or '0', NOWS }
if #ids > 0 then
  for _, row in ipairs(redis.call('HMGET', P .. 'events:data', unpack(ids))) do
    if row then out[#out + 1] = row end
  end
end
return out
