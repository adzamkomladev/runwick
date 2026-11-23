-- EventsSince/1 (08 §7.2, 22 §7.1) on the read path. ARGS: 1 after event id, 2 limit.
local g, m = U.read_gate()
if g then return g end
if m.admission == 'maintenance' then return { 'NONE', 'application_maintenance' } end
-- Entries are layout-1 rows (one id each) or layout-2 packs (22 §3.2: consecutive ids from the member's). The pack holding after + 1
-- may start at or before after, so the last entry at or before after is read too; rows are expanded and cut to (after, limit].
local after, limit = tonumber(ARGS(1)), tonumber(ARGS(2))
local ids = redis.call('ZRANGE', P .. 'events', ARGS(1), '-inf', 'BYSCORE', 'REV', 'LIMIT', 0, 1)
for _, id in ipairs(redis.call('ZRANGE', P .. 'events', '(' .. ARGS(1), '+inf', 'BYSCORE', 'LIMIT', 0, limit)) do ids[#ids + 1] = id end
local out = { 'OK', m.rev or '0', NOWS }
if #ids > 0 then
  local n = 0
  local function emit(id, row)
    if id > after and n < limit then
      n = n + 1
      out[#out + 1] = row
    end
  end
  for i, v in ipairs(redis.call('HMGET', P .. 'events:data', unpack(ids))) do
    if v then U.event_rows(tonumber(ids[i]), v, emit) end
    if n >= limit then break end
  end
end
return out
