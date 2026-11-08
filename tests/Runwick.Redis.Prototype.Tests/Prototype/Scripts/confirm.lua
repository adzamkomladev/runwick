-- Receipt-confirmation marker: a fresh write/reply boundary after reconnect. Under appendfsync always
-- the reply arrives only after this write (and everything before it in the AOF) is fsynced.
-- ARGV: 1 store id, 2 command id.
local m = redis.call('HMGET', META, 'store', 'guard')
if m[1] ~= ARGV[1] then return { 'NOEFFECT', 'UnsupportedVersion', 'store_identity_mismatch' } end
if m[2] and m[2] ~= '' then return { 'RECOVERING', m[2] } end
local k = P .. 'rcpt:' .. ARGV[2]
if redis.call('EXISTS', k) == 0 then return { 'NOEFFECT', 'NotFound', 'receipt_not_found' } end
local t = redis.call('TIME')
redis.call('HSET', k, 'confirmedAt', string.format('%d', tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)))
return { 'CONFIRMED' }
