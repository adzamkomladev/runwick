-- Authoritative reads. Every read checks the guard first: pending intents surface as RECOVERING,
-- never as empty work, committed success or confirmed rollback.
-- ARGV: 1 store id, 2 what, 3.. arguments.
local META = KEYS[1]
local P = string.sub(META, 1, #META - 4)
local m = redis.call('HMGET', META, 'store', 'guard', 'rev')
if not m[1] then return { 'NOEFFECT', 'AdmissionClosed', 'application_not_registered' } end
if m[1] ~= ARGV[1] then return { 'NOEFFECT', 'UnsupportedVersion', 'store_identity_mismatch' } end
if m[2] and m[2] ~= '' then return { 'RECOVERING', m[2] } end
local what = ARGV[2]
if what == 'job' then
  return { 'OK', m[3] or '0', redis.call('HGETALL', P .. 'job:' .. ARGV[3]) }
elseif what == 'summary' then
  return { 'OK', m[3] or '0', redis.call('HGETALL', P .. 'counters'),
    redis.call('ZCARD', P .. 'pending'), redis.call('ZCARD', P .. 'leases'),
    redis.call('ZCARD', P .. 'outboxIdx'), redis.call('ZCARD', P .. 'auditIdx') }
elseif what == 'resolve' then
  -- ARGV: 3 command id, 4 op, 5 fingerprint, 6 NotAfter, 7 class floor ms
  local r = redis.call('HMGET', P .. 'rcpt:' .. ARGV[3], 'op', 'fp', 'status', 'result', 'decidedAt')
  if r[1] then
    if r[1] ~= ARGV[4] or r[2] ~= ARGV[5] then return { 'Conflict', m[3] or '0' } end
    return { 'Recorded', m[3] or '0', r[3], r[4], r[5] }
  end
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  local na = tonumber(ARGV[6])
  if now < na then return { 'NotObserved', m[3] or '0' } end
  if now < na + tonumber(ARGV[7]) then return { 'NotApplied', m[3] or '0' } end
  return { 'ExpiredUnknown', m[3] or '0' }
end
return redis.error_reply('ERR unknown read')
