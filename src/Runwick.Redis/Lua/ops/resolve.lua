-- ResolveCommand/1 (04 §7.13, 22 §4.5): on the write path when a receipt exists (the confirmation marker). ARGV (prelude
-- layout): 2 store id, 3 operation ('' = any), 4 command id, 5 fingerprint, 6 NotAfter (bound) ms, 8 retention floor ms.
local m = redis.call('HMGET', META, 'store', 'guard')
if not m[1] then return { 'NONE', 'application_not_registered' } end
if m[1] ~= STORE then return { 'NONE', 'store_identity_mismatch' } end
if m[2] and m[2] ~= '' then return { 'RECOVERING', m[2] } end
local k = P .. 'rcpt:' .. CMD
local r = redis.call('HMGET', k, 'op', 'ver', 'fp', 'status', 'decided', 'result')
if r[1] then
  if r[2] ~= '1' then return { 'Incompatible', r[2] } end
  if OP ~= '' and (r[1] ~= OP or r[3] ~= FP) then return { 'Conflict' } end
  if not r[6] or r[6] == '' then return { 'NotObserved' } end
  redis.call('HSET', k, 'confirmed', NOWS)
  return { 'Recorded', r[1], r[2], r[4], r[5], r[6] }
end
if NOW < NOT_AFTER then return { 'NotObserved' } end
if NOW < NOT_AFTER + FLOOR then return { 'NotApplied' } end
return { 'ExpiredUnknown' }
