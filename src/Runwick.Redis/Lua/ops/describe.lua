-- Describe/1, application part (22 §6): the application gate at one revision. rw:store is read by the client.
local m = redis.call('HGETALL', META)
local t = {}
for i = 1, #m, 2 do t[m[i]] = m[i + 1] end
if not t.store then return { 'NONE', 'application_not_registered' } end
if t.guard and t.guard ~= '' then return { 'RECOVERING', t.guard } end
return { 'OK', t.rev or '0', t.store, t.gaterev or '1', t.reqepoch or '1', t.caps or '{}', t.admission or 'open', NOWS }
