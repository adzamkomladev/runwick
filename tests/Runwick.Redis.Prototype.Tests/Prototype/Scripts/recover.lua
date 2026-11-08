-- Reapply a bounded batch of the pending intent's after-images; publish after the last batch.
-- ARGV: 1 failAfter, 2 expected guard ('' = whatever is pending), 3 first op (1-based), 4 batch size.
failAfter = tonumber(ARGV[1])
local expected, from, count = ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4])
local m = redis.call('HMGET', META, 'guard', 'intent', 'intentsha', 'rev')
if not m[1] or m[1] == '' then return { 'CLEAN' } end
if expected ~= '' and m[1] ~= expected then return { 'OTHER', m[1] } end
if redis.sha1hex(m[2]) ~= m[3] then return redis.error_reply('INVARIANT intent_fingerprint_mismatch') end
local it = cjson.decode(m[2])
if it.id ~= m[1] then return redis.error_reply('INVARIANT intent_guard_mismatch') end
if it.rev ~= dadd(m[4] or '0', 1) then return redis.error_reply('INVARIANT intent_revision_mismatch') end
local last = math.min(#it.ops, from + count - 1)
for i = from, last do apply(it.ops[i]) end
if last < #it.ops then
  injectAtEnd()
  return { 'MORE', m[1], last + 1 }
end
publish(it.rev)
injectAtEnd()
return { 'PUBLISHED', m[1], it.reply[1], it.reply[2], it.reply[3], it.reply[4], it.reply[5] }
