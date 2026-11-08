-- Finishes the pending intent in bounded batches (22 §4.2). Never deletes an intent. ARGV: 1 failAfter, 2 expected guard
-- ('' = any), 3 batch size. Replies: CLEAN | OTHER guard | MORE guard | PUBLISHED guard reply... | STUCK reason.
local m = redis.call('HMGET', META, 'guard', 'intent', 'intentsha', 'rev', 'ipos')
if not m[1] or m[1] == '' then return { 'CLEAN' } end
if ARGV[2] ~= '' and m[1] ~= ARGV[2] then return { 'OTHER', m[1] } end
if not m[2] or redis.sha1hex(m[2]) ~= m[3] then return { 'STUCK', 'intent_fingerprint_mismatch', m[1] } end
local it = cmsgpack.unpack(m[2])
if it.id ~= m[1] then return { 'STUCK', 'intent_guard_mismatch', m[1] } end
if it.rev ~= U.inc(m[4] or '0') then return { 'STUCK', 'intent_revision_mismatch', m[1] } end
local nxt = U.project(it, tonumber(m[5]) or 1, tonumber(ARGV[3]) or 100)
if nxt then return { 'MORE', m[1] } end
local out = { 'PUBLISHED', m[1] }
for _, v in ipairs(it.reply) do out[#out + 1] = v end
return out
