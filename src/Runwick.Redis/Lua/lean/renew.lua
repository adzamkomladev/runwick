-- Renew/1 and RenewMany/1, lean path: leases of jobs with no concurrency scope (no permits to move). No receipt. ARGS: 1 lease ms,
-- 2 many, 3 leases JSON array.
local m = L.frame()
if not m then return GENERIC() end
local ms = tonumber(ARGS(1))
local leases = cjson.decode(ARGS(3))
if not ms or type(leases) ~= 'table' or #leases < 1 or #leases > 64 then return GENERIC() end
local expiry = NOW + ms
local es = L.S(expiry)
local states = { reserved = true, running = true }
local many = ARGS(2) == '1'
local fields = { 'state', 'attempt', 'token', 'worker', 'gen', 'lease', 'scopes', 'cancelat' }
local out, seen, renew = {}, {}, {}
for i = 1, many and #leases or 1 do
  local l = L.lease_of(leases[i])
  if not l or seen[l.job] then return GENERIC() end
  seen[l.job] = true
  local a = redis.call('HMGET', P .. 'job:' .. l.job, unpack(fields))
  local j = { id = l.job }
  for k = 1, #fields do if a[k] then j[fields[k]] = a[k] end end
  local status, code = L.ownership(j, l, states)
  if not status and j.scopes and j.scopes ~= '' then return GENERIC() end
  if status then
    if not many then return { 'R', status, 'N', '0', code, '', NOWS, '', '', '', '', '' } end
    out[i] = '{"code":' .. q(code) .. ',"status":' .. q(status) .. '}'
  else
    renew[#renew + 1] = j
    local c = j.cancelat ~= nil and 'true' or 'false'
    out[i] = many and '{"cancelRequested":' .. c .. ',"leaseExpiresAt":' .. es .. ',"status":"Applied"}'
      or '{"cancelRequested":' .. c .. ',"leaseExpiresAt":' .. es .. '}'
  end
end
local reply = L.reply(many and '{"items":[' .. concat(out, ',') .. ']}' or out[1])
if #renew == 0 then return L.with_payloads(reply, nil, nil) end -- RenewMany with nothing renewed: no commit (r.nocommit)
if not L.can_commit(m) then return GENERIC() end
for _, j in ipairs(renew) do
  L.jw(j, 'lease', es)
  L.zadd('leases', es, j.id)
end
L.commit(m, reply)
return L.with_payloads(reply, nil, nil)
