-- Renew/1 and RenewMany/1 (04 §7.6, PF-06): no receipt. ARGS: 1 lease ms, 2 many, 3 leases JSON array.
local M = {}
local function renew_one(lease, expiry)
  local j = U.job(lease.job)
  local refused = U.ownership(j, lease, { reserved = true, running = true })
  if refused then return refused end
  local required = U.permit_scopes(j)
  for _, sid in ipairs(required) do
    local p = U.jget('permits', sid .. ':' .. j.id)
    if not (p and p.attempt == lease.attempt and p.token == lease.token and U.S(p.lease) == j.lease) then
      return U.noeffect('InvariantFault', 'permit_set_mismatch')
    end
  end
  for _, sid in ipairs(required) do
    local p = U.jget('permits', sid .. ':' .. j.id)
    p.lease = expiry
    U.jset('permits', sid .. ':' .. j.id, p)
    U.zadd('permits:' .. sid, expiry, j.id)
  end
  U.jw(j, 'lease', U.S(expiry))
  U.zadd('leases', expiry, j.id)
  return U.applied({ leaseExpiresAt = expiry, cancelRequested = j.cancelat ~= nil })
end
function M.decide()
  local expiry = NOW + tonumber(ARGS(1))
  local leases = cjson.decode(ARGS(3))
  if ARGS(2) ~= '1' then return renew_one(U.lease_of(leases[1]), expiry) end
  local items, renewed = U.arr(), 0
  for _, l in ipairs(leases) do
    local r = renew_one(U.lease_of(l), expiry)
    if r.effect == 'A' then
      renewed = renewed + 1
      items[#items + 1] = { status = 'Applied', leaseExpiresAt = expiry, cancelRequested = r.result.cancelRequested }
    else
      items[#items + 1] = { status = r.status, code = r.code }
    end
  end
  local r = U.applied({ items = items })
  r.nocommit = renewed == 0
  return r
end
return U.run(M)
