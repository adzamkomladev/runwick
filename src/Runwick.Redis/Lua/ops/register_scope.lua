-- RegisterScope/1 (04 §7.1). ARGS: JSON array of {kind, key, capacity, id}.
local M = {}
function M.decide()
  local first = {}
  for _, s in ipairs(cjson.decode(ARGS(1))) do
    local f = s.kind .. '\0' .. s.key
    if not first[f] then first[f] = s end
  end
  local out = U.arr()
  for _, f in ipairs(U.sorted(first)) do
    local s = first[f]
    local id, row, created = U.seed_scope('scopes', s.kind, s.key, s.id, U.new_concurrency_scope(s.kind, s.key, s.capacity))
    out[#out + 1] = { scopeId = id, kind = s.kind, key = s.key, created = created, storedCapacity = row.capacity,
      configuredCapacity = s.capacity, policyRevision = row.prev, fencingEnabled = row.fencing }
  end
  return U.applied({ scopes = out })
end
return U.run(M)
