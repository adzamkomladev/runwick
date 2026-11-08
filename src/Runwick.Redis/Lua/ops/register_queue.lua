-- RegisterQueue/1: seed-if-absent in byte order. ARGS: JSON array of queue names.
local M = {}
function M.decide()
  local names = cjson.decode(ARGS(1))
  table.sort(names, U.blt)
  local out, seen = U.arr(), {}
  for _, q in ipairs(names) do
    if not seen[q] then
      seen[q] = true
      local row = U.jget('queues', q)
      local created = false
      if not row then
        row = { paused = false, revision = 1, created = NOW, updated = NOW }
        U.jset('queues', q, row)
        created = true
      end
      out[#out + 1] = { queue = q, created = created, paused = row.paused, revision = row.revision }
    end
  end
  return U.applied({ queues = out })
end
return U.run(M)
