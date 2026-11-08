-- RecordScalingEvent (11 §6): no receipt. ARGS: 1 event id hex, 2 row JSON object without createdAt (built by the client).
-- The row is stored as sent with createdAt (decision time) added in front; the scaling index is scored by createdAt.
local M = {}
function M.decide()
  local id, row = ARGS(1), ARGS(2)
  U.zadd('scaling', NOW, id)
  U.hset('scaling:data', id, '{"createdAt":' .. NOWS .. ',' .. string.sub(row, 2))
  return U.applied({ eventId = id })
end
return U.run(M)
