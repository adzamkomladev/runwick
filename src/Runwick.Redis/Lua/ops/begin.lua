-- Begin/1 (04 §7.5, 10 §4). ARGS: 1 lease JSON, 2 jitter ms.
local M = {}
function M.decide() return U.begin_job(U.lease_of(cjson.decode(ARGS(1))), tonumber(ARGS(2))) end
return U.run(M)
