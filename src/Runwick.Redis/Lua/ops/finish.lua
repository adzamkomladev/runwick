-- Finish/1 (04 §7.7). ARGS: 1 body JSON, 2 retry draw.
local M = {}
function M.decide() return U.finish_job(cjson.decode(ARGS(1)), tonumber(ARGS(2))) end
return U.run(M)
