-- Finish/1 (04 §7.7). ARGS: 1 body JSON, 2 retry draw, 3 hint: on a resend, the call the command was first sent in ('' otherwise).
local M = {}
function M.hint() return ARGS(3) or '' end
function M.decide() return U.finish_job(cjson.decode(ARGS(1)), tonumber(ARGS(2))) end
return U.run(M)
