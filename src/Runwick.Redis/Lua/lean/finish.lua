-- Finish/1, lean path: a finish by the lease's owner of a job with no concurrency scope (U.finish_job and U.close). ARGS: 1 body JSON,
-- 2 retry draw, 3 hint (the call the command was first sent in; set on a resend).
local m = L.frame()
if not m or (ARGS(3) or '') ~= '' then return GENERIC() end
if #ARGS(1) > 262144 then return GENERIC() end -- the generic size limits (§4.3) decide large bodies
local p = L.finish_plan(cjson.decode(ARGS(1)), tonumber(ARGS(2)))
if not p then return GENERIC() end
local v = L.finish_write(p)
if not L.can_commit(m) then return GENERIC() end
if FP ~= '' then L.receipt(CMD, CLS, OP, FP, NOT_AFTER, FLOOR, L.finish_compact(v)) end
local reply = L.reply(L.finish_text(v))
L.commit(m, reply)
return L.with_payloads(reply, nil, nil)
