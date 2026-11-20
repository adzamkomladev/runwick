-- FinishMany/1, lean path: every item a first send (no receipt yet, deadline not passed and within a minute) and a finish by the
-- lease's owner of a job with no concurrency scope (U.finish_items with U.finish_job). The items' receipts are one key per call,
-- rcpt:<call>:f (22 §3.2). ARGS: 1 n, then per item: command id, fingerprint, NotAfter ms, body JSON, retry draw, hint.
local m = L.frame()
if not m then return GENERIC() end
local n = tonumber(ARGS(1))
if not n or n < 1 or n > 64 then return GENERIC() end
local plans, cmds, jobs, bytes = {}, {}, {}, 0
for i = 1, n do
  local b = 2 + (i - 1) * 6
  local cmd, fp, na, body = ARGS(b), ARGS(b + 1), tonumber(ARGS(b + 2)), ARGS(b + 3)
  bytes = bytes + #body
  -- A hint marks a resend: the generic path looks its receipt up and keeps writing it in the per-item form.
  if cmds[cmd] or not na or NOW >= na or na - NOW > 60000 or bytes > 262144 or ARGS(b + 5) ~= ''
    or redis.call('HEXISTS', P .. 'rcpt:' .. cmd, 'cls') == 1 then
    return GENERIC()
  end
  cmds[cmd] = true
  local p = L.finish_plan(cjson.decode(body), tonumber(ARGS(b + 4)))
  if not p or jobs[p.j.id] then return GENERIC() end
  jobs[p.j.id] = true
  p.cmd, p.fp, p.na = cmd, fp, na
  plans[i] = p
end
local items, retain = {}, 0
for i, p in ipairs(plans) do
  local v = L.finish_write(p)
  local text = L.finish_text(v)
  L.hset('rcpt:' .. CMD .. ':f', p.cmd, '1|' .. p.fp .. '|' .. sformat('%d', p.na) .. '|' .. NOWS .. '|' .. L.finish_compact(v))
  if p.na > retain then retain = p.na end
  items[i] = '{"code":null,"commandId":' .. q(p.cmd) .. ',"decidedAt":' .. NOWS .. ',"effect":"Applied","replayed":false,"result":' .. text
    .. ',"status":"Applied"}'
end
L.zadd('rcpt:exp:durable', sformat('%d', retain + 86400000), CMD .. ':f')
if not L.can_commit(m) then return GENERIC() end
local reply = L.reply('{"items":[' .. concat(items, ',') .. ']}')
L.commit(m, reply)
return L.with_payloads(reply, nil, nil)
