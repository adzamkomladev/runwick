-- ReadSchedule (13 §6.9) on the read path. ARGS: 1 key (or id), 2 '1' when ARGS(1) is the schedule id.
local g, m = U.read_gate()
if g then
  if g[1] == 'CLOSED' then return { 'OK', g[2] } end
  return g
end
local id = ARGS(2) == '1' and ARGS(1) or U.hget('sched', 'k:' .. ARGS(1))
local row = id and U.hget('sched', id)
if not row then return { 'OK', m.rev or '0' } end
return { 'OK', m.rev or '0', row, U.hget('sched', 'd:' .. id .. ':' .. U.S(cjson.decode(row).drev)) }
