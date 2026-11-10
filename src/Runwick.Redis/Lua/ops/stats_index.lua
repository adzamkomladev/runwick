-- StatsIndex (16 §2.4, §2.6; 22 §7.2): no receipt, no manifest entry (it changes no protocol). Builds idx:terminal and the
-- jobtypes counts for jobs written before this binary kept them, walking idx:created by (created, id) from meta.bf and
-- storing the cursor; 'done' when the walk reaches the end. Applications registered by this binary start 'done'. ARGS: 1 rows.
local M = {}
function M.decide()
  local bf = U.meta.bf
  if bf == 'done' then return U.applied({ done = true }) end
  local from, after
  if bf then
    local s, k = string.match(bf, '^(%d+):(.*)$')
    from, after = tonumber(s), k
  end
  local rows, n, ca, ck = tonumber(ARGS(1)), 0, from, after
  local offset = 0
  while n < rows do
    local page = redis.call('ZRANGE', P .. 'idx:created', from and U.S(from) or '-inf', '+inf', 'BYSCORE', 'LIMIT', offset, 100, 'WITHSCORES')
    if #page == 0 then
      U.hset('meta', 'bf', 'done')
      return U.applied({ done = true })
    end
    offset = offset + #page / 2
    for i = 1, #page, 2 do
      local id, score = page[i], tonumber(page[i + 1])
      if not (from and score == from and not U.blt(after, id)) then
        local v = redis.call('HMGET', P .. 'job:' .. id, 'queue', 'name', 'state', 'terminalat')
        if v[1] then
          U.jobtype(v[1], v[2], 1)
          if U.TERMINAL[v[3]] and v[4] then U.zadd('idx:terminal', tonumber(v[4]), id) end
        end
        n, ca, ck = n + 1, score, id
        if n >= rows then break end
      end
    end
  end
  U.hset('meta', 'bf', U.S(ca) .. ':' .. ck)
  return U.applied({ done = false })
end
return U.run(M)
