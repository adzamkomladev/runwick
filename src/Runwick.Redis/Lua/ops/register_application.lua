-- RegisterApplication/1: creates meta from the store gate the client read from rw:store (22 §3.1). ARGS: layout, epoch, mode, digest.
local M = {}
local created = false
function M.seed()
  created = true
  return { store = STORE, layout = ARGS(1), epoch = ARGS(2), mode = ARGS(3), digest = ARGS(4), admission = 'open', reqepoch = '1',
    caps = '{}', gaterev = '1' }
end
function M.decide()
  local m = U.meta
  if created then
    for _, f in ipairs({ 'store', 'layout', 'epoch', 'mode', 'digest', 'admission', 'reqepoch', 'caps', 'gaterev' }) do U.hset('meta', f, m[f]) end
    U.hset('meta', 'created', NOWS)
  end
  return U.applied({ created = created, revision = U.raw(m.gaterev), admissionMode = m.admission })
end
return U.run(M)
