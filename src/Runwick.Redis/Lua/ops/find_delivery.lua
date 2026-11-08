-- FindDelivery (14 §6.3) on the read path, no receipt. ARGS: 1 field <sourceHex>:<messageHex>.
-- Reply: OK rev row('' = none) | CLOSED rev | NONE code | RECOVERING guard.
local g, m = U.read_gate()
if g then return g end
return { 'OK', m.rev or '0', redis.call('HGET', P .. 'outbox', ARGS(1)) or '' }
