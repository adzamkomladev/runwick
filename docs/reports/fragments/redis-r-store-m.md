## Redis Sentinel write authority and the Store page topology (work areas R and M)

Written 2026-11-17. Scope of this change: **Sentinel only**. Redis Cluster routing, declared keys and per-node management are not built. Everything below ran on the shared host, with containers on the host network (one primary, two replicas, three Sentinels per test).

**What it does.** An opt-in mode, `RedisOptions.Sentinel` (default off, nothing changes for a standalone primary), lets a Runwick process follow a Sentinel-managed primary under a stricter rule than "reconnect and hope": the process sends a command only while it holds a verified, unexpired authority, and otherwise sends nothing.

- **Verification** (every second by default, on its own connection to the candidate): a majority of all configured Sentinels name the same primary with health flags exactly `master`; the candidate passes the existing settings check (`appendfsync always` and the rest); if it is a different node from the last verified one, its replication history must descend from the verified one and cover its offset; the existing store-identity and revision check passes. Only then do the work, control and admin connections move to it and a lease of four seconds (default) start.
- **Closing authority.** Evidence closes it at once: a Sentinel reporting `s_down`, `o_down` or a failover, two Sentinels naming different primaries, a different primary named, a lost connection to the bound primary. Silence closes it when the lease ends: a process cut off from every Sentinel stops sending after at most the lease. That is the fence for a former primary that stays reachable from this process.
- **Blocked.** If a promoted node does not continue the verified history, or the store identity or revision went backwards, authority is blocked: every call fails as incompatible (`authority_continuity_unverified`, `store_identity_mismatch`, `history_discontinuity`) until an operator reconciles the histories and restarts the process. Operator intervention is "reconcile and restart"; there is no in-product override.
- **Stopped calls are never sent**, fail as `store_authority_unverified` (not applied) and keep their command identity with the caller. A call in flight when the primary fails has the ordinary outcome unknown.
- Profile `redis-aof-always-sentinel-v1`, acknowledgement profile `redis.aof-always-sentinel-v1`, outbox profile `redis.aof-always-sentinel.v1`. The Sentinel mode needs `appendfsync always`.

**Store page.** The Redis diagnostics card gains "Write authority (Sentinel)": state (verified, unresolved, blocked), reason, primary address, verification epoch, how many Sentinels agree, last verification time and the recent changes this process saw (bounded to 16). The common Topology row now shows a line such as "Sentinel-managed primary 127.0.0.1:36939: authority verified" instead of only a status. For a standalone primary the section is hidden (status Unsupported). These are this process's observations, not a cluster-wide verdict. The committed dashboard bundle was rebuilt.

**Tests** (`RedisSentinelTests`, real containers; one primary, two replicas, three Sentinels).

| Case | Result |
|---|---|
| Host verifies the primary before writing; carries the Sentinel profile and the sentinel acknowledgement profile in its receipt | passes |
| Primary killed after the replicas caught up: writes are refused with `store_authority_unverified` until the promoted node is verified, then resume under a higher epoch; all 30 earlier jobs still resolve Confirmed; the returning old primary becomes a replica and the host keeps the new primary | passes |
| Replicas pointed away from the primary, 20 more writes acknowledged and verified, primary killed, a replica promoted: authority blocks with `authority_continuity_unverified`, a dispatch fails with that reason, nothing reaches the new primary | passes |
| All three Sentinels paused: after the lease no dispatch is sent (the primary's key count does not move); when they return, writes resume on the same primary | passes |
| No reachable Sentinel: startup is refused with `sentinel_majority_unreachable` | passes |
| Configuration: Sentinel strings need endpoints and a service name; standalone still refuses them; `everysec` and an invalid lease are refused | passes (3) |

Full numbers (counts of the whole Redis project and others) are in the final message of this task. An earlier version of the lossy-promotion case paused the replica containers; it did not make them miss writes, because a paused container's socket still receives and buffers the replication stream, so the promoted node had everything and the host (correctly) resumed. The case now sends `REPLICAOF 127.0.0.1 1` to both replicas, which is a real loss.

**What this does not claim, and what is not done.**

- Replication is asynchronous and `WAIT`/`WAITAOF` are not used. A failover can lose writes acknowledged after the last verification, up to one recheck interval plus the lease. The continuity check proves the promoted node had everything this process saw at its last verification, nothing more.
- Fencing is on the client side. A program that is not a Runwick process, or a Runwick process that ignores its lease, can still write to a partitioned former primary.
- Test coverage is the five failure cases above. Not exercised: Sentinel and data-node TLS and ACL failures separately, a Sentinel that disagrees for a long time, failover during a pending intent or in the middle of a claim or finish (the existing intent recovery should resume, but it was not injected here), AOF rewrite during failover, failback of the old primary while a process is mid-command, and many hosts at once.
- The authenticated overhead budget (5%) was not measured. In standalone mode the only added work on a command is a null check. In Sentinel mode it is one lock and a clock read per command, plus one verification a second (two Sentinel commands to each Sentinel and one connection setup to the primary).
- The migrator and the CLI take a plain primary address; they do not discover it through Sentinel.
- **Redis Cluster is not started** (routing, `MOVED`/`ASK`, declared keys, per-node management, slot-aware diagnostics). The R3 recovery proof remains a measurement outside the product code.
- No in-product operator action to certify a promotion; the only intervention is reconcile and restart.
