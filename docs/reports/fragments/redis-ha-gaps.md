# Redis Sentinel and Cluster: the open gaps (work areas R and M)

Written 2026-11-23. Branch `redis-ha-gaps`, from `main-merge`. No layout, script, receipt or migration change.

## Short answer

- The operator tools now work on Sentinel and Cluster setups. Status, admission, capability activation, re-identification and the migrator take the same `RedisOptions` a host takes. On a cluster they look at every primary. Through Sentinel they find the primary the same way a host does. The CLI has a matching `--redis-topology` flag.
- A test now shows that a `MOVED` reply alone closes write authority. Writing it turned up a small race, which is fixed: a check that was already running when the `MOVED` reply arrived could reopen authority with its older view.
- A wrong password, a missing ACL permission and a TLS mismatch each stop the process with a clear reason before anything is written. A rejected login now reads `primary_authentication_failed` and a missing permission `primary_permission_denied`. Before, both read as "unreachable".
- Making every script declare all its keys is not small, so it was not built. The reasons are below and in spec 22 §2.6.
- The overhead (Cluster and Sentinel against standalone, same drain) is **not measured yet**. The bench now accepts `--redis-topology`; the commands are at the end.

## What changed and why it is safe

1. **Operator commands follow the topology** (`src/Runwick.Redis/RunwickRedisMigrations.cs`, `RedisOps.cs`). There is one connection step for every operator command:
   - **Standalone:** the old path, unchanged. The old method signatures still use it.
   - **Sentinel:** a one-off check runs with the same code a host uses: a majority of the Sentinels must name the same healthy primary, and that primary must pass the settings check. The command then works only on that primary. If the check fails, the command refuses with its reason.
   - **Cluster:** every primary is checked against the settings, and every primary is scanned for `rw:{*}:meta`. If a primary that owns slots cannot be reached, the command refuses with `store_unavailable`. A partial answer would look complete, so refusing is the safe choice.

   Scripts still go by key, so each change lands on the owner of that application's slot. Messages never name a server; a failure says "primary 2 of 3", because the CLI prints the message.
2. **CLI** (`src/Runwick.Cli/CliCommands.cs`). `--redis-topology standalone|sentinel|cluster` works on `migrate`, `doctor` and every `store` command. The default is standalone, so existing commands are unchanged. Using the flag with a store other than Redis is a usage error. `doctor` now checks each primary.
3. **Clearer reasons** (`RxAuthority.Unreachable`). Only the reason text changes. The behaviour is the same: nothing is sent and the next check decides.
4. **A stale check opens nothing** (`RxAuthority`). Each piece of evidence (a `MOVED` reply, a lost connection) raises a counter. A check that started before the latest piece of evidence finishes without granting authority, and the check the evidence queued decides instead. This can only keep authority closed for longer, never open it.
5. **Bench** (`tests/Runwick.Workloads/BenchMode.cs`). New `--redis-topology sentinel|cluster`. The bench flushes every primary it reaches, migrates with the matching options and starts the host in that mode.

## Declared keys: assessed, not built

Today every script lists only `meta` in `KEYS`. Redis refuses a script up front (`ASK`/`TRYAGAIN`) only for the keys it lists. So listing every key would turn "can fail partway while its slot moves" into "is refused before it runs". The catch is that most of the 48 operations cannot know their keys before they run:

- A claim, Recover, ReserveAndBegin, cleanup and schedule catch-up pick their jobs from an index inside the script.
- A finish or cancel reaches the scope, rate, uniqueness and lane keys that are stored on the job.

Listing all keys up front would take a read call followed by a write call that re-checks what the read saw. That means one more round trip on every worker call, plus a retry path for each operation, which is the opposite of the current performance work. The case is already handled in two other ways:

- A slot marked as moving closes authority at the next check.
- A script that fails partway leaves a pending intent, and the new owner finishes it.

What is left is a window of at most one recheck interval, plus the first command after `MOVED`.

## Tests

New class `tests/Runwick.Redis.Tests/RedisHaGapsTests.cs` (REDIS-24), real containers:

| Case | Shows |
|---|---|
| Operator commands on a cluster | Three applications on three primaries: status lists all three; drain, capability and re-identify change all three (the new store ID is checked on each node); one-application admission finds the one on node 3; with node 3 paused, status refuses with `store_unavailable` |
| `MOVED` closes authority | Verifications are held through a test hook, and the slot and its keys are moved in one shell call. The next dispatch gets `MOVED`, and authority closes with `hash_slot_moved` (seen in the recent events). The dispatch after that is refused. Releasing the held check opens nothing. Authority resumes on the new owner, and all five earlier jobs are confirmed |
| Cluster ACL and TLS | One primary's ACL denies `CONFIG`: the cluster migrator refuses (`store_profile_mismatch`, "of 3"), and a host whose slot that node owns does not start, with nothing written to the slot. `ssl=true` against plain nodes: no start (`cluster_majority_unreachable`), nothing written |
| Sentinel discovery, password, TLS, failover | The migrator and status go through the Sentinels. A wrong data-node password is refused as `primary_authentication_failed` by the migrator and at startup, with the primary's key count unchanged. `ssl=true` against plain Sentinels is refused. After the primary is killed, status reaches the promoted node with the same store and application |
| CLI (`CliRedisTests`) | `--redis-topology sentinel` without `serviceName` gives `topology_unsupported`. Cluster and Sentinel strings that nothing answers give `store_unavailable` (exit 4), and doctor reports the store as unavailable |

Results: see the task's final message. The whole CLI Redis class passed, 21 of 21. It also caught a server address in a migration message, which is now fixed.

## Not done

- **Overhead against the 5% budget: not measured.** Commands are below.
- TLS was tested only as a mismatch (TLS asked of plain servers). A server certificate the client rejects was not set up. It takes the same path (the connection fails, so no vote is counted), but it was not exercised.
- The operator commands check the topology once per command and hold no lease. A failover in the middle of a long command is met by Redis itself (`READONLY` on a demoted node), not by Runwick.
- Not injected: a missing permission on a node that does not own the slot (it costs that node's vote only), and a wrong Sentinel password (it reads as `sentinel_majority_unreachable`).

## Commands for the follow-up runs

```bash
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
cd /root/work/Spindle   # on branch redis-ha-gaps
dotnet build Runwick.slnx -c Release -v q
dotnet test tests/Runwick.Redis.Tests -c Release --no-build
RUNWICK_TEST_REDIS=127.0.0.1:<throwaway always-profile redis> dotnet test tests/Runwick.Contract.Tests -c Release --no-build   # count the Redis-kind rows
dotnet test tests/Runwick.Cli.Tests -c Release --no-build
dotnet test tests/Runwick.Dashboard.Tests -c Release --no-build
```

Overhead (same drain, three topologies, under `flock /tmp/runwick-bench.lock`; see the task's final message for the container setup):

```bash
W=tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll
flock /tmp/runwick-bench.lock dotnet $W bench --store redis --redis 127.0.0.1:<standalone> --mode drain --n 20000 --workers 16 --calls 8 --out sa.json
flock /tmp/runwick-bench.lock dotnet $W bench --store redis --redis-topology sentinel --redis "127.0.0.1:<s1>,127.0.0.1:<s2>,127.0.0.1:<s3>,serviceName=mymaster" --mode drain --n 20000 --workers 16 --calls 8 --out sen.json
flock /tmp/runwick-bench.lock dotnet $W bench --store redis --redis-topology cluster --redis "127.0.0.1:<p1>,127.0.0.1:<p2>,127.0.0.1:<p3>" --mode drain --n 20000 --workers 16 --calls 8 --out cl.json
```
