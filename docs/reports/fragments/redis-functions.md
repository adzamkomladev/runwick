## Redis: the worker commands as a Redis Functions library (perf round two, Redis F)

### Short answer

- **Done.** Claim, Finish, FinishMany, Renew and RenewMany (the operations with a lean path) are now also one Redis Functions library. The client loads it once per server and calls it with `FCALL`. The lean frame's helper functions are defined once, when the library loads, not on every call.
- **Nothing stored, replied or replayed changed.** `diffcheck.py` found 0 differences in 74 keys between this branch called by `FCALL` and redis-next called by `EVALSHA`. No migration, no layout change.
- **The gain is small: about 6 to 7% of the in-script time, and less at one job per call than the earlier profile suggested.** A claim of 1 job went from about 540 to about 504 µs of Lua time; claims and finishes of 8 went from 176 to 166 and from 160 to 151 µs per job. The earlier estimate of "310 to 420 µs fixed cost per call" was mostly not the helper definitions. It is the real work of the first commands in each call, which runs with cold caches on this loaded host, and a library cannot change that.
- **EVALSHA is still there** as the fallback when a server refuses `FUNCTION` or `FCALL` (for example an ACL), and Describe says so.
- FinishAndClaim and Dispatch were **not** put in the library. They have no lean path, so their generic frame would still define its 250 helpers on every call inside a function. Getting a gain there means rewriting the generic frame (`prelude.lua`, `jobs.lua`) to set its per-call state in one place. That is a larger change.

### What changed

**The library.** `RxScript.Library` (src/Runwick.Redis/Store/RxClient.cs) builds one text from the files that already exist:

- each lean operation's generic frame (prelude, jobs.lua, the operation body) as a function, `G['finish']` and so on, defined first so its names resolve exactly as in the script;
- `lean/frame.lua`, the lean helpers;
- for each operation a registered function, `rw_<hash>_<op>`, that sets `KEYS` and `ARGV` from its arguments and runs the lean body. That body calls the same `GENERIC()` as before when it cannot decide the command itself.

The library is named `rw_` plus the first 16 hex digits of the SHA-1 of its text. A binary with different scripts gets a different name, so an old and a new binary can run side by side on one server. The client never uses `FUNCTION LOAD REPLACE`, so one binary cannot replace another binary's code. (The original 2026-11-08 reason for not using Functions was exactly that risk. A name made from the content removes it.)

**One change inside `lean/frame.lua`.** The frame used to read `KEYS`, `ARGV` and the server time at the top of the script. In a library that top part runs once per server, so these per-call values now get set in `L.frame()`, the first step of every lean body. The after-image records, the event rows, the quoting cache and the write count are reset there too. It reads the same values in the same order: nothing before `L.frame()` ever ran a Redis command. A Functions library also cannot read globals such as `string` while it loads, so the frame now reads `string.format` and its neighbours inside `L.frame()` as well (`L.libs()`). The same file still serves the `EVALSHA` scripts unchanged in form.

**Loading.**

- The profile check (startup, every recheck, and every primary that the Sentinel or Cluster authority verifies) lists the library and sends `FUNCTION LOAD` only if the server does not have it. This covers a failover: the new primary is checked before any command goes to it. In Cluster mode the check runs on the slot's owner, which is the primary this application uses.
- A call answered `ERR Function not found` did not run. The client loads the library on the node that owns the key and resends once. This covers `FUNCTION FLUSH` and a server that lost it. If the resend is answered "not found" again, the call ends as not applied (`store_unavailable`). It never ends as "outcome unknown".
- **Fallback.** If the server refuses `FUNCTION LOAD` (ACL, renamed command, or a library it will not compile), or refuses `FCALL` itself (`NOPERM` for `fcall`, unknown command), nothing ran. The client switches to `EVALSHA` of the same scripts for the rest of the process. Describe's new `functions` fact gives the library name, or `evalsha:` followed by the server's refusal. This fact is not part of the profile verdict.

**AOF and replicas.** Redis stores loaded libraries in the AOF and the RDB and sends them to replicas. The tests check both on throwaway servers. With `appendfsync always`, the library survives a `kill -9` and restart. A new replica has it after its first sync.

**Crash rule.** The lean commit, which stores the intent when a write fails partway, is the same code in both forms. LS-03 runs every write prefix of every lean command, and these calls now go through `FCALL`.

### Proof that nothing changed

`diffcheck.py` gained `--functions`, which runs the new side's lean operations by `FCALL` from the library, composed by `luascript.library` exactly as the binary composes it. The library name the Python tool computes matches the one the binary loaded.

- This branch by `FCALL` against redis-next by `EVALSHA`: 74 keys, 0 differences (keys, replies, replays and the events feed).
- This branch by `EVALSHA` against redis-next: 0 differences.
- This branch by `FCALL` against redis-next's generic path: 0 differences.

REDIS-11 holds: no text comparison was added.

### Before and after

`profile_worker.py` (Lua CPU time inside the script, median of 60 calls, three rounds rotating the order, a throwaway Redis 8.2 with `appendfsync always`, host load average 46 to 56, under `/tmp/runwick-bench.lock`). The new `--functions` mode calls the library by `FCALL` and times the lean body inside each function. The `EVALSHA` modes time the whole script, which includes defining the helpers. The difference between the two is what the library saves. In-script µs per job, median of the three rounds:

| | Claim, 1 per call | Finish, 1 per call | Claim, 8 per call | Finish, 8 per call |
|---|---:|---:|---:|---:|
| redis-next, EVALSHA | 540 | 472 | 176 | 160 |
| this branch, FCALL | 504 | 470 | 166 | 151 |
| this branch, EVALSHA fallback | 552 | 471 | 173 | 161 |

Round by round, FCALL was lower than redis-next in 8 of the 9 rounds of the two claims and the 8-job finish. The single-job finish is inside the noise (434 to 490 µs between rounds). Redis's own time per call (`INFO commandstats`, which includes work outside Lua) moves too much on this host to read a difference from three rounds, though it points the same way. The fallback costs the same as before.

What the numbers say: the library saves about 35 to 40 µs per call. That matches the "script start and helpers" step of the earlier lean-path profile (about 50 µs). The rest of the fixed per-call cost is the work itself. Functions do not reduce it.

The library takes about 0.9 MB of Lua memory on the server (`used_memory_vm_functions`), because it holds four copies of the generic frame, one per operation.

### Tests

New class `RedisFunctionsTests` (REDIS-24), each case on its own throwaway Redis 8.2 container:

- the profile check loads the library before any call, Describe names it, and claim and finish go by `FCALL` with no failed calls;
- after `FUNCTION FLUSH` the next claim is answered "not found" once, the library is loaded again, and the call is applied;
- a user whose ACL denies `FUNCTION`, `FCALL` and `FCALL_RO` runs claims and finishes by `EVALSHA`, and Describe reports the refusal;
- `kill -9` and restart from the AOF keep the library, a new replica receives it in its first sync, and a new client's calls after the restart have no failed `FCALL`.

`RedisLeanScriptTests` (LS-01 to LS-03) still pass. LS-01 now calls `L.libs()` itself, because it uses the frame's helpers without a call frame. Run together against a throwaway Redis 8.2 (`appendfsync always`): 7 of 7 passed. The full Redis, Core and Contract suites and the drain pairs were not run here (commands in the hand-off).

### What is not done

- **FinishAndClaim and Dispatch are not in the library** (see the short answer). Putting them in as they are would cost a larger library and save nothing.
- **No Sentinel or Cluster failover test.** A promoted replica already has the library (the replica test). A primary that lacks it is handled by the same "not found, load, resend" step that `FUNCTION FLUSH` tests, and the authority's profile check also loads it on every newly verified primary. The Sentinel and Cluster suites (REDIS-17, REDIS-18) run through this code, but nothing in them checks it specifically.
- **The fallback is for the life of the process.** If an ACL that refused `FCALL` is later relaxed, the process keeps using `EVALSHA` until it restarts.
- **The 90% of BullMQ target is still not met.** This change is about 6% of the Lua time of a claimed and finished job.
