## SQLite: group commit across write kinds

### Why
In the Q6/Q7 runs every SQLite commit is a disk sync (`synchronous=FULL`). On this shared disk a sync sometimes takes up to about 1 s. Before this change only producer calls could share a commit, at most 8 of them. Finishes, claims, renewals, cleanup, presence and outbox writes each paid their own sync, so the queue behind a stall drained one sync at a time. That queue wait showed up as dispatch latency, producer refusals (`local_capacity_exceeded`, 2-7%) and a pickup p99 of 2-5 s against a 2 s target.

### What changed
- New option `SqliteOptions.GroupCommit`, **off by default**. When it is on, the writer thread takes a write together with the writes already queued right behind it, as long as they are the same kind of call (renewal, settlement, admission, producer or background). It runs them in one `BEGIN IMMEDIATE` with one `COMMIT`, so a group pays one disk sync.
- Each write in a group runs under its own savepoint. It reads its own decision time and checks its own gates and receipt. If it is refused, replayed or fails, only its own savepoint is rolled back and only its own caller hears about it.
- No reply goes out before the shared commit. If the commit's result is unknown, every caller gets `commit_outcome_unknown` under its own command ID and can resend it as today. Acknowledgement and replay rules are unchanged.
- Priority: a group never takes a lower kind of call. Before starting each write after the first, the writer checks two things: whether a higher kind is waiting, and whether the group has already spent 25 ms on statements. If either is true, it commits what has run and puts the unstarted writes back in their old queue places. So a renewal waits for at most the write in progress and one commit.
- Limits are 32 writes per group and 1 MiB of prepared dispatch envelopes. That is wider than the producer lane (16 calls in the bench), so a full producer burst fits in one commit. With the option off, the old producer-only grouping (8 calls, one application) stays as it was.
- The old producer group was rebuilt on the same code. It now also uses the early stop and the cancellation rule, and it runs after-commit hints before replying. The old code skipped those hints, but no dispatch path used them.
- Bench flag `--sqlite-group-commit` turns the option on, and the bench JSON records it as `sqlite.groupCommit`. To run any test suite with the option on, set `RUNWICK_SQLITE_GROUP_COMMIT=1`. This covers in-process test hosts only, not Peer processes.

### Why it is opt-in
Grouping changes timing that callers can see. A reply can wait for the other writes in its group, and the commit counter rises more slowly. The full SQLite and Contract suites have not yet been run with the option on. It should become the default only if both suites pass with `RUNWICK_SQLITE_GROUP_COMMIT=1` and the paired benchmark shows a gain.

### Tests
`SqliteGroupCommitTests` (4 tests, passing):
- **Mixed group:** Finish, a faulted Finish and FinishMany share one commit, and only the faulted write is rolled back.
- **Cancellation:** a write cancelled after the group took it from the queue is skipped and not applied, and the others commit.
- **Crash:** a crash image copied mid-group contains the write that was replied before the group and nothing from the unreplied members.
- **Priority:** a renewal that arrives mid-group is answered while the rest of the group is still held.

### Not done
- Neither the full suites nor the benchmark has been run yet.
- The time cap is 25 ms of statements actually spent, measured as the group runs. It is not a per-write estimate made in advance.

### Measured results
(To be filled in by the Sonnet run.)
