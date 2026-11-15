# G2 and G3 design notes (2026-11-15)

Both modes change SQL or Lua in all three stores, so they are built after the grouped-admission and outbox branches merge. This note fixes what each mode means and how it is tested, so the build can start from it.

## G3: lean history (per-queue, off by default)

**What it does.** A queue can be registered with `History = Lean`. For jobs on that queue the store keeps the job row, its generations and its attempts, and writes no rows to `queue_events` (PostgreSQL, SQLite) or the events keys (Redis).

**What stays the same.** Command receipts, uniqueness keys, leases, fences, retries, rates, retention of jobs and attempts, and the job detail page's state, attempts and errors. Audit rows for management actions stay, because they record who did what, not what happened to a job.

**What is lost.** The per-job timeline (dispatched, reserved, started, finished and the rest) and the queue's event stream (`EventsSince`) for jobs of that queue. The dashboard shows "history is lean for this queue" in place of the timeline.

**Where the switch lives.** A `history_mode` column on the queue registration, read in the same statement that already reads the queue row, so the write path pays no extra round trip. Redis stores it in the queue hash and `U.event` returns early. Changing the mode applies to events written after the change; existing events are kept until retention removes them.

**Order of work.**
1. Measure the upper bound first (`--pg-no-events` in the bench discards event inserts by rule). Build only if the paired drain gain is worth the change.
2. Add the registration option, the column and the migration in each store.
3. Guard every event insert: about 20 sites in the PostgreSQL functions, four in SQLite, one helper in Redis.
4. Dashboard and event API: show a clear marker instead of an empty timeline, and keep `EventsSince` working (it returns nothing for lean queues).
5. Tests: a lean queue produces no event rows through dispatch, claim, finish, retry, recover and cancel; a normal queue on the same application is unchanged; receipt replay still returns the original results; both modes in the contract suite.

## G2: reduced replay protection (defer)

**Decision.** Do not build a mode that skips receipts. The plan says a lighter mode needs an explicit deduplication horizon and enough historical finish identity to survive retries and reruns, and the measured benefit is unknown. Receipts are about a quarter of logical writes, but the finish receipt is what lets a lost reply be answered with the original result.

**What would be tried instead, in this order.**
1. Measure the receipt share of drain cost with the same kind of upper-bound probe as G3 (receipt inserts discarded). If the gain is small, close G2 as not worth the loss of idempotency.
2. If the gain is large, test a shorter horizon: keep the receipt for the replay window, but drop it earlier for jobs that reached a terminal state and whose finish identity is held in the job row itself (command ID, outcome and decision time on the generation row). A resend inside the horizon is answered from that row; outside it, a resend may create another job, which the docs must say in plain words.
3. Tests would cover lost replies inside and outside the horizon, terminal jobs, retry and rerun, and cleanup.

G2 is documented as deferred unless step 1 shows a gain that justifies step 2.
