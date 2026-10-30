# Execution defaults and timing relationships

Status: concrete D-05 recommendation, 2026-10-29. Extends the [worker](worker-engine-internals.md), [storage protocol](storage-command-protocol.md) and [configuration contract](public-api-contract.md). Values below are proposed initial defaults for implementation and fault/load testing, not user-approved settings or measured production limits. No runtime tests ran.

## 1. Different clocks and different budgets

Separate five concerns: when a job is eligible; how long an attempt has storage authority; how long its pipeline may execute; how long the worker tries to resolve a storage outcome; and how long the host allows shutdown. None is a substitute for another.

Storage decides eligibility, lease validity, retry timestamps, command deadlines and receipt retention. The worker uses monotonic elapsed time for pipeline durations, renewal scheduling and local budgets. TimeProvider timestamp/elapsed APIs support that separation and deterministic engine fixtures. Wall-clock agreement between worker and store is not required for measuring elapsed execution; producer command deadlines can still be rejected under skew. [TimeProvider elapsed time](https://learn.microsoft.com/en-us/dotnet/api/system.timeprovider.getelapsedtime?view=net-10.0)

All persisted durations use the common millisecond quantum and checked arithmetic. Equality with a lease/command expiry means expired; a pipeline finishing at or after its execution-timeout boundary is timed out. Timer delivery may be delayed, so classify using the recorded completion timestamp as well as timer signals.

## 2. Proposed initial values

### Generation policy: persisted at dispatch

| Setting | Proposed default | Meaning |
|---|---|---|
| MaxExecutions | 3 | Includes the first committed Begin; at most two automatic retries |
| ExecutionTimeout | None | No universal handler deadline; jobs/applications may configure a positive timeout |
| Retry evaluator | exponential-v1 with equal jitter | Initial nominal delay 5 s, maximum nominal delay 60 s |
| MaxConsecutiveUnstartedAbandonments | 5 | Fifth consecutive recovered pre-Begin abandonment terminates the generation |
| Unstarted recovery delay | exponential-v1 with equal jitter | Initial nominal delay 1 s, cap 30 s; independent of execution retry budget |

No default pipeline timeout avoids automatically repeating legitimate long-running jobs just because a generic library deadline elapsed. A missing timeout deliberately permits a job to run indefinitely while authority is renewed; operators can request cancellation and applications can set a job-specific bound. This is not a stuck-job detector or a wall-clock completion guarantee.

### Worker/control policy: validated for each host

| Setting | Proposed default | Meaning |
|---|---|---|
| LeaseDuration | 60 s | Requested lifetime from a fresh storage decision; not maximum execution duration |
| LeaseSafetyMargin | 10 s | Conservative subtraction from local authority estimate |
| RenewalInterval | 15 s | Per-attempt jitter in [12 s, 18 s], scheduled relative to the last fresh request's send time |
| LeaseWatchdogInterval | At most 1 s | Local check cadence; delayed scheduling never extends the deadline |
| SchedulerAllowance | 2 s | Validation allowance for local scheduling, not a guaranteed OS pause bound |
| ControlRequestTimeout | 5 s | End-to-end local wait budget including I/O admission; expiration does not prove rollback |
| Control retry backoff | Nominal 100 ms exponential, cap 1 s, equal jitter | Storage retry/resolution only; preserve command identity |
| MinimumInvocationMargin | 15 s | Minimum remaining confirmed local authority before invocation admission |
| ClaimResolutionBudget | 15 s | From first Reserve submission; unresolved expiry suspends pool admission |
| BeginResolutionBudget | 15 s | From first Begin submission; no invocation on an unresolved result |
| CompletionResolutionBudget | 30 s | From publication of frozen Finish after cleanup; bounded by authority and shutdown |
| CancellationGrace | 10 s | From first local user/timeout/shutdown cancellation request until pipeline exit |
| CleanupBudget | 10 s | From starting scope disposal after pipeline exit |
| OwnershipCommandLifetime | 30 s | Frozen NotAfter offset for new Reserve/Begin/Renew/Finish/Recover commands; store time still validates it |
| RecoveryScanInterval | Nominal 5 s, jitter [4 s, 6 s] | Bounded expiry discovery; does not grant/revoke ownership itself |
| ShutdownBudget | 25 s | Planned maximum Spindle shutdown work, subject to the host's earlier cancellation |
| ShutdownDrain / cancellation / finalization slices | 15 s / 5 s / 5 s | Absolute phase cutoffs, not fresh per-job budgets |

The 30-second ownership command lifetime does not permit spending 30 seconds on a renewal network call: each request is bounded by the shorter request timeout, resolution budget and safety deadline. Begin's result retains its original lease; it does not silently grant another full lease. These settings are internal worker policies, not per-dispatch options.

Worker-command receipt proposal: keep Reserve/Begin/Recover receipts until at least NotAfter + 10 minutes, and Finish receipts until at least NotAfter + 24 hours for diagnosis/recovery. Accepted 2026-10-30 (R1): Renew carries no replayable receipt (see the [storage protocol](storage-command-protocol.md#4-lost-responses-and-replay)); a lost Renew reply is resolved by a fresh ownership-checked Renew, not by receipt lookup. The per-attempt arbiter still serializes a new Renew behind any unresolved Begin/Finish. Never prune a pending publication/recovery record. Expired replay cannot newly apply even after cleanup. Dispatch/admin/outbox retention is a separate contract; these worker windows do not shorten business deduplication. Measure renewal receipt growth explicitly before freezing retention defaults.

## 3. Renewal safety arithmetic

For a fresh, non-replayed grant, record local monotonic send time s and receive time t. Let r = ExpiresAt - DecidedAt from the storage reply. Set local authority deadline d = s + r - safetyMargin. At receive, usable margin is d - t. The send-time anchor subtracts the whole round trip conservatively; a replayed historical result cannot establish a new deadline.

Normal fresh Renew with r=60 s and safety margin 10 s yields d=s+50 s. If the response arrives 4 s later, only 46 s remains. Renewal due time is calculated from s, not response arrival: at most s+18 s under default jitter. Also cap it at d minus two request timeouts, maximum retry backoff and scheduler allowance; shorter remaining grants cannot use the full interval. If the response arrives after that due time, enqueue renewal promptly rather than sleeping another interval, or latch loss if the safety deadline passed.

Startup validates at least:

```text
0 < safetyMargin < leaseDuration
renewalIntervalMax + 2*controlRequestTimeout + retryBackoffMax
    + schedulerAllowance < leaseDuration - safetyMargin
minimumInvocationMargin >= 2*controlRequestTimeout
    + retryBackoffMax + schedulerAllowance
shutdownDrain + shutdownCancellation + shutdownFinalization <= shutdownBudget
```

Default renewal check: 18 + 10 + 1 + 2 = 31 s < 50 s. Default invocation margin: 15 s >= 13 s. This reserves nominal room for two bounded request attempts; it does not prove a transaction ended, survive arbitrary thread-pool starvation, or prevent a store clock jump. Runtime remaining-margin checks always override the static arithmetic.

Validate positive finite durations, jitter ranges, overflow and lease support at startup. Reject contradictory settings rather than quietly increasing the lease or reducing safety. A host may use longer intervals/leases together, with the same relationships. Long pipeline timeouts, cleanup or resolution budgets need not fit inside one lease: successful renewal may extend authority repeatedly. Actual loss or local budget expiry still wins.

Serialize Begin/Renew/Finish and their ambiguity resolution through the per-attempt arbiter. Start no competing lifecycle mutation while its prior result is unresolved. Receipt lookup/replay follows that original command; a short caller timeout is not permission to create a replacement command. Watchdog checks run independently of the arbiter and can latch loss while I/O is stuck.

After an uncertain Reserve/Begin is resolved, require a fresh ownership-checked renewal and adequate invocation margin before running user code. Check the latch/deadline again when the runner starts: if authority was lost while queued on the thread pool, skip decoding/DI/handler invocation. The already-committed Begin remains counted. Never revive a locally lost attempt from a late reply.

Renewal/finalization need reserved I/O capacity. SP-009 must verify actual outstanding lease counts against connection/executor capacity and tail latency, not merely configure a timer. The [D-07 defaults/budgets](capacity-defaults-and-budgets.md) now proposes worker targets, control-I/O partitions and group-wide renewal-wave checks. The per-attempt arithmetic above does not prove every lease can obtain two attempts under group-wide timeout saturation.

## 4. Execution and outcome classification

Start the pipeline clock at local invocation admission, before thread-pool queuing. Include queue delay, decoding/upgrades, DI construction and middleware/handler work. End it once the complete awaited pipeline ends. Exclude earlier Begin resolution, later async disposal and Finish acknowledgement.

If a configured timeout elapsed before the scheduled runner starts, do not invoke the handler. Finalize TimedOut without a scope if still owner. If timeout arrives during execution, request cancellation and start CancellationGrace. Timer callbacks cannot force termination or safely dispose an active handler's scope.

| Observation | Proposed outcome policy |
|---|---|
| Timely normal return or explicit middleware success | Succeeded; current durable cancellation may coexist in history |
| Explicit permanent result; deterministic supported payload/upgrade failure; pipeline misuse | PermanentFailure |
| Explicit retry or ordinary unclassified handler exception | RetryableFailure, bounded by MaxExecutions |
| Infrastructure failure before a confirmed Begin | Storage retry/resolve or ownership recovery; no fabricated handler invocation |
| Pipeline duration >= configured timeout | TimedOut, even if it returns normally after cancellation |
| Shutdown cancellation causes unsuccessful exit | Interrupted unless timeout/durable user cancellation determines final state |
| Explicit middleware cancellation | UserCancelled |
| OperationCanceledException without an engine cause | Exception classifier; not automatically durable user cancellation |
| Lease loss/abandonment latch | No new Finish; resolve an already-submitted Finish if needed |

An ordinary handler failure is retryable by default; applications can classify permanent business failures explicitly. Do not classify every ArgumentException as permanent across all user code. Process-fatal failures may never reach Finish and are handled by recovery. Preserve the observed error/cause and storage's applied outcome separately.

Storage retains the existing precedence under its locks: timely success can win a concurrent user cancellation; unsuccessful execution with a durable cancellation becomes cancelled; otherwise permanent failure terminates and retryable/timeout/interrupted outcomes retry only when budget remains. No timeout, local callback or management response independently asserts that physical execution stopped.

Cleanup failure after timely success is diagnostic and does not itself request another execution. Cleanup must be complete before a new Finish is issued under the current engine design. If disposal hangs past CleanupBudget, stop renewing, latch local abandonment and retain the ticket/task; recovery may later repeat the job. Report this explicitly as unconfirmed completion, not safe success or a cleanup-induced retry command.

## 5. Retry timing and accounting

Let n be the committed execution number of the failed attempt within its generation, starting at 1. For automatic retry compute nominal milliseconds b = min(maxDelay, initialDelay * 2^(n-1)), using saturating arithmetic. Equal jitter samples an integer delay uniformly from ceil(b/2) through b, inclusive. Persist the chosen delay/eligible instant with the atomic outcome and receipt. A replay returns that choice; it never redraws it.

With three executions and a 5-second initial delay, retry after execution 1 waits 2.5–5 seconds; retry after execution 2 waits 5–10 seconds. Execution 3 is terminal on retryable failure. Eligibility starts at the Finish/Recover storage decision plus the chosen delay, not at client receipt. Capacity, pause and scheduling can add more waiting.

A trusted middleware RequestedRetryDelay replaces exponential calculation for that outcome: reject negatives as a middleware contract fault, round upward to the time quantum, clamp zero to 1 ms and cap at MaximumDelay. Do not add jitter to an explicit requested delay. The execution is still charged and the same maximum execution count applies.

Unstarted recovery uses its own counter k: after increment, if k >= 5, fail with reservation_abandonment_exhausted; otherwise use initial 1 s, cap 30 s and the same formula/jitter based on k. Successful Begin resets k. Rate deferral does not increment k. Graceful shutdown of a reserved-not-begun job uses ReleaseReservation (accepted 2026-10-30, R2), which charges neither budget; only an unconfirmed release falls back to expiry recovery and the abandonment count.

Running crash recovery uses the already-consumed execution number, applies remaining execution budget and schedules from its recovery decision. It does not charge another execution for discovering the crash. Durable cancellation takes precedence for unsuccessful recovery. Manual retry creates a new generation with copied effective policies and zero generation counters; lifetime history is unchanged.

The retry evaluator takes an explicit sampled entropy input; its calculation is deterministic for that input. Choose/store the outcome in the transaction or Redis recoverable intent, before publication. Rolled-back calculations may be discarded; published outcomes and replayed recovery plans must retain their chosen values.

## 6. Cancellation grace and bounded abandonment

Record cancellation causes individually. Start the grace clock at the first locally observed user/timeout/shutdown cancellation request; repeated signals do not reset it. A durable user request is normally observed on renewal, with optional signals as hints. Therefore the grace period is not a promise that cancellation completes within 10 seconds of an operator clicking Cancel.

Use asynchronous cancellation dispatch and observe its task/errors without blocking renewal scheduling. CancelAsync transitions the token's cancellation state and schedules callbacks asynchronously; its completion does not prove arbitrary handler work has ended. Keep tracking the handler task separately. [.NET cancellation API](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtokensource.cancelasync?view=net-10.0)

If the pipeline exits within grace, perform bounded cleanup and finalization under the remaining authority. If it has not exited when grace expires, stop issuing renewals and latch local abandonment. Do not release permits manually, issue Finish for active code, dispose its scope concurrently or free its local ticket. An already-in-flight renewal may have extended the lease; recovery waits for the actual persisted expiry.

The old task can continue occupying its local ticket while another process eventually recovers the job. For timeouts that may mean a retry; for durable user cancellation it normally means logical cancellation. Neither proves the old task's external effects stopped. The design bounds replacement tasks in that process and ongoing renewal work, not the lifetime of arbitrary in-process code.

Lease-safety loss can happen before grace ends; latch it immediately and stop all new owner mutations. A Finish already sent may still have committed and must remain resolvable. Never reinterpret a successful receipt as failure solely because the local deadline elapsed afterward.

## 7. Budget interactions and shutdown

Claim and Begin resolution have separate clocks. They may run sequentially, but each operation remains constrained by current authority; resolved historical results do not create fresh time. Expired unresolved Reserve suspends the pool. Expired unresolved Begin prevents invocation and stops renewal/abandons locally; persistent command uncertainty retains tracking and suspends further pool admission as in the worker design.

CompletionResolutionBudget begins after cleanup when the frozen Finish is published, including any wait in the bounded completion coordinator. It can span fresh renewals between resolved operations, but a single unknown Finish blocks competing renewal until resolved. At budget/safety expiry, stop renewal and retain ambiguity; never rerun the pipeline to obtain another result.

At shutdown use absolute cutoffs measured from the first locally observed stop signal; admission closes immediately. Do not restart those clocks when StopAsync is later invoked or for each attempt:

| Interval under the 25-second proposal | Behavior |
|---|---|
| 0–15 s | Drain already-begun work, resolve claims, renew and finalize; do not start unbegun reservations |
| 15–20 s | Request cancellation of remaining pipelines; allow cooperative exit/cleanup while renewing within authority |
| 20–25 s | Reserve remaining time for completed cleanup/finalization and pending receipt resolution; continue observing user work, but no new execution |
| At 25 s or earlier host cancellation | Stop control I/O/renewal and return boundedly; report unresolved work; leave recovery to durable lease rules |

The finalization slice is not an instruction to dispose live code or ignore a finished handler. A task that completes during it can still clean up/finalize if time remains. All ordinary grace/cleanup/resolution budgets are shortened by the applicable absolute shutdown boundary. Host deadline cancellation overrides every phase, including a token already cancelled before Spindle's StopAsync starts.

Inspected .NET 10 HostOptions defaults ShutdownTimeout to 30 seconds. Host.StopAsync creates a shared timeout token before stopping services, which are sequential by default; Spindle may receive less than the full interval. The 25-second proposal leaves nominal room but cannot reserve it against other services. Do not overwrite host options automatically. Validate configured budgets and document the deployment termination grace; honor the actual token even when static settings appear sufficient. [HostOptions source](https://raw.githubusercontent.com/dotnet/runtime/v10.0.0/src/libraries/Microsoft.Extensions.Hosting/src/HostOptions.cs), [Host.StopAsync source](https://raw.githubusercontent.com/dotnet/runtime/v10.0.0/src/libraries/Microsoft.Extensions.Hosting/src/Internal/Host.cs)

The host can dispose its service provider after stopping while noncooperative tasks remain. Spindle cannot guarantee DI availability beyond host lifetime or provide a safe in-process hard kill. Tests/runbooks must distinguish orderly drain from abrupt process termination.

## 8. Required contract additions

- Replace the ambiguous retry UseJitter boolean with evaluator name/version and explicit jitter kind. Add a separate unstarted-abandonment delay policy to the generation snapshot and dispatch fingerprint.
- Add BeginResolutionBudget and CancellationGrace to validated worker policy, alongside the existing claim/completion/cleanup budgets. Worker timing changes do not rewrite previously persisted generation policy.
- Persist chosen recovery retry timing and distinguish observed pipeline outcome from unconfirmed durable outcome. Lease-loss/abandonment latches are local authority state, not arbitrary new terminal job states.
- Add operation-class receipt-retention policy and prevent pruning unresolved publication/recovery state. D-06/D-13 must reconcile schema, command versions and mixed-worker validation.
- Update wire vectors for the new explicit policy fields. Older missing fields need an explicit versioned interpretation or rejection, never accidental current-default substitution.

## 9. Acceptance cases

| ID | Required result |
|---|---|
| TIME-01 | Invalid renewal/jitter/I/O-margin combinations fail startup; default arithmetic passes without implying measured capacity |
| TIME-02 | Delayed fresh reply subtracts full RTT; replay never refreshes deadline; delayed response cannot revive lost authority |
| TIME-03 | Authority or pipeline deadline expires during thread-pool queueing: no handler invocation, correct Begin accounting |
| TIME-04 | Completion exactly at timeout, delayed timer callback and slow Finish classify by pipeline duration, not acknowledgement latency |
| TIME-05 | Retry indices, jitter bounds, cap/overflow and explicit-delay rules are exact; response loss preserves chosen delay |
| TIME-06 | Unstarted recovery fails on fifth consecutive abandonment; Begin resets count; rate deferral charges neither budget |
| TIME-07 | Ignored cancellation stops renewal after grace, retains ticket/scope and never force-releases permits; late renewal does not revive local work |
| TIME-08 | Slow/throwing cancellation callback and hung disposal cannot block unrelated leases or turn success into a fabricated retry |
| TIME-09 | Unknown Reserve/Begin/Finish respects separate budgets, preserves command identity and bounds/suspends admission |
| TIME-10 | No-timeout long job can renew across many leases; a configured short timeout does not inherit lease duration |
| TIME-11 | Shared host token arrives before/inside each shutdown phase; no new budget starts and no cancelled host token is reused as a normal Finish token |
| TIME-12 | Finish receipt after local loss resolves historical success; receipt expiry yields explicit unknown without reapplication |
| TIME-13 | Busy I/O pool, scope contention, store clock jumps and thread-pool stalls test runtime safety beyond static inequalities |

Run deterministic timing fixtures plus real-store fault/process tests; fake time alone cannot prove database expiry or cancellation of a transaction. No test is reported as passing. The [PostgreSQL integration](postgres-schema-and-operations.md) and [upgrade](postgres-upgrades-and-compatibility.md) packets now develop D-06/D-13; provider/dependency selection follows.
