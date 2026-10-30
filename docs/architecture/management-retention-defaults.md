> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Management and retention defaults — planning closure 4 of 6

Status: D-11 implementation baseline, 2026-10-29. Completes numerical/permission choices in the existing management, observability and timing contracts. Values are initial release candidates requiring the planned tests, not legal retention advice or measured safe limits for every host.

## Permissions and bounded management

Use capability names: `Jobs.Read`, `Jobs.ReadPayload`, `Jobs.ReadError`, `Jobs.Cancel`, `Jobs.Retry`, `Jobs.Rerun`, `Queues.Read`, `Queues.Control`, `Schedules.Read`, `Schedules.Trigger`, `Schedules.Control`, `Schedules.Edit`, `Capacity.Read`, `Capacity.Edit`, `Settings.Read`, `Retention.Manage`, `Audit.Read`. RevalidateSchedule requires Schedules.Edit. Scope every grant to the configured application and applicable queues/resources. Viewer grants safe read families; Operator adds job/queue/schedule operational controls; Administrator adds edit/retention capabilities. Sensitive detail and audit remain separate explicit grants even for role presets.

| Limit | Baseline |
|---|---|
| List page | 50 default, 200 maximum; keyset pagination only |
| Historical query interval | 7 days default, 31 days per request maximum; older history is available in bounded intervals |
| Cursor | Signed/protected; binds mount/application, filters, sort, authorized scope; 15 min expiry, 2 KiB maximum |
| Query database budget | 1 s; response/request budget 5 s, caller's earlier cancellation wins |
| Exact count | Explicit request within the query budget; otherwise approximate/unknown with coverage metadata |
| Normal mutation body | 64 KiB; unsupported content types rejected |
| Schedule definition body | 2 MiB endpoint-specific cap, plus existing 1 MiB payload/metadata limits |
| Bulk control | 100 explicit IDs/revisions, 4 concurrent per-item operations; separate receipt/result per item |
| Operator reason | 512 UTF-8 bytes; bounded actor fields 256 bytes each |
| Stored detailed exception | 16 KiB UTF-8, explicit truncation marker; safe code/message separate |
| Detail reveal response | At most the stored payload/error limit; no concatenated bulk payload export |
| Browser poll | 5 s minimum, one in flight per panel, pause hidden tabs; failure backoff to 60 s |
| Per-principal/mount HTTP rate | 120 reads/min, 30 mutations/min, 10 reveals/min; burst at most one quarter of the minute allowance |
| Authentication failures | 10/min per bounded source/identity bucket; HTTPS and explicit verifier still required |
| Per-management-instance work | 16 active requests, 32 waiters, 1 s admission wait; resource class limits still apply |

Limits return explicit validation/429/503 results; no dropped successful mutation or silent page truncation. HTTP request limits are process-local unless the host supplies a shared gateway policy; do not advertise aggregate replica-wide throttling. Cap limiter buckets at 10,000 per instance with 10 min idle eviction; overflow uses a shared restricted bucket. Do not allocate an unbounded map from attacker-supplied identities.

Default shared projection cache is **off**. Query results remain bounded, but cache correctness does not become an authorization dependency. If enabled later by host policy, scope/revision/permission checks in the original management design remain mandatory. Metrics label combinations are capped at 1,000 library-owned series with a bounded overflow grouping; diagnostic buffers cap at 1,024 records and report drops. No database I/O from observable callbacks.

## Command lifetimes

Ordinary prepared dispatch and administrative mutations: NotAfter defaults to 5 min; maximum 5 min for these operation classes. Store receipts through NotAfter + 24 h. This gives user-driven recovery time without allowing indefinite application of a stale command. Schedule-specific controls use the scheduler 30 s lifetime even when initiated by an administrator; that operation-specific bound overrides the generic 5 min administrative default. Scheduling and ownership commands retain their existing 30 s lifetime; Reserve/Begin/Recover/ReleaseReservation receipt floors remain NotAfter + 10 min, Finish at least NotAfter + 24 h. Renew writes no receipt (R1, 2026-10-30). Scheduler mutation/control receipts retain at least NotAfter + 24 h.

Outbox transport commands use 30 s, while their separate immutable delivery mapping retains through AcceptUntil + 24 h. Do not force the outbox's 7-day acceptance horizon into the ordinary command deadline. Commands already accepted into a Redis recovery intent cannot lose that intent because the original NotAfter passed. Receipts and audit are separate retention classes.

## Retention defaults and constraints

| Record class | Baseline and safe-deletion condition |
|---|---|
| Succeeded/cancelled payload and detailed errors | 7 days after that generation becomes terminal |
| Failed payload/detailed errors | 30 days after terminal state; manual retry requires retained usable payload |
| Job/attempt summary history | 90 days after terminal state, subject to all reference/identity obligations |
| Administrative/disclosure/retention audit | 90 days; successful changes and required audit remain atomic |
| Scaling decision history | 7 days; only actual changes, not every unchanged sample |
| Schedule occurrence detail | 30 days after materialization; retain durable high-water mark/tombstone |
| Worker presence detail | 24 h after expiry, with no live references; expiry never recovers leases |
| Command receipts | Operation-class horizons above; pending recovery prevents removal |
| Fixed-window identities | At least through their immutable expiry; replay-relevant references retained independently |
| Active uniqueness | Until the owning lifecycle releases it; no independent TTL |
| Rate grants | Until outside the current logical window and safe under edit-freeze policy |
| Scope fences, schedule/store identities | No automatic TTL/recreation under the old identity |
| Destination outbox mappings | At least AcceptUntil + 24 h |
| Source outbox | Delivered detail 7 days; identity/result through the later required horizon; pending/blocked never automatically deleted |

Retention does not apply to active generations. A manual retry protects its retained payload before cleanup can remove it; after details expire return HistoryUnavailable, not an empty reconstructed job. Whole-job purge must wait for correctness dependencies; logical references may outlive detail rows with an explicit expired-detail projection. A policy reduction never overrides deduplication, active ownership, grant, intent, fence or outbox floors.

Cleanup: one worker per store group, one batch at a time, up to 100 rows or 1 MiB decoded detail per batch, 500 ms database work target, 1 s between batches and 60 s idle poll. Use keyset/progress cursors and transactional expected-state checks; never OFFSET-scan all history. The target is not hard preemption: retain the I/O slot until the task ends. Cleanup uses the background heavy-work allowance and yields during ownership pressure. Multiple processes may cooperate through guarded row claims; idempotence must hold without a permanent cleanup leader.

Audit policy edits and explicit destructive cleanup requests. Automatic cleanup writes bounded batch/progress summaries, not one new audit row per deleted row; audit-summary retention must not recursively create unbounded audit. Failures pause/retry bounded work and surface progress. Backups and exported copies have host-owned retention; deleting a live detail row does not claim to erase every backup.

## Acceptance

OPS-01: bounds/rates/cursors remain authorized across replicas and replay; OPS-02: retry/cleanup/uniqueness races preserve payload/identity dependencies; OPS-03: every receipt class and deadline equality; OPS-04: shortened policy cannot erase outbox/rate/intent/fence requirements; OPS-05: million-row cleanup remains resumable/bounded and does not starve control I/O; OPS-06: denied/throttled/log-overflow traffic stays bounded. Map to SP-011/SP-020–022/SP-026–029, alongside MGMT/RET/OBS cases. No tests have run.
