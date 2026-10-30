# PostgreSQL migrations and mixed-version compatibility

Status: concrete D-13 proposal, 2026-10-29. Uses the gates/version axes from the [consolidated PostgreSQL contract](postgres-schema-and-operations.md). No migration tool, live schema or compatibility test exists. The examples describe future upgrade classes, not released Runwick versions.

## 1. Compatibility is explicit and checked during use

A binary ships a manifest listing readable schema revisions, writable revisions, supported protocol epochs, operation/result versions, policy evaluators and payload/profile tuples. The store lists installed/accepted formats; the application lists required capabilities and admission mode. A matching package major number alone is insufficient.

Startup validates the manifest and registers diagnostic presence. Every mutation then checks gates inside its transaction; the check cannot be a cached startup boolean. Read operations check their projection/receipt codec against the current store gate. The same checks apply to caller-owned transaction staging, not just worker-owned connections.

Payload decoder support is narrower than operation support: a worker may safely run the protocol while skipping an unsupported payload/profile tuple. Conversely, decoding a payload does not mean an old Begin implementation understands mandatory rate policy. Required unknown semantics fail closed before new mutation.

Application drain mode blocks Dispatch/Stage, Reserve, Begin, Materialize and manual Trigger/Retry (all new admission or work creation). Compatible Renew/Finish/Recover, cancellation, authorized queries/receipt resolution and audited deployment controls remain available. Schedule edits, normal policy edits and background cleanup pause during the transition unless explicitly included in its migration plan. This intentionally favors a bounded, understandable first upgrade protocol over a universal zero-downtime promise.

Normal maintenance mode rejects routine mutations/queries unless the migration manifest explicitly lists a safe subset. Before entering it, finish drain/recovery or record a controlled stop plan for remaining owners. Never allow only renewal on a schema where its lease/permit updates have become incompatible.

## 2. Mixed-version matrix

| Store/application condition | Older client | Newer client | Required action |
|---|---|---|---|
| Current schema/semantics supported by both | May operate within manifest | May operate within manifest | Ordinary rolling code update with regression evidence |
| Additive expansion, new feature inactive | Operates only if declared compatible with expanded schema | May operate under old required capabilities | Do not emit new semantics merely because a new column exists |
| New feature activates an unsupported operation/policy | New admission rejected by gate | Operates after migration/activation completes | Drain/replace old writers; no silent downgrade |
| New payload/profile only | Skips unsupported jobs before Reserve | Claims supported tuples | Preserve old decoders for existing backlog/replay horizons |
| Contract/destructive schema incompatible with old reader/writer | Rejected | Only supported binary may run | Maintenance/contract gate; old code rollback unavailable |
| Application drained during compatible expansion | Settlement/recovery only | Settlement/recovery only until reopened | Creation paths remain disabled |
| Old receipt codec still supported, original operation no longer applies | Authorized historical result can resolve | Same | Resolution must not mutate/reapply old intent |
| Unknown receipt codec or incompatible read schema | Explicit compatibility/unavailable result | Same unless supported | No fabricated NotFound/success; retain recoverable history |
| Restored database or lossy failover | Cannot assume original authority/history | Cannot assume original authority/history | Stop old writers and execute restore reconciliation |

There is no safety guarantee for a pre-gate client with unrestricted SQL credentials. Such a client requires shutdown and, when necessary, credential/session revocation before activation. Gate checks are part of Runwick's supported protocol, not database enforcement of arbitrary client code. Do not use expired worker presence as proof of quiescence.

## 3. Migration manifest and runner

Each immutable migration manifest includes: ID/checksum, source/target schema revisions, reader/writer compatibility, required application modes, transactional versus nontransactional phases, expected lock impact, backfill validation, restart/recovery procedure and rollback disposition. Changing a released migration body without changing its ID is an error.

Use an explicit deployment command/API, separate from worker startup and dashboard requests. Development initialization can invoke that same runner explicitly. The runner uses a dedicated primary connection and a session-level advisory lock keyed by database/schema identity for the entire multi-transaction migration. This serializes cooperating migrators; jobs still use row locks. Session advisory locks survive rollback and require explicit release/session termination, so never return a still-locked connection to a pool. Loss of the lock-owning connection aborts further migration work. [Advisory-lock behavior](https://www.postgresql.org/docs/18/explicit-locking.html)

Before bootstrap metadata exists, the same database/schema migration lock protects initial creation. Check existing object ownership/shape rather than assuming IF NOT EXISTS proves compatibility. Use a separate migration role; normal runtime credentials do not need DDL. Record partial progress honestly—started is not applied.

## 4. Expand, activate, contract

1. **Preflight:** inspect manifests, active application/transaction state, persistence profile, expected locks and available backup/recovery procedure. Reject an incompatible source revision or changed checksum. Compute a reviewable migration plan.
2. **Expand:** acquire migration lock; publish the compatible expansion mode in a short store-gate transaction. Add only changes explicitly safe for declared older clients. Use lock/statement timeouts and stop on unexpected dependency/lock impact.
3. **Backfill:** process bounded keyset batches with checkpoints, immutable source versions and validation counts. New writes already use a compatible representation. Never mark a backfill complete simply because one empty page was observed against an unstable predicate.
4. **Validate:** check constraints, indexes, data/profile/policy invariants and codec compatibility. Publish the new installed schema/manifest only after its required objects and backfills are verified. Keep feature activation separate.
5. **Drain if semantics change:** set application gate to drain under its exclusive row lock. In-flight old transactions finish before that change commits. Stop old worker/producer/scheduler versions; compatible owners settle or recover. Do not hold a long transaction while waiting for handlers.
6. **Activate:** verify required compatible actors and drained durable state, update required capabilities/revisions with receipt/audit, then reopen application admission. If an old supported binary reconnects, the gate rejects unsupported creation commands.
7. **Contract:** in a later explicitly reviewed migration, remove obsolete columns/codecs only after old clients are excluded and backlog/receipt/window/outbox horizons permit removal. Use maintenance where old SQL would break. Record that the rollback floor has advanced.

No live old handler can be proven stopped solely from an expired lease. A semantics-changing upgrade that requires absence of old external activity must stop those processes or establish application-specific fencing/idempotency before reopening. Drained durable ownership is one check, not a physical execution guarantee.

New schema with feature inactive may be backward compatible; activated semantics may not be. Rollback must evaluate both. Never disable required rate checks to make an older binary start, and never overwrite historical prepared policy with current defaults during backfill.

## 5. PostgreSQL DDL and crash recovery

Use ordinary transactional DDL when a bounded lock duration is acceptable. For large existing tables, explicitly plan concurrent index creation and validate the resulting index state. PostgreSQL does not allow CREATE INDEX CONCURRENTLY inside a transaction block; a failed build can leave an invalid index. The migration ledger therefore records each such phase and verifies definitions/validity before continuing or rebuilding. A blind retry with IF NOT EXISTS is insufficient. [Concurrent index creation](https://www.postgresql.org/docs/18/sql-createindex.html)

For suitable foreign-key/CHECK additions, consider NOT VALID followed by VALIDATE CONSTRAINT. This separates initial enforcement on new writes from checking historical rows, but neither phase is lock-free. Each migration must document actual locks and table-size impact. [Constraint validation](https://www.postgresql.org/docs/18/sql-altertable.html)

Use per-phase bounded lock/statement timeouts. Do not automatically terminate unrelated business sessions. If a caller-owned transaction holds a shared gate or required table lock, fail/defer with diagnostics and leave the application in its last recorded safe mode. Reconnect only after inspecting the ledger and actual schema under a newly acquired migration lock.

Recovery after a crash depends on phase:

- Rolled-back transactional phase: verify ledger/objects, then retry unchanged migration.
- Committed phase with lost response: inspect ledger and object definitions; do not create another migration identity.
- Interrupted concurrent index: inspect validity/definition and perform its declared repair path.
- Partial backfill: resume checkpoint with idempotent assignments and revalidation.
- Interrupted activation: resolve original revisioned command; do not blindly reopen admission.

Schema revision is advanced only by completed validated phases. An error must never label partially installed objects as a compatible final schema. Migration diagnostics contain object names/codes and safe progress, not payloads or secrets.

## 6. Specific Runwick compatibility traps

| Change | Required treatment |
|---|---|
| Name/version-only claim → serializer-aware tuple | Replace old claiming code before emitting new profiles; enforce operation capability at gate, not presence alone |
| Begin without rate semantics → rate-aware Begin | Drain/replace incompatible admission writers before activation; old code cannot ignore job_rate_requirements |
| Retry/jitter/unstarted-delay policy changes | Version snapshot/evaluator, retain historical decoder; no default substitution into prepared replay |
| Millisecond normalization/fingerprint rule changes | New encoding/fingerprint version with comparison support for retained bindings, or explicit incompatibility |
| Previous jsonb payload import | Exact original bytes cannot generally be reconstructed; any future import needs declared legacy representation and new compatibility identity, not a byte-preservation claim |
| Audit schema/permissions change | Preserve atomic successful-operation audit; compatible readers may project fewer fields but cannot bypass authorization |
| Serializer/converter upgrade | Run golden vectors and retain historical profile; same package name is not semantic proof |
| Retention horizon shortened | Re-evaluate correctness dependencies; never prune live windows/grants/receipts or reset fences |

The jsonb case is a hypothetical import/legacy issue, not a required initial migration: no database was built from that earlier sketch. The first real schema already stores payload bytes.

## 7. Restore and rollback boundaries

Binary rollback is allowed only when the previous binary's manifest accepts the installed schema, active requirements, remaining policy/payload/receipt formats and operation semantics. Destructive downgrade is not the default rollback strategy. Prefer a forward corrective migration or a controlled restore procedure.

A database restore can lose receipts, occurrence high-water marks, grants and fencing increments. Quiesce old writers/workers and reconcile external effects before resuming. A store UUID or schema revision does not detect every copied/restored history. An independent clone must receive a new logical store identity through an explicit offline procedure; old prepared envelopes then fail binding instead of targeting the clone accidentally.

Document provider/server versions and persistence/failover profiles in each test/release report. This packet uses PostgreSQL 18 documentation for SQL behavior, but does not select a minimum supported server version or certify an Npgsql version. The provider/version test matrix is the next dependency-selection item.

## 8. Acceptance cases

| ID | Required proof |
|---|---|
| UPG-01 | Concurrent migrators, lock-connection loss and checksum mismatch cannot apply divergent migrations |
| UPG-02 | Additive expansion permits only explicitly compatible old/new clients; activation rejects old required-semantic writers |
| UPG-03 | Long in-flight caller transaction blocks gate change until completion or migration timeout; no partial staged commit |
| UPG-04 | Drain/maintenance modes preserve explicitly allowed settlement/receipt paths and block every creation path |
| UPG-05 | Interrupted DDL/index/backfill phases resume from actual catalog state without falsely marking readiness |
| UPG-06 | Rollback after semantic activation is rejected when old binaries would ignore rates/profiles/policies |
| UPG-07 | Old receipt can resolve without reapplication; missing codec/history produces explicit incompatibility/unknown |
| UPG-08 | Expired presence/lease never substitutes for stopping incompatible writers or noncooperative physical execution |
| UPG-09 | Restore/clone tests demonstrate identity binding and documented history/fence limits |
| UPG-10 | Retention/migration/worker load respects bounded locks and renewal margins; failed migration stays in a safe recorded mode |

Pair these with PGX, CMD/OWN/RATE/SCH, SER and TIME cases. None ran during this planning pass. D-06/D-13 have concrete integration recommendations; final DDL, query plans and evidence still require review/testing. The [provider plan](provider-dependency-plan.md) records Npgsql selection and proposed server targets; [capacity defaults/budgets](capacity-defaults-and-budgets.md) adds D-07 recommendations. The [scheduler packet](scheduler-evaluator-and-defaults.md) now requires exact evaluator/data-hash compatibility and prepared forward cutovers; add its SCHED upgrade cases. All six planning items are closed in the [final baseline](../architecture-baseline.md); these contracts now feed implementation.
