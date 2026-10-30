> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Consolidated PostgreSQL schema and operation contract

Planning reconciliation: the [final baseline](../architecture-baseline.md) and [outbox closure](transactional-outbox.md) add ReceiveOutbox/1 and delivery identities below. [Operational defaults](management-retention-defaults.md) supply receipt/retention floors. These are initial-schema requirements, not migrations claimed to exist.

Status: D-06/D-13 engineering proposal, 2026-10-29. Reconciles the [execution schema](postgres-storage-design.md), [capacity](capacity-control.md), [scheduler](scheduling-internals.md), [management](management-and-dashboard.md), [serialization](identity-and-serialization.md) and [timing](execution-defaults-and-timing.md) additions. This document takes precedence for the integration points identified below. It is a schema/transaction specification, not executable DDL or a tested migration.

## 1. Version axes and first release

Keep these identities distinct:

| Axis | Meaning |
|---|---|
| Store identity | UUID identifying the logical database/schema installation, bound into prepared commands |
| Schema revision | Physical tables/constraints/indexes installed by migrations |
| Protocol epoch | Meaning of shared records/results that all participating clients must understand |
| Operation version | Semantics of one command, such as Begin or MaterializeOccurrences |
| Receipt/envelope/fingerprint version | Encoding and equality rules needed for replay |
| Payload version + serializer profile | Decoder/upgrade capability for a job |
| Policy evaluator version | Meaning of retry/rate/recurrence settings |
| Package/build version | Software artifact identity; not a substitute for any axis above |

Propose epoch 1 and operation version 1 for the first released consolidated contract. The early execution milestone can implement a subset, but must reject unimplemented operations/features. In particular Begin/1's full definition includes rate admission; an execution-only milestone may process jobs with no rate requirements but cannot accept a rate-enabled application. Do not publish an unversioned Begin that ignores extra requirements.

No Runwick database has been implemented from earlier documents. Draft changes from jsonb to bytes or name/version to full decoding tuples are corrections to the initial schema, not fictitious migrations from a released v0. Future changes get immutable migration IDs and compatibility manifests as specified in [upgrade compatibility](postgres-upgrades-and-compatibility.md).

All application-scoped keys include application. Use canonical UUID byte order for lock sorting and cursor encoding across drivers; do not inherit CLR Guid byte-array layout accidentally. Stable text identities compare bytewise/ordinally under an explicitly selected deterministic database collation. SQL collation/encoding fixtures must prove agreement with the wire contract. All duration columns use checked bigint milliseconds, including scheduler/rate settings previously sketched in microseconds.

## 2. Store and application control records

Store-level records are the explicit exception to the application-column rule:

```text
store_metadata:
  singleton_id smallint PK CHECK = 1
  store_id uuid UNIQUE
  schema_revision bigint, protocol_epoch integer
  migration_mode text                  normal | expand | maintenance
  compatibility_manifest_version integer
  compatibility_manifest jsonb        explicit accepted schema/operation/codec sets
  manifest_digest bytea, revision bigint

schema_migrations:
  migration_id text PK, checksum bytea
  target_schema_revision bigint, phase text
  state text                          started | applied | failed
  started_at timestamptz, completed_at timestamptz?
  checkpoint jsonb?, diagnostic_code text?

application_controls:
  application text PK
  revision bigint, required_protocol_epoch integer
  required_capabilities jsonb          validated, bounded capability/version map
  admission_mode text                  open | drain | maintenance
  updated_at timestamptz, actor_subject text?
```

The manifest declares compatibility, not client authority. Validate its format/digest and known requirements; an unknown required capability is an error. Runtime roles cannot invent schema readiness by updating these rows directly. Separate migration privileges from ordinary runtime access; application-control changes pass revisioned, audited commands.

Every normal mutation transaction acquires store_metadata FOR SHARE, then its application_controls FOR SHARE, verifies the operation/version/mode, and keeps those locks until commit. Only then acquire its command receipt and lower-level locks. Bounded queries acquire the store gate and verify their read projection version; receipt resolution remains available when its decoder/schema is explicitly compatible. A migration-maintenance mode can reject even queries when no safe projection remains.

Updating an application gate takes store FOR SHARE then that application row FOR UPDATE and never acquires job/scope locks afterward. It can insert its own receipt/audit below the gate. Updating the store gate takes store FOR UPDATE and does not acquire application/job rows afterward. These changes wait for older transactions holding shared gate locks to end; later transactions observe the new mode. Do not upgrade a shared gate lock after acquiring a job lock.

Shared gate locks do not serialize ordinary readers with each other, but they add cost and can delay activation under long caller transactions. Measure that cost. Gate-row policy is cooperative protocol enforcement for supported clients, not a security sandbox against arbitrary SQL using database credentials. Old clients that never implement gates must be stopped/revoked before incompatible activation; worker presence expiry is not evidence they cannot write.

## 3. Record inventory and mandatory reconciliations

Existing detailed fields live in the linked designs. The following inventory is the complete required set of record families for the planned features, with integration changes that migrations must include:

| Record family | Identity / required integration |
|---|---|
| jobs | application/job_id; name/payload/profile tuple, seven states, generation/revision, counters, ownership, queue/priority, eligibility; add payload_retained and detail_expired_at? for retained-history projections |
| job_payloads | application/job_id; original payload_utf8 bytea, encoding version, profile ID/version, bounded business/diagnostic metadata; immutable |
| job_generations | application/job_id/generation; execution/abandonment limits, timeout, both versioned retry policies, counters, policy snapshot version |
| job_attempts | application/attempt_id; job/generation, reservation/execution identities, ownership, timestamps, observed outcome/error and applied disposition; include admission_deferred |
| concurrency_scopes / job_concurrency_requirements / concurrency_permits | Preserve existing scope/requirement/permit design and immutable membership; expected permit set verified on renewal/finalization |
| rate_scopes / job_rate_requirements / rate_grants | window_ms, limit/revision, logical time/freeze; grants retain scope/attempt identity and policy revision independently of job history |
| queue_controls | application/queue; pause/revision; seed-only registration, fixed low/medium/high vocabulary in this proposal |
| dispatch_keys | application/job_name/kind/key; retained logical job ID, expiry, request_fingerprint_version and digest; no FK that loses a live window when job history expires |
| outbox_deliveries | UNIQUE(application, source_store_id, message_id); delivery fingerprint/version, immutable accept_until, retain_until, logical job_id, disposition/profile; cleanup index(application, retain_until, source_store_id, message_id); no cascading job FK |
| storage_commands | application/command_id; operation/version, protocol epoch, fingerprint_version/digest, not_after, receipt_codec_version, decided_at/result, retention_class and retain_until |
| queue_events | Bounded execution/history events; observed times, IDs and safe details; no heartbeat event on every renewal |
| management_audit | Separate successful control/disclosure audit with actor issuer/subject/scheme, action/target, safe reason, expected/resulting revisions, decision time and optional trace; independent retention |
| schedules / schedule_definitions | Stable identity and definition/control/cursor revisions; immutable definitions with original payload_template_utf8, profile ID/version, policy snapshot version; grace/catch-up durations in ms; cron evaluator profile, zone-data label and 32-byte zone_data_sha256 per the scheduler packet |
| schedule_occurrences / schedule_audit_ranges | Existing occurrence identity/high-water rules and explicit coverage records; logical job references survive detail pruning |
| worker_instances / worker_capabilities / worker_subscriptions / worker_pool_snapshots | Bounded presence/capability tuple/pool state and queue subscriptions; no FK from leases/attempts that treats presence as authority |
| pool_controls / scaling_events | Revisioned target overrides with expiry, target worker incarnation/pool identity, and bounded diagnostic decision history |
| retention_policies / cleanup_progress | Revisioned per-record-class retention and resumable progress/last error; correctness dependency checks still run under owning locks |

Source outbox_metadata/outbox_messages belong to the separate business-source schema described in [transactional outbox](transactional-outbox.md). Source Stage joins the caller transaction; relay claim/renew/mark are short source transactions. They are not destination queue rows or permission to hold queue locks across network calls.

Profile consistency: add UNIQUE(application, job_id, serializer_profile_id, serializer_profile_version) on jobs and a matching FK from job_payloads. This avoids two independently editable copies. Do not mutate profiles during retry. job_payloads remains removable for eligible detail retention while the jobs tombstone remains. Manual retry requires retained payload and compatible policy; missing payload returns HistoryUnavailable without starting a new generation.

Current-generation/current-attempt deferred FKs remain as designed. Closed attempt identity may need to outlive detailed error text. Retention must not delete attempts while permit rows reference them. Rate grants, dispatch window identities, outbox delivery mappings and management audit must not cascade with job deletion. Scope fence rows are not recreated under an old identity.

Store payload metadata in an explicitly typed bounded representation. If jsonb is used for metadata/control fields, verify their accepted values fit that representation; original job bytes never pass through jsonb. Schedule templates obey the same bytes/profile contract as ordinary dispatch and deterministic occurrence preparation freezes the produced job bytes before committing.

Separate management_audit from queue_events so execution-history retention cannot erase required administrative history. Unique key: (application, command_id, action, item_ordinal); item_ordinal=0 for single-target commands, deterministic ordinal for a bounded batch. Sensitive disclosure uses a dedicated command/access ID. An audit write is part of the control transaction; external exporting is not.

Worker capability identity is (application, instance_id, capability_revision, job_name, payload_version, profile_id, profile_version). Publish a bounded complete revision atomically with its presence update, or stage rows and atomically switch the active revision. Queries join only the active revision. Expired presence may be pruned without touching jobs/leases. A worker instance ID is never reused after restart.

Pool overrides are incarnation-specific initially: (application, instance_id, pool_name), expected control revision, requested target and expires_at. A restarted worker does not inherit an old instance's override. Registration-time pool bounds still constrain the override; a new target does not forcibly end existing tasks. Service-wide permanent pool configuration remains deployment configuration rather than an invented global worker identity.

## 4. Consolidated command catalog

All listed operations are proposed version 1. Durable mutations have immutable command ID/body/deadline and typed result; observational queries and replaceable presence heartbeats do not pretend to be replayable lifecycle commands.

| Operation family | Atomic boundary / special result |
|---|---|
| Dispatch / provider Stage | Job + bytes + generation + memberships + uniqueness + receipt; Stage remains inside caller's transaction |
| ReceiveOutbox | Delivery identity + dispatch effects/uniqueness + retained job mapping/disposition + transport receipt; matching mapping wins before cutoff rejection; new delivery requires fresh time before immutable accept_until |
| Reserve | Full decoding tuple + queue/policy checks + job/attempt/permits + receipt |
| Begin | Current ownership + budget/cancellation + all rate grants, or committed RateLimited closure/release; one result receipt |
| Renew | Job and exact permit set share expiry; current ownership and operation compatibility required; no receipt row (R1) |
| Finish / Recover | Attempt close + policy/accounting + job transition + owned permit release + applicable uniqueness/event + receipt |
| ReleaseReservation | Reserved-only ownership check + attempt closed `released` + prior waiting state restored + exact permit release + receipt; no budget or rate charge (accepted 2026-10-30) |
| Cancel / ManualRetry | Generation/revision guards + transition + required uniqueness/membership checks + audit + receipt |
| RegisterQueue / SetQueueState | Seed/drift or revisioned pause/resume + audit where changed + receipt |
| RegisterScope / UpdateConcurrencyPolicy / UpdateRatePolicy | Seed/drift or revisioned policy + logical-time/freeze rules + audit + receipt; no live membership rewrite |
| RegisterSchedule / MaterializeOccurrences | Definition seed/drift, or validated revisions + occurrence/jobs/keys/cursor/audit coverage + receipt |
| PauseSchedule / ResumeSchedule / UpdateSchedule / TriggerSchedule / ResolveCatchUp | Existing schedule guards and explicit cursor/cutover effects + audit + receipt |
| RecordScheduleEvaluationFailure | Expected schedule revisions and exact evaluator/data profile; deterministic review reason + needs_review + audit/receipt, no cursor movement |
| RevalidateSchedule | Expected revisions/profile + valid prepared evaluation; clear non-episode review with audit/receipt, preserve cursor/pause and create no jobs |
| SetPoolOverride / SetRetentionPolicy | Expected control revision + validated limits/expiry + audit + receipt |
| SetApplicationMode / ActivateCapabilities | Gate revision/version guards + audit + receipt; special gate-first lock path |
| RecordDisclosure | Authorized access-intent audit + receipt; precedes sensitive response delivery |
| CleanupBatch | Bounded explicit record class and cutoff/cursor, dependency-safe deletes/redactions + progress/result; replayable mutations retain receipts |
| ResolveCommand | Authorized compatible receipt read; no new job/lifecycle mutation |
| PublishPresence / ready probe / bounded management projection | Presence sequence/capability revision or observational snapshot; no ownership authority |

Standard reply envelope distinguishes Effect (ObservedNoEffect or Applied), the existing Replayed flag and the semantic code. RateLimited is Applied, possibly Replayed, never ObservedNoEffect. An accepted no-op that retains a receipt/audit also has Applied effects. PostgreSQL acknowledged outcomes are atomic; transport uncertainty is surfaced by the client recovery API. Redis may additionally report Recovering under its profile. The internal type sketches now include this distinction; implementation must validate allowed combinations.

Pure NoWork/QueuePaused/CapacityBlocked observations need not retain a receipt and may change on re-observation. Changed-body conflicts are guaranteed against retained/applied identity; do not claim a rejected observation permanently reserved an ID. Applied commands and audit cannot commit without a complete result. Finalize a receipt only once; a deferred constraint trigger checks that placeholder result/decided_at fields are populated before commit.

Schema/codec rejection happens before unsafe writes. Known deadlock/serialization rollback can retry the same command while its deadline permits. Lost COMMIT acknowledgement remains unknown, even when a later timeout or connection close occurs. Queries/receipt lookup use the authoritative primary for current control decisions.

## 5. Lock-order matrix

Every normal path starts with store gate → application gate → command receipt, where applicable. Within an application the lower-level ranks are:

```text
outbox delivery identities (source UUID/message UUID order)
→ schedule rows (sorted) → queue control rows (sorted) → existing jobs (UUID order)
→ dispatch keys (canonical job-name/kind/key bytes)
→ concurrency scopes (UUID order) → rate scopes (UUID order)
→ owned dependent rows / append-only audit and events
```

Do not acquire an earlier-ranked row after a later one. Lock sorted rows explicitly; query ORDER BY is not an all-purpose promise about every implicit/FK lock. PostgreSQL row locks last to transaction end and deadlocks remain possible when caller-owned business transactions introduce external ordering. [PostgreSQL locking](https://www.postgresql.org/docs/18/explicit-locking.html)

| Path after gate/receipt | Required ordering and restriction |
|---|---|
| Reserve | Queue FOR SHARE → selected job FOR NO KEY UPDATE → concurrency scopes; create dependent attempt/permits |
| Begin | Job → any terminal active keys → concurrency scopes → rate scopes → grants/dependent writes; rate deferral preserves active key |
| Renew | Job → concurrency scopes; never locks rate/schedule just to renew |
| ReleaseReservation | Job → active key only when a durable cancellation terminalizes → concurrency scopes; same shape as Finish |
| Finish/Recover/ManualRetry | Job → relevant active key(s) → concurrency scopes → dependent writes; Recover does not refund rate grants |
| Cancel | Job → active keys/scopes only when a waiting job terminalizes; owned cancellation merely records request |
| Dispatch/Stage | Keys → brand-new job records → scope provisioning/FK locks; duplicate binding never locks an existing job afterward |
| ReceiveOutbox | Delivery identity → dispatch keys → new jobs → scope provisioning; never locks a reused existing job afterward |
| Outbox mapping cleanup | Delivery identities only after gate/receipt where applicable; fresh cutoff/retention check; no later job lock |
| Materialize/Trigger | Schedule → all batch keys sorted globally → all new job records → scope provisioning/FKs; no nested command receipts |
| Queue policy | Queue control only plus audit/receipt; never locks jobs afterward |
| Scope policy/cleanup | Relevant concurrency scopes then rate scopes as needed; never goes backward to jobs/keys |
| Schedule edit/control | Schedule only plus immutable definitions/coverage/audit; never cancels materialized jobs inside that transaction |
| Job detail cleanup | Existing jobs first, validate state/dependencies, then dependent detail removal; does not delete scopes/grants/windows |

Fresh job creation is the only deliberate exception to the existing-job rank. Preallocate new IDs and assert they are absent; never treat a collision as permission to update an existing job under held keys. Dispatch aliases retain logical IDs without existing-job FKs/locks.

For bulk materialization, provision all missing concurrency scopes in canonical natural-key order, then all missing rate scopes; take subsequent FK/key-share references in sorted IDs within each family. Never alternate scope families per job. Scope edits must not change natural keys or delete fenced identities. Hidden unique-index/FK interactions still require forced-interleaving tests; do not claim the written ordering proves deadlock freedom. On a retryable database deadlock, roll back the entire operation and preserve command identity.

Never hold these locks across user handlers, payload converters, recurrence computation, backoff sleeps or network export. Prepare bounded immutable schedule/job plans before entering the transaction and revalidate policy/revisions inside it. PostgreSQL DDL has separate locking requirements; the migration protocol must not borrow worker lock assumptions.

## 6. Read paths, indexes and retention

Keep the existing ready, expired-lease, terminal-history and permit indexes. The ready predicate includes the full supported decoding tuple; add a covering/type-oriented index only after the unsupported-backlog workload demonstrates need. Required additional index shapes:

| Query | Index/ordering shape |
|---|---|
| Receipt cleanup | (application, retain_until, command_id), not only not_after |
| Rate count/expiry | (application, rate_scope_id, authorized_at, attempt_id) |
| Due schedules | (application, scan_after_at, schedule_id) with enabled/unpaused/not-review and next_due_at non-null predicate |
| Occurrence history | (application, schedule_id, scheduled_at, occurrence_kind, occurrence_key) |
| Administrative audit | (application, decided_at, audit_id) and (application, target_kind, target_id, decided_at, audit_id) |
| Worker presence | (application, expires_at, instance_id); active capability revision and subscription join keys |
| Bounded job lists | (application, queue, created_at, job_id), with further state/time indexes justified by authorized query plans |
| Cleanup progress | Unique (application, record_class, partition_key) plus policy revision and resumable cursor |

Index definitions must match actual predicates/order and bound worst-case reads. Default UI lists do not query payload JSON. No query/count filters unauthorized rows after pagination. A scope-policy cache is a hint only; mutations reread authority under locks.

Retain lightweight job/attempt/generation identity needed for retained references while independently deleting eligible payload/error details. Mark missing details explicitly. Never cascade-delete grants, live deduplication windows, receipts or management audit. Physical whole-job purge requires all correctness dependencies to be past their horizons and no remaining permit/owned attempt references; it is a separate cleanup path, not the default payload-retention action.

Receipt cleanup uses stored retain_until computed from its operation class and NotAfter, under the receipt lock. It cannot delete another transaction's uncommitted placeholder. Rate cleanup owns its rate scope; job cleanup never acquires jobs after entering a scope cleanup transaction. Schedule tombstones/high-water marks and fenced scope identities have separate retention obligations.

## 7. Acceptance extension

| ID | Required proof |
|---|---|
| PGX-01 | Store/application gate update races every command family: old transaction completes first or new requirement is observed before mutation |
| PGX-02 | Full serializer tuple and immutable payload/profile consistency enforced across dispatch, schedule, claim and retry |
| PGX-03 | Begin rate grant/deferral, audit and receipt agree; an unimplemented capability cannot be silently ignored |
| PGX-04 | Finish versus duplicate dispatch/manual retry; materialization versus scope creation/edits; overlapping scope batches in opposite input orders |
| PGX-05 | All FK/unique/check/deferred constraints hold after injected rollback/commit loss; incomplete receipt cannot commit |
| PGX-06 | Payload/detail retention preserves required identities; missing payload blocks manual retry; rate/audit/window lifetimes independent |
| PGX-07 | Scoped paginated queries use intended plans at realistic history sizes; gates/renewals remain within latency/memory budgets |
| PGX-08 | Caller business transactions blocking gate/migration activation produce bounded timeout/retry diagnostics without forced partial commit |
| PGX-09 | Millisecond durations, canonical text/UUID ordering and new generation policy versions match cross-driver vectors |

No case has run. Pair these with the shared driver, SER, TIME and management suites. [Upgrade compatibility](postgres-upgrades-and-compatibility.md) specifies how these contracts can change after release.
