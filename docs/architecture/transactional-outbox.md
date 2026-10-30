# Transactional outbox — planning closure 1 of 6

Status: implementation baseline, 2026-10-29. Closes D-12's design choices; runtime evidence remains SP-017. This is an optional delivery integration, not an exactly-once handler or cross-database transaction guarantee.

## Scope and API

Ship PostgreSQL and same-host SQLite source adapters, each able to deliver to any certified Spindle destination profile. Packages: `Spindle.Outbox` (relay/contracts, depends on Spindle), `Spindle.Outbox.Postgres` (Npgsql source), and `Spindle.Outbox.Sqlite` (Microsoft.Data.Sqlite source). No EF dependency or automatic SaveChanges interceptor. Other source databases are outside the initial adapter set; Redis is a destination, not a business-transaction source adapter.

Use native StageAsync when business data and queue writes actually share one compatible connection/transaction. Use the outbox when they do not. The caller must explicitly commit the business transaction in both cases. Wolverine's documented integration likewise persists outgoing envelopes in the business transaction; its batching discussion is a useful implementation comparison, not a reason to import its whole messaging model. [Wolverine outbox integration](https://wolverinefx.net/guide/durability/efcore/outbox-and-inbox.html)

Proposed public surface is `IPostgresOutbox.StageAsync(job, NpgsqlTransaction, destination, options, ct)` and a SQLite equivalent, returning `StagedOutboxMessage` with source/message identity. Stage serializes and inserts the immutable intent on that exact caller connection. It does not contact the destination, commit/dispose the transaction or report a queue DispatchReceipt. Hosts using EF can pass its actual provider transaction explicitly. The adapter owns no invisible business-transaction retry.

Stage uses a stable caller-provided or preallocated MessageId, immutable source-store UUID, destination store/application/profile binding, exact payload bytes and frozen effective policy. The registered destination profile/policy manifest must be available locally and is revalidated by the receiver; configuration drift causes visible rejection, not silent rewriting. Reusing an existing message ID with changed intent is a conflict. A source UUID persists across ordinary restarts and is never regenerated as a retry trick.

## Three different identities

1. **Source message identity:** `(source_store_id, message_id)`, immutable for the business event.
2. **Destination delivery identity:** destination store/application plus that source identity; retained independently of job detail and short transport receipts.
3. **Transport command identity:** one bounded ReceiveOutbox request. Its operation ID/deadline/body remain immutable through retries of that request.

A generic PreparedDispatch created during the business transaction can expire before an outage ends. Therefore do not pretend ordinary short dispatch receipts are a complete outbox protocol. Add `ReceiveOutbox/1`: it atomically checks a durable delivery identity, applies ordinary dispatch/uniqueness rules, stores the resulting job mapping/disposition, and writes a transport receipt. This operation is required on every advertised outbox destination driver.

Add destination `outbox_deliveries(application, source_store_id, message_id, delivery_fingerprint, accept_until, retain_until, job_id, disposition, confirmed_profile)` with a unique application/source/message key and cleanup index. Job IDs are logical references, not cascading history dependencies. The stored confirmed_profile is a profile identifier, not independent proof of a fresh persistence boundary: Redis replay still requires the selected profile’s fresh write confirmation before reporting delivery. Its logical fingerprint covers destination binding, source/message ID, absolute acceptance cutoff and all frozen intent/diagnostic fields. It excludes changing transport command ID/deadline; the ordinary command fingerprint includes those too.

Receipt lock precedes delivery-identity lock, which precedes dispatch uniqueness keys/new-job/scope creation. This adds a lock rank before the existing dispatch path. Inbox cleanup never locks jobs afterward. Same-source messages do not promise FIFO or completion ordering. Receipts/mappings may return Reused when the caller explicitly requested ordinary business uniqueness; delivery confirmation does not imply a new handler ran.

## Absolute replay horizon

Default acceptance horizon: **7 days from source staging time**, sampled from the source database. Persist the resulting absolute AcceptUntil in the source intent and every destination request. Configurable maximum is 30 days; destinations validate that policy. Clock skew may reduce useful delivery time or reject a far-future cutoff; it cannot slide a persisted cutoff on retry.

The destination retains its delivery mapping through at least `AcceptUntil + 24 h`. It reads a matching mapping before expiry rejection, but a new mapping may only be applied at a fresh destination decision time strictly before AcceptUntil. Pruning and delivery serialize on the same identity. All transport attempts carry the same cutoff, so even after pruning an old original request cannot newly apply. Do not extend the cutoff under the same message ID or regenerate it after restore.

Transport NotAfter is at most 30 s and at most AcceptUntil. Persist each new request's exact bytes/ID in the source row before sending. On an unknown reply, first resolve/replay that request. After its deadline, a new ReceiveOutbox command is permitted **only** under the same immutable delivery identity/cutoff, relying on the atomic delivery mapping even if the prior request is still finishing. This is a narrow, explicit outbox rule; ordinary DispatchAsync still cannot replace an expired unknown operation with a new ID.

After the horizon, resolve a retained mapping but do not newly deliver. Missing history becomes blocked/expired-unknown for operator reconciliation. Extending the horizon or intentionally rerunning work requires an audited new message identity acknowledging possible duplicate business effects. No finite retention scheme promises deduplication after loss of the retained destination history.

## Source records and relay

Source schema is versioned separately from the queue schema:

| Record | Essential fields |
|---|---|
| outbox_metadata | Source UUID, schema/protocol version |
| outbox_messages | Message ID, destination binding, immutable intent/codec/fingerprint, staged_at, accept_until, state, next_attempt_at, lease owner/token/expiry, delivery attempt count |
| Current transport attempt | Operation ID, deadline and exact request bytes; persisted on the message before network submission |
| Delivery result / diagnostics | Confirmed destination job/disposition/profile, delivered_at, bounded error code, revision and blocked reason |

States are pending, leased, delivered, blocked. Retrying leases return to pending with a due time. Index due pending work and lease expiry; preserve blocked/undelivered messages until an explicit disposition. Store payloads with the same bounds/access controls as queued jobs; do not log envelope contents.

Relay sequence: claim a bounded source batch in a short source transaction; commit; send outside the source transaction; confirm the destination's selected durability profile; mark delivered in another source transaction guarded by the current lease token/revision. Source rollback publishes nothing. A queue acknowledgement followed by a lost delivered mark causes the next relay to retrieve the same delivery mapping, not a new logical job. A stale relay may reach the destination, but cannot mark a replacement source lease delivered; destination identity deduplication remains the protection.

Initial budgets: 4 in-flight messages, claim at most 4 free slots, 60 s source lease with 15 s renewal, 5 s per provider call, 1 s idle poll, exponential retry from 1 s to 60 s with equal jitter. Keep one source control connection reserved for renewal/marking and one for claim/read work; this is **2 additional source connections per relay group**, plus caller-owned business connections. Destination traffic uses its bounded producer/outbox admission class. No source transaction stays open over destination I/O. A hung provider task retains its I/O slot.

Transient outages retry within AcceptUntil; incompatible policy/codec/binding and deterministic invalid intent block with diagnostics. Stop new relay claims during source renewal uncertainty or destination pressure. Source lease loss stops new sends for that ownership; an in-flight send remains resolvable. Shutdown stops claim first and observes outstanding operations within the host deadline; it never marks delivered based merely on a send attempt.

Delivered body/details can be removed after 7 days from delivery. Keep source identity/result tombstones until the later of that cleanup horizon and AcceptUntil + 24 h; do not recycle message IDs. Pending/blocked records have no automatic TTL. Source or destination restore requires quiescing relays and reconciling histories; restoring a destination without its delivery mappings cannot preserve previous delivery guarantees. Source-only restoration within the retained destination horizon can recover through mappings.

## Required evidence

| Case | Required result |
|---|---|
| OUTBOX-01 | Caller rollback removes business and outbox writes; no destination contact during Stage |
| OUTBOX-02 | Queue accepts, source delivered-mark reply is lost: retry maps to the original job |
| OUTBOX-03 | Two relay owners, stale source token, expired transport request: one delivery mapping and no stale source update |
| OUTBOX-04 | Absolute cutoff equality, cleanup race and delayed original command never create a fresh delivery after cutoff |
| OUTBOX-05 | Destination unavailable at staging, policy drift and malformed payload are visible without false delivery success |
| OUTBOX-06 | Source/destination restore scenarios demonstrate the finite horizon and history-loss limitation |
| OUTBOX-07 | Source connection, payload, task and retry bounds hold during long outage and shutdown |

This closes the planning scope. All cases are implementation obligations, not passing results.
