> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Dispatch preparation, replay, and fingerprint contract

Status: versioned encoding proposal, 2026-10-29. Supports the [consolidated public API](public-api-contract.md) and resolves the earlier storage draft's open fingerprint inputs. Exact implementation/compatibility test vectors still require review. No serializer, codec or cryptographic test implementation is included in this planning pass.

The [identity/serialization decisions](identity-and-serialization.md) now select a proposed built-in System.Text.Json profile, strict numeric comparison, concrete bounds, original-byte storage, profile-aware worker capabilities and a base64-payload JSON export envelope. These refine this document; golden vectors and architecture approval remain outstanding.

## 1. Two identities with different purposes

| Identity | Purpose | Lifetime and comparison |
|---|---|---|
| Mutation command | Resolve a specific operation after a lost response | Application + operation ID, immutable fingerprint, absolute NotAfter, retained result |
| Fixed-window dispatch key | Recognize equivalent business dispatch intent across separate operations | Application + stable job name + window key, fixed first-admission expiry, request fingerprint |

New command IDs do not bypass a fixed-window conflict. Conversely, a business window key does not replace the command receipt needed to resolve an uncertain Reserve/Begin/Finish. Active uniqueness is first-writer reuse, not a request-equality test.

Each command stores its fingerprint version. Each window binding stores its request-fingerprint version and digest. A newer application cannot silently reinterpret an old binding with a new normalization algorithm; it must be able to compare under that stored version or report an explicit compatibility failure.

## 2. Preparation is deterministic and freezes effective intent

Preparation resolves stable name/current payload version, serializes against its registered serializer profile, merges configuration, validates route/priority, computes resource/uniqueness keys, normalizes instants, and builds an immutable envelope. It binds the configured logical store identity/application and assigns operation ID and NotAfter once. Re-dispatch of that prepared envelope does not inspect mutable application objects or re-run key selectors/default resolution.

Pure key selectors and version upgrades are registered executable code, never persisted delegates. Snapshot serialized bytes, not a reference to a caller-owned mutable payload. Validate input byte/depth/string/collection bounds before hashing and again on imported envelopes. The exact supported type/number bounds are published per serializer profile.

Canonicalize before fingerprinting. Times and duration policies obey the [driver matrix's millisecond quantum](driver-capabilities.md#6-shared-encodings-precision-and-restore-boundaries). NotBefore omitted is encoded as `{mode: immediate}`. Explicit NotBefore is `{mode: instant, utcEpochMilliseconds: value}` and retains that normalized requested instant even if it is already in the past. Actual eligible_at can use max(requested, storageNow) without changing request identity.

This avoids a different hash on every immediate retry while preserving a caller's explicit scheduling intent. A deadline generated at preparation is also frozen. A worker/producer clock skew may cause deadline rejection; it must not make an expired operation apply again.

## 3. Fixed-window request fingerprint v1

Hash the following effective intent, not only the CLR payload:

| Included | Reason |
|---|---|
| Envelope/fingerprint and serializer profile versions | Make interpretation explicit |
| Application and stable job name | Scope identity |
| Original payload version and canonical serialized payload | Detect changed data/contract |
| Resolved queue, priority and requested eligibility mode/instant | Routing/timing changes are meaningful |
| Effective MaxExecutions, timeout, retry evaluator/version/jitter/settings, abandonment guard and unstarted-recovery delay policy | Policy digest only (R3): drift is reported, not a conflict |
| Resolved stable concurrency/rate scope membership and resource keys | Prevent a retry silently changing required admission scopes |
| Active/window key values and requested window duration | Prevent changing the combined uniqueness request |
| Declared business metadata | Tenant/resource or other execution-relevant metadata cannot drift unnoticed |

Exclude command ID/deadline, proposed new job/attempt IDs, storage decision time, actual admission timestamp, and the worker that eventually runs the job. Exclude live ceiling/rate values and operational policy revisions: membership is fixed intent, but shared capacity remains live authority. Use stable scope kind/key identities rather than provider-generated row IDs in the cross-driver request encoding.

Exclude only explicitly classified diagnostic trace metadata, such as traceparent/tracestate, from business comparison. Do not classify arbitrary headers or baggage as diagnostic merely because they are called metadata. If a handler uses a field to choose business behavior, it belongs in payload or business metadata and is included. A duplicate does not replace the original job's trace/business fields.

Accepted 2026-10-30 (R3): the window fingerprint is **two digests**, both versioned and stored on the binding:

| Digest | Inputs | Role |
|---|---|---|
| **Business digest** | Envelope/fingerprint and serializer profile versions; application; stable job name; original payload version and canonical payload; resolved queue, priority and eligibility mode/instant; resolved concurrency/rate scope membership and resource keys; active/window key values and window; declared business metadata | Decides reuse versus `UniquenessConflict` |
| **Policy digest** | Effective MaxExecutions, timeout, retry evaluator/version/jitter/settings, abandonment guard, unstarted-recovery policy | Diagnostic only |

A matching business digest returns the original job as `Reused`. If the policy digest differs, the receipt additionally reports `PolicyDrift = true` and the driver records a bounded event; it is never a conflict. The original job keeps the generation policy it was created with. Rationale: resolved *defaults* are part of the policy digest, and a routine library upgrade or configuration change that alters a default must not turn every in-window idempotent re-dispatch into an outage. Changing what the job *is* or *where it runs* remains a conflict; changing how many times it may retry does not.

Reusing PreparedDispatch preserves the earlier settings; applications wanting new behavior use a new business request key.

The fingerprint is computed on submitted pre-upgrade payload intent. A v1 payload and a v2 payload that upgrade to equivalent values are not automatically the same request. Do not execute user upgrade code merely to decide producer idempotency.

## 4. Canonical value encoding

Propose a small, versioned typed encoding for hashing, distinct from the JSON storage format. It is not claimed to implement a separate standardized JSON canonicalization scheme.

```text
tag 0: null
tag 1: false
tag 2: true
tag 3: string  -> uint32 big-endian byte length + UTF-8 bytes
tag 4: number  -> uint32 big-endian byte length + validated ASCII JSON number lexeme
tag 5: array   -> uint32 big-endian count + encoded elements in order
tag 6: object  -> uint32 big-endian property count
                 + sorted (encoded string name, encoded value) pairs
```

Object names sort lexicographically by their decoded UTF-8 bytes. Reject duplicate decoded property names, including names written with different JSON escapes. Strings are decoded before encoding, so `"a"` and `"\u0061"` are equal. Reject malformed UTF-8/unpaired surrogate input; do not silently replace it. No Unicode normalization or case folding is performed. Whitespace and object-property order do not affect the hash; array order and null-versus-absent do.

Numbers preserve their validated JSON lexeme: `1`, `1.0`, and `1e0` differ in v1, as do `0` and `-0`. This avoids lossy floating-point conversion during hashing but is intentionally strict. The registered serializer/profile must emit stable number spellings for a prepared request. NaN/Infinity or non-JSON forms are rejected. Bound numeric lexeme length and the accepted payload type ranges.

Schema-generated values (versions, counters, milliseconds) use invariant base-10 integer spelling without leading zeroes or a plus sign. Ordinary payload numeric lexemes follow the declared serializer profile. C# decimal/double equivalence is not inferred. If a later fingerprint version normalizes numeric equivalence differently, it requires an explicit compatibility path.

Use `SHA-256(UTF8(domain) || 0x00 || Encode(envelope))` with distinct fixed domains `runwick.dispatch-request.v1` and `runwick.command.v1`. Length/count bounds prevent ambiguous concatenation. Store the algorithm/version alongside the digest; do not rely on GetHashCode, process-random hashes, or a database collation. The digest is an identity comparison aid, not authentication or authorization.

Persist the original fingerprint; do not recompute it from PostgreSQL jsonb or another provider's normalized rendering of payload JSON. Prepared-envelope export preserves the original UTF-8 bytes and schema/profile versions. Driver storage must preserve those bytes exactly (PostgreSQL bytea, SQLite BLOB, Redis binary-safe value); derived query projections cannot replace the execution/replay source.

## 5. Mutation command fingerprint

The command envelope includes operation name and semantic version, logical store identity, application, operation ID, NotAfter, and the full canonical command body. Store identity is bound by the provider envelope even though the initial StoreCommand record relies on its configured store instance. For dispatch, the body includes the frozen execution intent above **plus** diagnostic metadata and any other supplied argument omitted from business comparison. Command replay requires the same complete canonical arguments even where business deduplication would tolerate a difference.

Equivalent encodings after canonicalization are the same arguments; changed normalized deadline, policy or metadata is CommandConflict. The command receipt stores the original decision/result. A repeated command never recalculates retry jitter, changes key expiry, creates another generation, or replaces the original profile confirmation with an unverified claim.

Provider-local plan metadata may have its own internal encoding, but it must be covered by the appropriate command/version fingerprint. A Redis recovery intent cannot accept an altered projection plan under the original command ID.

## 6. Binding, reuse, and retention semantics

Create a fixed-window binding and record its fingerprint/expiry in the same atomic operation as job admission or explicitly permitted aliasing. Expiry is based on the storage decision instant plus window duration, not completion and not duplicate access. A caller-owned transaction can spend part of the window before commit; the API does not promise a fresh full window after commit.

On a live binding, compare using its stored fingerprint version. Same digest returns its original job ID; incompatible intent returns an idempotency conflict without replacing the payload. Keep the binding independently of detailed job history. On expiry, a new request can create a new job unless active uniqueness still prevents it.

If a new window key meets an existing active binding, the combined-policy proposal aliases it to that active job and stores the **incoming request's fingerprint**. Its payload may differ from the existing job because active uniqueness intentionally means first-writer reuse. The receipt must identify Active reuse; it must not imply the incoming payload will be executed. Subsequent retries of that window key compare against the incoming intent that established the alias.

When existing live active and window bindings identify different jobs, return the documented composition conflict rather than rewrite either. Manual retry reacquires active uniqueness but does not extend a fixed window. Schedule cross-occurrence reuse additionally requires explicit schedule opt-in.

## 7. Export/import and recovery boundaries

PreparedDispatch export uses a versioned bounded JSON envelope containing base64 original payload bytes, effective intent, operation identity/deadline, and serializer/fingerprint versions. Decode the bytes and construct the defined logical body before canonical hashing; the base64 transport spelling is not an additional fingerprint input. Import validates all fields and recomputes the digest; an envelope is not trusted merely because it contains a hash. It cannot select arbitrary CLR types, a different configured store, privileged application namespace, or bypass effective mandatory policy validation.

Imported historical prepared policy is checked for compatibility and mandatory constraints, not silently overwritten with new defaults. After validating access, envelope integrity and store identity, check for an existing matching receipt before deciding whether a previously unapplied operation may still run under current mandatory constraints. If the host cannot safely honor a new application, fail explicitly and allow receipt resolution. The same operation's already-committed receipt remains historical truth; rejecting a new application cannot erase it.

The envelope contains business data and is not automatically encrypted by this format. Keep it out of ordinary logging/error strings. Cross-process durable preparation/outbox storage needs application access controls. A caller preserving only the ID can query a receipt but cannot necessarily reconstruct immutable arguments after that receipt expires.

No new fingerprint rule repairs a queue history lost by restore or lossy failover. The acknowledgement and restore profiles remain those in the driver matrix.

## 8. Required compatibility vectors

Before freezing v1, publish byte-level inputs, canonical bytes and digests for:

- Object order and insignificant whitespace equivalence; array-order differences.
- Escaped/unescaped equal strings, duplicate decoded keys, malformed Unicode, and composed/decomposed Unicode differences.
- `1`/`1.0`/`1e0`, negative zero, large integers/decimals, maximum lexeme bounds and invalid numeric forms.
- Missing/null/default policy fields; immediate eligibility versus an explicit past/future instant.
- Equal instants with different UTC offsets and millisecond boundary rounding.
- Diagnostic trace changes versus tenant/business metadata changes.
- Same caller payload with changed effective route/budget/timeout/scope membership.
- Unchanged prepared request after defaults or serializer deployment changes.
- Window-key replay with an older stored fingerprint version and history retention.
- Active aliasing of different payloads, conflicting active/window bindings, and transaction rollback.
- Imported envelopes with altered app/store/versions, expired deadlines, and operation-body mismatch.

Generate independent implementations/vectors before claiming cross-driver equivalence. The identity/serialization decision packet recommends the typed encoding, strict numeric comparison, exported-envelope shape and immutable profile lifecycle; architecture review and executable compatibility evidence remain required.

## Outbox wire extension

[ReceiveOutbox/1](transactional-outbox.md) uses a distinct versioned envelope; do not reinterpret a PreparedDispatch with a replacement ID. Its domain-separated delivery fingerprint (`runwick.outbox.delivery/1`) covers the canonical destination store/application/profile, source UUID/message ID, absolute AcceptUntil and frozen execution/diagnostic intent. Reuse this document's canonical primitive/byte encodings, not runtime object hashes. The transport fingerprint (`runwick.outbox.command/1`) additionally covers operation/version, command ID and NotAfter. Both codec and fingerprint versions are explicit.

Freeze the source intent once. Diagnostic fields are included in delivery identity equality even though ordinary business-window equality can exclude them. Renewing a transport attempt changes only its ID/deadline and transport fingerprint; never reserialize the payload, slide AcceptUntil or rerun mutable key selectors. Add cross-driver vectors for same delivery/new transport, changed cutoff, changed destination/profile/metadata, expired transport with live delivery mapping, and cleanup racing a delayed original request.
