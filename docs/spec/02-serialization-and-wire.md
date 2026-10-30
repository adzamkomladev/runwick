# 02 — Serialization, canonical encoding and dispatch wire contract

**Sources:** docs/architecture/identity-and-serialization.md, docs/architecture/dispatch-wire-contract.md (primary), docs/review-2026-10-30.md (R1, R3, batch dispatch), docs/architecture/public-api-contract.md §5/§6/§7/§11, docs/uniqueness-and-execution.md, docs/architecture/driver-contract-suite.md (SER/ENC/UNI/CMD cases), docs/architecture/capacity-defaults-and-budgets.md §6 (envelope budget), docs/architecture/storage-command-protocol.md §2/§4 (command records and fingerprint inputs), docs/architecture/driver-capabilities.md §6 (millisecond quantum), docs/architecture/postgres-storage-design.md (`dispatch_keys`, `job_payloads`), docs/architecture/execution-defaults-and-timing.md §2 (policy defaults), docs/architecture/management-retention-defaults.md (command lifetimes), docs/architecture/observability-and-operations.md §2 (trace metadata).

Scope: M1+M2 (PostgreSQL execution through gate G-1). The outbox wire extension (`ReceiveOutbox/1`, `runwick.outbox.delivery/1`, `runwick.outbox.command/1`), scheduler occurrence preparation and rate-scope admission are not in M2. This file leaves room for them (the `rateScopes` field, reuse of the canonical encoder) but does not define them.

This specification is normative. "MUST", "MUST NOT" and "rejected" are binding. Every byte of every digest input is determined by this file; a C# encoder and an independent Node.js encoder written from it MUST produce identical bytes for the golden vectors in §12.

---

## Changes (2026-10-30)

### Contradictions resolved

| # | Contradiction | Resolution |
|---|---|---|
| C-1 | runwick-explained.md and the original dispatch-wire-contract §3 describe one request fingerprint that includes resolved retry/timeout/abandonment defaults. | R3 wins: two digests (business + policy), §7. Only the business digest decides reuse vs `UniquenessConflict`; a policy-digest mismatch sets `DispatchReceipt.PolicyDrift = true`. |
| C-2 | postgres-schema-and-operations.md lists `dispatch_keys` with a single "request_fingerprint_version and digest". | postgres-storage-design.md R3 shape wins: `business_fingerprint bytea`, `policy_fingerprint bytea`, `fingerprint_version integer` (§7.4). |
| C-3 | dispatch-wire-contract §3 lists "envelope/fingerprint ... versions" as separate inputs. | One integer `fingerprintVersion` (= 1) is the logical request-envelope version; the prepared-export `formatVersion` is transport-only and never a digest input (§7.1, §9). |
| C-4 | Early PostgreSQL draft stored the payload as `jsonb`. | Superseded: original UTF-8 bytes in `bytea` (`job_payloads.payload_utf8`), never passed through jsonb (§5). |
| C-5 | uniqueness-and-execution.md says canonical fingerprinting "still needs a precise specification". | Superseded by this file. Its retry delays (10 s / 5 min) are historical; policy defaults are those of execution-defaults-and-timing (5 s / 60 s; 1 s / 30 s). |
| C-6 | storage-command-protocol §4 requires a fingerprint for "every mutating command"; R1 removes Renew's receipt. | Renew computes no command fingerprint and has no receipt; it keeps a command ID for correlation only (§8.3). |
| C-7 | Capability-set bound never given. | 512 distinct tuples per ready probe / Reserve (brief item 9; §6). |

### Implementation choices (byte-level details the sources leave open)

| ID | Choice |
|---|---|
| IC-01 | Policy digest uses its own domain `runwick.dispatch-policy.v1`; business digest uses `runwick.dispatch-request.v1`; command digest uses `runwick.command.v1` (§7.3). |
| IC-02 | Domain bytes are the ASCII domain string, followed by a single `0x00`, followed by the canonical encoding. No length prefix on the domain. |
| IC-03 | Runwick-defined envelopes never omit a field: an unset optional value is encoded as `null` (tag 0). "Absent" therefore only occurs inside user payload JSON. |
| IC-04 | Schema-generated values: UUIDs as tag-3 strings, lowercase `8-4-4-4-12` hex; instants as tag-4 integer UTC Unix epoch milliseconds; durations as tag-4 integer milliseconds; enums as tag-3 strings of the declared C# member name; booleans as tags 1/2. |
| IC-05 | Field names in Runwick envelopes are fixed camelCase strings listed in this file (no suffixes such as `Ms`), except the source-given `utcEpochMilliseconds`. |
| IC-06 | Scope membership arrays are sets: deduplicated, then sorted by (kind UTF-8 bytes, key UTF-8 bytes) unsigned bytewise. Scope kind strings are `queue`, `job-type`, `resource`. |
| IC-07 | Diagnostic metadata keys in v1 are exactly `traceparent` and `tracestate`; every other metadata key is business metadata. Metadata values are strings. |
| IC-08 | Metadata "16 KiB encoded" = Σ over all business + diagnostic entries of (UTF-8 bytes of key + UTF-8 bytes of value) ≤ 16,384; ≤ 32 entries combined. |
| IC-09 | Proposed job ID (`Guid.CreateVersion7()`) is assigned at Prepare, carried in the prepared envelope and included in the command body (not in the business digest). |
| IC-10 | Dispatch operation names: `Dispatch` version 1 (single, also used by `StageAsync`) and `DispatchMany` version 1 (batch, also used by `StageManyAsync`). Batch ordinals are 0-based. |
| IC-11 | Prepared export envelope: JSON object with `format` = `runwick.prepared-dispatch` / `runwick.prepared-batch`, `formatVersion` = 1, fixed member order on write, any order accepted on read, ECMAScript `JSON.stringify` string escaping, and hex digests included for integrity checking (recomputed and compared on import). |
| IC-12 | Base64 is RFC 4648 §4 standard alphabet, `=` padding required, no line breaks/whitespace; canonical = re-encoding the decoded bytes reproduces the input exactly. |
| IC-13 | Payload byte stream: no UTF-8 BOM accepted; JSON whitespace is only U+0020, U+0009, U+000A, U+000D. |
| IC-14 | Instant rounding to the 1 ms quantum uses floor/ceiling on the mathematical value (toward −∞ / +∞): NotBefore and delays/elapsed round up; NotAfter and expiries round down. Configured durations (window, timeouts, retry delays, lease) that are not whole milliseconds are rejected, not rounded. |
| IC-15 | Uniqueness window bound: 1 ms ≤ window ≤ 315,360,000,000 ms (3,650 days). |
| IC-16 | `runwick.json` v1 enum handling: `JsonStringEnumConverter` with no naming policy and integer values disallowed; `[Flags]` enums are rejected at registration. Polymorphic discriminator property is `$type`, values are strings under the identifier grammar. |
| IC-17 | Stable error codes for serialization/import failures (§11). |
| IC-18 | Reserve's `supportedContracts` are deduplicated and sorted (jobName bytes, payloadVersion numeric, profileId bytes, profileVersion numeric) before fingerprinting/submission. |
| IC-19 | `JavaScriptEncoder.Default` is the profile's output escaper. Stored bytes are whatever the profile emits; digests are escape-independent. |
| IC-20 | Prepare rejects a request whose export rendering (§9) would exceed 2 MiB, so every PreparedDispatch/PreparedBatch is exportable. |

---

## 1. Identity grammar

| Field | Rule |
|---|---|
| Application, queue, schedule, pool, serializer-profile names, metadata keys | 1–128 ASCII chars; first char `[A-Za-z0-9]`; remaining `[A-Za-z0-9._-]`. Regex: `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` |
| Job name | Same grammar, 1–200 chars: `^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$` |
| Priority | One of `low`, `medium`, `high` (persisted strings; no custom labels in v1) |
| Payload version, serializer profile version | Positive signed 32-bit integer (1 … 2,147,483,647); no downgrade inference |
| Uniqueness keys (active, window) and resource scope keys | 1–512 UTF-8 bytes; valid Unicode scalar values only; no code point in Unicode category Cc (U+0000–U+001F, U+007F–U+009F); opaque, case-sensitive, never normalized or case-folded |
| Job / attempt / command / operation IDs | UUID; job, attempt and command IDs generated with `Guid.CreateVersion7()`; ownership tokens random (`RandomNumberGenerator`) |

All names compare ordinally and case-sensitively (UTF-8 bytewise). Leading/trailing spaces are rejected (the grammar already excludes spaces), never trimmed. CLR namespace/type/assembly names never become a durable identity; every durable contract has an explicit stable job name (`[JobName]` or fluent `Named`). Renaming a CLR type keeps the registered name. Changing the stable name creates a different contract; no automatic aliases.

## 2. Decoding capability tuple

The decoding capability is the 4-tuple:

```text
(jobName, payloadVersion, serializerProfileId, serializerProfileVersion)
```

```csharp
internal sealed record JobContract(
    string Name,
    int PayloadVersion,
    string SerializerProfileId,
    int SerializerProfileVersion);
```

Rules:

1. One handler registration per stable job name selects the current payload CLR type and its adjacent upgrade chain (`.Upgrade<TFrom,TTo>(fromVersion, toVersion, fn)`). Gaps, cycles, duplicate routes and duplicate types fail startup. No downgrade path is implied.
2. A worker advertises a tuple only if it has an installed decoder for (payloadVersion, profile) **and** a complete upgrade path to its current handler input.
3. Profile ID/version are stored on the jobs readiness row, `job_payloads`, worker presence/capability rows and the `ReservedJob` contract. Ready probes and Reserve filter on the **complete** tuple. A job whose tuple is unsupported is never reserved (never discovered only after Begin) and burns no budget.
4. Per probe / Reserve request at most **512** distinct tuples; duplicates are removed before submission; an empty set reserves nothing.
5. Supported-but-malformed payload data is a permanent payload failure after Begin through normal Finish accounting.
6. Presence and queue subscriptions explain unsupported backlog; they never authorize ownership.
7. Profile identity is a stable application contract, not an assembly or SDK version. An existing (profile ID, version) is never rebound to incompatible behavior.

## 3. The `runwick.json` v1 serializer profile

Profile ID `runwick.json`, profile version `1`. Implemented with System.Text.Json on the .NET 10 SDK. Runwick registers its own immutable `JsonSerializerOptions`; it never inherits ASP.NET `JsonOptions`, `JsonSerializerOptions.Default/Web`, or any process-global serializer. Every option is set explicitly so SDK default changes cannot redefine persisted behavior. After configuration the options are finalized with `MakeReadOnly()`.

### 3.1 Option values (complete)

| `JsonSerializerOptions` member | v1 value |
|---|---|
| `AllowDuplicateProperties` | `false` |
| `AllowOutOfOrderMetadataProperties` | `false` |
| `AllowTrailingCommas` | `false` |
| `ReadCommentHandling` | `JsonCommentHandling.Disallow` |
| `NumberHandling` | `JsonNumberHandling.Strict` (no quoted numbers, no named `NaN`/`Infinity`) |
| `PropertyNamingPolicy` | `JsonNamingPolicy.CamelCase` |
| `DictionaryKeyPolicy` | `null` (dictionary keys are data, emitted verbatim) |
| `PropertyNameCaseInsensitive` | `false` |
| `UnmappedMemberHandling` | `JsonUnmappedMemberHandling.Disallow` |
| `RespectNullableAnnotations` | `true` |
| `RespectRequiredConstructorParameters` | `true` |
| `DefaultIgnoreCondition` | `JsonIgnoreCondition.Never` (nulls/defaults are emitted) |
| `IgnoreReadOnlyProperties` | `false` |
| `IgnoreReadOnlyFields` | `true` |
| `IncludeFields` | `false` |
| `ReferenceHandler` | `null` (no `$id`/`$ref`; cycles fail via depth) |
| `MaxDepth` | `32` |
| `WriteIndented` | `false` |
| `IndentCharacter` / `IndentSize` / `NewLine` | `' '` / `2` / `"\n"` (no effect while unindented; set for determinism) |
| `Encoder` | `JavaScriptEncoder.Default` (IC-19) |
| `UnknownTypeHandling` | `JsonUnknownTypeHandling.JsonElement` (members typed `object` are nevertheless rejected at registration, §3.2) |
| `PreferredObjectCreationHandling` | `JsonObjectCreationHandling.Replace` |
| `DefaultBufferSize` | `16384` |
| `TypeInfoResolver` | Runwick's explicit registration resolver: source-generated `JsonTypeInfo` where supplied, otherwise a bounded registration-time reflection path for the registered types only |
| `Converters` | exactly: `JsonStringEnumConverter(namingPolicy: null, allowIntegerValues: false)`; Runwick `UtcDateTimeConverter` (§3.3) |

### 3.2 Representation rules

| Concern | v1 behavior |
|---|---|
| Root | Non-null JSON object for a registered job contract. Any other root (array, scalar, `null`) is rejected. |
| Property naming | camelCase by default; an explicit `[JsonPropertyName]` is allowed and becomes part of the payload schema. |
| Binding | Case-sensitive; duplicate decoded property names rejected at every object level (including names differing only in JSON escapes). |
| Missing / unknown members | Required members and required constructor parameters enforced; unmapped JSON properties for the declared historical DTO rejected. |
| Nulls / defaults | Emitted null/default values preserved; never omitted by global options. A member explicitly marked `[JsonIgnore]` is a schema decision. |
| Numbers | Strict JSON numbers. The emitted numeric token spelling is retained as the hashing lexeme (`1`, `1.0`, `1e0` differ). Non-finite floating values are rejected on write. |
| Structure | No comments, trailing commas, reference metadata or cycles. |
| Fields/properties | Public serializable properties only; no automatic private or public field serialization. |
| Enums | Declared C# member names as strings; integer values rejected on read. `[Flags]` enums rejected at registration (IC-16). Any other representation requires a different named/versioned profile. |
| Polymorphism | Only explicitly registered closed hierarchies (`[JsonPolymorphic]`/`[JsonDerivedType]` or fluent equivalent) with stable string discriminators under the identifier grammar; discriminator property `$type`; `UnknownDerivedTypeHandling = FailSerialization`; `IgnoreUnrecognizedTypeDiscriminators = false`. Unknown discriminator or unregistered runtime subtype fails. Never resolves arbitrary CLR types. |
| Unstructured data | `JsonElement` allowed within the structural bounds. Members typed `object`, `dynamic`, or an interface without a registered representation are rejected at registration. |
| Binary | `byte[]`/`ReadOnlyMemory<byte>` as standard base64 JSON strings (STJ default); the encoded characters count toward payload size. |
| GUID, DateOnly, TimeOnly, DateTimeOffset, TimeSpan | Ordinary STJ representations (GUID `"D"` format; `DateOnly` `yyyy-MM-dd`; `TimeOnly` ISO-8601 time; `DateTimeOffset` ISO-8601 with offset preserved; `TimeSpan` constant `"c"` format). No queue-timestamp rounding is applied to payload data. `DateTimeOffset` is the recommended instant type. |
| `DateTime` | Fixed `UtcDateTimeConverter` (§3.3). |
| Strings that look like dates | Never normalized. |

Registration-time metadata validation rejects any type/member attribute that relaxes the profile: `[JsonNumberHandling]` other than `Strict`, `[JsonUnmappedMemberHandling(Skip)]`, `[JsonExtensionData]`, `AllowDuplicateProperties`-relaxing contract customization, `[JsonConverter]` targeting `object`, and custom converters not registered with Runwick. Custom converters are executable application configuration, never loaded from payload data; their output passes the same scanner (§4.2). An application needing other semantics registers a differently named/versioned profile with its own vectors.

Required-member and nullability checks are distinct; STJ nullable enforcement has known gaps (generic/collection elements). It is not a domain validator. Worker validation middleware owns business invariants.

### 3.3 `UtcDateTimeConverter`

- Write: only `DateTimeKind.Utc`; writes `value.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFZ", CultureInfo.InvariantCulture)` (trailing fractional zeros and the dot trimmed by the `F` specifier). `Local` or `Unspecified` kind fails serialization (`payload_invalid_value`).
- Read: accepts only a string matching `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,7})?Z$`; returns `DateTimeKind.Utc`. Offsets other than `Z`, missing designator, or other forms are rejected.

## 4. Payload bounds and the structural scanner

### 4.1 Limits (profile v1)

A host may impose **smaller** admission limits. Raising a structural bound requires a new profile version and compatible workers.

| Limit | Value | Exact counting rule | Boundary |
|---|---|---|---|
| Serialized payload | 1 MiB | Byte length of the stored UTF-8 payload bytes | 1,048,576 accepted; 1,048,577 rejected |
| Nesting depth | 32 | Each object/array is a container; the root object is depth 1; a container opened inside a depth-*d* container is depth *d*+1. Scalars do not add depth. | Container at depth 32 accepted; opening a container at depth 33 rejected |
| Decoded string value | 256 KiB | UTF-8 byte length after unescaping, for string **values** (not property names) | 262,144 accepted; 262,145 rejected |
| Decoded property name | 256 bytes | UTF-8 byte length after unescaping; the empty name `""` is permitted | 256 accepted; 257 rejected |
| Properties per object | 256 | Members of one object (after rejecting duplicates) | 256 accepted; 257 rejected |
| Elements per array | 16,384 | Elements of one array | 16,384 accepted; 16,385 rejected |
| Numeric token | 128 bytes | ASCII byte length of the lexeme including sign, `.`, exponent marker and exponent sign | 128 accepted; 129 rejected. Still subject to the declared CLR type range on decode. |
| Metadata (business + diagnostic) | 16 KiB, 32 entries | Σ (UTF-8 bytes of key + UTF-8 bytes of value) over all entries ≤ 16,384; entry count ≤ 32 (IC-08) | 16,384 / 32 accepted; 16,385 / 33 rejected |
| Metadata key | 128 chars | Identifier grammar (§1) | — |
| Prepared export envelope | 2 MiB | Byte length of the UTF-8 export JSON (§9) including base64 payload and every policy/key/metadata field; batch: the whole batch envelope | 2,097,152 accepted; 2,097,153 rejected |
| Batch | 100 items | Items in one `DispatchMany` | 100 accepted; 101 rejected |

### 4.2 Scanner (normative, run before hashing and again on import)

Prepare serializes into a bounded buffer and aborts as soon as the 1 MiB limit is exceeded (no unbounded intermediate). The scanner then validates, in a single pass over the bytes:

1. **Encoding.** Bytes MUST be well-formed UTF-8 (RFC 3629): no overlong forms, no encoded surrogates U+D800–U+DFFF, nothing above U+10FFFF, no truncated sequence. A leading BOM (`EF BB BF`) is rejected (IC-13).
2. **Grammar.** RFC 8259 JSON with exactly one top-level value, which MUST be an object. Insignificant whitespace is only `0x20 0x09 0x0A 0x0D`. Literals `true`, `false`, `null` lowercase only. No comments, no trailing commas, no content after the root.
3. **Strings.** Raw U+0000–U+001F inside a string is rejected (must be escaped). Escapes: `\" \\ \/ \b \f \n \r \t \uXXXX` (hex case-insensitive). A `\uD800–\uDBFF` escape MUST be immediately followed by a `\uDC00–\uDFFF` escape and the pair decodes to one scalar value; a lone or reversed surrogate escape is rejected. Escaped control characters (e.g. `\u0000`) are valid data. No Unicode normalization and no case folding.
4. **Numbers.** Lexeme MUST match `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` and be ≤ 128 bytes. `NaN`, `Infinity`, `+1`, `01`, `.5`, `1.`, hex and quoted numbers are rejected. The lexeme is retained verbatim; it is never converted through `double`/`decimal` for hashing. A `JsonElement` may carry an integer outside Int64; a typed `long` member rejects it on decode. No driver clamps a number.
5. **Duplicates.** Within each object, property names are compared by their **decoded** UTF-8 bytes; any repeat is rejected (`{"a":1,"a":2}` is a duplicate).
6. **Structural bounds** of §4.1 per container/token.

Implementations keep only bounded per-object name sets and sorting buffers and hash incrementally after bounded canonical ordering; they do not build an unbounded second object graph. Custom-converter output goes through the same scanner.

## 5. Stored payload bytes

- The execution source in every driver is the **immutable original UTF-8 bytes** produced by the profile at Prepare: PostgreSQL `job_payloads.payload_utf8 bytea` with `payload_encoding_version = 1` (1 = raw UTF-8 JSON bytes), SQLite BLOB, Redis binary-safe string (SQLite/Redis not in M2).
- `serializer_profile_id text` and `serializer_profile_version integer` are stored with `payload_version` in `job_payloads` and on the jobs readiness row, inserted in the same transaction, identical values (driver conformance enforces equality).
- Payload bytes never pass through `jsonb`; no jsonb shadow is maintained. The dashboard decodes authorized projections on demand. Any future indexed projection is derived and never the source for execution, retry, export or fingerprints.
- Manual retry and upgrades retain the original bytes/profile/version. A worker decodes the historical DTO under the stored profile, runs the registered deterministic upgrade chain and hands the current DTO to the handler; the snapshot and the stored fingerprints are never rewritten.
- Business/diagnostic metadata is stored bounded in `job_payloads.headers` (typed representation; never credentials or serialized CLR types).
- Profile removal requires draining relevant backlog and respecting replay horizons; deleting a decoder is not a migration.

## 6. Configuration inputs frozen by Prepare

Prepare: resolves stable name and current payload version; serializes with the registered profile; merges configuration (library defaults → application defaults → job attributes → fluent settings → permitted dispatch overrides; conflicting same-layer declarations fail startup); validates queue/priority; computes active/window/resource keys via registered deterministic selectors (explicit and derived keys must agree); normalizes instants; resolves scope membership; binds store ID and application; assigns operation ID (`Guid.CreateVersion7()`), proposed job ID (IC-09) and NotAfter (default and maximum 5 min after the local clock, rounded down to the ms); computes the three digests; and freezes the result. Re-dispatching a PreparedDispatch never re-reads the caller's object, re-runs selectors or re-resolves defaults. The caller must not mutate the payload concurrently with Prepare; later mutations cannot affect the frozen request. PreparedDispatch never exposes the payload in `ToString()`/logging.

Instant/duration normalization (1 ms quantum, IC-14):

| Value | Conversion |
|---|---|
| NotBefore (`DateTimeOffset`) | `ceil((UtcTicks − 621355968000000000) / 10000)` → epoch ms (signed 64-bit) |
| NotAfter | `floor(...)` of the same expression |
| Configured durations (window, execution timeout, retry initial/maximum delay, lease) | `Ticks % 10000` MUST be 0, else rejected; value = `Ticks / 10000` ms; MUST be positive |
| Execution elapsed, requested retry delay (Finish body) | `ceil(Ticks / 10000)` ms; negative requested delay is a middleware contract fault |

Omitted NotBefore is `{"mode":"immediate"}`. Explicit NotBefore is `{"mode":"instant","utcEpochMilliseconds":n}` and keeps that normalized value even when already in the past (actual `eligible_at` = max(requested, storage now) does not change identity).

## 7. Request fingerprints (R3: business digest + policy digest)

### 7.1 Business request envelope (`runwick.dispatch-request.v1`)

A canonical object (§8) with exactly these members (encoder sorts them; listed here alphabetically by UTF-8 bytes, which is their encoded order):

| Member | Canonical value |
|---|---|
| `application` | string |
| `businessMetadata` | object: business metadata key → string value (may be `{}`) |
| `concurrencyScopes` | array of `{"key": string, "kind": string}` — resolved concurrency scope membership, deduplicated and sorted (IC-06); `kind` ∈ `queue`, `job-type`, `resource` |
| `eligibility` | `{"mode":"immediate"}` or `{"mode":"instant","utcEpochMilliseconds": integer}` |
| `fingerprintVersion` | integer `1` |
| `jobName` | string |
| `payload` | the canonical value of the stored payload bytes (root object) |
| `payloadVersion` | integer |
| `priority` | string (`low`/`medium`/`high`) |
| `queue` | string (resolved, after permitted override/routing) |
| `rateScopes` | array of `{"key","kind"}`, same rules; always `[]` in M2 (rate-enabled applications are refused) |
| `serializerProfileId` | string |
| `serializerProfileVersion` | integer |
| `uniqueness` | `{"activeKey": string\|null, "window": integer ms\|null, "windowKey": string\|null}`; `window` non-null iff `windowKey` non-null |

Inputs deliberately **excluded**: command/operation ID and NotAfter; proposed job ID and attempt IDs; storage decision time and actual admission timestamp; the worker that eventually runs the job; live scope capacities/ceilings, rate limit values and operational policy revisions (membership is intent, capacity is live authority); provider-generated scope row IDs (stable kind/key are used instead); **diagnostic metadata** (`traceparent`, `tracestate`); and all execution-policy fields (they go to the policy digest).

Consequence (intended): adding/removing a queue `MaxConcurrent`, a job-type `MaxConcurrent` or a `PerResource` scope changes membership and therefore the business digest of fresh in-window requests.

### 7.2 Policy envelope (`runwick.dispatch-policy.v1`)

| Member | Canonical value |
|---|---|
| `executionTimeout` | integer ms, or `null` when no timeout (default) |
| `fingerprintVersion` | integer `1` |
| `maxConsecutiveUnstartedAbandonments` | integer (default 5) |
| `maxExecutions` | integer (default 3) |
| `retry` | retry object (below); default exponential/1/equal/5000/60000 |
| `unstartedRetry` | retry object; default exponential/1/equal/1000/30000 |

Retry object: `{"backoff": string, "evaluatorVersion": integer, "initialDelay": integer ms, "jitterKind": string, "maximumDelay": integer ms}`; v1 values `backoff = "exponential"`, `evaluatorVersion = 1`, `jitterKind = "equal"`. These mirror `RetryPolicySnapshot(Backoff, EvaluatorVersion, InitialDelay, MaximumDelay, JitterKind)`.

All values are the **effective resolved** values including library/application defaults; no field is ever absent.

### 7.3 Digest function

```text
digest(domain, value) = SHA-256( ASCII(domain) || 0x00 || Encode(value) )
```

| Digest | Domain string | Envelope |
|---|---|---|
| Business | `runwick.dispatch-request.v1` | §7.1 |
| Policy | `runwick.dispatch-policy.v1` (IC-01) | §7.2 |
| Command | `runwick.command.v1` | §8.3 |

Digests are 32 raw bytes in storage (`bytea`), lowercase hex (64 chars) in text/JSON. The algorithm/version is stored next to every digest (`fingerprint_version = 1`). Never use `GetHashCode`, process-random hashes or database collations. A digest is an identity comparison aid, not authentication or authorization. Fingerprints are computed on submitted **pre-upgrade** intent; a v1 and a v2 payload that upgrade to equal values are different requests. User upgrade code is never run to decide idempotency.

### 7.4 Binding comparison

`dispatch_keys(application, job_name, key_kind, key)` stores `bound_job_id`, `business_fingerprint bytea`, `policy_fingerprint bytea`, `fingerprint_version integer`, `expires_at`, `updated_at`. Bindings and fingerprints are written in the same transaction as job admission (or permitted aliasing). Window expiry = storage decision instant + window; not completion, not duplicate access.

| Live window binding vs incoming request | Result |
|---|---|
| `fingerprint_version` not computable by this host (≠ 1) | `UnsupportedVersion`, nothing written |
| Business digest equal, policy digest equal | `Reused`, `PolicyDrift = false` |
| Business digest equal, policy digest different | `Reused`, `PolicyDrift = true`; a bounded `policy_drift` `queue_events` row is written; original generation policy retained; never a conflict |
| Business digest different | `UniquenessConflict` (window fingerprint mismatch); nothing replaced |
| Active binding only | First-writer reuse (`DuplicateMatch.Active`) — no fingerprint comparison; receipt never implies the incoming payload executes |
| New window key meets a live active binding | Alias to the active job; the window binding stores the **incoming** request's business and policy digests; later retries of that window key compare against them |
| Live active and window bindings identify different jobs | Composition conflict; neither rewritten |

### 7.5 Situation table

| Situation | Result |
|---|---|
| Same PreparedDispatch after local defaults change | Same operation/body/deadline; replay without re-running defaults or selectors |
| Fresh request, same window key, changed queue, priority, eligibility, payload, payload version, profile, scope membership, uniqueness keys/window or business metadata | `UniquenessConflict` while the binding is live |
| Fresh request, same window key, only execution policy changed (budget, timeout, retry/abandonment settings, including changed defaults) | `Reused` with `PolicyDrift = true` |
| Fresh operation with only diagnostic trace changes | Reuses the business window; never replaces original trace/payload |
| Same command ID with changed diagnostic metadata | `CommandConflict` (command digest covers full arguments) |
| New profile for same payload schema | New capability tuple; business digest differs |
| Unsupported historical codec/fingerprint version | Explicit compatibility failure; ID-only `ResolveAsync` remains available under authorization |

## 8. Canonical value encoding (v1)

### 8.1 Byte layout

Every value is a 1-byte tag followed by its body. All length/count fields are **unsigned 32-bit big-endian**.

| Tag | Type | Body |
|---|---|---|
| `0x00` | null | none |
| `0x01` | false | none |
| `0x02` | true | none |
| `0x03` | string | uint32 BE byte length *n*, then *n* bytes of UTF-8 of the **decoded** string |
| `0x04` | number | uint32 BE byte length *n*, then *n* ASCII bytes of the validated JSON number lexeme |
| `0x05` | array | uint32 BE element count *c*, then *c* encoded values in array order |
| `0x06` | object | uint32 BE member count *c*, then *c* pairs (`Encode(name as tag-3 string)`, `Encode(value)`) sorted by name |

Rules:

- **Object member order:** ascending unsigned lexicographic comparison of the decoded UTF-8 name bytes; when one name is a prefix of another the shorter sorts first. (C#: `MemoryExtensions.SequenceCompareTo` over UTF-8 bytes; Node: `Buffer.compare`.) Never UTF-16 ordinal order, culture order or database collation. Duplicate names are rejected before encoding.
- **Strings:** decoded before encoding, so `"a"` ≡ `"a"`. No Unicode normalization (`"é"` U+00E9 ≠ `"é"`), no case folding. Malformed UTF-8 or unpaired surrogates are rejected, never replaced.
- **Numbers:** lexeme verbatim; `1`, `1.0`, `1e0`, `1E0` all differ; `0` ≠ `-0`. Schema-generated integers (versions, counters, milliseconds, ordinals) use invariant base-10 with no leading zeros, no `+`, `-` only for negatives, `0` for zero.
- **Null vs absent:** in payload JSON a member set to `null` is encoded as a member with tag 0; an absent member is simply not among the pairs — they differ. Runwick envelopes never omit members (IC-03).
- **Whitespace and payload object member order do not affect the encoding; array order does.**
- No outer framing beyond the domain prefix; length/count prefixes make concatenation unambiguous.

### 8.2 Schema-type mapping for Runwick-defined envelopes (IC-04)

| C#/logical type | Canonical |
|---|---|
| `string` | tag 3 |
| `int`, `long` | tag 4, invariant base-10 |
| `bool` | tag 1 / tag 2 |
| `Guid` | tag 3, `Guid.ToString("D")` lowercase (36 chars) |
| Instant | tag 4, UTC epoch milliseconds (§6 rounding) |
| `TimeSpan` | tag 4, integer milliseconds (§6) |
| enum | tag 3, declared member name (e.g. `"RetryableFailure"`) |
| nullable unset | tag 0 |
| list | tag 5 in the order defined for that field |
| record / map | tag 6 |

### 8.3 Command envelope (`runwick.command.v1`)

```text
{
  "application":      string,
  "body":             object (per operation),
  "commandId":        uuid string,
  "fingerprintVersion": 1,
  "notAfter":         integer epoch ms,
  "operation":        string,
  "operationVersion": integer,
  "storeId":          uuid string (store_metadata.store_id)
}
```

Command fingerprint = `digest("runwick.command.v1", envelope)`, stored in `storage_commands.fingerprint` with `fingerprint_version = 1`. Reusing a command ID with a different digest is `CommandConflict`. Equivalent encodings after canonicalization are the same arguments. A repeated command never recalculates jitter, moves a key expiry, creates another generation or replaces the original profile confirmation.

Bodies:

| Operation / version | `body` members (canonical names → source) |
|---|---|
| `Dispatch`/1 | `business` → §7.1 object; `diagnosticMetadata` → object of `traceparent`/`tracestate` present entries (may be `{}`); `policy` → §7.2 object; `proposedJobId` → uuid |
| `DispatchMany`/1 | `continueOnConflict` → bool; `items` → array in ordinal order of `{"business", "diagnosticMetadata", "ordinal" (0-based integer), "policy", "proposedJobId"}` |
| `Reserve`/1 | `after` → `{"eligibleAt": ms, "jobId": uuid}` or null; `leaseDuration` → ms; `priority`; `queue`; `supportedContracts` → array of `{"name","payloadVersion","serializerProfileId","serializerProfileVersion"}` deduplicated/sorted (IC-18), ≤ 512; `workerInstanceId` → uuid |
| `Begin`/1 | `lease` → lease object |
| `Finish`/1 (also used by `CompleteInTransactionAsync` / StagedCompletion) | `error` → `{"code","exceptionType","message","stackTrace"(string\|null)}` or null; `executionElapsed` → ms (ceil); `lease` → lease object; `outcome` → `AttemptOutcome` member name; `requestedRetryDelay` → ms (ceil) or null |
| `Recover`/1 | `expectedAttemptId`, `expectedOwnershipToken`, `jobId` → uuid |
| `ReleaseReservation`/1 | `lease` → lease object |
| `Renew`/1 | **no fingerprint, no receipt** (R1); command ID used for correlation only |
| Other receipt-backed operations (e.g. `RegisterScope`/1, cancellation/retry management) | Their request record mapped member-by-member with camelCase member names and §8.2 types |

Lease object: `{"attemptId": uuid, "generation": integer, "jobId": uuid, "ownershipToken": uuid, "workerInstanceId": uuid}`.

Attempt error text is sanitized/bounded before the command is built; the hashed text is the sanitized text.

## 9. Prepared export envelope (`runwick.prepared-dispatch` / `runwick.prepared-batch`, formatVersion 1)

A versioned, bounded, strict JSON envelope; not arbitrary CLR serialization. It contains business data and is not encrypted: keep it out of logs and error strings; durable storage of it needs application access controls.

### 9.1 Single dispatch — members, in write order

| # | Member | JSON type / value |
|---|---|---|
| 1 | `format` | `"runwick.prepared-dispatch"` |
| 2 | `formatVersion` | `1` |
| 3 | `fingerprintVersion` | `1` |
| 4 | `storeId` | uuid string |
| 5 | `application` | string |
| 6 | `operation` | `"Dispatch"` |
| 7 | `operationVersion` | `1` |
| 8 | `operationId` | uuid string (the command ID) |
| 9 | `notAfter` | integer epoch ms |
| 10 | `proposedJobId` | uuid string |
| 11 | `jobName` | string |
| 12 | `payloadVersion` | integer |
| 13 | `serializerProfileId` | string |
| 14 | `serializerProfileVersion` | integer |
| 15 | `payloadEncodingVersion` | `1` |
| 16 | `payloadBase64` | canonical base64 of the original payload bytes (IC-12) |
| 17 | `queue` | string |
| 18 | `priority` | string |
| 19 | `eligibility` | object as §7.1 (`mode` first, then `utcEpochMilliseconds`) |
| 20 | `concurrencyScopes` | array as §7.1, each `{"kind","key"}` |
| 21 | `rateScopes` | array (M2: `[]`) |
| 22 | `uniqueness` | `{"activeKey","windowKey","window"}` (nulls written as `null`) |
| 23 | `businessMetadata` | object, keys in canonical byte order |
| 24 | `diagnosticMetadata` | object, keys in canonical byte order |
| 25 | `policy` | `{"maxExecutions","executionTimeout","retry","maxConsecutiveUnstartedAbandonments","unstartedRetry"}`; retry objects `{"backoff","evaluatorVersion","jitterKind","initialDelay","maximumDelay"}` |
| 26 | `businessDigest` | 64 lowercase hex |
| 27 | `policyDigest` | 64 lowercase hex |
| 28 | `commandDigest` | 64 lowercase hex |

Writer: UTF-8, no BOM, no insignificant whitespace, members in the order above, integers in invariant base-10, strings escaped exactly as ECMAScript `JSON.stringify` does for well-formed strings (`\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, other U+0000–U+001F as `\u00xx` lowercase hex; everything else literal UTF-8, including `/`, non-ASCII, U+2028/U+2029). The export bytes are not a digest input.

### 9.2 Batch

```text
{"format":"runwick.prepared-batch","formatVersion":1,"fingerprintVersion":1,
 "storeId", "application", "operation":"DispatchMany", "operationVersion":1,
 "operationId", "notAfter", "continueOnConflict": bool,
 "items":[ { "ordinal", members 10–27 of §9.1 in that order } ... ],   // 1..100 items, ordinal order
 "commandDigest"}
```

Batch canonical form for hashing: each item's `business` and `policy` digests are computed exactly as for a single dispatch (each item may bind its own window key); the batch command digest covers the `DispatchMany`/1 body of §8.3. The whole batch is one command, one transaction, one receipt (per-ordinal results); validation is all-or-nothing before any write; ≤ 100 items; the whole batch export ≤ 2 MiB; local admission counts it as one operation holding one envelope.

### 9.3 Import validation (in order)

1. Byte length ≤ 2,097,152 before parsing, else `prepared_envelope_too_large`.
2. Strict JSON parse with §4.2 rules (UTF-8, grammar, duplicates rejected); root object.
3. `format`/`formatVersion`/`fingerprintVersion`/`operation`/`operationVersion`/`payloadEncodingVersion` supported, else `prepared_envelope_unsupported_version`.
4. Exact schema for that format version: every member present, no unknown member, correct JSON types; integers match `-?(0|[1-9][0-9]*)` within range; member order is **not** required on read; identifiers satisfy §1; metadata/uniqueness/scope rules satisfied; `window` iff `windowKey`; batch 1–100 items with ordinals 0..n−1 in order.
5. `payloadBase64` canonical (IC-12), decoded length ≤ 1 MiB; decoded bytes pass the §4.2 scanner.
6. `storeId` equals the configured store's ID (`prepared_envelope_store_mismatch`); `application` equals the configured application and the caller is authorized for it (`prepared_envelope_application_mismatch`).
7. Rebuild business, policy and command envelopes from the decoded fields; recompute all digests; any mismatch with the included hex is `prepared_envelope_digest_mismatch`. The included hash is never trusted on its own.
8. Imported historical policy is checked against current mandatory constraints, never overwritten with new defaults.
9. Dispatch: the store first looks for an existing receipt for `operationId` (returns the historical result after authorization, even if current policy would now forbid a new application); if none and `notAfter` has passed → `CommandExpired`; if none and the host can no longer honor the prepared policy → explicit policy/compatibility failure, receipt resolution still available.

Import never materializes a job DTO, runs handlers or upgrades, or resolves type names. NotObserved is never proof of rollback; ExpiredUnknown is never retried under a new ID automatically.

## 10. Metadata

- Two maps: `businessMetadata` and `diagnosticMetadata`. Keys under the §1 identifier grammar. Values: strings of Unicode scalar values excluding Cc controls.
- `diagnosticMetadata` may contain only `traceparent` (W3C version 00: `^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`, trace-id and parent-id not all zeros) and `tracestate` (≤ 512 bytes, ASCII 0x20–0x7E). `businessMetadata` MUST NOT contain those keys.
- Combined bounds per IC-08. Baggage is not propagated through stored metadata in v1; arbitrary headers are never classified as diagnostic. A field a handler uses to choose behavior belongs in payload or business metadata. Trace metadata never selects tenant, permission or execution authority.
- A duplicate (reused) dispatch never replaces the original job's trace or business fields.

## 11. Error codes (IC-17)

| Code | Condition |
|---|---|
| `payload_too_large` | > 1 MiB |
| `payload_invalid_utf8` | §4.2 step 1 / malformed escapes / lone surrogate |
| `payload_invalid_json` | grammar, BOM, trailing content, comments, trailing commas |
| `payload_root_not_object` | root not a non-null object |
| `payload_depth_exceeded` | depth > 32 |
| `payload_string_too_long` / `payload_property_name_too_long` | > 256 KiB / > 256 bytes |
| `payload_too_many_properties` / `payload_array_too_long` | > 256 / > 16,384 |
| `payload_invalid_number` / `payload_number_too_long` | grammar / > 128 bytes |
| `payload_duplicate_property` | duplicate decoded name |
| `payload_unmapped_member`, `payload_missing_required`, `payload_invalid_value` | STJ binding failures (decode) / converter rejection (e.g. non-UTC `DateTime`) |
| `metadata_invalid` / `metadata_too_large` | key/value/trace format / > 16 KiB or > 32 entries |
| `identity_invalid` | §1 grammar or uniqueness/resource key rule |
| `batch_too_large` | > 100 items |
| `prepared_envelope_too_large`, `prepared_envelope_invalid`, `prepared_envelope_unsupported_version`, `prepared_envelope_store_mismatch`, `prepared_envelope_application_mismatch`, `prepared_envelope_digest_mismatch` | §9.3 |

All Prepare/import failures are **known not applied**.

## 12. Golden vectors

Computed with two independent encoders (Python and Node.js) from this file; both agree.

### 12.1 Primitive encodings (`Encode(parse(json))`, hex)

| JSON input | Canonical bytes |
|---|---|
| `null` | `00` |
| `false` | `01` |
| `true` | `02` |
| `"a"` | `030000000161` |
| `"a"` | `030000000161` (equal to `"a"`) |
| `"é"` (é, composed) | `0300000002c3a9` |
| `"é"` (decomposed) | `030000000365cc81` (differs) |
| `1` | `040000000131` |
| `1.0` | `0400000003312e30` |
| `1e0` | `0400000003316530` |
| `-0` | `04000000022d30` |
| `[1,2]` | `0500000002040000000131040000000132` |
| `[2,1]` | `0500000002040000000132040000000131` (differs) |
| `{"b":2,"a":1}` | `0600000002030000000161040000000131030000000162040000000132` |
| `{ "a" : 1 ,\n "b" : 2 }` | identical to the previous row |
| `{}` | `0600000000` |

### 12.2 Worked dispatch example

Inputs: store `5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e5f`; application `billing`; job `send-welcome-email`; payload version 2; profile `runwick.json`/1; payload bytes (63 bytes)

```text
{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":"en"}
```

queue `emails`; priority `high`; NotBefore omitted; concurrency scopes `[{"kind":"queue","key":"emails"}]`; no rate scopes; active key `user:42`; window key `req-7f3a`; window 24 h (86,400,000 ms); business metadata `{"tenant":"acme"}`; default policy; diagnostic `traceparent` `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`; operation ID `0192f3a1-8000-7000-8000-000000000001`; proposed job ID `0192f3a1-8000-7000-8000-000000000002`; NotAfter 1793354700000 (2026-10-30T10:05:00Z).

Business envelope canonical bytes (hex, 605 bytes):

```text
060000000e030000000b6170706c69636174696f6e030000000762696c6c696e670300000010627573696e6573734d657461646174610600000001030000000674656e616e74030000000461636d650300000011636f6e63757272656e637953636f7065730500000001060000000203000000036b65790300000006656d61696c7303000000046b696e6403000000057175657565030000000b656c69676962696c697479060000000103000000046d6f64650300000009696d6d656469617465030000001266696e6765727072696e7456657273696f6e04000000013103000000076a6f624e616d65030000001273656e642d77656c636f6d652d656d61696c03000000077061796c6f6164060000000203000000066c6f63616c650300000002656e0300000006757365724964030000002430313932663361312d376334652d376232612d396431302d336535663661376238633964030000000e7061796c6f616456657273696f6e04000000013203000000087072696f72697479030000000468696768030000000571756575650300000006656d61696c73030000000a7261746553636f7065730500000000030000001373657269616c697a657250726f66696c654964030000000c72756e7769636b2e6a736f6e030000001873657269616c697a657250726f66696c6556657273696f6e040000000131030000000a756e697175656e657373060000000303000000096163746976654b65790300000007757365723a3432030000000677696e646f7704000000083836343030303030030000000977696e646f774b657903000000087265712d37663361
```

Policy envelope canonical bytes (hex, 431 bytes):

```text
06000000060300000010657865637574696f6e54696d656f757400030000001266696e6765727072696e7456657273696f6e04000000013103000000236d6178436f6e7365637574697665556e737461727465644162616e646f6e6d656e7473040000000135030000000d6d6178457865637574696f6e7304000000013303000000057265747279060000000503000000076261636b6f6666030000000b6578706f6e656e7469616c03000000106576616c7561746f7256657273696f6e040000000131030000000c696e697469616c44656c6179040000000435303030030000000a6a69747465724b696e640300000005657175616c030000000c6d6178696d756d44656c617904000000053630303030030000000e756e737461727465645265747279060000000503000000076261636b6f6666030000000b6578706f6e656e7469616c03000000106576616c7561746f7256657273696f6e040000000131030000000c696e697469616c44656c6179040000000431303030030000000a6a69747465724b696e640300000005657175616c030000000c6d6178696d756d44656c617904000000053330303030
```

| Vector | Change from the base example | Digest (SHA-256 hex) |
|---|---|---|
| GV-B0 | base business | `e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277` |
| GV-B1 | payload bytes `{ "locale" : "en",\n  "userId" : "0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d" }` | same as GV-B0 |
| GV-B2 | payload `"locale":"en"` | same as GV-B0 |
| GV-B3 | queue `bulk` | `20559a776a6a5c0b9677c0262b4bd09596699bf5e5cc1fdba7fd01b8f7d19f56` |
| GV-B4 | business metadata `tenant` = `globex` | `227b7a85093aecaca215a40ad29cbe60c9112794fa4f37298a2f934d7279a407` |
| GV-B5 | NotBefore `2026-10-30T10:00:00Z` → `{"mode":"instant","utcEpochMilliseconds":1793354400000}` | `b78eef3cb5297df3040045bc877ff479612b2571fcdd425e24f4e3b211a74c67` |
| GV-B6 | NotBefore `2026-10-30T12:00:00+02:00` | same as GV-B5 (same instant) |
| GV-B7 | NotBefore `2026-10-30T10:00:00.0001Z` → 1793354400001 (ceil) | `e74d140895560330d2f42041db9198244bb451f1579ad88a4b8c024c4f59210a` |
| GV-P0 | base policy (defaults) | `b6af7a208ee69df3802b637b02e52582dd2525eaf10b99720be012e0c69b91b3` |
| GV-P1 | retry `initialDelay` 10000 | `553184ed28dccf4da5a06ea3d19315ac90461c56fe65197e9085bf2f58e3bb65` (business unchanged → `Reused` + `PolicyDrift`) |
| GV-C0 | `Dispatch`/1 command envelope | `6103715514c292481704a7901e98ccccaa7ead786c69b7c742aee6746806a362` |
| GV-C1 | traceparent parent-id `b7ad6b7169203331` | `87f00f288c36013da7e3b20b83cdeba13acf13a9f6506795bff7fbc018cd141d` (business unchanged; same command ID → `CommandConflict`) |

Export rendering of GV-C0 (1,510 bytes):

```json
{"format":"runwick.prepared-dispatch","formatVersion":1,"fingerprintVersion":1,"storeId":"5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e5f","application":"billing","operation":"Dispatch","operationVersion":1,"operationId":"0192f3a1-8000-7000-8000-000000000001","notAfter":1793354700000,"proposedJobId":"0192f3a1-8000-7000-8000-000000000002","jobName":"send-welcome-email","payloadVersion":2,"serializerProfileId":"runwick.json","serializerProfileVersion":1,"payloadEncodingVersion":1,"payloadBase64":"eyJ1c2VySWQiOiIwMTkyZjNhMS03YzRlLTdiMmEtOWQxMC0zZTVmNmE3YjhjOWQiLCJsb2NhbGUiOiJlbiJ9","queue":"emails","priority":"high","eligibility":{"mode":"immediate"},"concurrencyScopes":[{"kind":"queue","key":"emails"}],"rateScopes":[],"uniqueness":{"activeKey":"user:42","windowKey":"req-7f3a","window":86400000},"businessMetadata":{"tenant":"acme"},"diagnosticMetadata":{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},"policy":{"maxExecutions":3,"executionTimeout":null,"retry":{"backoff":"exponential","evaluatorVersion":1,"jitterKind":"equal","initialDelay":5000,"maximumDelay":60000},"maxConsecutiveUnstartedAbandonments":5,"unstartedRetry":{"backoff":"exponential","evaluatorVersion":1,"jitterKind":"equal","initialDelay":1000,"maximumDelay":30000}},"businessDigest":"e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277","policyDigest":"b6af7a208ee69df3802b637b02e52582dd2525eaf10b99720be012e0c69b91b3","commandDigest":"6103715514c292481704a7901e98ccccaa7ead786c69b7c742aee6746806a362"}
```

SP-004 publishes these and the §13 vectors as fixture files (input bytes, canonical bytes, digests) consumed by both the C# test suite and the independent Node.js encoder.

## 13. Required wire compatibility vectors (dispatch-wire-contract §8)

| ID | Inputs | Expected properties |
|---|---|---|
| WV-01 Order/whitespace | Payload objects with permuted members and arbitrary JSON whitespace; arrays `[1,2]` vs `[2,1]` | Object variants: identical canonical bytes and business digest (GV-B0 = GV-B1). Arrays: different bytes/digest. |
| WV-02 Strings/Unicode | `"a"` vs `"a"`; `{"a":1,"a":2}`; raw bytes `C3 28`, `ED A0 80`, `C0 AF`; `"\ud800"`; `"\udc00\ud800"`; composed `é` vs `é`; leading BOM | Escaped/unescaped equal; duplicate → `payload_duplicate_property`; malformed bytes and lone/reversed surrogates → `payload_invalid_utf8`; composed ≠ decomposed (§12.1); BOM → `payload_invalid_json`. |
| WV-03 Numbers | `1`, `1.0`, `1e0`, `1E0`, `0`, `-0`; `12345678901234567890123`; `0.1000000000000000055511151231257827`; lexeme of 128 digits and of 129 digits; `NaN`, `Infinity`, `+1`, `01`, `.5`, `1.`, `"1"` as number for an `int` member | Each valid spelling has distinct canonical bytes; large integer/decimal lexemes preserved byte-for-byte (typed `long` member rejects the 23-digit value on decode); 128 accepted, 129 → `payload_number_too_long`; invalid forms → `payload_invalid_number` / `payload_invalid_json`; quoted number → binding failure. |
| WV-04 Missing/null/default | Payload with member `null` vs member absent; policy with `executionTimeout` none (`null`) vs 30,000 ms; policy fields left at defaults vs explicitly set to the same values; NotBefore omitted vs explicit past instant vs explicit future instant | Payload null ≠ absent; explicit-equal-to-default policy = defaulted policy (same GV-P0); timeout none vs set → policy digest differs, business equal; immediate ≠ past instant ≠ future instant in the business digest; a past instant is kept, not replaced by "now". |
| WV-05 Instants | Same instant in `+02:00` and `Z`; `.0001` ms remainder | GV-B5 = GV-B6; ceil to 1793354400001 (GV-B7). NotAfter `…:00.9999Z` floors to `…:00.999`. |
| WV-06 Metadata | Change only `traceparent`/`tracestate` vs change business metadata (`tenant`) | Trace change: business and policy digests equal, command digest differs (GV-C0 vs GV-C1); fresh operation → `Reused`; same command ID → `CommandConflict`. Tenant change: business digest differs (GV-B4) → `UniquenessConflict` under a live window key. |
| WV-07 Route/budget/scope | Same payload with changed queue (GV-B3), changed priority, changed `maxExecutions`, changed `executionTimeout`, added `resource` scope | Queue/priority/scope change → business digest differs → conflict. MaxExecutions/timeout change → only policy digest differs → `Reused`, `PolicyDrift = true`, original generation policy kept. |
| WV-08 Defaults/serializer deployment | Export GV-C0, change application retry defaults and redeploy, import and re-dispatch | Same operation ID, same three digests, same NotAfter; replay returns the original receipt; no defaults re-resolved. A serializer update that changes emitted bytes for the same DTO either reproduces the stored byte/digest vectors or requires a new profile version (SER-11). |
| WV-09 Stored version/retention | Window binding with `fingerprint_version = 2` (unknown); binding with version 1 after the bound job's history was purged | Unknown version → `UnsupportedVersion`, nothing written. Version 1 binding still compares and returns the retained `bound_job_id` until `expires_at` (no FK to jobs). |
| WV-10 Active aliasing/composition/rollback | Active key `user:42` bound to job J1; new request with a different payload, same active key, new window key W; separately live active→J1 and window→J2; staged dispatch rolled back | Request reuses J1 with `DuplicateMatch.ActiveAndWindow`/`Active` as applicable; W stores the incoming request's digests; payload of J1 unchanged. J1≠J2 → composition conflict, neither rewritten. Rollback leaves no job, binding or receipt. |
| WV-11 Import tampering | Export GV-C0 then alter: `application`; `storeId`; `formatVersion` 2; `serializerProfileVersion`; unaltered envelope imported after its `notAfter` passed (no receipt); one payload byte; `businessDigest` hex; add unknown member; non-canonical base64 (final group `QR==` instead of `QQ==`, i.e. nonzero unused bits); envelope of 2,097,153 bytes | Respectively: `prepared_envelope_application_mismatch`; `prepared_envelope_store_mismatch`; `prepared_envelope_unsupported_version`; `prepared_envelope_digest_mismatch`; `CommandExpired` (existing receipt would still resolve); `prepared_envelope_digest_mismatch`; `prepared_envelope_digest_mismatch`; `prepared_envelope_invalid`; `prepared_envelope_invalid`; `prepared_envelope_too_large`. |
| WV-12 Batch | 3-item batch, item 1 window-conflicts; same with `continueOnConflict = true`; 101 items | Default: whole batch fails, nothing created. ContinueOnConflict: item 1 reported as failure, items 0 and 2 committed atomically, one receipt with ordinals 0–2. 101 → `batch_too_large`, known not applied. Replaying the same `DispatchMany` operation ID returns the recorded per-ordinal results. |

## 14. Acceptance cases

### 14.1 SER cases (identity-and-serialization §7; SP-004/006/007)

| ID | Exact assertions |
|---|---|
| SER-01 | A job contract with no `[JobName]`/`Named`, a name violating §1, or two same-layer conflicting declarations (name, payload version, profile) fails host startup before any dispatch. Renaming the CLR type with the same registered name yields identical business digests and the same capability tuple. |
| SER-02 | Effective policy follows defaults → application → attributes → fluent → permitted dispatch overrides, each value recording its origin; middleware alias `Order` ties resolve by ordinal alias order; a dispatch option attempting to disable a required uniqueness dimension or override retry/timeout/budget is rejected; repeated runs produce byte-identical policy envelopes. |
| SER-03 | Every WV-02/WV-03 invalid input and every §4.1 boundary (exact limit accepted, limit+1 rejected) yields the §11 code identically at Prepare, import and worker decode. Unknown member → `payload_unmapped_member`. |
| SER-04 | Mutating ASP.NET `JsonOptions` or `JsonSerializerOptions.Default`-derived options in the host changes no stored payload byte or digest. Mutating Runwick's finalized options throws `InvalidOperationException` (read-only). |
| SER-05 | After `Prepare` returns, mutating the source DTO changes neither payload bytes, keys, digests nor the export rendering. (Concurrent mutation during Prepare is outside the caller contract and not tested for a particular outcome.) |
| SER-06 | For payloads containing `1.0`, `1e0`, `-0`, a 23-digit integer, `é`, `\/` and `\u0000` escapes, the bytes returned by Reserve (`ReservedJob.PayloadUtf8`) and read back from `job_payloads.payload_utf8` equal the Prepare bytes exactly, with the same profile ID/version. |
| SER-07 | A pending job with tuple (name, v, `runwick.json`, 2) is never reserved by a worker advertising only (name, v, `runwick.json`, 1); its budgets/counters are unchanged and it is reported as unsupported backlog. A supported tuple with malformed bytes is reserved, begun, and finished as a permanent payload failure through normal accounting. |
| SER-08 | A v1 payload executed by a worker whose handler is v2 receives the upgraded DTO; stored bytes, payload version, job ID and stored digests are unchanged afterward (also after manual retry). Removing the v1 decoder makes the backlog visible as unsupported rather than failing it. |
| SER-09 | An unknown `$type` discriminator or an assembly-qualified type name in payload fails decode and never loads a type; registering a type carrying `[JsonNumberHandling(AllowReadingFromString)]`, `[JsonExtensionData]` or `[JsonUnmappedMemberHandling(Skip)]` fails registration. |
| SER-10 | Import enforces §9.3 (WV-11 table); a valid import reproduces the original three digests and operation ID; an import whose policy is no longer permitted and that has no receipt fails with a policy/compatibility error while `ResolveAsync(operationId)` still answers. |
| SER-11 | CI runs the §12 golden vectors and the published fixtures under the C# encoder and the independent Node.js encoder; any byte/digest difference fails the build unless a new profile or fingerprint version is introduced with its own vectors. |
| SER-12 | Preparing and importing a 1 MiB payload and a 2 MiB batch envelope completes with peak additional allocation bounded by a small constant multiple of the input size (no unbounded second object graph) and without truncation or normalization by the provider; recorded latency is reported against the agreed workload. |

### 14.2 Related driver-suite cases (driver-contract-suite)

| ID | Assertion relevant to this file |
|---|---|
| driver-contract-suite:CMD-01 / CMD-02 | Same command ID + same command digest → one effect, stable result; different digest (operation, body, deadline, application) → `CommandConflict`, no cross-application leakage. |
| driver-contract-suite:UNI-02 | Same window key + same business digest → stable reuse; changed payload → explicit conflict; changed policy only → `Reused` with `PolicyDrift`. |
| driver-contract-suite:ADM-01 | Unsupported payload version or serializer profile ID/version never claimed. |
| driver-contract-suite:ENC-01 | Boundary timestamps (ms rounding), UUID ordering by big-endian canonical bytes (compared against server `ORDER BY` on `uuid`), large fences: identical precision/order across drivers; exact counters or explicit overflow. Canonical object-name ordering is bytewise and never depends on database collation. |
| driver-contract-suite:CFG-01 | Unsupported operation/fingerprint/profile version rejected before writes. |
