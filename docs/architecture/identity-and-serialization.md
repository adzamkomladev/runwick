# Identity, configuration and serialization decisions

Status: concrete recommendations for D-03/D-04, 2026-10-29. Refines the [public API](public-api-contract.md), [wire contract](dispatch-wire-contract.md) and [.NET 10+ foundation](platform-and-packages.md). These are reviewable engineering selections, not claims of user approval or working code. The platform floor and attribute/fluent support remain settled. No runtime code or codec tests were executed.

## 1. Durable identities

Recommend an explicit stable job name for every durable contract, supplied by JobName or fluent Named registration. An attribute is not mandatory; an explicit identity is. CLR namespace/type/assembly names never become a fallback durable identity. Renaming a CLR class retains its registered name. Changing the stable job name creates a different contract; automatic aliases or rewriting queued identities are outside this initial design.

| Field | Proposed validation |
|---|---|
| Application, queue, schedule, pool and serializer-profile names | 1–128 ASCII characters; start with an ASCII letter/digit, remaining characters letters/digits/dot/underscore/hyphen |
| Job name | Same grammar, 1–200 characters |
| Payload/profile version | Positive signed 32-bit integer; no downgrade inference |
| Uniqueness/business resource key | 1–512 UTF-8 bytes; valid Unicode, no control characters; opaque and case-sensitive |
| Job/attempt/command IDs | Existing UUID-based types with canonical encoding; never selected by parsing a CLR name |

Names compare ordinally and case-sensitively; reject leading/trailing spaces instead of trimming. Keys are not Unicode-normalized or case-folded. Document this explicitly so producers agree on identity. Parameterized values and provider key encoding are still mandatory; the identifier grammar is not a substitute for escaping or authorization. These length/grammar limits are proposed release constraints, subject to compatibility fixtures before freeze.

One handler registration per stable job name selects the current payload CLR type and adjacent upgrade chain. The decoding capability becomes **(job name, original payload version, serializer profile ID, serializer profile version)**. A worker advertises only tuples with an installed decoder and complete path to its current handler input. Presence and queue subscriptions explain unsupported backlog; they never authorize ownership.

This extends earlier two-field capability examples. Profile information must be available on the jobs readiness row, in Reserve/ready probes and worker presence, before claiming. A worker must not reserve a job and discover only after Begin that its serializer profile is unsupported. Supported-but-malformed data remains a permanent payload failure after Begin. Do not burn execution budgets on known unsupported profiles.

## 2. Configuration resolution

Compile configuration once at host startup into an immutable registry. Resolve overridable scalars in this order: library defaults → application defaults → job attributes → fluent job settings → permitted dispatch overrides. Record each selected value's origin for diagnostics. Matching repeated declarations are harmless; conflicting declarations at the same layer fail startup.

The exceptions are intentional:

- Stable name, payload version and serializer selection are identity declarations: attribute/fluent declarations must agree. They are not ordinary last-writer overrides.
- Required uniqueness/scope membership cannot be disabled by dispatch options. Explicit versus derived keys must agree when both are present.
- Queue, priority and NotBefore may be overridden where application policy permits. Routing recomputes destination queue membership while preserving job/resource requirements.
- Retry/timeout/abandonment budgets are effective generation snapshots. No per-dispatch override changes them in the initial public surface.
- Shared capacities, rates and pause state remain store-owned live controls. Startup seeds missing configuration and reports drift; it does not overwrite operator changes.
- Middleware ordering is global, then attribute aliases by explicit Order, then fluent additions. Reject duplicate effective aliases/types. When attribute Order ties, use ordinal alias order; do not depend on reflection enumeration. Producers need only metadata aliases, not worker middleware types.

For the first release, recommend the existing `low`, `medium`, `high` vocabulary. Persist strings and keep per-dispatch promotion/demotion supported. Custom labels remain a later explicit extension unless architecture review selects them before schema freeze; do not advertise them in v1 examples. This resolves the initial CHECK shape without introducing an unplanned mutable lane catalog. Fairness/aging still selects lanes; strings never require a lexical priority sort.

Registration finalization validates names, known queues/lanes, serializer metadata, upgrade graph, middleware bindings, scope selectors and required policy completeness. It does not instantiate scoped handlers. Trusted converters/selectors must be deterministic and thread-safe; freezing options cannot prove arbitrary application code is pure. The caller must not mutate a payload concurrently with Prepare; after preparation completes, later mutations cannot affect the frozen request.

## 3. Built-in serializer profile

Recommend `runwick.json` profile version 1, implemented with System.Text.Json on the selected .NET 10 SDK. Register independent immutable options/type metadata; do not inherit ASP.NET JsonOptions or a process-global serializer. Explicitly configure the profile so SDK default changes cannot redefine persisted behavior.

| Concern | Profile v1 behavior |
|---|---|
| Root | Non-null JSON object for a registered job contract |
| Property naming | camelCase by default; explicit JsonPropertyName allowed and becomes part of the payload schema |
| Binding | Case-sensitive; duplicate decoded property names rejected at every object level |
| Missing/unknown members | Respect required members/required constructor parameters; reject unmapped JSON properties for the declared historical DTO |
| Nulls/default values | Preserve emitted null/default values; do not silently omit them by global options; explicitly ignored members are schema decisions |
| Numbers | Strict JSON numbers; no quoted-number coercion, named NaN or Infinity; retain numeric token spelling for hashing |
| Structure | No comments, trailing commas, reference-preservation metadata or cycles |
| Fields/properties | Public serializable properties by default; no automatic private-field serialization |
| Enums | Built-in profile uses declared string names with integer values disabled; custom representation requires a declared profile variant |
| Polymorphism | Only explicitly registered closed hierarchies with stable discriminator values; unknown discriminator/runtime subtype fails, no fallback to arbitrary CLR types |
| Unstructured data | JsonElement allowed within the structural bounds; object/dynamic/interface values require an explicitly registered representation |
| Binary data | Bounded byte arrays use base64 JSON strings; encoded bytes count against payload size |

.NET 10 supplies duplicate-property rejection and strict serializer controls. Set the individual controls explicitly, including AllowDuplicateProperties=false, UnmappedMemberHandling=Disallow, PropertyNameCaseInsensitive=false, RespectNullableAnnotations=true and RespectRequiredConstructorParameters=true. The strict preset informs the profile; it is not a substitute for Runwick's format/version definition. [System.Text.Json changes in .NET 10](https://learn.microsoft.com/en-us/dotnet/core/whats-new/dotnet-10/libraries)

Required-member checks and nullability checks are distinct. System.Text.Json's nullable enforcement has limitations, including generic/collection-element cases; it is not a full domain validator. Worker validation middleware remains responsible for business invariants, and optional producer validation must not be described as a replacement. [Nullable enforcement](https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/nullable-annotations)

Select JsonTypeInfo from explicit registration, using generated metadata where supplied or a bounded registration-time reflection path. Finalize options with a configured resolver and MakeReadOnly. This freezes option mutation, not the internal state of user converters. Validate effective metadata so type/member attributes cannot silently relax duplicate handling, number coercion or unknown-member rules for the built-in profile. If an application needs different semantics, register a different named/versioned profile with its own vectors. [Read-only options](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.jsonserializeroptions.makereadonly?view=net-10.0)

Use ordinary registered System.Text.Json representations for GUIDs, DateOnly, TimeOnly and DateTimeOffset. Do not apply queue timestamp rounding to arbitrary payload data: the millisecond quantum governs Runwick envelope/control fields only. Recommend DateTimeOffset for instants. The built-in profile rejects DateTime with Unspecified/Local Kind to prevent host-local interpretation; UTC DateTime is allowed through a fixed profile converter. These date rules require explicit vectors. Do not normalize user strings merely because they look like dates.

Explicit polymorphic mappings are a supported serializer mechanism, but Runwick must validate the closed registration rather than enabling unknown derived-type fallback. Converters and polymorphic maps are executable application configuration, never code loaded from a queue payload. [Polymorphic serialization](https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/polymorphism)

## 4. Bounded preparation and import

Proposed initial limits below are review values, not performance claims. Publish them as one coherent format/acceptance policy and test the bounds. A host may impose smaller admission limits; raising the structural profile bounds requires a new profile version and compatible workers.

| Limit | Proposed value |
|---|---|
| Serialized payload | 1 MiB UTF-8 |
| Nested JSON containers | 32 levels, counting root as level 1 |
| Decoded string value | 256 KiB UTF-8 |
| Decoded property name | 256 UTF-8 bytes |
| Properties per object | 256 |
| Elements per array | 16,384 |
| Numeric token | 128 ASCII bytes, still subject to declared CLR type range on decoding |
| Business plus diagnostic metadata | 16 KiB encoded, 32 entries total |
| Metadata key | 128 ASCII characters under the identifier grammar |
| Exported prepared envelope | 2 MiB total, including base64 payload and all policy/key fields |

Enforce the total byte limit while serializing into a bounded buffer. Then scan tokens for Unicode validity, decoded duplicate names, depth and per-container/token limits before canonical hashing. Maintain only bounded per-object name sets and sorting buffers. Hash incrementally after bounded canonical ordering; do not construct an unlimited second object graph. For custom converters, validate their final output through the same scanner.

The built-in typed serializer rejects non-finite floating values; canonical hashing never converts valid numeric tokens through double. A JsonElement may represent a JSON integer outside Int64; hashing preserves its lexeme, while a typed Int64 member must reject it on decode. Custom CLR numeric types need registered deterministic conversion. No driver may silently clamp a number.

Import validates the envelope's strict schema, sizes, versions and store/application binding before materializing a registered job DTO. Reject unknown envelope fields for its schema version. Import must not run handlers, payload upgrades or arbitrary type-name resolution. Decode the payload only in the authorized worker execution path; envelope parsing itself is not user code execution.

Payload data is stored as base64 UTF-8 bytes in the exported JSON envelope; command identity hashes the decoded validated payload using the existing typed canonical encoding. Reject a noncanonical/invalid base64 encoding on import. This preserves the serialized snapshot while keeping fingerprint comparison insensitive to insignificant JSON object ordering/whitespace as already specified.

## 5. Persist bytes and decoding identity

Recommend immutable original UTF-8 bytes as the execution source in every driver: PostgreSQL bytea, SQLite BLOB and Redis binary-safe string values. Persist profile ID/version alongside payload version, and carry the same metadata in the jobs readiness row and ReservedJob contract. The duplicated profile metadata is immutable and inserted in the same transaction/publication unit.

This supersedes the PostgreSQL draft's payload jsonb column. jsonb stores a decomposed representation and does not preserve input formatting/key ordering; it also has its own numeric/Unicode acceptance constraints. Exact payload preservation and cross-driver format acceptance justify byte storage here. [PostgreSQL JSON types](https://www.postgresql.org/docs/current/datatype-json.html)

Do not maintain a full jsonb shadow by default. The dashboard reads authorized decoded projections on demand; it does not query arbitrary payload properties. Any future indexed payload projection is derived, separately budgeted and never the source for execution, retry, exported replay or fingerprints. Policy/control JSON can remain jsonb where its schema already defines normalized values.

Retain the original payload/profile through manual retry and upgrades. A worker decodes the historical DTO under the stored profile, performs the registered deterministic upgrade path and passes the current DTO to the handler. Upgrading does not rewrite the original snapshot or recalculate the producer fingerprint. Old workers cannot claim a new unsupported profile even if the payload version matches.

Profile identity is a stable application contract, not an assembly version or SDK patch. Cosmetic serializer output changes tolerated by canonical encoding do not necessarily change meaning; changed converters, numeric spellings or member/null rules can change request identity and require compatibility review. Never rebind an existing profile ID/version to incompatible behavior. Golden vectors detect what an options hash cannot: converter semantics are not safely fingerprinted by a delegate address.

## 6. Replay and deployment behavior

Preserve the existing SHA-256 domain-separated typed encoding and strict numeric comparison. In fingerprint v1, 1, 1.0 and 1e0 remain distinct, as do missing and null. Composed/decomposed Unicode remains distinct. These choices avoid silently asserting equivalence the application may not intend. Document the consequence for producers generating fresh requests under one window key.

| Situation | Result |
|---|---|
| Same prepared request after local defaults change | Same operation/body/deadline; resolve/replay without rerunning defaults/selectors |
| Fresh request/window key with changed queue, payload/profile, scope membership or business metadata | Idempotency conflict while prior window binding is live |
| Fresh request/window key with only changed execution policy (budget, timeout, retry/abandonment settings, including changed defaults) | Reused with `PolicyDrift` diagnostic; original generation policy retained (R3, 2026-10-30) |
| Fresh operation with only diagnostic trace changes | May reuse the business window; never replace the original trace/payload |
| Same command ID with changed diagnostic metadata | CommandConflict; command comparison covers full arguments |
| Same active key, different payload | First-writer reuse remains intentional; receipt says Active reuse and does not promise the incoming payload executes |
| New profile for the same payload schema | Requires a new capability tuple; request comparison includes profile identity |
| Unsupported historical codec/fingerprint version | Explicit compatibility failure for import/application; ID-only receipt resolution remains available under authorization |
| Stored matching receipt after policy tightening | Return historical result after authorization and any required persistence confirmation; do not apply it again |
| No receipt observed, prepared policy no longer allowed | No new application; return policy/compatibility failure while preserving possible earlier outcome ambiguity |
| Expired operation, no retained receipt | ExpiredUnknown; no automatic new command ID/deadline |

ResolveAsync by operation ID remains available even when the caller cannot decode its historical payload. Returning a prior receipt cannot certify a supplied changed body: application/replay still validates the canonical command fingerprint. NotObserved is never proof of rollback. Profile removal requires draining relevant backlog and respecting replay/outbox horizons; deleting a decoder is not a migration strategy.

## 7. Reconciliation and acceptance

Update JobContract/capability tuples, readiness SQL, worker presence, payload records, prepared envelope and scope/configuration diagnostics together. Do not deploy profile-aware producers while old name/version-only workers can still claim their jobs; D-13 must negotiate the operation/reader capabilities or require draining/replacing incompatible workers before enabling the new format.

Additional SP-004/006/007 cases:

| ID | Required result |
|---|---|
| SER-01 | Missing/invalid explicit identity and same-layer conflicts fail before dispatch; CLR rename leaves durable name unchanged |
| SER-02 | Attribute/fluent precedence, alias-order ties and forbidden dispatch overrides produce deterministic effective policy |
| SER-03 | Duplicate decoded names, malformed Unicode, unknown members, invalid numbers and each structural boundary are rejected consistently |
| SER-04 | Changing host ASP.NET options cannot change queue serialization; finalized profile options resist mutation |
| SER-05 | Post-Prepare source mutation leaves bytes/keys/body unchanged; concurrent mutation during Prepare is outside the supported caller contract |
| SER-06 | Each driver returns identical original payload bytes/profile, including numeric spellings and string escapes |
| SER-07 | Same job name/version with unsupported profile is never reserved; supported malformed data fails through normal Begin/Finish accounting |
| SER-08 | Historical profile decode/upgrades preserve original data, ID and fingerprint; profile removal makes missing support visible |
| SER-09 | Unknown polymorphic discriminator cannot load arbitrary types; built-in profile cannot be weakened by member-level coercion metadata |
| SER-10 | Prepared import validates versions/sizes/bindings, preserves replay identity and reports policy-change ambiguity honestly |
| SER-11 | SDK/converter updates pass independent byte/hash vectors or require a documented profile/fingerprint version change |
| SER-12 | Maximum payload/import sizes have bounded allocations and acceptable latency; no provider-specific silent truncation or normalization |

Existing wire vectors still apply. SP-004 must publish independent canonical-byte/digest fixtures; no passing results are claimed here. D-03/D-04 await review. The timing packet develops D-05; [PostgreSQL integration](postgres-schema-and-operations.md) and [upgrades](postgres-upgrades-and-compatibility.md) now reconcile D-06/D-13. Provider selections and planning closure are recorded in the [final baseline](../architecture-baseline.md).
