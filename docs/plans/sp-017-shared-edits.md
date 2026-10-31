# SP-017 — edits to shared files (apply at integration)

SP-017 was written as new files only, because other sessions were editing shared files at the same time. This list gives every edit an existing file needs, with exact old/new text or an insertion point. Apply the edits in order. Nothing here has been built or run.

New files from SP-017, for reference:

- `docs/spec/14-outbox.md`
- `src/Runwick/Protocol/OutboxProtocol.cs`
- `src/Runwick.Postgres/Sql/0003_outbox.sql`
- `src/Runwick.Postgres/Store/PgStore.Outbox.cs`
- `src/Runwick.Outbox/`:
  - `Runwick.Outbox.csproj`
  - `OutboxContracts.cs`
  - `OutboxSource.cs`
  - `OutboxRelay.cs`
  - `OutboxServices.cs`
- `src/Runwick.Outbox.Postgres/`:
  - `Runwick.Outbox.Postgres.csproj`
  - `PostgresOutbox.cs`
  - `PostgresOutboxSource.cs`
  - `PostgresOutboxServiceCollectionExtensions.cs`
  - `Sql/source_v1.sql`
- `src/Runwick.Outbox.Sqlite/`:
  - `Runwick.Outbox.Sqlite.csproj`
  - `SqliteOutbox.cs`
  - `SqliteOutboxSource.cs`
  - `SqliteOutboxServiceCollectionExtensions.cs`
  - `Sql/source_v1.sql`
- `tests/Runwick.Outbox.Tests/`:
  - `Runwick.Outbox.Tests.csproj`
  - `OutboxSupport.cs`
  - `StagingTests.cs`
  - `RelayTests.cs`
  - `ReceiveOutboxProtocolTests.cs`

## 1. Build wiring

### 1.1 `Directory.Build.props` — InternalsVisibleTo for src projects

Insert after `    <InternalsVisibleTo Include="Runwick.Workloads" />` (inside the `IsSrcProject` item group):

```xml
    <InternalsVisibleTo Include="Runwick.Outbox" />
    <InternalsVisibleTo Include="Runwick.Outbox.Postgres" />
    <InternalsVisibleTo Include="Runwick.Outbox.Sqlite" />
    <InternalsVisibleTo Include="Runwick.Outbox.Tests" />
```

**Why each grant is needed:**

- **`Runwick.Outbox`** uses internals of `Runwick`: `RunwickRuntime`, `DispatchPreparer`, the `PreparedDispatch` internals, and `Runwick.Protocol`.
- **The tests** use internals of `Runwick.Postgres` (`PgStore`) and `Runwick` (`StoreReply`, `FaultPoint`).

The three Outbox csproj files also declare their own grants. The duplicates are harmless; remove them if you prefer one list.

### 1.2 `tests/Runwick.Harness/Runwick.Harness.csproj`

Insert after `    <InternalsVisibleTo Include="Runwick.Workloads" />`:

```xml
    <InternalsVisibleTo Include="Runwick.Outbox.Tests" />
```

### 1.3 `Directory.Packages.props`

Insert after `    <PackageVersion Include="Npgsql" Version="10.0.3" />`:

```xml
    <PackageVersion Include="Microsoft.Data.Sqlite" Version="10.0.14" />
```

In the trailing comment, replace `    Microsoft.Data.Sqlite 10.0.14, StackExchange.Redis 3.4.20, Inertia.Net 1.5.3 -->` with `    StackExchange.Redis 3.4.20, Inertia.Net 1.5.3 -->`.

### 1.4 `Runwick.slnx`

1. Insert after `    <Project Path="src/Runwick.Postgres/Runwick.Postgres.csproj" />`:

   ```xml
       <Project Path="src/Runwick.Outbox/Runwick.Outbox.csproj" />
       <Project Path="src/Runwick.Outbox.Postgres/Runwick.Outbox.Postgres.csproj" />
       <Project Path="src/Runwick.Outbox.Sqlite/Runwick.Outbox.Sqlite.csproj" />
   ```

2. Insert after `    <Project Path="tests/Runwick.Postgres.Tests/Runwick.Postgres.Tests.csproj" />`:

   ```xml
       <Project Path="tests/Runwick.Outbox.Tests/Runwick.Outbox.Tests.csproj" />
   ```

### 1.5 `eng/package-graph.json` (edit only; nothing under `eng/` was executed)

1. Replace the three outbox rows:

   ```json
       "Runwick.Outbox":           { "exists": true,  "milestone": "M3", "allowedProjectRefs": ["Runwick"], "allowedPackages": [] },
       "Runwick.Outbox.Postgres":  { "exists": true,  "milestone": "M3", "allowedProjectRefs": ["Runwick.Outbox"], "allowedPackages": ["Npgsql"] },
       "Runwick.Outbox.Sqlite":    { "exists": true,  "milestone": "M3", "allowedProjectRefs": ["Runwick.Outbox"], "allowedPackages": ["Microsoft.Data.Sqlite"] },
   ```

2. In `"deferred"`, remove `"Microsoft.Data.Sqlite": "10.0.14", `.

3. Check the dependency-graph checker: the Outbox projects reference `Runwick` transitively through `Runwick.Outbox`. The test project references `Runwick.Harness`, so `Runwick.Postgres` comes in transitively.

### 1.6 Lock files

The four new projects need a `packages.lock.json`. CI uses `RestoreLockedMode`, so run `dotnet restore` once after the G-1 run has ended and commit the generated lock files.

## 2. Code

### 2.1 `src/Runwick.Postgres/Migrations/PgMigrator.cs`

1. Replace:

   ```csharp
           // 0003_outbox (SP-017) is inserted here at integration; revision 3.
   ```

   with:

   ```csharp
           new("0003_outbox", LoadSql("0003_outbox.sql"), 3),
   ```

2. In `ManifestAdditions`, insert between the `[2] = …` and `[4] = …` lines:

   ```csharp
           [3] = ["ReceiveOutbox"],
   ```

Check after applying: the migrator's catalog validation extracts names from SQL by regex. `0003_outbox.sql` declares the table `outbox_deliveries`, the index `outbox_deliveries_cleanup`, and these constraints:

- `outbox_deliveries_pk`
- `_fingerprint_ck`
- `_window_ck`
- `_disposition_ck`
- `_match_ck`
- `_profile_ck`

All match the existing patterns. The file is embedded by the existing `Sql\**\*.sql` glob.

### 2.2 `src/Runwick.Postgres/PostgresServices.Store.cs`

Insert after `        services.AddSingleton<IReadinessDiagnostics>(sp => sp.GetRequiredService<PgStore>());`:

```csharp
        services.AddSingleton<IOutboxDestinationStore>(sp => sp.GetRequiredService<PgStore>());
```

Until this is applied, the tests register it themselves with `TryAddSingleton` in `OutboxSupport.cs`. A production host has no destination, so the relay fails at start with "does not implement ReceiveOutbox/1".

### 2.3 `src/Runwick.Postgres/Store/PgTransaction.cs` — `PgOp.IsCreation`

Replace:

```csharp
    public bool IsCreation => Operation is Operations.Dispatch or Operations.DispatchMany or Operations.Reserve
        or Operations.Begin or Operations.RegisterQueue or Operations.RegisterScope;
```

with:

```csharp
    public bool IsCreation => Operation is Operations.Dispatch or Operations.DispatchMany or Operations.Reserve
        or Operations.Begin or Operations.RegisterQueue or Operations.RegisterScope or OutboxOperations.ReceiveOutbox;
```

This makes ReceiveOutbox a creation path for the capability gate (03 §10.3). If another session has changed this line since, add `or OutboxOperations.ReceiveOutbox` to whatever is there.

### 2.4 `src/Runwick/Protocol/StoreProtocol.cs` — `Operations.DeadlineCap`

Replace:

```csharp
    public static TimeSpan DeadlineCap(string operation) =>
        operation is Reserve or Begin or Renew or Finish or Recover or ReleaseReservation ? TimeSpan.FromSeconds(60) : TimeSpan.FromMinutes(10);
```

with:

```csharp
    public static TimeSpan DeadlineCap(string operation) =>
        operation is Reserve or Begin or Renew or Finish or Recover or ReleaseReservation ? TimeSpan.FromSeconds(60)
        : operation == OutboxOperations.ReceiveOutbox ? OutboxOperations.TransportCap
        : TimeSpan.FromMinutes(10);
```

Optional: add `public const string ReceiveOutbox = "ReceiveOutbox";` to `Operations`, and make `OutboxOperations.ReceiveOutbox => Operations.ReceiveOutbox`.

`PgStore.Outbox.cs` also enforces the 30 s cap in the body (`deadline_too_far`), so this edit only moves the rejection earlier.

`RetentionClass` needs no change: ReceiveOutbox is durable by default.

### 2.5 `src/Runwick/Hosting/RunwickRuntime.cs` — `KnownCapabilities`

Replace:

```csharp
        ["transactional_completion"] = 1, ["rate_admission"] = 1,
```

with:

```csharp
        ["transactional_completion"] = 1, ["rate_admission"] = 1, ["outbox_destination"] = 1,
```

### 2.6 Optional: receipt codec

`PgStore.Outbox.cs` carries its own reply codec (`DeliveryJson` / `ReadDeliveryJson`) so that it did not have to edit `ReceiptCodec.cs`. Moving them into `ReceiptCodec` is cosmetic.

## 3. Tests

### 3.1 `tests/Runwick.Postgres.Tests/MigrationTests.cs` — `SpecManifest`

In the `"operations"` object, replace:

```text
           "ActivateCapabilities":[1],"EditRatePolicy":[1],"ListUnsupportedBacklog":[1]},
```

with:

```text
           "ActivateCapabilities":[1],"EditRatePolicy":[1],"ListUnsupportedBacklog":[1],"ReceiveOutbox":[1]},
```

Member order inside the JSON object does not matter: the comparison uses `JsonNode.DeepEquals`. The `0003_outbox` kill-theory rows and the `AllPhases` comment are already present.

### 3.2 `tests/Runwick.Harness/Store/SnapshotReader.cs` — `Tables`

Insert after `        ("storage_commands", "application, retention_class, command_id"),`:

```csharp
        ("outbox_deliveries", "application, source_store_id, message_id"),
```

Without this, the OUTBOX-01 and CMD-02 "destination snapshot unchanged" assertions do not cover mappings.

### 3.3 Not written: P-09 workload

The P-09 workload is not written. It goes in `tests/Runwick.Workloads`, an existing project. Its definition is in 14 §9.4. It needs:

- a source database next to the destination;
- 8 business writers staging 20 000 messages;
- one relay group with `MaxInFlight = 16`;
- a latency histogram from staging commit to the delivered mark;
- pass thresholds ≥ 100 deliveries/s and p99 ≤ 2 s.

`eng/run-g2.sh` needs the workload ID and `dotnet test tests/Runwick.Outbox.Tests` in its list.

### 3.4 Not written: hash-domain vectors

Add golden vectors for `runwick.outbox.delivery/1` and `runwick.outbox.command/1` under `tests/vectors`, plus a Core.Tests case (15 §10). Generate them from `OutboxFingerprints` after the first build and review them by hand against 14 §7.

## 4. Specifications

### 4.1 `docs/spec/00-overview.md` §10

Insert after the `10-rate-admission.md` row:

```markdown
| [14-outbox.md](14-outbox.md) | M3 SP-017: `IPostgresOutbox`/`ISqliteOutbox` Stage, source schema v1, relay state machine and budgets, `ReceiveOutbox/1`, `outbox_deliveries`, hash domains, horizon rules, OUTBOX/OUT/TX/CMD/ISO cases, P-09 | transactional-outbox, storage-command-protocol (outbox destination), driver-contract-suite |
```

In §3, "Not in M2", the row `| Transactional outbox | M3 / SP-017 | — |` can point to 14.

### 4.2 `docs/spec/02-serialization-and-wire.md`

Add a Changes entry above `## Changes (2026-10-31, SP-011, SP-015)`:

```markdown
## Changes (2026-10-31, SP-017)

- Two hash domains for the outbox, normative in 14 §7: `runwick.outbox.delivery/1` (delivery fingerprint: destination binding, source identity, absolute AcceptUntil, business and policy digests, diagnostic metadata, proposed job ID; excludes the transport command) and `runwick.outbox.command/1` (the §8.3 command envelope for `ReceiveOutbox/1` with the delivery digest as body). The outbox reuses the §9 prepared envelope as its frozen intent codec; the envelope's own operation ID and NotAfter are ignored on that path (14 C-3).
```

### 4.3 `docs/spec/03-storage-protocol.md`

1. Add a Changes entry above `## Changes (2026-10-31, SP-013)`:

   ```markdown
   ## Changes (2026-10-31, SP-017)

   - `ReceiveOutbox/1` (14 §5): durable receipts, deadline cap 30 s, a creation path for the gate, capability `outbox_destination`. It checks a durable delivery identity before ordinary dispatch and stores the job mapping with the receipt. After its transport command expires, a new command is permitted only under the same delivery identity and AcceptUntil (14 §6.2); ordinary Dispatch keeps the rule that an expired unknown operation is never replaced.
   ```

2. In the operation table row that lists `ReceiveOutbox/1`, remove it from the "not in M2" row and add its own row: `| ReceiveOutbox/1 | producer | creation | durable | 30 s | M3 (14 §5) |`. Use the column order of the table as it stands.

### 4.4 `docs/spec/04-postgres.md`

1. Add a Changes entry above `## Changes (2026-10-31, SP-013)`:

   ```markdown
   ## Changes (2026-10-31, SP-017)

   - `0003_outbox` (revision 3) adds `outbox_deliveries` and `outbox_deliveries_cleanup` (14 §5.2). The manifest for revision 3 adds `ReceiveOutbox`.
   - §5.1 rank 3a: the outbox delivery identity, a transaction advisory lock `pg_advisory_xact_lock(hashtextextended('runwick.outbox/' || application || '/' || source_store_id || '/' || message_id, 0))`, then the mapping row `FOR UPDATE`. Cleanup takes the same lock.
   ```

2. In §5.1, replace:

   ```text
      [not in M2: outbox delivery identities; schedule rows]
   ```

   with:

   ```text
   3a outbox delivery identity  pg_advisory_xact_lock(hash(application, source, message)), then outbox_deliveries FOR UPDATE (ReceiveOutbox only)
      [schedule rows: spec 13]
   ```

3. In §3, the record inventory, change the `outbox_deliveries` row from **not in M2** to `0003_outbox (M3, 14 §5.2)`.

### 4.5 `docs/spec/06-defaults.md`

1. Add a Changes entry above `## Changes (2026-10-31, SP-013)`:

   ```markdown
   ## Changes (2026-10-31, SP-017)

   - §1.12 adds the outbox relay budgets and horizon bounds (14 §4.2, §6).
   ```

2. Insert before `## 2. Formulas`:

   ```markdown
   ### 1.12 Outbox relay and horizon (SP-017)

   | Name | Value | Unit | Where used | Source |
   |---|---|---|---|---|
   | Acceptance horizon | 7 d default; (0, 30 d], whole ms | — | Stage | 14 §6.1 |
   | Mapping retention | ≥ AcceptUntil + 24 h | — | outbox_deliveries | 14 §6.4 |
   | Transport NotAfter | ≤ 30 s and ≤ AcceptUntil | — | ReceiveOutbox/1 | 14 §6.2 |
   | MaxInFlight / claim batch | 4 (1 – 64) | messages | Relay | 14 §4.2 |
   | Source lease / renewal | 60 s / 15 s (lease ≥ 5 s, ≥ 3 × renewal) | s | Relay | 14 §4.2 |
   | Provider call timeout | 5 s (< renewal) | s | Relay | 14 §4.2 |
   | Idle poll | 1 s | s | Relay | 14 §4.2 |
   | Retry | 1 s → 60 s, exponential, equal jitter | s | Relay | 14 §4.2 |
   | Source connections | 2 per relay group (claim lane, control lane) | connections | Relay | 14 §4.2 |
   ```

### 4.6 `docs/spec/15-test-plan-g2.md` §10

Replace the last sentence:

```text
Definitions are completed with spec 14.
```

with:

```text
Definitions are in 14 §9 (project `tests/Runwick.Outbox.Tests`, traits `Item=SP-017`, `Case`); P-09 is 14 §9.4.
```

## 5. Integration notes and risks

1. **Analyzers.**
   - `PostgresOutboxSource.GuardedAsync`, `SqliteOutboxSource.GuardedAsync` and `PgStore.ReadDeliveryAsync` build SQL with interpolated constant fragments. CA2100 may fire; follow the `PgMigrator` precedent: a pragma or `[SuppressMessage]` with a justification.
   - The relay logs with `LogWarning(ex, "…")`. If CA1848 or CA2254 is enforced, convert the calls to `LoggerMessage`.
   - `catch (Exception ex) when (ex is not OutOfMemoryException)` may trip CA1031, as in the worker engine.
2. **SQLite.** The claim uses `UPDATE … RETURNING`, which needs SQLite ≥ 3.35; the e_sqlite3 bundled with Microsoft.Data.Sqlite 10 has it. Staging uses `SqliteTransaction.SaveAsync`, `ReleaseAsync` and `RollbackAsync(savepoint)`; confirm these async overloads exist on `SqliteTransaction` (they come from `DbTransaction`, .NET 5+).
3. **Producer lane width.** 04 §9 gives the producer class 2 concurrent calls. With `MaxInFlight = 4`, two sends queue behind it. P-09 may need a separate outbox lane or a wider producer class; 14 §9.4 runs at 16 in flight.
4. **Restaging with relative delays.** With a relative delay in `DispatchOptions`, restaging produces a different `NotBefore` and so a different policy digest. That gives a false `outbox_message_conflict`. Callers should restage with an absolute `NotBefore`; this belongs in 01 if confirmed.
5. **Read-only lookup label.** `FindDeliveryAsync` runs with the operation label `"ResolveCommand"`, so it inherits the draining allowance and fault targeting of receipt resolution. If fault tests target `ResolveCommand` broadly, they will also hit this lookup.
6. **Rate seeds.** The relay now passes `DispatchItem.Rates = preparer.RatesFor(intent)`, matching `JobDispatcher` after SP-013. If SP-013 changes that API, `OutboxRelay.DeliverAsync` follows.
