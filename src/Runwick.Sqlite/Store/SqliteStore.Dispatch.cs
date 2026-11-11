using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Internal;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Sqlite.Store;

internal sealed partial class SqliteStore
{
    // Dispatch/1 and DispatchMany/1 (04 §7.2, 20 §7.2): the steps of rw_dispatch_core_v2 in C#, with the item rows written
    // by multi-row INSERT statements (set-based, user decision 2026-11-08). ReceiveOutbox/1 and the schedule operations
    // reach the same core.

    /// <summary>SQLite's default bound-parameter limit since 3.32 (20 §7.2): one statement binds at most this many.</summary>
    internal const int MaxParameters = 32_766;

    /// <summary>Test seam (SQL-12): a smaller per-statement parameter limit, so a test can cross the chunk boundary cheaply.</summary>
    internal static int ParameterLimit { get; set; } = MaxParameters;

    public Task<StoreReply<DispatchResult>> DispatchAsync(StoreCommand<DispatchRequest> command, CancellationToken ct) =>
        runner.DispatchAsync(DispatchOp(command), tx => DispatchBody(tx, [command.Body.Item], many: false, continueOnConflict: false),
            MapDispatch, PreparedEnvelopeCodec.UpperBound(command.Application, command.Body.Item.Intent), ct);

    public Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        runner.CallAsync(DispatchManyOp(command), tx => DispatchBody(tx, command.Body.Items, many: true, command.Body.ContinueOnConflict), MapDispatchMany, ct);

    /// <summary>Dispatch/1 on a caller-owned transaction (20 §8).</summary>
    public StoreReply<DispatchResult> StageDispatch(SqliteTransaction transaction, StoreCommand<DispatchRequest> command, CancellationToken ct) =>
        runner.Stage(transaction, DispatchOp(command), tx => DispatchBody(tx, [command.Body.Item], many: false, continueOnConflict: false), MapDispatch, null, ct);

    /// <summary>DispatchMany/1 on a caller-owned transaction.</summary>
    public StoreReply<DispatchBatchResult> StageDispatchMany(SqliteTransaction transaction, StoreCommand<DispatchBatchRequest> command, CancellationToken ct) =>
        runner.Stage(transaction, DispatchManyOp(command), tx => DispatchBody(tx, command.Body.Items, many: true, command.Body.ContinueOnConflict),
            MapDispatchMany, null, ct);

    static SqOp DispatchOp(StoreCommand<DispatchRequest> command) =>
        Op(command.Application, command.CommandId, Operations.Dispatch, command.NotAfter, command.Body.CommandFingerprint, SqLane.Producer, command.Body.StoreId);

    static SqOp DispatchManyOp(StoreCommand<DispatchBatchRequest> command) =>
        Op(command.Application, command.CommandId, Operations.DispatchMany, command.NotAfter, command.Body.CommandFingerprint, SqLane.Producer, command.Body.StoreId);

    static StoreReply<DispatchResult> MapDispatch(SqRow row) =>
        row.HasValue ? row.Applied(ReceiptCodec.ReadDispatch(row.Result!)) : row.Reject<DispatchResult>();

    static StoreReply<DispatchBatchResult> MapDispatchMany(SqRow row) =>
        row.HasValue ? row.Applied(ReceiptCodec.ReadDispatchMany(row.Result!)) : row.Reject<DispatchBatchResult>();

    /// <summary>dispatch_v2 after the frame.</summary>
    static SqRow DispatchBody(SqTx tx, IReadOnlyList<DispatchItem> items, bool many, bool continueOnConflict)
    {
        var core = DispatchCore(tx, items, continueOnConflict, "initial");
        if (core.Reject is { } reject)
        {
            return reject;
        }

        var result = many ? new JsonObject { ["items"] = core.Items } : Strip(core.Items![0]!.AsObject());
        return SqRow.Applied("Applied", null, tx.Now, result);
    }

    static JsonObject Strip(JsonObject item)
    {
        var o = (JsonObject)item.DeepClone();
        o.Remove("ordinal");
        return o;
    }

    static string? InvalidDispatch(IReadOnlyList<DispatchItem> items)
    {
        foreach (var item in items)
        {
            var p = item.Intent.Policy;
            if (p.MaxExecutions is < 1 or > 100 || p.MaxConsecutiveUnstartedAbandonments is < 1 or > 100
                || p.ExecutionTimeoutMs is < 1 or > 2_592_000_000 || item.Scopes.Count > 8 || item.Intent.RateScopes.Count > 8
                || item.Rates.Count != item.Intent.RateScopes.Count
                || item.Rates.Any(r => r.Limit is < 1 or > 100_000 || r.WindowMs is < 1_000 or > 86_400_000)
                || !ValidRetry(p.Retry) || !ValidRetry(p.UnstartedRetry))
            {
                return "policy or scope bounds";
            }
        }

        return null;
    }

    static bool ValidRetry(RetryPolicy r) =>
        r.InitialDelayMs is >= 1 and <= 86_400_000 && r.MaximumDelayMs >= r.InitialDelayMs && r.MaximumDelayMs <= 86_400_000;

    /// <summary>The core's outcome: a rejection (nothing to commit) or the per-item results.</summary>
    internal sealed record DispatchCoreResult(SqRow? Reject, JsonArray? Items);

    sealed class KeyState
    {
        public required string Name { get; init; }

        public required string Kind { get; init; }

        public required string Key { get; init; }

        public Guid? Bound { get; set; }

        public byte[]? Business { get; set; }

        public byte[]? Policy { get; set; }

        public int? Version { get; set; }

        public DateTimeOffset? Expires { get; set; }

        public bool Dirty { get; set; }
    }

    /// <summary>Steps 2–10 of 04 §7.2 (rw_dispatch_core_v2) on the open write transaction, with its decision time.</summary>
    internal static DispatchCoreResult DispatchCore(SqTx tx, IReadOnlyList<DispatchItem> items, bool continueOnConflict, string createdReason)
    {
        var now = tx.Now;
        if (InvalidDispatch(items) is { } invalid)
        {
            return new(SqRow.NoEffect("InvalidRequest", "invalid_request", null, invalid), null);
        }

        if (items.Any(i => i.Rates.Count > 0))
        {
            var caps = tx.One(tx.Sql("SELECT required_capabilities FROM runwick_application_controls WHERE application = @application"), r => r.GetString(0));
            if (caps is null || !JsonNode.Parse(caps)!.AsObject().ContainsKey("rate_admission"))
            {
                return new(SqRow.NoEffect("UnsupportedVersion", "rate_admission_not_active", null), null);
            }
        }

        // 3 (computed first): the batch's key rows in bytewise order.
        var keys = items.SelectMany(i => new[]
            {
                i.Intent.ActiveKey is { } a ? (i.Intent.JobName, "active", a) : default,
                i.Intent.WindowKey is { } w ? (i.Intent.JobName, "window", w) : default,
            })
            .Where(k => k.Item3 is not null).Distinct()
            .OrderBy(k => k.JobName, Utf8Order.Instance).ThenBy(k => k.Item2, Utf8Order.Instance).ThenBy(k => k.Item3, Utf8Order.Instance)
            .Select(k => new KeyState { Name = k.JobName, Kind = k.Item2, Key = k.Item3 }).ToList();
        var keyIndex = keys.Select((k, i) => (k, i)).ToDictionary(x => (x.k.Name, x.k.Kind, x.k.Key), x => x.i);

        // 2. Queue registration.
        var queues = items.Select(i => i.Intent.Queue).Distinct().ToList();
        var known = tx.Sql("");
        known.CommandText = $"SELECT count(*) FROM runwick_queue_controls WHERE application = @application AND queue IN {known.In("q", queues)}";
        if (tx.Count(known) != queues.Count)
        {
            return new(SqRow.NoEffect("InvalidRequest", "queue_not_registered", null), null);
        }

        // 3. Seed the key rows if absent (the write lock already makes them exclusive), then read them.
        if (keys.Count > 0)
        {
            tx.BeforeMutation();
            foreach (var k in keys)
            {
                tx.Exec(tx.Sql("""
                    INSERT INTO runwick_dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
                    VALUES (@application, @n, @k, @v, @now, @now) ON CONFLICT DO NOTHING
                    """).P("n", k.Name).P("k", k.Kind).P("v", k.Key).P("now", now), mutation: false);
                var row = tx.One(tx.Sql("""
                    SELECT bound_job_id, business_fingerprint, policy_fingerprint, fingerprint_version, expires_at FROM runwick_dispatch_keys
                    WHERE application = @application AND job_name = @n AND key_kind = @k AND key = @v
                    """).P("n", k.Name).P("k", k.Kind).P("v", k.Key),
                    r => (Bound: r.UuidN(0), B: r.IsDBNull(1) ? null : r.Blob(1), P: r.IsDBNull(2) ? null : r.Blob(2), V: r.IntN(3), E: r.TimeN(4)));
                (k.Bound, k.Business, k.Policy, k.Version, k.Expires) = row;
            }

            tx.AfterMutation();
        }

        // 4. Decide (the decision time was read after BEGIN IMMEDIATE).
        if (!tx.Decide())
        {
            return new(SqRow.NoEffect("CommandExpired", "command_expired", now), null);
        }

        // 5. Resolve items in ordinal order against the in-memory key state.
        var results = new JsonArray();
        var created = new List<int>();
        var drifts = new List<(int Ordinal, Guid Job, string Match)>();
        var conflicts = new List<(int Ordinal, string Code, Guid? Job)>();
        for (var i = 0; i < items.Count; i++)
        {
            var it = items[i].Intent;
            var digests = items[i].Digests;
            var ak = it.ActiveKey is { } a ? keys[keyIndex[(it.JobName, "active", a)]] : null;
            var wk = it.WindowKey is { } w ? keys[keyIndex[(it.JobName, "window", w)]] : null;
            if (wk is { Bound: { } wBound } && wk.Expires > now)
            {
                if (wk.Version != 1)
                {
                    return new(SqRow.NoEffect("UnsupportedVersion", "fingerprint_version_unsupported", now), null);
                }

                if (!digests.Business.AsSpan().SequenceEqual(wk.Business))
                {
                    conflicts.Add((i, "window_fingerprint_mismatch", wBound));
                    results.Add(new JsonObject { ["ordinal"] = i, ["failureCode"] = "uniqueness_conflict", ["conflictingJobId"] = wBound });
                    continue;
                }

                if (ak is { Bound: { } aBound } && aBound != wBound)
                {
                    conflicts.Add((i, "uniqueness_composition_conflict", aBound));
                    results.Add(new JsonObject { ["ordinal"] = i, ["failureCode"] = "uniqueness_composition_conflict", ["conflictingJobId"] = aBound });
                    continue;
                }

                var match = ak is { Bound: { } b2 } && b2 == wBound ? "ActiveAndWindow" : "Window";
                var drift = !digests.Policy.AsSpan().SequenceEqual(wk.Policy);
                results.Add(new JsonObject { ["ordinal"] = i, ["jobId"] = wBound, ["disposition"] = "Reused", ["match"] = match, ["policyDrift"] = drift });
                if (drift)
                {
                    drifts.Add((i, wBound, match));
                }

                continue;
            }

            if (ak is { Bound: { } activeJob })
            {
                if (wk is not null)
                {
                    BindWindow(wk, activeJob, items[i], now);
                }

                results.Add(new JsonObject { ["ordinal"] = i, ["jobId"] = activeJob, ["disposition"] = "Reused", ["match"] = "Active", ["policyDrift"] = false });
                continue;
            }

            var job = it.ProposedJobId;
            if (ak is not null)
            {
                ak.Bound = job;
                ak.Dirty = true;
            }

            if (wk is not null)
            {
                BindWindow(wk, job, items[i], now);
            }

            created.Add(i);
            results.Add(new JsonObject { ["ordinal"] = i, ["jobId"] = job, ["disposition"] = "Created", ["match"] = "None", ["policyDrift"] = false });
        }

        if (conflicts.Count > 0 && !continueOnConflict)
        {
            string? detail = null;
            if (items.Count > 1)
            {
                detail = "[" + string.Join(",", conflicts.Select(c =>
                    $"{{\"ordinal\":{c.Ordinal},\"code\":\"{(c.Code == "window_fingerprint_mismatch" ? "uniqueness_conflict" : c.Code)}\",\"conflictingJobId\":{(c.Job is { } j ? "\"" + j.ToString("D") + "\"" : "null")}}}")) + "]";
            }

            return new(SqRow.NoEffect("UniquenessConflict", conflicts[0].Code, now, detail, conflicts[0].Job), null);
        }

        var wrote = keys.Exists(k => k.Dirty) || created.Count > 0 || drifts.Count > 0;
        if (wrote)
        {
            tx.BeforeMutation();
        }

        // 6. Key writes, in sorted order.
        foreach (var k in keys.Where(k => k.Dirty))
        {
            tx.Exec(tx.Sql("""
                UPDATE runwick_dispatch_keys
                SET bound_job_id = @bound, business_fingerprint = @b, policy_fingerprint = @p, fingerprint_version = @v, expires_at = @e, updated_at = @now
                WHERE application = @application AND job_name = @n AND key_kind = @k AND key = @key
                """).P("bound", k.Bound).P("b", k.Business).P("p", k.Policy).P("v", k.Version).P("e", k.Expires).P("now", now)
                .P("n", k.Name).P("k", k.Kind).P("key", k.Key), mutation: false);
        }

        if (created.Count > 0)
        {
            var byJob = created.Select(i => items[i]).OrderBy(i => i.Intent.ProposedJobId, UuidOrder.Instance).ToList();

            // 7. Jobs, payloads, generations in ascending job_id, as multi-row statements.
            InsertRows(tx, "runwick_jobs", ["application", "job_id", "job_name", "payload_version", "serializer_profile_id", "serializer_profile_version",
                "queue", "priority", "state", "eligible_at", "created_at", "updated_at", "active_unique_key"],
                byJob.Select(i => new object?[]
                {
                    tx.Application, i.Intent.ProposedJobId, i.Intent.JobName, i.Intent.PayloadVersion, i.Intent.SerializerProfileId,
                    i.Intent.SerializerProfileVersion, i.Intent.Queue, i.Intent.Priority, "pending",
                    i.Intent.NotBeforeMs is { } nb && nb > SqTime.Ms(now) ? nb : SqTime.Ms(now), now, now, i.Intent.ActiveKey,
                }));
            InsertRows(tx, "runwick_job_payloads", ["application", "job_id", "payload_utf8", "payload_encoding_version", "serializer_profile_id",
                "serializer_profile_version", "headers"],
                byJob.Select(i => new object?[]
                {
                    tx.Application, i.Intent.ProposedJobId, i.Intent.PayloadUtf8, 1, i.Intent.SerializerProfileId, i.Intent.SerializerProfileVersion,
                    Headers(i, now),
                }));
            InsertRows(tx, "runwick_job_generations", ["application", "job_id", "generation", "created_at", "created_reason", "max_executions",
                "max_unstarted_abandonments", "execution_timeout_ms", "retry_policy", "unstarted_retry_policy"],
                byJob.Select(i => new object?[]
                {
                    tx.Application, i.Intent.ProposedJobId, 1, now, createdReason, i.Intent.Policy.MaxExecutions,
                    i.Intent.Policy.MaxConsecutiveUnstartedAbandonments, i.Intent.Policy.ExecutionTimeoutMs,
                    PolicyJson(i.Intent.Policy.Retry).ToJsonString(), PolicyJson(i.Intent.Policy.UnstartedRetry).ToJsonString(),
                }));

            // 8. Scope provisioning: seed-if-absent in (kind, key) order, the first item's capacity in job order wins;
            // then requirements in ascending scope_id.
            var scopeSeeds = byJob.SelectMany(i => i.Scopes.Select(s => (Job: i.Intent.ProposedJobId, s.Kind, s.Key, Value: s.Capacity, Window: 0L))).ToList();
            ProvisionRequirements(tx, now, scopeSeeds, rate: false);
            var rateSeeds = byJob.SelectMany(i => i.Rates.Select(r => (Job: i.Intent.ProposedJobId, r.Kind, r.Key, Value: r.Limit, Window: r.WindowMs))).ToList();
            ProvisionRequirements(tx, now, rateSeeds, rate: true);
        }

        // 9. Events: dispatched in ordinal order, then policy drift.
        if (created.Count > 0 || drifts.Count > 0)
        {
            var events = created.Select(i => new object?[]
                {
                    tx.Application, items[i].Intent.ProposedJobId, tx.Op.CommandId, "dispatched", now,
                    new JsonObject { ["generation"] = 1, ["queue"] = items[i].Intent.Queue, ["priority"] = items[i].Intent.Priority, ["ordinal"] = i }.ToJsonString(),
                })
                .Concat(drifts.Select(d => new object?[]
                {
                    tx.Application, d.Job, tx.Op.CommandId, "policy_drift", now, new JsonObject { ["ordinal"] = d.Ordinal, ["match"] = d.Match }.ToJsonString(),
                }));
            InsertRows(tx, "runwick_queue_events", ["application", "job_id", "command_id", "event_kind", "occurred_at", "details"], events);
        }

        if (wrote)
        {
            tx.AfterMutation();
        }

        return new(null, results);
    }

    static void BindWindow(KeyState wk, Guid job, DispatchItem item, DateTimeOffset now)
    {
        wk.Bound = job;
        wk.Business = item.Digests.Business;
        wk.Policy = item.Digests.Policy;
        wk.Version = 1;
        wk.Expires = now + TimeSpan.FromMilliseconds(item.Intent.WindowMs!.Value);
        wk.Dirty = true;
    }

    /// <summary>Seeds scopes (or rate scopes) if absent, then inserts the requirement rows in ascending (scope_id, job_id).</summary>
    static void ProvisionRequirements(SqTx tx, DateTimeOffset now, List<(Guid Job, string Kind, string Key, int Value, long Window)> seeds, bool rate)
    {
        if (seeds.Count == 0)
        {
            return;
        }

        var table = rate ? "runwick_rate_scopes" : "runwick_concurrency_scopes";
        var idColumn = rate ? "rate_scope_id" : "scope_id";
        var firsts = seeds.GroupBy(s => (s.Kind, s.Key)).Select(g => g.First())
            .OrderBy(s => s.Kind, Utf8Order.Instance).ThenBy(s => s.Key, Utf8Order.Instance);
        var ids = new Dictionary<(string, string), Guid>();
        foreach (var s in firsts)
        {
            tx.Exec(rate
                ? tx.Sql("""
                    INSERT INTO runwick_rate_scopes (application, rate_scope_id, scope_kind, scope_key, limit_count, window_ms, created_at, updated_at)
                    VALUES (@application, @id, @kind, @key, @v, @w, @now, @now) ON CONFLICT (application, scope_kind, scope_key) DO NOTHING
                    """).P("id", Guid.CreateVersion7(now)).P("kind", s.Kind).P("key", s.Key).P("v", s.Value).P("w", s.Window).P("now", now)
                : tx.Sql("""
                    INSERT INTO runwick_concurrency_scopes (application, scope_id, scope_kind, scope_key, capacity, fencing_enabled, created_at, updated_at)
                    VALUES (@application, @id, @kind, @key, @v, @f, @now, @now) ON CONFLICT (application, scope_kind, scope_key) DO NOTHING
                    """).P("id", Guid.CreateVersion7(now)).P("kind", s.Kind).P("key", s.Key).P("v", s.Value)
                    .P("f", s.Kind == "resource" && s.Value == 1).P("now", now), mutation: false);
            ids[(s.Kind, s.Key)] = tx.Query(tx.Sql($"SELECT {idColumn} FROM {table} WHERE application = @application AND scope_kind = @kind AND scope_key = @key")
                .P("kind", s.Kind).P("key", s.Key), r => r.Uuid(0))[0];
        }

        var rows = seeds.Select(s => (Scope: ids[(s.Kind, s.Key)], s.Job)).Distinct()
            .OrderBy(x => x.Scope, UuidOrder.Instance).ThenBy(x => x.Job, UuidOrder.Instance)
            .Select(x => new object?[] { tx.Application, x.Job, x.Scope });
        InsertRows(tx, rate ? "runwick_job_rate_requirements" : "runwick_job_concurrency_requirements", ["application", "job_id", idColumn], rows);
    }

    /// <summary>
    /// Multi-row <c>INSERT … VALUES (…), (…)</c> in the given row order, chunked so one statement binds at most
    /// <see cref="ParameterLimit"/> parameters (20 §7.2).
    /// </summary>
    internal static int InsertRows(SqTx tx, string table, string[] columns, IEnumerable<object?[]> rows)
    {
        var perStatement = Math.Max(1, ParameterLimit / columns.Length);
        var total = 0;
        foreach (var chunk in rows.Chunk(perStatement))
        {
            var cmd = tx.Connection.CreateCommand();
            cmd.Transaction = tx.Transaction;
            var sql = new StringBuilder("INSERT INTO ").Append(table).Append(" (").AppendJoin(", ", columns).Append(") VALUES ");
            var p = 0;
            for (var r = 0; r < chunk.Length; r++)
            {
                sql.Append(r == 0 ? "(" : ", (");
                for (var c = 0; c < columns.Length; c++)
                {
                    var name = "@p" + p++.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    sql.Append(c == 0 ? "" : ", ").Append(name);
                    cmd.Parameters.AddWithValue(name, SqParams.Convert(chunk[r][c]));
                }

                sql.Append(')');
            }

            cmd.CommandText = sql.ToString();
            total += tx.Exec(cmd, mutation: false);
        }

        return total;
    }

    /// <summary>
    /// The payload row's headers: the metadata maps and, for a store-built scheduled job (13 §3.2), its occurrence; a manual
    /// trigger's occurrence is the decision time.
    /// </summary>
    static string Headers(DispatchItem item, DateTimeOffset now)
    {
        var intent = item.Intent;
        static JsonObject Map(IReadOnlyDictionary<string, string> m)
        {
            var o = new JsonObject();
            foreach (var key in m.Keys.Order(Utf8Order.Instance))
            {
                o[key] = m[key];
            }

            return o;
        }

        var headers = new JsonObject { ["business"] = Map(intent.BusinessMetadata), ["diagnostic"] = Map(intent.DiagnosticMetadata) };
        if (item.Schedule is { } s)
        {
            var manual = s.ScheduledAt == default;
            headers["schedule"] = new JsonObject
            {
                ["key"] = s.ScheduleName,
                ["scheduledAt"] = SqTime.Ms(manual ? now : s.ScheduledAt),
                ["coveredFrom"] = SqTime.Ms(manual ? now : s.CoveredFrom),
                ["coveredUntil"] = SqTime.Ms(manual ? now : s.CoveredUntil),
            };
        }

        return headers.ToJsonString();
    }

    // ------------------------------------------------------------------ ResolveCommand/1

    public Task<ReceiptResolution> ResolveCommandAsync(CommandIdentity command, CancellationToken ct) =>
        ResolveCoreAsync(command.Application, command.CommandId, command.Operation, command.Fingerprint, command.NotAfter, ct);

    public Task<ReceiptResolution> ResolveOperationAsync(string application, Guid commandId, DateTimeOffset notAfterBound, CancellationToken ct) =>
        ResolveCoreAsync(application, commandId, null, null, notAfterBound, ct);

    /// <summary>
    /// Classifies a command's receipt (03 §8.6) on the read path: the WAL snapshot shows only committed rows, and a write in
    /// progress holds the write lock, so a committed receipt or none is all a reader can see.
    /// </summary>
    Task<ReceiptResolution> ResolveCoreAsync(string application, Guid commandId, string? operation, byte[]? fingerprint, DateTimeOffset notAfter,
        CancellationToken ct) => runner.ReadAsync<ReceiptResolution>(conn =>
        {
            var tx = new SqTx(conn, null, new SqOp(application, commandId, "ResolveCommand", DateTimeOffset.MaxValue, null, SqLane.Producer), runner.Faults, ct);
            tx.Fault(FaultPoint.DuringReceiptLookup);
            SqReceiptRow? found = null;
            foreach (var cls in operation is null ? BothClasses : [Operations.RetentionClass(operation)])
            {
                found = SqRunner.ReadReceipt(tx, cls);
                if (found is not null)
                {
                    break;
                }
            }

            var now = time.GetUtcNow();
            if (found is { CodecVersion: not 1 } or { OperationVersion: not 1 })
            {
                // 23 §8: a receipt this binary cannot decode is never reported as not found, conflict or success.
                throw new StoreIncompatibleException($"Receipt codec {found.CodecVersion}/operation version {found.OperationVersion} is not supported by this binary.");
            }

            if (found is not null)
            {
                return (operation is not null && (found.Operation != operation || !found.Fingerprint.AsSpan().SequenceEqual(fingerprint)))
                    ? new ReceiptResolution.Conflict()
                    : found.Result is null
                        ? new ReceiptResolution.NotObserved()
                        : new ReceiptResolution.Recorded(found.Operation, found.OperationVersion, found.Status!, found.DecidedAt!.Value,
                            JsonDocument.Parse(found.Result.ToJsonString()).RootElement.Clone());
            }

            var floor = operation is null ? TimeSpan.FromHours(24) : Operations.RetentionFloor(operation);
            return now < notAfter ? new ReceiptResolution.NotObserved()
                : now < notAfter + floor ? new ReceiptResolution.NotApplied()
                : new ReceiptResolution.ExpiredUnknown();
        }, ct);

    static readonly string[] BothClasses = [Operations.DurableClass, Operations.OwnershipClass];
}
