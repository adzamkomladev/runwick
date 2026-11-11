using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

/// <summary>The PostgreSQL implementation of the storage protocol (03) per 04 §7.</summary>
internal sealed partial class PgStore(PgDataSources sources, PgRunner runner, TimeProvider time) : IRunwickStore, IAsyncDisposable
{
    public Guid StoreId => runner.StoreId ?? throw new InvalidOperationException("Describe has not run yet.");

    public string AcknowledgementProfile => AcknowledgementProfiles.PostgresSynchronousCommit;

    internal PgRunner Runner => runner;

    internal PgDataSources Sources => sources;

    public ValueTask DisposeAsync() => sources.DisposeAsync();

    async Task<Guid> EnsureStoreIdAsync(string application, CancellationToken ct) =>
        runner.StoreId ?? (await DescribeAsync(application, ct).ConfigureAwait(false)).StoreId;

    PgOp Op(string application, Guid commandId, string operation, DateTimeOffset notAfter, byte[]? fingerprint, PgLane lane, Guid? storeId = null) =>
        new(application, commandId, operation, PgTime.Floor(notAfter), fingerprint, lane, storeId);

    // ------------------------------------------------------------------ Describe / Register*

    public async Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct)
    {
        try
        {
            var conn = await sources.For(PgLane.Background).OpenConnectionAsync(ct).ConfigureAwait(false);
            await using (conn.ConfigureAwait(false))
            {
                using var cmd = new NpgsqlCommand("""
                    SELECT s.store_id, s.schema_revision, s.protocol_epoch, s.migration_mode, s.compatibility_manifest_version,
                           s.manifest_digest, clock_timestamp(),
                           a.revision, a.required_protocol_epoch, a.required_capabilities::text, a.admission_mode,
                           COALESCE((to_jsonb(s) ->> 'fault_hooks')::boolean, false),
                           ARRAY(SELECT p.proname || ':' || md5(p.prosrc)
                                 FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
                                 WHERE n.nspname = 'runwick')
                    FROM runwick.store_metadata AS s
                    LEFT JOIN runwick.application_controls AS a ON a.application = @application
                    WHERE s.singleton_id = 1
                    """, conn);
                cmd.P("application", application);
                var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        throw new StoreIncompatibleException("The store has no store_metadata row; run the migrator.");
                    }

                    ApplicationGate? gate = await reader.IsDBNullAsync(7, ct).ConfigureAwait(false) ? null : new ApplicationGate(
                        reader.GetInt64(7), reader.GetInt32(8), ParseCapabilities(reader.GetString(9)), reader.GetString(10));
                    var descriptor = new StoreDescriptor(
                        reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3), reader.GetInt32(4),
                        (byte[])reader[5], gate, PgTime.Read(reader, 6))
                    {
                        EngineVersion = conn.PostgreSqlVersion.ToString(),
                    };
                    if (descriptor.SchemaRevision == Migrations.PgMigrator.CurrentRevision)
                    {
                        // PF-05 (Q7): the function bodies must be this binary's, for the store's variant, and a fault-hooks
                        // (test) store is refused unless this is the harness.
                        var hooks = reader.GetBoolean(11);
                        if (hooks && !PgRunner.HarnessMarker)
                        {
                            throw new StoreIncompatibleException("The store was migrated with fault hooks (a test store): schema_incompatible (fault_hooks).");
                        }

                        var actual = ((string[])reader[12]).Select(x => x.Split(':')).ToDictionary(x => x[0], x => x[1], StringComparer.Ordinal);
                        var problems = Migrations.PgMigrator.FunctionProblems(Migrations.PgMigrator.CurrentFunctions(hooks), actual,
                            Migrations.PgMigrator.AllFunctionNames);
                        if (problems.Count > 0)
                        {
                            throw new StoreIncompatibleException("Store functions differ from this binary's: " + string.Join("; ", problems));
                        }
                    }

                    runner.StoreId ??= descriptor.StoreId;
                    return descriptor;
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01" or "3F000")
        {
            throw new StoreIncompatibleException("The Runwick schema is not installed; run the migrator.", ex);
        }
        catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException)
        {
            throw new StoreUnavailableException("Could not reach the PostgreSQL store.", ex);
        }
    }

    static Dictionary<string, int> ParseCapabilities(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.TryGetInt32(out var v) ? v : int.MaxValue, StringComparer.Ordinal);
    }

    public async Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(StoreCommand<RegisterApplicationRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterApplication, command.CommandId, command.NotAfter, CommandEnvelopes.RegisterApplication());
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.RegisterApplication, command.NotAfter, fp, PgLane.Producer),
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<ApplicationSeedResult>.Rollback(tx.Reject<ApplicationSeedResult>(StoreStatus.CommandExpired, "command_expired"));
                }

                using var cmd = tx.Sql("""
                    SELECT revision, required_protocol_epoch, required_capabilities::text, admission_mode, xmin = pg_current_xact_id()::xid
                    FROM runwick.application_controls WHERE application = @application
                    """);
                var row = (await tx.QueryAsync(cmd, r => (Gate: new ApplicationGate(r.GetInt64(0), r.GetInt32(1), ParseCapabilities(r.GetString(2)), r.GetString(3)), Fresh: r.GetBoolean(4))).ConfigureAwait(false))[0];
                var value = new ApplicationSeedResult(row.Fresh, row.Gate);
                return new PgOutcome<ApplicationSeedResult>.Commit(StoreReply<ApplicationSeedResult>.Applied(value, tx.Now), ReceiptCodec.Application(value));
            },
            (tx, row) => Task.FromResult(StoreReply<ApplicationSeedResult>.Applied(ReceiptCodec.ReadApplication(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    public async Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct)
    {
        var queues = command.Body.Queues.Distinct().Order(Internal.Utf8Order.Instance).ToArray();
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterQueue, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterQueues(command.Body));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.RegisterQueue, command.NotAfter, fp, PgLane.Producer),
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<QueueSeedResult>.Rollback(tx.Reject<QueueSeedResult>(StoreStatus.CommandExpired, "command_expired"));
                }

                using var insert = tx.Sql("""
                    INSERT INTO runwick.queue_controls (application, queue, created_at, updated_at)
                    SELECT @application, q.queue, @now, @now
                    FROM unnest(@queues::text[]) AS q(queue)
                    ORDER BY q.queue COLLATE "C"
                    ON CONFLICT DO NOTHING
                    RETURNING queue
                    """).P("now", tx.Now).Arr("queues", queues, NpgsqlDbType.Text);
                var created = (await tx.QueryAsync(insert, r => r.GetString(0), mutation: true).ConfigureAwait(false)).ToHashSet(StringComparer.Ordinal);
                using var read = tx.Sql("""
                    SELECT queue, paused, revision FROM runwick.queue_controls
                    WHERE application = @application AND queue = ANY (@queues::text[])
                    ORDER BY queue COLLATE "C"
                    """).Arr("queues", queues, NpgsqlDbType.Text);
                var seeds = await tx.QueryAsync(read, r => new QueueSeed(r.GetString(0), created.Contains(r.GetString(0)), r.GetBoolean(1), r.GetInt64(2))).ConfigureAwait(false);
                var value = new QueueSeedResult(seeds);
                return new PgOutcome<QueueSeedResult>.Commit(StoreReply<QueueSeedResult>.Applied(value, tx.Now), ReceiptCodec.Queues(value));
            },
            (tx, row) => Task.FromResult(StoreReply<QueueSeedResult>.Applied(ReceiptCodec.ReadQueues(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    public async Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterScope, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterScopes(command.Body));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.RegisterScope, command.NotAfter, fp, PgLane.Producer),
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<ScopeSeedResult>.Rollback(tx.Reject<ScopeSeedResult>(StoreStatus.CommandExpired, "command_expired"));
                }

                var seeds = await ProvisionScopesAsync(tx, command.Body.Scopes).ConfigureAwait(false);
                var value = new ScopeSeedResult([.. seeds.Values
                    .OrderBy(s => s.Kind, Internal.Utf8Order.Instance).ThenBy(s => s.Key, Internal.Utf8Order.Instance)]);
                return new PgOutcome<ScopeSeedResult>.Commit(StoreReply<ScopeSeedResult>.Applied(value, tx.Now), ReceiptCodec.Scopes(value));
            },
            (tx, row) => Task.FromResult(StoreReply<ScopeSeedResult>.Applied(ReceiptCodec.ReadScopes(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    /// <summary>Seed-if-absent in sorted (kind, key) order, then resolve IDs without row locks (04 §7.1).</summary>
    static async Task<Dictionary<(string Kind, string Key), ScopeSeed>> ProvisionScopesAsync(PgTx tx, IReadOnlyList<ScopeRequirement> scopes)
    {
        var distinct = scopes
            .GroupBy(s => (s.Kind, s.Key)).Select(g => g.First())
            .OrderBy(s => s.Kind, Internal.Utf8Order.Instance).ThenBy(s => s.Key, Internal.Utf8Order.Instance).ToList();
        var result = new Dictionary<(string, string), ScopeSeed>();
        if (distinct.Count == 0)
        {
            return result;
        }

        var ids = distinct.Select(_ => Guid.CreateVersion7()).ToArray();
        var kinds = distinct.Select(s => s.Kind).ToArray();
        var keys = distinct.Select(s => s.Key).ToArray();
        var capacities = distinct.Select(s => s.Capacity).ToArray();
        using var insert = tx.Sql("""
            INSERT INTO runwick.concurrency_scopes (
                application, scope_id, scope_kind, scope_key, capacity, fencing_enabled, created_at, updated_at)
            SELECT @application, s.scope_id, s.kind, s.key, s.capacity, (s.kind = 'resource' AND s.capacity = 1),
                   runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp())
            FROM unnest(@ids::uuid[], @kinds::text[], @keys::text[], @capacities::integer[]) AS s(scope_id, kind, key, capacity)
            ORDER BY s.kind COLLATE "C", s.key COLLATE "C"
            ON CONFLICT (application, scope_kind, scope_key) DO NOTHING
            RETURNING scope_id
            """).Arr("ids", ids, NpgsqlDbType.Uuid).Arr("kinds", kinds, NpgsqlDbType.Text)
                .Arr("keys", keys, NpgsqlDbType.Text).Arr("capacities", capacities, NpgsqlDbType.Integer);
        var created = (await tx.QueryAsync(insert, r => r.GetGuid(0), mutation: true).ConfigureAwait(false)).ToHashSet();
        using var read = tx.Sql("""
            SELECT c.scope_id, c.scope_kind, c.scope_key, c.capacity, c.policy_revision, c.fencing_enabled
            FROM runwick.concurrency_scopes AS c
            JOIN unnest(@kinds::text[], @keys::text[]) AS s(kind, key) ON c.scope_kind = s.kind AND c.scope_key = s.key
            WHERE c.application = @application
            """).Arr("kinds", kinds, NpgsqlDbType.Text).Arr("keys", keys, NpgsqlDbType.Text);
        var configured = distinct.ToDictionary(s => (s.Kind, s.Key), s => s.Capacity);
        foreach (var seed in await tx.QueryAsync(read, r => new ScopeSeed(
            r.GetGuid(0), r.GetString(1), r.GetString(2), false, r.GetInt32(3), 0, r.GetInt64(4), r.GetBoolean(5))).ConfigureAwait(false))
        {
            result[(seed.Kind, seed.Key)] = seed with { Created = created.Contains(seed.ScopeId), ConfiguredCapacity = configured[(seed.Kind, seed.Key)] };
        }

        return result;
    }

    // ------------------------------------------------------------------ shared readers

    static JsonObject PolicyJson(RetryPolicy r) => new()
    {
        ["backoff"] = r.Backoff,
        ["evaluatorVersion"] = r.EvaluatorVersion,
        ["initialDelay"] = r.InitialDelayMs,
        ["jitterKind"] = r.JitterKind,
        ["maximumDelay"] = r.MaximumDelayMs,
    };

    static RetryPolicy ReadRetry(string json)
    {
        var o = JsonNode.Parse(json)!.AsObject();
        return new RetryPolicy(o["backoff"]!.GetValue<string>(), o["evaluatorVersion"]!.GetValue<int>(), o["jitterKind"]!.GetValue<string>(),
            o["initialDelay"]!.GetValue<long>(), o["maximumDelay"]!.GetValue<long>());
    }

    static (Dictionary<string, string> Business, Dictionary<string, string> Diagnostic) ReadHeaders(string json)
    {
        var o = JsonNode.Parse(json)!.AsObject();
        static Dictionary<string, string> Map(JsonNode? n) =>
            n is JsonObject m ? m.ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal) : new(StringComparer.Ordinal);
        return (Map(o["business"]), Map(o["diagnostic"]));
    }

    /// <summary>The <c>schedule</c> member of a payload row's headers (scheduled jobs only).</summary>
    static JobScheduleInfo? ReadScheduleHeader(string json) =>
        JsonNode.Parse(json)!.AsObject()["schedule"] is JsonObject s
            ? new JobScheduleInfo(s["key"]!.GetValue<string>(), PgTime.FromMs(s["scheduledAt"]!.GetValue<long>()),
                PgTime.FromMs(s["coveredFrom"]!.GetValue<long>()), PgTime.FromMs(s["coveredUntil"]!.GetValue<long>()))
            : null;
}
