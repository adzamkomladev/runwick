using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Protocol;
using Runwick.Sqlite.Migrations;

namespace Runwick.Sqlite.Store;

/// <summary>The SQLite implementation of the storage protocol (03) per 20 §7: the PostgreSQL steps on one write lock.</summary>
internal sealed partial class SqliteStore(SqExecutor executor, SqRunner runner, TimeProvider time) : IRunwickStore, IDisposable
{
    public const string ManagementRead = "ManagementRead";
    public const string RecordDisclosure = "RecordDisclosure";

    public Guid StoreId => runner.StoreId ?? throw new InvalidOperationException("Describe has not run yet.");

    public string AcknowledgementProfile => AcknowledgementProfiles.SqliteWalFull;

    internal SqRunner Runner => runner;

    internal SqExecutor Executor => executor;

    /// <summary>Describe's engine and file facts (20 §6): sqlite_version, sqlite_source_id, journal_mode, synchronous, topology.</summary>
    public IReadOnlyDictionary<string, string> EngineFacts { get; private set; } = new Dictionary<string, string>();

    public void Dispose() => executor.Dispose();

    async Task<Guid> EnsureStoreIdAsync(string application, CancellationToken ct) =>
        runner.StoreId ?? (await DescribeAsync(application, ct).ConfigureAwait(false)).StoreId;

    static SqOp Op(string application, Guid commandId, string operation, DateTimeOffset notAfter, byte[]? fingerprint, SqLane lane, Guid? storeId = null) =>
        new(application, commandId, operation, SqTime.Floor(notAfter), fingerprint, lane, storeId);

    DateTimeOffset Now() => SqTime.Floor(time.GetUtcNow());

    // ------------------------------------------------------------------ Describe / Register*

    public async Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct)
    {
        string topology;
        try
        {
            topology = SqFile.CheckTopology(SqFile.FilePath(executor.ConnectionString));
        }
        catch (SqProfileException ex)
        {
            throw new StoreIncompatibleException($"{ex.Code}: {ex.Message}");
        }

        try
        {
            return await executor.ReadAsync(conn =>
            {
                var (version, sourceId) = SqFile.CheckEngine(conn);
                SqFile.CheckProfile(conn);
                SqFile.Exec(conn, "BEGIN");
                try
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = """
                        SELECT s.store_id, s.schema_revision, s.protocol_epoch, s.migration_mode, s.compatibility_manifest_version,
                               s.manifest_digest, a.revision, a.required_protocol_epoch, a.required_capabilities, a.admission_mode
                        FROM runwick_store_metadata AS s
                        LEFT JOIN runwick_application_controls AS a ON a.application = @application
                        WHERE s.singleton_id = 1
                        """;
                    cmd.P("application", application);
                    using var r = cmd.ExecuteReader();
                    if (!r.Read())
                    {
                        throw new StoreIncompatibleException("The store has no runwick_store_metadata row; run the migrator.");
                    }

                    ApplicationGate? gate = r.IsDBNull(6) ? null : new ApplicationGate(r.GetInt64(6), r.GetInt32(7), ParseCapabilities(r.GetString(8)), r.GetString(9));
                    var descriptor = new StoreDescriptor(r.Uuid(0), r.GetInt64(1), r.GetInt32(2), r.GetString(3), r.GetInt32(4), r.Blob(5), gate, Now());
                    if (descriptor.SchemaRevision == SqliteMigrator.CurrentRevision && !SqliteMigrator.SchemaMatches(conn))
                    {
                        throw new StoreIncompatibleException("The SQLite schema objects differ from this binary's: schema_incompatible.");
                    }

                    EngineFacts = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["sqlite_version"] = version,
                        ["sqlite_source_id"] = sourceId,
                        ["journal_mode"] = "wal",
                        ["synchronous"] = "full",
                        ["topology"] = topology,
                    };
                    runner.StoreId ??= descriptor.StoreId;
                    return descriptor;
                }
                finally
                {
                    SqFile.Exec(conn, "COMMIT");
                }
            }, ct).ConfigureAwait(false);
        }
        catch (SqProfileException ex)
        {
            throw new StoreIncompatibleException($"{ex.Code}: {ex.Message}");
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 1 && ex.Message.Contains("no such", StringComparison.Ordinal))
        {
            throw new StoreIncompatibleException("The Runwick SQLite schema is not installed; run the migrator.", ex);
        }
        catch (SqliteException ex)
        {
            throw new StoreUnavailableException("Could not read the SQLite store.", ex);
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.RegisterApplication, command.NotAfter, fp, SqLane.Producer),
            tx =>
            {
                var row = tx.Query(tx.Sql("""
                    SELECT revision, required_protocol_epoch, required_capabilities, admission_mode
                    FROM runwick_application_controls WHERE application = @application
                    """), r => new ApplicationGate(r.GetInt64(0), r.GetInt32(1), ParseCapabilities(r.GetString(2)), r.GetString(3)))[0];
                var value = new ApplicationSeedResult(tx.ApplicationSeeded, row);
                return new SqOutcome<ApplicationSeedResult>.Commit(StoreReply<ApplicationSeedResult>.Applied(value, tx.Now), ReceiptCodec.Application(value));
            },
            (tx, row) => StoreReply<ApplicationSeedResult>.Applied(ReceiptCodec.ReadApplication(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }

    public async Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(StoreCommand<RegisterQueuesRequest> command, CancellationToken ct)
    {
        var queues = command.Body.Queues.Distinct().Order(Internal.Utf8Order.Instance).ToArray();
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterQueue, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterQueues(command.Body));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.RegisterQueue, command.NotAfter, fp, SqLane.Producer),
            tx =>
            {
                var created = new HashSet<string>(StringComparer.Ordinal);
                foreach (var q in queues)
                {
                    if (tx.Exec(tx.Sql("""
                        INSERT INTO runwick_queue_controls (application, queue, created_at, updated_at) VALUES (@application, @queue, @now, @now)
                        ON CONFLICT DO NOTHING
                        """).P("queue", q).P("now", tx.Now)) == 1)
                    {
                        created.Add(q);
                    }
                }

                var read = tx.Sql("");
                read.CommandText = $"SELECT queue, paused, revision FROM runwick_queue_controls WHERE application = @application AND queue IN {read.In("q", queues)}";
                var seeds = tx.Query(read, r => new QueueSeed(r.GetString(0), created.Contains(r.GetString(0)), r.Bool(1), r.GetInt64(2)))
                    .OrderBy(s => s.Queue, Internal.Utf8Order.Instance).ToList();
                var value = new QueueSeedResult(seeds);
                return new SqOutcome<QueueSeedResult>.Commit(StoreReply<QueueSeedResult>.Applied(value, tx.Now), ReceiptCodec.Queues(value));
            },
            (tx, row) => StoreReply<QueueSeedResult>.Applied(ReceiptCodec.ReadQueues(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }

    public async Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(StoreCommand<RegisterScopesRequest> command, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.RegisterScope, command.CommandId, command.NotAfter,
            CommandEnvelopes.RegisterScopes(command.Body));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.RegisterScope, command.NotAfter, fp, SqLane.Producer),
            tx =>
            {
                var seeds = ProvisionScopes(tx, command.Body.Scopes);
                var value = new ScopeSeedResult([.. seeds.Values
                    .OrderBy(s => s.Kind, Internal.Utf8Order.Instance).ThenBy(s => s.Key, Internal.Utf8Order.Instance)]);
                return new SqOutcome<ScopeSeedResult>.Commit(StoreReply<ScopeSeedResult>.Applied(value, tx.Now), ReceiptCodec.Scopes(value));
            },
            (tx, row) => StoreReply<ScopeSeedResult>.Applied(ReceiptCodec.ReadScopes(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }

    /// <summary>Seed-if-absent in sorted (kind, key) order, then read the rows (04 §7.1).</summary>
    static Dictionary<(string Kind, string Key), ScopeSeed> ProvisionScopes(SqTx tx, IReadOnlyList<ScopeRequirement> scopes)
    {
        var distinct = scopes
            .GroupBy(s => (s.Kind, s.Key)).Select(g => g.First())
            .OrderBy(s => s.Kind, Internal.Utf8Order.Instance).ThenBy(s => s.Key, Internal.Utf8Order.Instance).ToList();
        var result = new Dictionary<(string, string), ScopeSeed>();
        foreach (var s in distinct)
        {
            var created = tx.Exec(tx.Sql("""
                INSERT INTO runwick_concurrency_scopes (application, scope_id, scope_kind, scope_key, capacity, fencing_enabled, created_at, updated_at)
                VALUES (@application, @id, @kind, @key, @capacity, @fencing, @now, @now)
                ON CONFLICT (application, scope_kind, scope_key) DO NOTHING
                """).P("id", Guid.CreateVersion7()).P("kind", s.Kind).P("key", s.Key).P("capacity", s.Capacity)
                .P("fencing", s.Kind == "resource" && s.Capacity == 1).P("now", tx.Now)) == 1;
            var seed = tx.Query(tx.Sql("""
                SELECT scope_id, scope_kind, scope_key, capacity, policy_revision, fencing_enabled FROM runwick_concurrency_scopes
                WHERE application = @application AND scope_kind = @kind AND scope_key = @key
                """).P("kind", s.Kind).P("key", s.Key), r => new ScopeSeed(r.Uuid(0), r.GetString(1), r.GetString(2), created, r.GetInt32(3),
                    s.Capacity, r.GetInt64(4), r.Bool(5)))[0];
            result[(s.Kind, s.Key)] = seed;
        }

        return result;
    }

    // ------------------------------------------------------------------ shared readers

    static JsonObject PolicyJson(Serialization.RetryPolicy r) => new()
    {
        ["backoff"] = r.Backoff,
        ["evaluatorVersion"] = r.EvaluatorVersion,
        ["initialDelay"] = r.InitialDelayMs,
        ["jitterKind"] = r.JitterKind,
        ["maximumDelay"] = r.MaximumDelayMs,
    };

    static Serialization.RetryPolicy ReadRetry(string json)
    {
        var o = JsonNode.Parse(json)!.AsObject();
        return new Serialization.RetryPolicy(o["backoff"]!.GetValue<string>(), o["evaluatorVersion"]!.GetValue<int>(), o["jitterKind"]!.GetValue<string>(),
            o["initialDelay"]!.GetValue<long>(), o["maximumDelay"]!.GetValue<long>());
    }

    static (Dictionary<string, string> Business, Dictionary<string, string> Diagnostic) ReadHeaders(string json)
    {
        var o = JsonNode.Parse(json)!.AsObject();
        static Dictionary<string, string> Map(JsonNode? n) =>
            n is JsonObject m ? m.ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal) : new(StringComparer.Ordinal);
        return (Map(o["business"]), Map(o["diagnostic"]));
    }

    static JobScheduleInfo? ReadScheduleHeader(string json) =>
        JsonNode.Parse(json)!.AsObject()["schedule"] is JsonObject s
            ? new JobScheduleInfo(s["key"]!.GetValue<string>(), SqTime.FromMs(s["scheduledAt"]!.GetValue<long>()),
                SqTime.FromMs(s["coveredFrom"]!.GetValue<long>()), SqTime.FromMs(s["coveredUntil"]!.GetValue<long>()))
            : null;
}
