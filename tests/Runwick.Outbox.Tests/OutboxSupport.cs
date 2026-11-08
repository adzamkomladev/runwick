using System.Collections.Concurrent;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Hosting;
using Runwick.Outbox.Postgres;
using Runwick.Outbox.Sqlite;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Outbox.Tests;

public enum SourceKind { Postgres, Sqlite }

public static class OutboxData
{
    /// <summary>Every Stage/relay case runs on both source adapters (TX analogues "on both sources").</summary>
    public static TheoryData<SourceKind> Sources => new() { SourceKind.Postgres, SourceKind.Sqlite };

    /// <summary>Both sources times every destination store kind (RUNWICK_TEST_STORES narrows the stores, as for the contract suite).</summary>
    public static TheoryData<SourceKind, StoreKind> SourcesAndStores
    {
        get
        {
            var data = new TheoryData<SourceKind, StoreKind>();
            foreach (var store in Enum.GetValues<StoreKind>().Where(k => Environment.GetEnvironmentVariable("RUNWICK_TEST_STORES") is not { Length: > 0 } only
                || only.Split(',').Contains(k.ToString(), StringComparer.OrdinalIgnoreCase)))
            {
                data.Add(SourceKind.Postgres, store);
                data.Add(SourceKind.Sqlite, store);
            }

            return data;
        }
    }
}

/// <summary>
/// One outbox source (PostgreSQL database or same-host SQLite file) plus relay hosts that deliver into a per-test
/// destination store (PostgreSQL unless a test picks another kind; 14 §9.1). Source and destination are separate databases; nothing here fakes a protocol effect.
/// </summary>
internal sealed class OutboxFixture : IAsyncDisposable
{
    readonly List<OutboxHost> _hosts = [];
    readonly string? _sourceDatabase;
    readonly StoreServer? _sourceServer;
    readonly string? _sqlitePath;

    OutboxFixture(DriverFixture destination, SourceKind kind, string sourceConnectionString, string? sourceDatabase, string? sqlitePath,
        StoreServer? sourceServer = null)
    {
        _sourceServer = sourceServer;
        Destination = destination;
        Kind = kind;
        SourceConnectionString = sourceConnectionString;
        _sourceDatabase = sourceDatabase;
        _sqlitePath = sqlitePath;
    }

    public DriverFixture Destination { get; }

    public SourceKind Kind { get; }

    public string SourceConnectionString { get; }

    /// <summary>The relay's source connection string: <c>rw:outbox:relay</c> on PostgreSQL so OUTBOX-07 can count its sessions.</summary>
    public string RelayConnectionString => Kind == SourceKind.Postgres
        ? new NpgsqlConnectionStringBuilder(SourceConnectionString) { ApplicationName = "rw:outbox:relay" }.ConnectionString
        : SourceConnectionString;

    public Guid SourceStoreId { get; private set; }

    /// <summary>Budgets shortened for tests; still valid under <see cref="OutboxRelayOptions"/> validation.</summary>
    public static void Fast(OutboxRelayOptions o)
    {
        o.LeaseDuration = TimeSpan.FromSeconds(6);
        o.RenewalInterval = TimeSpan.FromSeconds(2);
        o.ProviderCallTimeout = TimeSpan.FromMilliseconds(1500);
        o.IdlePoll = TimeSpan.FromMilliseconds(50);
        o.RetryInitial = TimeSpan.FromMilliseconds(100);
        o.RetryMax = TimeSpan.FromSeconds(1);
    }

    public static async Task<OutboxFixture> StartAsync(string name, SourceKind kind, StoreProfile profile = StoreProfile.DirectPrimary,
        StoreKind store = StoreKind.Postgres)
    {
        var ns = name + "_" + (kind == SourceKind.Postgres ? "pg" : "sl") + "_" + Guid.NewGuid().ToString("N")[..8];
        var destination = await DriverFixture.StartAsync(ns, store, profile);
        OutboxFixture fixture;
        if (kind == SourceKind.Postgres)
        {
            var server = destination.Server ?? await StoreServer.SharedAsync();
            var db = await server.CreateEmptyDatabaseAsync(ns + "_src");
            fixture = new OutboxFixture(destination, kind, server.ConnectionStringFor(db, "rw:outbox:business"), db, null, server);
            fixture.SourceStoreId = await PostgresOutboxSchema.InstallAsync(fixture.SourceConnectionString);
        }
        else
        {
            var path = Path.Combine(Path.GetTempPath(), ns + ".db");
            var cs = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false, DefaultTimeout = 30 }.ConnectionString;
            fixture = new OutboxFixture(destination, kind, cs, null, path);
            fixture.SourceStoreId = await SqliteOutboxSchema.InstallAsync(cs);
        }

        await fixture.SourceExecAsync("CREATE TABLE business_rows (id TEXT PRIMARY KEY)");
        return fixture;
    }

    /// <summary>
    /// A host with <c>AddRunwick</c> (destination) and the source adapter; the relay starts unless <paramref name="start"/> is false.
    /// </summary>
    public async Task<OutboxHost> OpenHost(string identity = "relay", Action<OutboxRelayOptions>? relay = null, OutboxDestination? destination = null,
        string? application = null, TimeProvider? time = null, bool ensureReady = true, bool start = true,
        Func<IOutboxSource, IOutboxSource>? wrapSource = null, Action<RunwickBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSingleton<IFaultInjector>(Destination.Faults);
        if (time is not null)
        {
            services.AddSingleton(time);
        }

        services.AddRunwick(r =>
        {
            Destination.UseStore(r.Application(application ?? Destination.Application), identity);
            if (configure is null)
            {
                TestJobs.Register(r);
            }
            else
            {
                configure(r);
            }
        });
        Action<OutboxRelayOptions> options = o =>
        {
            Fast(o);
            relay?.Invoke(o);
        };
        if (Kind == SourceKind.Postgres)
        {
            services.AddRunwickPostgresOutbox(RelayConnectionString, destination, options);
        }
        else
        {
            services.AddRunwickSqliteOutbox(RelayConnectionString, destination, options);
        }

        if (wrapSource is not null)
        {
            // Last registration wins: the relay sees the wrapper (fault seams around the real adapter, never a fake store).
            services.AddSingleton<IOutboxSource>(_ => wrapSource(Kind == SourceKind.Postgres
                ? new PostgresOutboxSource(RelayConnectionString)
                : new SqliteOutboxSource(RelayConnectionString)));
        }

        var host = new OutboxHost(services.BuildServiceProvider(), Kind);
        _hosts.Add(host);
        if (ensureReady)
        {
            await host.Runtime.EnsureReadyAsync(CancellationToken.None);
        }

        if (start)
        {
            await host.StartRelayAsync();
        }

        return host;
    }

    /// <summary>A second, independently owned source adapter (a stale or competing relay owner in OUTBOX-03).</summary>
    public async Task<FaultySource> NewSourceAsync()
    {
        var source = new FaultySource(Kind == SourceKind.Postgres
            ? new PostgresOutboxSource(RelayConnectionString)
            : new SqliteOutboxSource(RelayConnectionString));
        await source.InitializeAsync(CancellationToken.None);
        return source;
    }

    /// <summary>Opens a caller-owned business transaction on the source (the caller commits; the adapter never does).</summary>
    public async Task<BusinessTransaction> BeginBusinessAsync()
    {
        DbConnection connection = Kind == SourceKind.Postgres ? new NpgsqlConnection(SourceConnectionString) : new SqliteConnection(SourceConnectionString);
        await connection.OpenAsync();
        var tx = Kind == SourceKind.Postgres
            ? (DbTransaction)await ((NpgsqlConnection)connection).BeginTransactionAsync()
            : ((SqliteConnection)connection).BeginTransaction(deferred: false);
        return new BusinessTransaction(connection, tx);
    }

    /// <summary>Business row + Stage in one caller transaction, committed or rolled back by the caller.</summary>
    public async Task<StagedOutboxMessage> StageAsync(OutboxHost host, IQueueJob job, OutboxStageOptions? options = null, bool commit = true, string? businessRow = null)
    {
        await using var business = await BeginBusinessAsync();
        await business.InsertRowAsync(businessRow ?? Guid.NewGuid().ToString("N"));
        var staged = await host.StageAsync(job, business, options);
        if (commit)
        {
            await business.Transaction.CommitAsync();
        }
        else
        {
            await business.Transaction.RollbackAsync();
        }

        return staged;
    }

    public async Task<string?> StateAsync(Guid messageId) =>
        (await SourceScalarAsync<string>(Sql("SELECT state FROM {messages} WHERE message_id = {m}"), messageId));

    public async Task<string?> BlockedReasonAsync(Guid messageId) =>
        await SourceScalarAsync<string>(Sql("SELECT blocked_reason FROM {messages} WHERE message_id = {m}"), messageId);

    public async Task<Guid?> DeliveredJobAsync(Guid messageId)
    {
        var value = await SourceScalarAsync<object>(Sql("SELECT delivered_job_id FROM {messages} WHERE message_id = {m}"), messageId);
        return value switch
        {
            Guid g => g,
            string s => Guid.Parse(s, System.Globalization.CultureInfo.InvariantCulture),
            _ => null,
        };
    }

    /// <summary>The exact staged envelope bytes (the relay imports these; tests reuse them to build transport commands).</summary>
    public async Task<byte[]> EnvelopeAsync(Guid messageId) =>
        (await SourceScalarAsync<byte[]>(Sql("SELECT intent FROM {messages} WHERE message_id = {m}"), messageId))!;

    public async Task<long> SourceCountAsync(string sql) => Convert.ToInt64(await SourceScalarAsync<object>(Sql(sql), null), System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Polls the source row until it reaches <paramref name="state"/> (assertion helper; the relay is not driven by it).</summary>
    public async Task WaitStateAsync(Guid messageId, string state, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        string? last = null;
        while (DateTime.UtcNow < deadline)
        {
            last = await StateAsync(messageId);
            if (last == state)
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.Fail($"Message {messageId} stayed {last ?? "<missing>"}; expected {state}.");
    }

    public Task<long> JobsAsync(string? application = null) =>
        Destination.CountAsync("SELECT count(*) FROM runwick.jobs WHERE application = @a", ("a", application ?? Destination.Application));

    public Task<long> MappingsAsync(string? application = null) =>
        Destination.CountAsync("SELECT count(*) FROM runwick.outbox_deliveries WHERE application = @a", ("a", application ?? Destination.Application));

    public Task<long> MappingsFor(Guid messageId, string? application = null) =>
        Destination.CountAsync("SELECT count(*) FROM runwick.outbox_deliveries WHERE application = @a AND source_store_id = @s AND message_id = @m",
            ("a", application ?? Destination.Application), ("s", SourceStoreId), ("m", messageId));

    /// <summary>Source-side SQL with dialect placeholders: <c>{messages}</c>, <c>{metadata}</c>, <c>{m}</c>.</summary>
    public string Sql(string template) => Kind == SourceKind.Postgres
        ? template.Replace("{messages}", "runwick_outbox.outbox_messages", StringComparison.Ordinal)
            .Replace("{metadata}", "runwick_outbox.outbox_metadata", StringComparison.Ordinal).Replace("{m}", "@m", StringComparison.Ordinal)
        : template.Replace("{messages}", "runwick_outbox_messages", StringComparison.Ordinal)
            .Replace("{metadata}", "runwick_outbox_metadata", StringComparison.Ordinal).Replace("{m}", "$m", StringComparison.Ordinal);

    public async Task SourceExecAsync(string sql, Guid? messageId = null)
    {
        await using var connection = await OpenSourceAsync();
        await using var cmd = Command(connection, sql, messageId);
        await cmd.ExecuteNonQueryAsync();
    }

    async Task<T?> SourceScalarAsync<T>(string sql, Guid? messageId)
    {
        await using var connection = await OpenSourceAsync();
        await using var cmd = Command(connection, sql, messageId);
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? default : (T)value;
    }

    async Task<DbConnection> OpenSourceAsync()
    {
        DbConnection connection = Kind == SourceKind.Postgres
            ? new NpgsqlConnection(new NpgsqlConnectionStringBuilder(SourceConnectionString) { ApplicationName = "rw:outbox:assert" }.ConnectionString)
            : new SqliteConnection(SourceConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    DbCommand Command(DbConnection connection, string sql, Guid? messageId)
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        if (messageId is { } m)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = Kind == SourceKind.Postgres ? "m" : "$m";
            p.Value = Kind == SourceKind.Postgres ? m : SqliteOutboxSource.Text(m);
            cmd.Parameters.Add(p);
        }

        return cmd;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var host in _hosts)
        {
            await host.DisposeAsync();
        }

        _hosts.Clear();
        await Destination.DisposeAsync();
        if (_sourceDatabase is not null)
        {
            await _sourceServer!.DropDatabaseAsync(_sourceDatabase);
        }

        if (_sqlitePath is not null)
        {
            SqliteConnection.ClearAllPools();
            File.Delete(_sqlitePath);
        }
    }
}

/// <summary>A caller-owned business transaction on the source.</summary>
internal sealed class BusinessTransaction(DbConnection connection, DbTransaction transaction) : IAsyncDisposable
{
    public DbConnection Connection => connection;

    public DbTransaction Transaction => transaction;

    public async Task InsertRowAsync(string id)
    {
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = connection is NpgsqlConnection ? "INSERT INTO business_rows (id) VALUES (@id)" : "INSERT INTO business_rows (id) VALUES ($id)";
        var p = cmd.CreateParameter();
        p.ParameterName = connection is NpgsqlConnection ? "id" : "$id";
        p.Value = id;
        cmd.Parameters.Add(p);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<long> CountRowsAsync()
    {
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT count(*) FROM business_rows";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    public async ValueTask DisposeAsync()
    {
        await transaction.DisposeAsync();
        await connection.DisposeAsync();
    }
}

/// <summary>One relay group host: Runwick (destination) plus one outbox source adapter.</summary>
internal sealed class OutboxHost(ServiceProvider services, SourceKind kind) : IAsyncDisposable
{
    bool _started;

    public ServiceProvider Services => services;

    internal OutboxRelay Relay => services.GetRequiredService<OutboxRelay>();

    internal IOutboxSource Source => services.GetRequiredService<IOutboxSource>();

    internal PgStore Store => services.GetRequiredService<PgStore>();

    internal RunwickRuntime Runtime => services.GetRequiredService<RunwickRuntime>();

    public IPreparedJobDispatcher Prepared => services.GetRequiredService<IPreparedJobDispatcher>();

    public Task<StagedOutboxMessage> StageAsync(IQueueJob job, BusinessTransaction business, OutboxStageOptions? options = null) => kind switch
    {
        SourceKind.Postgres => services.GetRequiredService<IPostgresOutbox>().StageAsync(job, (NpgsqlTransaction)business.Transaction, options),
        _ => services.GetRequiredService<ISqliteOutbox>().StageAsync(job, (SqliteTransaction)business.Transaction, options),
    };

    public async Task StartRelayAsync()
    {
        await Relay.StartAsync(CancellationToken.None);
        _started = true;
    }

    /// <summary>Host shutdown with a deadline (14 §5.6): claims stop first, in-flight sends are observed until the deadline.</summary>
    public async Task StopRelayAsync(TimeSpan? deadline = null)
    {
        if (!_started)
        {
            return;
        }

        _started = false;
        using var cts = new CancellationTokenSource(deadline ?? TimeSpan.FromSeconds(5));
        await Relay.StopAsync(cts.Token);
    }

    public async ValueTask DisposeAsync()
    {
        await StopRelayAsync();
        await services.DisposeAsync();
    }
}

/// <summary>Builds <c>ReceiveOutbox/1</c> transport commands exactly as the relay does (14 §5.3), for protocol-level cases.</summary>
internal static class Transport
{
    public static OutboxSourceIdentity Identity(Guid sourceStoreId, Guid messageId) => new(sourceStoreId, messageId);

    public static DateTimeOffset Ms(DateTimeOffset value) => DateTimeOffset.FromUnixTimeMilliseconds(value.ToUnixTimeMilliseconds());

    public static byte[] Fingerprint(PreparedDispatch prepared, OutboxSourceIdentity identity, DateTimeOffset acceptUntil) =>
        OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, acceptUntil.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);

    public static StoreCommand<ReceiveOutboxRequest> Command(PreparedDispatch prepared, OutboxSourceIdentity identity, DateTimeOffset acceptUntil,
        Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var cutoff = Ms(acceptUntil);
        var deadline = notAfter ?? Min(DateTimeOffset.UtcNow + TimeSpan.FromSeconds(20), cutoff);
        return new StoreCommand<ReceiveOutboxRequest>(prepared.Application, commandId ?? Guid.CreateVersion7(), Ms(deadline),
            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), cutoff,
                Fingerprint(prepared, identity, cutoff), prepared.StoreId));
    }

    /// <summary>The same command with a changed field (CMD-02): everything else byte-identical.</summary>
    public static StoreCommand<ReceiveOutboxRequest> With(StoreCommand<ReceiveOutboxRequest> command, DateTimeOffset? notAfter = null,
        ReceiveOutboxRequest? body = null, string? application = null) =>
        new(application ?? command.Application, command.CommandId, notAfter ?? command.NotAfter, body ?? command.Body);

    static DateTimeOffset Min(DateTimeOffset a, DateTimeOffset b) => a < b ? a : b;
}

/// <summary>
/// Fault seam around a real source adapter: drops delivered-mark replies (before or after the write) and counts calls.
/// Every source effect still goes through the adapter's SQL.
/// </summary>
internal sealed class FaultySource(IOutboxSource inner) : IOutboxSource, IAsyncDisposable
{
    int _marksToLoseBefore;
    int _marksToLoseAfter;
    int _claims;
    int _maxClaimBatch;

    public Guid SourceStoreId => inner.SourceStoreId;

    /// <summary>Probability (0–1) that a delivered mark is lost before it reaches the source (OUT-01).</summary>
    public double MarkLossRate { get; set; }

    public int Claims => Volatile.Read(ref _claims);

    public int MaxClaimBatch => Volatile.Read(ref _maxClaimBatch);

    public ConcurrentQueue<(Guid MessageId, Guid JobId)> Marks { get; } = new();

    /// <summary>The next <paramref name="count"/> marks fail before the source write (the relay sees an error, nothing is written).</summary>
    public void LoseMarksBefore(int count) => Volatile.Write(ref _marksToLoseBefore, count);

    /// <summary>The next <paramref name="count"/> marks are written, then the reply is lost.</summary>
    public void LoseMarksAfter(int count) => Volatile.Write(ref _marksToLoseAfter, count);

    public Task InitializeAsync(CancellationToken ct) => inner.InitializeAsync(ct);

    public async Task<IReadOnlyList<OutboxLease>> ClaimAsync(Guid owner, int max, TimeSpan leaseDuration, CancellationToken ct)
    {
        var leases = await inner.ClaimAsync(owner, max, leaseDuration, ct);
        Interlocked.Increment(ref _claims);
        int seen;
        while ((seen = Volatile.Read(ref _maxClaimBatch)) < leases.Count && Interlocked.CompareExchange(ref _maxClaimBatch, leases.Count, seen) != seen)
        {
        }

        return leases;
    }

    public Task<bool> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken ct) => inner.RenewAsync(lease, leaseDuration, ct);

    public Task<bool> RecordTransportAsync(OutboxLease lease, Guid commandId, DateTimeOffset notAfter, CancellationToken ct) =>
        inner.RecordTransportAsync(lease, commandId, notAfter, ct);

    public async Task<bool> MarkDeliveredAsync(OutboxLease lease, OutboxDelivered delivered, CancellationToken ct)
    {
        if (Interlocked.Decrement(ref _marksToLoseBefore) >= 0 || (MarkLossRate > 0 && Random.Shared.NextDouble() < MarkLossRate))
        {
            throw new IOException("Injected loss of the delivered-mark request.");
        }

        var marked = await inner.MarkDeliveredAsync(lease, delivered, ct);
        if (marked)
        {
            Marks.Enqueue((lease.MessageId, delivered.JobId));
        }

        if (Interlocked.Decrement(ref _marksToLoseAfter) >= 0)
        {
            throw new IOException("Injected loss of the delivered-mark reply.");
        }

        return marked;
    }

    public Task<bool> RetryLaterAsync(OutboxLease lease, TimeSpan delay, string error, CancellationToken ct) => inner.RetryLaterAsync(lease, delay, error, ct);

    public Task<bool> BlockAsync(OutboxLease lease, string reason, CancellationToken ct) => inner.BlockAsync(lease, reason, ct);

    public Task<int> PurgeDeliveredAsync(int maxRows, long maxBytes, CancellationToken ct) => inner.PurgeDeliveredAsync(maxRows, maxBytes, ct);

    public async ValueTask DisposeAsync()
    {
        switch (inner)
        {
            case IAsyncDisposable a:
                await a.DisposeAsync();
                break;
            case IDisposable d:
                d.Dispose();
                break;
        }
    }
}
