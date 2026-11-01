using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Peer;
using Runwick.Harness.Proxy;
using Runwick.Hosting;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Harness.Store;

internal enum StoreProfile { DirectPrimary, DirectPrimaryViaProxy }

/// <summary>Per-test PostgreSQL store fixture (07 §2.1).</summary>
internal sealed class DriverFixture : IAsyncDisposable
{
    readonly List<TestClient> _clients = [];
    readonly List<PeerClient> _peers = [];

    DriverFixture(StoreServer server, string ns, string database, StoreProfile profile, PgFaultProxy? proxy)
    {
        Server = server;
        Namespace = ns;
        DatabaseName = database;
        Profile = profile;
        Proxy = proxy;
    }

    public StoreServer Server { get; }

    public string Namespace { get; }

    /// <summary>Default application name (= namespace).</summary>
    public string Application => Namespace;

    public string DatabaseName { get; }

    public StoreProfile Profile { get; }

    public PgFaultProxy? Proxy { get; }

    public HarnessFaultInjector Faults { get; } = new();

    /// <summary>Direct (unproxied) admin connection string for the per-test database.</summary>
    public string AdminConnectionString => Server.ConnectionStringFor(DatabaseName, "rw:harness:admin");

    public static async Task<DriverFixture> StartAsync(string isolatedNamespace, StoreProfile profile = StoreProfile.DirectPrimary)
    {
        var server = await StoreServer.SharedAsync().ConfigureAwait(false);
        var db = await server.CreateDatabaseAsync(isolatedNamespace).ConfigureAwait(false);
        PgFaultProxy? proxy = profile == StoreProfile.DirectPrimaryViaProxy
            ? new PgFaultProxy(server.Database.Host, server.Database.Port)
            : null;
        return new DriverFixture(server, isolatedNamespace, db, profile, proxy);
    }

    /// <summary>Client connection string tagged <c>rw:&lt;identity&gt;</c> (the driver appends <c>:control</c>/<c>:work</c>), routed through the proxy when the profile says so.</summary>
    public string ClientConnectionString(string identity, string? role = null)
    {
        var cs = new NpgsqlConnectionStringBuilder(Server.ConnectionStringFor(DatabaseName, role is null ? $"rw:{identity}" : $"rw:{identity}:{role}"))
        {
            IncludeErrorDetail = false,
        }.ConnectionString;
        return Proxy is null ? cs : Proxy.Rewrite(cs);
    }

    /// <summary>Opens an in-process client over the real registry, dispatcher, store and SQL.</summary>
    public async Task<TestClient> OpenClient(string processIdentity = "local", Action<RunwickBuilder>? configure = null,
        string? application = null, TimeProvider? time = null, bool ensureReady = true)
    {
        if (processIdentity != "local")
        {
            throw new NotSupportedException("Peer clients are opened with OpenPeer(processIdentity).");
        }

        var client = await TestClient.CreateAsync(ClientConnectionString(processIdentity), application ?? Application, Faults,
            configure, time, ensureReady).ConfigureAwait(false);
        _clients.Add(client);
        return client;
    }

    public Task<HarnessSnapshot> ReadCommittedSnapshot() => SnapshotReader.ReadAsync(AdminConnectionString);

    /// <summary>
    /// Replays every job's committed history through the ReferenceModel (15 §12.2 rule 4); empty when all are accepted.
    /// See <see cref="CommittedHistory"/> for the sources and the lease limit.
    /// </summary>
    public Task<IReadOnlyList<string>> HistoryViolations() => CommittedHistory.ViolationsAsync(AdminConnectionString);

    public FaultHandle ArmFault(FaultPoint point, FaultSpec action, FaultTarget target, int times = 1) =>
        Faults.Arm(point, action, target, times);

    public static async Task WaitAtBarrier(FaultHandle handle, TimeSpan? timeout = null) =>
        await handle.Barrier.WaitReachedAsync(timeout ?? TimeSpan.FromSeconds(30)).ConfigureAwait(false);

    public static void ReleaseBarrier(FaultHandle handle) => handle.Barrier.Release();

    public async Task RestartStore(RestartMode mode, TimeSpan downtime = default)
    {
        await Server.Database.RestartAsync(mode, downtime).ConfigureAwait(false);
        Proxy?.Retarget(Server.Database.Host, Server.Database.Port);
    }

    /// <summary>Starts a peer OS process (07 §2.4) connected to this store.</summary>
    public async Task<PeerClient> OpenPeer(string processIdentity, int maxExecutions = 3, string? application = null)
    {
        var peer = await PeerClient.StartAsync(processIdentity).ConfigureAwait(false);
        _peers.Add(peer);
        await peer.OpenAsync(ClientConnectionString(processIdentity), application ?? Application, maxExecutions).ConfigureAwait(false);
        return peer;
    }

    /// <summary>Internal SetQueueState/1 through a store opened for the application.</summary>
    public async Task SetQueuePaused(string application, string queue, bool paused)
    {
        var store = NewStore();
        await using (store.ConfigureAwait(false))
        {
            await store.DescribeAsync(application, CancellationToken.None).ConfigureAwait(false);
            var reply = await store.SetQueueStateAsync(application, queue, paused, null, Guid.CreateVersion7(),
                DateTimeOffset.UtcNow.AddSeconds(30), CancellationToken.None).ConfigureAwait(false);
            if (reply.Status != StoreStatus.Applied)
            {
                throw new InvalidOperationException($"SetQueueState failed: {reply}");
            }
        }
    }

    /// <summary>Test-only receipt cleanup: deletes receipts whose retain_until is before the given instant (07 I-3, 04 §7.16).</summary>
    public async Task<int> PruneReceipts(string retentionClass, DateTimeOffset retainUntilBefore)
    {
        var conn = new NpgsqlConnection(AdminConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            using var cmd = new NpgsqlCommand("""
                DELETE FROM runwick.storage_commands AS c
                USING (
                    SELECT s.application, s.retention_class, s.command_id
                    FROM runwick.storage_commands AS s
                    WHERE s.retention_class = @class AND s.retain_until < @before
                    ORDER BY s.retain_until, s.command_id
                    FOR UPDATE SKIP LOCKED
                ) AS d
                WHERE c.application = d.application AND c.retention_class = d.retention_class AND c.command_id = d.command_id
                """, conn);
            cmd.Parameters.AddWithValue("class", retentionClass);
            cmd.Parameters.AddWithValue("before", retainUntilBefore.UtcDateTime);
            return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    public async Task<DateTimeOffset> StoreNow()
    {
        var conn = new NpgsqlConnection(AdminConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            using var cmd = new NpgsqlCommand("SELECT clock_timestamp()", conn);
            var value = (DateTime)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
            return new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc));
        }
    }

    /// <summary>Reads one scalar with admin SQL (assertions only).</summary>
    public async Task<long> CountAsync(string sql, params (string Name, object Value)[] parameters)
    {
        var conn = new NpgsqlConnection(AdminConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            using var cmd = new NpgsqlCommand(sql, conn);
            foreach (var (name, value) in parameters)
            {
                cmd.Parameters.AddWithValue(name, value);
            }

            return Convert.ToInt64(await cmd.ExecuteScalarAsync().ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    /// <summary>Runs admin SQL against the per-test database (setup only; never used to fake protocol effects under test).</summary>
    public async Task ExecAsync(string sql, params (string Name, object Value)[] parameters)
    {
        var conn = new NpgsqlConnection(AdminConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            using var cmd = new NpgsqlCommand(sql, conn);
            foreach (var (name, value) in parameters)
            {
                cmd.Parameters.AddWithValue(name, value);
            }

            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }

    internal PgStore NewStore(string identity = "harness")
    {
        var sources = new PgDataSources(ClientConnectionString(identity), "runwick:" + Application);
        var runner = new PgRunner(sources, Faults, TimeProvider.System);
        return new PgStore(sources, runner, TimeProvider.System);
    }

    public async Task Stop()
    {
        Faults.DisarmAll();
        foreach (var c in _clients)
        {
            await c.DisposeAsync().ConfigureAwait(false);
        }

        _clients.Clear();
        foreach (var p in _peers)
        {
            await p.DisposeAsync().ConfigureAwait(false);
        }

        _peers.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await Stop().ConfigureAwait(false);
        if (Proxy is not null)
        {
            await Proxy.DisposeAsync().ConfigureAwait(false);
        }

        await Server.DropDatabaseAsync(DatabaseName).ConfigureAwait(false);
    }
}
