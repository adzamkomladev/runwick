using Npgsql;

namespace Runwick.Postgres.Store;

/// <summary>Call classes of 04 §9.1; each is a partition of its data source's connections.</summary>
internal enum PgLane
{
    Renewal,
    Settlement,
    Admission,
    Producer,
    Background,
}

/// <summary>Runwick-owned data sources (control 8, work 8) and their call-class gates (04 §9).</summary>
internal sealed class PgDataSources : IAsyncDisposable
{
    /// <summary>Idle connections the control pool keeps open (04 §9.2, PF-03); half the settlement lane since 2026-11-07.</summary>
    internal const int ControlWarm = 2;

    /// <summary>Statements prepared automatically per physical connection (04 §9.2, PF-03).</summary>
    internal const int AutoPrepare = 64;

    static readonly System.Collections.Concurrent.ConcurrentDictionary<PgDataSources, byte> Live = new();

    readonly string _connectionString;
    readonly string _applicationNamePrefix;
    readonly Lazy<NpgsqlDataSource> _control;
    readonly Lazy<NpgsqlDataSource> _work;
    readonly Dictionary<PgLane, SemaphoreSlim> _lanes = new()
    {
        [PgLane.Renewal] = new SemaphoreSlim(4, 4),
        [PgLane.Settlement] = new SemaphoreSlim(4, 4),
        [PgLane.Admission] = new SemaphoreSlim(4, 4),
        [PgLane.Producer] = new SemaphoreSlim(2, 2),
        [PgLane.Background] = new SemaphoreSlim(2, 2),
    };

    public PgDataSources(string connectionString, string applicationNamePrefix)
    {
        _connectionString = connectionString;
        _applicationNamePrefix = applicationNamePrefix;
        _control = new(() => Build("control", 8));
        _work = new(() => Build("work", 8));
        Live.TryAdd(this, 0);
    }

    /// <summary>
    /// Drops every idle pooled connection of every live store in this process on the migrated database, and with them their prepared
    /// statements; busy connections are dropped when returned. Called after a migration (04 §9.2, PF-03).
    /// </summary>
    internal static void ClearAll(string migratedConnectionString)
    {
        var target = Database(migratedConnectionString);
        foreach (var sources in Live.Keys)
        {
            if (Database(sources._connectionString) == target)
            {
                sources.Clear();
            }
        }
    }

    /// <summary>The server and database a connection string names; stores of other databases keep their pools.</summary>
    static (string?, int, string?) Database(string connectionString)
    {
        var csb = new NpgsqlConnectionStringBuilder(connectionString);
        return (csb.Host?.ToUpperInvariant(), csb.Port, csb.Database);
    }

    internal void Clear()
    {
        if (_control.IsValueCreated)
        {
            _control.Value.Clear();
        }

        if (_work.IsValueCreated)
        {
            _work.Value.Clear();
        }
    }

    public string ConnectionString => _connectionString;

    /// <summary>
    /// The listener session (05 §7.2, PF-07): one unpooled connection per listening component, outside the control and work
    /// pools, so a pool reset never drops its LISTEN and a busy pool never delays a notification.
    /// </summary>
    internal string ListenConnectionString()
    {
        var configured = new NpgsqlConnectionStringBuilder(_connectionString).ApplicationName;
        var prefix = string.IsNullOrEmpty(configured) ? _applicationNamePrefix : configured;
        var csb = new NpgsqlConnectionStringBuilder(_connectionString)
        {
            Enlist = false,
            Pooling = false,
            Multiplexing = false,
            MaxAutoPrepare = 0,
            ApplicationName = $"{prefix}:listen",
            IncludeErrorDetail = false,
            KeepAlive = 15,
        };
        if (csb.Timeout is 0 or > 5)
        {
            csb.Timeout = 5;
        }

        return csb.ConnectionString;
    }

    public NpgsqlDataSource For(PgLane lane) => lane is PgLane.Renewal or PgLane.Settlement ? _control.Value : _work.Value;

    public SemaphoreSlim Gate(PgLane lane) => _lanes[lane];

    NpgsqlDataSource Build(string role, int max)
    {
        var configured = new NpgsqlConnectionStringBuilder(_connectionString).ApplicationName;
        var prefix = string.IsNullOrEmpty(configured) ? _applicationNamePrefix : configured;
        var csb = new NpgsqlConnectionStringBuilder(_connectionString)
        {
            Enlist = false,
            Pooling = true,
            MinPoolSize = role == "control" ? ControlWarm : 0,
            MaxPoolSize = max,
            Multiplexing = false,
            MaxAutoPrepare = AutoPrepare,
            AutoPrepareMinUsages = 2,
            ApplicationName = $"{prefix}:{role}",
            IncludeErrorDetail = false,
            CommandTimeout = 0,
            CancellationTimeout = 2000,
            KeepAlive = 15,

            // No session state ever outlives a transaction on these pools (PF-08): settings are transaction-local
            // (set_config(..., true)), the migration lock and LISTEN use their own connections, and no cursor or temp
            // table is used. The reset sequence (CLOSE ALL … UNLISTEN *, 7 statements on every return) is skipped; an
            // open transaction is still rolled back on return.
            NoResetOnClose = true,
        };
        // A prepared statement is planned for each bind's values, never from a cached generic plan (04 §9.2, PF-03).
        csb.Options = string.IsNullOrWhiteSpace(csb.Options) ? PlanMode : csb.Options + " " + PlanMode;
        if (csb.Timeout is 0 or > 5)
        {
            csb.Timeout = 5;
        }

        return new NpgsqlDataSourceBuilder(csb.ConnectionString).Build();
    }

    internal const string PlanMode = "-c plan_cache_mode=force_custom_plan";

    public async ValueTask DisposeAsync()
    {
        Live.TryRemove(this, out _);
        if (_control.IsValueCreated)
        {
            await _control.Value.DisposeAsync().ConfigureAwait(false);
        }

        if (_work.IsValueCreated)
        {
            await _work.Value.DisposeAsync().ConfigureAwait(false);
        }

        foreach (var gate in _lanes.Values)
        {
            gate.Dispose();
        }
    }
}
