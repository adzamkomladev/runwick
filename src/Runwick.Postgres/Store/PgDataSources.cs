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

/// <summary>Runwick-owned data sources (control 6, work 8) and their call-class gates (04 §9).</summary>
internal sealed class PgDataSources : IAsyncDisposable
{
    readonly string _connectionString;
    readonly string _applicationNamePrefix;
    readonly Lazy<NpgsqlDataSource> _control;
    readonly Lazy<NpgsqlDataSource> _work;
    readonly Dictionary<PgLane, SemaphoreSlim> _lanes = new()
    {
        [PgLane.Renewal] = new SemaphoreSlim(4, 4),
        [PgLane.Settlement] = new SemaphoreSlim(2, 2),
        [PgLane.Admission] = new SemaphoreSlim(4, 4),
        [PgLane.Producer] = new SemaphoreSlim(2, 2),
        [PgLane.Background] = new SemaphoreSlim(2, 2),
    };

    public PgDataSources(string connectionString, string applicationNamePrefix)
    {
        _connectionString = connectionString;
        _applicationNamePrefix = applicationNamePrefix;
        _control = new(() => Build("control", 6));
        _work = new(() => Build("work", 8));
    }

    public string ConnectionString => _connectionString;

    public NpgsqlDataSource For(PgLane lane) => lane is PgLane.Renewal or PgLane.Settlement ? _control.Value : _work.Value;

    public SemaphoreSlim Gate(PgLane lane) => _lanes[lane];

    NpgsqlDataSource Build(string role, int max)
    {
        var csb = new NpgsqlConnectionStringBuilder(_connectionString)
        {
            Enlist = false,
            Pooling = true,
            MinPoolSize = 0,
            MaxPoolSize = max,
            Multiplexing = false,
            MaxAutoPrepare = 0,
            ApplicationName = $"{_applicationNamePrefix}:{role}",
            CommandTimeout = 0,
            CancellationTimeout = 2000,
            KeepAlive = 15,
        };
        if (csb.Timeout is 0 or > 5)
        {
            csb.Timeout = 5;
        }

        return new NpgsqlDataSourceBuilder(csb.ConnectionString).Build();
    }

    public async ValueTask DisposeAsync()
    {
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
