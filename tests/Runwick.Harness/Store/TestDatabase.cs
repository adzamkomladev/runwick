using Npgsql;
using Testcontainers.PostgreSql;

namespace Runwick.Harness.Store;

public enum RestartMode { Graceful, Kill }

/// <summary>
/// One PostgreSQL server per test assembly. Uses <c>RUNWICK_TEST_PG</c> (a connection string,
/// no Docker) when set; otherwise starts <c>RUNWICK_TEST_PG_IMAGE</c> (default <c>postgres:18</c>)
/// with fsync and synchronous commit on.
/// </summary>
public sealed class TestDatabase : IAsyncDisposable
{
    static readonly SemaphoreSlim Gate = new(1, 1);
    static TestDatabase? _shared;

    readonly PostgreSqlContainer? _container;
    string _connectionString;

    TestDatabase(PostgreSqlContainer? container, string connectionString)
    {
        _container = container;
        _connectionString = connectionString;
    }

    public static string? ExternalConnectionString => Environment.GetEnvironmentVariable("RUNWICK_TEST_PG");

    public static string Image => Environment.GetEnvironmentVariable("RUNWICK_TEST_PG_IMAGE") is { Length: > 0 } image
        ? image
        : "postgres:18";

    /// <summary>True when the server runs in a container this process controls.</summary>
    public bool CanRestart => _container is not null;

    public string ConnectionString => _connectionString;

    public string Host => new NpgsqlConnectionStringBuilder(_connectionString).Host!;

    public int Port => new NpgsqlConnectionStringBuilder(_connectionString).Port;

    public static async Task<TestDatabase> SharedAsync()
    {
        await Gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return _shared ??= await StartAsync().ConfigureAwait(false);
        }
        finally
        {
            Gate.Release();
        }
    }

    public static async Task<TestDatabase> StartAsync()
    {
        if (ExternalConnectionString is { Length: > 0 } external)
        {
            return new TestDatabase(null, Normalize(external));
        }

        var container = new PostgreSqlBuilder(Image)
            .WithUsername("postgres")
            .WithPassword("pg")
            .WithDatabase("rw")
            .WithCommand("-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "max_connections=400")
            .WithLabel("runwick.test", "true")
            // A fixed host port keeps the address stable across RestartStore.
            .WithPortBinding(FreePort(), 5432)
            .Build();
        await container.StartAsync().ConfigureAwait(false);
        return new TestDatabase(container, Normalize(container.GetConnectionString()));
    }

    static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    static string Normalize(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = true,
            MaxPoolSize = 50,
            IncludeErrorDetail = true,
            Timeout = 15,
            CommandTimeout = 60,
        };
        return builder.ConnectionString;
    }

    /// <summary>Connection string with an application name, so the wire proxy and logs can tell clients apart.</summary>
    public string For(string applicationName, bool pooling = true)
    {
        var builder = new NpgsqlConnectionStringBuilder(_connectionString)
        {
            ApplicationName = applicationName,
            Pooling = pooling,
        };
        return builder.ConnectionString;
    }

    public async Task RestartAsync(RestartMode mode, TimeSpan downtime = default)
    {
        if (_container is null)
        {
            throw new InvalidOperationException("RestartStore needs a container; RUNWICK_TEST_PG points at an external server.");
        }

        if (mode == RestartMode.Kill)
        {
            await _container.ExecAsync(["bash", "-c", "kill -9 $(head -1 /var/lib/postgresql/data/postmaster.pid 2>/dev/null || head -1 /var/lib/postgresql/*/docker/postmaster.pid)"]).ConfigureAwait(false);
        }

        await _container.StopAsync().ConfigureAwait(false);
        if (downtime > TimeSpan.Zero)
        {
            await Task.Delay(downtime).ConfigureAwait(false);
        }

        await _container.StartAsync().ConfigureAwait(false);
        NpgsqlConnection.ClearAllPools();
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }
}
