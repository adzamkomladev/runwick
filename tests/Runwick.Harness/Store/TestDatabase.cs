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

    /// <summary>
    /// A separate server on the harness-built libfaketime image (07 I-9) for store-clock steps (TIME-13); needs Docker.
    /// </summary>
    public static async Task<TestDatabase> StartFakeTimeAsync()
    {
        const string Tag = "runwick-pg-faketime:18";
        var dir = Path.Combine(AppContext.BaseDirectory, "Docker", "faketime");
        using (var build = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("docker", ["build", "-q", "--build-arg", "BASE=" + Image, "-t", Tag, dir])
               { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false })!)
        {
            var error = await build.StandardError.ReadToEndAsync().ConfigureAwait(false);
            await build.WaitForExitAsync().ConfigureAwait(false);
            if (build.ExitCode != 0)
            {
                throw new InvalidOperationException("docker build of the faketime image failed: " + error);
            }
        }

        var container = new PostgreSqlBuilder(Tag)
            .WithUsername("postgres")
            .WithPassword("pg")
            .WithDatabase("rw")
            .WithCommand("-c", "fsync=on", "-c", "synchronous_commit=on")
            .WithLabel("runwick.test", "true")
            .Build();
        await container.StartAsync().ConfigureAwait(false);
        return new TestDatabase(container, Normalize(container.GetConnectionString()));
    }

    /// <summary>Sets the store clock to real time + <paramref name="offset"/> (faketime image only).</summary>
    public Task StepClockAsync(TimeSpan offset) =>
        SetClockAsync((offset < TimeSpan.Zero ? "-" : "+") + ((long)Math.Abs(offset.TotalSeconds)).ToString(System.Globalization.CultureInfo.InvariantCulture) + "s");

    /// <summary>Stops the store clock at the whole-second instant <paramref name="at"/> (libfaketime absolute form; faketime image only).</summary>
    public Task FreezeClockAsync(DateTimeOffset at) =>
        at.UtcTicks % TimeSpan.TicksPerSecond != 0
            ? throw new ArgumentException("libfaketime freezes at whole seconds.", nameof(at))
            : SetClockAsync(at.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture));

    async Task SetClockAsync(string spec)
    {
        var result = await (_container ?? throw new InvalidOperationException("no container"))
            .ExecAsync(["sh", "-c", $"echo '{spec}' > /etc/faketimerc"]).ConfigureAwait(false);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException("clock step failed: " + result.Stderr);
        }
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

        // The downtime runs from the kill (or the stop request): stopping counts toward it. Stop and start go through
        // the Docker CLI and readiness is a direct connection polled every 250 ms, so the outage is the requested
        // downtime plus server start and crash recovery only (the Testcontainers wait strategy adds seconds of exec
        // polling on a loaded host). The fixed host port keeps the address stable.
        // A checkpoint first keeps crash recovery short: after minutes of suite traffic, WAL replay alone added ~20 s
        // to a 20 s Kill outage (G-1 run 6). Committed data is durable either way; only the restart time changes.
        if (mode == RestartMode.Kill)
        {
            var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(_connectionString) { Pooling = false }.ConnectionString);
            await using (admin.ConfigureAwait(false))
            {
                await admin.OpenAsync().ConfigureAwait(false);
                using var checkpoint = new NpgsqlCommand("CHECKPOINT", admin) { CommandTimeout = 120 };
                await checkpoint.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        var down = System.Diagnostics.Stopwatch.StartNew();
        await DockerAsync(mode == RestartMode.Kill ? ["kill", "--signal", "KILL", _container.Id] : ["stop", "--time", "30", _container.Id]).ConfigureAwait(false);
        await DockerAsync(["wait", _container.Id]).ConfigureAwait(false);
        if (downtime - down.Elapsed > TimeSpan.Zero)
        {
            await Task.Delay(downtime - down.Elapsed).ConfigureAwait(false);
        }

        NpgsqlConnection.ClearAllPools();
        await DockerAsync(["start", _container.Id]).ConfigureAwait(false);
        var ready = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                var conn = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(_connectionString) { Pooling = false, Timeout = 2 }.ConnectionString);
                await using (conn.ConfigureAwait(false))
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    using var cmd = new NpgsqlCommand("SELECT 1", conn);
                    await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                    break;
                }
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException or IOException && ready.Elapsed < TimeSpan.FromSeconds(120))
            {
                await Task.Delay(250).ConfigureAwait(false);
            }
        }

        NpgsqlConnection.ClearAllPools();
    }

    static async Task DockerAsync(string[] args)
    {
        using var p = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("docker", args)
            { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false })!;
        var stderr = p.StandardError.ReadToEndAsync();
        await p.StandardOutput.ReadToEndAsync().ConfigureAwait(false);
        await p.WaitForExitAsync().ConfigureAwait(false);
        if (p.ExitCode != 0)
        {
            throw new InvalidOperationException($"docker {string.Join(' ', args)} exited {p.ExitCode}: {await stderr.ConfigureAwait(false)}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }
}
