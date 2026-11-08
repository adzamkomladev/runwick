using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Npgsql;
using Runwick;
using Runwick.Postgres.Migrations;

/// <summary>A scratch store for one scenario: its own PostgreSQL database, SQLite file or Redis database number. Removed on dispose.</summary>
sealed class Scratch : IAsyncDisposable
{
    static int _redisDb;
    readonly Func<Task>? _drop;

    Scratch(StoreKind kind, string conn, string dir, Func<Task>? drop)
    {
        Kind = kind;
        Conn = conn;
        Dir = dir;
        _drop = drop;
    }

    public StoreKind Kind { get; }

    public string Conn { get; }

    /// <summary>A temporary directory for sinks and files.</summary>
    public string Dir { get; }

    public string App { get; } = "scn" + Guid.NewGuid().ToString("N")[..8];

    public static string PgAdmin => Environment.GetEnvironmentVariable("RUNWICK_TEST_PG")
        ?? "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres";

    public static string RedisBase => Environment.GetEnvironmentVariable("RUNWICK_TEST_REDIS") ?? "127.0.0.1:56392";

    public string Sink(string name = "sink") => Path.Combine(Dir, name);

    public static async Task<string> NewPgDatabaseAsync(string tag, bool migrate = true)
    {
        var name = "scn_" + tag + "_" + Guid.NewGuid().ToString("N")[..8];
        await using (var admin = new NpgsqlConnection(PgAdmin))
        {
            await admin.OpenAsync();
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE {name}", admin);
            await cmd.ExecuteNonQueryAsync();
        }

        var conn = new NpgsqlConnectionStringBuilder(PgAdmin) { Database = name, Pooling = false }.ConnectionString;
        if (migrate)
        {
            await RunwickPostgresMigrations.MigrateAsync(conn);
        }

        return conn;
    }

    public static async Task DropPgDatabaseAsync(string conn)
    {
        var name = new NpgsqlConnectionStringBuilder(conn).Database;
        await using var admin = new NpgsqlConnection(PgAdmin);
        await admin.OpenAsync();
        await using var cmd = new NpgsqlCommand($"DROP DATABASE IF EXISTS {name} WITH (FORCE)", admin);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<Scratch> CreateAsync(StoreKind kind, string tag)
    {
        var dir = Directory.CreateTempSubdirectory("rw-scn-").FullName;
        switch (kind)
        {
            case StoreKind.Pg:
                var pg = await NewPgDatabaseAsync(tag);
                return new Scratch(kind, pg, dir, () => DropPgDatabaseAsync(pg));
            case StoreKind.Sqlite:
                var sq = $"Data Source={Path.Combine(dir, "runwick.db")}";
                await RunwickSqliteMigrations.MigrateAsync(sq);
                return new Scratch(kind, sq, dir, null);
            default:
                var db = Interlocked.Increment(ref _redisDb) % 15 + 1;
                var rd = $"{RedisBase},defaultDatabase={db}";
                await FlushRedisAsync(db);
                await RunwickRedisMigrations.MigrateAsync(rd);
                return new Scratch(kind, rd, dir, () => FlushRedisAsync(db));
        }
    }

    static async Task FlushRedisAsync(int db)
    {
        var o = StackExchange.Redis.ConfigurationOptions.Parse(RedisBase);
        o.AllowAdmin = true;
        await using var mux = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(o);
        await mux.GetServer(mux.GetEndPoints().Single()).FlushDatabaseAsync(db);
    }

    public Task SetAdmissionAsync(string mode) => Kind switch
    {
        StoreKind.Pg => RunwickPostgresMigrations.SetAdmissionModeAsync(Conn, App, mode, "scenarios"),
        StoreKind.Sqlite => RunwickSqliteMigrations.SetAdmissionModeAsync(Conn, App, mode, "scenarios"),
        _ => RunwickRedisMigrations.SetAdmissionModeAsync(Conn, App, mode, "scenarios"),
    };

    /// <summary>The operator call a user runs once before the first schedule (spec 13 §6.1, 23 §3.6; CLI: <c>store activate-capability</c>).</summary>
    public Task ActivateSchedulerAsync() => Kind switch
    {
        StoreKind.Pg => RunwickPostgresMigrations.ActivateCapabilityAsync(Conn, App, "scheduler", "scenarios"),
        StoreKind.Sqlite => RunwickSqliteMigrations.ActivateCapabilityAsync(Conn, App, "scheduler", "scenarios"),
        _ => RunwickRedisMigrations.ActivateCapabilityAsync(Conn, App, "scheduler", "scenarios"),
    };

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_drop is not null)
            {
                await _drop();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"cleanup: {ex.Message}");
        }

        try
        {
            Directory.Delete(Dir, true);
        }
        catch (IOException)
        {
        }
    }
}

/// <summary>A child process of this same program.</summary>
sealed class Child : IAsyncDisposable
{
    readonly Process _p;
    readonly List<string> _out = [];
    readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    Child(Process p, string name)
    {
        _p = p;
        Name = name;
        p.OutputDataReceived += (_, e) =>
        {
            if (e.Data is null)
            {
                return;
            }

            lock (_out) { _out.Add(e.Data); }
            if (e.Data == "ready")
            {
                _ready.TrySetResult();
            }
        };
        p.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (_out) { _out.Add("stderr: " + e.Data); }
            }
        };
        p.Exited += (_, _) => _ready.TrySetResult();
        p.EnableRaisingEvents = true;
        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
    }

    public string Name { get; }

    public int ExitCode => _p.ExitCode;

    public bool HasExited => _p.HasExited;

    public string Output { get { lock (_out) { return string.Join(" | ", _out); } } }

    public static Child Start(string name, IEnumerable<string> args, IDictionary<string, string>? env = null, string? exe = null, bool program = true)
    {
        var self = Environment.ProcessPath!;
        var psi = new ProcessStartInfo(exe ?? self) { RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true };
        if (exe is null && Path.GetFileNameWithoutExtension(self) == "dotnet" && program)
        {
            psi.ArgumentList.Add(typeof(Child).Assembly.Location);
        }

        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        foreach (var (k, v) in env ?? new Dictionary<string, string>())
        {
            psi.Environment[k] = v;
        }

        return new Child(new Process { StartInfo = psi }, name);
    }

    public static Child Node(NodeOptions o) => Start(o.Name, o.ToArgs());

    /// <summary>Waits for "ready" (or exit); throws with the child's output if it exited instead.</summary>
    public async Task ReadyAsync(int seconds = 60)
    {
        await _ready.Task.WaitAsync(TimeSpan.FromSeconds(seconds));
        if (_p.HasExited)
        {
            throw new InvalidOperationException($"{Name} exited {_p.ExitCode} before ready: {Output}");
        }
    }

    public async Task<int> ExitAsync(int seconds = 120)
    {
        await _p.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(seconds));
        return _p.ExitCode;
    }

    /// <summary>Closes stdin (the node's stop signal) and waits for a clean exit.</summary>
    public async Task StopAsync()
    {
        if (_p.HasExited)
        {
            return;
        }

        try
        {
            _p.StandardInput.Close();
            await _p.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(40));
        }
        catch (TimeoutException)
        {
            _p.Kill(true);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_p.HasExited)
        {
            _p.Kill(true);
        }

        await _p.WaitForExitAsync();
        _p.Dispose();
    }
}

static class Wait
{
    public static async Task UntilAsync(Func<bool> condition, int seconds, string what)
    {
        var end = DateTime.UtcNow.AddSeconds(seconds);
        while (!condition())
        {
            if (DateTime.UtcNow > end)
            {
                throw new TimeoutException($"timed out after {seconds}s waiting for {what}");
            }

            await Task.Delay(100);
        }
    }

    public static int Count(string sink) => Directory.Exists(sink) ? Directory.GetFiles(sink).Length : 0;

    /// <summary>Per process name: how many jobs it ran.</summary>
    public static Dictionary<string, int> ByProcess(string sink) =>
        Directory.GetFiles(sink).GroupBy(f => File.ReadAllText(f).Split(' ')[0]).ToDictionary(g => g.Key, g => g.Count());

    /// <summary>The most jobs that were running at one instant, across every process (from the start/end ticks each job left).</summary>
    public static int PeakOverlap(string sink)
    {
        var events = new List<(long T, int D)>();
        foreach (var f in Directory.GetFiles(sink))
        {
            var p = File.ReadAllText(f).Split(' ');
            events.Add((long.Parse(p[1]), 1));
            events.Add((long.Parse(p[2]), -1));
        }

        int cur = 0, peak = 0;
        foreach (var (_, d) in events.OrderBy(e => e.T).ThenBy(e => e.D))
        {
            cur += d;
            peak = Math.Max(peak, cur);
        }

        return peak;
    }
}

/// <summary>A TCP relay in front of a store. <see cref="Cut"/> drops every connection and refuses new ones until <see cref="Restore"/>: an outage.</summary>
sealed class TcpGate : IAsyncDisposable
{
    readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    readonly string _host;
    readonly int _port;
    readonly List<TcpClient> _open = [];
    volatile bool _cut;

    public TcpGate(string host, int port)
    {
        (_host, _port) = (host, port);
        _listener.Start();
        _ = AcceptAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public void Cut()
    {
        _cut = true;
        lock (_open)
        {
            foreach (var c in _open)
            {
                c.Close();
            }

            _open.Clear();
        }
    }

    public void Restore() => _cut = false;

    async Task AcceptAsync()
    {
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync();
                if (_cut)
                {
                    client.Close();
                    continue;
                }

                _ = RelayAsync(client);
            }
        }
        catch (Exception ex) when (ex is ObjectDisposedException or SocketException)
        {
        }
    }

    async Task RelayAsync(TcpClient client)
    {
        try
        {
            var server = new TcpClient();
            await server.ConnectAsync(_host, _port);
            lock (_open) { _open.Add(client); _open.Add(server); }
            var a = client.GetStream();
            var b = server.GetStream();
            await Task.WhenAny(a.CopyToAsync(b), b.CopyToAsync(a));
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
        }
        finally
        {
            client.Close();
        }
    }

    public ValueTask DisposeAsync()
    {
        _listener.Stop();
        Cut();
        return ValueTask.CompletedTask;
    }
}
