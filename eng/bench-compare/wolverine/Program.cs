// WolverineFx benchmark: a queue of NoopMessage on Wolverine's own PostgreSQL, Redis (streams) or SQLite transport.
// Same modes and JSON fields as the other harnesses (../ci/README.md).
//   dotnet bench-wolverine.dll --transport postgres|redis|sqlite [--redis 127.0.0.1:56392] [--conn <pg admin>] --mode enqueue|bulk|drain|steady
//     [--n 20000] [--calls 1] [--workers 16] [--rate 100] [--seconds 30] [--max-seconds 1800] [--out f.json]
// Producers send with IMessageBus.SendAsync. Consumers are one listener with MaximumParallelMessages = --workers.
// Redis publishing is SendInline: SendAsync returns once Redis holds the message (the default is an in-memory
// buffer, which would time nothing); PostgreSQL and SQLite queues are always durable. DurabilityMode.Solo (one node: no leader election); otherwise Wolverine defaults. Fresh store per run: a new PostgreSQL
// database, a FLUSHALLed Redis, a new SQLite file. Wolverine has no bulk-send API, so bulk reports skipped.
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using JasperFx.Resources;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Wolverine;
using Wolverine.Postgresql;
using Wolverine.Redis;
using Wolverine.Sqlite;

var a = args.Select((v, i) => (v, i)).Where(x => x.v.StartsWith("--")).ToDictionary(x => x.v[2..], x => x.i + 1 < args.Length && !args[x.i + 1].StartsWith("--") ? args[x.i + 1] : "true");
string Opt(string k, string d) => a.TryGetValue(k, out var v) ? v : d;
var transport = Opt("transport", "postgres"); var mode = Opt("mode", "drain");
int n = int.Parse(Opt("n", "20000")), calls = int.Parse(Opt("calls", "1")), workers = int.Parse(Opt("workers", "16"));
double rate = double.Parse(Opt("rate", "100")), seconds = double.Parse(Opt("seconds", "30")), maxSeconds = double.Parse(Opt("max-seconds", "1800"));
var outFile = Opt("out", "");
var adminCs = Opt("conn", "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres");
var redis = Opt("redis", "127.0.0.1:56392");
var version = typeof(WolverineOptions).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];
var pad = new string('x', 1024 - 32);
var sqliteFile = Path.Combine(Path.GetTempPath(), $"bench-wolverine-{Environment.ProcessId}.db");

// Fresh store.
if (transport == "postgres")
{
    await using var admin = new NpgsqlConnection(adminCs);
    await admin.OpenAsync();
    foreach (var sql in new[] { "DROP DATABASE IF EXISTS bench_wolverine WITH (FORCE)", "CREATE DATABASE bench_wolverine" })
        await new NpgsqlCommand(sql, admin).ExecuteNonQueryAsync();
}
else if (transport == "redis")
{
    using var mux = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(redis + ",allowAdmin=true");
    foreach (var s in mux.GetServers()) await s.FlushAllDatabasesAsync();
}
else foreach (var f in new[] { sqliteFile, sqliteFile + "-wal", sqliteFile + "-shm" }) File.Delete(f);

IHost Build(bool listen)
{
    var b = Host.CreateApplicationBuilder();
    b.Logging.ClearProviders(); b.Logging.AddConsole(); b.Logging.SetMinimumLevel(LogLevel.Warning);
    b.Services.AddResourceSetupOnStartup();
    b.UseWolverine(o =>
    {
        o.Durability.Mode = DurabilityMode.Solo;
        o.Discovery.IncludeType<NoopHandler>();
        if (transport == "postgres")
        {
            o.UsePostgresqlPersistenceAndTransport(new NpgsqlConnectionStringBuilder(adminCs) { Database = "bench_wolverine" }.ConnectionString, "wolverine");
            o.PublishMessage<NoopMessage>().ToPostgresqlQueue("bench");
            if (listen) o.ListenToPostgresqlQueue("bench").MaximumParallelMessages(workers);
        }
        else if (transport == "redis")
        {
            o.UseRedisTransport(redis).AutoProvision();
            o.PublishMessage<NoopMessage>().ToRedisStream("bench").SendInline();
            if (listen) o.ListenToRedisStream("bench", "bench-group", StartFrom.Beginning).MaximumParallelMessages(workers); // a new group would skip the backlog
        }
        else
        {
            o.UseSqlitePersistenceAndTransport($"Data Source={sqliteFile}");
            o.PublishMessage<NoopMessage>().ToSqliteQueue("bench");
            if (listen) o.ListenToSqliteQueue("bench").MaximumParallelMessages(workers);
        }
    });
    return b.Build();
}

static double Pct(List<double> s, double p) => s.Count == 0 ? 0 : Math.Round(s[Math.Clamp((int)Math.Floor(p * s.Count) - 1, 0, s.Count - 1)], 2);
var result = new Dictionary<string, object?> { ["system"] = $"wolverine {version} ({transport})", ["mode"] = mode, ["workers"] = workers };

async Task Send(IHost h, int count, int callers, ConcurrentBag<double>? lat)
{
    var next = 0;
    await Task.WhenAll(Enumerable.Range(0, callers).Select(async _ =>
    {
        while (Interlocked.Increment(ref next) <= count)
        {
            using var scope = h.Services.CreateScope();
            var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
            var t = Stopwatch.GetTimestamp();
            await bus.SendAsync(new NoopMessage(pad, 0));
            lat?.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
        }
    }));
}

if (mode == "bulk") result["skipped"] = "Wolverine has no bulk-send API";
else if (mode == "enqueue")
{
    using var h = Build(false); await h.StartAsync();
    await Send(h, 1, 1, null); // warm the sending path
    var l = new ConcurrentBag<double>(); var sw = Stopwatch.StartNew();
    await Send(h, n, calls, l);
    var s = l.OrderBy(x => x).ToList();
    result["jobs"] = n; result["calls"] = calls; result["insertsPerSec"] = Math.Round(n / sw.Elapsed.TotalSeconds, 1);
    result["insertLatencyMs"] = new { p50 = Pct(s, .5), p95 = Pct(s, .95), p99 = Pct(s, .99) };
    await h.StopAsync();
}
else if (mode == "drain")
{
    using (var p = Build(false)) { await p.StartAsync(); await Send(p, n, 16, null); await p.StopAsync(); } // backlog, not timed
    var sw = Stopwatch.StartNew();
    using var h = Build(true); await h.StartAsync();
    double? first = null; // Wolverine compiles its handler code at start; time to the first job shows that cost on its own
    while (Interlocked.Read(ref NoopHandler.Done) < n && sw.Elapsed.TotalSeconds < maxSeconds)
    {
        if (first is null && Interlocked.Read(ref NoopHandler.Done) > 0) first = sw.Elapsed.TotalSeconds;
        await Task.Delay(5);
    }
    var el = sw.Elapsed.TotalSeconds; var done = Interlocked.Read(ref NoopHandler.Done);
    result["jobs"] = n; result["finished"] = done; result["drainSeconds"] = Math.Round(el, 3); result["completionsPerSec"] = Math.Round(done / el, 1);
    result["startupSeconds"] = Math.Round(first ?? el, 2); result["completionsPerSecExcludingStartup"] = Math.Round(done / Math.Max(1e-9, el - (first ?? 0)), 1);
    await h.StopAsync();
}
else
{
    using var h = Build(true); await h.StartAsync();
    await Send(h, 1, 1, null);
    var dl0 = Stopwatch.StartNew(); while (Interlocked.Read(ref NoopHandler.Done) < 1 && dl0.Elapsed.TotalSeconds < 60) await Task.Delay(20);
    Interlocked.Exchange(ref NoopHandler.Done, 0); NoopHandler.Latencies.Clear();
    var bus = h.Services.GetRequiredService<IMessageBus>();
    var sw = Stopwatch.StartNew(); var inserted = 0; var inflight = 0; var pend = new List<Task>(); var next = 0.0;
    while (sw.Elapsed.TotalSeconds < seconds)
    {
        if (Volatile.Read(ref inflight) < 16)
        {
            Interlocked.Increment(ref inflight);
            pend.Add(bus.SendAsync(new NoopMessage(pad, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())).AsTask()
                .ContinueWith(_ => { Interlocked.Increment(ref inserted); Interlocked.Decrement(ref inflight); }));
        }
        next += 1000 / rate; var wait = next - sw.Elapsed.TotalMilliseconds;
        if (wait > 0) await Task.Delay(TimeSpan.FromMilliseconds(wait));
    }
    await Task.WhenAll(pend);
    var dl = Stopwatch.StartNew();
    while (Interlocked.Read(ref NoopHandler.Done) < inserted && dl.Elapsed.TotalSeconds < 60) await Task.Delay(20);
    var done = Interlocked.Read(ref NoopHandler.Done); var s = NoopHandler.Latencies.OrderBy(x => x).ToList();
    result["offeredPerSec"] = rate; result["inserted"] = inserted; result["completed"] = done;
    result["completionsPerSec"] = Math.Round(done / sw.Elapsed.TotalSeconds, 1);
    result["latencyMs"] = new { p50 = Pct(s, .5), p95 = Pct(s, .95), p99 = Pct(s, .99), max = Pct(s, 1) };
    await h.StopAsync();
}
if (transport == "sqlite") foreach (var f in new[] { sqliteFile, sqliteFile + "-wal", sqliteFile + "-shm" }) File.Delete(f);

var text = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
Console.WriteLine(text);
if (outFile != "") await File.WriteAllTextAsync(outFile, text);

public record NoopMessage(string Pad, long SentAtMs);

public class NoopHandler
{
    public static long Done;
    public static readonly ConcurrentBag<double> Latencies = new();
    static readonly int CpuIters = int.TryParse(Environment.GetEnvironmentVariable("BENCH_CPU_ITERS"), out var i) ? i : 0;

    public static void Handle(NoopMessage m)
    {
        if (CpuIters > 0) // shared CPU-heavy job (../ci/README.md)
        {
            var b = new byte[1024]; b.AsSpan().Fill((byte)'x'); Span<byte> d = stackalloc byte[32];
            for (var k = 0; k < CpuIters; k++) { SHA256.HashData(b, d); d.CopyTo(b); }
        }
        if (m.SentAtMs > 0) Latencies.Add(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - m.SentAtMs);
        Interlocked.Increment(ref Done);
    }
}
