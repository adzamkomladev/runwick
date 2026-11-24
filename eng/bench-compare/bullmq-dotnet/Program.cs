// BullMQ for .NET benchmark (official port, NuGet "BullMQ"): same modes and JSON fields as the other harnesses (../ci/README.md).
//   dotnet bench-bullmq-dotnet.dll --backend redis|postgres [--redis 127.0.0.1:56392] [--conn <pg admin>] --mode enqueue|bulk|drain|steady
//     [--n 20000] [--calls 1] [--workers 16] [--rate 100] [--seconds 30] [--max-seconds 1800] [--out f.json]
// Redis is FLUSHALLed; PostgreSQL gets a freshly created database. One Worker with Concurrency = --workers. Jobs are removed on
// completion keeping the last 1000 (as the Node harness does). Completions are counted on the Worker's Completed event.
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using BullMQ;
using BullMQ.Postgres;
using Npgsql;

var a = args.Select((v, i) => (v, i)).Where(x => x.v.StartsWith("--")).ToDictionary(x => x.v[2..], x => x.i + 1 < args.Length && !args[x.i + 1].StartsWith("--") ? args[x.i + 1] : "true");
string Opt(string k, string d) => a.TryGetValue(k, out var v) ? v : d;
var backend = Opt("backend", "redis"); var mode = Opt("mode", "drain");
int n = int.Parse(Opt("n", "20000")), calls = int.Parse(Opt("calls", "1")), workers = int.Parse(Opt("workers", "16"));
double rate = double.Parse(Opt("rate", "100")), seconds = double.Parse(Opt("seconds", "30")), maxSeconds = double.Parse(Opt("max-seconds", "1800"));
var outFile = Opt("out", "");
int chunk = int.Parse(Opt("batch", "1000"));
void Phase(string name) => Console.Error.WriteLine($"@@PHASE {name} {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"); // read by ci/run.py
var cpuIters = int.TryParse(Environment.GetEnvironmentVariable("BENCH_CPU_ITERS"), out var ci) ? ci : 0;
var version = typeof(Queue).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];
var pad = new string('x', 1024 - 32);

QueueBaseOptions Base(QueueBaseOptions o)
{
    if (backend == "postgres")
    {
        var cs = new NpgsqlConnectionStringBuilder(Opt("conn", "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres")) { Database = "bench_bullmq_dotnet" };
        o.Postgres = new PostgresOptions { ConnectionString = cs.ConnectionString, PoolSize = Math.Max(20, workers + calls + 4) };
    }
    else o.Connection = ConnectionOptions.FromString(Opt("redis", "127.0.0.1:56392"));
    return o;
}

if (backend == "postgres")
{
    await using var admin = new NpgsqlConnection(Opt("conn", "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres"));
    await admin.OpenAsync();
    foreach (var sql in new[] { "DROP DATABASE IF EXISTS bench_bullmq_dotnet WITH (FORCE)", "CREATE DATABASE bench_bullmq_dotnet" })
        await new NpgsqlCommand(sql, admin).ExecuteNonQueryAsync();
}
else
{
    using var mux = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(Opt("redis", "127.0.0.1:56392") + ",allowAdmin=true");
    foreach (var s in mux.GetServers()) await s.FlushAllDatabasesAsync();
}

var jobOpts = new JobsOptions { RemoveOnComplete = new KeepJobs { Count = 1000 }, RemoveOnFail = new KeepJobs { Count = 1000 } };
await using var queue = new Queue("noop", (QueueOptions)Base(new QueueOptions { DefaultJobOptions = jobOpts }));
await queue.WaitUntilReadyAsync();
var result = new Dictionary<string, object?> { ["system"] = $"bullmq-dotnet {version} ({backend})", ["mode"] = mode, ["workers"] = workers };
long completed = 0, failed = 0; var lat = new ConcurrentBag<double>();

Worker StartWorker() => new("noop", (job, ct) =>
{
    if (cpuIters > 0)
    {
        var b = new byte[1024]; b.AsSpan().Fill((byte)'x'); Span<byte> d = stackalloc byte[32];
        for (var i = 0; i < cpuIters; i++) { SHA256.HashData(b, d); d.CopyTo(b); }
    }
    return Task.FromResult<object?>(null);
}, (WorkerOptions)Base(new WorkerOptions { Concurrency = workers, Autorun = false })); // listeners attach before RunAsync, so no event is missed

void Hook(Worker w)
{
    w.Completed += (job, _) =>
    {
        Interlocked.Increment(ref completed);
        if (mode == "steady") lat.Add(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - job.Timestamp);
    };
    w.Failed += (job, _) => Interlocked.Increment(ref failed);
    _ = w.RunAsync();
}

// Jobs by state as the store holds them (completed ones are trimmed to 1000, so the completed figure is the worker's events).
async Task<Dictionary<string, long>> States() => new()
{
    ["waiting"] = await queue.GetWaitingCountAsync(), ["active"] = await queue.GetActiveCountAsync(), ["delayed"] = await queue.GetDelayedCountAsync(),
    ["failed"] = await queue.GetFailedCountAsync(), ["completedEvents"] = Interlocked.Read(ref completed), ["failedEvents"] = Interlocked.Read(ref failed),
};
result["settings"] = new Dictionary<string, object?>
{
    ["store"] = backend, ["mode"] = mode, ["payloadBytes"] = 1024, ["callers"] = calls, ["workerSlots"] = workers, ["bulkChunk"] = chunk,
    ["removeOnComplete"] = 1000, ["serverGC"] = System.Runtime.GCSettings.IsServerGC, ["cores"] = Environment.ProcessorCount,
};

async Task Enqueue(int count, ConcurrentBag<double>? l)
{
    var next = 0;
    await Task.WhenAll(Enumerable.Range(0, calls).Select(async _ =>
    {
        while (Interlocked.Increment(ref next) <= count)
        {
            var t = Stopwatch.GetTimestamp();
            await queue.AddAsync("noop", new { id = next, pad });
            l?.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
        }
    }));
}

async Task Bulk(int count)
{
    for (var i = 0; i < count; i += chunk)
        await queue.AddBulkAsync(Enumerable.Range(i, Math.Min(chunk, count - i)).Select(k => ("noop", (object)new { id = k, pad }, (JobsOptions)null!)));
}

static double Pct(List<double> s, double p) => s.Count == 0 ? 0 : Math.Round(s[Math.Clamp((int)Math.Floor(p * s.Count) - 1, 0, s.Count - 1)], 2);

if (mode == "enqueue")
{
    var l = new ConcurrentBag<double>(); Phase("timed-start"); var sw = Stopwatch.StartNew();
    await Enqueue(n, l); Phase("timed-end");
    var s = l.OrderBy(x => x).ToList();
    result["jobs"] = n; result["calls"] = calls; result["insertsPerSec"] = Math.Round(n / sw.Elapsed.TotalSeconds, 1);
    result["accepted"] = await queue.GetWaitingCountAsync();
    result["insertLatencyMs"] = new { p50 = Pct(s, .5), p95 = Pct(s, .95), p99 = Pct(s, .99) };
}
else if (mode == "bulk")
{
    Phase("timed-start"); var sw = Stopwatch.StartNew(); await Bulk(n); Phase("timed-end");
    result["jobs"] = n; result["batch"] = chunk; result["calls"] = 1; result["insertsPerSec"] = Math.Round(n / sw.Elapsed.TotalSeconds, 1);
    result["accepted"] = await queue.GetWaitingCountAsync();
}
else if (mode == "drain")
{
    Phase("seed-start"); await Bulk(n); Phase("seed-end");
    result["readyBeforeStart"] = await States();
    await using var w = StartWorker(); Phase("timed-start");
    var sw = Stopwatch.StartNew();
    Hook(w);
    var progress = new List<double[]>(); var nextProgress = 1.0;
    // A failed job is an outcome, not a missing one: stop when every job has completed or failed, and report both.
    while (Interlocked.Read(ref completed) + Interlocked.Read(ref failed) < n && sw.Elapsed.TotalSeconds < maxSeconds)
    {
        await Task.Delay(5);
        if (sw.Elapsed.TotalSeconds >= nextProgress) { progress.Add([Math.Round(sw.Elapsed.TotalSeconds, 1), Interlocked.Read(ref completed) + Interlocked.Read(ref failed)]); nextProgress = Math.Floor(sw.Elapsed.TotalSeconds) + 1; }
    }
    var el = sw.Elapsed.TotalSeconds; var done = Interlocked.Read(ref completed); Phase("timed-end");
    result["progress"] = progress; result["failed"] = Interlocked.Read(ref failed);
    result["clocks"] = new { workerStartSeconds = Math.Round(el, 3), workerStartJobsPerSec = Math.Round(done / el, 1) };
    result["jobs"] = n; result["finished"] = done; result["drainSeconds"] = Math.Round(el, 3); result["completionsPerSec"] = Math.Round(done / el, 1);
    await w.CloseAsync(false);
}
else
{
    await using var w = StartWorker(); Hook(w); await w.WaitUntilReadyAsync(); await Task.Delay(500);
    Phase("timed-start"); var sw = Stopwatch.StartNew(); var inserted = 0; var inflight = 0; var pend = new List<Task>(); var next = 0.0;
    while (sw.Elapsed.TotalSeconds < seconds)
    {
        if (Volatile.Read(ref inflight) < 16)
        {
            Interlocked.Increment(ref inflight);
            pend.Add(queue.AddAsync("noop", new { pad }).ContinueWith(_ => { Interlocked.Increment(ref inserted); Interlocked.Decrement(ref inflight); }));
        }
        next += 1000 / rate; var wait = next - sw.Elapsed.TotalMilliseconds;
        if (wait > 0) await Task.Delay(TimeSpan.FromMilliseconds(wait));
    }
    await Task.WhenAll(pend);
    var dl = Stopwatch.StartNew();
    while (Interlocked.Read(ref completed) < inserted && dl.Elapsed.TotalSeconds < 60) await Task.Delay(20);
    Phase("timed-end");
    var s = lat.OrderBy(x => x).ToList();
    result["offeredPerSec"] = rate; result["inserted"] = inserted; result["completed"] = Interlocked.Read(ref completed);
    result["completionsPerSec"] = Math.Round(Interlocked.Read(ref completed) / sw.Elapsed.TotalSeconds, 1);
    result["latencyMs"] = new { p50 = Pct(s, .5), p95 = Pct(s, .95), p99 = Pct(s, .99), max = Pct(s, 1) };
    await w.CloseAsync(false);
}

result["outcome"] = await States();
var text = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
Console.WriteLine(text);
if (outFile != "") await File.WriteAllTextAsync(outFile, text);
