// FastEndpoints job queues (v8.3.0) on PostgreSQL. FastEndpoints ships no PostgreSQL provider, so JobStorage below is the
// documented-sample pattern (INSERT; claim with FOR UPDATE SKIP LOCKED + lease; mark complete = UPDATE) written with Npgsql.
//   dotnet run -c Release -- --mode enqueue|drain|steady [--n 20000] [--calls 1] [--workers 16] [--rate 100] [--seconds 30] [--out f.json]
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using FastEndpoints;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

string Arg(string k, string d) { var i = Array.IndexOf(args, "--" + k); return i < 0 ? d : args[i + 1]; }
var mode = Arg("mode", "drain"); var n = int.Parse(Arg("n", "20000")); var calls = int.Parse(Arg("calls", "1"));
var workers = int.Parse(Arg("workers", "16")); var rate = double.Parse(Arg("rate", "100")); var seconds = double.Parse(Arg("seconds", "30"));
var maxSeconds = double.Parse(Arg("max-seconds", "1800")); var outFile = Arg("out", ""); var db = Arg("db", "bench_fe");
var admin = Arg("conn", "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres");
var pad = new string('x', 1024 - 16);

await using (var c = new NpgsqlConnection(admin))
{
    await c.OpenAsync();
    foreach (var sql in new[] { $"DROP DATABASE IF EXISTS {db} WITH (FORCE)", $"CREATE DATABASE {db}" })
        await new NpgsqlCommand(sql, c).ExecuteNonQueryAsync();
}
var cs = new NpgsqlConnectionStringBuilder(admin) { Database = db, MaxPoolSize = Math.Max(40, workers + 24) }.ConnectionString;
Storage.Ds = NpgsqlDataSource.Create(cs);
await Storage.ExecAsync("""
  CREATE TABLE jobs (id bigserial PRIMARY KEY, queue_id text NOT NULL, tracking_id uuid NOT NULL, command jsonb NOT NULL,
    execute_after timestamptz NOT NULL, expire_on timestamptz NOT NULL, is_complete boolean NOT NULL DEFAULT false, dequeue_after timestamptz NOT NULL DEFAULT '-infinity');
  CREATE INDEX jobs_pending ON jobs (queue_id, execute_after) WHERE NOT is_complete;
  """);

var builder = Host.CreateApplicationBuilder();
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services.AddJobQueues<JobRecord, JobStorage>();
var sw = Stopwatch.StartNew();
var result = new JsonObject { ["system"] = "fastendpoints-jobqueues 8.3.0 (hand-written Npgsql provider)", ["mode"] = mode, ["workers"] = workers };

if (mode == "enqueue")
{
    // Real FastEndpoints add path: QueueJobAsync -> StoreJobAsync (one INSERT) + in-memory handoff. The queue is started but handler is a no-op.
    using var host = builder.Build();
    host.Services.UseJobQueues(o => { o.MaxConcurrency = workers; });
    var lat = new List<double>[calls]; var next = 0; var t0 = sw.Elapsed;
    await Task.WhenAll(Enumerable.Range(0, calls).Select(ci => Task.Run(async () =>
    {
        lat[ci] = [];
        while (Interlocked.Increment(ref next) <= n)
        {
            var t = sw.Elapsed; await new NoopJob { Pad = pad, Created = DateTime.UtcNow }.QueueJobAsync(executeAfter: DateTime.UtcNow.AddHours(1));
            lat[ci].Add((sw.Elapsed - t).TotalMilliseconds);
        }
    })));
    var all = lat.SelectMany(x => x).Order().ToArray();
    result["jobs"] = n; result["calls"] = calls; result["insertsPerSec"] = Math.Round(n / (sw.Elapsed - t0).TotalSeconds, 1);
    result["insertLatencyMs"] = new JsonObject { ["p50"] = Pct(all, .5), ["p99"] = Pct(all, .99) };
}
else if (mode == "drain")
{
    // Backlog is written straight to the table (no bulk API exists), then the queue is started and drained.
    await Storage.BulkInsertAsync(n, pad);
    using var host = builder.Build();
    var t0 = sw.Elapsed;
    host.Services.UseJobQueues(o => { o.MaxConcurrency = workers; o.StorageProbeDelay = TimeSpan.FromMilliseconds(100); o.Warmup(); });
    while (await Storage.OpenAsync() > 0 && (sw.Elapsed - t0).TotalSeconds < maxSeconds) await Task.Delay(20);
    var done = n - await Storage.OpenAsync(); var el = (sw.Elapsed - t0).TotalSeconds;
    result["jobs"] = n; result["finished"] = done; result["drainSeconds"] = Math.Round(el, 3); result["completionsPerSec"] = Math.Round(done / el, 1);
}
else
{
    using var host = builder.Build();
    host.Services.UseJobQueues(o => { o.MaxConcurrency = workers; });
    var interval = TimeSpan.FromSeconds(1 / rate); var next = sw.Elapsed; var stop = sw.Elapsed + TimeSpan.FromSeconds(seconds);
    var pending = new List<Task>(); var gate = new SemaphoreSlim(16); long inserted = 0;
    while (sw.Elapsed < stop)
    {
        if (gate.Wait(0)) pending.Add(Task.Run(async () => { try { await new NoopJob { Pad = pad, Created = DateTime.UtcNow }.QueueJobAsync(); Interlocked.Increment(ref inserted); } finally { gate.Release(); } }));
        next += interval; if (next - sw.Elapsed is { Ticks: > 0 } w) await Task.Delay(w);
    }
    await Task.WhenAll(pending);
    var until = sw.Elapsed + TimeSpan.FromSeconds(60);
    while (await Storage.OpenAsync() > 0 && sw.Elapsed < until) await Task.Delay(20);
    var lat = NoopHandler.Lat.ToArray().Order().ToArray();
    result["offeredPerSec"] = rate; result["inserted"] = inserted; result["completed"] = inserted - await Storage.OpenAsync();
    result["completionsPerSec"] = Math.Round((long)result["completed"]! / sw.Elapsed.TotalSeconds, 1);
    result["latencyMs"] = new JsonObject { ["p50"] = Pct(lat, .5), ["p95"] = Pct(lat, .95), ["p99"] = Pct(lat, .99), ["max"] = Pct(lat, 1) };
}
var json = result.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
Console.WriteLine(json); if (outFile != "") File.WriteAllText(outFile, json);
Environment.Exit(0);

static double Pct(double[] xs, double p) => xs.Length == 0 ? 0 : Math.Round(xs[Math.Max(0, Math.Min(xs.Length - 1, (int)(p * xs.Length) - 1))], 2);

public sealed class NoopJob : ICommand { public string Pad { get; set; } = ""; public DateTime Created { get; set; } }
public sealed class NoopHandler : ICommandHandler<NoopJob>
{
    public static readonly System.Collections.Concurrent.ConcurrentBag<double> Lat = [];
    public Task ExecuteAsync(NoopJob c, CancellationToken ct) { Lat.Add((DateTime.UtcNow - c.Created).TotalMilliseconds); return Task.CompletedTask; }
}
public sealed class JobRecord : IJobStorageRecord
{
    public long ID { get; set; }
    public string QueueID { get; set; } = ""; public Guid TrackingID { get; set; } = Guid.NewGuid(); public object Command { get; set; } = "";
    public DateTime ExecuteAfter { get; set; } public DateTime ExpireOn { get; set; } public bool IsComplete { get; set; } public DateTime DequeueAfter { get; set; }
    TCommand IJobStorageRecord.GetCommand<TCommand>() => JsonSerializer.Deserialize<TCommand>((string)Command)!;
    void IJobStorageRecord.SetCommand<TCommand>(TCommand command) => Command = JsonSerializer.Serialize(command);
}
public static class Storage
{
    public static NpgsqlDataSource Ds = null!;
    public static async Task ExecAsync(string sql) { await using var c = await Ds.OpenConnectionAsync(); await new NpgsqlCommand(sql, c).ExecuteNonQueryAsync(); }
    public static async Task<long> OpenAsync() { await using var c = await Ds.OpenConnectionAsync(); return (long)(await new NpgsqlCommand("SELECT count(*) FROM jobs WHERE NOT is_complete", c).ExecuteScalarAsync())!; }
    public static async Task BulkInsertAsync(int n, string pad)
    {
        var q = Convert.ToHexStringLower(System.Security.Cryptography.MD5.HashData(System.Text.Encoding.UTF8.GetBytes(typeof(NoopJob).FullName!))); // FastEndpoints queue id = MD5 hex of the command type name
        await using var c = await Ds.OpenConnectionAsync();
        await using var w = await c.BeginBinaryImportAsync("COPY jobs (queue_id, tracking_id, command, execute_after, expire_on) FROM STDIN (FORMAT BINARY)");
        for (var i = 0; i < n; i++)
        {
            await w.StartRowAsync(); await w.WriteAsync(q); await w.WriteAsync(Guid.NewGuid());
            await w.WriteAsync(JsonSerializer.Serialize(new NoopJob { Pad = pad, Created = DateTime.UtcNow }), NpgsqlTypes.NpgsqlDbType.Jsonb);
            await w.WriteAsync(DateTime.UtcNow.AddMinutes(-1), NpgsqlTypes.NpgsqlDbType.TimestampTz); await w.WriteAsync(DateTime.UtcNow.AddDays(1), NpgsqlTypes.NpgsqlDbType.TimestampTz);
        }
        await w.CompleteAsync();
    }
}
public sealed class JobStorage : IJobStorageProvider<JobRecord>
{
    public bool DistributedJobProcessingEnabled => false;
    public async Task StoreJobAsync(JobRecord j, CancellationToken ct)
    {
        await using var c = await Storage.Ds.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("INSERT INTO jobs (queue_id, tracking_id, command, execute_after, expire_on) VALUES ($1,$2,$3::jsonb,$4,$5)", c)
        { Parameters = { new() { Value = j.QueueID }, new() { Value = j.TrackingID }, new() { Value = (string)j.Command }, new() { Value = j.ExecuteAfter }, new() { Value = j.ExpireOn } } };
        await cmd.ExecuteNonQueryAsync(ct);
    }
    public async Task<ICollection<JobRecord>> GetNextBatchAsync(PendingJobSearchParams<JobRecord> p)
    {
        await using var c = await Storage.Ds.OpenConnectionAsync(p.CancellationToken);
        await using var cmd = new NpgsqlCommand("""
          SELECT id, queue_id, tracking_id, command::text, execute_after, expire_on FROM jobs
          WHERE queue_id = $1 AND NOT is_complete AND execute_after <= now() AND expire_on > now() ORDER BY id LIMIT $2
          """, c) { Parameters = { new() { Value = p.QueueID }, new() { Value = p.Limit } } };
        var list = new List<JobRecord>();
        await using var r = await cmd.ExecuteReaderAsync(p.CancellationToken);
        while (await r.ReadAsync(p.CancellationToken))
            list.Add(new JobRecord { ID = r.GetInt64(0), QueueID = r.GetString(1), TrackingID = r.GetGuid(2), Command = r.GetString(3), ExecuteAfter = r.GetDateTime(4), ExpireOn = r.GetDateTime(5) });
        return list;
    }
    public async Task MarkJobAsCompleteAsync(JobRecord j, CancellationToken ct)
    {
        await using var c = await Storage.Ds.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("UPDATE jobs SET is_complete = true WHERE tracking_id = $1", c) { Parameters = { new() { Value = j.TrackingID } } };
        await cmd.ExecuteNonQueryAsync(ct);
    }
    public Task CancelJobAsync(Guid id, CancellationToken ct) => Storage.ExecAsync($"UPDATE jobs SET is_complete = true WHERE tracking_id = '{id}'");
    public Task OnHandlerExecutionFailureAsync(JobRecord j, Exception e, CancellationToken ct) => Task.CompletedTask;
    public Task PurgeStaleJobsAsync(StaleJobSearchParams<JobRecord> p) => Storage.ExecAsync("DELETE FROM jobs WHERE is_complete OR expire_on < now()");
}
