using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Postgres.Migrations;
using Runwick.Worker;

namespace Runwick.Workloads;

/// <summary>
/// <c>bench</c>: Runwick's row in the comparison benchmark (<c>eng/bench-compare/run.sh</c>, next to River and pg-boss).
/// One process, one worker pool on queue <c>load</c>, a no-op handler and a 1 KiB payload, against a database that is
/// dropped and recreated for every run (so leftovers from earlier runs never count). The worker uses the shipped
/// PostgreSQL store directly, so it claims with the batched ReserveAndBegin command whenever several slots are free.
/// <list type="bullet">
/// <item><c>drain</c>: dispatch <c>--n</c> jobs first (DispatchMany, 100 per call), then start the worker and time the
/// first claim to the last confirmed finish.</item>
/// <item><c>steady</c>: start the worker, dispatch single jobs at <c>--rate</c>/s for <c>--seconds</c> (one call at a time
/// like the River and pg-boss benches, or open loop with <c>--calls n</c>), and measure dispatch call start to confirmed
/// finish per job; then wait up to 60 s for the rest to finish.</item>
/// </list>
/// Usage: <c>bench --mode drain|steady [--n 20000] [--rate 100] [--seconds 120] [--workers 16] [--payload 1024]
/// [--calls 1] [--conn admin-cs] [--db bench_runwick] [--out file.json] [--trace]</c> (--trace adds per-job stage gaps, held-job and settlement-queue samples, and store call phases). Writes the same JSON shape as the River and pg-boss
/// benches: <c>system</c>, <c>mode</c>, <c>completionsPerSec</c>, <c>latencyMs {p50, p95, p99, max}</c> (steady).
/// </summary>
internal static class BenchMode
{
    const string DefaultAdmin = "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres";
    const int Chunk = 100; // PayloadBounds.MaxBatchItems

    public static async Task<int> RunAsync(Args a)
    {
        var mode = a.OptStr("mode") ?? "drain";
        if (mode is not ("drain" or "steady"))
        {
            Console.Error.WriteLine("--mode must be drain or steady");
            return 2;
        }

        var n = a.Int("n", 20000);
        var rate = a.Double("rate", 100);
        var seconds = a.Double("seconds", 120);
        var workers = a.OptInt("slots") ?? a.Int("workers", 16);
        var payload = a.Int("payload", 1024);
        var database = a.OptStr("db") ?? "bench_runwick";
        var adminCs = a.OptStr("conn") ?? DefaultAdmin;
        var calls = Math.Max(1, a.Int("calls", 1));

        // Fresh database per run.
        var admin = new NpgsqlConnectionStringBuilder(adminCs) { ApplicationName = "bench:admin", Pooling = false }.ConnectionString;
        await G2Orchestrator.ExecAsync(admin, $"DROP DATABASE IF EXISTS {database} WITH (FORCE)").ConfigureAwait(false);
        await G2Orchestrator.ExecAsync(admin, $"CREATE DATABASE {database}").ConfigureAwait(false);
        await RunwickPostgresMigrations.MigrateAsync(new NpgsqlConnectionStringBuilder(adminCs)
            { Database = database, ApplicationName = "bench:migrate", Pooling = false }.ConnectionString).ConfigureAwait(false);
        var storeCs = new NpgsqlConnectionStringBuilder(adminCs)
        {
            Database = database,
            ApplicationName = "bench-runwick",
            Pooling = true,
            MaxPoolSize = Math.Max(100, workers + 16),
        }.ConnectionString;

        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton(new LoadBehaviour(TimeSpan.Zero));
        sc.AddRunwick(r =>
        {
            r.Application("bench").UsePostgres(storeCs);
            LoadRegistration.Register(r, typeCeiling: null);
            r.AddHandler<LoadJob, LoadHandler>();
            r.Producer(o => o.MaxConcurrentCalls = Math.Max(2, calls));
            r.AddWorker(w => w.MaxOutstandingAttempts(workers).Pool("main", p => p.Queues(LoadRegistration.Queue).FixedConcurrency(workers)));
        });
        await using var provider = sc.BuildServiceProvider();
        var engine = provider.GetRequiredService<WorkerEngine>();
        var hosted = provider.GetServices<IHostedService>().Single();
        var dispatcher = provider.GetRequiredService<IJobDispatcher>();

        // Event hook: first claim, finish count, last finish, per-job dispatch-to-finish latency (steady).
        var clock = Stopwatch.StartNew();
        long firstClaimTicks = -1, lastFinishTicks = 0, finished = 0, replayed = 0;
        var dispatchedAt = new ConcurrentDictionary<Guid, long>(); // steady: dispatch call start (clock ticks)
        var finishedAt = new ConcurrentDictionary<Guid, long>();   // steady: finish seen before the receipt returned
        var latency = new LatencyReservoir();
        // --trace: per-job stage gaps (reserved -> handler invoked -> finish confirmed) and per-call store phases.
        var trace = a.Has("trace");
        var stages = new Metrics();
        using var phases = trace ? PhaseListener.Attach(stages) : null;
        var reservedAt = new ConcurrentDictionary<Guid, long>();
        var invokedAt = new ConcurrentDictionary<Guid, long>();
        long reservedCount = 0;
        engine.Hooks.Event = (name, entry) =>
        {
            var now = clock.ElapsedTicks;
            if (trace && entry is not null)
            {
                var id = entry.Reserved.Position.JobId;
                if (name == "reserved")
                {
                    Interlocked.Increment(ref reservedCount);
                    reservedAt[id] = now;
                }
                else if (name == "handlerInvoked" && reservedAt.TryGetValue(id, out var r))
                {
                    invokedAt[id] = now;
                    stages.Observe("stage:reserved->handlerInvoked", Ms(now - r));
                }
                else if (name is "finished" or "finishReplayed")
                {
                    if (invokedAt.TryRemove(id, out var h))
                    {
                        stages.Observe("stage:handlerInvoked->finished", Ms(now - h));
                    }

                    if (reservedAt.TryRemove(id, out var r2))
                    {
                        stages.Observe("stage:reserved->finished", Ms(now - r2));
                    }
                }
            }

            if (name is "reserved" or "started" or "handlerInvoked")
            {
                Interlocked.CompareExchange(ref firstClaimTicks, now, -1);
            }
            else if (name is "finished" or "finishReplayed")
            {
                if (name == "finishReplayed")
                {
                    Interlocked.Increment(ref replayed);
                }

                Interlocked.Increment(ref finished);
                Interlocked.Exchange(ref lastFinishTicks, now);
                if (mode == "steady" && entry is not null)
                {
                    var id = entry.Reserved.Position.JobId;
                    if (dispatchedAt.TryRemove(id, out var started))
                    {
                        latency.Add(Ms(now - started));
                    }
                    else
                    {
                        finishedAt[id] = now;
                    }
                }
            }
        };

        var result = new JsonObject
        {
            ["system"] = "runwick",
            ["mode"] = mode,
            ["workers"] = workers,
            ["payloadBytes"] = payload,
            ["claim"] = "ReserveAndBegin (batched) when more than one slot is free",
        };
        long sequence = 0;
        var failedDispatch = 0L;

        if (mode == "drain")
        {
            var insertWatch = Stopwatch.StartNew();
            for (var i = 0; i < n; i += Chunk)
            {
                var items = Enumerable.Range(0, Math.Min(Chunk, n - i))
                    .Select(_ => new BatchDispatchItem(LoadJob.Create(++sequence, payload))).ToList();
                var receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
                failedDispatch += receipt.Items.Count(x => x.Receipt is null);
            }

            result["insertSeconds"] = Math.Round(insertWatch.Elapsed.TotalSeconds, 3);
            var expected = n - failedDispatch;
            var startTicks = clock.ElapsedTicks;
            await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
            using var sampling = new CancellationTokenSource();
            var sampler = trace ? Task.Run(async () =>
            {
                // Every 10 ms: jobs held by the worker (reserved, not yet confirmed finished) and the settlement queue depth.
                while (!sampling.IsCancellationRequested)
                {
                    stages.Observe("sample:heldJobs", Interlocked.Read(ref reservedCount) - Interlocked.Read(ref finished));
                    stages.Observe("sample:settlementQueue", engine.Completion.Pending);
                    await Task.Delay(10).ConfigureAwait(false);
                }
            }) : Task.CompletedTask;
            var deadline = Stopwatch.StartNew();
            while (Interlocked.Read(ref finished) < expected && deadline.Elapsed < TimeSpan.FromMinutes(30))
            {
                await Task.Delay(5).ConfigureAwait(false);
            }

            await sampling.CancelAsync().ConfigureAwait(false);
            await sampler.ConfigureAwait(false);
            var done = Interlocked.Read(ref finished);
            var first = Interlocked.Read(ref firstClaimTicks);
            var elapsed = Ms(Interlocked.Read(ref lastFinishTicks) - (first < 0 ? startTicks : first)) / 1000;
            result["jobs"] = n;
            result["failedDispatch"] = failedDispatch;
            result["finished"] = done;
            result["startToFirstClaimMs"] = first < 0 ? null : Math.Round(Ms(first - startTicks), 1);
            result["drainSeconds"] = Math.Round(elapsed, 3);
            result["completionsPerSec"] = elapsed > 0 ? Math.Round(done / elapsed, 1) : 0;
        }
        else
        {
            await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
            var interval = TimeSpan.FromSeconds(1 / rate);
            var startTicks = clock.ElapsedTicks;
            var stopAt = clock.Elapsed + TimeSpan.FromSeconds(seconds);
            var next = clock.Elapsed;
            long dispatched = 0;
            var pending = new List<Task>();
            var failures = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
            // --calls 1 (default): one Dispatch at a time on a fixed schedule, like the River and pg-boss steady modes
            // (a slow call delays the next arrival; the schedule catches up). --calls n: open loop, each arrival sent on
            // schedule with up to n calls in flight (beyond that the producer's local admission refuses it).
            while (clock.Elapsed < stopAt)
            {
                var task = DispatchOneAsync(LoadJob.Create(++sequence, payload));
                if (calls == 1)
                {
                    await task.ConfigureAwait(false);
                }
                else
                {
                    pending.Add(task);
                    if (pending.Count >= 256)
                    {
                        pending.RemoveAll(t => t.IsCompleted);
                    }
                }

                next += interval;
                if (next - clock.Elapsed is { Ticks: > 0 } wait)
                {
                    await Task.Delay(wait).ConfigureAwait(false);
                }
            }

            await Task.WhenAll(pending).ConfigureAwait(false);

            async Task DispatchOneAsync(LoadJob job)
            {
                var started = clock.ElapsedTicks;
                try
                {
                    var receipt = await dispatcher.DispatchAsync(job).ConfigureAwait(false);
                    Interlocked.Increment(ref dispatched);
                    var id = receipt.JobId.Value;
                    if (finishedAt.TryRemove(id, out var done))
                    {
                        latency.Add(Ms(done - started));
                        return;
                    }

                    dispatchedAt[id] = started;
                    if (finishedAt.TryRemove(id, out done) && dispatchedAt.TryRemove(id, out _))
                    {
                        latency.Add(Ms(done - started)); // the finish landed between the two checks
                    }
                }
                catch (Exception ex) when (ex is RunwickException or NpgsqlException or TimeoutException)
                {
                    Interlocked.Increment(ref failedDispatch);
                    failures.AddOrUpdate(ex is RunwickException rx ? rx.Code : ex.GetType().Name, 1, (_, c) => c + 1);
                }
            }

            var drainUntil = clock.Elapsed + TimeSpan.FromSeconds(60);
            while (Interlocked.Read(ref finished) < Interlocked.Read(ref dispatched) && clock.Elapsed < drainUntil)
            {
                await Task.Delay(5).ConfigureAwait(false);
            }

            var elapsed = Ms(clock.ElapsedTicks - startTicks) / 1000;
            var s = latency.Summarize();
            result["offeredPerSec"] = rate;
            result["seconds"] = seconds;
            result["inserted"] = dispatched;
            result["dispatchCalls"] = calls;
            result["failedDispatch"] = failedDispatch;
            result["failedDispatchByCode"] = new JsonObject([.. failures.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]);
            result["completed"] = Interlocked.Read(ref finished);
            result["completionsPerSec"] = Math.Round(Interlocked.Read(ref finished) / elapsed, 1);
            result["latencyMs"] = new JsonObject
            {
                ["p50"] = Math.Round(s.P50Ms, 2),
                ["p95"] = Math.Round(s.P95Ms, 2),
                ["p99"] = Math.Round(s.P99Ms, 2),
                ["max"] = Math.Round(s.MaxMs, 2),
            };
            result["latencySamples"] = s.Count;
        }

        result["finishReplayed"] = Interlocked.Read(ref replayed);
        result["batchClaims"] = engine.Pools.Sum(p => Interlocked.Read(ref p.BatchClaims));
        result["reservesSent"] = engine.Pools.Sum(p => Interlocked.Read(ref p.ReservesSent));
        result["invocations"] = Interlocked.Read(ref engine.Invocations);
        if (trace)
        {
            result["finishesSent"] = Interlocked.Read(ref engine.FinishesSent);
            result["finishManyCalls"] = Interlocked.Read(ref engine.Completion.BatchesSent);
            result["trace"] = stages.LatenciesJson();
        }

        result["diagnostics"] = new JsonArray([.. engine.Health().Diagnostics.Select(x => (JsonNode?)x)]);

        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        {
            await hosted.StopAsync(stop.Token).ConfigureAwait(false);
        }

        var json = result.ToJsonString(Json.Indented);
        Console.WriteLine(json);
        if (a.OptStr("out") is { } outFile)
        {
            await File.WriteAllTextAsync(outFile, json).ConfigureAwait(false);
        }

        return 0;
    }

    static double Ms(long stopwatchTicks) => stopwatchTicks * 1000.0 / Stopwatch.Frequency;
}
