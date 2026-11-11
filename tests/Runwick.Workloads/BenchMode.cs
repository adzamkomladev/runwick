using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using Runwick.Serialization;
using Runwick.Sqlite.Migrations;
using Runwick.Sqlite.Store;
using Runwick.Worker;

namespace Runwick.Workloads;

/// <summary>
/// <c>bench</c>: Runwick's row in the comparison benchmark (<c>eng/bench-compare/run.sh</c>, next to River and pg-boss).
/// One process, one worker pool on queue <c>load</c>, a no-op handler and a 1 KiB payload, against a fresh store.
/// <list type="bullet">
/// <item><c>drain</c>: dispatch <c>--n</c> jobs first (DispatchMany, up to 100 per call within the payload budget), then start the worker and time the
/// first claim to the last confirmed finish.</item>
/// <item><c>enqueue</c>: <c>--n</c> single-job Dispatch calls from <c>--calls</c> callers, worker never started; reports inserts/s.</item>
/// <item><c>bulk</c>: <c>--n</c> jobs added with DispatchMany, worker never started; reports inserts/s.</item>
/// <item><c>steady</c>: start the worker, dispatch single jobs at <c>--rate</c>/s for <c>--seconds</c> (one call at a time
/// like the River and pg-boss benches, or open loop with <c>--calls n</c>), and measure dispatch call start to confirmed
/// finish per job; then wait up to 60 s for the rest to finish.</item>
/// </list>
/// Usage: <c>bench --mode drain|enqueue|bulk|steady [--no-usage] [--n 20000] [--rate 100] [--seconds 120] [--workers 16]
/// [--payload 1024] [--job-ms 0] [--calls 1] [--store postgres|sqlite|redis] [--out file.json] [--trace] [--sqlite-count-statements]</c>.
/// Trace adds per-job stage gaps, held-job and settlement-queue samples, and store call phases. Writes the same JSON shape as the River and pg-boss
/// benches: <c>system</c>, <c>mode</c>, <c>completionsPerSec</c>, <c>latencyMs {p50, p95, p99, max}</c> (steady).
/// </summary>
internal static class BenchMode
{
    const string DefaultAdmin = "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres";
    // Leave space in the 2 MiB envelope for base64 expansion and per-item metadata.
    const int BatchPayloadBudget = 1024 * 1024;

    sealed class SqliteBenchDirectory(string parent) : IDisposable
    {
        public string FullName { get; } = Directory.CreateDirectory(Path.Combine(parent, "bench-runwick-" + Guid.NewGuid().ToString("N"))).FullName;

        public void Dispose()
        {
            try { Directory.Delete(FullName, recursive: true); } catch (IOException) { }
        }
    }

    public static async Task<int> RunAsync(Args a)
    {
        var mode = a.OptStr("mode") ?? "drain";
        if (mode is not ("drain" or "steady" or "enqueue" or "bulk"))
        {
            Console.Error.WriteLine("--mode must be drain, steady, enqueue or bulk");
            return 2;
        }

        var n = a.Int("n", 20000);
        var rate = a.Double("rate", 100);
        var seconds = a.Double("seconds", 120);
        var workers = a.OptInt("slots") ?? a.Int("workers", 16);
        var payload = a.Int("payload", 1024);
        if (payload is < 1 or > PayloadBounds.MaxPayloadBytes)
        {
            Console.Error.WriteLine($"--payload must be between 1 and {PayloadBounds.MaxPayloadBytes} bytes");
            return 2;
        }

        var chunk = Math.Min(PayloadBounds.MaxBatchItems, BatchPayloadBudget / payload);
        var database = a.OptStr("db") ?? "bench_runwick";
        var adminCs = a.OptStr("conn") ?? DefaultAdmin;
        var calls = Math.Max(1, a.Int("calls", 1));

        // Fresh store per run: a new PostgreSQL database, a new SQLite file, or a flushed Redis (only the instance named by --redis).
        var store = a.OptStr("store") ?? "postgres";
        if (store is not ("postgres" or "sqlite" or "redis"))
        {
            Console.Error.WriteLine("--store must be postgres, sqlite or redis");
            return 2;
        }

        Action<RunwickBuilder> useStore;
        using var sqliteDirectory = store == "sqlite" ? new SqliteBenchDirectory(a.OptStr("sqlite-dir") ?? Path.GetTempPath()) : null;
        if (sqliteDirectory is not null)
        {
            var sqliteCs = new SqliteConnectionStringBuilder { DataSource = Path.Combine(sqliteDirectory.FullName, "runwick.db") }.ConnectionString;
            await RunwickSqliteMigrations.MigrateAsync(sqliteCs).ConfigureAwait(false);
            useStore = r => r.UseSqlite(sqliteCs);
        }
        else if (store == "redis")
        {
            var redisCfg = a.OptStr("redis") ?? throw new ArgumentException("--store redis needs --redis host:port (a dedicated, throwaway instance: it is FLUSHALLed)");
            using (var adminRedis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(redisCfg + ",allowAdmin=true").ConfigureAwait(false))
            {
                await adminRedis.GetServer(adminRedis.GetEndPoints()[0]).FlushAllDatabasesAsync().ConfigureAwait(false);
            }

            await RunwickRedisMigrations.MigrateAsync(redisCfg).ConfigureAwait(false);
            useStore = r => r.UseRedis(redisCfg);
        }
        else
        {
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
                Options = a.Has("trace") ? "-c track_functions=all" : null, // --trace: per-function server time in pg_stat_user_functions
            }.ConnectionString;
            useStore = r => r.UsePostgres(storeCs);
        }

        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton(new LoadBehaviour(TimeSpan.FromMilliseconds(a.Double("job-ms", 0))));
        sc.AddRunwick(r =>
        {
            r.Application("bench");
            useStore(r);
            LoadRegistration.Register(r, typeCeiling: null);
            r.AddHandler<LoadJob, LoadHandler>();
            r.Producer(o => o.MaxConcurrentCalls = Math.Max(2, calls));
            r.AddWorker(w => w.MaxOutstandingAttempts(workers).Pool("main", p => p.Queues(LoadRegistration.Queue).FixedConcurrency(workers))
                .Configure(o => o.RecordResourceUsage = !a.Has("no-usage"))); // --no-usage: the attempt-usage overhead baseline
        });
        await using var provider = sc.BuildServiceProvider();
        var engine = provider.GetRequiredService<WorkerEngine>();
        var hosted = provider.GetServices<IHostedService>().Single();
        var dispatcher = provider.GetRequiredService<IJobDispatcher>();
        var sqlite = provider.GetRequiredService<IRunwickStore>() as SqliteStore;
        long sqliteStatements = 0;
        var countSqliteStatements = a.Has("sqlite-count-statements") && sqlite is not null;
        if (countSqliteStatements)
        {
            await sqlite!.Executor.WriteAsync(SqLane.Background, conn =>
            {
                SQLitePCL.raw.sqlite3_profile(conn.Handle, (SQLitePCL.delegate_profile)((_, _, _) =>
                    Interlocked.Increment(ref sqliteStatements)), null);
                return true;
            }, CancellationToken.None).ConfigureAwait(false);
        }

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
                    stages.Observe("stage:due->handlerInvoked", (DateTimeOffset.UtcNow - entry.Reserved.Cursor.EligibleAt).TotalMilliseconds);
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

                for (var previous = Interlocked.Read(ref lastFinishTicks); now > previous; previous = Interlocked.Read(ref lastFinishTicks))
                {
                    if (Interlocked.CompareExchange(ref lastFinishTicks, now, previous) == previous)
                    {
                        break;
                    }
                }

                Interlocked.Increment(ref finished);
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
            ["system"] = store == "postgres" ? "runwick" : "runwick-" + store,
            ["store"] = store,
            ["mode"] = mode,
            ["workers"] = workers,
            ["payloadBytes"] = payload,
            ["bulkChunk"] = chunk,
        };
        long sequence = 0;
        var failedDispatch = 0L;
        var check = a.Has("check"); // drain: tolerate and report outcome-unknown bulk adds, then compare jobs run with jobs accepted
        long unknownChunks = 0, acceptedItems = 0, unresolvedItems = 0;
        var unknownCodes = new ConcurrentDictionary<string, long>();
        var resolutions = new ConcurrentDictionary<string, long>();
        var resolveErrors = new ConcurrentDictionary<string, long>();

        if (mode == "enqueue")
        {
            // --n single-job Dispatch calls from --calls callers side by side; the worker is never started.
            var enqueueLatency = new LatencyReservoir();
            var nextJob = 0;
            var watch = Stopwatch.StartNew();
            await Task.WhenAll(Enumerable.Range(0, calls).Select(_ => Task.Run(async () =>
            {
                while (Interlocked.Increment(ref nextJob) <= n)
                {
                    var t = clock.ElapsedTicks;
                    await dispatcher.DispatchAsync(LoadJob.Create(Interlocked.Increment(ref sequence), payload)).ConfigureAwait(false);
                    enqueueLatency.Add(Ms(clock.ElapsedTicks - t));
                }
            }))).ConfigureAwait(false);
            var es = enqueueLatency.Summarize();
            result["jobs"] = n;
            result["calls"] = calls;
            result["insertsPerSec"] = Math.Round(n / watch.Elapsed.TotalSeconds, 1);
            result["insertLatencyMs"] = new JsonObject { ["p50"] = Math.Round(es.P50Ms, 2), ["p99"] = Math.Round(es.P99Ms, 2) };
        }
        else if (mode == "bulk")
        {
            // --n jobs added with DispatchMany from --calls callers side by side; worker never started.
            var watch = Stopwatch.StartNew();
            var nextChunk = -1;
            await Task.WhenAll(Enumerable.Range(0, calls).Select(_ => Task.Run(async () =>
            {
                for (var c = Interlocked.Increment(ref nextChunk); c * chunk < n; c = Interlocked.Increment(ref nextChunk))
                {
                    var items = Enumerable.Range(0, Math.Min(chunk, n - c * chunk))
                        .Select(_ => new BatchDispatchItem(LoadJob.Create(Interlocked.Increment(ref sequence), payload))).ToList();
                    var receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
                    Interlocked.Add(ref failedDispatch, receipt.Items.Count(x => x.Receipt is null));
                }
            }))).ConfigureAwait(false);
            result["jobs"] = n;
            result["calls"] = calls;
            result["failedDispatch"] = failedDispatch;
            result["insertsPerSec"] = Math.Round((n - failedDispatch) / watch.Elapsed.TotalSeconds, 1);
        }
        else if (mode == "drain")
        {
            // --calls n: n callers send DispatchMany chunks side by side (the producer admits n concurrent calls).
            var insertWatch = Stopwatch.StartNew();
            var nextChunk = -1;
            await Task.WhenAll(Enumerable.Range(0, calls).Select(_ => Task.Run(async () =>
            {
                for (var c = Interlocked.Increment(ref nextChunk); c * chunk < n; c = Interlocked.Increment(ref nextChunk))
                {
                    var items = Enumerable.Range(0, Math.Min(chunk, n - c * chunk))
                        .Select(_ => new BatchDispatchItem(LoadJob.Create(Interlocked.Increment(ref sequence), payload))).ToList();
                    BatchDispatchReceipt? receipt = null;
                    try
                    {
                        receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
                    }
                    catch (DispatchOutcomeUnknownException ex) when (check)
                    {
                        // --check: record the exact error, then ask the store what happened to that operation.
                        Interlocked.Increment(ref unknownChunks);
                        unknownCodes.AddOrUpdate($"{ex.Code}: {ex.InnerException?.GetType().Name}: {ex.InnerException?.Message}", 1, (_, v) => v + 1);
                        DispatchResolution? resolution = null;
                        for (var attempt = 0; attempt < 40 && resolution is null; attempt++)
                        {
                            try
                            {
                                resolution = await provider.GetRequiredService<IPreparedJobDispatcher>().ResolveAsync(ex.OperationId).ConfigureAwait(false);
                            }
                            catch (Exception rex) when (rex is not OperationCanceledException)
                            {
                                resolveErrors.AddOrUpdate($"{rex.GetType().Name}: {rex.Message}", 1, (_, v) => v + 1);
                                await Task.Delay(500).ConfigureAwait(false); // the store may still be finishing a pending write
                            }
                        }

                        resolutions.AddOrUpdate(resolution?.Status.ToString() ?? "StillUnresolvedAfter20s", 1, (_, v) => v + 1);
                        receipt = resolution?.BatchReceipt;
                        if (receipt is null)
                        {
                            Interlocked.Add(ref unresolvedItems, items.Count);
                        }
                    }

                    if (receipt is not null)
                    {
                        var rejected = receipt.Items.Count(x => x.Receipt is null);
                        Interlocked.Add(ref failedDispatch, rejected);
                        Interlocked.Add(ref acceptedItems, receipt.Items.Count - rejected);
                    }
                }
            }))).ConfigureAwait(false);

            if (check)
            {
                result["check"] = new JsonObject
                {
                    ["chunks"] = (n + chunk - 1) / chunk,
                    ["chunksWithUnknownOutcome"] = Interlocked.Read(ref unknownChunks),
                    ["errors"] = new JsonObject([.. unknownCodes.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                    ["resolveAttemptErrors"] = new JsonObject([.. resolveErrors.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                    ["resolutions"] = new JsonObject([.. resolutions.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                    ["itemsAccepted"] = Interlocked.Read(ref acceptedItems),
                    ["itemsInUnresolvedChunks"] = Interlocked.Read(ref unresolvedItems),
                };
            }

            result["insertSeconds"] = Math.Round(insertWatch.Elapsed.TotalSeconds, 3);
            result["insertCalls"] = calls;
            var commitsBeforeDrain = sqlite?.Runner.Commits;
            var statementsBeforeDrain = Interlocked.Read(ref sqliteStatements);
            var expected = check ? Interlocked.Read(ref acceptedItems) : n - failedDispatch;
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
            while (Interlocked.Read(ref finished) < expected && deadline.Elapsed < TimeSpan.FromSeconds(a.Double("max-seconds", 1800)))
            {
                await Task.Delay(5).ConfigureAwait(false);
            }

            if (check)
            {
                await Task.Delay(5000).ConfigureAwait(false); // grace: any extra job run beyond the accepted count shows up here
                (result["check"] as JsonObject)!["jobsRun"] = Interlocked.Read(ref finished);
                (result["check"] as JsonObject)!["jobsRunMinusAccepted"] = Interlocked.Read(ref finished) - expected;
            }

            await sampling.CancelAsync().ConfigureAwait(false);
            await sampler.ConfigureAwait(false);
            var done = Interlocked.Read(ref finished);
            var first = Interlocked.Read(ref firstClaimTicks);
            var elapsed = Ms(Interlocked.Read(ref lastFinishTicks) - (first < 0 ? startTicks : first)) / 1000;
            result["jobs"] = n;
            result["accepted"] = expected;
            result["failedDispatch"] = failedDispatch;
            result["finished"] = done;
            result["drained"] = done == expected;
            result["drainWriteCommits"] = sqlite?.Runner.Commits - commitsBeforeDrain;
            if (countSqliteStatements)
            {
                result["drainWriterStatements"] = Interlocked.Read(ref sqliteStatements) - statementsBeforeDrain;
            }
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

            var completedByEndOfOffers = Interlocked.Read(ref finished);
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
            result["offered"] = sequence;
            result["seconds"] = seconds;
            result["inserted"] = dispatched;
            result["dispatchCalls"] = calls;
            result["failedDispatch"] = failedDispatch;
            result["failedDispatchByCode"] = new JsonObject([.. failures.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]);
            result["completed"] = Interlocked.Read(ref finished);
            result["completedByEndOfOffers"] = completedByEndOfOffers;
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
        if (sqlite is not null)
        {
            result["sqlite"] = new JsonObject
            {
                ["profile"] = sqlite.AcknowledgementProfile,
                ["engine"] = new JsonObject([.. sqlite.EngineFacts.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                ["writeCommits"] = sqlite.Runner.Commits,
                ["busyConflicts"] = sqlite.Runner.BusyConflicts,
                ["writerConnectionsOpened"] = sqlite.Executor.WriterConnectionsOpened,
                ["writerStatements"] = countSqliteStatements ? Interlocked.Read(ref sqliteStatements) : null,
            };
        }

        using (var process = Process.GetCurrentProcess())
        {
            result["peakWorkingSetBytes"] = process.PeakWorkingSet64;
        }

        if (trace)
        {
            result["finishesSent"] = Interlocked.Read(ref engine.FinishesSent);
            result["finishManyCalls"] = Interlocked.Read(ref engine.Completion.BatchesSent);
            result["trace"] = stages.LatenciesJson();
            result["traceCounters"] = stages.CountersJson();
        }

        result["diagnostics"] = new JsonArray([.. engine.Health().Diagnostics.Select(x => (JsonNode?)x)]);

        if (mode is not ("enqueue" or "bulk")) // those never started the worker
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
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
