using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick.Harness.Timing;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Workloads;

/// <summary>
/// One worker process (07 §14.3). Defaults to PostgreSQL with <see cref="TimingStore"/>;
/// <c>--store sqlite --conn</c> attaches to a migrated SQLite file with the driver's phase timings.
/// <c>--sqlite-writer-busy-ms</c> overrides the SQLite busy timeout for contention experiments.
/// Writes a sample line every 5 s and a summary on <c>stop</c>.
/// Stdin commands: <c>reset</c> (end of warm-up: drop latency samples), <c>stop</c> (staged shutdown, summary, exit).
/// </summary>
internal static class WorkerMode
{
    public static async Task<int> RunAsync(Args a)
    {
        var metrics = new Metrics();
        using var phases = PhaseListener.Attach(metrics);
        using var telemetry = a.Has("telemetry") ? Sp029Workloads.ListenToTelemetry() : null;
        var invocations = new ConcurrentDictionary<Guid, int>();
        var concurrency = a.Int("concurrency", 16);
        var g2 = a.Has("g2");
        var store = a.OptStr("store") ?? "postgres";
        if (store is not ("postgres" or "sqlite"))
        {
            Console.Error.WriteLine("worker --store must be postgres or sqlite");
            return 2;
        }

        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton(new LoadBehaviour(TimeSpan.FromMilliseconds(a.Int("handler-ms", 0))));
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app"));
            if (store == "sqlite")
            {
                r.UseSqlite(a.Str("conn"), options =>
                {
                    if (a.OptInt("sqlite-writer-busy-ms") is { } milliseconds)
                    {
                        options.WriterBusyTimeout = TimeSpan.FromMilliseconds(milliseconds);
                    }
                });
            }
            else
            {
                r.UsePostgres(a.Str("conn"));
            }

            if (g2)
            {
                // G-2 workloads (G2Jobs): every G-2 contract registered; handlers and pool from the arguments.
                G2Jobs.Register(r, a);
                G2Jobs.AddHandlers(r, a);
                r.AddWorker(w => w.Pool("main", p => G2Jobs.Pool(p, a, concurrency)));
                return;
            }

            LoadRegistration.Register(r, a.OptInt("type-ceiling"));
            r.AddHandler<LoadJob, LoadHandler>();
            r.AddWorker(w => w.Pool("main", p => p.Queues(LoadRegistration.Queue).FixedConcurrency(concurrency)).Configure(o =>
            {
                if (a.OptInt("divisor") is { } divisor)
                {
                    TestTiming.Scaled(o, divisor);
                }
            }));
        });
        if (store == "postgres")
        {
            sc.AddSingleton<IRunwickStore>(sp => new TimingStore(sp.GetRequiredService<PgStore>(), metrics, detailed: g2));
        }
        await using var provider = sc.BuildServiceProvider();
        var engine = provider.GetRequiredService<WorkerEngine>();
        engine.Hooks.Event = (name, entry) =>
        {
            metrics.Count("event:" + name);
            if (name == "finished" && entry?.FinishPublishedAt is { } published)
            {
                metrics.Observe("pipelineToFinish", (engine.Clock.Now - published).TotalMilliseconds);
            }
            // SP-029 P-10 (--soak): no per-job map, which would grow with completed jobs and spoil the heap check.
            else if (name == "handlerInvoked" && entry is not null && !a.Has("soak")
                && invocations.AddOrUpdate(entry.Reserved.Position.JobId, 1, (_, n) => n + 1) > 1)
            {
                metrics.Count("jobsInvokedAgain");
            }
        };

        var hosted = provider.GetServices<IHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
        Console.WriteLine("ready " + engine.WorkerInstanceId.ToString("D"));

        var outPrefix = a.Str("out");
        using var stopSampling = new CancellationTokenSource();
        var lag = new TimerLag();
        var lagTask = lag.RunAsync(stopSampling.Token);
        var sampler = SampleAsync(outPrefix + ".samples.jsonl", engine, metrics, lag, stopSampling.Token);

        string? line;
        while ((line = await Console.In.ReadLineAsync().ConfigureAwait(false)) is not null)
        {
            if (line == "reset")
            {
                metrics.ResetLatencies();
                Console.WriteLine("reset");
            }
            else if (line == "gc")
            {
                // SP-029 P-10: post-GC managed heap and live cache sizes, one line per request.
                await File.AppendAllTextAsync(outPrefix + ".gc.jsonl", Sp029Workloads.GcSample(engine) + "\n").ConfigureAwait(false);
            }
            else if (line == "stop")
            {
                break;
            }
        }

        var stopStarted = Stopwatch.GetTimestamp();
        await hosted.StopAsync(CancellationToken.None).ConfigureAwait(false);
        metrics.Observe("stopAsync", Stopwatch.GetElapsedTime(stopStarted).TotalMilliseconds);
        await stopSampling.CancelAsync().ConfigureAwait(false);
        await sampler.ConfigureAwait(false);
        await lagTask.ConfigureAwait(false);

        var summary = new JsonObject
        {
            ["workerInstanceId"] = engine.WorkerInstanceId.ToString("D"),
            ["concurrency"] = concurrency,
            ["timing"] = a.OptInt("divisor") is { } d ? $"scaled /{d}" : "default",
            ["invocations"] = Interlocked.Read(ref engine.Invocations),
            ["distinctJobsInvoked"] = invocations.Count,
            ["finishesSent"] = Interlocked.Read(ref engine.FinishesSent),
            ["counters"] = metrics.CountersJson(),
            ["maxima"] = metrics.MaximaJson(),
            ["latencies"] = metrics.LatenciesJson(),
            ["diagnostics"] = new JsonArray([.. engine.Health().Diagnostics.Select(x => (JsonNode?)x)]),
            ["process"] = ProcessStats(lag),
        };
        if (g2)
        {
            summary["rateDeferrals"] = Interlocked.Read(ref engine.RateDeferrals);
        }

        await File.WriteAllTextAsync(outPrefix + ".summary.json", summary.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine("stopped");
        return 0;
    }

    static async Task SampleAsync(string path, WorkerEngine engine, Metrics metrics, TimerLag lag, CancellationToken ct)
    {
        await using var file = new StreamWriter(path, append: false) { AutoFlush = true };
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        var tick = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                var now = engine.Clock.Now;
                var entries = engine.Entries.ToList();
                var finishing = entries.Count(e => e.Phase == AttemptPhase.Finishing);
                var controlWait = engine.Lease.OldestWait(now);
                metrics.Max("entries", entries.Count);
                metrics.Max("finishing", finishing);
                metrics.Max("controlWaitMs", (long)controlWait.TotalMilliseconds);
                metrics.Observe("controlWaitSampled", controlWait.TotalMilliseconds);
                if (engine.PressureStopped)
                {
                    metrics.Count("pressureStoppedSeconds");
                }

                if (++tick % 5 != 0)
                {
                    continue;
                }

                var health = engine.Health();
                var sample = new JsonObject
                {
                    ["t"] = DateTimeOffset.UtcNow.ToString("O"),
                    ["status"] = health.Status,
                    ["admissionOpen"] = engine.AdmissionOpen,
                    ["pressureStopped"] = engine.PressureStopped,
                    ["entries"] = entries.Count,
                    ["finishing"] = finishing,
                    ["lost"] = entries.Count(e => e.IsLost),
                    ["controlWaitMs"] = controlWait.TotalMilliseconds,
                    ["invocations"] = Interlocked.Read(ref engine.Invocations),
                    ["counters"] = metrics.CountersJson(),
                    ["process"] = ProcessStats(lag),
                };
                lag.ResetWindow();
                await file.WriteLineAsync(sample.ToJsonString()).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public static JsonObject ProcessStats(TimerLag lag)
    {
        using var p = Process.GetCurrentProcess();
        var gc = GC.GetGCMemoryInfo();
        return new JsonObject
        {
            ["cpuMs"] = (long)p.TotalProcessorTime.TotalMilliseconds,
            ["rssBytes"] = p.WorkingSet64,
            ["heapBytes"] = GC.GetTotalMemory(false),
            ["allocatedBytes"] = GC.GetTotalAllocatedBytes(),
            ["gen0"] = GC.CollectionCount(0),
            ["gen1"] = GC.CollectionCount(1),
            ["gen2"] = GC.CollectionCount(2),
            ["gcPauseMs"] = (long)GC.GetTotalPauseDuration().TotalMilliseconds,
            ["gcPausePercent"] = gc.PauseTimePercentage,
            ["threads"] = p.Threads.Count,
            ["threadPoolThreads"] = ThreadPool.ThreadCount,
            ["threadPoolQueue"] = ThreadPool.PendingWorkItemCount,
            ["timerLagMaxMs"] = lag.WindowMaxMs,
            ["timerLagAllMaxMs"] = lag.MaxMs,
        };
    }
}

/// <summary>Timer lag: how late a 100 ms delay completes (thread-pool and timer health).</summary>
internal sealed class TimerLag
{
    long _windowMax;
    long _max;

    public long WindowMaxMs => Interlocked.Read(ref _windowMax);

    public long MaxMs => Interlocked.Read(ref _max);

    public void ResetWindow() => Interlocked.Exchange(ref _windowMax, 0);

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var started = Stopwatch.GetTimestamp();
                await Task.Delay(100, ct).ConfigureAwait(false);
                var late = (long)(Stopwatch.GetElapsedTime(started).TotalMilliseconds - 100);
                if (late > Interlocked.Read(ref _windowMax))
                {
                    Interlocked.Exchange(ref _windowMax, late);
                }

                if (late > Interlocked.Read(ref _max))
                {
                    Interlocked.Exchange(ref _max, late);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
