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
/// One worker process (07 §14.3): the shipped <c>AddRunwick(...).UsePostgres(...).AddWorker(...)</c> composition with
/// <see cref="TimingStore"/> over the PostgreSQL store. Writes a sample line every 5 s and a summary on <c>stop</c>.
/// Stdin commands: <c>reset</c> (end of warm-up: drop latency samples), <c>stop</c> (staged shutdown, summary, exit).
/// </summary>
internal static class WorkerMode
{
    public static async Task<int> RunAsync(Args a)
    {
        var metrics = new Metrics();
        var invocations = new ConcurrentDictionary<Guid, int>();
        var concurrency = a.Int("concurrency", 16);
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton(new LoadBehaviour(TimeSpan.FromMilliseconds(a.Int("handler-ms", 0))));
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app")).UsePostgres(a.Str("conn"));
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
        sc.AddSingleton<IRunwickStore>(sp => new TimingStore(sp.GetRequiredService<PgStore>(), metrics));
        await using var provider = sc.BuildServiceProvider();
        var engine = provider.GetRequiredService<WorkerEngine>();
        engine.Hooks.Event = (name, entry) =>
        {
            metrics.Count("event:" + name);
            if (name == "finished" && entry?.FinishPublishedAt is { } published)
            {
                metrics.Observe("pipelineToFinish", (engine.Clock.Now - published).TotalMilliseconds);
            }
            else if (name == "handlerInvoked" && entry is not null
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
