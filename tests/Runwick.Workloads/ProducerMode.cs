using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runwick.Postgres.Store;
using Runwick.Protocol;

namespace Runwick.Workloads;

/// <summary>
/// Open-loop Poisson producer in its own process (07 §14.3, I-11). Arrivals never wait for earlier dispatches; each
/// dispatch is timed from call to receipt. Stdin commands: <c>rate N</c> (arrivals/s), <c>reset</c> (drop latency
/// samples), <c>stop</c> (stop arriving, await in-flight dispatches, summary, exit). Writes one sample line per second.
/// </summary>
internal static class ProducerMode
{
    public static async Task<int> RunAsync(Args a)
    {
        var metrics = new Metrics();
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app")).UsePostgres(a.Str("conn"));
            LoadRegistration.Register(r, a.OptInt("type-ceiling"));
            if (a.OptInt("producer-calls") is { } calls)
            {
                r.Producer(o => o.MaxConcurrentCalls = calls);
            }
        });
        sc.AddSingleton<IRunwickStore>(sp => new TimingStore(sp.GetRequiredService<PgStore>(), metrics));
        await using var provider = sc.BuildServiceProvider();
        var runtime = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().ToList();
        foreach (var h in runtime)
        {
            await h.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        var dispatcher = provider.GetRequiredService<IJobDispatcher>();
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var payload = a.Int("payload", 1024);
        var rate = a.Double("rate", 100);
        var random = new Random(a.Int("seed", 20261030));
        var inFlight = 0L;
        var sequence = 0L;
        var pending = new HashSet<Task>();
        var gate = new Lock();
        using var stop = new CancellationTokenSource();

        var control = Task.Run(async () =>
        {
            string? line;
            while ((line = await Console.In.ReadLineAsync().ConfigureAwait(false)) is not null)
            {
                if (line.StartsWith("rate ", StringComparison.Ordinal))
                {
                    Volatile.Write(ref rate, double.Parse(line[5..], System.Globalization.CultureInfo.InvariantCulture));
                }
                else if (line == "reset")
                {
                    metrics.ResetLatencies();
                }
                else if (line == "stop")
                {
                    break;
                }
            }

            await stop.CancelAsync().ConfigureAwait(false);
        });

        await using var samples = new StreamWriter(a.Str("out") + ".samples.jsonl", append: false) { AutoFlush = true };
        var sampler = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
            try
            {
                while (await timer.WaitForNextTickAsync(stop.Token).ConfigureAwait(false))
                {
                    await samples.WriteLineAsync(new JsonObject
                    {
                        ["t"] = DateTimeOffset.UtcNow.ToString("O"),
                        ["rate"] = Volatile.Read(ref rate),
                        ["offered"] = metrics.Counter("offered"),
                        ["accepted"] = metrics.Counter("accepted"),
                        ["failed"] = metrics.Counter("failed"),
            ["outcomeUnresolved"] = metrics.Counter("outcomeUnresolved"),
                        ["inFlight"] = Interlocked.Read(ref inFlight),
                    }.ToJsonString()).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        if (a.OptInt("count") is { } count)
        {
            // Fixed backlog (G1-M2): dispatched one after another before arrivals start.
            for (var i = 0; i < count; i++)
            {
                metrics.Count("offered");
                Interlocked.Increment(ref inFlight);
                await DispatchOne(dispatcher, prepared, LoadJob.Create(Interlocked.Increment(ref sequence), payload), metrics,
                    () => Interlocked.Decrement(ref inFlight)).ConfigureAwait(false);
            }
        }

        Console.WriteLine("ready");
        var clock = Stopwatch.StartNew();
        var next = 0.0;
        while (!stop.IsCancellationRequested)
        {
            var r = Volatile.Read(ref rate);
            if (r <= 0)
            {
                try
                {
                    await Task.Delay(200, stop.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                next = clock.Elapsed.TotalSeconds;
                continue;
            }

            next += -Math.Log(1 - random.NextDouble()) / r;
            var wait = next - clock.Elapsed.TotalSeconds;
            if (wait > 0.001)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(wait), stop.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            metrics.Count("offered");
            var lateness = clock.Elapsed.TotalSeconds - next;
            if (lateness > 1)
            {
                metrics.Count("arrivalsLate1s");
            }

            var job = LoadJob.Create(Interlocked.Increment(ref sequence), payload);
            var task = DispatchOne(dispatcher, prepared, job, metrics, () => Interlocked.Decrement(ref inFlight));
            metrics.Max("inFlight", Interlocked.Increment(ref inFlight));
            lock (gate)
            {
                pending.Add(task);
                pending.RemoveWhere(t => t.IsCompleted);
            }
        }

        Task[] remaining;
        lock (gate)
        {
            remaining = [.. pending];
        }

        await Task.WhenAll(remaining).ConfigureAwait(false);
        await sampler.ConfigureAwait(false);
        await control.ConfigureAwait(false);
        var summary = new JsonObject
        {
            ["offered"] = metrics.Counter("offered"),
            ["accepted"] = metrics.Counter("accepted"),
            ["failed"] = metrics.Counter("failed"),
            ["counters"] = metrics.CountersJson(),
            ["maxima"] = metrics.MaximaJson(),
            ["latencies"] = metrics.LatenciesJson(),
            ["process"] = WorkerMode.ProcessStats(new TimerLag()),
        };
        await File.WriteAllTextAsync(a.Str("out") + ".summary.json", summary.ToJsonString(Json.Indented)).ConfigureAwait(false);
        foreach (var h in runtime)
        {
            await h.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        Console.WriteLine("stopped");
        return 0;
    }

    static async Task DispatchOne(IJobDispatcher dispatcher, IPreparedJobDispatcher prepared, LoadJob job, Metrics metrics, Action done)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await dispatcher.DispatchAsync(job).ConfigureAwait(false);
            metrics.Observe("dispatchCall", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            metrics.Count("accepted");
        }
        catch (DispatchOutcomeUnknownException ex) when (ex.Prepared is not null)
        {
            metrics.Count("outcomeUnknown");
            await RecoverAsync(prepared, ex.Prepared, metrics).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            metrics.Count("failed");
            metrics.Count("failed:" + (ex is RunwickException rx ? rx.Code : ex.GetType().Name));
        }
        finally
        {
            done();
        }
    }

    /// <summary>
    /// An outcome-unknown Dispatch may have committed (07 §3, CMD-02): resend the same prepared command until its
    /// outcome is known, so an applied original counts as accepted exactly once (Replayed) and never as a second job.
    /// </summary>
    static async Task RecoverAsync(IPreparedJobDispatcher dispatcher, PreparedDispatch prepared, Metrics metrics)
    {
        var giveUp = Stopwatch.StartNew();
        while (true)
        {
            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            try
            {
                await dispatcher.DispatchAsync(prepared).ConfigureAwait(false);
                metrics.Count("accepted");
                metrics.Count("accepted:recovered");
                return;
            }
            catch (RunwickException ex) when (ex.Disposition == ErrorDisposition.KnownNotApplied
                && ex is not (StoreUnavailableException or LocalCapacityExceededException))
            {
                metrics.Count("failed");
                metrics.Count("failed:recovered_" + ex.Code);
                return;
            }
            catch (Exception) when (giveUp.Elapsed < TimeSpan.FromMinutes(15))
            {
            }
            catch (Exception)
            {
                metrics.Count("outcomeUnresolved");
                return;
            }
        }
    }
}
