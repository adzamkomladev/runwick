using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Postgres.Store;
using Runwick.Protocol;

namespace Runwick.Workloads;

/// <summary>
/// G-2 producer (<c>produce2</c>), its own process. Two modes, both through the public dispatcher:
/// <list type="bullet">
/// <item><c>poisson</c>: open-loop Poisson arrivals at <c>--rate</c>, one Dispatch each (as the G-1 producer).</item>
/// <item><c>topup</c>: keeps every lane in <c>--lanes q/p:h,...</c> continuously claimable. Each 250 ms it reads the lane's
/// pending count from the store and dispatches (DispatchMany, ≤ 32 per batch) up to
/// <c>max(--min-backlog, ceil(consumption rate × h s))</c>. A short horizon keeps the oldest job young (no aging, P-04 b);
/// a long one makes lower lanes age past 60 s (P-04 c). The lane's oldest eligible age is sampled every second.</item>
/// </list>
/// Output files match the G-1 producer: <c>.samples.jsonl</c> (t, offered, accepted, failed, inFlight, lanes) and <c>.summary.json</c>.
/// Stdin: <c>rate N</c>, <c>reset</c>, <c>stop</c>.
/// </summary>
internal static class G2ProducerMode
{
    sealed class Lane(string queue, string priority, double horizon)
    {
        public string Queue => queue;
        public string Priority => priority;
        public double Horizon => horizon;
        public string Key => queue + "/" + priority;
        public long Dispatched;
        public long Pending;
        public double OldestAgeS;
        public double Target;
        public readonly Queue<(double T, long Consumed)> History = new();
    }

    public static async Task<int> RunAsync(Args a)
    {
        var metrics = new Metrics();
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app")).UsePostgres(a.Str("conn"));
            G2Jobs.Register(r, a);
            if (a.OptInt("producer-calls") is { } calls)
            {
                r.Producer(o => o.MaxConcurrentCalls = calls);
            }
        });
        sc.AddSingleton<IRunwickStore>(sp => new TimingStore(sp.GetRequiredService<PgStore>(), metrics));
        await using var provider = sc.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var h in hosted)
        {
            await h.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        var dispatcher = provider.GetRequiredService<IJobDispatcher>();
        var prepared = provider.GetRequiredService<IPreparedJobDispatcher>();
        var factory = G2Jobs.Factory(a.OptStr("job") ?? "work", a);
        var mode = a.OptStr("mode") ?? "poisson";
        var rate = a.Double("rate", 100);
        var lanes = (a.OptStr("lanes") ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s =>
        {
            var (lane, h) = s.Contains(':', StringComparison.Ordinal) ? (s.Split(':')[0], double.Parse(s.Split(':')[1], CultureInfo.InvariantCulture)) : (s, 0.0);
            return new Lane(lane.Split('/')[0], lane.Split('/')[1], h);
        }).ToList();
        var sequence = 0L;
        var inFlight = 0L;
        using var stop = new CancellationTokenSource();

        var control = Task.Run(async () =>
        {
            string? line;
            while ((line = await Console.In.ReadLineAsync().ConfigureAwait(false)) is not null)
            {
                if (line.StartsWith("rate ", StringComparison.Ordinal))
                {
                    Volatile.Write(ref rate, double.Parse(line[5..], CultureInfo.InvariantCulture));
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
                    var laneJson = new JsonObject();
                    foreach (var l in lanes)
                    {
                        laneJson[l.Key] = new JsonObject
                        {
                            ["pending"] = Interlocked.Read(ref l.Pending), ["oldestAgeS"] = Math.Round(l.OldestAgeS, 1),
                            ["target"] = Math.Round(l.Target, 0), ["dispatched"] = Interlocked.Read(ref l.Dispatched),
                        };
                    }

                    await samples.WriteLineAsync(new JsonObject
                    {
                        ["t"] = DateTimeOffset.UtcNow.ToString("O"),
                        ["rate"] = Volatile.Read(ref rate),
                        ["offered"] = metrics.Counter("offered"),
                        ["accepted"] = metrics.Counter("accepted"),
                        ["failed"] = metrics.Counter("failed"),
                        ["outcomeUnresolved"] = metrics.Counter("outcomeUnresolved"),
                        ["inFlight"] = Interlocked.Read(ref inFlight),
                        ["lanes"] = laneJson,
                    }.ToJsonString()).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        Console.WriteLine("ready");
        if (mode == "topup")
        {
            await TopUpAsync(a, dispatcher, prepared, factory, lanes, metrics, () => Interlocked.Increment(ref sequence), stop.Token).ConfigureAwait(false);
        }
        else
        {
            await PoissonAsync(a, dispatcher, prepared, factory, metrics, () => Volatile.Read(ref rate),
                () => Interlocked.Increment(ref sequence), d => Interlocked.Add(ref inFlight, d), stop.Token).ConfigureAwait(false);
        }

        await sampler.ConfigureAwait(false);
        await control.ConfigureAwait(false);
        var summary = new JsonObject
        {
            ["offered"] = metrics.Counter("offered"),
            ["accepted"] = metrics.Counter("accepted"),
            ["failed"] = metrics.Counter("failed"),
            ["outcomeUnresolved"] = metrics.Counter("outcomeUnresolved"),
            ["mode"] = mode,
            ["counters"] = metrics.CountersJson(),
            ["maxima"] = metrics.MaximaJson(),
            ["latencies"] = metrics.LatenciesJson(),
            ["process"] = WorkerMode.ProcessStats(new TimerLag()),
        };
        await File.WriteAllTextAsync(a.Str("out") + ".summary.json", summary.ToJsonString(Json.Indented)).ConfigureAwait(false);
        foreach (var h in hosted)
        {
            await h.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        Console.WriteLine("stopped");
        return 0;
    }

    static async Task PoissonAsync(Args a, IJobDispatcher dispatcher, IPreparedJobDispatcher prepared, Func<long, IQueueJob> factory, Metrics metrics,
        Func<double> rate, Func<long> next, Action<long> inFlight, CancellationToken stop)
    {
        var random = new Random(a.Int("seed", 20261030));
        var options = a.OptStr("queue") is { } q ? new DispatchOptions { Queue = q, Priority = a.OptStr("priority") } : null;
        var pending = new HashSet<Task>();
        var clock = Stopwatch.StartNew();
        var at = 0.0;
        while (!stop.IsCancellationRequested)
        {
            var r = rate();
            try
            {
                if (r <= 0)
                {
                    await Task.Delay(200, stop).ConfigureAwait(false);
                    at = clock.Elapsed.TotalSeconds;
                    continue;
                }

                at += -Math.Log(1 - random.NextDouble()) / r;
                var wait = at - clock.Elapsed.TotalSeconds;
                if (wait > 0.001)
                {
                    await Task.Delay(TimeSpan.FromSeconds(wait), stop).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }

            metrics.Count("offered");
            if (clock.Elapsed.TotalSeconds - at > 1)
            {
                metrics.Count("arrivalsLate1s");
            }

            inFlight(1);
            var job = factory(next());
            pending.Add(OneAsync(dispatcher, prepared, job, options, metrics, () => inFlight(-1)));
            pending.RemoveWhere(t => t.IsCompleted);
        }

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    static async Task OneAsync(IJobDispatcher dispatcher, IPreparedJobDispatcher prepared, IQueueJob job, DispatchOptions? options, Metrics metrics, Action done)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await dispatcher.DispatchAsync(job, options).ConfigureAwait(false);
            metrics.Observe("dispatchCall", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            metrics.Count("accepted");
        }
        catch (DispatchOutcomeUnknownException ex) when (ex.Prepared is not null)
        {
            metrics.Count("outcomeUnknown");
            await ResolveAsync(() => prepared.DispatchAsync(ex.Prepared), 1, metrics).ConfigureAwait(false);
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

    /// <summary>An outcome-unknown dispatch is resent with the same prepared command until known (CMD-02), as in the G-1 producer.</summary>
    static async Task ResolveAsync(Func<Task> resend, int items, Metrics metrics)
    {
        var giveUp = Stopwatch.StartNew();
        while (true)
        {
            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            try
            {
                await resend().ConfigureAwait(false);
                metrics.Count("accepted", items);
                metrics.Count("accepted:recovered", items);
                return;
            }
            catch (RunwickException ex) when (ex.Disposition == ErrorDisposition.KnownNotApplied
                && ex is not (StoreUnavailableException or LocalCapacityExceededException))
            {
                metrics.Count("failed", items);
                metrics.Count("failed:recovered_" + ex.Code, items);
                return;
            }
            catch (Exception) when (giveUp.Elapsed < TimeSpan.FromMinutes(15))
            {
            }
            catch (Exception)
            {
                metrics.Count("outcomeUnresolved", items);
                return;
            }
        }
    }

    static async Task TopUpAsync(Args a, IJobDispatcher dispatcher, IPreparedJobDispatcher prepared, Func<long, IQueueJob> factory, List<Lane> lanes,
        Metrics metrics, Func<long> next, CancellationToken stop)
    {
        var minBacklog = a.Int("min-backlog", 8);
        var maxBacklog = a.Int("max-backlog", 20_000);
        var app = a.Str("app");
        var cs = new NpgsqlConnectionStringBuilder(a.Str("conn")) { ApplicationName = "wl-feeder-probe" }.ConnectionString;
        var clock = Stopwatch.StartNew();
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await ReadLanesAsync(cs, app, lanes, stop).ConfigureAwait(false);
                var now = clock.Elapsed.TotalSeconds;
                foreach (var l in lanes)
                {
                    var consumed = Interlocked.Read(ref l.Dispatched) - l.Pending;
                    l.History.Enqueue((now, consumed));
                    while (l.History.Count > 2 && now - l.History.Peek().T > 10)
                    {
                        l.History.Dequeue();
                    }

                    var (t0, c0) = l.History.Peek();
                    var consumption = now - t0 > 1 ? Math.Max(0, consumed - c0) / (now - t0) : 0;
                    l.Target = Math.Min(maxBacklog, Math.Max(minBacklog, Math.Ceiling(consumption * l.Horizon)));
                    var need = (int)(l.Target - l.Pending);
                    for (var batch = 0; batch < 4 && need > 0 && !stop.IsCancellationRequested; batch++)
                    {
                        var n = Math.Min(need, 32);
                        await BatchAsync(dispatcher, prepared, factory, l, n, metrics, next).ConfigureAwait(false);
                        need -= n;
                    }
                }

                await Task.Delay(250, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (NpgsqlException)
            {
                metrics.Count("laneReadFailed");
            }
        }
    }

    static async Task ReadLanesAsync(string cs, string app, List<Lane> lanes, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("""
            SELECT queue, priority, count(*), extract(epoch FROM clock_timestamp() - min(eligible_at))::float8
            FROM runwick.jobs WHERE application = @a AND state IN ('pending', 'retrying') GROUP BY 1, 2
            """, conn);
        cmd.Parameters.AddWithValue("a", app);
        var seen = new Dictionary<string, (long Count, double Age)>(StringComparer.Ordinal);
        await using (var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                seen[r.GetString(0) + "/" + r.GetString(1)] = (r.GetInt64(2), r.IsDBNull(3) ? 0 : r.GetDouble(3));
            }
        }

        foreach (var l in lanes)
        {
            var (count, age) = seen.GetValueOrDefault(l.Key);
            Interlocked.Exchange(ref l.Pending, count);
            l.OldestAgeS = age;
        }
    }

    static async Task BatchAsync(IJobDispatcher dispatcher, IPreparedJobDispatcher prepared, Func<long, IQueueJob> factory, Lane lane, int n,
        Metrics metrics, Func<long> next)
    {
        var options = new DispatchOptions { Queue = lane.Queue, Priority = lane.Priority };
        var items = Enumerable.Range(0, n).Select(_ => new BatchDispatchItem(factory(next()), options)).ToList();
        metrics.Count("offered", n);
        var started = Stopwatch.GetTimestamp();
        try
        {
            var receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
            metrics.Observe("dispatchManyCall", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            var ok = receipt.Items.Count(i => i.Receipt is not null);
            metrics.Count("accepted", ok);
            Interlocked.Add(ref lane.Dispatched, ok);
            if (ok < n)
            {
                metrics.Count("failed", n - ok);
            }
        }
        catch (DispatchOutcomeUnknownException ex) when (ex.PreparedBatch is not null)
        {
            metrics.Count("outcomeUnknown", n);
            var before = metrics.Counter("accepted");
            await ResolveAsync(() => prepared.DispatchAsync(ex.PreparedBatch), n, metrics).ConfigureAwait(false);
            Interlocked.Add(ref lane.Dispatched, metrics.Counter("accepted") - before);
        }
        catch (Exception ex)
        {
            metrics.Count("failed", n);
            metrics.Count("failed:" + (ex is RunwickException rx ? rx.Code : ex.GetType().Name), n);
            await Task.Delay(200).ConfigureAwait(false);
        }
    }
}
