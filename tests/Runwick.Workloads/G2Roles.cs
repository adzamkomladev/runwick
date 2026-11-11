using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Hosting;
using Runwick.Outbox;
using Runwick.Outbox.Postgres;
using Runwick.Scheduling;

namespace Runwick.Workloads;

/// <summary>
/// The G-2 role processes other than worker and producer: <c>scheduler</c> (P-06s), <c>relay</c> and <c>stage</c> (P-09).
/// Each prints <c>ready</c> after start, reads <c>stop</c> (and role commands) from stdin, writes a summary and exits.
/// </summary>
internal static class G2Roles
{
    /// <summary>
    /// Scheduler process: <c>AddScheduler()</c> with <c>--schedules N</c> minute schedules <c>s0000…</c> (cron <c>* * * * *</c>,
    /// default misfire run_once) and, with <c>--catchup</c>, the schedule <c>catchup</c> (six-field <c>* * * * * *</c>,
    /// misfire catch_up with <c>--catchup-grace-s</c>, episode limit <c>--catchup-limit</c>).
    /// </summary>
    public static async Task<int> SchedulerAsync(Args a)
    {
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        var count = a.Int("schedules", 1000);
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app")).UsePostgres(a.Str("conn"));
            G2Jobs.Register(r, a);
            r.AddScheduler();
            for (var i = 0; i < count; i++)
            {
                var key = "s" + i.ToString("D4", CultureInfo.InvariantCulture);
                r.AddSchedule(key, new G2Tick(key)).Cron("* * * * *");
            }

            if (a.Has("catchup"))
            {
                r.AddSchedule("catchup", new G2Tick("catchup")).Cron("* * * * * *", ScheduleCronFormat.SixField)
                    .OnMisfire(MisfirePolicy.CatchUp, TimeSpan.FromSeconds(a.Int("catchup-grace-s", 30)))
                    .CatchUpLimits(a.Int("catchup-limit", 10_000), TimeSpan.FromDays(1));
            }
        });
        await using var provider = sc.BuildServiceProvider();
        var phases = new Metrics();
        using var phaseListener = PhaseListener.Attach(phases);
        var hosted = provider.GetServices<IHostedService>().ToList();
        var started = Stopwatch.GetTimestamp();
        foreach (var h in hosted)
        {
            await h.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        var seedSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        Console.WriteLine("ready");
        await WaitStopAsync(_ => { }).ConfigureAwait(false);
        foreach (var h in hosted)
        {
            await h.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        var scheduler = provider.GetRequiredService<SchedulerService>();
        var summary = new JsonObject
        {
            ["schedules"] = count + (a.Has("catchup") ? 1 : 0),
            ["startSeconds"] = Math.Round(seedSeconds, 1),
            ["materializations"] = Interlocked.Read(ref scheduler.Materializations),
            ["dueReads"] = Interlocked.Read(ref scheduler.DuePagesRead),
            ["schedulesRead"] = Interlocked.Read(ref scheduler.DueSchedulesRead),
            ["materializationCommands"] = Interlocked.Read(ref scheduler.MaterializationCommands),
            ["plannedOccurrences"] = Interlocked.Read(ref scheduler.PlannedOccurrences),
            ["revisionConflicts"] = Interlocked.Read(ref scheduler.RevisionConflicts),
            ["phases"] = phases.LatenciesJson(),
            ["localDiagnostics"] = scheduler.LocalDiagnostics.Count,
            ["process"] = WorkerMode.ProcessStats(new TimerLag()),
        };
        await File.WriteAllTextAsync(a.Str("out") + ".summary.json", summary.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine("stopped");
        return 0;
    }

    /// <summary>
    /// Outbox relay process (P-09): the destination Runwick host plus <c>AddRunwickPostgresOutbox</c> with <c>--slots</c>
    /// in flight. The source adapter is wrapped by <see cref="LossySource"/>: <c>loss p</c> on stdin loses that fraction
    /// of delivered marks after the destination accepted (the mark request fails, nothing is written to the source).
    /// Every confirmed mark is logged as <c>messageId,unixMs,jobId,disposition</c>; every lost one as <c>lost,messageId,unixMs</c>.
    /// </summary>
    public static async Task<int> RelayAsync(Args a)
    {
        var sc = new ServiceCollection();
        var trace = a.Has("trace");
        var phases = new Metrics();
        using var listener = trace ? PhaseListener.Attach(phases) : null;
        JsonObject? measuredPhases = null;
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Error));
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app")).UsePostgres(a.Str("conn"));
            G2Jobs.Register(r, a);
        });
        var source = new NpgsqlConnectionStringBuilder(a.Str("source")) { ApplicationName = "rw:outbox:relay" }.ConnectionString;
        sc.AddRunwickPostgresOutbox(source, relay: o => o.MaxInFlight = a.Int("slots", 4));
        await using var log = new MarkLog(a.Str("out") + ".marks.csv");
        LossySource? lossy = null;
        sc.AddSingleton<IOutboxSource>(_ => lossy = new LossySource(new PostgresOutboxSource(source), log));
        await using var provider = sc.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var h in hosted)
        {
            await h.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        Console.WriteLine("ready");
        await WaitStopAsync(line =>
        {
            if (line.StartsWith("loss ", StringComparison.Ordinal) && lossy is not null)
            {
                lossy.LossRate = double.Parse(line[5..], CultureInfo.InvariantCulture);
                Console.WriteLine("loss set");
            }
            else if (trace && line == "trace-start")
            {
                phases.ResetLatencies();
            }
            else if (trace && line == "trace-stop")
            {
                measuredPhases = phases.LatenciesJson();
            }
        }).ConfigureAwait(false);
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        {
            foreach (var h in Enumerable.Reverse(hosted))
            {
                await h.StopAsync(cts.Token).ConfigureAwait(false);
            }
        }

        var relay = provider.GetRequiredService<OutboxRelay>();
        var summary = new JsonObject
        {
            ["slots"] = a.Int("slots", 4),
            ["delivered"] = Interlocked.Read(ref relay.Delivered),
            ["blocked"] = Interlocked.Read(ref relay.Blocked),
            ["retriesScheduled"] = Interlocked.Read(ref relay.RetriesScheduled),
            ["leaseLosses"] = Interlocked.Read(ref relay.LeaseLosses),
            ["sends"] = Interlocked.Read(ref relay.Sends),
            ["marksLost"] = lossy?.Lost ?? 0,
            ["process"] = WorkerMode.ProcessStats(new TimerLag()),
        };
        if (trace)
        {
            summary["phaseScope"] = measuredPhases is null ? "lifetime" : "measure";
            summary["phases"] = measuredPhases ?? phases.LatenciesJson();
        }

        await File.WriteAllTextAsync(a.Str("out") + ".summary.json", summary.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine("stopped");
        return 0;
    }

    /// <summary>
    /// Outbox business writers (P-09): open-loop Poisson arrivals at <c>--rate</c> messages/s served by <c>--writers</c>
    /// concurrent business transactions on the source database. Each inserts a business row, stages a 1 KiB
    /// <see cref="G2Out"/> and commits; after the commit returns (source-visible-ready) it logs <c>messageId,unixMs</c>.
    /// The relay is disabled in this process.
    /// </summary>
    public static async Task<int> StageAsync(Args a)
    {
        var metrics = new Metrics();
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        sc.AddRunwick(r =>
        {
            r.Application(a.Str("app")).UsePostgres(a.Str("conn"));
            G2Jobs.Register(r, a);
        });
        var source = new NpgsqlConnectionStringBuilder(a.Str("source")) { ApplicationName = "wl-writer", MaxPoolSize = 32 }.ConnectionString;
        sc.AddRunwickPostgresOutbox(source, relay: o => o.Enabled = false);
        await using var provider = sc.BuildServiceProvider();
        await provider.GetRequiredService<RunwickRuntime>().EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
        var outbox = provider.GetRequiredService<IPostgresOutbox>();
        await using var dataSource = NpgsqlDataSource.Create(source);
        var pad = G2Jobs.Pad(a.Int("payload", 1024));
        var rate = a.Double("rate", 100);
        using var writers = new SemaphoreSlim(a.Int("writers", 8));
        await using var log = new MarkLog(a.Str("out") + ".commits.csv");
        var random = new Random(a.Int("seed", 20261030));
        using var stop = new CancellationTokenSource();
        var control = Task.Run(async () =>
        {
            await WaitStopAsync(line =>
            {
                if (line.StartsWith("rate ", StringComparison.Ordinal))
                {
                    Volatile.Write(ref rate, double.Parse(line[5..], CultureInfo.InvariantCulture));
                }
            }).ConfigureAwait(false);
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
                        ["t"] = DateTimeOffset.UtcNow.ToString("O"), ["offered"] = metrics.Counter("offered"),
                        ["committed"] = metrics.Counter("committed"), ["failed"] = metrics.Counter("failed"),
                        ["waitingForWriter"] = metrics.Counter("offered") - metrics.Counter("started"),
                    }.ToJsonString()).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        Console.WriteLine("ready");
        var pending = new HashSet<Task>();
        var clock = Stopwatch.StartNew();
        var at = 0.0;
        var n = 0L;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                at += -Math.Log(1 - random.NextDouble()) / Math.Max(0.001, Volatile.Read(ref rate));
                var wait = at - clock.Elapsed.TotalSeconds;
                if (wait > 0.001)
                {
                    await Task.Delay(TimeSpan.FromSeconds(wait), stop.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }

            metrics.Count("offered");
            var id = (++n).ToString(CultureInfo.InvariantCulture);
            pending.Add(Task.Run(async () =>
            {
                await writers.WaitAsync().ConfigureAwait(false);
                metrics.Count("started");
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await using var conn = await dataSource.OpenConnectionAsync().ConfigureAwait(false);
                    await using var tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
                    await using (var insert = new NpgsqlCommand("INSERT INTO business_rows (id) VALUES (@id)", conn, tx))
                    {
                        insert.Parameters.AddWithValue("id", id);
                        await insert.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    var staged = await outbox.StageAsync(new G2Out(id, pad), tx).ConfigureAwait(false);
                    await tx.CommitAsync().ConfigureAwait(false);
                    log.Write($"{staged.MessageId:D},{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");
                    metrics.Observe("stageCommit", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                    metrics.Count("committed");
                }
                catch (Exception ex) when (ex is NpgsqlException or RunwickException or InvalidOperationException)
                {
                    metrics.Count("failed");
                    metrics.Count("failed:" + ex.GetType().Name);
                }
                finally
                {
                    writers.Release();
                }
            }));
            pending.RemoveWhere(t => t.IsCompleted);
        }

        await Task.WhenAll(pending).ConfigureAwait(false);
        await sampler.ConfigureAwait(false);
        await control.ConfigureAwait(false);
        var summary = new JsonObject
        {
            ["offered"] = metrics.Counter("offered"),
            ["committed"] = metrics.Counter("committed"),
            ["failed"] = metrics.Counter("failed"),
            ["counters"] = metrics.CountersJson(),
            ["latencies"] = metrics.LatenciesJson(),
            ["process"] = WorkerMode.ProcessStats(new TimerLag()),
        };
        await File.WriteAllTextAsync(a.Str("out") + ".summary.json", summary.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine("stopped");
        return 0;
    }

    static async Task WaitStopAsync(Action<string> command)
    {
        string? line;
        while ((line = await Console.In.ReadLineAsync().ConfigureAwait(false)) is not null && line != "stop")
        {
            command(line);
        }
    }
}

/// <summary>Append-only CSV log written from many tasks through one channel.</summary>
internal sealed class MarkLog : IAsyncDisposable
{
    readonly Channel<string> _lines = Channel.CreateUnbounded<string>();
    readonly Task _writer;

    public MarkLog(string path) => _writer = Task.Run(async () =>
    {
        await using var file = new StreamWriter(path, append: false);
        await foreach (var line in _lines.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            await file.WriteLineAsync(line).ConfigureAwait(false);
            if (_lines.Reader.Count == 0)
            {
                await file.FlushAsync().ConfigureAwait(false);
            }
        }
    });

    public void Write(string line) => _lines.Writer.TryWrite(line);

    public async ValueTask DisposeAsync()
    {
        _lines.Writer.TryComplete();
        await _writer.ConfigureAwait(false);
    }
}

/// <summary>
/// The real PostgreSQL source adapter with an injected lost delivered mark (P-09, OUT-01 shape): with probability
/// <see cref="LossRate"/> the mark request fails before it reaches the source, after the destination already applied
/// the delivery. The lease then expires and the next owner's resend must return the same mapping.
/// </summary>
internal sealed class LossySource(IOutboxSource inner, MarkLog log) : IOutboxSource, IAsyncDisposable
{
    long _lost;

    public double LossRate { get; set; }

    public long Lost => Interlocked.Read(ref _lost);

    public Guid SourceStoreId => inner.SourceStoreId;

    public Task InitializeAsync(CancellationToken ct) => inner.InitializeAsync(ct);

    public Task<IReadOnlyList<OutboxLease>> ClaimAsync(Guid owner, int max, TimeSpan leaseDuration, CancellationToken ct) =>
        inner.ClaimAsync(owner, max, leaseDuration, ct);

    public Task<IReadOnlyList<OutboxLease>> ClaimWithTransportAsync(Guid owner, int max, TimeSpan leaseDuration, DateTimeOffset transportAsOf,
        DateTimeOffset transportBound, CancellationToken ct) =>
        inner.ClaimWithTransportAsync(owner, max, leaseDuration, transportAsOf, transportBound, ct);

    public Task<bool> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken ct) => inner.RenewAsync(lease, leaseDuration, ct);

    public Task<bool> RecordTransportAsync(OutboxLease lease, Guid commandId, DateTimeOffset notAfter, CancellationToken ct) =>
        inner.RecordTransportAsync(lease, commandId, notAfter, ct);

    public async Task<bool> MarkDeliveredAsync(OutboxLease lease, OutboxDelivered delivered, CancellationToken ct)
    {
        var rate = LossRate;
        if (rate > 0 && Random.Shared.NextDouble() < rate)
        {
            Interlocked.Increment(ref _lost);
            log.Write($"lost,{lease.MessageId:D},{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()},{delivered.JobId:D}");
            throw new IOException("Injected loss of the delivered-mark request (P-09).");
        }

        var marked = await inner.MarkDeliveredAsync(lease, delivered, ct).ConfigureAwait(false);
        if (marked)
        {
            log.Write($"{lease.MessageId:D},{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()},{delivered.JobId:D},{delivered.Disposition}");
        }

        return marked;
    }

    public Task<bool> RetryLaterAsync(OutboxLease lease, TimeSpan delay, string error, CancellationToken ct) => inner.RetryLaterAsync(lease, delay, error, ct);

    public Task<bool> BlockAsync(OutboxLease lease, string reason, CancellationToken ct) => inner.BlockAsync(lease, reason, ct);

    public Task<int> PurgeDeliveredAsync(int maxRows, long maxBytes, CancellationToken ct) => inner.PurgeDeliveredAsync(maxRows, maxBytes, ct);

    public async ValueTask DisposeAsync()
    {
        if (inner is IAsyncDisposable d)
        {
            await d.DisposeAsync().ConfigureAwait(false);
        }
    }
}
