using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Postgres.Migrations;
using Runwick.Worker;
using static Runwick.Workloads.G2Orchestrator;

namespace Runwick.Workloads;

/// <summary>
/// SP-029 workloads the G-1/G-2 runners did not have (performance plan P-03, P-07, P-10), built on the G-2 repetition
/// (<see cref="G2Orchestrator.RunOnceAsync"/>): same provisioning, sampling, analysis and result layout.
/// <list type="bullet">
/// <item>P-03 payload pressure: 64 KiB and 1 MiB payloads at reduced offered rates (<c>--rate-64k</c> 16/s, <c>--rate-1m</c> 1/s),
/// P-01 handler; the 1 MiB + 1 byte limit check; allocation bytes per job in worker and producer; WAL bytes per job.</item>
/// <item>P-07 management: P-01 runs, <c>--terminal-jobs</c> (1,000,000) terminal jobs are cloned in from real finished jobs and
/// spread over <c>--seed-span-days</c> (30) so detail cleanup is active, then a baseline window and a window with
/// <c>--viewers</c> (20) polling every <c>--poll-s</c> (5) through the management session.</item>
/// <item>P-10 soak: <c>--duration-min</c> (1,440) at <c>--rate</c>, retries and bounded permanent failures, minute schedules,
/// short retention so cleanup runs; the worker reports post-full-GC heap and cache sizes every <c>--gc-interval-s</c> (300).</item>
/// </list>
/// </summary>
internal static class Sp029Workloads
{
    public static readonly string[] Names = ["P-01t", "P-03", "P-07", "P-10"];

    static string I(long v) => v.ToString(CultureInfo.InvariantCulture);

    static string D(double v) => v.ToString(CultureInfo.InvariantCulture);

    static TimeSpan Min(Args a, string name, double fallback) => TimeSpan.FromMinutes(a.Double(name, fallback));

    public static async Task<int> RunAsync(Args a)
    {
        var workload = a.Positional[0];
        var root = a.OptStr("out") ?? Path.Combine("artifacts", "perf", $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{workload}");
        Directory.CreateDirectory(root);
        await using var db = await TestDatabase.StartAsync().ConfigureAwait(false);
        var env = await EnvironmentInfo.CaptureAsync(db, Conn(db, "postgres", "wl:admin"), a).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path.Combine(root, "env.json"), env.ToJsonString(Json.Indented)).ConfigureAwait(false);
        var result = new JsonObject { ["workload"] = workload, ["startedAt"] = DateTimeOffset.UtcNow.ToString("O") };
        var runs = new JsonArray();
        var summary = new JsonObject();
        result["runs"] = runs;
        result["summary"] = summary;
        switch (workload)
        {
            case "P-01t":
                await P01tAsync(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            case "P-03":
                await P03Async(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            case "P-07":
                await P07Async(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            default:
                await P10Async(db, root, a, runs, summary).ConfigureAwait(false);
                break;
        }

        result["finishedAt"] = DateTimeOffset.UtcNow.ToString("O");
        result["loadAverageAtEnd"] = EnvironmentInfo.Read("/proc/loadavg");
        var violations = runs.Sum(r => r!["correctness"]!["violationCount"]!.GetValue<int>());
        result["correctnessViolations"] = violations;
        result["runFailures"] = runs.Count(r => r!["failure"] is not null);
        await File.WriteAllTextAsync(Path.Combine(root, "result.json"), result.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine($"{workload}: {runs.Count} run(s), {violations} correctness violation(s); results in {root}");
        return violations == 0 ? 0 : 1;
    }

    static string[] Worker(Args a, string queues, string handlers, int handlerMs, params string[] more) =>
        ["--queues", queues, "--concurrency", I(a.Int("concurrency", 16)), "--handler-ms", I(handlerMs), "--handlers", handlers, .. more];

    static async Task MeasureAsync(G2Context c, Args a, double warmupMin, double measureMin)
    {
        await c.WaitAsync(Min(a, "warmup-min", warmupMin)).ConfigureAwait(false);
        await c.Inner.ResetAsync().ConfigureAwait(false);
        c.Mark("measureStart");
        await c.WaitAsync(Min(a, "measure-min", measureMin)).ConfigureAwait(false);
        c.Mark("measureEnd");
    }

    static double? Div(double? x, double? y) => x is { } n && y is { } d && d != 0 ? Math.Round(n / d, 1) : null;

    static double? Num(JsonNode? n) => n?.GetValue<double>();

    /// <summary>
    /// The G-1 analysis compares producer-accepted dispatches with the jobs in the store. Seeded jobs (P-07), scheduler
    /// jobs and cleanup deletions (P-10) move that count legitimately, so the check is redone with them:
    /// accepted ≤ stored + deleted − extra ≤ accepted + unresolved + upperSlack.
    /// </summary>
    static void Reconcile(JsonObject run, long extra, long upperSlack, long deleted)
    {
        var correctness = (JsonObject)run["correctness"]!;
        var list = (JsonArray)correctness["violations"]!;
        foreach (var v in list.Where(v => v!.GetValue<string>().StartsWith("producer accepted", StringComparison.Ordinal)).ToList())
        {
            list.Remove(v);
        }

        var accepted = run["producer"]?["accepted"]?.GetValue<long>() ?? 0;
        var unresolved = run["producer"]?["outcomeUnresolved"]?.GetValue<long>() ?? 0;
        var produced = correctness["jobs"]!.GetValue<long>() + deleted - extra;
        if (produced < accepted || produced > accepted + unresolved + upperSlack)
        {
            list.Add($"producer accepted {accepted} ({unresolved} unresolved) but stored + deleted − seeded/scheduled is {produced}");
        }

        correctness["reconciled"] = new JsonObject { ["seededOrScheduled"] = extra, ["deletedByCleanup"] = deleted, ["producerJobs"] = produced };
        correctness["violationCount"] = list.Count;
    }

    static async Task<long> JobsDeletedAsync(string cs) =>
        (long)await ScalarAsync(cs, "SELECT coalesce(sum(n_tup_del), 0) FROM pg_stat_user_tables WHERE schemaname = 'runwick' AND relname = 'jobs'").ConfigureAwait(false);

    static ServiceProvider Management(string cs)
    {
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        sc.AddRunwick(r =>
        {
            r.Application(G2Jobs.App).UsePostgres(cs);
            G2Jobs.Register(r, new Args([]));
        });
        return sc.BuildServiceProvider();
    }

    // ------------------------------------------------------------------------------------------------ P-01t

    /// <summary>
    /// Instrumentation cost on P-01 (plan threshold: throughput −10 % at most, allocations/job +15 % at most): the same P-01
    /// with telemetry off, then with every Runwick activity sampled and every Runwick instrument enabled (in-process
    /// listeners standing in for a bounded exporter), alternating per repetition.
    /// </summary>
    static async Task P01tAsync(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var handlerMs = a.Int("handler-ms", 100);
        for (var rep = 1; rep <= a.Int("reps", 3); rep++)
        {
            foreach (var on in new[] { false, true })
            {
                var spec = new G2Spec("P-01t", $"{(on ? "on" : "off")}-rep{rep}", [], [Worker(a, "pu:1", "work", handlerMs, on ? ["--telemetry"] : [])],
                    ["--mode", "poisson", "--job", "work", "--rate", D(a.Double("rate", 100))], a.Int("concurrency", 16), handlerMs,
                    Drain: true, Capabilities: []);
                var run = await RunOnceAsync(db, root, spec, c => MeasureAsync(c, a, 5, 15), drainMinutes: a.Double("drain-min", 90)).ConfigureAwait(false);
                runs.Add(run);
                var w = run["workers"]?[0];
                summary[spec.Label] = new JsonObject
                {
                    ["telemetry"] = on,
                    ["confirmedFinishesPerSec"] = run["window"]?["confirmedFinishesPerSec"]?.DeepClone(),
                    ["workerAllocatedBytes"] = w?["process"]?["allocatedBytes"]?.DeepClone(),
                    ["workerInvocations"] = w?["invocations"]?.DeepClone(),
                    ["workerAllocatedBytesPerJob"] = Div(Num(w?["process"]?["allocatedBytes"]), Num(w?["invocations"])),
                    ["workerCpuMsPerJob"] = Div(Num(w?["process"]?["cpuMs"]) * 1000, Num(w?["invocations"])) is { } us ? Math.Round(us / 1000, 3) : null,
                };
            }
        }
    }

    /// <summary>Samples every Runwick activity and listens to every Runwick instrument, counting what it receives.</summary>
    internal static IDisposable ListenToTelemetry()
    {
        long events = 0;
        var activities = new System.Diagnostics.ActivityListener
        {
            ShouldListenTo = s => s.Name == RunwickTelemetry.SourceName,
            Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) => System.Diagnostics.ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = _ => Interlocked.Increment(ref events),
        };
        System.Diagnostics.ActivitySource.AddActivityListener(activities);
        var meters = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (i, l) =>
            {
                if (i.Meter.Name == RunwickTelemetry.SourceName)
                {
                    l.EnableMeasurementEvents(i);
                }
            },
        };
        meters.SetMeasurementEventCallback<long>((_, _, _, _) => Interlocked.Increment(ref events));
        meters.SetMeasurementEventCallback<double>((_, _, _, _) => Interlocked.Increment(ref events));
        meters.SetMeasurementEventCallback<int>((_, _, _, _) => Interlocked.Increment(ref events));
        meters.Start();
        return new Listeners(activities, meters);
    }

    sealed class Listeners(System.Diagnostics.ActivityListener activities, System.Diagnostics.Metrics.MeterListener meters) : IDisposable
    {
        public void Dispose()
        {
            activities.Dispose();
            meters.Dispose();
        }
    }

    // ------------------------------------------------------------------------------------------------ P-03

    static async Task P03Async(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        summary["limitCheck"] = await LimitCheckAsync(db).ConfigureAwait(false);
        var handlerMs = a.Int("handler-ms", 100);
        foreach (var (label, payload, rate) in new[] { ("64k", 65_536, a.Double("rate-64k", 16)), ("1m", 1_048_576, a.Double("rate-1m", 1)) })
        {
            var reps = new JsonArray();
            for (var rep = 1; rep <= a.Int("reps", 3); rep++)
            {
                var spec = new G2Spec("P-03", $"{label}-rep{rep}", [], [Worker(a, "pu:1", "work", handlerMs)],
                    ["--mode", "poisson", "--job", "work", "--rate", D(rate), "--payload", I(payload)], a.Int("concurrency", 16), handlerMs,
                    Drain: true, Capabilities: []);
                var run = await RunOnceAsync(db, root, spec, c => MeasureAsync(c, a, 2, 15), drainMinutes: a.Double("drain-min", 30)).ConfigureAwait(false);
                runs.Add(run);
                var w = run["workers"]?[0];
                var p = run["producer"];
                reps.Add(new JsonObject
                {
                    ["payloadBytes"] = payload,
                    ["offeredPerSec"] = run["producerWindow"]?["offeredPerSec"]?.DeepClone(),
                    ["acceptedPerSec"] = run["producerWindow"]?["acceptedPerSec"]?.DeepClone(),
                    ["ingressBytesPerSec"] = Num(run["producerWindow"]?["acceptedPerSec"]) is { } acc ? Math.Round(acc * payload) : null,
                    ["confirmedFinishesPerSec"] = run["window"]?["confirmedFinishesPerSec"]?.DeepClone(),
                    ["backlogSlopePerSec"] = run["window"]?["backlogSlopePerSec"]?.DeepClone(),
                    ["dispatchP99Ms"] = p?["latencies"]?["dispatchCall"]?["p99Ms"]?.DeepClone(),
                    ["pipelineToFinishP99Ms"] = w?["latencies"]?["pipelineToFinish"]?["p99Ms"]?.DeepClone(),
                    ["walBytesPerJob"] = run["window"]?["walBytesPerJob"]?.DeepClone(),
                    ["workerAllocatedBytesPerJob"] = Div(Num(w?["process"]?["allocatedBytes"]), Num(w?["invocations"])),
                    ["producerAllocatedBytesPerJob"] = Div(Num(p?["process"]?["allocatedBytes"]), Num(p?["accepted"])),
                    ["workerPeakRssBytes"] = w?["process"]?["rssBytes"]?.DeepClone(),
                    ["failedDispatches"] = p?["failed"]?.DeepClone(),
                    ["violations"] = run["correctness"]?["violationCount"]?.DeepClone(),
                    ["failure"] = run["failure"]?.DeepClone(),
                });
            }

            summary[label] = reps;
        }
    }

    /// <summary>One byte over the 1 MiB payload bound must be refused with <c>payload_too_large</c> and store nothing.</summary>
    static async Task<JsonObject> LimitCheckAsync(TestDatabase db)
    {
        const string database = "wl_p03_limit";
        await ExecAsync(Conn(db, "postgres", "wl:admin"), $"DROP DATABASE IF EXISTS {database} WITH (FORCE)").ConfigureAwait(false);
        await ExecAsync(Conn(db, "postgres", "wl:admin"), $"CREATE DATABASE {database}").ConfigureAwait(false);
        await RunwickPostgresMigrations.MigrateAsync(Conn(db, database, "wl:migrate")).ConfigureAwait(false);
        var cs = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = "wl-limit" }.ConnectionString;
        await using var provider = Management(cs);
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var h in hosted)
        {
            await h.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        string? code = null;
        try
        {
            // The serialized object adds {"id":"1","pad":""} around the pad, so this is above the bound.
            await provider.GetRequiredService<IJobDispatcher>().DispatchAsync(new G2Work("1", new string('x', 1_048_576))).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            code = ex is RunwickException rx ? rx.Code : ex.GetType().Name;
        }

        foreach (var h in hosted)
        {
            await h.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        var stored = await ScalarAsync(Conn(db, database, "wl:analysis"), "SELECT count(*) FROM runwick.jobs").ConfigureAwait(false);
        return new JsonObject { ["oversizeRejectedWith"] = code, ["jobsStored"] = stored, ["passed"] = code == "payload_too_large" && stored == 0 };
    }

    // ------------------------------------------------------------------------------------------------ P-07

    static async Task P07Async(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        for (var rep = 1; rep <= a.Int("reps", 1); rep++)
        {
            var spec = new G2Spec("P-07", $"rep{rep}", [], [Worker(a, "pu:1", "work", a.Int("handler-ms", 100))],
                ["--mode", "poisson", "--job", "work", "--rate", D(a.Double("rate", 100))], a.Int("concurrency", 16), a.Int("handler-ms", 100),
                Drain: true, Capabilities: []);
            long seeded = 0;
            var run = await RunOnceAsync(db, root, spec, async c =>
            {
                var store = c.StoreConnection("wl:seed");
                await c.WaitAsync(Min(a, "warmup-min", 5)).ConfigureAwait(false);
                c.Mark("seedStart");
                var seed = await SeedAsync(store, a.Int("terminal-jobs", 1_000_000), a.Double("seed-span-days", 30)).ConfigureAwait(false);
                seeded = seed["seeded"]!.GetValue<long>();
                c.Mark("seedEnd");
                c.Extra["seed"] = seed;
                await c.WaitAsync(Min(a, "settle-min", 1)).ConfigureAwait(false);
                await c.Inner.ResetAsync().ConfigureAwait(false);
                c.Mark("measureStart");
                var s0 = await TableStatsAsync(store).ConfigureAwait(false);
                await c.WaitAsync(Min(a, "measure-min", 15)).ConfigureAwait(false);
                c.Mark("baselineEnd");
                var s1 = await TableStatsAsync(store).ConfigureAwait(false);

                await using var provider = Management(new NpgsqlConnectionStringBuilder(store) { Pooling = true, ApplicationName = "wl-viewers" }.ConnectionString);
                var viewers = new Viewers(provider.GetRequiredService<IManagementSession>(), TimeSpan.FromSeconds(a.Double("poll-s", 5)));
                using var stop = new CancellationTokenSource();
                var tasks = Enumerable.Range(0, a.Int("viewers", 20)).Select(i => viewers.RunAsync(i, stop.Token)).ToList();
                await c.WaitAsync(Min(a, "measure-min", 15)).ConfigureAwait(false);
                c.Mark("measureEnd");
                await stop.CancelAsync().ConfigureAwait(false);
                await Task.WhenAll(tasks).ConfigureAwait(false);
                var s2 = await TableStatsAsync(store).ConfigureAwait(false);

                var samples = c.Inner.Sampler.Samples;
                double Rate(string from, string to)
                {
                    var w = samples.Where(x => x.At >= c.Inner.Marks[from] && x.At <= c.Inner.Marks[to]).ToList();
                    return w.Count < 2 ? 0 : (Analysis.Terminal(w[^1]) - Analysis.Terminal(w[0])) / (w[^1].At - w[0].At).TotalSeconds;
                }

                var (baseline, loaded) = (Rate("measureStart", "baselineEnd"), Rate("baselineEnd", "measureEnd"));
                var retention = await provider.GetRequiredService<IManagementSession>()
                    .GetRetentionAsync(new ManagementCaller(new ManagementActor("svc:p07", null, "host"), ManagementRoles.Viewer)).ConfigureAwait(false);
                // Rows the viewers made PostgreSQL scan sequentially: the extra over the baseline window (the sampler's own
                // count scan runs in both), per poll. An exact count over the job table would show as ≈ one table per poll.
                var extraSeq = (s2.SeqTupRead - s1.SeqTupRead) - (s1.SeqTupRead - s0.SeqTupRead);
                c.Extra["management"] = new JsonObject
                {
                    ["baselineFinishesPerSec"] = Math.Round(baseline, 2),
                    ["withViewersFinishesPerSec"] = Math.Round(loaded, 2),
                    ["throughputDegradationPercent"] = baseline == 0 ? null : Math.Round(100 * (baseline - loaded) / baseline, 1),
                    ["viewers"] = a.Int("viewers", 20),
                    ["polls"] = viewers.Polls,
                    ["errors"] = viewers.Errors,
                    ["errorKinds"] = new JsonObject(viewers.ErrorKinds.ToArray().Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
                    ["latencies"] = viewers.Metrics.LatenciesJson(),
                    ["jobRows"] = s2.Rows,
                    ["extraSeqRowsReadPerPoll"] = viewers.Polls == 0 ? null : Math.Round((double)extraSeq / viewers.Polls, 1),
                    ["cleanup"] = new JsonObject(retention.Classes.Select(x => KeyValuePair.Create(x.RecordClass, (JsonNode?)x.TotalRows))),
                };
            }, async c => c.Extra["jobsDeleted"] = await JobsDeletedAsync(c.StoreConnection("wl:analysis")).ConfigureAwait(false),
                a.Double("drain-min", 60)).ConfigureAwait(false);
            Reconcile(run, seeded, 0, run["extra"]?["jobsDeleted"]?.GetValue<long>() ?? 0);
            runs.Add(run);
            summary[$"rep{rep}"] = run["extra"]?["management"]?.DeepClone();
        }
    }

    sealed record TableStats(long Rows, long SeqTupRead);

    static async Task<TableStats> TableStatsAsync(string cs)
    {
        var rows = await RowsAsync(cs, """
            SELECT coalesce(sum(n_live_tup), 0), coalesce(sum(seq_tup_read), 0) FROM pg_stat_user_tables
            WHERE schemaname = 'runwick' AND relname IN ('jobs', 'job_attempts', 'job_generations', 'job_payloads')
            """).ConfigureAwait(false);
        return new(Convert.ToInt64(rows[0][0], CultureInfo.InvariantCulture), Convert.ToInt64(rows[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>Dashboard viewers: each polls a job list page, the succeeded and pending filters and the queue list every interval.</summary>
    sealed class Viewers(IManagementSession session, TimeSpan interval)
    {
        static readonly ManagementCaller Caller = new(new ManagementActor("svc:p07-viewer", null, "host"), ManagementRoles.Viewer);
        long _polls;
        long _errors;

        public Metrics Metrics { get; } = new();

        public System.Collections.Concurrent.ConcurrentDictionary<string, long> ErrorKinds { get; } = new(StringComparer.Ordinal);

        public long Polls => Interlocked.Read(ref _polls);

        public long Errors => Interlocked.Read(ref _errors);

        public async Task RunAsync(int index, CancellationToken ct)
        {
            try
            {
                await Task.Delay(interval * (index / 20.0), ct).ConfigureAwait(false);
                using var timer = new PeriodicTimer(interval);
                do
                {
                    await Call("listJobs", () => session.ListJobsAsync(Caller, new JobListQuery(), ct)).ConfigureAwait(false);
                    await Call("listSucceeded", () => session.ListJobsAsync(Caller, new JobListQuery(State: "succeeded"), ct)).ConfigureAwait(false);
                    await Call("listPending", () => session.ListJobsAsync(Caller, new JobListQuery(Queue: "pu", State: "pending"), ct)).ConfigureAwait(false);
                    await Call("listQueues", () => session.ListQueuesAsync(Caller, cancellationToken: ct)).ConfigureAwait(false);
                    Interlocked.Increment(ref _polls);
                }
                while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        async Task Call<T>(string name, Func<Task<T>> call)
        {
            var started = Stopwatch.GetTimestamp();
            try
            {
                await call().ConfigureAwait(false);
                var ms = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                Metrics.Observe(name, ms);
                Metrics.Observe("all", ms);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Interlocked.Increment(ref _errors);
                ErrorKinds.AddOrUpdate(ex.GetType().Name, 1, (_, n) => n + 1);
            }
        }
    }

    /// <summary>
    /// Clones the store's succeeded jobs (jobs, generations, attempts, payloads, with new identities) until
    /// <paramref name="target"/> terminal jobs exist, every timestamp shifted back by whole seconds up to the span, so
    /// the rows are real store rows with realistic age. Batches of 50,000 per transaction.
    /// </summary>
    static async Task<JsonObject> SeedAsync(string cs, long target, double spanDays)
    {
        var started = Stopwatch.GetTimestamp();
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync().ConfigureAwait(false);
        async Task Exec(string sql, NpgsqlTransaction? tx = null, params (string, object)[] ps)
        {
            await using var cmd = new NpgsqlCommand(sql, conn, tx) { CommandTimeout = 0 };
            foreach (var (n, v) in ps)
            {
                cmd.Parameters.AddWithValue(n, v);
            }

            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await Exec("CREATE TEMP TABLE seed_template AS SELECT job_id FROM runwick.jobs WHERE application = 'wl' AND state = 'succeeded'").ConfigureAwait(false);
        long template;
        await using (var cmd = new NpgsqlCommand("SELECT count(*) FROM seed_template", conn))
        {
            template = (long)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
        }

        if (template == 0)
        {
            throw new InvalidOperationException("P-07 seed: no succeeded job to clone after the warm-up");
        }

        var ts = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        await using (var cmd = new NpgsqlCommand("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = 'runwick' AND data_type = 'timestamp with time zone'
              AND table_name IN ('jobs', 'job_generations', 'job_attempts', 'job_payloads')
            """, conn))
        await using (var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
        {
            while (await r.ReadAsync().ConfigureAwait(false))
            {
                (ts.TryGetValue(r.GetString(0), out var l) ? l : ts[r.GetString(0)] = []).Add(r.GetString(1));
            }
        }

        string Shift(string table, string alias) =>
            string.Concat(ts.GetValueOrDefault(table, []).Select(col => $", '{col}', {alias}.{col} - m.shift"));

        long seeded = 0;
        const long batch = 50_000;
        while (seeded < target)
        {
            var n = Math.Min(batch, target - seeded);
            await using var tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
            await Exec("""
                CREATE TEMP TABLE seed_map ON COMMIT DROP AS
                SELECT t.job_id AS old_job, gen_random_uuid() AS new_job, make_interval(secs => floor(random() * @span)) AS shift
                FROM seed_template t CROSS JOIN generate_series(1, @copies) LIMIT @n
                """, tx, ("span", spanDays * 86_400), ("copies", (int)((n + template - 1) / template)), ("n", n)).ConfigureAwait(false);
            await Exec("""
                CREATE TEMP TABLE seed_amap ON COMMIT DROP AS
                SELECT m.new_job, a.attempt_id AS old_att, gen_random_uuid() AS new_att
                FROM seed_map m JOIN runwick.job_attempts a ON a.application = 'wl' AND a.job_id = m.old_job
                """, tx).ConfigureAwait(false);
            await Exec($"""
                INSERT INTO runwick.jobs
                SELECT (jsonb_populate_record(NULL::runwick.jobs, to_jsonb(j) || jsonb_build_object('job_id', m.new_job, 'current_attempt_id', am.new_att{Shift("jobs", "j")}))).*
                FROM seed_map m JOIN runwick.jobs j ON j.application = 'wl' AND j.job_id = m.old_job
                LEFT JOIN seed_amap am ON am.new_job = m.new_job AND am.old_att = j.current_attempt_id
                """, tx).ConfigureAwait(false);
            await Exec($"""
                INSERT INTO runwick.job_generations
                SELECT (jsonb_populate_record(NULL::runwick.job_generations, to_jsonb(g) || jsonb_build_object('job_id', m.new_job{Shift("job_generations", "g")}))).*
                FROM seed_map m JOIN runwick.job_generations g ON g.application = 'wl' AND g.job_id = m.old_job
                """, tx).ConfigureAwait(false);
            await Exec($"""
                INSERT INTO runwick.job_attempts
                SELECT (jsonb_populate_record(NULL::runwick.job_attempts, to_jsonb(x) || jsonb_build_object('job_id', m.new_job, 'attempt_id', am.new_att{Shift("job_attempts", "x")}))).*
                FROM seed_amap am JOIN seed_map m ON m.new_job = am.new_job
                JOIN runwick.job_attempts x ON x.application = 'wl' AND x.attempt_id = am.old_att
                """, tx).ConfigureAwait(false);
            await Exec($"""
                INSERT INTO runwick.job_payloads
                SELECT (jsonb_populate_record(NULL::runwick.job_payloads, to_jsonb(p) || jsonb_build_object('job_id', m.new_job{Shift("job_payloads", "p")}))).*
                FROM seed_map m JOIN runwick.job_payloads p ON p.application = 'wl' AND p.job_id = m.old_job
                """, tx).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
            seeded += n;
        }

        await Exec("ANALYZE runwick.jobs, runwick.job_generations, runwick.job_attempts, runwick.job_payloads").ConfigureAwait(false);
        return new JsonObject
        {
            ["templateJobs"] = template, ["seeded"] = seeded, ["spanDays"] = spanDays,
            ["seconds"] = Math.Round(Stopwatch.GetElapsedTime(started).TotalSeconds, 1),
        };
    }

    // ------------------------------------------------------------------------------------------------ P-10

    /// <summary>Faults of the soak handler: per 1,000 job ids, permanent failures (every execution) and one-off failures (first execution only).</summary>
    public sealed record SoakFaults(int FailPermille, int RetryPermille);

    internal sealed class SoakHandler(IJobContext context, G2Behaviour behaviour, SoakFaults faults) : IJobHandler<G2Work>
    {
        public Task HandleAsync(G2Work job, CancellationToken cancellationToken)
        {
            var n = long.Parse(job.Id, CultureInfo.InvariantCulture) % 1000;
            if (n < faults.FailPermille || (n < faults.FailPermille + faults.RetryPermille && context.ExecutionNumber == 1))
            {
                throw new InvalidOperationException(n < faults.FailPermille ? "soak: permanent failure" : "soak: transient failure");
            }

            return behaviour.Duration <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(behaviour.Duration, cancellationToken);
        }
    }

    public static void AddSoakHandler(RunwickBuilder r, Args a)
    {
        r.Services.AddSingleton(new SoakFaults(a.Int("fail-permille", 2), a.Int("retry-permille", 20)));
        r.AddHandler<G2Work, SoakHandler>();
    }

    /// <summary>The worker's <c>gc</c> line: managed heap after a forced full blocking GC, and the live cache sizes.</summary>
    internal static string GcSample(WorkerEngine engine)
    {
        var heap = GC.GetTotalMemory(forceFullCollection: true);
        using var p = Process.GetCurrentProcess();
        return new JsonObject
        {
            ["t"] = DateTimeOffset.UtcNow.ToString("O"),
            ["heapAfterFullGcBytes"] = heap,
            ["rssBytes"] = p.WorkingSet64,
            ["attemptEntries"] = engine.Entries.Count,
            ["scopeMap"] = engine.ScopeMap.Count,
            ["finishesSent"] = Interlocked.Read(ref engine.FinishesSent),
            ["invocations"] = Interlocked.Read(ref engine.Invocations),
            ["threads"] = p.Threads.Count,
        }.ToJsonString();
    }

    static async Task P10Async(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var duration = Min(a, "duration-min", 1440);
        var concurrency = a.Int("concurrency", 16);
        var spec = new G2Spec("P-10", "soak", [], [Worker(a, "pu:1,ps:1", "soakwork,tick", a.Int("handler-ms", 10), "--soak",
                "--fail-permille", I(a.Int("fail-permille", 2)), "--retry-permille", I(a.Int("retry-permille", 20)))],
            ["--mode", "poisson", "--job", "work", "--rate", D(a.Double("rate", 35))], concurrency, a.Int("handler-ms", 10),
            Drain: true, Capabilities: ["scheduler"]);
        var run = await RunOnceAsync(db, root, spec, async c =>
        {
            // Realistic but short retention, so every cleanup class has work during the run (whole hours, 1 h minimum).
            await using (var provider = Management(c.StoreConnection("wl:retention")))
            {
                var session = provider.GetRequiredService<IManagementSession>();
                var admin = new ManagementCaller(new ManagementActor("svc:p10", null, "host"), ManagementRoles.Administrator);
                var current = await session.GetRetentionAsync(admin).ConfigureAwait(false);
                var policy = new RetentionPolicy(TimeSpan.FromHours(1), TimeSpan.FromHours(1), TimeSpan.FromHours(a.Int("history-h", 2)),
                    TimeSpan.FromDays(7), TimeSpan.FromDays(7), TimeSpan.FromHours(2), TimeSpan.FromHours(24));
                var edited = await session.EditRetentionPolicyAsync(admin, Guid.CreateVersion7(), current.Revision, policy, "P-10 soak").ConfigureAwait(false);
                c.Extra["retentionPolicy"] = edited.Succeeded ? $"detail 1 h, history {a.Int("history-h", 2)} h, occurrences 2 h" : "edit failed: " + edited.Code;
            }

            c.Start("scheduler", "scheduler", ["--schedules", I(a.Int("schedules", 100))], TimeSpan.FromMinutes(10));
            var interval = TimeSpan.FromSeconds(a.Double("gc-interval-s", 300));
            async Task Gc()
            {
                foreach (var w in c.Inner.Workers)
                {
                    await w.SendAsync("gc").ConfigureAwait(false);
                }
            }

            await c.WaitAsync(Min(a, "warmup-min", 5)).ConfigureAwait(false);
            await c.Inner.ResetAsync().ConfigureAwait(false);
            c.Mark("measureStart");
            var end = DateTime.UtcNow + duration;
            while (DateTime.UtcNow < end)
            {
                await Gc().ConfigureAwait(false);
                await c.WaitAsync(interval < end - DateTime.UtcNow ? interval : end - DateTime.UtcNow).ConfigureAwait(false);
            }

            await Gc().ConfigureAwait(false);
            await Task.Delay(2000).ConfigureAwait(false);
            c.Mark("measureEnd");
            await c.StopAsync("scheduler", TimeSpan.FromMinutes(2)).ConfigureAwait(false);
        }, async c =>
        {
            c.Extra["jobsDeleted"] = await JobsDeletedAsync(c.StoreConnection("wl:analysis")).ConfigureAwait(false);
            var scheduler = Path.Combine(c.Dir, "scheduler.summary.json");
            c.Extra["scheduledJobs"] = File.Exists(scheduler) ? JsonNode.Parse(await File.ReadAllTextAsync(scheduler).ConfigureAwait(false))?["materializations"]?.DeepClone() : null;
            c.Extra["growth"] = Growth(Path.Combine(c.Dir, "worker1.gc.jsonl"), c.Inner.Marks["measureStart"], c.Inner.Marks["measureEnd"], concurrency);
        }, a.Double("drain-min", 60)).ConfigureAwait(false);
        Reconcile(run, 0, run["extra"]?["scheduledJobs"]?.GetValue<long>() ?? 0, run["extra"]?["jobsDeleted"]?.GetValue<long>() ?? 0);
        runs.Add(run);
        summary["growth"] = run["extra"]?["growth"]?.DeepClone();
        summary["confirmedFinishesPerSec"] = run["window"]?["confirmedFinishesPerSec"]?.DeepClone();
        summary["outcomes"] = run["store"]?["outcomes"]?.DeepClone();
    }

    /// <summary>
    /// P-10 bounded growth (plan: last 6 h vs the first comparable steady 6 h of 24 h, i.e. the last vs the first quarter of
    /// the measured run): median post-full-GC heap ratio ≤ 1.10, and live attempt/scope caches not growing with completions
    /// (last-quarter mean ≤ 1.1 × first-quarter mean + 1, attempt entries never above the ticket count).
    /// </summary>
    internal static JsonObject Growth(string path, DateTimeOffset start, DateTimeOffset end, int concurrency)
    {
        var o = new JsonObject();
        if (!File.Exists(path))
        {
            o["error"] = "no gc samples";
            return o;
        }

        var samples = File.ReadAllLines(path).Where(l => l.EndsWith('}')).Select(l => JsonNode.Parse(l)!)
            .Select(n => (At: DateTimeOffset.Parse(n["t"]!.GetValue<string>(), CultureInfo.InvariantCulture), N: n))
            .Where(x => x.At >= start && x.At <= end).ToList();
        var quarter = (end - start) / 4;
        var q1 = samples.Where(x => x.At < start + quarter).ToList();
        var q4 = samples.Where(x => x.At >= end - quarter).ToList();
        if (q1.Count == 0 || q4.Count == 0)
        {
            o["error"] = $"too few gc samples ({samples.Count})";
            return o;
        }

        static double Median(IEnumerable<double> xs)
        {
            var s = xs.OrderBy(x => x).ToList();
            return s.Count % 2 == 1 ? s[s.Count / 2] : (s[(s.Count / 2) - 1] + s[s.Count / 2]) / 2;
        }

        double Med(List<(DateTimeOffset At, JsonNode N)> q, string k) => Median(q.Select(x => x.N[k]!.GetValue<double>()));
        double Mean(List<(DateTimeOffset At, JsonNode N)> q, string k) => q.Average(x => x.N[k]!.GetValue<double>());
        var heapRatio = Med(q4, "heapAfterFullGcBytes") / Med(q1, "heapAfterFullGcBytes");
        var cachesBounded = Mean(q4, "attemptEntries") <= (1.1 * Mean(q1, "attemptEntries")) + 1
            && Mean(q4, "scopeMap") <= (1.1 * Mean(q1, "scopeMap")) + 1
            && samples.Max(x => x.N["attemptEntries"]!.GetValue<long>()) <= concurrency;
        o["samples"] = samples.Count;
        o["firstQuarter"] = new JsonObject
        {
            ["samples"] = q1.Count, ["heapMedianBytes"] = Med(q1, "heapAfterFullGcBytes"), ["rssMedianBytes"] = Med(q1, "rssBytes"),
            ["attemptEntriesMean"] = Math.Round(Mean(q1, "attemptEntries"), 2), ["scopeMapMean"] = Math.Round(Mean(q1, "scopeMap"), 2),
            ["finishesAtEnd"] = q1[^1].N["finishesSent"]!.DeepClone(),
        };
        o["lastQuarter"] = new JsonObject
        {
            ["samples"] = q4.Count, ["heapMedianBytes"] = Med(q4, "heapAfterFullGcBytes"), ["rssMedianBytes"] = Med(q4, "rssBytes"),
            ["attemptEntriesMean"] = Math.Round(Mean(q4, "attemptEntries"), 2), ["scopeMapMean"] = Math.Round(Mean(q4, "scopeMap"), 2),
            ["finishesAtEnd"] = q4[^1].N["finishesSent"]!.DeepClone(),
        };
        o["heapRatio"] = Math.Round(heapRatio, 3);
        o["rssRatio"] = Math.Round(Med(q4, "rssBytes") / Med(q1, "rssBytes"), 3);
        o["heapBounded"] = heapRatio <= 1.10;
        o["cachesBounded"] = cachesBounded;
        return o;
    }
}
