using System.Globalization;
using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;

namespace Runwick.Workloads;

/// <summary>Shape of one workload run (07 §14.3).</summary>
internal sealed record WorkloadSpec(
    string Name,
    int Workers,
    int Concurrency,
    int HandlerMs,
    double Rate,
    int? Count,
    int? TypeCeiling,
    int? Divisor,
    bool Proxy,
    bool Drain);

/// <summary>Live handles of one repetition, used by the phase scripts.</summary>
internal sealed class RunContext(string dir, DbSampler sampler, IReadOnlyList<ChildProcess> workers, ChildProcess? producer,
    PgFaultProxy? proxy, TestDatabase db)
{
    public string Dir => dir;

    public DbSampler Sampler => sampler;

    public IReadOnlyList<ChildProcess> Workers => workers;

    public ChildProcess? Producer => producer;

    public PgFaultProxy? Proxy => proxy;

    public TestDatabase Database => db;

    public Dictionary<string, DateTimeOffset> Marks { get; } = new(StringComparer.Ordinal);

    public JsonObject Extra { get; } = [];

    public void Mark(string name)
    {
        Marks[name] = DateTimeOffset.UtcNow;
        Console.WriteLine($"  {DateTime.UtcNow:HH:mm:ss} {name}");
    }

    public async Task ResetAsync()
    {
        foreach (var w in workers)
        {
            await w.SendAsync("reset").ConfigureAwait(false);
        }

        if (producer is not null)
        {
            await producer.SendAsync("reset").ConfigureAwait(false);
        }
    }

    public async Task WaitAsync(TimeSpan span)
    {
        var end = DateTime.UtcNow + span;
        while (DateTime.UtcNow < end)
        {
            if (workers.Any(w => w.HasExited) || producer?.HasExited == true)
            {
                throw new InvalidOperationException("a child process exited during the workload");
            }

            await Task.Delay(TimeSpan.FromSeconds(Math.Min(5, Math.Max(0.05, (end - DateTime.UtcNow).TotalSeconds)))).ConfigureAwait(false);
        }
    }
}

/// <summary><c>run &lt;workload&gt;</c>: provisions the store, runs the children and phase script, analyses, writes results.</summary>
internal static class Orchestrator
{
    static double _drainMinutes = 60;

    public static async Task<int> RunAsync(Args a)
    {
        var workload = a.Positional.Count > 0 ? a.Positional[0] : throw new ArgumentException("run <workload>");
        var root = a.OptStr("out") ?? Path.Combine("artifacts", "perf", $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{workload}");
        Directory.CreateDirectory(root);
        await using var db = await TestDatabase.StartAsync().ConfigureAwait(false);
        var admin = Conn(db, "postgres", "wl:admin");
        var env = await EnvironmentInfo.CaptureAsync(db, admin, a).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path.Combine(root, "env.json"), env.ToJsonString(Json.Indented)).ConfigureAwait(false);

        var reps = a.Int("reps", 3);
        _drainMinutes = a.Double("drain-min", 60);
        var warmup = TimeSpan.FromMinutes(a.Double("warmup-min", 5));
        var measure = TimeSpan.FromMinutes(a.Double("measure-min", 15));
        var result = new JsonObject { ["workload"] = workload, ["startedAt"] = DateTimeOffset.UtcNow.ToString("O") };
        var runs = new JsonArray();
        result["runs"] = runs;

        switch (workload)
        {
            case "P-01":
                for (var rep = 1; rep <= reps; rep++)
                {
                    runs.Add(await RunOnceAsync(db, root, $"rep{rep}", P01(a), async c =>
                    {
                        await c.WaitAsync(warmup).ConfigureAwait(false);
                        await c.ResetAsync().ConfigureAwait(false);
                        c.Mark("measureStart");
                        await c.WaitAsync(measure).ConfigureAwait(false);
                        c.Mark("measureEnd");
                    }).ConfigureAwait(false));
                }

                break;
            case "P-02":
            {
                // The 07 §14.3 ramp (50/s, +50/s). When even its first step saturates, a low ramp (deviation, recorded)
                // finds this host's sustainable rate for the confirmation runs and G1-M3.
                var ramp = await RunOnceAsync(db, root, "ramp", P01(a) with { Name = "P-02", HandlerMs = 0, Rate = 50 },
                    c => RampAsync(c, 50, 50, a)).ConfigureAwait(false);
                runs.Add(ramp);
                var sustainable = ramp["extra"]!["sustainableRate"]!.GetValue<double>();
                if (sustainable == 0)
                {
                    var lowStart = a.Double("low-ramp-start", 0.5);
                    var low = await RunOnceAsync(db, root, "lowramp", P01(a) with { Name = "P-02", HandlerMs = 0, Rate = lowStart },
                        c => RampAsync(c, lowStart, a.Double("low-ramp-step", 0.5), a)).ConfigureAwait(false);
                    runs.Add(low);
                    sustainable = low["extra"]!["sustainableRate"]!.GetValue<double>();
                    result["sustainableRateSource"] = "low ramp (deviation: the 50/s first step saturated)";
                }

                result["sustainableRate"] = sustainable;
                var confirmWarmup = TimeSpan.FromMinutes(a.Double("confirm-warmup-min", 2));
                for (var rep = 1; rep <= reps && sustainable > 0; rep++)
                {
                    runs.Add(await RunOnceAsync(db, root, $"confirm{rep}", P01(a) with { Name = "P-02", HandlerMs = 0, Rate = sustainable }, async c =>
                    {
                        await c.WaitAsync(confirmWarmup).ConfigureAwait(false);
                        await c.ResetAsync().ConfigureAwait(false);
                        c.Mark("measureStart");
                        await c.WaitAsync(measure).ConfigureAwait(false);
                        c.Mark("measureEnd");
                    }).ConfigureAwait(false));
                }

                break;
            }

            case "P-08a" or "P-08b":
            {
                var latency = TimeSpan.FromSeconds(workload == "P-08a" ? 1 : 5);
                var phase = TimeSpan.FromMinutes(a.Double("phase-min", 5));
                for (var rep = 1; rep <= reps; rep++)
                {
                    runs.Add(await RunOnceAsync(db, root, $"rep{rep}", P01(a) with { Name = workload, Proxy = true }, async c =>
                    {
                        await c.WaitAsync(TimeSpan.FromMinutes(a.Double("warmup-min", 2))).ConfigureAwait(false);
                        await c.ResetAsync().ConfigureAwait(false);
                        c.Mark("measureStart");
                        c.Mark("faultOn");
                        c.Proxy!.SetLatency(latency, LatencyDirection.Both);
                        await c.WaitAsync(phase).ConfigureAwait(false);
                        c.Proxy!.SetLatency(TimeSpan.Zero, LatencyDirection.Both);
                        c.Mark("faultOff");
                        await c.WaitAsync(phase).ConfigureAwait(false);
                        c.Mark("measureEnd");
                    }).ConfigureAwait(false));
                }

                break;
            }

            case "P-08c":
                for (var rep = 1; rep <= reps; rep++)
                {
                    runs.Add(await RunOnceAsync(db, root, $"rep{rep}", P01(a) with { Name = workload }, async c =>
                    {
                        await c.WaitAsync(TimeSpan.FromMinutes(a.Double("warmup-min", 2))).ConfigureAwait(false);
                        await c.ResetAsync().ConfigureAwait(false);
                        c.Mark("measureStart");
                        c.Mark("outage");
                        await c.Database.RestartAsync(RestartMode.Kill, TimeSpan.FromSeconds(a.Double("downtime-s", 60))).ConfigureAwait(false);
                        c.Mark("restored");
                        await c.WaitAsync(TimeSpan.FromMinutes(a.Double("observe-min", 10))).ConfigureAwait(false);
                        c.Mark("measureEnd");
                    }).ConfigureAwait(false));
                }

                break;
            case "G1-M1":
                runs.Add(await RunOnceAsync(db, root, "rep1", P01(a) with { Name = workload, Rate = 0 }, async c =>
                {
                    await c.WaitAsync(TimeSpan.FromMinutes(1)).ConfigureAwait(false);
                    await c.ResetAsync().ConfigureAwait(false);
                    c.Mark("measureStart");
                    await c.WaitAsync(TimeSpan.FromMinutes(a.Double("measure-min", 10))).ConfigureAwait(false);
                    c.Mark("measureEnd");
                }).ConfigureAwait(false));
                break;
            case "G1-M2":
                runs.Add(await RunOnceAsync(db, root, "rep1",
                    P01(a) with { Name = workload, HandlerMs = 120_000, Rate = 0, Count = a.Int("count", 176), Drain = false }, async c =>
                    {
                        await WaitForAsync(c, s => s.State("running") >= 16, TimeSpan.FromMinutes(3), "16 running").ConfigureAwait(false);
                        await c.ResetAsync().ConfigureAwait(false);
                        c.Mark("measureStart");
                        await c.WaitAsync(measure).ConfigureAwait(false);
                        c.Mark("measureEnd");
                    }).ConfigureAwait(false));
                break;
            case "G1-M3":
                runs.Add(await RunOnceAsync(db, root, "rep1",
                    P01(a) with { Name = workload, Workers = 2, Concurrency = 8, HandlerMs = 0, TypeCeiling = 4, Rate = a.Double("rate", 100) }, async c =>
                    {
                        await c.WaitAsync(TimeSpan.FromMinutes(a.Double("warmup-min", 2))).ConfigureAwait(false);
                        await c.ResetAsync().ConfigureAwait(false);
                        c.Mark("measureStart");
                        await c.WaitAsync(measure).ConfigureAwait(false);
                        c.Mark("measureEnd");
                    }).ConfigureAwait(false));
                break;
            default:
                Console.Error.WriteLine($"unknown workload {workload}");
                return 2;
        }

        result["finishedAt"] = DateTimeOffset.UtcNow.ToString("O");
        result["loadAverageAtEnd"] = EnvironmentInfo.Read("/proc/loadavg");
        var violations = runs.Sum(r => r!["correctness"]!["violationCount"]!.GetValue<int>());
        result["correctnessViolations"] = violations;
        await File.WriteAllTextAsync(Path.Combine(root, "result.json"), result.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine($"{workload}: {runs.Count} run(s), {violations} correctness violation(s); results in {root}");
        return violations == 0 ? 0 : 1;
    }

    static WorkloadSpec P01(Args a) => new("P-01", Workers: 1, Concurrency: a.Int("concurrency", 16), HandlerMs: a.Int("handler-ms", 100),
        Rate: a.Double("rate", 100), Count: null, TypeCeiling: null, Divisor: a.OptInt("divisor"), Proxy: false, Drain: true);

    static string Conn(TestDatabase db, string database, string applicationName) =>
        new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = applicationName, Pooling = false }.ConnectionString;

    static async Task<JsonObject> RunOnceAsync(TestDatabase db, string root, string label, WorkloadSpec spec, Func<RunContext, Task> script)
    {
        var dir = Path.Combine(root, label);
        Directory.CreateDirectory(dir);
        var database = ("wl_" + spec.Name + "_" + label).Replace('-', '_').ToLowerInvariant();
        Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} {spec.Name} {label}: database {database}");
        await Exec(Conn(db, "postgres", "wl:admin"), $"DROP DATABASE IF EXISTS {database} WITH (FORCE)").ConfigureAwait(false);
        await Exec(Conn(db, "postgres", "wl:admin"), $"CREATE DATABASE {database}").ConfigureAwait(false);
        await RunwickPostgresMigrations.MigrateAsync(Conn(db, database, "wl:migrate")).ConfigureAwait(false);

        var sampleConn = Conn(db, database, "wl:sampler");
        await using var proxy = spec.Proxy ? new PgFaultProxy(db.Host, db.Port) : null;
        var workers = new List<ChildProcess>();
        ChildProcess? producer = null;
        var sampler = new DbSampler(sampleConn, database, Path.Combine(dir, "db-samples.jsonl"));
        sampler.Start();
        var startedAt = DateTimeOffset.UtcNow;
        RunContext? context = null;
        string? failure = null;
        try
        {
            for (var i = 1; i <= spec.Workers; i++)
            {
                var cs = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = $"wl-worker{i}" }.ConnectionString;
                if (proxy is not null)
                {
                    cs = proxy.Rewrite(cs);
                }

                List<string> args = ["worker", "--conn", cs, "--app", "wl", "--concurrency", spec.Concurrency.ToString(CultureInfo.InvariantCulture),
                    "--handler-ms", spec.HandlerMs.ToString(CultureInfo.InvariantCulture), "--out", Path.Combine(dir, $"worker{i}")];
                if (spec.TypeCeiling is { } ceiling)
                {
                    args.AddRange(["--type-ceiling", ceiling.ToString(CultureInfo.InvariantCulture)]);
                }

                if (spec.Divisor is { } divisor)
                {
                    args.AddRange(["--divisor", divisor.ToString(CultureInfo.InvariantCulture)]);
                }

                var w = ChildProcess.Start($"worker{i}", Path.Combine(dir, $"worker{i}.log"), [.. args]);
                workers.Add(w);
                w.WaitFor("ready", TimeSpan.FromMinutes(2));
            }

            if (spec.Rate > 0 || spec.Count is not null)
            {
                var cs = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = "wl-producer" }.ConnectionString;
                List<string> args = ["produce", "--conn", cs, "--app", "wl", "--rate", spec.Rate.ToString(CultureInfo.InvariantCulture),
                    "--out", Path.Combine(dir, "producer")];
                if (spec.Count is { } count)
                {
                    args.AddRange(["--count", count.ToString(CultureInfo.InvariantCulture)]);
                }

                if (spec.TypeCeiling is { } ceiling)
                {
                    args.AddRange(["--type-ceiling", ceiling.ToString(CultureInfo.InvariantCulture)]);
                }

                producer = ChildProcess.Start("producer", Path.Combine(dir, "producer.log"), [.. args]);
                producer.WaitFor("ready", TimeSpan.FromMinutes(2));
            }

            context = new RunContext(dir, sampler, workers, producer, proxy, db);
            await script(context).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is InvalidOperationException or TimeoutException or NpgsqlException)
        {
            failure = ex.ToString();
            Console.WriteLine($"  run failed: {ex.Message}");
        }

        context ??= new RunContext(dir, sampler, workers, producer, proxy, db);
        if (producer is not null)
        {
            context.Mark("producerStop");
            await producer.StopAsync(TimeSpan.FromMinutes(5)).ConfigureAwait(false);
        }

        if (spec.Drain && failure is null)
        {
            try
            {
                await WaitForAsync(context, s => s.Open == 0, TimeSpan.FromMinutes(_drainMinutes), "drained").ConfigureAwait(false);
                context.Mark("drained");
            }
            catch (TimeoutException ex)
            {
                failure = ex.Message;
            }
        }

        context.Mark("workerStop");
        var stopExit = new JsonArray();
        foreach (var w in workers)
        {
            stopExit.Add(await w.StopAsync(TimeSpan.FromMinutes(2)).ConfigureAwait(false));
        }

        context.Mark("workersStopped");
        await Task.Delay(TimeSpan.FromSeconds(6)).ConfigureAwait(false);
        await sampler.DisposeAsync().ConfigureAwait(false);
        foreach (var w in workers)
        {
            await w.DisposeAsync().ConfigureAwait(false);
        }

        if (producer is not null)
        {
            await producer.DisposeAsync().ConfigureAwait(false);
        }

        var analysis = await Analysis.AnalyseAsync(context, spec, sampleConn, startedAt).ConfigureAwait(false);
        analysis["label"] = label;
        analysis["database"] = database;
        analysis["failure"] = failure;
        analysis["workerExitCodes"] = stopExit;
        analysis["extra"] = context.Extra;
        await File.WriteAllTextAsync(Path.Combine(dir, "result.json"), analysis.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine($"  {label}: {Analysis.OneLine(analysis)}");
        if (failure is null)
        {
            await Exec(Conn(db, "postgres", "wl:admin"), $"DROP DATABASE IF EXISTS {database} WITH (FORCE)").ConfigureAwait(false);
        }

        return analysis;
    }

    /// <summary>
    /// P-02 ramp: +step every 3 min (1 min settle + 2 min measured) until saturation (07 §14.3): confirmed finishes below 98 %
    /// of the offered arrivals measured in the same window, or a backlog growing monotonically for the last 60 s.
    /// </summary>
    static async Task RampAsync(RunContext c, double start, double step, Args a)
    {
        var rate = start;
        var steps = new JsonArray();
        c.Extra["steps"] = steps;
        double sustainable = 0;
        var (settle, measured) = (TimeSpan.FromMinutes(a.Double("step-settle-min", 1)), TimeSpan.FromMinutes(a.Double("step-measure-min", 2)));
        for (var i = 0; i < a.Int("max-steps", 40); i++)
        {
            await c.Producer!.SendAsync("rate " + rate.ToString(CultureInfo.InvariantCulture)).ConfigureAwait(false);
            c.Mark($"step{i}:{rate}");
            await c.WaitAsync(settle).ConfigureAwait(false);
            var from = c.Sampler.Samples[^1];
            await c.WaitAsync(measured).ConfigureAwait(false);
            var window = c.Sampler.Samples.Where(s => s.At >= from.At).ToList();
            var to = window[^1];
            var seconds = (to.At - from.At).TotalSeconds;
            var finishes = (Analysis.Terminal(to) - Analysis.Terminal(from)) / seconds;
            var offered = OfferedPerSec(Path.Combine(c.Dir, "producer.samples.jsonl"), from.At, to.At) ?? rate;
            var accepted = (to.States.Values.Sum() - from.States.Values.Sum()) / seconds;
            var last60 = window.Where(s => s.At >= to.At.AddSeconds(-60)).Select(s => s.Backlog).ToList();
            var growing = last60.Count >= 2 && last60.Zip(last60.Skip(1)).All(p => p.Second > p.First);
            var saturated = finishes < 0.98 * offered || growing;
            steps.Add(new JsonObject
            {
                ["nominalRate"] = rate, ["offeredPerSec"] = Math.Round(offered, 3), ["acceptedPerSec"] = Math.Round(accepted, 3),
                ["confirmedFinishesPerSec"] = Math.Round(finishes, 3), ["backlogStart"] = from.Backlog, ["backlogEnd"] = to.Backlog,
                ["backlogGrowing60s"] = growing, ["saturated"] = saturated,
            });
            Console.WriteLine($"    step {rate}/s: offered {offered:F2}/s, accepted {accepted:F2}/s, {finishes:F2} finishes/s, backlog {from.Backlog}→{to.Backlog}{(saturated ? " SATURATED" : "")}");
            if (saturated)
            {
                break;
            }

            sustainable = rate;
            rate += step;
        }

        c.Extra["sustainableRate"] = sustainable;
        c.Mark("rampEnd");
    }

    static double? OfferedPerSec(string path, DateTimeOffset from, DateTimeOffset to)
    {
        var lines = File.ReadAllLines(path).Where(l => l.EndsWith('}')).Select(l => JsonNode.Parse(l)!)
            .Select(n => (At: DateTimeOffset.Parse(n["t"]!.GetValue<string>(), CultureInfo.InvariantCulture), Offered: n["offered"]!.GetValue<long>()))
            .Where(x => x.At >= from && x.At <= to).ToList();
        return lines.Count < 2 ? null : (lines[^1].Offered - lines[0].Offered) / (lines[^1].At - lines[0].At).TotalSeconds;
    }

    static async Task WaitForAsync(RunContext c, Func<DbSample, bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var samples = c.Sampler.Samples;
            if (samples.Count > 0 && samples[^1].At > DateTimeOffset.UtcNow.AddSeconds(-10) && condition(samples[^1]))
            {
                return;
            }

            await Task.Delay(1000).ConfigureAwait(false);
        }

        throw new TimeoutException($"not {what} within {timeout}");
    }

    static async Task Exec(string cs, string sql)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}
