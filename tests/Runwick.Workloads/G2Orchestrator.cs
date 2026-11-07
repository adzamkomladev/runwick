using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Hosting;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;

namespace Runwick.Workloads;

/// <summary>One G-2 repetition: its children, policies and the capabilities to activate before they start.</summary>
internal sealed record G2Spec(
    string Name,
    string Label,
    IReadOnlyList<string> Common,
    IReadOnlyList<string[]> Workers,
    string[]? Producer,
    int Concurrency,
    int HandlerMs,
    bool Drain,
    IReadOnlyList<string> Capabilities,
    bool OutboxSource = false);

/// <summary>Live handles of one G-2 repetition (wraps the G-1 <see cref="RunContext"/> used by <see cref="Analysis"/>).</summary>
internal sealed class G2Context(RunContext inner, G2Spec spec, TestDatabase db, string database, string? sourceDatabase)
{
    readonly Dictionary<string, ChildProcess> _children = new(StringComparer.Ordinal);

    public RunContext Inner => inner;

    public G2Spec Spec => spec;

    public string Dir => inner.Dir;

    public JsonObject Extra => inner.Extra;

    /// <summary>Workload-specific correctness violations (duplicate occurrence identities, duplicate delivery mappings, …).</summary>
    public List<string> Violations { get; } = [];

    /// <summary>A run failure the script or analysis found (for example an undrained source); not a correctness violation.</summary>
    public string? Failure { get; set; }

    /// <summary>Workload-specific values that must stay in memory between the script and <c>after</c>.</summary>
    public Dictionary<string, object> State { get; } = new(StringComparer.Ordinal);

    public string StoreConnection(string applicationName) =>
        new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = applicationName, Pooling = false }.ConnectionString;

    public string SourceConnection(string applicationName) =>
        new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = sourceDatabase, ApplicationName = applicationName, Pooling = false }.ConnectionString;

    public IReadOnlyDictionary<string, ChildProcess> Children => _children;

    public void Mark(string name) => inner.Mark(name);

    /// <summary>Starts a role child (<c>scheduler</c>, <c>relay</c>, <c>stage</c>) with the run's common arguments.</summary>
    public ChildProcess Start(string name, string mode, IEnumerable<string> roleArgs, TimeSpan readyTimeout, bool source = false)
    {
        List<string> args = [mode, "--conn", StoreConnectionPooled("wl-" + name), "--app", G2Jobs.App, "--g2", .. spec.Common, .. roleArgs,
            "--out", Path.Combine(Dir, name)];
        if (source)
        {
            args.AddRange(["--source", new NpgsqlConnectionStringBuilder(SourceConnection("wl-" + name)) { Pooling = true }.ConnectionString]);
        }

        var child = ChildProcess.Start(name, Path.Combine(Dir, name + ".log"), [.. args]);
        _children[name] = child;
        child.WaitFor("ready", readyTimeout);
        return child;
    }

    public async Task<int> StopAsync(string name, TimeSpan timeout)
    {
        var child = _children[name];
        _children.Remove(name);
        var code = await child.StopAsync(timeout).ConfigureAwait(false);
        await child.DisposeAsync().ConfigureAwait(false);
        return code;
    }

    string StoreConnectionPooled(string applicationName) =>
        new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = applicationName }.ConnectionString;

    /// <summary>Waits <paramref name="span"/>; fails the run if any child process exits meanwhile.</summary>
    public async Task WaitAsync(TimeSpan span, Func<Task>? every5s = null)
    {
        var end = DateTime.UtcNow + span;
        while (DateTime.UtcNow < end)
        {
            Check();
            if (every5s is not null)
            {
                await every5s().ConfigureAwait(false);
            }

            await Task.Delay(TimeSpan.FromSeconds(Math.Min(5, Math.Max(0.05, (end - DateTime.UtcNow).TotalSeconds)))).ConfigureAwait(false);
        }
    }

    /// <summary>Polls <paramref name="condition"/> every 2 s until it holds; false on timeout.</summary>
    public async Task<bool> UntilAsync(Func<Task<bool>> condition, TimeSpan timeout, Func<Task>? every = null)
    {
        var end = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < end)
        {
            Check();
            if (every is not null)
            {
                await every().ConfigureAwait(false);
            }

            if (await condition().ConfigureAwait(false))
            {
                return true;
            }

            await Task.Delay(2000).ConfigureAwait(false);
        }

        return false;
    }

    void Check()
    {
        var producerExited = inner.Producer?.HasExited == true && !inner.Marks.ContainsKey("producerStop");
        if (inner.Workers.Any(w => w.HasExited) || producerExited || _children.Values.Any(c => c.HasExited))
        {
            throw new InvalidOperationException("a child process exited during the workload");
        }
    }

    public async Task DisposeChildrenAsync()
    {
        foreach (var name in _children.Keys.ToList())
        {
            await StopAsync(name, TimeSpan.FromMinutes(2)).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// <c>run P-04|P-05|P-06r|P-06s|P-09</c> (15 §11): the G-2 measurements. Same provisioning, sampling, analysis and
/// result layout as the G-1 orchestrator (one database per repetition, <c>result.json</c> with <c>runs[]</c>,
/// <c>correctnessViolations</c>, <c>env.json</c>); the workloads themselves are in <see cref="G2Workloads"/>.
/// </summary>
internal static class G2Orchestrator
{
    public static readonly string[] Names = ["P-04", "P-05", "P-06r", "P-06s", "P-09"];

    static readonly ControlActor Actor = new("svc:workloads", "G-2 workload runner", "runwick", "G-2 measurement");

    public static async Task<int> RunAsync(Args a)
    {
        var workload = a.Positional[0];
        var root = a.OptStr("out") ?? Path.Combine("artifacts", "perf", $"{DateTime.UtcNow:yyyyMMdd-HHmmss}-{workload}");
        Directory.CreateDirectory(root);
        await using var db = await TestDatabase.StartAsync().ConfigureAwait(false);
        var admin = Conn(db, "postgres", "wl:admin");
        var env = await EnvironmentInfo.CaptureAsync(db, admin, a).ConfigureAwait(false);
        env["connectionBudgets"] = "control 8, work 8, listener 0/1 per worker; outbox relay 2 source connections";
        await File.WriteAllTextAsync(Path.Combine(root, "env.json"), env.ToJsonString(Json.Indented)).ConfigureAwait(false);
        var result = new JsonObject { ["workload"] = workload, ["startedAt"] = DateTimeOffset.UtcNow.ToString("O") };
        var runs = new JsonArray();
        result["runs"] = runs;
        var summary = new JsonObject();
        result["summary"] = summary;

        switch (workload)
        {
            case "P-04":
                await G2Workloads.P04Async(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            case "P-05":
                await G2Workloads.P05Async(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            case "P-06r":
                await G2Workloads.P06rAsync(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            case "P-06s":
                await G2Workloads.P06sAsync(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            case "P-09":
                await G2Workloads.P09Async(db, root, a, runs, summary).ConfigureAwait(false);
                break;
            default:
                Console.Error.WriteLine($"unknown workload {workload}");
                return 2;
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

    public static string Conn(TestDatabase db, string database, string applicationName) =>
        new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = applicationName, Pooling = false }.ConnectionString;

    /// <summary>
    /// One repetition: database, migration, capability activation, workers and producer, the phase script, staged stop,
    /// optional drain, the G-1 analysis plus the G-2 additions (<paramref name="after"/>, lease losses as violations).
    /// </summary>
    public static async Task<JsonObject> RunOnceAsync(TestDatabase db, string root, G2Spec spec, Func<G2Context, Task> script,
        Func<G2Context, Task>? after = null, double drainMinutes = 60)
    {
        var dir = Path.Combine(root, spec.Label);
        Directory.CreateDirectory(dir);
        var database = ("wl_" + spec.Name + "_" + spec.Label).Replace('-', '_').ToLowerInvariant();
        var sourceDatabase = spec.OutboxSource ? database + "_src" : null;
        Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} {spec.Name} {spec.Label}: database {database}");
        foreach (var d in new[] { database, sourceDatabase }.OfType<string>())
        {
            await ExecAsync(Conn(db, "postgres", "wl:admin"), $"DROP DATABASE IF EXISTS {d} WITH (FORCE)").ConfigureAwait(false);
            await ExecAsync(Conn(db, "postgres", "wl:admin"), $"CREATE DATABASE {d}").ConfigureAwait(false);
        }

        await RunwickPostgresMigrations.MigrateAsync(Conn(db, database, "wl:migrate")).ConfigureAwait(false);
        if (sourceDatabase is not null)
        {
            await Runwick.Outbox.Postgres.PostgresOutboxSchema.InstallAsync(Conn(db, sourceDatabase, "wl:migrate")).ConfigureAwait(false);
            await ExecAsync(Conn(db, sourceDatabase, "wl:migrate"), "CREATE TABLE business_rows (id text PRIMARY KEY)").ConfigureAwait(false);
        }

        await PrepareStoreAsync(Conn(db, database, "wl:admin"), spec).ConfigureAwait(false);

        var sampleConn = Conn(db, database, "wl:sampler");
        var workers = new List<ChildProcess>();
        ChildProcess? producer = null;
        var sampler = new DbSampler(sampleConn, database, Path.Combine(dir, "db-samples.jsonl"));
        sampler.Start();
        var startedAt = DateTimeOffset.UtcNow;
        G2Context? context = null;
        string? failure = null;
        string Cs(string app) => new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = app }.ConnectionString;
        try
        {
            for (var i = 1; i <= spec.Workers.Count; i++)
            {
                List<string> args = ["worker", "--conn", Cs($"wl-worker{i}"), "--app", G2Jobs.App, "--g2", .. spec.Common, .. spec.Workers[i - 1],
                    "--out", Path.Combine(dir, $"worker{i}")];
                var w = ChildProcess.Start($"worker{i}", Path.Combine(dir, $"worker{i}.log"), [.. args]);
                workers.Add(w);
                w.WaitFor("ready", TimeSpan.FromMinutes(3));
            }

            if (spec.Producer is not null)
            {
                List<string> args = ["produce2", "--conn", Cs("wl-producer"), "--app", G2Jobs.App, "--g2", .. spec.Common, .. spec.Producer,
                    "--out", Path.Combine(dir, "producer")];
                producer = ChildProcess.Start("producer", Path.Combine(dir, "producer.log"), [.. args]);
                producer.WaitFor("ready", TimeSpan.FromMinutes(3));
            }

            context = new G2Context(new RunContext(dir, sampler, workers, producer, null, db), spec, db, database, sourceDatabase);
            await script(context).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is InvalidOperationException or TimeoutException or NpgsqlException or IOException)
        {
            failure = ex.ToString();
            Console.WriteLine($"  run failed: {ex.Message}");
        }

        context ??= new G2Context(new RunContext(dir, sampler, workers, producer, null, db), spec, db, database, sourceDatabase);
        if (producer is not null)
        {
            context.Mark("producerStop");
            await producer.StopAsync(TimeSpan.FromMinutes(5)).ConfigureAwait(false);
        }

        if (spec.Drain && failure is null)
        {
            try
            {
                var drained = await context.UntilAsync(() =>
                {
                    var s = sampler.Samples;
                    return Task.FromResult(s.Count > 0 && s[^1].At > DateTimeOffset.UtcNow.AddSeconds(-10) && s[^1].Open == 0);
                }, TimeSpan.FromMinutes(drainMinutes)).ConfigureAwait(false);
                if (drained)
                {
                    context.Mark("drained");
                }
                else
                {
                    failure = $"not drained within {drainMinutes} min";
                }
            }
            catch (InvalidOperationException ex)
            {
                failure = "drain: " + ex.Message;
            }
        }

        await context.DisposeChildrenAsync().ConfigureAwait(false);
        context.Mark("workerStop");
        var stopExit = new JsonArray();
        foreach (var w in workers)
        {
            stopExit.Add(await w.StopAsync(TimeSpan.FromMinutes(2)).ConfigureAwait(false));
        }

        context.Mark("workersStopped");
        if (failure is null && stopExit.Any(code => code?.GetValue<int>() != 0))
        {
            failure = "worker exit codes " + string.Join(", ", stopExit);
        }

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

        var wspec = new WorkloadSpec(spec.Name, spec.Workers.Count, spec.Concurrency, spec.HandlerMs, 0, null, null, null, false, spec.Drain);
        var analysis = await Analysis.AnalyseAsync(context.Inner, wspec, sampleConn, startedAt).ConfigureAwait(false);
        if (after is not null)
        {
            try
            {
                await after(context).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException or IOException or FormatException or KeyNotFoundException)
            {
                failure ??= "analysis failed: " + ex;
            }
        }

        failure ??= context.Failure;

        // Lease losses during healthy load are correctness violations in every G-2 workload (none injects lease faults).
        var lost = (analysis["workers"] as JsonArray ?? []).Sum(w => (w?["counters"] as JsonObject)?
            .Where(kv => kv.Key.StartsWith("event:lost:", StringComparison.Ordinal)).Sum(kv => kv.Value!.GetValue<long>()) ?? 0);
        if (lost > 0)
        {
            context.Violations.Add($"{lost} worker lease loss(es) during healthy load");
        }

        var correctness = (JsonObject)analysis["correctness"]!;
        var all = (JsonArray)correctness["violations"]!;
        foreach (var v in context.Violations)
        {
            all.Add(v);
        }

        correctness["violationCount"] = all.Count;
        correctness["leaseLosses"] = lost;
        analysis["label"] = spec.Label;
        analysis["database"] = database;
        analysis["failure"] = failure;
        analysis["workerExitCodes"] = stopExit;
        analysis["extra"] = context.Extra;
        await File.WriteAllTextAsync(Path.Combine(dir, "result.json"), analysis.ToJsonString(Json.Indented)).ConfigureAwait(false);
        Console.WriteLine($"  {spec.Label}: {Analysis.OneLine(analysis)}");
        if (failure is null)
        {
            foreach (var d in new[] { database, sourceDatabase }.OfType<string>())
            {
                await ExecAsync(Conn(db, "postgres", "wl:admin"), $"DROP DATABASE IF EXISTS {d} WITH (FORCE)").ConfigureAwait(false);
            }
        }

        return analysis;
    }

    /// <summary>Registers the application and activates the run's capabilities (rate_admission, scheduler) through the store protocol.</summary>
    internal static async Task PrepareStoreAsync(string cs, G2Spec spec)
    {
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        var args = new Args(spec.Common);
        sc.AddRunwick(r =>
        {
            r.Application(G2Jobs.App).UsePostgres(cs);
            G2Jobs.Register(r, args);
        });
        await using var provider = sc.BuildServiceProvider();
        await provider.GetRequiredService<RunwickRuntime>().EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
        if (spec.Capabilities.Count == 0)
        {
            return;
        }

        var store = provider.GetRequiredService<IRateControlStore>();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromMinutes(2)).ToUnixTimeMilliseconds());
        var reply = await store.ActivateCapabilitiesAsync(new StoreCommand<ActivateCapabilitiesRequest>(G2Jobs.App, Guid.CreateVersion7(), notAfter,
            new ActivateCapabilitiesRequest(spec.Capabilities.ToDictionary(c => c, _ => 1, StringComparer.Ordinal), Actor)), CancellationToken.None).ConfigureAwait(false);
        if (reply.Status != StoreStatus.Applied)
        {
            throw new InvalidOperationException($"capability activation: {reply.Status} {reply.Error?.Code}");
        }
    }

    public static async Task ExecAsync(string cs, string sql)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <summary>Runs <paramref name="sql"/> and returns every row as an array of values (DBNull → null).</summary>
    public static async Task<List<object?[]>> RowsAsync(string cs, string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = 600 };
        foreach (var (n, v) in parameters)
        {
            cmd.Parameters.AddWithValue(n, v);
        }

        var rows = new List<object?[]>();
        await using var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await r.ReadAsync().ConfigureAwait(false))
        {
            var row = new object?[r.FieldCount];
            for (var i = 0; i < r.FieldCount; i++)
            {
                row[i] = r.IsDBNull(i) ? null : r.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    public static async Task<double> ScalarAsync(string cs, string sql, params (string Name, object Value)[] parameters)
    {
        var rows = await RowsAsync(cs, sql, parameters).ConfigureAwait(false);
        return rows.Count == 0 || rows[0][0] is null ? 0 : Convert.ToDouble(rows[0][0], CultureInfo.InvariantCulture);
    }

    public static string Num(double v) => v.ToString(CultureInfo.InvariantCulture);
}
