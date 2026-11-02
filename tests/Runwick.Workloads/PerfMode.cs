using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;

namespace Runwick.Workloads;

/// <summary>
/// <c>perf</c> (performance plan PF-01, <c>eng/run-perf.sh</c>): one dedicated Testcontainers server reached at the
/// container IP (no <c>docker-proxy</c>), then
/// <list type="number">
/// <item>the store-call floor with client CPU and allocation per job (and an allocation profile);</item>
/// <item>the idle cost: statements/s and CPU of an idle worker, an idle scheduler and an idle outbox relay, measured one
/// at a time from <c>pg_stat_statements</c> on their own databases;</item>
/// <item>a shortened P-01 (one repetition) with worker CPU and allocation per finished job;</item>
/// </list>
/// and a <c>summary.md</c>. Usage: <c>perf --out dir [--n 100] [--warmup 10] [--cost-jobs 300] [--idle-s 60]
/// [--p01-warmup-min 1] [--p01-measure-min 3] [--p01-drain-min 2] (this host cannot drain 100/s; a "not drained" failure only marks that) [--skip-p01] [--skip-idle]</c>.
/// </summary>
internal static class PerfMode
{
    static readonly CultureInfo Ic = CultureInfo.InvariantCulture;

    public static async Task<int> RunAsync(Args a)
    {
        var root = Path.GetFullPath(a.OptStr("out") ?? Path.Combine("artifacts", "perf", DateTime.UtcNow.ToString("yyyy-MM-dd", Ic)));
        Directory.CreateDirectory(root);
        var loadStart = EnvironmentInfo.Read("/proc/loadavg");
        await using var db = await TestDatabase.StartDirectAsync().ConfigureAwait(false);
        Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} server at {db.Host}:{db.Port} (direct: {db.Direct})");
        var admin = Conn(db, "postgres", "perf:admin");
        var env = await EnvironmentInfo.CaptureAsync(db, admin, a).ConfigureAwait(false);
        env["path"] = db.Direct ? "container IP, no docker-proxy" : "mapped port (docker-proxy)";
        await File.WriteAllTextAsync(Path.Combine(root, "env.json"), env.ToJsonString(Json.Indented)).ConfigureAwait(false);

        // 1. Floor.
        Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} floor");
        var floorJson = Path.Combine(root, "floor.json");
        await FloorMode.RunAsync(new Args(["--conn", db.ConnectionString, "--db", "rw_perf_floor", "--n", I(a.Int("n", 100)),
            "--warmup", I(a.Int("warmup", 10)), "--cost-jobs", I(a.Int("cost-jobs", 300)), "--alloc-profile",
            "--out", Path.Combine(root, "floor.md"), "--json", floorJson])).ConfigureAwait(false);
        var floor = JsonNode.Parse(await File.ReadAllTextAsync(floorJson).ConfigureAwait(false))!.AsObject();

        // 2. Idle cost.
        JsonArray idle = [];
        if (!a.Has("skip-idle"))
        {
            idle = await IdleAsync(db, root, TimeSpan.FromSeconds(a.Double("idle-s", 60))).ConfigureAwait(false);
            await File.WriteAllTextAsync(Path.Combine(root, "idle.json"), idle.ToJsonString(Json.Indented)).ConfigureAwait(false);
        }

        // 3. Shortened P-01.
        JsonObject? p01 = null;
        if (!a.Has("skip-p01"))
        {
            Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} P-01 (shortened)");
            var dir = Path.Combine(root, "p01");
            await Orchestrator.RunAsync(new Args(["P-01", "--out", dir, "--reps", "1",
                "--warmup-min", D(a.Double("p01-warmup-min", 1)), "--measure-min", D(a.Double("p01-measure-min", 3)),
                "--drain-min", D(a.Double("p01-drain-min", 2))]), db).ConfigureAwait(false);
            p01 = P01Cost(Path.Combine(dir, "rep1"));
            await File.WriteAllTextAsync(Path.Combine(root, "p01-cost.json"), p01.ToJsonString(Json.Indented)).ConfigureAwait(false);
        }

        var summary = Summary(env, db, loadStart, EnvironmentInfo.Read("/proc/loadavg"), floor, idle, p01);
        await File.WriteAllTextAsync(Path.Combine(root, "summary.md"), summary).ConfigureAwait(false);
        Console.WriteLine(summary);
        Console.WriteLine($"summary: {Path.Combine(root, "summary.md")}");
        return 0;
    }

    // ------------------------------------------------------------------------------------------------ idle cost

    static async Task<JsonArray> IdleAsync(TestDatabase db, string root, TimeSpan window)
    {
        var dir = Path.Combine(root, "idle");
        Directory.CreateDirectory(dir);
        var results = new JsonArray();
        foreach (var role in new[] { "worker", "scheduler", "relay" })
        {
            var database = "rw_perf_idle_" + role;
            var source = role == "relay" ? database + "_src" : null;
            foreach (var d in new[] { database, source }.OfType<string>())
            {
                await G2Orchestrator.ExecAsync(Conn(db, "postgres", "perf:admin"), $"DROP DATABASE IF EXISTS {d} WITH (FORCE)").ConfigureAwait(false);
                await G2Orchestrator.ExecAsync(Conn(db, "postgres", "perf:admin"), $"CREATE DATABASE {d}").ConfigureAwait(false);
            }

            await RunwickPostgresMigrations.MigrateAsync(Conn(db, database, "perf:migrate")).ConfigureAwait(false);
            if (source is not null)
            {
                await Runwick.Outbox.Postgres.PostgresOutboxSchema.InstallAsync(Conn(db, source, "perf:migrate")).ConfigureAwait(false);
            }

            if (role != "worker")
            {
                await G2Orchestrator.PrepareStoreAsync(Conn(db, database, "perf:admin"),
                    new G2Spec("idle", role, [], [], null, 0, 0, Drain: false, Capabilities: role == "scheduler" ? ["scheduler"] : [])).ConfigureAwait(false);
            }

            string Pooled(string d, string app) => new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = d, ApplicationName = app }.ConnectionString;
            List<string> args = role switch
            {
                "worker" => ["worker", "--conn", Pooled(database, "perf-worker"), "--app", "wl", "--concurrency", "16", "--handler-ms", "100",
                    "--out", Path.Combine(dir, "worker")],
                "scheduler" => ["scheduler", "--conn", Pooled(database, "perf-scheduler"), "--app", G2Jobs.App, "--g2", "--schedules", "0",
                    "--out", Path.Combine(dir, "scheduler")],
                _ => ["relay", "--conn", Pooled(database, "perf-relay"), "--app", G2Jobs.App, "--g2", "--source", Pooled(source!, "perf-relay"),
                    "--out", Path.Combine(dir, "relay")],
            };
            Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss} idle {role}: {window.TotalSeconds:F0} s");
            var child = ChildProcess.Start(role, Path.Combine(dir, role + ".log"), [.. args]);
            try
            {
                child.WaitFor("ready", TimeSpan.FromMinutes(3));
                await Task.Delay(TimeSpan.FromSeconds(15)).ConfigureAwait(false); // startup work settles
                var dbs = new[] { database, source }.OfType<string>().ToArray();
                var before = await StatementsAsync(admin: Conn(db, "postgres", "perf:stats"), dbs).ConfigureAwait(false);
                var cpu0 = ProcCpuMs(child.Id);
                var t0 = DateTimeOffset.UtcNow;
                await Task.Delay(window).ConfigureAwait(false);
                var cpu1 = ProcCpuMs(child.Id);
                var seconds = (DateTimeOffset.UtcNow - t0).TotalSeconds;
                var after = await StatementsAsync(Conn(db, "postgres", "perf:stats"), dbs).ConfigureAwait(false);
                var deltas = after.Select(kv => (Query: kv.Key, Calls: kv.Value - before.GetValueOrDefault(kv.Key)))
                    .Where(x => x.Calls > 0).OrderByDescending(x => x.Calls).ToList();
                var top = new JsonArray();
                foreach (var (q, calls) in deltas.Take(12))
                {
                    top.Add(new JsonObject { ["perSec"] = Math.Round(calls / seconds, 3), ["query"] = Head(q, 110) });
                }

                results.Add(new JsonObject
                {
                    ["role"] = role,
                    ["seconds"] = Math.Round(seconds, 1),
                    ["statementsPerSec"] = Math.Round(deltas.Sum(x => x.Calls) / seconds, 2),
                    ["cpuMsPerSec"] = cpu0 is null || cpu1 is null ? null : Math.Round((cpu1.Value - cpu0.Value) / seconds, 2),
                    ["top"] = top,
                });
            }
            finally
            {
                await child.StopAsync(TimeSpan.FromMinutes(1)).ConfigureAwait(false);
                await child.DisposeAsync().ConfigureAwait(false);
            }
        }

        return results;
    }

    /// <summary>Calls per normalized statement text on the given databases (excluding this probe's own query).</summary>
    static async Task<Dictionary<string, long>> StatementsAsync(string admin, string[] databases)
    {
        await using var conn = new NpgsqlConnection(admin);
        await conn.OpenAsync().ConfigureAwait(false);
        await using (var ext = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", conn))
        {
            await ext.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await using var cmd = new NpgsqlCommand("""
            SELECT s.query, sum(s.calls)::bigint
            FROM pg_stat_statements AS s JOIN pg_database AS d ON d.oid = s.dbid
            WHERE d.datname = ANY (@dbs) AND s.query NOT LIKE '%pg_stat_statements%'
            GROUP BY s.query
            """, conn);
        cmd.Parameters.AddWithValue("dbs", databases);
        var result = new Dictionary<string, long>(StringComparer.Ordinal);
        await using var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await r.ReadAsync().ConfigureAwait(false))
        {
            result[r.GetString(0)] = r.GetInt64(1);
        }

        return result;
    }

    /// <summary>User + system CPU of a process, from <c>/proc/&lt;pid&gt;/stat</c> (clock ticks of 10 ms).</summary>
    static double? ProcCpuMs(int pid)
    {
        try
        {
            var stat = File.ReadAllText($"/proc/{pid}/stat");
            var fields = stat[(stat.LastIndexOf(')') + 2)..].Split(' ');
            return (long.Parse(fields[11], Ic) + long.Parse(fields[12], Ic)) * 10.0;
        }
        catch (IOException)
        {
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------------ P-01 cost

    /// <summary>Worker CPU and allocation per finished job over P-01's measured window (worker 1 s samples).</summary>
    static JsonObject P01Cost(string dir)
    {
        var result = JsonNode.Parse(File.ReadAllText(Path.Combine(dir, "result.json")))!.AsObject();
        var marks = result["marks"]!.AsObject();
        var start = DateTimeOffset.Parse(marks["measureStart"]!.GetValue<string>(), Ic);
        var end = DateTimeOffset.Parse(marks["measureEnd"]!.GetValue<string>(), Ic);
        var samples = File.ReadAllLines(Path.Combine(dir, "worker1.samples.jsonl")).Where(l => l.EndsWith('}')).Select(l => JsonNode.Parse(l)!)
            .Select(x => (At: DateTimeOffset.Parse(x["t"]!.GetValue<string>(), Ic), Cpu: x["process"]!["cpuMs"]!.GetValue<long>(),
                Alloc: x["process"]!["allocatedBytes"]!.GetValue<long>(), Inv: x["invocations"]!.GetValue<long>()))
            .Where(x => x.At >= start && x.At <= end).ToList();
        var o = new JsonObject
        {
            ["finishesPerSec"] = result["window"]?["confirmedFinishesPerSec"]?.DeepClone(),
            ["terminalJobs"] = result["window"]?["terminalJobs"]?.DeepClone(),
            ["violations"] = result["correctness"]?["violationCount"]?.DeepClone(),
            ["failure"] = result["failure"]?.DeepClone(),
        };
        if (samples.Count >= 2)
        {
            var inv = samples[^1].Inv - samples[0].Inv;
            o["invocations"] = inv;
            o["cpuMsPerJob"] = inv == 0 ? null : Math.Round((double)(samples[^1].Cpu - samples[0].Cpu) / inv, 2);
            o["allocKiBPerJob"] = inv == 0 ? null : Math.Round((samples[^1].Alloc - samples[0].Alloc) / 1024.0 / inv, 1);
        }

        return o;
    }

    // ------------------------------------------------------------------------------------------------ summary

    static string Summary(JsonObject env, TestDatabase db, string? loadStart, string? loadEnd, JsonObject floor, JsonArray idle, JsonObject? p01)
    {
        var sb = new StringBuilder();
        sb.AppendLine(Ic, $"# Performance run ({DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC)");
        sb.AppendLine();
        sb.AppendLine(Ic, $"- **Non-reference host** ({env["cpuCount"]} vCPU, {env["cpuModel"]}). Load average at start {loadStart}, at end {loadEnd}.");
        sb.AppendLine(Ic, $"- Server: dedicated Testcontainers `{env["postgresImage"]}` at {db.Host}:{db.Port}; **docker-proxy {(db.Direct ? "bypassed (container IP)" : "in the path (mapped port; the bridge IP was unreachable)")}**.");
        sb.AppendLine(Ic, $"- Commit {env["commit"]} ({env["uncommittedFiles"]} uncommitted paths).");
        sb.AppendLine();
        sb.AppendLine("## Floor (isolated calls, p50 ms)");
        sb.AppendLine();
        sb.AppendLine("| Operation | p50 ms | SQL commands |");
        sb.AppendLine("|---|---|---|");
        foreach (var (op, v) in floor["p50Ms"]!.AsObject())
        {
            sb.AppendLine(Ic, $"| {op} | {v} | {floor["commands"]?[op]} |");
        }

        sb.AppendLine();
        sb.AppendLine(Ic, $"Load average after the floor: {floor["loadAverage"]}.");
        sb.AppendLine();
        sb.AppendLine("## Client cost per job (floor client, Dispatch + Reserve + Begin + Finish, no listener)");
        sb.AppendLine();
        sb.AppendLine(Ic, $"CPU {floor["cpuMsPerJob"]} ms/job, allocated {floor["allocKiBPerJob"]} KiB/job, wall {floor["wallMsPerJob"]} ms/job over {floor["costJobs"]} jobs.");
        sb.AppendLine();
        if (floor["allocationProfile"] is JsonArray profile)
        {
            sb.AppendLine("Top allocation sample types: " + string.Join(", ", profile.Take(10).Select(p => $"`{p!["type"]}` {p["samples"]}")) + ". Full table: floor.md.");
            sb.AppendLine();
        }

        if (idle.Count > 0)
        {
            sb.AppendLine("## Idle cost (no work offered; each role alone on its own database)");
            sb.AppendLine();
            sb.AppendLine("| Role | statements/s (incl. BEGIN/COMMIT) | CPU ms/s |");
            sb.AppendLine("|---|---|---|");
            foreach (var r in idle)
            {
                sb.AppendLine(Ic, $"| {r!["role"]} | {r["statementsPerSec"]} | {r["cpuMsPerSec"]} |");
            }

            sb.AppendLine();
            foreach (var r in idle)
            {
                sb.AppendLine(Ic, $"Top statements, {r!["role"]}:");
                sb.AppendLine();
                foreach (var t in r["top"]!.AsArray())
                {
                    sb.AppendLine(Ic, $"- {t!["perSec"]}/s `{t["query"]!.GetValue<string>().Replace("`", "'", StringComparison.Ordinal)}`");
                }

                sb.AppendLine();
            }
        }

        if (p01 is not null)
        {
            sb.AppendLine("## P-01, shortened (1 repetition, 100/s offered, 16 slots, 100 ms handler)");
            sb.AppendLine();
            sb.AppendLine(Ic, $"Confirmed finishes/s {p01["finishesPerSec"]}; worker CPU {p01["cpuMsPerJob"]} ms/job and allocated {p01["allocKiBPerJob"]} KiB/job over {p01["invocations"]} invocations; violations {p01["violations"]}; failure {(p01["failure"] is null ? "none" : p01["failure"]!.ToString().Split('\n')[0])}.");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    static string Conn(TestDatabase db, string database, string app) =>
        new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = database, ApplicationName = app, Pooling = false }.ConnectionString;

    static string Head(string sql, int n)
    {
        var s = string.Join(' ', sql.Split((char[])['\n', '\r', ' ', '\t'], StringSplitOptions.RemoveEmptyEntries));
        return s.Length > n ? s[..n] + "…" : s;
    }

    static string I(int v) => v.ToString(Ic);

    static string D(double v) => v.ToString(Ic);
}
