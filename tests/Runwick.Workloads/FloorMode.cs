using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;

namespace Runwick.Workloads;

/// <summary>
/// Store-call floor probe (perf-floor diagnosis): one job at a time through Dispatch, Reserve, Begin, Renew and Finish
/// on an otherwise idle store, so each call runs without queueing. Per call it records the wall time, the number of
/// SQL commands Npgsql sent and their summed wall time (from the <c>Npgsql</c> ActivitySource); the remainder is pool
/// open/return, COMMIT and client-side work. Also times the bare round trip and a minimal INSERT transaction.
/// Usage: floor --conn cs [--db rw_perffloor] [--n 200] [--warmup 20] [--out file.md]
/// </summary>
internal static class FloorMode
{
    sealed record Sample(string Op, double TotalMs, int Commands, double CommandMs, List<(string Sql, double Ms)> Detail);

    static readonly AsyncLocal<List<(string Sql, double Ms)>?> Current = new();

    public static async Task<int> RunAsync(Args a)
    {
        var baseCs = a.Str("conn");
        var dbName = a.OptStr("db") ?? "rw_perffloor";
        var n = a.Int("n", 200);
        var warmup = a.Int("warmup", 20);
        var admin = new NpgsqlConnectionStringBuilder(baseCs) { Database = "postgres", Pooling = false }.ConnectionString;
        await Exec(admin, $"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)").ConfigureAwait(false);
        await Exec(admin, $"CREATE DATABASE {dbName}").ConfigureAwait(false);
        var dbCs = new NpgsqlConnectionStringBuilder(baseCs) { Database = dbName, Pooling = false }.ConnectionString;
        await RunwickPostgresMigrations.MigrateAsync(dbCs).ConfigureAwait(false);

        var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Npgsql",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = act =>
            {
                var sql = act.GetTagItem("db.query.text") as string ?? act.GetTagItem("db.statement") as string ?? act.DisplayName;
                Current.Value?.Add((sql, act.Duration.TotalMilliseconds));
            },
        };
        ActivitySource.AddActivityListener(listener);

        var clientCs = new NpgsqlConnectionStringBuilder(baseCs) { Database = dbName, ApplicationName = "rw:floor" }.ConnectionString;
        await using var client = await TestClient.CreateAsync(clientCs, "floor", new HarnessFaultInjector(), r => TestJobs.Register(r))
            .ConfigureAwait(false);

        var samples = new List<Sample>();
        async Task<T> Time<T>(string op, Func<Task<T>> call, bool keep)
        {
            var detail = new List<(string, double)>();
            Current.Value = detail;
            var t0 = Stopwatch.GetTimestamp();
            var result = await call().ConfigureAwait(false);
            var ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            Current.Value = null;
            if (keep)
            {
                lock (samples)
                {
                    samples.Add(new Sample(op, ms, detail.Count, detail.Sum(d => d.Item2), detail));
                }
            }

            return result;
        }

        // Bare round trips on the driver's own work pool, for comparison.
        var rawCs = new NpgsqlConnectionStringBuilder(clientCs) { ApplicationName = "rw:floor:raw", MaxAutoPrepare = 0 }.ConnectionString;
        await using var raw = NpgsqlDataSource.Create(rawCs);
        await Exec(rawCs, "CREATE TABLE IF NOT EXISTS public.floor_probe (id bigserial primary key, v int)").ConfigureAwait(false);

        for (var i = 0; i < warmup + n; i++)
        {
            var keep = i >= warmup;
            await Time("select1 (open+1 RT)", async () =>
            {
                await using var c = await raw.OpenConnectionAsync().ConfigureAwait(false);
                await using var cmd = new NpgsqlCommand("SELECT 1", c);
                return await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            }, keep).ConfigureAwait(false);
            await Time("insert tx (open+BEGIN/INSERT+COMMIT)", async () =>
            {
                await using var c = await raw.OpenConnectionAsync().ConfigureAwait(false);
                await using var tx = await c.BeginTransactionAsync().ConfigureAwait(false);
                await using var cmd = new NpgsqlCommand("INSERT INTO public.floor_probe (v) VALUES (1)", c, tx);
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return 0;
            }, keep).ConfigureAwait(false);

            await Time("Dispatch", () => client.Dispatcher.DispatchAsync(new PingJob("f")), keep).ConfigureAwait(false);
            var reserved = await Time("Reserve", () => client.ReserveAsync(), keep).ConfigureAwait(false);
            if (reserved.Status != StoreStatus.Applied)
            {
                throw new InvalidOperationException($"Reserve: {reserved.Status} {reserved.Error?.Code}");
            }

            var handle = reserved.Value!.Lease.Handle;
            var begun = await Time("Begin", () => client.BeginAsync(handle), keep).ConfigureAwait(false);
            Check(begun.Reply.Status, "Begin");
            Check((await Time("Renew", () => client.RenewAsync(handle), keep).ConfigureAwait(false)).Status, "Renew");
            Check((await Time("Finish", () => client.FinishAsync(handle, AttemptOutcome.Succeeded), keep).ConfigureAwait(false)).Status, "Finish");
        }

        // The command listener allocates per command; the cost passes below run without it.
        listener.Dispose();

        var report = new StringBuilder(Render(samples, n));
        var json = new System.Text.Json.Nodes.JsonObject
        {
            ["n"] = n,
            ["loadAverage"] = File.ReadAllText("/proc/loadavg").Trim(),
            ["p50Ms"] = new System.Text.Json.Nodes.JsonObject(samples.GroupBy(s => s.Op).Select(g =>
                KeyValuePair.Create(g.Key, (System.Text.Json.Nodes.JsonNode?)Math.Round(P([.. g.Select(s => s.TotalMs).Order()], 0.5), 2)))),
            ["commands"] = new System.Text.Json.Nodes.JsonObject(samples.GroupBy(s => s.Op).Select(g =>
                KeyValuePair.Create(g.Key, (System.Text.Json.Nodes.JsonNode?)Math.Round(g.Average(s => s.Commands), 1)))),
        };

        // Client CPU and allocation per job (Dispatch + Reserve + Begin + Finish), sequential, no listener attached.
        var costJobs = a.Int("cost-jobs", 0);
        if (costJobs > 0)
        {
            async Task JobAsync()
            {
                await client.Dispatcher.DispatchAsync(new PingJob("c")).ConfigureAwait(false);
                var r = await client.ReserveAsync().ConfigureAwait(false);
                Check(r.Status, "Reserve");
                var h = r.Value!.Lease.Handle;
                Check((await client.BeginAsync(h).ConfigureAwait(false)).Reply.Status, "Begin");
                Check((await client.FinishAsync(h, AttemptOutcome.Succeeded).ConfigureAwait(false)).Status, "Finish");
            }

            for (var i = 0; i < 20; i++)
            {
                await JobAsync().ConfigureAwait(false);
            }

            using var self = Process.GetCurrentProcess();
            self.Refresh();
            var cpu0 = self.TotalProcessorTime;
            var alloc0 = GC.GetTotalAllocatedBytes(true);
            var wall0 = Stopwatch.GetTimestamp();
            for (var i = 0; i < costJobs; i++)
            {
                await JobAsync().ConfigureAwait(false);
            }

            var wall = Stopwatch.GetElapsedTime(wall0);
            self.Refresh();
            var cpuMs = (self.TotalProcessorTime - cpu0).TotalMilliseconds / costJobs;
            var allocKiB = (GC.GetTotalAllocatedBytes(true) - alloc0) / 1024.0 / costJobs;
            json["costJobs"] = costJobs;
            json["cpuMsPerJob"] = Math.Round(cpuMs, 2);
            json["allocKiBPerJob"] = Math.Round(allocKiB, 1);
            json["wallMsPerJob"] = Math.Round(wall.TotalMilliseconds / costJobs, 2);
            report.AppendLine(CultureInfo.InvariantCulture,
                $"Client cost per job (Dispatch + Reserve + Begin + Finish, {costJobs} jobs, no listener): CPU {cpuMs:F2} ms, allocated {allocKiB:F1} KiB, wall {wall.TotalMilliseconds / costJobs:F1} ms.");
            report.AppendLine();

            if (a.Has("alloc-profile"))
            {
                // Allocation sampling: the runtime's AllocationTick event fires about every 100 KiB allocated and names
                // the type that crossed the threshold, so tick counts per type approximate each type's share.
                using var profiler = new AllocationProfiler();
                for (var i = 0; i < costJobs; i++)
                {
                    await JobAsync().ConfigureAwait(false);
                }

                await Task.Delay(1000).ConfigureAwait(false);
                var top = profiler.Top(25);
                var total = Math.Max(1, profiler.Total);
                var arr = new System.Text.Json.Nodes.JsonArray();
                report.AppendLine(CultureInfo.InvariantCulture, $"Allocation profile ({profiler.Total} AllocationTick samples over {costJobs} jobs; share of samples):");
                report.AppendLine();
                report.AppendLine("| Type | samples | share |");
                report.AppendLine("|---|---|---|");
                foreach (var (type, count) in top)
                {
                    report.AppendLine(CultureInfo.InvariantCulture, $"| `{type}` | {count} | {100.0 * count / total:F1} % |");
                    arr.Add(new System.Text.Json.Nodes.JsonObject { ["type"] = type, ["samples"] = count });
                }

                report.AppendLine();
                json["allocationProfile"] = arr;
            }
        }

        var text = report.ToString();
        Console.WriteLine(text);
        if (a.OptStr("out") is { } outFile)
        {
            await File.WriteAllTextAsync(outFile, text).ConfigureAwait(false);
        }

        if (a.OptStr("json") is { } jsonFile)
        {
            await File.WriteAllTextAsync(jsonFile, json.ToJsonString(Json.Indented)).ConfigureAwait(false);
        }

        return 0;
    }

    /// <summary>In-process allocation sampler over the runtime's <c>GCAllocationTick</c> events (GC keyword, verbose).</summary>
    sealed class AllocationProfiler : System.Diagnostics.Tracing.EventListener
    {
        readonly Dictionary<string, int> _counts = new(StringComparer.Ordinal);

        public int Total { get; private set; }

        protected override void OnEventSourceCreated(System.Diagnostics.Tracing.EventSource eventSource)
        {
            if (eventSource.Name == "Microsoft-Windows-DotNETRuntime")
            {
                EnableEvents(eventSource, System.Diagnostics.Tracing.EventLevel.Verbose, (System.Diagnostics.Tracing.EventKeywords)0x1);
            }
        }

        protected override void OnEventWritten(System.Diagnostics.Tracing.EventWrittenEventArgs e)
        {
            if (e.EventName is not { } name || !name.StartsWith("GCAllocationTick", StringComparison.Ordinal) || e.PayloadNames is null)
            {
                return;
            }

            var i = e.PayloadNames.IndexOf("TypeName");
            var type = i >= 0 ? e.Payload?[i] as string ?? "?" : "?";
            lock (_counts)
            {
                _counts[type] = _counts.GetValueOrDefault(type) + 1;
                Total++;
            }
        }

        public List<(string Type, int Count)> Top(int n)
        {
            lock (_counts)
            {
                return [.. _counts.OrderByDescending(kv => kv.Value).Take(n).Select(kv => (kv.Key, kv.Value))];
            }
        }
    }

    static void Check(StoreStatus s, string op)
    {
        if (s != StoreStatus.Applied)
        {
            throw new InvalidOperationException($"{op}: {s}");
        }
    }

    static string Render(List<Sample> samples, int n)
    {
        var ic = CultureInfo.InvariantCulture;
        var sb = new StringBuilder();
        sb.AppendLine(ic, $"n = {n} per operation, sequential, load average {File.ReadAllText("/proc/loadavg").Trim()}");
        sb.AppendLine();
        sb.AppendLine("| Operation | wall p50 | wall p90 | wall mean | SQL commands | SQL wall sum p50 | other (open, COMMIT, client) p50 |");
        sb.AppendLine("|---|---|---|---|---|---|---|");
        foreach (var g in samples.GroupBy(s => s.Op))
        {
            var tot = g.Select(s => s.TotalMs).Order().ToArray();
            var cmd = g.Select(s => s.CommandMs).Order().ToArray();
            var other = g.Select(s => s.TotalMs - s.CommandMs).Order().ToArray();
            sb.AppendLine(ic, $"| {g.Key} | {P(tot, 0.5):F2} | {P(tot, 0.9):F2} | {tot.Average():F2} | {g.Average(s => s.Commands):F1} | {P(cmd, 0.5):F2} | {P(other, 0.5):F2} |");
        }

        sb.AppendLine();
        sb.AppendLine("Per-statement client wall time (p50 ms, ordinal within the call):");
        sb.AppendLine();
        foreach (var g in samples.GroupBy(s => s.Op).Where(g => g.First().Commands > 1))
        {
            sb.AppendLine(ic, $"**{g.Key}**");
            sb.AppendLine();
            var maxCmds = g.Max(s => s.Commands);
            for (var i = 0; i < maxCmds; i++)
            {
                var vals = g.Where(s => s.Detail.Count > i).Select(s => s.Detail[i].Ms).Order().ToArray();
                var sql = g.First(s => s.Detail.Count > i).Detail[i].Sql;
                var head = string.Join(' ', sql.Split((char[])['\n', '\r', ' '], StringSplitOptions.RemoveEmptyEntries)).Replace("|", "\\|", StringComparison.Ordinal);
                sb.AppendLine(ic, $"{i + 1}. {P(vals, 0.5):F2} ms — `{(head.Length > 90 ? head[..90] + "…" : head)}`");
            }

            sb.AppendLine();
        }

        return sb.ToString();
    }

    static double P(double[] sorted, double q) => sorted.Length == 0 ? double.NaN : sorted[Math.Min(sorted.Length - 1, (int)(q * sorted.Length))];

    static async Task Exec(string cs, string sql)
    {
        await using var c = new NpgsqlConnection(cs);
        await c.OpenAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}
