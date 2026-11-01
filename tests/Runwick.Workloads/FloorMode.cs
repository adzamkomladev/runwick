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

        using var listener = new ActivityListener
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

        var report = Render(samples, n);
        Console.WriteLine(report);
        if (a.OptStr("out") is { } outFile)
        {
            await File.WriteAllTextAsync(outFile, report).ConfigureAwait(false);
        }

        return 0;
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
