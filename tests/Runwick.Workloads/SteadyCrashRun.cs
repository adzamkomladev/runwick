using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;

namespace Runwick.Workloads;

/// <summary>
/// <c>bench --store sqlite --mode steady --crash-cycles k</c> (Q6/Q7): the steady window is split into k + 1 segments, each a
/// separate bench process on the same SQLite file. Segments 0 to k − 1 are killed at the end of their window (no graceful
/// stop, dispatch calls and jobs still in flight); the next segment reopens the file and keeps going; the last one drains the
/// store. Each segment writes a ledger of accepted, started and finished load jobs, and the parent checks it against the store:
/// every accepted job must have succeeded, and a job may only run again if an earlier run was cut off by a crash.
/// All other bench flags (rate, calls, workers, trace, viewers, seed, retention, schedule, outbox, sampling) pass through.
/// </summary>
internal static class SteadyCrashRun
{
    // Flags the parent sets per segment; the copies the user gave are not forwarded.
    static readonly HashSet<string> ParentOnly = ["crash-cycles", "out", "seconds", "sqlite-seed", "sqlite-dir", "sqlite-keep", "drain-store-seconds"];

    public static async Task<int> RunAsync(Args a)
    {
        var cycles = a.Int("crash-cycles", 2);
        if (a.OptStr("store") != "sqlite" || a.OptStr("mode") != "steady" || cycles < 1)
        {
            Console.Error.WriteLine("--crash-cycles needs --store sqlite --mode steady and at least one cycle");
            return 2;
        }

        var segmentSeconds = a.Double("seconds", 120) / (cycles + 1);
        var dir = Directory.CreateDirectory(Path.Combine(a.OptStr("sqlite-dir") ?? Path.GetTempPath(), "bench-crash-" + Guid.NewGuid().ToString("N"))).FullName;
        var db = Path.Combine(dir, "runwick.db");
        var forwarded = new List<string>();
        for (var i = 0; i < a.Raw.Count; i++)
        {
            var name = a.Raw[i].StartsWith("--", StringComparison.Ordinal) ? a.Raw[i][2..] : null;
            var hasValue = name is not null && i + 1 < a.Raw.Count && !a.Raw[i + 1].StartsWith("--", StringComparison.Ordinal);
            if (name is not null && ParentOnly.Contains(name))
            {
                i += hasValue ? 1 : 0;
                continue;
            }

            forwarded.Add(a.Raw[i]);
        }

        var runStartMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var segments = new JsonArray();
        for (var segment = 0; segment <= cycles; segment++)
        {
            var last = segment == cycles;
            List<string> args = ["bench", .. forwarded, "--segment", Inv(segment), "--seconds", Inv(segmentSeconds), "--sqlite-reuse", db,
                "--ledger", Ledger(dir, segment), "--out", SegmentOut(dir, segment)];
            args.AddRange(last ? ["--drain-store-seconds", Inv(a.Int("drain-store-seconds", 300))] : ["--crash"]);
            if (segment == 0 && a.OptStr("sqlite-seed") is { } seed)
            {
                args.AddRange(["--sqlite-seed", seed]);
            }

            var started = Stopwatch.StartNew();
            var exitCode = await RunSelfAsync(args).ConfigureAwait(false);
            Console.Error.WriteLine($"segment {segment} exited {exitCode} after {started.Elapsed.TotalSeconds:F1} s");
            segments.Add(new JsonObject
            {
                ["index"] = segment,
                ["killed"] = !last,
                ["exitCode"] = exitCode,
                ["wallSeconds"] = Math.Round(started.Elapsed.TotalSeconds, 1),
                ["result"] = File.Exists(SegmentOut(dir, segment)) ? JsonNode.Parse(await File.ReadAllTextAsync(SegmentOut(dir, segment)).ConfigureAwait(false)) : null,
            });
        }

        var result = new JsonObject
        {
            ["system"] = "runwick-sqlite",
            ["store"] = "sqlite",
            ["mode"] = "steady-crash",
            ["crashCycles"] = cycles,
            ["segmentSeconds"] = Math.Round(segmentSeconds, 1),
            ["directory"] = dir,
            ["combined"] = Combine(segments),
            ["reconciliation"] = await ReconcileAsync(db, dir, cycles, runStartMs).ConfigureAwait(false),
            ["segments"] = segments,
        };
        var json = result.ToJsonString(Json.Indented);
        Console.WriteLine(json);
        if (a.OptStr("out") is { } outFile)
        {
            await File.WriteAllTextAsync(outFile, json).ConfigureAwait(false);
        }

        if (!a.Has("sqlite-keep"))
        {
            Directory.Delete(dir, recursive: true);
        }

        return 0;
    }

    static string Inv(double value) => value.ToString(CultureInfo.InvariantCulture);

    static string Ledger(string dir, int segment) => Path.Combine(dir, $"ledger-{segment}.txt");

    static string SegmentOut(string dir, int segment) => Path.Combine(dir, $"segment-{segment}.json");

    static async Task<int> RunSelfAsync(List<string> args)
    {
        var info = new ProcessStartInfo(Environment.ProcessPath!) { UseShellExecute = false };
        if (Path.GetFileNameWithoutExtension(Environment.ProcessPath) == "dotnet")
        {
            info.ArgumentList.Add(typeof(SteadyCrashRun).Assembly.Location);
        }

        foreach (var arg in args)
        {
            info.ArgumentList.Add(arg);
        }

        using var process = Process.Start(info)!;
        await process.WaitForExitAsync().ConfigureAwait(false);
        return process.ExitCode;
    }

    /// <summary>Totals over all segments; a killed segment counts what it measured up to the kill.</summary>
    static JsonObject Combine(JsonArray segments)
    {
        var results = segments.Select(s => s!["result"]).OfType<JsonObject>().ToList();
        long Sum(string field) => results.Sum(r => r[field]?.GetValue<long>() ?? 0);
        var refusals = new SortedDictionary<string, long>(StringComparer.Ordinal);
        var writerQueue = new SortedDictionary<string, double>(StringComparer.Ordinal);
        foreach (var r in results)
        {
            foreach (var (code, count) in r["failedDispatchByCode"]?.AsObject() ?? new JsonObject())
            {
                refusals[code] = refusals.GetValueOrDefault(code) + count!.GetValue<long>();
            }

            foreach (var (phase, summary) in r["writerQueueMs"]?.AsObject() ?? new JsonObject())
            {
                writerQueue[phase] = Math.Max(writerQueue.GetValueOrDefault(phase), summary!["p99"]!.GetValue<double>());
            }
        }

        return new JsonObject
        {
            ["offered"] = Sum("offered"),
            ["accepted"] = Sum("inserted"),
            ["refused"] = Sum("failedDispatch"),
            ["refusedByCode"] = new JsonObject([.. refusals.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
            ["writerQueueP99MsWorstSegment"] = new JsonObject([.. writerQueue.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
            ["busyConflicts"] = results.Sum(r => r["sqlite"]?["busyConflicts"]?.GetValue<long>() ?? 0),
            ["scheduleJobsFinished"] = results.Sum(r => r["schedule"]?["jobsFinished"]?.GetValue<long>() ?? 0),
            ["outboxStaged"] = results.Sum(r => r["outbox"]?["staged"]?.GetValue<long>() ?? 0),
            ["outboxStageErrors"] = results.Sum(r => r["outbox"]?["stageErrors"]?.GetValue<long>() ?? 0),
        };
    }

    /// <summary>The ledgers against the store, after the last segment drained it.</summary>
    static async Task<JsonObject> ReconcileAsync(string db, string dir, int cycles, long runStartMs)
    {
        var offered = new Dictionary<Guid, (int Segment, long Ms)>();
        var invoked = new Dictionary<Guid, List<(int Segment, long Ms)>>();
        var finished = new Dictionary<Guid, (int Segment, long Ms)>();
        var finishedIn = new HashSet<(Guid, int)>();
        for (var segment = 0; segment <= cycles; segment++)
        {
            if (!File.Exists(Ledger(dir, segment)))
            {
                continue;
            }

            foreach (var line in await File.ReadAllLinesAsync(Ledger(dir, segment)).ConfigureAwait(false))
            {
                var parts = line.Split(' ');
                if (parts.Length != 3 || !Guid.TryParseExact(parts[1], "N", out var id) || !long.TryParse(parts[2], CultureInfo.InvariantCulture, out var ms))
                {
                    continue; // a line cut off by the kill
                }

                switch (parts[0])
                {
                    case "a":
                        offered.TryAdd(id, (segment, ms));
                        break;
                    case "i":
                        (invoked.TryGetValue(id, out var runs) ? runs : invoked[id] = []).Add((segment, ms));
                        break;
                    case "f":
                        finished.TryAdd(id, (segment, ms));
                        finishedIn.Add((id, segment));
                        break;
                }
            }
        }

        var store = new Dictionary<Guid, (string State, long Executions)>();
        var side = new JsonObject();
        using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = db, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            using var jobs = connection.CreateCommand();
            // Load jobs created during the run; the retained fixture's jobs are older.
            jobs.CommandText = "SELECT job_id, state, execution_count FROM runwick_jobs WHERE application = 'bench' AND queue = $q AND created_at >= $start";
            jobs.Parameters.AddWithValue("$q", LoadRegistration.Queue);
            jobs.Parameters.AddWithValue("$start", runStartMs - 1000);
            using (var rows = await jobs.ExecuteReaderAsync().ConfigureAwait(false))
            {
                while (await rows.ReadAsync().ConfigureAwait(false))
                {
                    store[new Guid((byte[])rows[0], bigEndian: true)] = (rows.GetString(1), rows.GetInt64(2));
                }
            }

            using var sideJobs = connection.CreateCommand();
            sideJobs.CommandText = "SELECT queue, state, count(*), sum(execution_count > 1) FROM runwick_jobs WHERE application = 'bench' AND queue IN ($s, $o) AND created_at >= $start GROUP BY queue, state";
            sideJobs.Parameters.AddWithValue("$s", SideQueues.Schedule);
            sideJobs.Parameters.AddWithValue("$o", SideQueues.Outbox);
            sideJobs.Parameters.AddWithValue("$start", runStartMs - 1000);
            using var sideRows = await sideJobs.ExecuteReaderAsync().ConfigureAwait(false);
            while (await sideRows.ReadAsync().ConfigureAwait(false))
            {
                var queue = (side[sideRows.GetString(0)] ??= new JsonObject()).AsObject();
                queue[sideRows.GetString(1)] = sideRows.GetInt64(2);
                queue["executedMoreThanOnce"] = (queue["executedMoreThanOnce"]?.GetValue<long>() ?? 0) + sideRows.GetInt64(3);
            }
        }

        var outboxSource = Path.Combine(dir, "outbox-source.db");
        if (File.Exists(outboxSource))
        {
            side["outboxSourceByState"] = await BenchMode.CountOutboxSourceAsync(
                new SqliteConnectionStringBuilder { DataSource = outboxSource, Pooling = false }.ConnectionString).ConfigureAwait(false);
        }

        // A job may run more than once only if every run but the last started in a killed segment that never confirmed its finish.
        long ranMoreThanOnce = 0, rerunsAfterCrash = 0, rerunsNotExplained = 0;
        foreach (var (id, runs) in invoked)
        {
            if (runs.Count < 2)
            {
                continue;
            }

            ranMoreThanOnce++;
            var explained = runs.Take(runs.Count - 1).All(r => r.Segment < cycles && !finishedIn.Contains((id, r.Segment)))
                && runs.Zip(runs.Skip(1)).All(p => p.First.Segment < p.Second.Segment);
            _ = explained ? rerunsAfterCrash++ : rerunsNotExplained++;
        }

        var notSucceeded = new SortedDictionary<string, long>(StringComparer.Ordinal);
        long missing = 0, finishNotConfirmed = 0, crossedACrash = 0;
        foreach (var (id, offer) in offered)
        {
            if (!store.TryGetValue(id, out var row))
            {
                missing++;
                continue;
            }

            if (row.State != "succeeded")
            {
                notSucceeded[row.State] = notSucceeded.GetValueOrDefault(row.State) + 1;
            }

            finishNotConfirmed += finished.ContainsKey(id) ? 0 : 1;
            crossedACrash += invoked.TryGetValue(id, out var runs) && (runs.Count > 1 || runs[0].Segment > offer.Segment) ? 1 : 0;
        }

        var withoutReceipt = store.Where(kv => !offered.ContainsKey(kv.Key)).ToList();
        var toFinish = offered.Where(kv => finished.ContainsKey(kv.Key)).Select(kv => (double)(finished[kv.Key].Ms - kv.Value.Ms)).ToList();
        var toPickup = offered.Where(kv => invoked.ContainsKey(kv.Key)).Select(kv => (double)(invoked[kv.Key][0].Ms - kv.Value.Ms)).ToList();
        var span = finished.Count == 0 || offered.Count == 0 ? 0 : (finished.Values.Max(f => f.Ms) - offered.Values.Min(o => o.Ms)) / 1000.0;
        return new JsonObject
        {
            ["acceptedJobs"] = offered.Count,
            ["acceptedMissingFromStore"] = missing,
            ["acceptedNotSucceededByState"] = new JsonObject([.. notSucceeded.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
            ["acceptedFinishNotConfirmedToWorker"] = finishNotConfirmed,
            ["acceptedJobsThatCrossedACrash"] = crossedACrash,
            ["storeJobsWithoutReceipt"] = withoutReceipt.Count,
            ["storeJobsWithoutReceiptNotSucceeded"] = withoutReceipt.Count(kv => kv.Value.State != "succeeded"),
            ["jobsRunMoreThanOnce"] = ranMoreThanOnce,
            ["rerunsAfterCrash"] = rerunsAfterCrash,
            ["rerunsNotExplainedByACrash"] = rerunsNotExplained,
            ["storeExecutionCountAboveOne"] = store.Values.Count(r => r.Executions > 1),
            ["durableCompletionsPerSec"] = span > 0 ? Math.Round(finished.Count(kv => offered.ContainsKey(kv.Key)) / span, 1) : 0,
            ["offerToFinishMs"] = Percentiles(toFinish),
            ["offerToPickupMs"] = Percentiles(toPickup),
            ["sideJobsByQueueAndState"] = side,
        };
    }

    internal static JsonObject Percentiles(List<double> values)
    {
        values.Sort();
        double At(double q) => values.Count == 0 ? 0 : values[Math.Clamp((int)Math.Ceiling(q * values.Count) - 1, 0, values.Count - 1)];
        return new JsonObject { ["count"] = values.Count, ["p50"] = At(0.5), ["p95"] = At(0.95), ["p99"] = At(0.99), ["max"] = At(1) };
    }
}
