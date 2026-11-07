using System.Globalization;
using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Registration;
using Runwick.ReferenceModel;

namespace Runwick.Workloads;

/// <summary>End-of-run analysis of one repetition (07 §14.4–§14.6): window metrics, store queries, plans, correctness.</summary>
internal static class Analysis
{
    public static long Terminal(DbSample s) => s.State("succeeded") + s.State("failed") + s.State("cancelled");

    public static async Task<JsonObject> AnalyseAsync(RunContext c, WorkloadSpec spec, string conn, DateTimeOffset startedAt)
    {
        var samples = c.Sampler.Samples;
        var start = c.Marks.GetValueOrDefault("measureStart", startedAt);
        var end = c.Marks.GetValueOrDefault("measureEnd", c.Marks.GetValueOrDefault("producerStop", DateTimeOffset.UtcNow));
        var window = samples.Where(s => s.At >= start && s.At <= end).ToList();
        var o = new JsonObject
        {
            ["spec"] = new JsonObject
            {
                ["workers"] = spec.Workers, ["concurrency"] = spec.Concurrency, ["handlerMs"] = spec.HandlerMs, ["rate"] = spec.Rate,
                ["count"] = spec.Count, ["typeCeiling"] = spec.TypeCeiling, ["timing"] = spec.Divisor is { } d ? $"scaled /{d}" : "default",
                ["proxy"] = spec.Proxy,
            },
            ["marks"] = new JsonObject(c.Marks.OrderBy(m => m.Value).Select(m => KeyValuePair.Create(m.Key, (JsonNode?)m.Value.ToString("O")))),
            ["dbSampleFailures"] = c.Sampler.Failures,
        };

        if (window.Count >= 2)
        {
            var (first, last) = (window[0], window[^1]);
            var seconds = (last.At - first.At).TotalSeconds;
            var jobs = Terminal(last) - Terminal(first);
            var receipts = last.Inserts.Where(kv => kv.Key.StartsWith("storage_commands", StringComparison.Ordinal)).Sum(kv => kv.Value)
                - first.Inserts.Where(kv => kv.Key.StartsWith("storage_commands", StringComparison.Ordinal)).Sum(kv => kv.Value);
            o["window"] = new JsonObject
            {
                ["seconds"] = Math.Round(seconds, 1),
                ["confirmedFinishesPerSec"] = Math.Round(jobs / seconds, 2),
                ["terminalJobs"] = jobs,
                ["backlogMax"] = window.Max(s => s.Backlog),
                ["backlogMean"] = Math.Round(window.Average(s => s.Backlog), 1),
                ["backlogSlopePerSec"] = Math.Round(Slope(window), 3),
                ["backlogStart"] = first.Backlog,
                ["backlogEnd"] = last.Backlog,
                ["walBytesPerJob"] = jobs == 0 ? null : Math.Round((double)(last.WalBytes - first.WalBytes) / jobs, 0),
                ["walBytesPerSec"] = Math.Round((last.WalBytes - first.WalBytes) / seconds, 0),
                ["transactionsPerSec"] = Math.Round((last.Transactions - first.Transactions) / seconds, 2),
                ["receiptWritesPerSec"] = Math.Round(receipts / seconds, 2),
                ["insertsPerJob"] = jobs == 0 ? null : new JsonObject(last.Inserts.Where(kv => first.Inserts.ContainsKey(kv.Key) && kv.Value > first.Inserts[kv.Key])
                    .Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)Math.Round((double)(kv.Value - first.Inserts[kv.Key]) / jobs, 3)))),
                ["lockWaitersMax"] = window.Max(s => s.LockWaiters),
                ["lockWaitersMean"] = Math.Round(window.Average(s => s.LockWaiters), 3),
            };
        }

        o["connectionsMax"] = new JsonObject(samples.SelectMany(s => s.Connections).GroupBy(kv => kv.Key)
            .OrderBy(g => g.Key, StringComparer.Ordinal).Select(g => KeyValuePair.Create(g.Key, (JsonNode?)g.Max(kv => kv.Value))));
        o["producer"] = ReadJson(Path.Combine(c.Dir, "producer.summary.json"));
        o["producerWindow"] = ProducerWindow(Path.Combine(c.Dir, "producer.samples.jsonl"), start, end);
        var workers = new JsonArray();
        for (var i = 1; i <= spec.Workers; i++)
        {
            var w = ReadJson(Path.Combine(c.Dir, $"worker{i}.summary.json"));
            if (w is JsonObject wo)
            {
                wo["admission"] = AdmissionTimeline(Path.Combine(c.Dir, $"worker{i}.samples.jsonl"), c.Marks.GetValueOrDefault("restored"));
            }

            workers.Add(w);
        }

        o["workers"] = workers;
        o["ceilings"] = Ceilings(workers, spec);
        try
        {
            o["store"] = await StoreQueriesAsync(conn, start, end).ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            o["store"] = "failed: " + ex.Message;
        }

        o["correctness"] = await CorrectnessAsync(conn, workers, o["producer"], spec, c).ConfigureAwait(false);
        return o;
    }

    public static string OneLine(JsonObject r) =>
        $"finishes/s {r["window"]?["confirmedFinishesPerSec"]}, backlog slope {r["window"]?["backlogSlopePerSec"]}/s, "
        + $"violations {r["correctness"]?["violationCount"]}, failure {(r["failure"] is null ? "none" : "yes")}";

    static double Slope(List<DbSample> window)
    {
        var t0 = window[0].At;
        var xs = window.Select(s => (s.At - t0).TotalSeconds).ToArray();
        var ys = window.Select(s => (double)s.Backlog).ToArray();
        var (mx, my) = (xs.Average(), ys.Average());
        var den = xs.Sum(x => (x - mx) * (x - mx));
        return den == 0 ? 0 : xs.Zip(ys).Sum(p => (p.First - mx) * (p.Second - my)) / den;
    }

    static JsonNode? ReadJson(string path) => File.Exists(path) ? JsonNode.Parse(File.ReadAllText(path)) : null;

    static JsonObject? ProducerWindow(string path, DateTimeOffset start, DateTimeOffset end)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        var lines = File.ReadAllLines(path).Select(l => JsonNode.Parse(l)!).Where(n => n["t"] is not null)
            .Select(n => (At: DateTimeOffset.Parse(n["t"]!.GetValue<string>(), CultureInfo.InvariantCulture), N: n))
            .Where(x => x.At >= start && x.At <= end).ToList();
        if (lines.Count < 2)
        {
            return null;
        }

        var seconds = (lines[^1].At - lines[0].At).TotalSeconds;
        long D(string k) => lines[^1].N[k]!.GetValue<long>() - lines[0].N[k]!.GetValue<long>();
        return new JsonObject
        {
            ["offeredPerSec"] = Math.Round(D("offered") / seconds, 2),
            ["acceptedPerSec"] = Math.Round(D("accepted") / seconds, 2),
            ["failedPerSec"] = Math.Round(D("failed") / seconds, 3),
            ["inFlightMax"] = lines.Max(x => x.N["inFlight"]!.GetValue<long>()),
        };
    }

    /// <summary>Pause/resume instants from the worker's 5 s samples; for P-08c, the first ready sample with an Applied Reserve after restoration.</summary>
    static JsonObject AdmissionTimeline(string path, DateTimeOffset restored)
    {
        var transitions = new JsonArray();
        string? previous = null;
        DateTimeOffset? readyAfterRestore = null;
        long? reservesAtRestore = null;
        if (File.Exists(path))
        {
            foreach (var line in File.ReadLines(path))
            {
                var n = JsonNode.Parse(line)!;
                var at = DateTimeOffset.Parse(n["t"]!.GetValue<string>(), CultureInfo.InvariantCulture);
                var state = $"{n["status"]}/{(n["admissionOpen"]!.GetValue<bool>() ? "open" : "closed")}/{(n["pressureStopped"]!.GetValue<bool>() ? "pressure" : "flowing")}";
                if (state != previous)
                {
                    transitions.Add(new JsonObject { ["t"] = at.ToString("O"), ["state"] = state });
                    previous = state;
                }

                var reserves = n["counters"]?["reserve:Applied"]?.GetValue<long>() ?? 0;
                if (restored != default && at >= restored)
                {
                    reservesAtRestore ??= reserves;
                    if (readyAfterRestore is null && n["status"]!.GetValue<string>() == "ready" && reserves > reservesAtRestore)
                    {
                        readyAfterRestore = at;
                    }
                }
            }
        }

        return new JsonObject
        {
            ["transitions"] = transitions,
            ["secondsFromRestorationToReady"] = readyAfterRestore is { } r ? Math.Round((r - restored).TotalSeconds, 1) : null,
        };
    }

    /// <summary>07 §14.6 items 3–4: store-time floor, slot-bound ceiling, serial-claim ceiling.</summary>
    static JsonObject Ceilings(JsonArray workers, WorkloadSpec spec)
    {
        double Mean(string op) => workers.Where(w => w?["latencies"]?[op] is not null)
            .Select(w => w!["latencies"]![op]!["meanMs"]!.GetValue<double>()).DefaultIfEmpty(0).Average();
        double P99(string op) => workers.Where(w => w?["latencies"]?[op] is not null)
            .Select(w => w!["latencies"]![op]!["p99Ms"]!.GetValue<double>()).DefaultIfEmpty(0).Max();
        var (r, b, f) = (Mean("reserve"), Mean("begin"), Mean("finish"));
        return new JsonObject
        {
            ["tReserveMeanMs"] = Math.Round(r, 2), ["tReserveP99Ms"] = P99("reserve"),
            ["tBeginMeanMs"] = Math.Round(b, 2), ["tBeginP99Ms"] = P99("begin"),
            ["tFinishMeanMs"] = Math.Round(f, 2), ["tFinishP99Ms"] = P99("finish"),
            ["tRenewMeanMs"] = Math.Round(Mean("renew"), 2), ["tRenewP99Ms"] = P99("renew"),
            ["storeTimeFloorMs"] = Math.Round(r + b + f, 2),
            ["slotBoundCeilingPerSec"] = b + spec.HandlerMs + f == 0 ? null : Math.Round(spec.Concurrency * spec.Workers * 1000 / (b + spec.HandlerMs + f), 1),
            ["serialClaimCeilingPerSec"] = r == 0 ? null : Math.Round(spec.Workers * 1000 / r, 1),
        };
    }

    static async Task<JsonObject> StoreQueriesAsync(string cs, DateTimeOffset start, DateTimeOffset end)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync().ConfigureAwait(false);
        var o = new JsonObject();
        await using (var cmd = new NpgsqlCommand("""
            SELECT count(*),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), percentile_cont(0.95) WITHIN GROUP (ORDER BY ms),
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY ms), max(ms)
            FROM (SELECT extract(epoch FROM a.authorized_at - j.created_at) * 1000 AS ms
                  FROM runwick.job_attempts a JOIN runwick.jobs j ON j.application = a.application AND j.job_id = a.job_id
                  WHERE a.execution_number = 1 AND a.authorized_at BETWEEN @s AND @e) x
            """, conn))
        {
            cmd.Parameters.AddWithValue("s", start.UtcDateTime);
            cmd.Parameters.AddWithValue("e", end.UtcDateTime);
            await using var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            await r.ReadAsync().ConfigureAwait(false);
            o["dueToBeginFirstExecution"] = new JsonObject
            {
                ["count"] = r.GetInt64(0),
                ["p50Ms"] = r.IsDBNull(1) ? null : Math.Round(r.GetDouble(1), 1),
                ["p95Ms"] = r.IsDBNull(2) ? null : Math.Round(r.GetDouble(2), 1),
                ["p99Ms"] = r.IsDBNull(3) ? null : Math.Round(r.GetDouble(3), 1),
                ["maxMs"] = r.IsDBNull(4) ? null : Math.Round(Convert.ToDouble(r.GetValue(4), CultureInfo.InvariantCulture), 1),
            };
        }

        var rows = new JsonObject();
        foreach (var table in new[] { "jobs", "job_attempts", "job_generations", "storage_commands", "queue_events", "concurrency_permits", "worker_instances" })
        {
            await using var cmd = new NpgsqlCommand($"SELECT count(*) FROM runwick.{table}", conn);
            rows[table] = (long)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
        }

        o["rowsAtEnd"] = rows;
        o["outcomes"] = await GroupAsync(conn, "SELECT coalesce(outcome, '(open)'), count(*) FROM runwick.job_attempts GROUP BY 1").ConfigureAwait(false);
        o["states"] = await GroupAsync(conn, "SELECT state, count(*) FROM runwick.jobs GROUP BY 1").ConfigureAwait(false);
        o["plans"] = await PlansAsync(conn).ConfigureAwait(false);
        return o;
    }

    static async Task<JsonObject> GroupAsync(NpgsqlConnection conn, string sql)
    {
        var o = new JsonObject();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await r.ReadAsync().ConfigureAwait(false))
        {
            o[r.GetString(0)] = r.GetInt64(1);
        }

        return o;
    }

    /// <summary>EXPLAIN (ANALYZE, BUFFERS) of the readiness probe and the Reserve candidate query (07 §14.4), rolled back.</summary>
    static async Task<JsonObject> PlansAsync(NpgsqlConnection conn)
    {
        const string Contract = """
            AND EXISTS (SELECT 1 FROM unnest(ARRAY['wl.load']::text[], ARRAY[1]::integer[], ARRAY[@pid]::text[], ARRAY[@pv]::integer[])
                        AS c(name, version, profile_id, profile_version)
                        WHERE c.name = j.job_name AND c.version = j.payload_version
                          AND c.profile_id = j.serializer_profile_id AND c.profile_version = j.serializer_profile_version)
            """;
        var plans = new JsonObject();
        string pid;
        int pv;
        string priority;
        await using (var cmd = new NpgsqlCommand("SELECT serializer_profile_id, serializer_profile_version, priority FROM runwick.jobs LIMIT 1", conn))
        await using (var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
        {
            if (!await r.ReadAsync().ConfigureAwait(false))
            {
                return plans;
            }

            (pid, pv, priority) = (r.GetString(0), r.GetInt32(1), r.GetString(2));
        }

        var queries = new Dictionary<string, string>
        {
            ["probe"] = $"""
                SELECT j.eligible_at FROM runwick.jobs AS j
                WHERE j.application = 'wl' AND j.queue = 'load' AND j.priority = @priority
                  AND j.state IN ('pending', 'retrying') AND j.eligible_at <= clock_timestamp()
                  {Contract}
                ORDER BY j.eligible_at, j.job_id LIMIT 1
                """,
            ["reserveCandidate"] = $"""
                SELECT j.job_id, j.eligible_at FROM runwick.jobs AS j
                WHERE j.application = 'wl' AND j.queue = 'load' AND j.priority = @priority
                  AND j.state IN ('pending', 'retrying') AND j.eligible_at <= statement_timestamp()
                  {Contract}
                ORDER BY j.eligible_at, j.job_id LIMIT 1
                FOR NO KEY UPDATE OF j SKIP LOCKED
                """,
        };
        foreach (var (name, sql) in queries)
        {
            await using var tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + sql, conn, tx);
            cmd.Parameters.AddWithValue("priority", priority);
            cmd.Parameters.AddWithValue("pid", pid);
            cmd.Parameters.AddWithValue("pv", pv);
            var lines = new List<string>();
            await using (var r = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
            {
                while (await r.ReadAsync().ConfigureAwait(false))
                {
                    lines.Add(r.GetString(0));
                }
            }

            await tx.RollbackAsync().ConfigureAwait(false);
            plans[name] = string.Join('\n', lines);
        }

        return plans;
    }

    /// <summary>
    /// Correctness during a workload (07 §14.1): ReferenceModel snapshot invariants; at most one succeeded attempt per job
    /// and every succeeded attempt matches a succeeded job (no false durable success); handler invocations never exceed
    /// begun executions (no double invocation); every accepted dispatch is stored; drained runs leave no open job;
    /// per-process resources within the declared caps (tickets, control 8 / work 8 connections).
    /// </summary>
    static async Task<JsonObject> CorrectnessAsync(string cs, JsonArray workers, JsonNode? producer, WorkloadSpec spec, RunContext c)
    {
        var violations = new List<string>();
        var model = await SnapshotReader.ReadModelAsync(cs).ConfigureAwait(false);
        violations.AddRange(SnapshotInvariants.Violations(model).Take(20));

        var jobs = model.Jobs.Count;
        var succeededJobs = model.Jobs.Count(j => j.State == "succeeded");
        var succeededAttempts = model.Attempts.Where(a => a.Outcome == "succeeded").GroupBy(a => a.JobId).ToList();
        if (succeededAttempts.Any(g => g.Count() > 1))
        {
            violations.Add($"{succeededAttempts.Count(g => g.Count() > 1)} job(s) with more than one succeeded attempt");
        }

        if (succeededAttempts.Count != succeededJobs)
        {
            violations.Add($"succeeded attempts on {succeededAttempts.Count} jobs but {succeededJobs} succeeded jobs");
        }

        var executions = model.Jobs.Sum(j => j.LifetimeExecutions);
        var invocations = workers.Sum(w => w?["invocations"]?.GetValue<long>() ?? 0);
        if (invocations > executions)
        {
            violations.Add($"{invocations} handler invocations > {executions} begun executions");
        }

        // Outcome-unknown dispatches are resolved by the producer (resent with the same command); one it could not
        // resolve may or may not have applied, so it widens the bound by one and nothing more.
        var accepted = producer?["accepted"]?.GetValue<long>();
        var unresolved = producer?["outcomeUnresolved"]?.GetValue<long>() ?? 0;
        if (accepted is { } n && (jobs < n || jobs > n + unresolved))
        {
            violations.Add($"producer accepted {n} dispatches ({unresolved} unresolved outcomes) but the store holds {jobs} jobs");
        }

        var open = model.Jobs.Count(j => j.State is "pending" or "retrying" or "reserved" or "running");
        if (spec.Drain && c.Marks.ContainsKey("drained") && open != 0)
        {
            violations.Add($"{open} open jobs after drain");
        }

        foreach (var w in workers)
        {
            var entries = w?["maxima"]?["entries"]?.GetValue<long>() ?? 0;
            if (entries > spec.Concurrency)
            {
                violations.Add($"worker held {entries} attempt entries > {spec.Concurrency} tickets");
            }
        }

        foreach (var (app, count) in c.Sampler.Samples.SelectMany(s => s.Connections))
        {
            if ((app.EndsWith(":control", StringComparison.Ordinal) && count > 8) || (app.EndsWith(":work", StringComparison.Ordinal) && count > PgDataSources.WorkPoolMax(new ProducerOptions().MaxConcurrentCalls)))
            {
                violations.Add($"{app}: {count} connections above its cap");
                break;
            }
        }

        return new JsonObject
        {
            ["violationCount"] = violations.Count,
            ["violations"] = new JsonArray([.. violations.Select(v => (JsonNode?)v)]),
            ["jobs"] = jobs,
            ["succeededJobs"] = succeededJobs,
            ["openJobs"] = open,
            ["beganExecutions"] = executions,
            ["handlerInvocations"] = invocations,
            ["jobsInvokedMoreThanOnce"] = workers.Sum(w => w?["counters"]?["jobsInvokedAgain"]?.GetValue<long>() ?? 0),
            ["jobsWithMoreThanOneExecution"] = model.Jobs.Count(j => j.LifetimeExecutions > 1),
        };
    }
}
