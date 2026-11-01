using System.Globalization;
using System.Text.Json.Nodes;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Worker;
using static Runwick.Workloads.G2Orchestrator;

namespace Runwick.Workloads;

/// <summary>The G-2 workload scripts (15 §11; docs/reports/g2-workloads-design.md has the parameters and deviations).</summary>
internal static class G2Workloads
{
    static string I(int v) => v.ToString(CultureInfo.InvariantCulture);

    static string D(double v) => v.ToString(CultureInfo.InvariantCulture);

    static TimeSpan Min(Args a, string name, double fallback) => TimeSpan.FromMinutes(a.Double(name, fallback));

    static JsonObject Latency(JsonNode? worker, string op) => worker?["latencies"]?[op]?.DeepClone() as JsonObject ?? [];

    static DateTimeOffset Mark(G2Context c, string name) => c.Inner.Marks[name];

    // ------------------------------------------------------------------------------------------------ P-04

    /// <summary>
    /// P-04 fairness/CPU (SP-012). Variants (<c>--variants a,b,c,d</c>): (a) queue weights 1:3; (b) lanes 6:3:1 with
    /// lanes kept young (no lane reaches the 60 s aging threshold); (c) lanes 6:3:1 with the lower lanes aged; (d) the
    /// queue-weight run with a 100 ms CPU-bound handler. Each measures the first <c>--admissions</c> Applied
    /// reservations after warm-up, by queue and lane, against the selector's own expected vector.
    /// </summary>
    public static async Task P04Async(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var n = a.Int("admissions", 10_000);
        var horizon = a.Double("lane-horizon-s", 5);
        var aged = a.Double("aged-horizon-s", 120);
        foreach (var variant in (a.OptStr("variants") ?? "a,b,c,d").Split(','))
        {
            var cpu = variant == "d";
            var lanesSpec = variant switch
            {
                "a" or "d" => $"fa/medium:{D(horizon)},fb/medium:{D(horizon)}",
                "b" => $"fl/high:{D(horizon)},fl/medium:{D(horizon)},fl/low:{D(horizon)}",
                "c" => $"fl/high:{D(horizon)},fl/medium:{D(aged)},fl/low:{D(aged)}",
                _ => throw new ArgumentException($"unknown P-04 variant {variant}"),
            };
            var queues = variant is "a" or "d" ? "fa:1,fb:3" : "fl:1";
            var concurrency = cpu ? a.Int("cpu-concurrency", 4) : a.Int("concurrency", 16);
            var handlerMs = cpu ? a.Int("cpu-handler-ms", 100) : 0;
            List<string> worker = ["--queues", queues, "--lane-mode", "weighted", "--concurrency", I(concurrency), "--handler-ms", I(handlerMs), "--handlers", "work"];
            if (cpu)
            {
                worker.Add("--spin");
            }

            var spec = new G2Spec("P-04", variant, [], [[.. worker]],
                ["--mode", "topup", "--job", "work", "--lanes", lanesSpec, "--min-backlog", I(a.Int("min-backlog", 8))],
                concurrency, handlerMs, Drain: true, Capabilities: []);
            var laneKeys = lanesSpec.Split(',').Select(s => s.Split(':')[0]).ToList();
            var ages = laneKeys.ToDictionary(k => k, _ => (Min: double.MaxValue, Max: 0.0), StringComparer.Ordinal);
            var run = await RunOnceAsync(db, root, spec, async c =>
            {
                var store = c.StoreConnection("wl:script");
                await c.WaitAsync(Min(a, "warmup-min", 2)).ConfigureAwait(false);
                if (variant == "c")
                {
                    // The lower lanes must be aged (oldest eligible ≥ 60 s) before the measured admissions start.
                    var agedOk = await c.UntilAsync(async () =>
                    {
                        var rows = await LaneAgesAsync(store).ConfigureAwait(false);
                        return rows.TryGetValue("fl/medium", out var m) && m >= 65 && rows.TryGetValue("fl/low", out var l) && l >= 65;
                    }, Min(a, "aged-wait-min", 15)).ConfigureAwait(false);
                    c.Extra["lowerLanesAgedBeforeMeasure"] = agedOk;
                }

                await c.Inner.ResetAsync().ConfigureAwait(false);
                c.Mark("measureStart");
                var start = Mark(c, "measureStart");
                var reached = await c.UntilAsync(async () =>
                    await ScalarAsync(store, "SELECT count(*) FROM runwick.job_attempts WHERE application = 'wl' AND reserved_at >= @s",
                        ("s", start.UtcDateTime)).ConfigureAwait(false) >= n,
                    Min(a, "max-measure-min", 300), async () =>
                    {
                        foreach (var (lane, age) in await LaneAgesAsync(store).ConfigureAwait(false))
                        {
                            if (ages.TryGetValue(lane, out var mm))
                            {
                                ages[lane] = (Math.Min(mm.Min, age), Math.Max(mm.Max, age));
                            }
                        }
                    }).ConfigureAwait(false);
                c.Extra["admissionTargetReached"] = reached;
                c.Mark("measureEnd");
            }, async c =>
            {
                var store = c.StoreConnection("wl:analysis");
                var rows = await RowsAsync(store, """
                    SELECT j.queue, j.priority, count(*)
                    FROM (SELECT job_id FROM runwick.job_attempts WHERE application = 'wl' AND reserved_at >= @s
                          ORDER BY reserved_at, attempt_id LIMIT @n) x
                    JOIN runwick.jobs j ON j.application = 'wl' AND j.job_id = x.job_id
                    GROUP BY 1, 2
                    """, ("s", Mark(c, "measureStart").UtcDateTime), ("n", n)).ConfigureAwait(false);
                var counts = rows.ToDictionary(r => $"{r[0]}/{r[1]}", r => Convert.ToInt64(r[2], CultureInfo.InvariantCulture), StringComparer.Ordinal);
                var total = counts.Values.Sum();
                var selectorQueues = queues.Split(',').Select(q => (q.Split(':')[0], int.Parse(q.Split(':')[1], CultureInfo.InvariantCulture))).ToList();
                var lowerAged = variant == "c";
                var expected = Simulate(selectorQueues, laneKeys, k => lowerAged && !k.EndsWith("/high", StringComparison.Ordinal) ? 120 : 0, Math.Max(n, 1000));
                var shares = new JsonObject();
                var maxDev = 0.0;
                foreach (var lane in laneKeys)
                {
                    var observed = total == 0 ? 0 : 100.0 * counts.GetValueOrDefault(lane) / total;
                    var dev = observed - expected[lane];
                    maxDev = Math.Max(maxDev, Math.Abs(dev));
                    shares[lane] = new JsonObject
                    {
                        ["admissions"] = counts.GetValueOrDefault(lane), ["observedPct"] = Math.Round(observed, 2),
                        ["expectedPct"] = Math.Round(expected[lane], 2), ["deviationPp"] = Math.Round(dev, 2),
                        ["oldestEligibleAgeMinS"] = ages[lane].Min == double.MaxValue ? null : Math.Round(ages[lane].Min, 1),
                        ["oldestEligibleAgeMaxS"] = Math.Round(ages[lane].Max, 1),
                    };
                }

                var lowerMaxAge = laneKeys.Where(k => !k.EndsWith("/high", StringComparison.Ordinal) && k.StartsWith("fl/", StringComparison.Ordinal))
                    .Select(k => ages[k].Max).DefaultIfEmpty(0).Max();
                c.Extra["variant"] = variant switch
                {
                    "a" => "queue weights 1:3", "b" => "lanes 6:3:1, no aging (lanes kept young)", "c" => "lanes 6:3:1, lower lanes aged",
                    _ => "queue weights 1:3, 100 ms CPU-bound handler",
                };
                c.Extra["admissionsCounted"] = total;
                c.Extra["admissionsRequested"] = n;
                c.Extra["shares"] = shares;
                c.Extra["expectedSource"] = "LaneSelector replay (05 §8.4–§8.5) with the measured lane set" + (lowerAged ? ", lower lanes aged" : ", no lane aged");
                c.Extra["maxDeviationPp"] = Math.Round(maxDev, 2);
                c.Extra["threshold"] = variant is "a" or "b"
                    ? new JsonObject { ["rule"] = "within 2 pp over ≥ 10,000 admissions, aging disabled", ["result"] = maxDev <= 2 && total >= 10_000 ? "pass" : "miss",
                        ["admissionsBelow10000"] = total < 10_000 }
                    : new JsonObject { ["rule"] = variant == "c" ? "aging follows the CAP vectors (recorded)" : "CPU run (recorded)", ["result"] = "recorded" };
                if (variant == "b")
                {
                    c.Extra["agingPossible"] = lowerMaxAge >= 60;
                }

                c.Extra["cpu"] = CpuPerJob(c);
            }).ConfigureAwait(false);
            runs.Add(run);
            summary[variant] = new JsonObject
            {
                ["maxDeviationPp"] = run["extra"]?["maxDeviationPp"]?.DeepClone(), ["threshold"] = run["extra"]?["threshold"]?["result"]?.DeepClone(),
                ["admissions"] = run["extra"]?["admissionsCounted"]?.DeepClone(), ["cpuMsPerJob"] = run["extra"]?["cpu"]?["cpuMsPerJob"]?.DeepClone(),
            };
        }
    }

    static async Task<Dictionary<string, double>> LaneAgesAsync(string store)
    {
        var rows = await RowsAsync(store, """
            SELECT queue || '/' || priority, extract(epoch FROM clock_timestamp() - min(eligible_at))::float8
            FROM runwick.jobs WHERE application = 'wl' AND state IN ('pending', 'retrying') GROUP BY 1
            """).ConfigureAwait(false);
        return rows.ToDictionary(r => (string)r[0]!, r => r[1] is double d ? d : 0, StringComparer.Ordinal);
    }

    /// <summary>Replays the engine's own selector (05 §8.4–§8.5) over a continuously claimable lane set: the expected vector.</summary>
    static Dictionary<string, double> Simulate(List<(string Queue, int Weight)> queues, List<string> lanes, Func<string, double> ageSeconds, int n)
    {
        var selector = new LaneSelector(new PoolSettings("sim", queues, 16, LaneSelectionMode.Weighted, (6, 3, 1)));
        var ready = lanes.ToDictionary(l => new LaneKey(l.Split('/')[0], l.Split('/')[1]), l => TimeSpan.FromSeconds(ageSeconds(l)));
        var counts = lanes.ToDictionary(l => l, _ => 0, StringComparer.Ordinal);
        for (var i = 0; i < n; i++)
        {
            var choice = selector.Choose(ready, TimeSpan.FromSeconds(i))!;
            selector.OnApplied(choice);
            counts[choice.Queue + "/" + choice.Priority]++;
        }

        return counts.ToDictionary(kv => kv.Key, kv => 100.0 * kv.Value / n, StringComparer.Ordinal);
    }

    /// <summary>Worker CPU per finished job over the measured window (worker 5 s samples, store terminal counts).</summary>
    static JsonObject CpuPerJob(G2Context c)
    {
        var start = Mark(c, "measureStart");
        var end = c.Inner.Marks.GetValueOrDefault("measureEnd", DateTimeOffset.UtcNow);
        var cpu = 0L;
        for (var i = 1; i <= c.Spec.Workers.Count; i++)
        {
            var path = Path.Combine(c.Dir, $"worker{i}.samples.jsonl");
            if (!File.Exists(path))
            {
                continue;
            }

            var s = File.ReadAllLines(path).Select(l => JsonNode.Parse(l)!)
                .Select(x => (At: DateTimeOffset.Parse(x["t"]!.GetValue<string>(), CultureInfo.InvariantCulture), Cpu: x["process"]!["cpuMs"]!.GetValue<long>()))
                .Where(x => x.At >= start && x.At <= end).ToList();
            if (s.Count >= 2)
            {
                cpu += s[^1].Cpu - s[0].Cpu;
            }
        }

        var db = c.Inner.Sampler.Samples.Where(x => x.At >= start && x.At <= end).ToList();
        var jobs = db.Count >= 2 ? Analysis.Terminal(db[^1]) - Analysis.Terminal(db[0]) : 0;
        return new JsonObject
        {
            ["workerCpuMs"] = cpu, ["jobs"] = jobs, ["cpuMsPerJob"] = jobs == 0 ? null : Math.Round((double)cpu / jobs, 2),
            ["handlerMs"] = c.Spec.HandlerMs,
            ["engineCpuMsPerJob"] = jobs == 0 ? null : Math.Round(((double)cpu / jobs) - (c.Spec.Workers[0].Contains("--spin") ? c.Spec.HandlerMs : 0), 2),
        };
    }

    // ------------------------------------------------------------------------------------------------ P-05

    /// <summary>
    /// P-05 contention subset (SP-012) at <c>--replicas 1,3,6</c>: (u) unconstrained independent work on queue <c>pu</c>;
    /// (k) shared queue (<c>pc</c>, ceiling 24), job-type (A, B: 8 each) and resource ceilings (C: 2 per key, one hot key
    /// taking <c>--hot-fraction</c> of C), with heterogeneous capability sets (replica i registers handlers {A,B}, {B,C} or
    /// {C,A}). A feeder keeps <c>--backlog</c> jobs claimable. The bottleneck is reported from the store-call latencies,
    /// reserve outcomes, scope occupancy and lock waits, not assumed.
    /// </summary>
    public static async Task P05Async(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var replicas = (a.OptStr("replicas") ?? "1,3,6").Split(',').Select(s => int.Parse(s, CultureInfo.InvariantCulture)).ToList();
        var concurrency = a.Int("concurrency", 16);
        var handlerMs = a.Int("handler-ms", 100);
        string[] common = ["--queue-ceiling", I(a.Int("queue-ceiling", 24)), "--type-ceiling", I(a.Int("type-ceiling", 8)),
            "--resource-ceiling", I(a.Int("resource-ceiling", 2)), "--hot-fraction", D(a.Double("hot-fraction", 0.5))];
        string[][] subsets = [["a", "b"], ["b", "c"], ["c", "a"]];
        foreach (var scenario in (a.OptStr("scenarios") ?? "unconstrained,constrained").Split(','))
        {
            var constrained = scenario == "constrained";
            var perScenario = new JsonObject();
            summary[scenario] = perScenario;
            JsonObject? baseline = null;
            foreach (var n in replicas)
            {
                var workers = Enumerable.Range(0, n).Select(i => (string[])["--queues", constrained ? "pc:1" : "pu:1", "--concurrency", I(concurrency),
                    "--handler-ms", I(handlerMs), "--handlers", constrained ? string.Join(',', n == 1 ? ["a", "b", "c"] : subsets[i % 3]) : "work"]).ToList();
                var spec = new G2Spec("P-05", $"{scenario}-{n}", common, workers,
                    ["--mode", "topup", "--job", constrained ? "mix" : "work", "--lanes", constrained ? "pc/medium:0" : "pu/medium:0",
                        "--min-backlog", I(a.Int("backlog", 500))],
                    concurrency, handlerMs, Drain: true, Capabilities: []);
                var scopes = new Dictionary<string, (int Capacity, int Samples, int AtCapacity, long Held)>(StringComparer.Ordinal);
                var run = await RunOnceAsync(db, root, spec, async c =>
                {
                    var store = c.StoreConnection("wl:script");
                    await c.WaitAsync(Min(a, "warmup-min", 5)).ConfigureAwait(false);
                    await c.Inner.ResetAsync().ConfigureAwait(false);
                    c.Mark("measureStart");
                    await c.WaitAsync(Min(a, "measure-min", 15), async () =>
                    {
                        if (!constrained)
                        {
                            return;
                        }

                        foreach (var r in await RowsAsync(store, """
                            SELECT s.scope_kind || '/' || s.scope_key, s.capacity, count(p.attempt_id)
                            FROM runwick.concurrency_scopes s
                            LEFT JOIN runwick.concurrency_permits p ON p.application = s.application AND p.scope_id = s.scope_id
                            WHERE s.application = 'wl' AND (s.scope_kind <> 'resource' OR s.scope_key = 'hot' OR p.attempt_id IS NOT NULL)
                            GROUP BY 1, 2
                            """).ConfigureAwait(false))
                        {
                            var key = (string)r[0]!;
                            var (cap, held) = (Convert.ToInt32(r[1], CultureInfo.InvariantCulture), Convert.ToInt64(r[2], CultureInfo.InvariantCulture));
                            var prev = scopes.GetValueOrDefault(key, (Capacity: cap, Samples: 0, AtCapacity: 0, Held: 0L));
                            scopes[key] = (cap, prev.Samples + 1, prev.AtCapacity + (held >= cap ? 1 : 0), prev.Held + held);
                        }
                    }).ConfigureAwait(false);
                    c.Mark("measureEnd");
                }, c =>
                {
                    var scopeJson = new JsonObject();
                    foreach (var (k, v) in scopes.Where(kv => kv.Key.StartsWith("queue/pc", StringComparison.Ordinal) || kv.Key.StartsWith("job-type/", StringComparison.Ordinal)
                        || kv.Key == "resource/hot"))
                    {
                        scopeJson[k] = new JsonObject
                        {
                            ["capacity"] = v.Capacity, ["samples"] = v.Samples, ["atCapacityFraction"] = v.Samples == 0 ? 0 : Math.Round((double)v.AtCapacity / v.Samples, 3),
                            ["meanHeld"] = v.Samples == 0 ? 0 : Math.Round((double)v.Held / v.Samples, 2),
                        };
                    }

                    c.Extra["scopes"] = scopeJson;
                    return Task.CompletedTask;
                }).ConfigureAwait(false);
                runs.Add(run);
                var row = ContentionRow(run, n);
                baseline ??= n == 1 ? row : null;
                row["bottleneck"] = Bottleneck(row, baseline);
                perScenario[I(n)] = row;
            }

            double? Tp(int n) => perScenario[I(n)]?["finishesPerSec"]?.GetValue<double?>();
            if (Tp(1) is { } one && one > 0)
            {
                var ratios = new JsonObject();
                foreach (var n in replicas.Where(n => n != 1))
                {
                    ratios[$"{n}v1"] = Tp(n) is { } t ? Math.Round(t / one, 2) : null;
                }

                perScenario["throughputRatios"] = ratios;
                if (!constrained && Tp(3) is { } three)
                {
                    perScenario["threshold"] = new JsonObject
                    {
                        ["rule"] = "three replicas ≥ 2× single-replica throughput before a measured shared bottleneck",
                        ["ratio3v1"] = Math.Round(three / one, 2), ["result"] = three / one >= 2 ? "pass" : "miss",
                    };
                }
            }
        }
    }

    static JsonObject ContentionRow(JsonObject run, int replicas)
    {
        var workers = run["workers"] as JsonArray ?? [];
        long Count(string key) => workers.Sum(w => w?["counters"]?[key]?.GetValue<long>() ?? 0);
        var reserves = workers.Sum(w => (w?["counters"] as JsonObject)?.Where(kv => kv.Key.StartsWith("reserve:", StringComparison.Ordinal))
            .Sum(kv => kv.Value!.GetValue<long>()) ?? 0);
        double Frac(string key) => reserves == 0 ? 0 : Math.Round((double)Count(key) / reserves, 3);
        return new JsonObject
        {
            ["replicas"] = replicas,
            ["finishesPerSec"] = run["window"]?["confirmedFinishesPerSec"]?.DeepClone(),
            ["tReserveMeanMs"] = run["ceilings"]?["tReserveMeanMs"]?.DeepClone(),
            ["tBeginMeanMs"] = run["ceilings"]?["tBeginMeanMs"]?.DeepClone(),
            ["tFinishMeanMs"] = run["ceilings"]?["tFinishMeanMs"]?.DeepClone(),
            ["storeTimeFloorMs"] = run["ceilings"]?["storeTimeFloorMs"]?.DeepClone(),
            ["serialClaimCeilingPerSec"] = run["ceilings"]?["serialClaimCeilingPerSec"]?.DeepClone(),
            ["slotBoundCeilingPerSec"] = run["ceilings"]?["slotBoundCeilingPerSec"]?.DeepClone(),
            ["reserves"] = reserves,
            ["reserveNoWorkFraction"] = Frac("reserve:NoWork"),
            ["reserveCapacityBlockedFraction"] = Frac("reserve:CapacityBlocked"),
            ["lockWaitersMean"] = run["window"]?["lockWaitersMean"]?.DeepClone(),
            ["lockWaitersMax"] = run["window"]?["lockWaitersMax"]?.DeepClone(),
            ["transactionsPerSec"] = run["window"]?["transactionsPerSec"]?.DeepClone(),
            ["scopes"] = run["extra"]?["scopes"]?.DeepClone(),
            ["loadAverage"] = EnvironmentInfo.Read("/proc/loadavg"),
        };
    }

    /// <summary>Names the measured bottleneck: a saturated ceiling, store-call latency growth against one replica, or claim races.</summary>
    static string Bottleneck(JsonObject row, JsonObject? baseline)
    {
        var parts = new List<string>();
        if (row["scopes"] is JsonObject scopes)
        {
            parts.AddRange(scopes.Where(kv => kv.Value?["atCapacityFraction"]?.GetValue<double>() >= 0.5)
                .Select(kv => $"ceiling {kv.Key} at capacity in {kv.Value!["atCapacityFraction"]!.GetValue<double>():P0} of samples"));
        }

        var floor = row["storeTimeFloorMs"]?.GetValue<double>() ?? 0;
        var baseFloor = baseline?["storeTimeFloorMs"]?.GetValue<double>() ?? 0;
        if (baseline is not null && !ReferenceEquals(row, baseline) && baseFloor > 0 && floor >= 1.5 * baseFloor)
        {
            parts.Add($"store-call latency: Reserve+Begin+Finish {floor:F0} ms vs {baseFloor:F0} ms at one replica");
        }

        if (row["reserveCapacityBlockedFraction"]?.GetValue<double>() >= 0.2)
        {
            parts.Add($"{row["reserveCapacityBlockedFraction"]!.GetValue<double>():P0} of Reserves CapacityBlocked");
        }

        if (row["reserveNoWorkFraction"]?.GetValue<double>() >= 0.2)
        {
            parts.Add($"{row["reserveNoWorkFraction"]!.GetValue<double>():P0} of Reserves NoWork (lost claim races or empty probes)");
        }

        if (row["lockWaitersMean"]?.GetValue<double>() >= 1)
        {
            parts.Add($"lock waiters mean {row["lockWaitersMean"]}");
        }

        return parts.Count == 0 ? "no shared bottleneck measured (per-replica serial claim / slot bound)" : string.Join("; ", parts);
    }

    // ------------------------------------------------------------------------------------------------ P-06 rate half

    /// <summary>
    /// P-06 rate half (SP-013): <c>--rate-limit</c> authorizations per <c>--rate-window-ms</c> rolling window (default 100/1 s).
    /// Runs: <c>scopes1</c> (job-type scope) and <c>scopes8</c> (job type + 7 resource scopes) offered <c>--rate</c>
    /// (default 100/s); <c>churn</c> (1 scope) offered <c>--churn-rate</c> against <c>--churn-limit</c> (default auto:
    /// half the authorizations/s scopes1 achieved, so offered Begins exceed the limit on any host). Records Begin
    /// latency with the rate step (Applied and RateLimited), grant rows per authorized job, WAL bytes per grant, and checks
    /// the exact rolling-window bound on every scope.
    /// </summary>
    public static async Task P06rAsync(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var limit = a.Int("rate-limit", 100);
        var windowMs = a.Int("rate-window-ms", 1000);
        double achieved = 0;
        foreach (var label in (a.OptStr("variants") ?? "scopes1,scopes8,churn").Split(','))
        {
            var churn = label == "churn";
            var runLimit = limit;
            if (churn)
            {
                runLimit = a.OptStr("churn-limit") is { } v && v != "auto"
                    ? int.Parse(v, CultureInfo.InvariantCulture)
                    : Math.Clamp((int)Math.Floor(achieved / 2), 1, limit);
            }

            var offered = churn ? a.Double("churn-rate", 2.0 * limit) : a.Double("rate", 100);
            var job = label == "scopes8" ? "rated8" : "rated1";
            var spec = new G2Spec("P-06r", label, ["--rate-limit", I(runLimit), "--rate-window-ms", I(windowMs)],
                [["--queues", "pr:1", "--concurrency", I(a.Int("concurrency", 16)), "--handler-ms", "0", "--handlers", "rated1,rated8"]],
                ["--mode", "poisson", "--job", job, "--rate", D(offered)], a.Int("concurrency", 16), 0, Drain: true, Capabilities: ["rate_admission"]);
            var run = await RunOnceAsync(db, root, spec, async c =>
            {
                await c.WaitAsync(Min(a, "warmup-min", 2)).ConfigureAwait(false);
                await c.Inner.ResetAsync().ConfigureAwait(false);
                c.Mark("measureStart");
                await c.WaitAsync(Min(a, "measure-min", 15)).ConfigureAwait(false);
                c.Mark("measureEnd");
            }, async c =>
            {
                var store = c.StoreConnection("wl:analysis");
                var (s, e) = (Mark(c, "measureStart").UtcDateTime, Mark(c, "measureEnd").UtcDateTime);
                var seconds = (e - s).TotalSeconds;
                var grants = await ScalarAsync(store, "SELECT count(*) FROM runwick.rate_grants WHERE authorized_at BETWEEN @s AND @e", ("s", s), ("e", e)).ConfigureAwait(false);
                var authorized = await ScalarAsync(store, "SELECT count(*) FROM runwick.job_attempts WHERE authorized_at BETWEEN @s AND @e", ("s", s), ("e", e)).ConfigureAwait(false);
                var deferred = await ScalarAsync(store, "SELECT count(*) FROM runwick.job_attempts WHERE outcome = 'admission_deferred' AND finished_at BETWEEN @s AND @e",
                    ("s", s), ("e", e)).ConfigureAwait(false);
                var window = c.Inner.Sampler.Samples.Where(x => x.At >= Mark(c, "measureStart") && x.At <= Mark(c, "measureEnd")).ToList();
                var wal = window.Count >= 2 ? window[^1].WalBytes - window[0].WalBytes : 0;

                // Exact rolling window (10 §1): no scope ever holds more than N grants in any (t − W, t].
                var worst = await RowsAsync(store, $"""
                    SELECT s.scope_kind || '/' || s.scope_key, s.limit_count, max(x.n)
                    FROM (SELECT rate_scope_id, count(*) OVER (PARTITION BY rate_scope_id ORDER BY authorized_at
                              RANGE BETWEEN interval '{windowMs} milliseconds' - interval '1 microsecond' PRECEDING AND CURRENT ROW) AS n
                          FROM runwick.rate_grants) x
                    JOIN runwick.rate_scopes s ON s.rate_scope_id = x.rate_scope_id
                    GROUP BY 1, 2
                    """).ConfigureAwait(false);
                var scopes = new JsonObject();
                foreach (var r in worst)
                {
                    var (cap, max) = (Convert.ToInt64(r[1], CultureInfo.InvariantCulture), Convert.ToInt64(r[2], CultureInfo.InvariantCulture));
                    scopes[(string)r[0]!] = new JsonObject { ["limit"] = cap, ["maxGrantsInAnyWindow"] = max };
                    if (max > cap)
                    {
                        c.Violations.Add($"rate scope {r[0]}: {max} grants in one rolling window > limit {cap}");
                    }
                }

                var worker = (c.Inner.Workers.Count > 0 ? JsonNode.Parse(File.ReadAllText(Path.Combine(c.Dir, "worker1.summary.json"))) : null);
                c.Extra["rateLimit"] = runLimit;
                c.Extra["rateWindowMs"] = windowMs;
                c.Extra["rateScopesPerJob"] = label == "scopes8" ? 8 : 1;
                c.Extra["offeredPerSec"] = offered;
                c.Extra["authorizationsPerSec"] = Math.Round(authorized / seconds, 3);
                c.Extra["rejectedBeginsPerSec"] = Math.Round(deferred / seconds, 3);
                c.Extra["rejectedPerAuthorized"] = authorized == 0 ? null : Math.Round(deferred / authorized, 3);
                c.Extra["grants"] = grants;
                c.Extra["grantRowsPerJob"] = authorized == 0 ? null : Math.Round(grants / authorized, 3);
                c.Extra["walBytesPerGrant"] = grants == 0 ? null : Math.Round(wal / grants, 0);
                c.Extra["walBytesPerAuthorizedJob"] = authorized == 0 ? null : Math.Round(wal / authorized, 0);
                c.Extra["rateDecisionLatency"] = new JsonObject
                {
                    ["beginAll"] = Latency(worker, "begin"), ["beginApplied"] = Latency(worker, "begin@Applied"),
                    ["beginRateLimited"] = Latency(worker, "begin@RateLimited"),
                };
                c.Extra["workerRateDeferrals"] = worker?["rateDeferrals"]?.DeepClone();
                c.Extra["scopes"] = scopes;
                if (label == "scopes1")
                {
                    achieved = authorized / seconds;
                }
            }).ConfigureAwait(false);
            runs.Add(run);
            summary[label] = new JsonObject
            {
                ["rateLimit"] = runLimit, ["offeredPerSec"] = offered, ["authorizationsPerSec"] = run["extra"]?["authorizationsPerSec"]?.DeepClone(),
                ["rejectedBeginsPerSec"] = run["extra"]?["rejectedBeginsPerSec"]?.DeepClone(),
                ["beginP99Ms"] = run["extra"]?["rateDecisionLatency"]?["beginAll"]?["p99Ms"]?.DeepClone(),
                ["grantRowsPerJob"] = run["extra"]?["grantRowsPerJob"]?.DeepClone(), ["walBytesPerGrant"] = run["extra"]?["walBytesPerGrant"]?.DeepClone(),
            };
        }
    }

    // ------------------------------------------------------------------------------------------------ P-06 scheduler half

    /// <summary>
    /// P-06 scheduler half (SP-016). Phase 1: a scheduler process with <c>--schedules</c> (1,000) minute schedules and
    /// <c>--workers</c> (2) worker processes running the occurrences; materialization lag (scheduled instant →
    /// materialized/dispatched, store time) over the measured window, unmaterialized occurrences counted as censored
    /// lags. Phase 2: a per-second catch_up schedule is registered, the scheduler is stopped for
    /// <c>--catchup-occurrences</c> (1,000) seconds and restarted; the catch-up episode is timed until it closes.
    /// </summary>
    public static async Task P06sAsync(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var schedules = a.Int("schedules", 1000);
        var occurrences = a.Int("catchup-occurrences", 1000);
        var workerArgs = Enumerable.Range(0, a.Int("workers", 2)).Select(_ => (string[])["--queues", "ps:1", "--concurrency", I(a.Int("concurrency", 16)),
            "--handler-ms", I(a.Int("handler-ms", 0)), "--handlers", "tick"]).ToList();
        var spec = new G2Spec("P-06s", "steady-catchup", [], workerArgs, null, a.Int("concurrency", 16), a.Int("handler-ms", 0),
            Drain: false, Capabilities: ["scheduler"]);
        string[] catchupArgs = ["--catchup", "--catchup-grace-s", I(a.Int("catchup-grace-s", 30)), "--catchup-limit", I(a.Int("catchup-limit", 10_000))];
        var seedTimeout = Min(a, "seed-timeout-min", 60);
        var run = await RunOnceAsync(db, root, spec, async c =>
        {
            var store = c.StoreConnection("wl:script");
            c.Start("scheduler", "scheduler", ["--schedules", I(schedules)], seedTimeout);
            c.Mark("schedulerReady");
            await c.WaitAsync(Min(a, "warmup-min", 3)).ConfigureAwait(false);
            await c.Inner.ResetAsync().ConfigureAwait(false);
            c.Mark("measureStart");
            await c.WaitAsync(Min(a, "measure-min", 15)).ConfigureAwait(false);
            c.Mark("measureEnd");
            await c.WaitAsync(TimeSpan.FromSeconds(a.Double("lag-tail-s", 60))).ConfigureAwait(false);
            await c.StopAsync("scheduler", TimeSpan.FromMinutes(2)).ConfigureAwait(false);
            c.Mark("schedulerStopped");
            if (occurrences <= 0)
            {
                return;
            }

            // Register the per-second catch_up schedule with a short-lived scheduler, so its cursor is current at the stop.
            c.Start("scheduler-seed", "scheduler", ["--schedules", "0", .. catchupArgs], seedTimeout);
            await c.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await c.StopAsync("scheduler-seed", TimeSpan.FromMinutes(2)).ConfigureAwait(false);
            c.Mark("catchupStopped");
            await c.WaitAsync(TimeSpan.FromSeconds(occurrences)).ConfigureAwait(false);
            c.Mark("catchupRestart");
            c.Start("scheduler-restart", "scheduler", ["--schedules", I(schedules), .. catchupArgs], seedTimeout);
            c.Mark("catchupRestartReady");
            var restart = Mark(c, "catchupRestart").UtcDateTime;
            var done = await c.UntilAsync(async () =>
            {
                var rows = await RowsAsync(store, """
                    SELECT episode_id IS NULL, needs_review, evaluated_through FROM runwick.schedules
                    WHERE application = 'wl' AND schedule_key = 'catchup'
                    """).ConfigureAwait(false);
                return rows.Count == 1 && ((bool)rows[0][1]! || ((bool)rows[0][0]! && rows[0][2] is DateTime t && t >= restart));
            }, Min(a, "catchup-timeout-min", 60)).ConfigureAwait(false);
            c.Extra["catchupCompletedInTime"] = done;
            c.Mark("catchupDone");
            await c.StopAsync("scheduler-restart", TimeSpan.FromMinutes(2)).ConfigureAwait(false);
        }, async c =>
        {
            var store = c.StoreConnection("wl:analysis");
            var (s, e) = (Mark(c, "measureStart").UtcDateTime, Mark(c, "measureEnd").UtcDateTime);
            var stopped = c.Inner.Marks.GetValueOrDefault("schedulerStopped", DateTimeOffset.UtcNow).UtcDateTime;
            var lag = await RowsAsync(store, """
                SELECT count(*),
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY l), percentile_cont(0.95) WITHIN GROUP (ORDER BY l),
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY l), max(l),
                       count(*) FILTER (WHERE coalesced OR covered_through > covered_from)
                FROM (SELECT extract(epoch FROM o.materialized_at - o.scheduled_at)::float8 AS l, o.coalesced, o.covered_from, o.covered_through
                      FROM runwick.schedule_occurrences o JOIN runwick.schedules sc USING (application, schedule_id)
                      WHERE o.application = 'wl' AND o.occurrence_kind = 'scheduled' AND sc.schedule_key LIKE 's%'
                        AND o.scheduled_at BETWEEN @s AND @e AND o.materialized_at <= @stopped) x
                """, ("s", s), ("e", e), ("stopped", stopped)).ConfigureAwait(false);
            var minutes = Enumerable.Range(0, (int)Math.Ceiling((e - s).TotalMinutes) + 1)
                .Select(i => new DateTime(s.Year, s.Month, s.Day, s.Hour, s.Minute, 0, DateTimeKind.Utc).AddMinutes(i))
                .Count(t => t >= s && t <= e);
            var expected = (long)minutes * schedules;
            var materialized = Convert.ToInt64(lag[0][0], CultureInfo.InvariantCulture);
            var missing = Math.Max(0, expected - materialized);

            // Occurrences not materialized by the stop are censored at (stop − instant): the p99 then is a lower bound.
            double? p99 = lag[0][3] is double q ? q : null;
            if (missing > 0 && expected > 0 && missing >= 0.01 * expected)
            {
                p99 = Math.Max(p99 ?? 0, (stopped - e).TotalSeconds);
            }

            c.Extra["schedules"] = schedules;
            c.Extra["materializationLag"] = new JsonObject
            {
                ["occurrencesExpected"] = expected, ["occurrencesMaterialized"] = materialized, ["occurrencesNotMaterializedByStop"] = missing,
                ["coalescedOrRangeCovered"] = Convert.ToInt64(lag[0][5], CultureInfo.InvariantCulture),
                ["p50S"] = Round(lag[0][1]), ["p95S"] = Round(lag[0][2]), ["p99S"] = p99 is { } p ? Math.Round(p, 3) : null, ["maxS"] = Round(lag[0][4]),
                ["p99IsCensoredLowerBound"] = missing > 0 && missing >= 0.01 * expected,
                ["threshold"] = new JsonObject { ["rule"] = "p99 ≤ 3 s", ["result"] = p99 is { } x && x <= 3 && missing == 0 ? "pass" : "miss" },
            };

            // Distinct occurrence identities: one row per (schedule, instant), one job per created occurrence, no extra scheduled jobs.
            var dupInstants = await ScalarAsync(store, """
                SELECT count(*) FROM (SELECT 1 FROM runwick.schedule_occurrences WHERE application = 'wl' AND occurrence_kind = 'scheduled'
                                      GROUP BY schedule_id, scheduled_at HAVING count(*) > 1) x
                """).ConfigureAwait(false);
            var dupJobs = await ScalarAsync(store, """
                SELECT count(*) FROM (SELECT 1 FROM runwick.schedule_occurrences WHERE application = 'wl' AND disposition = 'created'
                                      GROUP BY job_id HAVING count(*) > 1) x
                """).ConfigureAwait(false);
            var created = await ScalarAsync(store, "SELECT count(*) FROM runwick.schedule_occurrences WHERE application = 'wl' AND disposition = 'created'").ConfigureAwait(false);
            var scheduledJobs = await ScalarAsync(store,
                "SELECT count(*) FROM runwick.job_generations WHERE application = 'wl' AND generation = 1 AND created_reason = 'scheduled'").ConfigureAwait(false);
            var orphans = await ScalarAsync(store, """
                SELECT count(*) FROM runwick.schedule_occurrences o WHERE o.application = 'wl' AND o.disposition = 'created'
                  AND NOT EXISTS (SELECT 1 FROM runwick.jobs j WHERE j.application = o.application AND j.job_id = o.job_id)
                """).ConfigureAwait(false);
            if (dupInstants > 0)
            {
                c.Violations.Add($"{dupInstants} (schedule, instant) pairs with more than one occurrence row");
            }

            if (dupJobs > 0)
            {
                c.Violations.Add($"{dupJobs} jobs shared by more than one created occurrence");
            }

            if (Math.Abs(created - scheduledJobs) > 0.5)
            {
                c.Violations.Add($"{created} created occurrences but {scheduledJobs} scheduled jobs");
            }

            if (orphans > 0)
            {
                c.Violations.Add($"{orphans} created occurrences whose job does not exist (lost jobs)");
            }

            c.Extra["identities"] = new JsonObject
            {
                ["createdOccurrences"] = created, ["scheduledJobs"] = scheduledJobs, ["duplicateInstants"] = dupInstants, ["jobsSharedByOccurrences"] = dupJobs,
                ["occurrencesWithoutJob"] = orphans,
            };

            if (c.Inner.Marks.TryGetValue("catchupRestart", out var restartMark))
            {
                var restart = restartMark.UtcDateTime;
                var stop = Mark(c, "catchupStopped").UtcDateTime;
                var cu = await RowsAsync(store, """
                    SELECT count(*), min(o.materialized_at), max(o.materialized_at), min(o.scheduled_at), max(o.scheduled_at),
                           max(extract(epoch FROM o.materialized_at - o.scheduled_at))::float8,
                           (SELECT max(n) FROM (SELECT count(*) AS n FROM runwick.schedule_occurrences o2
                                                WHERE o2.application = 'wl' AND o2.schedule_id = sc.schedule_id AND o2.materialized_at >= @r
                                                GROUP BY o2.command_id) y),
                           sc.needs_review, sc.review_reason
                    FROM runwick.schedules sc
                    LEFT JOIN runwick.schedule_occurrences o ON o.application = sc.application AND o.schedule_id = sc.schedule_id
                         AND o.materialized_at >= @r AND o.scheduled_at <= @r
                    WHERE sc.application = 'wl' AND sc.schedule_key = 'catchup'
                    GROUP BY sc.schedule_id, sc.needs_review, sc.review_reason
                    """, ("r", restart)).ConfigureAwait(false);
                if (cu.Count == 1)
                {
                    var row = cu[0];
                    var emitted = Convert.ToInt64(row[0], CultureInfo.InvariantCulture);
                    var maxBatch = row[6] is null ? 0 : Convert.ToInt64(row[6], CultureInfo.InvariantCulture);
                    if (maxBatch > ScheduleBounds.MaxBatch)
                    {
                        c.Violations.Add($"catch-up materialized {maxBatch} occurrences in one command > {ScheduleBounds.MaxBatch}");
                    }

                    var ready = Mark(c, "catchupRestartReady").UtcDateTime;
                    c.Extra["catchUp"] = new JsonObject
                    {
                        ["occurrencesRequested"] = occurrences,
                        ["downtimeS"] = Math.Round((restart - stop).TotalSeconds, 1),
                        ["occurrencesEmitted"] = emitted,
                        ["firstScheduledAt"] = (row[3] as DateTime?)?.ToString("O", CultureInfo.InvariantCulture),
                        ["lastScheduledAt"] = (row[4] as DateTime?)?.ToString("O", CultureInfo.InvariantCulture),
                        ["restartToReadyS"] = Math.Round((ready - restart).TotalSeconds, 1),
                        ["readyToFirstMaterializedS"] = row[1] is DateTime f ? Math.Round((f - ready).TotalSeconds, 1) : null,
                        ["durationFirstToLastS"] = row[1] is DateTime f2 && row[2] is DateTime l2 ? Math.Round((l2 - f2).TotalSeconds, 1) : null,
                        ["restartToLastMaterializedS"] = row[2] is DateTime l ? Math.Round((l - restart).TotalSeconds, 1) : null,
                        ["maxOccurrencesPerCommand"] = maxBatch,
                        ["maxLagS"] = Round(row[5]),
                        ["needsReview"] = row[7] is true,
                        ["reviewReason"] = row[8] as string,
                        ["bound"] = $"≤ {ScheduleBounds.MaxBatch} occurrences per command, 250 ms pacing between batches, episode limit {a.Int("catchup-limit", 10_000)}",
                    };
                }

                c.Extra["minuteSchedulesAfterRestart"] = new JsonObject
                {
                    ["misfireRangesCovered"] = await ScalarAsync(store, """
                        SELECT count(*) FROM runwick.schedule_occurrences o JOIN runwick.schedules sc USING (application, schedule_id)
                        WHERE o.application = 'wl' AND sc.schedule_key LIKE 's%' AND o.materialized_at >= @r AND o.covered_through > o.covered_from
                        """, ("r", restart)).ConfigureAwait(false),
                };
            }

            var schedulerSummaries = new JsonObject();
            foreach (var name in new[] { "scheduler", "scheduler-seed", "scheduler-restart" })
            {
                var path = Path.Combine(c.Dir, name + ".summary.json");
                schedulerSummaries[name] = File.Exists(path) ? JsonNode.Parse(File.ReadAllText(path)) : null;
            }

            c.Extra["schedulers"] = schedulerSummaries;
        }).ConfigureAwait(false);
        runs.Add(run);
        summary["lagP99S"] = run["extra"]?["materializationLag"]?["p99S"]?.DeepClone();
        summary["lagThreshold"] = run["extra"]?["materializationLag"]?["threshold"]?["result"]?.DeepClone();
        summary["catchUp"] = run["extra"]?["catchUp"]?.DeepClone();
        summary["leaseLosses"] = run["correctness"]?["leaseLosses"]?.DeepClone();
    }

    static double? Round(object? v) => v is double d ? Math.Round(d, 3) : null;

    // ------------------------------------------------------------------------------------------------ P-09

    /// <summary>
    /// P-09 outbox (SP-017): business writers stage <c>--rate</c> (100) messages/s with a 1 KiB intent on a PostgreSQL
    /// source database separate from the store; one relay process with <c>--slots</c> (4). Phase 1 measures confirmed
    /// deliveries/s and source-visible-ready (commit returned) → delivered-mark latency. Phase 2 loses
    /// <c>--mark-loss</c> of delivered marks after the destination applied them for <c>--loss-min</c>; the drain then
    /// proves every message has exactly one delivery mapping and one job.
    /// </summary>
    public static async Task P09Async(TestDatabase db, string root, Args a, JsonArray runs, JsonObject summary)
    {
        var spec = new G2Spec("P-09", "relay", [], [], null, 0, 0, Drain: false, Capabilities: [], OutboxSource: true);
        var sessions = new Dictionary<string, long>(StringComparer.Ordinal);
        var run = await RunOnceAsync(db, root, spec, async c =>
        {
            var source = c.SourceConnection("wl:script");
            async Task SampleSessions()
            {
                foreach (var r in await RowsAsync(source, """
                    SELECT coalesce(application_name, ''), count(*) FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1
                    """).ConfigureAwait(false))
                {
                    var k = (string)r[0]!;
                    sessions[k] = Math.Max(sessions.GetValueOrDefault(k), Convert.ToInt64(r[1], CultureInfo.InvariantCulture));
                }
            }

            c.Start("relay", "relay", ["--slots", I(a.Int("slots", 4))], TimeSpan.FromMinutes(3), source: true);
            c.Start("writer", "stage", ["--rate", D(a.Double("rate", 100)), "--writers", I(a.Int("writers", 8)), "--payload", I(a.Int("payload", 1024))],
                TimeSpan.FromMinutes(3), source: true);
            await c.WaitAsync(Min(a, "warmup-min", 1), SampleSessions).ConfigureAwait(false);
            c.Mark("measureStart");
            await c.WaitAsync(Min(a, "measure-min", 15), SampleSessions).ConfigureAwait(false);
            c.Mark("measureEnd");
            var loss = a.Double("mark-loss", 0.05);
            if (loss > 0 && a.Double("loss-min", 3) > 0)
            {
                await c.Children["relay"].SendAsync("loss " + D(loss)).ConfigureAwait(false);
                c.Mark("lossStart");
                await c.WaitAsync(Min(a, "loss-min", 3), SampleSessions).ConfigureAwait(false);
                await c.Children["relay"].SendAsync("loss 0").ConfigureAwait(false);
                c.Mark("lossEnd");
            }

            await c.StopAsync("writer", TimeSpan.FromMinutes(5)).ConfigureAwait(false);
            c.Mark("writerStopped");
            var drained = await c.UntilAsync(async () => await ScalarAsync(source,
                "SELECT count(*) FROM runwick_outbox.outbox_messages WHERE state IN ('pending', 'leased')").ConfigureAwait(false) == 0,
                Min(a, "drain-min", 60), SampleSessions).ConfigureAwait(false);
            c.Extra["sourceDrained"] = drained;
            if (drained)
            {
                c.Mark("drained");
            }
            else
            {
                // Backlog, not loss: the identity checks below still run over everything delivered.
                c.Failure = $"source not drained within {a.Double("drain-min", 60)} min";
            }

            await c.StopAsync("relay", TimeSpan.FromMinutes(2)).ConfigureAwait(false);
        }, async c =>
        {
            var store = c.StoreConnection("wl:analysis");
            var source = c.SourceConnection("wl:analysis");
            var commits = ReadCsv(Path.Combine(c.Dir, "writer.commits.csv")).ToDictionary(r => Guid.Parse(r[0]), r => long.Parse(r[1], CultureInfo.InvariantCulture));
            var marks = ReadCsv(Path.Combine(c.Dir, "relay.marks.csv")).ToList();
            var confirmed = new Dictionary<Guid, (long At, Guid Job)>();
            var lostMarks = new List<(Guid Message, Guid Job)>();
            foreach (var m in marks)
            {
                if (m[0] == "lost")
                {
                    lostMarks.Add((Guid.Parse(m[1]), Guid.Parse(m[3])));
                }
                else if (!confirmed.ContainsKey(Guid.Parse(m[0])))
                {
                    confirmed[Guid.Parse(m[0])] = (long.Parse(m[1], CultureInfo.InvariantCulture), Guid.Parse(m[2]));
                }
            }

            var (s, e) = (Mark(c, "measureStart").ToUnixTimeMilliseconds(), Mark(c, "measureEnd").ToUnixTimeMilliseconds());
            var lat = commits.Where(kv => kv.Value >= s && kv.Value <= e)
                .Select(kv => confirmed.TryGetValue(kv.Key, out var cf) ? (double?)(cf.At - kv.Value) : null).ToList();
            var known = lat.Where(x => x is not null).Select(x => x!.Value).OrderBy(x => x).ToList();
            double Q(double q) => known.Count == 0 ? 0 : known[Math.Min(known.Count - 1, (int)Math.Ceiling(q * known.Count) - 1)];
            var deliveriesInWindow = confirmed.Values.Count(v => v.At >= s && v.At <= e);
            var seconds = (e - s) / 1000.0;
            var unconfirmedInWindow = lat.Count(x => x is null);
            var p99 = unconfirmedInWindow > 0.01 * Math.Max(1, lat.Count) ? double.PositiveInfinity : Q(0.99);
            c.Extra["phase1"] = new JsonObject
            {
                ["seconds"] = Math.Round(seconds, 1),
                ["committedPerSec"] = Math.Round(lat.Count / seconds, 2),
                ["confirmedDeliveriesPerSec"] = Math.Round(deliveriesInWindow / seconds, 2),
                ["latencySamples"] = known.Count, ["unconfirmedAtEnd"] = unconfirmedInWindow,
                ["readyToConfirmP50Ms"] = Q(0.5), ["readyToConfirmP95Ms"] = Q(0.95),
                ["readyToConfirmP99Ms"] = double.IsInfinity(p99) ? null : p99, ["readyToConfirmMaxMs"] = known.Count == 0 ? 0 : known[^1],
                ["thresholds"] = new JsonObject
                {
                    ["deliveriesPerSec"] = deliveriesInWindow / seconds >= 100 ? "pass" : "miss",
                    ["p99"] = !double.IsInfinity(p99) && p99 <= 2000 ? "pass" : "miss",
                },
            };

            // Correctness across both databases: every committed message is in the source, delivered, mapped once, with one job.
            var sourceRows = (await RowsAsync(source, "SELECT message_id, state, delivered_job_id FROM runwick_outbox.outbox_messages").ConfigureAwait(false))
                .ToDictionary(r => (Guid)r[0]!, r => (State: (string)r[1]!, Job: r[2] as Guid?));
            var mappings = (await RowsAsync(store, "SELECT message_id, job_id FROM runwick.outbox_deliveries WHERE application = 'wl'").ConfigureAwait(false))
                .Select(r => ((Guid)r[0]!, (Guid)r[1]!)).ToList();
            var mappingByMessage = mappings.GroupBy(m => m.Item1).ToDictionary(g => g.Key, g => g.Select(x => x.Item2).ToList());
            var jobs = await ScalarAsync(store, "SELECT count(*) FROM runwick.jobs WHERE application = 'wl'").ConfigureAwait(false);
            var missingInSource = commits.Keys.Count(k => !sourceRows.ContainsKey(k));
            var blocked = sourceRows.Count(r => r.Value.State == "blocked");
            var backlog = sourceRows.Count(r => r.Value.State is "pending" or "leased");
            var deliveredRows = sourceRows.Where(r => r.Value.State == "delivered").ToList();
            var secondMappings = mappingByMessage.Count(kv => kv.Value.Count > 1);
            var distinctJobs = mappings.Select(m => m.Item2).Distinct().Count();
            var unknownMappings = mappingByMessage.Keys.Count(k => !sourceRows.ContainsKey(k));
            var deliveredWithoutMapping = deliveredRows.Count(r => !mappingByMessage.ContainsKey(r.Key));
            var mismatched = deliveredRows.Count(r => r.Value.Job is { } j && mappingByMessage.TryGetValue(r.Key, out var js) && js[0] != j);
            var lostRemapped = lostMarks.Select(l => l.Message).Distinct().Count(mid =>
                !mappingByMessage.TryGetValue(mid, out var js) || js.Count != 1 || lostMarks.Any(l => l.Message == mid && l.Job != js[0])
                || (confirmed.TryGetValue(mid, out var cf) && cf.Job != js[0]));
            void V(bool bad, string text)
            {
                if (bad)
                {
                    c.Violations.Add(text);
                }
            }

            V(missingInSource > 0, $"{missingInSource} committed messages missing from the source");
            V(blocked > 0, $"{blocked} source messages blocked");
            V(secondMappings > 0, $"{secondMappings} messages with more than one delivery mapping");
            V(unknownMappings > 0, $"{unknownMappings} delivery mappings for messages the source does not hold");
            V(deliveredWithoutMapping > 0, $"{deliveredWithoutMapping} delivered source messages without a destination mapping");
            V(c.Extra["sourceDrained"]?.GetValue<bool>() == true && mappings.Count != sourceRows.Count,
                $"{mappings.Count} delivery mappings for {sourceRows.Count} source messages after the drain");
            V(distinctJobs != mappings.Count, $"{mappings.Count} mappings share {distinctJobs} jobs");
            V(Math.Abs(jobs - mappings.Count) > 0.5, $"{jobs} destination jobs for {mappings.Count} delivery mappings (a second job)");
            V(mismatched > 0, $"{mismatched} delivered marks whose job differs from the destination mapping");
            V(lostRemapped > 0, $"{lostRemapped} lost-mark messages redelivered to a different or second mapping");
            c.Extra["deliveryIdentities"] = new JsonObject
            {
                ["committed"] = commits.Count, ["sourceMessages"] = sourceRows.Count, ["delivered"] = deliveredRows.Count, ["blocked"] = blocked,
                ["pendingOrLeasedAtEnd"] = backlog, ["mappings"] = mappings.Count,
                ["destinationJobs"] = jobs, ["messagesWithSecondMapping"] = secondMappings, ["markMismatches"] = mismatched,
            };
            c.Extra["lostMarks"] = new JsonObject
            {
                ["injected"] = lostMarks.Count, ["messages"] = lostMarks.Select(l => l.Message).Distinct().Count(),
                ["redeliveredToSameMapping"] = lostMarks.Select(l => l.Message).Distinct().Count(mid => confirmed.ContainsKey(mid)) ,
                ["secondMappings"] = lostRemapped,
                ["rate"] = a.Double("mark-loss", 0.05),
            };
            c.Extra["sourceSessionsMax"] = new JsonObject(sessions.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value)));
            c.Extra["relay"] = File.Exists(Path.Combine(c.Dir, "relay.summary.json")) ? JsonNode.Parse(File.ReadAllText(Path.Combine(c.Dir, "relay.summary.json"))) : null;
            c.Extra["writer"] = File.Exists(Path.Combine(c.Dir, "writer.summary.json")) ? JsonNode.Parse(File.ReadAllText(Path.Combine(c.Dir, "writer.summary.json"))) : null;
            c.Extra["sevenDayCutoff"] = new JsonObject
            {
                ["run"] = false,
                ["reason"] = "No cheap time seam: staged_at/AcceptUntil come from the source database clock (clock_timestamp()) and the destination "
                    + "checks the horizon against its own database time; the relay TimeProvider governs neither. Controlling both needs a faketime "
                    + "PostgreSQL for source and destination (07 I-9) plus a 7-day step. The cutoff itself is covered by OUTBOX-04 with a short horizon.",
            };
        }).ConfigureAwait(false);
        runs.Add(run);
        summary["phase1"] = run["extra"]?["phase1"]?.DeepClone();
        summary["lostMarks"] = run["extra"]?["lostMarks"]?.DeepClone();
        summary["sevenDayCutoff"] = "not run (see runs[0].extra.sevenDayCutoff)";
    }

    static IEnumerable<string[]> ReadCsv(string path) =>
        File.Exists(path) ? File.ReadLines(path).Where(l => l.Length > 0).Select(l => l.Split(',')) : [];
}
