using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Tests;

/// <summary>
/// PGX-07 subset for SP-012 (postgres-schema-and-operations §7, 09 §6, 15 §5): <c>EXPLAIN (ANALYZE, BUFFERS)</c> of the
/// three backlog-sensitive reads, ReadinessProbe/2, the Reserve/1 candidate scan and ListUnsupportedBacklog/1, at 10⁵
/// and 10⁶ job rows. The statements are read from the product source at run time, so the plans are of the shipped SQL,
/// not a copy. The rows are bulk-loaded with <c>generate_series</c> (setup only). The bulk load runs alone (its own
/// non-parallel collection), so it neither slows nor is slowed by the timing-sensitive worker cases.
/// </summary>
[Collection("PlanMeasurements")]
[Trait("Item", "SP-012")]
public sealed class ReadinessPlanTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly string[] Queues = ["default", "q1", "q2", "q3"];

    static readonly string[] Priorities = ["high", "medium", "low"];

    // ------------------------------------------------------------------ statements from the product source

    static string SourceFile(string relative)
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "src", "Runwick.Postgres", "Store", relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException("product source not found above the test output folder", relative);
    }

    /// <summary>The <paramref name="index"/>-th raw string literal (0-based) after <paramref name="anchor"/> in a source file.</summary>
    static string Statement(string file, string anchor, int index)
    {
        var text = SourceFile(file);
        var at = text.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"anchor '{anchor}' not found in {file}");
        for (var i = 0; ; i++)
        {
            var open = text.IndexOf("\"\"\"", at, StringComparison.Ordinal);
            var close = text.IndexOf("\"\"\"", open + 3, StringComparison.Ordinal);
            if (i == index)
            {
                return text[(open + 3)..close];
            }

            at = close + 3;
        }
    }

    static string ProbeSql => Statement("PgStore.Readiness.cs", "public Task<ReadySnapshot> ProbeAsync(", 0);

    /// <summary>
    /// Reserve's candidate claim, step C of <c>runwick.reserve_v2</c> (04 Changes 2026-11-02, PF-05 and PF-06 batched claim),
    /// as a plain statement: the INTO list is dropped and each function argument <c>p_x</c> becomes the parameter <c>@x</c>.
    /// </summary>
    static string ReserveCandidateSql => CandidateSql("reserve_v2", "-- C: the candidate claim.");

    /// <summary>
    /// ReserveAndBegin/1's claim, step C of <c>runwick.reserve_and_begin_v1</c> (PF-06 batched claim): the same statement with
    /// <c>LIMIT v_max</c>, which becomes the parameter <c>@max</c>.
    /// </summary>
    static string ClaimCandidateSql => CandidateSql("reserve_and_begin_v1", "-- C: the candidate claim (").Replace("v_max", "@max", StringComparison.Ordinal);

    static string CandidateSql(string function, string marker)
    {
        var text = SqlFile("0010_reserve_and_begin.sql");
        var at = text.IndexOf("CREATE FUNCTION runwick." + function + "(", StringComparison.Ordinal);
        Assert.True(at >= 0, function + " not found in 0010_reserve_and_begin.sql");
        var start = text.IndexOf("SELECT j.job_id", text.IndexOf(marker, at, StringComparison.Ordinal), StringComparison.Ordinal);
        var end = text.IndexOf("SKIP LOCKED", start, StringComparison.Ordinal) + "SKIP LOCKED".Length;
        var sql = Regex.Replace(text[start..end], @"^\s*INTO [^\n]*\n", "", RegexOptions.Multiline);
        return Regex.Replace(sql, @"\bp_(\w+)", "@$1");
    }

    /// <summary>
    /// The plan PL/pgSQL may cache for the batched claim after its first executions (<c>LIMIT</c> a parameter): the
    /// statement prepared with the function's parameter types and explained under <c>plan_cache_mode =
    /// force_generic_plan</c>. Reports the access paths to <c>jobs</c>.
    /// </summary>
    static async Task<List<(string Node, string? Index)>> GenericClaimAccessAsync(DriverFixture f, string application)
    {
        var names = new[]
        {
            ("application", "text"), ("queue", "text"), ("priority", "text"), ("after_job_id", "uuid"), ("after_eligible_at", "timestamptz"),
            ("names", "text[]"), ("versions", "integer[]"), ("profile_ids", "text[]"), ("profile_versions", "integer[]"), ("max", "integer"),
        };
        var sql = ClaimCandidateSql;
        foreach (var ((name, _), i) in names.Select((n, i) => (n, i)).OrderByDescending(x => x.n.Item1.Length))
        {
            sql = Regex.Replace(sql, "@" + name + @"\b", "$$" + (i + 1));
        }

        await using var conn = new NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        using (var prepare = new NpgsqlCommand(
            $"SET plan_cache_mode = force_generic_plan; PREPARE rw_claim_plan({string.Join(", ", names.Select(n => n.Item2))}) AS {sql}", conn))
        {
            await prepare.ExecuteNonQueryAsync();
        }

        // A utility statement takes no bind parameters: the application is inlined as a literal.
        using var cmd = new NpgsqlCommand(
            $"EXPLAIN (FORMAT JSON) EXECUTE rw_claim_plan('{application.Replace("'", "''", StringComparison.Ordinal)}', 'default', 'normal', NULL, NULL, '{{x}}', '{{1}}', '{{p}}', '{{1}}', 32)",
            conn);
        var json = (string)(await cmd.ExecuteScalarAsync())!;
        var access = new List<(string, string?)>();
        Walk(JsonNode.Parse(json)!.AsArray()[0]!["Plan"]!.AsObject(), access);
        return access;

        static void Walk(JsonObject node, List<(string, string?)> access)
        {
            if (node["Relation Name"]?.GetValue<string>() == "jobs")
            {
                access.Add((node["Node Type"]!.GetValue<string>(), node["Index Name"]?.GetValue<string>()));
            }

            foreach (var child in node["Plans"]?.AsArray() ?? [])
            {
                Walk(child!.AsObject(), access);
            }
        }
    }

    static string SqlFile(string name)
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "src", "Runwick.Postgres", "Sql", name);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException("product migration not found above the test output folder", name);
    }

    static string DiagnosticSql => Statement("PgStore.Readiness.cs", "public Task<UnsupportedBacklog> ListUnsupportedBacklogAsync(", 0);

    // ------------------------------------------------------------------ data

    /// <summary>
    /// Adds rows up to <paramref name="total"/>: 80 % succeeded history, 20 % pending spread over 4 queues × 3 lanes, and
    /// 1 % of the pending rows with an unsupported tuple (payload version 9). Eligibility is in the past, oldest first.
    /// Each row gets the generation row of <paramref name="template"/>, a job dispatched through the product, and each
    /// pending row its payload row (history rows are loaded as pruned, <c>payload_retained = false</c>). The rows are
    /// consistent by construction, so the load session skips the per-row foreign-key triggers
    /// (<c>session_replication_role = replica</c>): checking 10⁶ deferred keys one by one dominated the load time.
    /// </summary>
    static async Task LoadAsync(DriverFixture f, Guid template, long from, long total)
    {
        async Task<string> Columns(string table) => string.Join(", ", await DispatchSupport.TextAsync(f, """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'runwick' AND table_name = @t AND column_name NOT IN ('application', 'job_id')
            ORDER BY ordinal_position
            """, ("t", table)));
        var generation = await Columns("job_generations");
        var payload = await Columns("job_payloads");
        await LongExecAsync(f, replica: true, $"""
            WITH ins AS (
                INSERT INTO runwick.jobs (application, job_id, job_name, payload_version, serializer_profile_id, serializer_profile_version,
                                          queue, priority, state, eligible_at, created_at, updated_at, terminal_at, terminal_reason,
                                          payload_retained)
                SELECT @a, gen_random_uuid(), 'test.work',
                       CASE WHEN i % 10 >= 8 AND i % 1000 = 9 THEN 9 ELSE 1 END,
                       @pid, @pv,
                       (ARRAY['default', 'q1', 'q2', 'q3'])[1 + (i % 4)::int],
                       (ARRAY['high', 'medium', 'low'])[1 + ((i / 4) % 3)::int],
                       CASE WHEN i % 10 < 8 THEN 'succeeded' ELSE 'pending' END,
                       t, t, t,
                       CASE WHEN i % 10 < 8 THEN t END,
                       CASE WHEN i % 10 < 8 THEN 'succeeded' END,
                       i % 10 >= 8
                FROM generate_series(@from, @to - 1) AS i
                CROSS JOIN LATERAL (SELECT runwick.ms_floor(now() - interval '1 day' + i * interval '1 ms') AS t) AS x
                RETURNING application, job_id, payload_retained
            ),
            g AS (
                INSERT INTO runwick.job_generations (application, job_id, {generation})
                SELECT ins.application, ins.job_id, {generation}
                FROM ins CROSS JOIN (SELECT * FROM runwick.job_generations WHERE job_id = @tpl) AS tg
            )
            INSERT INTO runwick.job_payloads (application, job_id, {payload})
            SELECT ins.application, ins.job_id, {payload}
            FROM ins CROSS JOIN (SELECT * FROM runwick.job_payloads WHERE job_id = @tpl) AS tp
            WHERE ins.payload_retained
            """, ("a", f.Application), ("pid", RunwickJsonProfile.ProfileId), ("pv", RunwickJsonProfile.ProfileVersion), ("from", from), ("to", total),
            ("tpl", template));
    }

    /// <summary>Setup SQL with no command timeout (the bulk load and VACUUM of 10⁶ rows exceed the 30 s default under load).</summary>
    static async Task LongExecAsync(DriverFixture f, bool replica, string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        if (replica)
        {
            using var set = new NpgsqlCommand("SET session_replication_role = replica", conn);
            await set.ExecuteNonQueryAsync();
        }

        using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = 0 };
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        await cmd.ExecuteNonQueryAsync();
    }

    // ------------------------------------------------------------------ EXPLAIN

    sealed record Plan(string Text, JsonObject Root, long Buffers, double Ms, IReadOnlyList<(string Node, string? Index)> JobsAccess);

    static NpgsqlCommand Bind(NpgsqlCommand cmd, DriverFixture f, IReadOnlyList<JobContract> contracts, string statement)
    {
        cmd.Parameters.AddWithValue("application", f.Application);
        if (statement == "probe")
        {
            var lanes = Queues.SelectMany(q => Priorities.Select(p => (q, p))).ToArray();
            cmd.Parameters.Add(new NpgsqlParameter("lane_queues", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = lanes.Select(l => l.q).ToArray() });
            cmd.Parameters.Add(new NpgsqlParameter("lane_priorities", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = lanes.Select(l => l.p).ToArray() });
        }

        if (statement == "claim")
        {
            cmd.Parameters.AddWithValue("max", IReserveAndBeginStore.MaxJobs);
        }

        if (statement is "reserve" or "claim")
        {
            cmd.Parameters.AddWithValue("queue", "q1");
            cmd.Parameters.AddWithValue("priority", "medium");
            cmd.Parameters.Add(new NpgsqlParameter("after_job_id", NpgsqlDbType.Uuid) { Value = DBNull.Value });
            cmd.Parameters.Add(new NpgsqlParameter("after_eligible_at", NpgsqlDbType.TimestampTz) { Value = DBNull.Value });
        }

        if (statement == "diagnostic")
        {
            cmd.Parameters.AddWithValue("per_lane", 256);
        }
        else
        {
            cmd.Parameters.Add(new NpgsqlParameter("names", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = contracts.Select(c => c.Name).ToArray() });
            cmd.Parameters.Add(new NpgsqlParameter("versions", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = contracts.Select(c => c.PayloadVersion).ToArray() });
            cmd.Parameters.Add(new NpgsqlParameter("profile_ids", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = contracts.Select(c => c.SerializerProfileId).ToArray() });
            cmd.Parameters.Add(new NpgsqlParameter("profile_versions", NpgsqlDbType.Array | NpgsqlDbType.Integer)
            {
                Value = contracts.Select(c => c.SerializerProfileVersion).ToArray(),
            });
        }

        return cmd;
    }

    static async Task<Plan> ExplainAsync(DriverFixture f, IReadOnlyList<JobContract> contracts, string statement, string sql)
    {
        await using var conn = new NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();

        // Reserve's candidate scan takes a row lock: it runs inside a transaction that is rolled back.
        await using var tx = await conn.BeginTransactionAsync();
        string json;
        using (var cmd = Bind(new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, conn, tx), f, contracts, statement))
        {
            json = (string)(await cmd.ExecuteScalarAsync())!;
        }

        var text = new StringBuilder();
        using (var cmd = Bind(new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + sql, conn, tx), f, contracts, statement))
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                text.AppendLine(reader.GetString(0));
            }
        }

        await tx.RollbackAsync();
        var root = JsonNode.Parse(json)!.AsArray()[0]!.AsObject();
        var plan = root["Plan"]!.AsObject();
        var buffers = plan["Shared Hit Blocks"]!.GetValue<long>() + plan["Shared Read Blocks"]!.GetValue<long>();
        var access = new List<(string, string?)>();
        Walk(plan, access);
        return new Plan(text.ToString(), root, buffers, root["Execution Time"]!.GetValue<double>(), access);

        static void Walk(JsonObject node, List<(string, string?)> access)
        {
            if (node["Relation Name"]?.GetValue<string>() == "jobs")
            {
                access.Add((node["Node Type"]!.GetValue<string>(), node["Index Name"]?.GetValue<string>()));
            }

            foreach (var child in node["Plans"]?.AsArray() ?? [])
            {
                Walk(child!.AsObject(), access);
            }
        }
    }

    static void Record(string label, Plan plan)
    {
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine($"== {label}: {plan.Buffers} shared buffers, {plan.Ms:F2} ms");
        output?.WriteLine(plan.Text);
        if (Environment.GetEnvironmentVariable("RUNWICK_PLAN_DIR") is { Length: > 0 } dir)
        {
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, Regex.Replace(label, "[^a-z0-9]+", "-") + ".txt"),
                $"{label}: {plan.Buffers} shared buffers, {plan.Ms:F2} ms{Environment.NewLine}{plan.Text}");
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "PGX-07")]
    public async Task Probe_reserve_and_diagnostic_plans_use_the_ready_index_with_bounded_buffers_at_a_million_rows()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pgx07"));
        var c = await f.OpenClient(configure: r =>
        {
            WorkerHost.RegisterWork(r);
            foreach (var q in Queues.Skip(1))
            {
                r.Queue(q);
            }
        });
        var contracts = c.Contracts;
        var template = (await c.Dispatcher.DispatchAsync(new WorkJob("template"))).JobId.Value;
        var statements = new[] { ("probe", ProbeSql), ("reserve", ReserveCandidateSql), ("claim", ClaimCandidateSql), ("diagnostic", DiagnosticSql) };
        Assert.All(statements, s => Assert.Contains("runwick.jobs", s.Item2, StringComparison.Ordinal));
        Assert.Contains("FOR NO KEY UPDATE OF j SKIP LOCKED", ReserveCandidateSql, StringComparison.Ordinal);
        Assert.Contains("FOR NO KEY UPDATE OF j SKIP LOCKED", ClaimCandidateSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @max", ClaimCandidateSql, StringComparison.Ordinal);

        var results = new Dictionary<(string, long), Plan>();
        long loaded = 0;
        foreach (var size in new long[] { 100_000, 1_000_000 })
        {
            for (var from = loaded; from < size; from += 250_000)
            {
                await LoadAsync(f, template, from, Math.Min(size, from + 250_000));
            }

            await LongExecAsync(f, replica: false, "VACUUM (ANALYZE) runwick.jobs, runwick.job_generations, runwick.job_payloads");
            loaded = size;
            Assert.Equal(size + 1, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
            foreach (var (name, sql) in statements)
            {
                var plan = await ExplainAsync(f, contracts, name, sql);
                Record($"{name} at {size:N0} rows", plan);
                results[(name, size)] = plan;

                // Every access to jobs goes through the partial ready index: no sequential or bitmap scan of the table.
                Assert.NotEmpty(plan.JobsAccess);
                Assert.All(plan.JobsAccess, a =>
                {
                    Assert.Contains(a.Node, new[] { "Index Scan", "Index Only Scan" });
                    Assert.Equal("jobs_ready", a.Index);
                });
            }

            // The batched claim's cached (generic) plan, where LIMIT is a parameter, also uses the ready index.
            var generic = await GenericClaimAccessAsync(f, f.Application);
            TestContext.Current.TestOutputHelper?.WriteLine($"claim generic plan at {size:N0} rows: {string.Join(", ", generic)}");
            Assert.NotEmpty(generic);
            Assert.All(generic, a => Assert.Equal("jobs_ready", a.Index));

            // The real operations agree with the plans' inputs at this size.
            var snapshot = await c.Store.ProbeAsync(f.Application, [.. Queues.SelectMany(q => Priorities.Select(p => new LaneKey(q, p)))],
                contracts, CancellationToken.None);
            Assert.Equal(12, snapshot.Lanes.Count);
            Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync("q1", "medium")).Status);
        }

        // Bounded: independent of the history and backlog size. The probe and the Reserve candidate read a handful of
        // index pages per lane (one more B-tree level at 10⁶ at most); the diagnostic reads its 256-row sample per lane.
        foreach (var (name, _) in statements)
        {
            var small = results[(name, 100_000)].Buffers;
            var large = results[(name, 1_000_000)].Buffers;
            Assert.True(large <= (small * 2) + 64, $"{name}: {small} buffers at 10^5, {large} at 10^6");
        }

        Assert.InRange(results[("probe", 1_000_000)].Buffers, 1, 400);
        Assert.InRange(results[("reserve", 1_000_000)].Buffers, 1, 40);
        Assert.InRange(results[("claim", 1_000_000)].Buffers, 1, 40 + (8 * IReserveAndBeginStore.MaxJobs));
        Assert.InRange(results[("diagnostic", 1_000_000)].Buffers, 1, (12 * 256) + 512);
    }
}

/// <summary>Runs <see cref="ReadinessPlanTests"/> with no other test in parallel.</summary>
[CollectionDefinition("PlanMeasurements", DisableParallelization = true)]
public sealed class PlanMeasurementsCollection;
