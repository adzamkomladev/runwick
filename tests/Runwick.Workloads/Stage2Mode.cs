using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Registration;
using Runwick.Serialization;
using Runwick.Sqlite.Migrations;

namespace Runwick.Workloads;

/// <summary>
/// <c>stage2</c>: the Stage 2 experiments of the queue throughput plan (docs/plans/queue-throughput-engineering-plan.md). Tooling only; no product code.
/// <c>--part dispatch</c> times, one caller at a time with fresh operation and job IDs, the public dispatch entry point, the preparation step alone,
/// dispatch of a command prepared outside the timer, and the raw store call, for single jobs and for batches, and reports per operation wall time,
/// process CPU, allocated bytes and store-container CPU. The arms rotate order every round. <c>--part worker</c> is in <see cref="Stage2Worker"/>.
/// Usage: <c>stage2 --part dispatch --store postgres|redis|sqlite [--redis host:port] [--conn admin-cs] [--container name] [--n 1500] [--batch 100]
/// [--batch-n 30] [--rounds 5] [--payload 1024] [--out file.json]</c>.
/// </summary>
internal static class Stage2Mode
{
    public static Task<int> RunAsync(Args a) => (a.OptStr("part") ?? "dispatch") switch
    {
        "dispatch" => DispatchPartAsync(a),
        "worker" => Stage2Worker.RunAsync(a),
        "redis-replay" => RedisReplayAsync(a),
        "concurrent" => ConcurrentPartAsync(a),
        _ => Task.FromResult(2),
    };

    /// <summary>One configured store, ready to take dispatches; disposes its SQLite directory.</summary>
    internal sealed class Setup : IAsyncDisposable
    {
        public required ServiceProvider Provider { get; init; }
        public string? SqliteDir { get; init; }
        public Func<Task<JsonObject>>? Counters { get; init; }
        public async ValueTask DisposeAsync()
        {
            await Provider.DisposeAsync().ConfigureAwait(false);
            if (SqliteDir is not null)
            {
                SqliteConnection.ClearAllPools();
                try { Directory.Delete(SqliteDir, recursive: true); } catch (IOException) { }
            }
        }
    }

    /// <summary>Fresh store (new database, new file or flushed Redis), one application <c>bench</c>, queue <c>load</c>, no-op handler.</summary>
    internal static async Task<Setup> BuildAsync(Args a, string store, Action<WorkerOptions>? worker = null, int slots = 16, bool withWorker = false, Metrics? timing = null)
    {
        Action<RunwickBuilder> use;
        string? dir = null;
        Func<Task<JsonObject>>? counters = null;
        var producerCalls = a.Int("producer-calls", 2);
        if (store == "sqlite")
        {
            dir = Directory.CreateDirectory(Path.Combine(a.OptStr("sqlite-dir") ?? Path.GetTempPath(), "bench-runwick-s2-" + Guid.NewGuid().ToString("N"))).FullName;
            var cs = new SqliteConnectionStringBuilder { DataSource = Path.Combine(dir, "runwick.db"), Pooling = false }.ConnectionString;
            await RunwickSqliteMigrations.MigrateAsync(cs).ConfigureAwait(false);
            use = r => r.UseSqlite(cs);
        }
        else if (store == "redis")
        {
            var cfg = a.OptStr("redis") ?? "127.0.0.1:56392";
            using (var admin = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(cfg + ",allowAdmin=true").ConfigureAwait(false))
            {
                foreach (var server in admin.GetServers().Where(s => s.IsConnected && !s.IsReplica))
                {
                    await server.FlushAllDatabasesAsync().ConfigureAwait(false);
                }
            }

            await RunwickRedisMigrations.MigrateAsync(cfg, new RedisOptions { Persistence = RedisPersistence.AofAlways }).ConfigureAwait(false);
            counters = () => BenchMode.RedisCountersAsync(cfg);
            use = r => r.UseRedis(cfg, o => o.Persistence = RedisPersistence.AofAlways);
        }
        else
        {
            var adminCs = a.OptStr("conn") ?? "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres";
            var database = a.OptStr("db") ?? "bench_runwick_s2";
            var admin = new NpgsqlConnectionStringBuilder(adminCs) { ApplicationName = "bench:admin", Pooling = false }.ConnectionString;
            await G2Orchestrator.ExecAsync(admin, $"DROP DATABASE IF EXISTS {database} WITH (FORCE)").ConfigureAwait(false);
            await G2Orchestrator.ExecAsync(admin, $"CREATE DATABASE {database}").ConfigureAwait(false);
            await RunwickPostgresMigrations.MigrateAsync(new NpgsqlConnectionStringBuilder(adminCs)
                { Database = database, ApplicationName = "bench:migrate", Pooling = false }.ConnectionString).ConfigureAwait(false);
            var cs = new NpgsqlConnectionStringBuilder(adminCs)
            {
                Database = database, ApplicationName = "bench-runwick", Pooling = true, MaxPoolSize = Math.Max(100, slots + 16),
                Options = a.Has("trace") ? "-c track_functions=all" : null,
            }.ConnectionString;
            var counterCs = new NpgsqlConnectionStringBuilder(adminCs) { Database = database, ApplicationName = "bench:counters", Pooling = false }.ConnectionString;
            counters = () => BenchMode.PostgresCountersAsync(counterCs, database);
            use = r => r.UsePostgres(cs, o => o.GroupProducerAdmission = !a.Has("no-pg-group"));
        }

        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton(new LoadBehaviour(TimeSpan.Zero));
        sc.AddSingleton(new G2Behaviour(TimeSpan.Zero, Spin: false));
        sc.AddRunwick(r =>
        {
            r.Application("bench");
            use(r);
            LoadRegistration.Register(r, typeCeiling: null);
            r.AddHandler<LoadJob, LoadHandler>();
            r.Producer(o => o.MaxConcurrentCalls = producerCalls);
            if (withWorker)
            {
                r.AddWorker(w => w.MaxOutstandingAttempts(slots + 2).Pool("main", p => p.Queues(LoadRegistration.Queue).FixedConcurrency(slots))
                    .Configure(o => { o.RecordResourceUsage = true; worker?.Invoke(o); }));
            }
        });
        if (timing is not null)
        {
            // The same decoration on both worker arms: it records each claim's and finish's batch size and duration.
            var inner = sc.Last(d => d.ServiceType == typeof(IRunwickStore));
            sc.AddSingleton<IRunwickStore>(sp => new TimingStore(
                inner.ImplementationFactory is { } f ? (IRunwickStore)f(sp) : inner.ImplementationInstance as IRunwickStore ?? (IRunwickStore)ActivatorUtilities.CreateInstance(sp, inner.ImplementationType!), timing));
        }

        return new Setup { Provider = sc.BuildServiceProvider(), SqliteDir = dir, Counters = counters };
    }

    // ---- Part A: dispatch ----------------------------------------------------------------------------------------------------------------------

    sealed record ArmResult(string Arm, int Ops, double MeanMs, double P50Ms, double P99Ms, double CpuMsPerOp, double AllocBytesPerOp, double StoreCpuMsPerOp,
        JsonObject Phases, JsonObject? Counters);

    static async Task<int> DispatchPartAsync(Args a)
    {
        var store = a.OptStr("store") ?? "postgres";
        var n = a.Int("n", store == "postgres" ? 600 : 1500);
        var batch = a.Int("batch", 100);
        var batchN = a.Int("batch-n", store == "postgres" ? 15 : 30); // batches per arm
        var rounds = a.Int("rounds", 5);
        var payload = a.Int("payload", 1024);
        var container = a.OptStr("container");
        await using var setup = await BuildAsync(a, store).ConfigureAwait(false);
        var sp = setup.Provider;
        var dispatcher = sp.GetRequiredService<JobDispatcher>();
        var preparer = sp.GetRequiredService<DispatchPreparer>();
        var runtime = sp.GetRequiredService<RunwickRuntime>();
        var registry = sp.GetRequiredService<JobRegistry>();
        var driver = sp.GetRequiredService<IRunwickStore>();
        var phaseMetrics = new Metrics();
        using var phaseListener = PhaseListener.Attach(phaseMetrics);
        await runtime.EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
        long seq = 0;
        LoadJob Job() => LoadJob.Create(Interlocked.Increment(ref seq), payload);
        var storeId = runtime.StoreId;
        var application = registry.Application;

        DispatchItem ItemOf(PreparedDispatch p) => new(p.Intent, p.Scopes, p.Digests) { Rates = preparer.RatesFor(p.Intent) };
        StoreCommand<DispatchRequest> CommandOf(PreparedDispatch p) =>
            new(p.Application, p.OperationId.Value, p.NotAfter, new DispatchRequest(ItemOf(p), p.StoreId, p.Digests.Command));
        StoreCommand<DispatchBatchRequest> BatchCommandOf(PreparedBatch p) =>
            new(p.Application, p.OperationId.Value, p.NotAfter, new DispatchBatchRequest(
                [.. p.Items.Select((intent, i) => new DispatchItem(intent, p.Scopes[i], p.ItemDigests[i]) { Rates = preparer.RatesFor(intent) })],
                p.ContinueOnConflict, p.StoreId, p.CommandDigest));
        List<BatchDispatchItem> Items() => [.. Enumerable.Range(0, batch).Select(_ => new BatchDispatchItem(Job()))];

        // Warm every path once so JIT and connection setup are out of the timed arms.
        for (var i = 0; i < 20; i++)
        {
            await dispatcher.DispatchAsync(Job()).ConfigureAwait(false);
        }

        await dispatcher.DispatchManyAsync(Items()).ConfigureAwait(false);

        var cgroup = container is null ? null : CgroupCpuFile(container);
        double StoreCpuMs() => cgroup is null ? 0 : ReadCgroupUsec(cgroup) / 1000.0;
        var proc = Process.GetCurrentProcess();
        double ProcCpuMs() { proc.Refresh(); return proc.TotalProcessorTime.TotalMilliseconds; }

        // Each arm: setupChunk builds work outside the timer (fresh IDs every time), run times exactly the part under test.
        async Task<ArmResult> Measure(string arm, int ops, int chunk, Func<int, Func<Task>[]> setupChunk, bool counters = true)
        {
            var lat = new LatencyReservoir();
            phaseMetrics.ResetLatencies();
            var before = counters && setup.Counters is not null ? await setup.Counters().ConfigureAwait(false) : null;
            double wall = 0, cpu = 0, scpu = 0;
            long alloc = 0;
            for (var done = 0; done < ops; done += chunk)
            {
                var work = setupChunk(Math.Min(chunk, ops - done));
                GC.Collect(); // keep a collection triggered by setup allocations out of the timed slice
                var a0 = GC.GetTotalAllocatedBytes(precise: true);
                var c0 = ProcCpuMs();
                var s0 = StoreCpuMs();
                var w0 = Stopwatch.GetTimestamp();
                foreach (var op in work)
                {
                    var t0 = Stopwatch.GetTimestamp();
                    await op().ConfigureAwait(false);
                    lat.Add(Stopwatch.GetElapsedTime(t0).TotalMilliseconds);
                }

                wall += Stopwatch.GetElapsedTime(w0).TotalMilliseconds;
                cpu += ProcCpuMs() - c0;
                scpu += StoreCpuMs() - s0;
                alloc += GC.GetTotalAllocatedBytes(precise: true) - a0;
            }

            JsonObject? delta = null;
            if (before is not null)
            {
                var after = await setup.Counters!().ConfigureAwait(false);
                delta = new JsonObject();
                foreach (var (k, v) in after)
                {
                    var d = (double?)v - (double?)before[k] ?? (double?)v ?? 0;
                    if (d != 0 && (k.StartsWith("calls:", StringComparison.Ordinal) || k is "commandCalls" or "commandUsec" or "cpuSeconds" or "transactions" or "rowsWritten" or "walBytes" || k.StartsWith("function", StringComparison.Ordinal)))
                    {
                        delta[k] = Math.Round(d / ops, 3); // per operation
                    }
                }
            }

            var s = lat.Summarize();
            return new ArmResult(arm, ops, wall / ops, s.P50Ms, s.P99Ms, cpu / ops, (double)alloc / ops, scpu / ops, phaseMetrics.LatenciesJson(), delta);
        }

        Func<int, Func<Task>[]> Singles(Func<PreparedDispatch, Func<Task>> make) =>
            c => [.. Enumerable.Range(0, c).Select(_ => make(dispatcher.Prepare(Job())))];

        var all = new List<(string Arm, int Round, ArmResult Result)>();
        var singleArms = new (string Name, Func<Task<ArmResult>> Run)[]
        {
            ("public-single", () => Measure("public-single", n, n, c => [.. Enumerable.Range(0, c).Select(_ => (Func<Task>)(async () => await dispatcher.DispatchAsync(Job()).ConfigureAwait(false)))])),
            ("prepare-single", () => Measure("prepare-single", n, n, c => [.. Enumerable.Range(0, c).Select(_ => (Func<Task>)(() => { dispatcher.Prepare(Job()); return Task.CompletedTask; }))], counters: false)),
            ("prepared-single", () => Measure("prepared-single", n, 250, Singles(p => async () => await dispatcher.DispatchAsync(p).ConfigureAwait(false)))),
            ("store-single", () => Measure("store-single", n, 250, c =>
            {
                var cmds = Enumerable.Range(0, c).Select(_ => CommandOf(dispatcher.Prepare(Job()))).ToArray();
                return [.. cmds.Select(cmd => (Func<Task>)(async () => await driver.DispatchAsync(cmd, CancellationToken.None).ConfigureAwait(false)))];
            })),
            // Same prepared command sent again: the store answers from its receipt without writing a job.
            ("replay-single", () => Measure("replay-single", n, 250, c =>
            {
                var prepared = Enumerable.Range(0, c).Select(_ => dispatcher.Prepare(Job())).ToArray();
                foreach (var p in prepared) { dispatcher.DispatchAsync(p).GetAwaiter().GetResult(); }
                return [.. prepared.Select(p => (Func<Task>)(async () => await dispatcher.DispatchAsync(p).ConfigureAwait(false)))];
            })),
        };
        var batchArms = new (string Name, Func<Task<ArmResult>> Run)[]
        {
            ("public-batch", () => Measure("public-batch", batchN, batchN, c => [.. Enumerable.Range(0, c).Select(_ => { var items = Items(); return (Func<Task>)(async () => await dispatcher.DispatchManyAsync(items).ConfigureAwait(false)); })])),
            ("prepare-batch", () => Measure("prepare-batch", batchN, batchN, c => [.. Enumerable.Range(0, c).Select(_ => { var items = Items(); return (Func<Task>)(() => { dispatcher.PrepareMany(items); return Task.CompletedTask; }); })], counters: false)),
            ("prepared-batch", () => Measure("prepared-batch", batchN, batchN, c => [.. Enumerable.Range(0, c).Select(_ => { var p = dispatcher.PrepareMany(Items()); return (Func<Task>)(async () => await dispatcher.DispatchAsync(p).ConfigureAwait(false)); })])),
            ("store-batch", () => Measure("store-batch", batchN, batchN, c => [.. Enumerable.Range(0, c).Select(_ => { var cmd = BatchCommandOf(dispatcher.PrepareMany(Items())); return (Func<Task>)(async () => await driver.DispatchManyAsync(cmd, CancellationToken.None).ConfigureAwait(false)); })])),
        };

        // Inside-preparation split (no store): serialize the payload, validate and build the intent, then compute digests.
        var compiled = registry.ByType(typeof(LoadJob))!;
        var splitJobs = Enumerable.Range(0, 2000).Select(_ => Job()).ToArray();
        (double Serialize, double Intent, double Digests, double Full) Split()
        {
            var t = Stopwatch.GetTimestamp();
            foreach (var j in splitJobs) { RunwickJsonProfile.V1.Serialize(j, compiled.PayloadType); }
            var serialize = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
            var items = new List<(DispatchIntent Intent, CanonicalValue Payload)>();
            t = Stopwatch.GetTimestamp();
            foreach (var j in splitJobs) { var (i, _, p) = preparer.PrepareItem(j, null); items.Add((i, p)); }
            var intent = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
            t = Stopwatch.GetTimestamp();
            foreach (var (i, p) in items) { Fingerprints.ForDispatch(storeId, application, Guid.CreateVersion7(), 0, i, p); }
            var digests = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
            t = Stopwatch.GetTimestamp();
            foreach (var j in splitJobs) { dispatcher.Prepare(j); }
            var full = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
            return (serialize / splitJobs.Length, intent / splitJobs.Length, digests / splitJobs.Length, full / splitJobs.Length);
        }

        var splits = new List<JsonObject>();
        for (var round = 1; round <= rounds; round++)
        {
            BenchMode.Phase($"round-{round}-start");
            // Rotate which arm goes first so a store that slows or fills up does not favour one arm.
            var s = singleArms.Select((x, i) => (x, i)).OrderBy(t => (t.i + round) % singleArms.Length).Select(t => t.x);
            foreach (var arm in s.Concat(batchArms.Select((x, i) => (x, i)).OrderBy(t => (t.i + round) % batchArms.Length).Select(t => t.x)))
            {
                BenchMode.Phase($"{arm.Name}-r{round}");
                all.Add((arm.Name, round, await arm.Run().ConfigureAwait(false)));
            }

            var (ser, intent, dig, full) = Split();
            splits.Add(new JsonObject
            {
                ["round"] = round, ["serializePayloadMs"] = Math.Round(ser, 4), ["prepareItemMs"] = Math.Round(intent, 4),
                ["digestsMs"] = Math.Round(dig, 4), ["prepareSingleMs"] = Math.Round(full, 4),
            });
        }

        var result = new JsonObject
        {
            ["mode"] = "stage2-dispatch", ["store"] = store, ["payloadBytes"] = payload, ["opsPerSingleArm"] = n, ["batchItems"] = batch, ["batchesPerArm"] = batchN,
            ["rounds"] = rounds, ["container"] = container, ["tmpdir"] = Path.GetTempPath(),
            ["preparationSplit"] = new JsonArray([.. splits]),
            ["arms"] = new JsonArray([.. all.Select(x => (JsonNode)new JsonObject
            {
                ["arm"] = x.Arm, ["round"] = x.Round, ["ops"] = x.Result.Ops,
                ["meanMs"] = Math.Round(x.Result.MeanMs, 4), ["p50Ms"] = Math.Round(x.Result.P50Ms, 4), ["p99Ms"] = Math.Round(x.Result.P99Ms, 4),
                ["clientCpuMsPerOp"] = Math.Round(x.Result.CpuMsPerOp, 4), ["allocBytesPerOp"] = Math.Round(x.Result.AllocBytesPerOp),
                ["storeCpuMsPerOp"] = Math.Round(x.Result.StoreCpuMsPerOp, 4),
                ["storePhases"] = x.Result.Phases, ["storeCountersPerOp"] = x.Result.Counters,
            })]),
        };
        var json = result.ToJsonString(Json.Indented);
        if (a.OptStr("out") is { } file)
        {
            await File.WriteAllTextAsync(file, json).ConfigureAwait(false);
        }
        else
        {
            Console.WriteLine(json);
        }

        return 0;
    }

    // The Docker cgroup of a container: CPU time the store itself used, which no client counter shows for PostgreSQL.
    internal static string? CgroupCpuFile(string container)
    {
        var id = Run("docker", "inspect", "-f", "{{.Id}}", container).Trim();
        foreach (var path in new[] { $"/sys/fs/cgroup/system.slice/docker-{id}.scope/cpu.stat", $"/sys/fs/cgroup/docker/{id}/cpu.stat" })
        {
            if (File.Exists(path))
            {
                return path;
            }
        }

        return null;
    }

    internal static double ReadCgroupUsec(string file) =>
        File.ReadLines(file).Select(l => l.Split(' ')).Where(p => p[0] == "usage_usec").Select(p => double.Parse(p[1], System.Globalization.CultureInfo.InvariantCulture)).FirstOrDefault();

    internal static string Run(string file, params string[] args)
    {
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, UseShellExecute = false };
        foreach (var x in args) { psi.ArgumentList.Add(x); }
        using var p = Process.Start(psi)!;
        var o = p.StandardOutput.ReadToEnd();
        p.WaitForExit();
        return o;
    }

    // ---- Part A, Redis only: raw replay of a prepared Dispatch with fresh IDs, bypassing every Runwick layer above the wire ------------------------

    /// <summary>
    /// Captures the exact FCALL one real Dispatch sends (function, key, 11-value frame and arguments), then sends the same call with a fresh command ID and
    /// proposed job ID from a plain StackExchange.Redis connection, and compares it with a PING (round trip alone), a no-op FCALL-sized EVAL (round trip plus
    /// parsing a request of the same size) and the same replay with the AOF fsync policy switched to <c>no</c> (what fsync costs, when the volume has any).
    /// </summary>
    static async Task<int> RedisReplayAsync(Args a)
    {
        var cfg = a.OptStr("redis") ?? "127.0.0.1:56392";
        var n = a.Int("n", 3000);
        var rounds = a.Int("rounds", 5);
        var container = a.OptStr("container");
        await using var setup = await BuildAsync(a, "redis").ConfigureAwait(false);
        var sp = setup.Provider;
        var dispatcher = sp.GetRequiredService<JobDispatcher>();
        var runtime = sp.GetRequiredService<RunwickRuntime>();
        var application = sp.GetRequiredService<JobRegistry>().Application;
        var redis = (RedisStore)sp.GetRequiredService<IRunwickStore>();
        await runtime.EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
        StackExchange.Redis.RedisValue[]? captured = null;
        string? script = null;
        RxScript? captureScript = null;
        string? capturedStatus = null;
        redis.Runner.Client.Traced = (sc, args, reply) =>
        {
            if (sc.Name.Contains("ispatch", StringComparison.Ordinal) && !sc.Name.Contains("Many", StringComparison.Ordinal))
            {
                captured = args;
                script = sc.Name;
                captureScript = sc;
                capturedStatus = (string?)reply.FirstOrDefault();
            }
        };
        var probe = dispatcher.Prepare(LoadJob.Create(1, a.Int("payload", 1024)));
        await dispatcher.DispatchAsync(probe).ConfigureAwait(false);
        redis.Runner.Client.Traced = null;
        if (captured is null)
        {
            Console.Error.WriteLine("no dispatch captured");
            return 1;
        }

        var jobId = probe.Intent.ProposedJobId.ToString("N");
        var shape = captured.Select((v, i) => $"{i}:{(((string?)v) ?? "").Length}{(((string?)v) == jobId ? " =jobId" : "")}{(i == 3 ? " =commandId" : "")}").ToArray();
        Console.Error.WriteLine($"captured {script}: {string.Join(' ', shape)}");
        using var mux = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(cfg + ",allowAdmin=true").ConfigureAwait(false);
        var db = mux.GetDatabase();
        var server = mux.GetServers()[0];
        var meta = RxClient.Meta(application);
        var useFcall = redis.Runner.Client.UseFunctions && captureScript!.Function is not null && redis.Runner.Client.FunctionsRefused is null;
        var cgroup = container is null ? null : CgroupCpuFile(container);
        double StoreCpuMs() => cgroup is null ? 0 : ReadCgroupUsec(cgroup) / 1000.0;
        var frameLen = 11;
        var jobIdD = probe.Intent.ProposedJobId.ToString("D");
        // The proposed job ID sits inside the item JSON as well as on its own; a stale copy would make the store see a duplicate job, so every form is replaced.
        void FreshJobId(StackExchange.Redis.RedisValue[] args)
        {
            var fresh = Guid.CreateVersion7();
            for (var k = frameLen; k < args.Length; k++)
            {
                var text = (string?)args[k];
                if (text is not null && (text.Contains(jobId, StringComparison.Ordinal) || text.Contains(jobIdD, StringComparison.Ordinal)))
                {
                    args[k] = text.Replace(jobId, fresh.ToString("N"), StringComparison.Ordinal).Replace(jobIdD, fresh.ToString("D"), StringComparison.Ordinal);
                }
            }
        }
        long notApplied = 0;
        var okStatus = capturedStatus;
        var results = new JsonArray();
        var padding = new string('x', captured.Sum(v => ((string?)v)?.Length ?? 0));
        for (var round = 1; round <= (a.Has("only-concurrent") ? 0 : rounds); round++)
        {
            foreach (var policy in new[] { "always", "no" }.OrderBy(_ => (round % 2 == 0) ? 1 : 0).ThenBy(p => p == "always" ? (round % 2 == 0 ? 1 : 0) : (round % 2 == 0 ? 0 : 1)))
            {
                await server.ConfigSetAsync("appendfsync", policy).ConfigureAwait(false);
                foreach (var arm in new[] { "ping", "echo-sized", "fcall-replay" })
                {
                    var lat = new LatencyReservoir();
                    var s0 = StoreCpuMs();
                    var c0 = Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds;
                    var w = Stopwatch.StartNew();
                    for (var i = 0; i < n; i++)
                    {
                        var t = Stopwatch.GetTimestamp();
                        if (arm == "ping")
                        {
                            await db.PingAsync().ConfigureAwait(false);
                        }
                        else if (arm == "echo-sized")
                        {
                            await db.ExecuteAsync("ECHO", padding).ConfigureAwait(false);
                        }
                        else
                        {
                            var args = (StackExchange.Redis.RedisValue[])captured.Clone();
                            args[3] = Guid.NewGuid().ToString("N");
                            args[5] = DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds();
                            FreshJobId(args);

                            if (useFcall)
                            {
                                await db.ExecuteAsync("FCALL", [captureScript!.Function!, 1, meta, .. args.Select(x => (object)x)]).ConfigureAwait(false);
                            }
                            else
                            {
                                var reply = (StackExchange.Redis.RedisResult[])(await db.ScriptEvaluateAsync(captureScript!.Sha, [meta], args).ConfigureAwait(false))!;
                                if (reply.Length == 0 || (string?)reply[0] != okStatus) { notApplied++; }
                            }
                        }

                        lat.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                    }

                    w.Stop();
                    results.Add(new JsonObject
                    {
                        ["round"] = round, ["appendfsync"] = policy, ["arm"] = arm, ["meanMs"] = Math.Round(w.Elapsed.TotalMilliseconds / n, 4),
                        ["p50Ms"] = Math.Round(lat.Summarize().P50Ms, 4), ["clientCpuMsPerOp"] = Math.Round((Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds - c0) / n, 4),
                        ["storeCpuMsPerOp"] = Math.Round((StoreCpuMs() - s0) / n, 4),
                    });
                }
            }
        }

        // Many callers: the same raw replay from c concurrent callers. If its ceiling matches the public dispatcher's, the ceiling is the server's script time.
        foreach (var callers in new[] { 16, 64 })
        {
            for (var round = 1; round <= rounds; round++)
            {
                var cmdBefore = (double?)(await setup.Counters!().ConfigureAwait(false))["commandCalls"] ?? 0;
                var next = -1;
                var s0 = StoreCpuMs();
                var w = Stopwatch.StartNew();
                await Task.WhenAll(Enumerable.Range(0, callers).Select(_ => Task.Run(async () =>
                {
                    while (Interlocked.Increment(ref next) < n * 3)
                    {
                        var args = (StackExchange.Redis.RedisValue[])captured.Clone();
                        args[3] = Guid.NewGuid().ToString("N");
                            args[5] = DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds();
                        FreshJobId(args);

                        var reply = (StackExchange.Redis.RedisResult[])(await db.ScriptEvaluateAsync(captureScript!.Sha, [meta], args).ConfigureAwait(false))!;
                        if (reply.Length == 0 || (string?)reply[0] != okStatus) { Interlocked.Increment(ref notApplied); }
                    }
                }))).ConfigureAwait(false);
                results.Add(new JsonObject
                {
                    ["round"] = round, ["appendfsync"] = "always", ["arm"] = $"fcall-replay-c{callers}", ["opsPerSec"] = Math.Round(n * 3 / w.Elapsed.TotalSeconds, 1),
                    ["storeCpuMsPerOp"] = Math.Round((StoreCpuMs() - s0) / (n * 3), 4),
                    ["redisCommandsPerOp"] = Math.Round(((double?)(await setup.Counters!().ConfigureAwait(false))["commandCalls"] - cmdBefore) / (n * 3) ?? 0, 2),
                });
            }
        }

        await server.ConfigSetAsync("appendfsync", "always").ConfigureAwait(false);
        var json = new JsonObject { ["mode"] = "stage2-redis-replay", ["captured"] = script, ["argShape"] = string.Join(' ', shape), ["n"] = n, ["appliedStatus"] = okStatus, ["notApplied"] = Interlocked.Read(ref notApplied), ["results"] = results }.ToJsonString(Json.Indented);
        if (a.OptStr("out") is { } file) { await File.WriteAllTextAsync(file, json).ConfigureAwait(false); } else { Console.WriteLine(json); }
        return 0;
    }

    // ---- Part A at many callers: where the one-caller split stops holding ----------------------------------------------------------------------

    /// <summary>
    /// <c>--part concurrent --callers 16,64</c>: the same three arms as the one-caller experiment (public dispatch, dispatch of a command prepared before the
    /// timer, raw store call with a prebuilt store command) driven by that many callers at once, with fresh IDs every time. Reports ops/s, process CPU and
    /// allocated bytes per op, store CPU per op and garbage collections during the arm. If the prepared arm is much faster than the public arm the extra
    /// cost is in the client's preparation under load; if all three plateau together the store is the limit.
    /// </summary>
    static async Task<int> ConcurrentPartAsync(Args a)
    {
        var store = a.OptStr("store") ?? "redis";
        var n = a.Int("n", store == "redis" ? 12000 : store == "sqlite" ? 3000 : 3000);
        var rounds = a.Int("rounds", 5);
        var payload = a.Int("payload", 1024);
        var container = a.OptStr("container");
        var callerSets = (a.OptStr("callers") ?? "16,64").Split(',').Select(int.Parse).ToArray();
        var results = new JsonArray();
        var cgroup = container is null ? null : CgroupCpuFile(container);
        double StoreCpuMs() => cgroup is null ? 0 : ReadCgroupUsec(cgroup) / 1000.0;
        foreach (var callers in callerSets)
        {
            // One fresh store per caller count; every round and arm adds to it (the same growth for every arm).
            a = new Args([.. a.Raw, "--producer-calls", callers.ToString()]);
            await using var setup = await BuildAsync(a, store).ConfigureAwait(false);
            var sp = setup.Provider;
            var dispatcher = sp.GetRequiredService<JobDispatcher>();
            var preparer = sp.GetRequiredService<DispatchPreparer>();
            var runtime = sp.GetRequiredService<RunwickRuntime>();
            var driver = sp.GetRequiredService<IRunwickStore>();
            await runtime.EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
            long seq = 0;
            LoadJob Job() => LoadJob.Create(Interlocked.Increment(ref seq), payload);
            for (var i = 0; i < 100; i++) { await dispatcher.DispatchAsync(Job()).ConfigureAwait(false); }
            StoreCommand<DispatchRequest> CommandOf(PreparedDispatch p) =>
                new(p.Application, p.OperationId.Value, p.NotAfter, new DispatchRequest(new DispatchItem(p.Intent, p.Scopes, p.Digests) { Rates = preparer.RatesFor(p.Intent) }, p.StoreId, p.Digests.Command));
            var proc = Process.GetCurrentProcess();
            async Task<JsonObject> Arm(string arm)
            {
                // Preparation for the prepared and store arms happens before the clock, in 2,000-job slices so no command outlives its lifetime.
                Func<int, Task>[] work;
                Func<Task> prep = () => Task.CompletedTask;
                PreparedDispatch[] prepared = [];
                StoreCommand<DispatchRequest>[] commands = [];
                var next = -1;
                var s = arm switch
                {
                    "public" => (Func<int, Task>)(async _ => await dispatcher.DispatchAsync(Job()).ConfigureAwait(false)),
                    "prepared" => async i => await dispatcher.DispatchAsync(prepared[i]).ConfigureAwait(false),
                    _ => async i => await driver.DispatchAsync(commands[i], CancellationToken.None).ConfigureAwait(false),
                };
                _ = work = [s]; _ = prep;
                long done = 0;
                double wall = 0, cpu = 0, scpu = 0;
                long alloc = 0;
                int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
                for (var start = 0; start < n; start += 2000)
                {
                    var count = Math.Min(2000, n - start);
                    if (arm != "public") { prepared = [.. Enumerable.Range(0, count).Select(_ => dispatcher.Prepare(Job()))]; }
                    if (arm == "store") { commands = [.. prepared.Select(CommandOf)]; }
                    next = -1;
                    GC.Collect();
                    proc.Refresh();
                    var c0 = proc.TotalProcessorTime.TotalMilliseconds; var s0 = StoreCpuMs(); var a0 = GC.GetTotalAllocatedBytes(precise: true);
                    var w = Stopwatch.StartNew();
                    await Task.WhenAll(Enumerable.Range(0, callers).Select(_ => Task.Run(async () =>
                    {
                        for (var i = Interlocked.Increment(ref next); i < count; i = Interlocked.Increment(ref next)) { await s(i).ConfigureAwait(false); }
                    }))).ConfigureAwait(false);
                    wall += w.Elapsed.TotalSeconds;
                    proc.Refresh();
                    cpu += proc.TotalProcessorTime.TotalMilliseconds - c0; scpu += StoreCpuMs() - s0; alloc += GC.GetTotalAllocatedBytes(precise: true) - a0;
                    done += count;
                }

                return new JsonObject
                {
                    ["opsPerSec"] = Math.Round(done / wall, 1), ["clientCpuMsPerOp"] = Math.Round(cpu / done, 4), ["storeCpuMsPerOp"] = Math.Round(scpu / done, 4),
                    ["allocBytesPerOp"] = Math.Round((double)alloc / done), ["gcGen0"] = GC.CollectionCount(0) - g0, ["gcGen1"] = GC.CollectionCount(1) - g1, ["gcGen2"] = GC.CollectionCount(2) - g2,
                };
            }

            var arms = new[] { "public", "prepared", "store" };
            for (var round = 1; round <= rounds; round++)
            {
                foreach (var arm in arms.Select((x, i) => (x, i)).OrderBy(t => (t.i + round) % arms.Length).Select(t => t.x))
                {
                    BenchMode.Phase($"c{callers}-{arm}-r{round}");
                    var r = await Arm(arm).ConfigureAwait(false);
                    r["callers"] = callers; r["arm"] = arm; r["round"] = round;
                    results.Add(r);
                }
            }
        }

        var json = new JsonObject { ["mode"] = "stage2-concurrent", ["store"] = store, ["opsPerArm"] = n, ["rounds"] = rounds, ["tmpdir"] = Path.GetTempPath(), ["results"] = results }.ToJsonString(Json.Indented);
        if (a.OptStr("out") is { } file) { await File.WriteAllTextAsync(file, json).ConfigureAwait(false); } else { Console.WriteLine(json); }
        return 0;
    }
}
