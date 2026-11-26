using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Runwick.Management;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Serialization;
using Runwick.Sqlite.Migrations;
using Runwick.Sqlite.Store;
using Runwick.Worker;

namespace Runwick.Workloads;

/// <summary>
/// <c>bench</c>: Runwick's row in the comparison benchmark (<c>eng/bench-compare/run.sh</c>, next to River and pg-boss).
/// One process, one worker pool on queue <c>load</c>, a no-op handler and a 1 KiB payload by default.
/// <list type="bullet">
/// <item><c>drain</c>: dispatch <c>--n</c> jobs first (DispatchMany, up to 100 per call within the payload budget), then start the worker and time the
/// first claim to the last confirmed finish.</item>
/// <item><c>enqueue</c>: <c>--n</c> single-job Dispatch calls from <c>--calls</c> callers, worker never started; reports inserts/s.</item>
/// <item><c>bulk</c>: <c>--n</c> jobs added with DispatchMany, worker never started; reports inserts/s.</item>
/// <item><c>steady</c>: start the worker, dispatch single jobs at <c>--rate</c>/s for <c>--seconds</c> (one call at a time
/// like the River and pg-boss benches, or open loop with <c>--calls n</c>), and measure dispatch call start to confirmed
/// finish per job; then wait up to 60 s for the rest to finish.</item>
/// </list>
/// Usage: <c>bench --mode drain|enqueue|bulk|steady [--no-usage] [--n 20000] [--rate 100] [--seconds 120] [--workers 16]
/// [--payload 1024] [--job-ms 0] [--calls 1] [--store postgres|sqlite|redis] [--redis-topology standalone|sentinel|cluster] [--out file.json] [--trace] [--sqlite-count-statements]</c>.
/// <c>--warmup-jobs</c> adds jobs on the same producer before timing an enqueue run.
/// SQLite uses a fresh file. <c>--sqlite-keep</c> keeps it, and <c>--sqlite-seed path</c> copies a drained fixture before the run.
/// <c>--sqlite-profile-statements</c> groups native writer time by statement prefix for diagnosis.
/// <c>--sqlite-hold-read</c> pins a read snapshot during a traced drain and measures checkpoint progress after releasing it.
/// <c>--store-viewers</c> enables the co-hosted management sampler and polls the cached Store snapshot every five seconds per viewer during steady mode.
/// <c>--store-warmup-seconds</c> lets the host sample before timing steady mode and primes snapshot serialization when viewers are enabled.
/// SQLite steady side load (Q6/Q7): <c>--retention-hours h</c> sets a short retention policy and keeps cleanup polling (with
/// <c>--cleanup-batch-rows n</c>), <c>--schedule</c> adds a schedule firing every second, <c>--outbox-rate r</c> stages r outbox
/// messages/s in a second SQLite file that the relay delivers, <c>--sample-seconds s</c> records completions, refusals and WAL
/// checkpoint progress over time, and <c>--crash-cycles k</c> kills and reopens the process k times (<see cref="SteadyCrashRun"/>).
/// G5 leases (docs/reports/fragments/g5-leases.md): <c>--fewer-lease-renewals</c> or <c>--lease-seconds n --renew-seconds m</c> set the
/// lease profile and add a <c>lease</c> block (renewals per job and per second, store counters; <c>--lease-report</c> adds it for the
/// default profile); <c>--outage-container name --outage-at s --outage-seconds d</c> docker-pauses the store during steady mode;
/// <c>--lease-crash</c> kills a worker and times how long its jobs take to reach a second one (<see cref="LeaseCrashRun"/>).
/// Trace adds per-job stage gaps, held-job and settlement-queue samples, and store call phases. Writes the same JSON shape as the River and pg-boss
/// benches: <c>system</c>, <c>mode</c>, <c>completionsPerSec</c>, <c>latencyMs {p50, p95, p99, max}</c> (steady).
/// </summary>
internal static class BenchMode
{
    const string DefaultAdmin = "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres";
    // Leave space in the 2 MiB envelope for base64 expansion and per-item metadata.
    const int BatchPayloadBudget = 1024 * 1024;

    // existing: --sqlite-reuse, a file another run (a crash-run segment) owns; it is never deleted here.
    sealed class SqliteBenchDirectory(string parent, bool keep, string? existing = null) : IDisposable
    {
        public string FullName { get; } = existing is not null
            ? Path.GetDirectoryName(Path.GetFullPath(existing))!
            : Directory.CreateDirectory(Path.Combine(parent, "bench-runwick-" + Guid.NewGuid().ToString("N"))).FullName;

        public string DatabasePath => existing ?? Path.Combine(FullName, "runwick.db");

        public void Dispose()
        {
            if (!keep && existing is null)
            {
                try { Directory.Delete(FullName, recursive: true); } catch (IOException) { }
            }
        }
    }

    public static async Task<int> RunAsync(Args a)
    {
        if (a.Has("crash-cycles") && !a.Has("segment"))
        {
            return await SteadyCrashRun.RunAsync(a).ConfigureAwait(false);
        }

        if (a.Has("lease-crash"))
        {
            return await LeaseCrashRun.RunAsync(a).ConfigureAwait(false);
        }

        var mode = a.OptStr("mode") ?? "drain";
        if (mode is not ("drain" or "steady" or "enqueue" or "bulk" or "work"))
        {
            Console.Error.WriteLine("--mode must be drain, steady, enqueue, bulk or work");
            return 2;
        }

        var n = a.Int("n", 20000);
        var rate = a.Double("rate", 100);
        var seconds = a.Double("seconds", 120);
        var workers = a.OptInt("slots") ?? a.Int("workers", 16);
        var payload = a.Int("payload", 1024);
        if (payload is < 1 or > PayloadBounds.MaxPayloadBytes)
        {
            Console.Error.WriteLine($"--payload must be between 1 and {PayloadBounds.MaxPayloadBytes} bytes");
            return 2;
        }

        var database = a.OptStr("db") ?? "bench_runwick";
        var adminCs = a.OptStr("conn") ?? DefaultAdmin;
        var calls = Math.Max(1, a.Int("calls", 1));

        // Fresh store per run: a new PostgreSQL database, a new SQLite file, or a flushed Redis (only the instance named by --redis).
        var store = a.OptStr("store") ?? "postgres";
        if (store is not ("postgres" or "sqlite" or "redis"))
        {
            Console.Error.WriteLine("--store must be postgres, sqlite or redis");
            return 2;
        }

        // --batch-items n: items per DispatchMany call (default the portable 100; PostgreSQL accepts up to 1,000); the 1 MiB byte budget still applies.
        var batchCap = store == "postgres" ? PayloadBounds.PostgresMaxBatchItems : PayloadBounds.MaxBatchItems;
        var batchItems = a.Int("batch-items", PayloadBounds.MaxBatchItems);
        if (batchItems < 1 || batchItems > batchCap)
        {
            Console.Error.WriteLine($"--batch-items must be between 1 and {batchCap} for {store}");
            return 2;
        }

        var chunk = Math.Min(batchItems, BatchPayloadBudget / payload);

        var holdRead = a.Has("sqlite-hold-read");
        if (holdRead && (store != "sqlite" || mode != "drain" || !a.Has("trace")))
        {
            Console.Error.WriteLine("--sqlite-hold-read needs --store sqlite --mode drain --trace");
            return 2;
        }

        if (a.Has("outage-container") && (mode != "steady" || store == "sqlite"))
        {
            Console.Error.WriteLine("--outage-container needs --mode steady and --store postgres or redis");
            return 2;
        }

        var storeViewers = a.Int("store-viewers", 0);
        var storeWarmupSeconds = a.Int("store-warmup-seconds", 0);
        if (storeViewers < 0 || storeWarmupSeconds < 0 || ((storeViewers > 0 || storeWarmupSeconds > 0) && mode != "steady"))
        {
            Console.Error.WriteLine("--store-viewers and --store-warmup-seconds must be nonnegative and need --mode steady");
            return 2;
        }

        // Q6/Q7 side load (steady, SQLite): --retention-hours h [--cleanup-batch-rows n], --schedule, --outbox-rate r, --sample-seconds s.
        // A crash-run segment also gets --sqlite-reuse, --ledger, and --crash or --drain-store-seconds (SteadyCrashRun).
        var retentionHours = a.OptInt("retention-hours");
        var schedule = a.Has("schedule");
        var outboxRate = a.Double("outbox-rate", 0);
        var sampleSeconds = a.Double("sample-seconds", 0);
        var crash = a.Has("crash");
        if ((retentionHours is not null || schedule || outboxRate > 0 || sampleSeconds > 0 || crash || a.Has("drain-store-seconds"))
            && (store != "sqlite" || mode != "steady"))
        {
            Console.Error.WriteLine("--retention-hours, --schedule, --outbox-rate, --sample-seconds, --crash-cycles and the segment flags need --store sqlite --mode steady");
            return 2;
        }

        Action<RunwickBuilder> useStore;
        // G5 lease report: cumulative counters the store itself reports (Redis INFO, PostgreSQL pg_stat_database); null for SQLite.
        Func<Task<JsonObject>>? storeCounters = null;
        var keepSqlite = a.Has("sqlite-keep");
        using var sqliteDirectory = store == "sqlite"
            ? new SqliteBenchDirectory(a.OptStr("sqlite-dir") ?? Path.GetTempPath(), keepSqlite, a.OptStr("sqlite-reuse"))
            : null;
        string? outboxSourceCs = null;
        if (sqliteDirectory is not null)
        {
            var sqliteCs = new SqliteConnectionStringBuilder { DataSource = sqliteDirectory.DatabasePath, Pooling = false }.ConnectionString;
            var seed = a.OptStr("sqlite-seed");
            if (seed is not null)
            {
                using var source = new SqliteConnection(new SqliteConnectionStringBuilder
                {
                    DataSource = seed,
                    Mode = SqliteOpenMode.ReadOnly,
                    Pooling = false,
                }.ConnectionString);
                using var destination = new SqliteConnection(sqliteCs);
                source.Open();
                destination.Open();
                source.BackupDatabase(destination);
            }

            await RunwickSqliteMigrations.MigrateAsync(sqliteCs).ConfigureAwait(false);
            if (seed is not null)
            {
                using var copy = new SqliteConnection(sqliteCs);
                copy.Open();
                using var checkSeed = copy.CreateCommand();
                checkSeed.CommandText = "SELECT EXISTS (SELECT 1 FROM runwick_jobs WHERE application = 'bench' AND terminal_at IS NULL)";
                if ((long)checkSeed.ExecuteScalar()! != 0)
                {
                    Console.Error.WriteLine("--sqlite-seed needs a drained bench fixture with only terminal jobs");
                    return 2;
                }
            }

            useStore = r => r.UseSqlite(sqliteCs);
            if (schedule && (await RunwickSqliteMigrations.ActivateCapabilityAsync(sqliteCs, "bench", "scheduler", "ops").ConfigureAwait(false)).Count == 0)
            {
                Console.Error.WriteLine("--schedule needs a store where application 'bench' already exists (use --sqlite-seed)");
                return 2;
            }

            if (outboxRate > 0)
            {
                // The outbox source is the application's own database: a second file next to the store, not the store file.
                outboxSourceCs = new SqliteConnectionStringBuilder { DataSource = Path.Combine(sqliteDirectory.FullName, "outbox-source.db"), Pooling = false }.ConnectionString;
                await Runwick.Outbox.Sqlite.SqliteOutboxSchema.InstallAsync(outboxSourceCs).ConfigureAwait(false);
            }
        }
        else if (store == "redis")
        {
            var redisCfg = a.OptStr("redis") ?? throw new ArgumentException("--store redis needs --redis host:port (a dedicated, throwaway instance: it is FLUSHALLed)");
            // --redis-topology sentinel|cluster: the opt-in write-authority modes (22 §2.5, §2.6); --redis then lists the Sentinels and serviceName,
            // or the cluster's nodes. Every primary the connection reaches is flushed.
            var topology = a.OptStr("redis-topology") ?? "standalone";
            // --redis-reuse: keep what is there (the second worker of a --lease-crash run).
            if (!a.Has("redis-reuse"))
            {
                using var adminRedis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(redisCfg + ",allowAdmin=true").ConfigureAwait(false);
                foreach (var server in adminRedis.GetServers().Where(s => s.IsConnected && !s.IsReplica))
                {
                    await server.FlushAllDatabasesAsync().ConfigureAwait(false);
                }
            }

            // --redis-everysec: the opt-in appendfsync everysec profile (the server must run it); default is appendfsync always.
            var persistence = a.Has("redis-everysec") ? RedisPersistence.AofEverySecond : RedisPersistence.AofAlways;
            await RunwickRedisMigrations.MigrateAsync(redisCfg, new RedisOptions
            {
                Persistence = persistence,
                Sentinel = topology == "sentinel" ? new() : null,
                Cluster = topology == "cluster" ? new() : null,
            }).ConfigureAwait(false);
            // --redis-group [--group-items n]: the opt-in grouped producer dispatch (perf plan F); default off.
            var group = a.Has("redis-group");
            var groupItems = a.Int("group-items", 8);
            var groupSlots = a.Int("group-slots", 2);
            storeCounters = () => RedisCountersAsync(redisCfg);
            useStore = r => r.UseRedis(redisCfg, o =>
            {
                o.Persistence = persistence;
                o.Sentinel = topology == "sentinel" ? new() : null;
                o.Cluster = topology == "cluster" ? new() : null;
                o.GroupDispatches = group;
                o.MaxGroupItems = groupItems;
                o.MaxGroupsInFlight = groupSlots;
            });
        }
        else
        {
            var admin = new NpgsqlConnectionStringBuilder(adminCs) { ApplicationName = "bench:admin", Pooling = false }.ConnectionString;
            if (!a.Has("db-reuse"))
            {
                // --db-reuse: run against an existing database (for example one cloned from a retained-data template); it is only migrated.
                await G2Orchestrator.ExecAsync(admin, $"DROP DATABASE IF EXISTS {database} WITH (FORCE)").ConfigureAwait(false);
                await G2Orchestrator.ExecAsync(admin, $"CREATE DATABASE {database}").ConfigureAwait(false);
            }

            await RunwickPostgresMigrations.MigrateAsync(new NpgsqlConnectionStringBuilder(adminCs)
                { Database = database, ApplicationName = "bench:migrate", Pooling = false }.ConnectionString).ConfigureAwait(false);
            if (a.Has("pg-no-events"))
            {
                // --pg-no-events: upper-bound probe for a lean-history mode. The event table keeps its rows (the SQL functions read them back) but loses
                // its secondary indexes and its WAL logging, which is most of the per-event cost. The run proves nothing about correctness.
                await using var probe = new Npgsql.NpgsqlConnection(new NpgsqlConnectionStringBuilder(adminCs) { Database = database, Pooling = false }.ConnectionString);
                await probe.OpenAsync().ConfigureAwait(false);
                var drops = new List<string>();
                await using (var q = new Npgsql.NpgsqlCommand("SELECT indexname FROM pg_indexes WHERE schemaname = 'runwick' AND tablename = 'queue_events' AND indexname <> 'queue_events_pk'", probe))
                await using (var rd = await q.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await rd.ReadAsync().ConfigureAwait(false))
                    {
                        drops.Add(rd.GetString(0));
                    }
                }

                foreach (var ix in drops)
                {
                    await using var drop = new Npgsql.NpgsqlCommand($"DROP INDEX runwick.\"{ix}\"", probe);
                    await drop.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                await using var unlogged = new Npgsql.NpgsqlCommand("ALTER TABLE runwick.queue_events SET UNLOGGED", probe);
                await unlogged.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            if (a.Has("pg-no-receipts"))
            {
                // --pg-no-receipts: upper-bound probe for a reduced-replay-protection mode (G2). The receipt partitions stay readable but lose their WAL
                // logging, which is most of the per-receipt cost. The run proves nothing about durability or replay.
                await using var probe = new Npgsql.NpgsqlConnection(new NpgsqlConnectionStringBuilder(adminCs) { Database = database, Pooling = false }.ConnectionString);
                await probe.OpenAsync().ConfigureAwait(false);
                foreach (var part in new[] { "storage_commands_ownership", "storage_commands_durable" })
                {
                    await using var unlogged = new Npgsql.NpgsqlCommand($"ALTER TABLE runwick.{part} SET UNLOGGED", probe);
                    await unlogged.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }

            var storeCs = new NpgsqlConnectionStringBuilder(adminCs)
            {
                Database = database,
                ApplicationName = "bench-runwick",
                Pooling = true,
                MaxPoolSize = Math.Max(100, workers + 16),
                Options = a.Has("trace") ? "-c track_functions=all" : null, // --trace: per-function server time in pg_stat_user_functions
            }.ConnectionString;
            var counterCs = new NpgsqlConnectionStringBuilder(adminCs) { Database = database, ApplicationName = "bench:counters", Pooling = false }.ConnectionString;
            storeCounters = () => PostgresCountersAsync(counterCs, database);
            // Grouped producer admission (PF2-B1) is on by default; --no-pg-group turns it off (--pg-group is kept as a no-op).
            // It only groups callers beyond the producer slots (--producer-calls).
            useStore = r => r.UsePostgres(storeCs, o => o.GroupProducerAdmission = !a.Has("no-pg-group"));
        }

        WorkerOptions? leaseOptions = null;
        // --lease-report (implied by any lease flag or an outage): renewals, lost leases and store counters in the result.
        var leaseReport = a.Has("lease-report") || a.Has("fewer-lease-renewals") || a.Has("lease-seconds") || a.Has("renew-seconds") || a.Has("outage-container");
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(LogLevel.Warning));
        if (storeViewers > 0 || retentionHours is not null)
        {
            sc.AddSingleton<ManagementRoleMarker>();
        }

        sc.AddSingleton(new LoadBehaviour(TimeSpan.FromMilliseconds(a.Double("job-ms", 0))));
        sc.AddSingleton(new G2Behaviour(TimeSpan.Zero, Spin: false));
        if (outboxSourceCs is not null)
        {
            sc.AddRunwickSqliteOutbox(outboxSourceCs);
        }

        sc.AddRunwick(r =>
        {
            r.Application("bench");
            useStore(r);
            LoadRegistration.Register(r, typeCeiling: null);
            r.AddHandler<LoadJob, LoadHandler>();
            // --producer-calls n: producer slots (connections) independent of the caller count; default one per caller.
            r.Producer(o => o.MaxConcurrentCalls = a.OptInt("producer-calls") ?? Math.Max(2, calls));
            if (schedule)
            {
                // One schedule firing every second on its own queue, so its jobs never count as load jobs.
                r.AddJob<G2Tick>().Named("wl.tick").ToQueue(SideQueues.Schedule);
                r.AddHandler<G2Tick, G2Handler<G2Tick>>();
                r.AddScheduler();
                r.AddSchedule("bench-tick", new G2Tick("bench-tick")).Cron("* * * * * *", ScheduleCronFormat.SixField);
            }

            if (outboxSourceCs is not null)
            {
                r.AddJob<G2Out>().Named("wl.out").ToQueue(SideQueues.Outbox);
                r.AddHandler<G2Out, G2Handler<G2Out>>();
            }

            var sideQueues = (schedule ? 1 : 0) + (outboxSourceCs is not null ? 1 : 0);
            r.AddWorker(w =>
            {
                if (sideQueues > 0)
                {
                    w.Pool("side", p => p.Queues([.. new[] { schedule ? SideQueues.Schedule : null, outboxSourceCs is not null ? SideQueues.Outbox : null }.OfType<string>()])
                        .FixedConcurrency(2 * sideQueues));
                }

                w.MaxOutstandingAttempts(workers + 2 * sideQueues).Pool("main", p => p.Queues(LoadRegistration.Queue).FixedConcurrency(workers))
                .Retention(o =>
                {
                    if (retentionHours is not null)
                    {
                        // Keep cleanup busy for the whole window: small batches, and look again soon after an empty pass.
                        o.BatchRows = a.Int("cleanup-batch-rows", o.BatchRows);
                        o.IdlePoll = TimeSpan.FromSeconds(5);
                    }
                })
                .Configure(o =>
                {
                    o.RecordResourceUsage = !a.Has("no-usage"); // --no-usage: the attempt-usage overhead baseline
                    // G1 buffered finish: unset, the store decides (on for PostgreSQL and SQLite, off for Redis);
                    // --buffered-finish forces it on, --no-buffered-finish forces it off.
                    if (a.Has("buffered-finish"))
                    {
                        o.UseBufferedFinish();
                    }
                    else if (a.Has("no-buffered-finish"))
                    {
                        o.BufferedFinish = false;
                    }

                    if (a.Has("combined-finish-claim")) // 05 §12.7: the finish of a job and the claim of its replacement in one call
                    {
                        o.UseCombinedFinishClaim();
                    }

                    // G5: --fewer-lease-renewals (the 5 min / 90 s profile), or --lease-seconds n and --renew-seconds m by hand.
                    if (a.Has("fewer-lease-renewals"))
                    {
                        o.UseFewerLeaseRenewals();
                    }

                    if (a.OptStr("lease-seconds") is not null)
                    {
                        o.LeaseDuration = TimeSpan.FromSeconds(a.Double("lease-seconds", 60));
                    }

                    if (a.OptStr("renew-seconds") is not null)
                    {
                        o.RenewalInterval = TimeSpan.FromSeconds(a.Double("renew-seconds", 15));
                        o.RenewalJitter = TimeSpan.FromTicks(Math.Min(o.RenewalJitter.Ticks, o.RenewalInterval.Ticks / 5)); // keep jitter below the interval
                    }

                    leaseOptions = o;

                    if (a.OptInt("claims-in-flight") is { } inFlight) // 05 §9.7: batched claims a pool may have outstanding at once
                    {
                        o.ClaimsInFlight = inFlight;
                    }

                    if (a.OptInt("claim-ahead") is { } ahead) // 05 §9.8: jobs a pool may claim beyond its concurrency
                    {
                        o.ClaimAhead = ahead;
                    }
                });
            });
        });
        await using var provider = sc.BuildServiceProvider();
        var engine = provider.GetRequiredService<WorkerEngine>();
        // The Runwick host, plus the outbox relay when --outbox-rate is set.
        var hosted = new HostedGroup([.. provider.GetServices<IHostedService>()]);
        var dispatcher = provider.GetRequiredService<IJobDispatcher>();
        var driver = provider.GetRequiredService<IRunwickStore>();
        var sqlite = driver as SqliteStore;
        long sqliteStatements = 0;
        var sqliteStatementProfile = a.Has("sqlite-profile-statements") && sqlite is not null
            ? new ConcurrentDictionary<string, (long Count, long Nanoseconds)>(StringComparer.Ordinal)
            : null;
        var countSqliteStatements = sqlite is not null && (a.Has("sqlite-count-statements") || sqliteStatementProfile is not null);
        if (countSqliteStatements)
        {
            await sqlite!.Executor.WriteAsync(SqLane.Background, conn =>
            {
                if (sqliteStatementProfile is null)
                {
                    SQLitePCL.raw.sqlite3_profile(conn.Handle, (SQLitePCL.delegate_profile)((_, _, _) =>
                        Interlocked.Increment(ref sqliteStatements)), null);
                }
                else
                {
                    SQLitePCL.raw.sqlite3_profile(conn.Handle, (SQLitePCL.strdelegate_profile)((_, sql, ns) =>
                    {
                        Interlocked.Increment(ref sqliteStatements);
                        var statement = sql.AsSpan().TrimStart();
                        var newline = statement.IndexOf('\n');
                        var prefix = statement[..Math.Min(newline < 0 ? statement.Length : newline, 120)].ToString();
                        sqliteStatementProfile.AddOrUpdate(prefix, static (_, elapsed) => (1, elapsed),
                            static (_, sample, elapsed) => (sample.Count + 1, sample.Nanoseconds + elapsed), ns);
                    }), null);
                }

                return true;
            }, CancellationToken.None).ConfigureAwait(false);
        }

        // Event hook: first claim, finish count, last finish, per-job dispatch-to-finish latency (steady).
        var clock = Stopwatch.StartNew();
        long firstClaimTicks = -1, lastFinishTicks = 0, finished = 0, replayed = 0;
        var dispatchedAt = new ConcurrentDictionary<Guid, long>(); // steady: dispatch call start (clock ticks)
        var finishedAt = new ConcurrentDictionary<Guid, long>();   // steady: finish seen before the receipt returned
        var latency = new LatencyReservoir();
        var enqueueCallLatency = new LatencyReservoir(); // steady: per accepted Dispatch call
        var delayMs = a.Double("delay-ms", 0); // steady: each job is not eligible until this long after its offer (a delayed-job workload)
        // --trace: per-job stage gaps (reserved -> handler invoked -> finish confirmed) and per-call store phases.
        var trace = a.Has("trace");
        var stages = new Metrics();
        using var phases = trace ? PhaseListener.Attach(stages) : null;
        if (trace && driver is RedisStore redis)
        {
            redis.Runner.Client.Traced = (script, _, reply) =>
                stages.Count($"redis:{script.Name}:{(string?)reply.FirstOrDefault()}");
        }

        var reservedAt = new ConcurrentDictionary<Guid, long>();
        var invokedAt = new ConcurrentDictionary<Guid, long>();
        long reservedCount = 0;
        // --ledger: one line per accepted load job (a), handler start (i) and confirmed finish (f), with wall-clock ms, for
        // the crash-run reconciliation. Each line is its own write, so a killed process loses no line it finished writing.
        var ledgerPath = a.OptStr("ledger");
        using var ledger = ledgerPath is null ? null : new StreamWriter(ledgerPath, append: true) { AutoFlush = true };
        void Ledger(char kind, Guid id, long unixMs)
        {
            if (ledger is not null)
            {
                lock (ledger)
                {
                    ledger.WriteLine(FormattableString.Invariant($"{kind} {id:N} {unixMs}"));
                }
            }
        }

        var sideFinished = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
        // G5: lost leases by cause, and with --outage-container each load job's handler starts, finish and loss (wall-clock ms).
        var lostByCause = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
        var outageJobs = a.Has("outage-container") ? new ConcurrentDictionary<Guid, OutageJob>() : null;
        engine.Hooks.Event = (name, entry) =>
        {
            var now = clock.ElapsedTicks;
            if (entry is not null && entry.Reserved.Queue != LoadRegistration.Queue)
            {
                if (name is "finished" or "finishReplayed")
                {
                    sideFinished.AddOrUpdate(entry.Reserved.Queue, 1, (_, c) => c + 1);
                }

                return; // schedule and outbox jobs are side load, not counted as load jobs
            }

            if (ledger is not null && entry is not null && name is "handlerInvoked" or "finished" or "finishReplayed")
            {
                Ledger(name == "handlerInvoked" ? 'i' : 'f', entry.Reserved.Position.JobId, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            }
            if (name.StartsWith("lost:", StringComparison.Ordinal))
            {
                lostByCause.AddOrUpdate(name[5..], 1, (_, c) => c + 1);
            }

            if (outageJobs is not null && entry is not null && (name is "handlerInvoked" or "finished" or "finishReplayed" || name.StartsWith("lost:", StringComparison.Ordinal)))
            {
                var job = outageJobs.GetOrAdd(entry.Reserved.Position.JobId, _ => new OutageJob());
                var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                lock (job)
                {
                    if (name == "handlerInvoked")
                    {
                        job.Starts.Add(nowMs);
                    }
                    else if (name.StartsWith("lost:", StringComparison.Ordinal))
                    {
                        job.LostMs ??= nowMs;
                    }
                    else
                    {
                        job.FinishedMs ??= nowMs;
                    }
                }
            }

            if (trace && name.StartsWith("pressure", StringComparison.Ordinal))
            {
                stages.Count(name);
            }

            if (trace && entry is not null)
            {
                var id = entry.Reserved.Position.JobId;
                if (name == "reserved")
                {
                    Interlocked.Increment(ref reservedCount);
                    reservedAt[id] = now;
                }
                else if (name == "handlerInvoked" && reservedAt.TryGetValue(id, out var r))
                {
                    invokedAt[id] = now;
                    stages.Observe("stage:reserved->handlerInvoked", Ms(now - r));
                    stages.Observe("stage:due->handlerInvoked", (DateTimeOffset.UtcNow - entry.Reserved.Cursor.EligibleAt).TotalMilliseconds);
                }
                else if (name is "finished" or "finishReplayed")
                {
                    if (invokedAt.TryRemove(id, out var h))
                    {
                        stages.Observe("stage:handlerInvoked->finished", Ms(now - h));
                    }

                    if (reservedAt.TryRemove(id, out var r2))
                    {
                        stages.Observe("stage:reserved->finished", Ms(now - r2));
                    }
                }
            }

            if (name is "reserved" or "started" or "handlerInvoked")
            {
                Interlocked.CompareExchange(ref firstClaimTicks, now, -1);
            }
            else if (name is "finished" or "finishReplayed")
            {
                if (name == "finishReplayed")
                {
                    Interlocked.Increment(ref replayed);
                }

                for (var previous = Interlocked.Read(ref lastFinishTicks); now > previous; previous = Interlocked.Read(ref lastFinishTicks))
                {
                    if (Interlocked.CompareExchange(ref lastFinishTicks, now, previous) == previous)
                    {
                        break;
                    }
                }

                if (mode == "steady" && entry is not null)
                {
                    var id = entry.Reserved.Position.JobId;
                    if (dispatchedAt.TryRemove(id, out var started))
                    {
                        latency.Add(Ms(now - started));
                    }
                    else
                    {
                        finishedAt[id] = now;
                    }
                }

                Interlocked.Increment(ref finished);
            }
        };

        var result = new JsonObject
        {
            ["system"] = store == "postgres" ? "runwick" : "runwick-" + store,
            ["store"] = store,
            ["mode"] = mode,
            ["workers"] = workers,
            ["payloadBytes"] = payload,
            ["bulkChunk"] = chunk,
        };
        long sequence = 0;
        var failedDispatch = 0L;
        var check = a.Has("check"); // drain: tolerate and report outcome-unknown bulk adds, then compare jobs run with jobs accepted
        long unknownChunks = 0, acceptedItems = 0, unresolvedItems = 0;
        var unknownCodes = new ConcurrentDictionary<string, long>();
        var resolutions = new ConcurrentDictionary<string, long>();
        var resolveErrors = new ConcurrentDictionary<string, long>();
        using var snapshotConnection = holdRead
            ? new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = sqliteDirectory!.DatabasePath,
                Mode = SqliteOpenMode.ReadOnly,
                Pooling = false,
            }.ConnectionString)
            : null;

        var leaseStartSeconds = clock.Elapsed.TotalSeconds;
        var countersBefore = leaseReport && storeCounters is not null ? await storeCounters().ConfigureAwait(false) : null;
        JsonObject? outageResult = null;
        if (mode == "enqueue")
        {
            // --n single-job Dispatch calls from --calls callers side by side; the worker is never started.
            var enqueueLatency = new LatencyReservoir();
            var warmup = Math.Max(0, a.Int("warmup-jobs", 0));
            if (warmup > 0)
            {
                await EnqueueAsync(warmup, timings: null).ConfigureAwait(false);
                stages.ResetLatencies();
            }

            var allocatedBefore = trace ? GC.GetTotalAllocatedBytes(precise: true) : 0;
            Phase("timed-start");
            var watch = Stopwatch.StartNew();
            await EnqueueAsync(n, enqueueLatency).ConfigureAwait(false);
            watch.Stop();
            Phase("timed-end");
            if (trace)
            {
                result["enqueueManagedAllocatedBytes"] = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
            }

            var es = enqueueLatency.Summarize();
            result["jobs"] = n;
            result["warmupJobs"] = warmup;
            result["calls"] = calls;
            result["insertsPerSec"] = Math.Round(n / watch.Elapsed.TotalSeconds, 1);
            result["insertLatencyMs"] = new JsonObject
            {
                ["p50"] = Math.Round(es.P50Ms, 2),
                ["p95"] = Math.Round(es.P95Ms, 2),
                ["p99"] = Math.Round(es.P99Ms, 2),
            };
            result["producerCalls"] = a.OptInt("producer-calls") ?? Math.Max(2, calls);
            if (driver is Runwick.Postgres.Store.PgStore { GroupsDispatch: true } grouped)
            {
                result["pgGroup"] = new JsonObject
                {
                    ["groups"] = grouped.DispatchGroups, ["groupedCommands"] = grouped.GroupedDispatches, ["fallbacks"] = grouped.GroupFallbacks,
                    ["meanGroupSize"] = grouped.DispatchGroups == 0 ? 0 : Math.Round((double)grouped.GroupedDispatches / grouped.DispatchGroups, 2),
                };
            }

            Task EnqueueAsync(int count, LatencyReservoir? timings)
            {
                var nextJob = 0;
                return Task.WhenAll(Enumerable.Range(0, calls).Select(_ => Task.Run(async () =>
                {
                    while (Interlocked.Increment(ref nextJob) <= count)
                    {
                        var started = clock.ElapsedTicks;
                        await dispatcher.DispatchAsync(LoadJob.Create(Interlocked.Increment(ref sequence), payload)).ConfigureAwait(false);
                        timings?.Add(Ms(clock.ElapsedTicks - started));
                    }
                })));
            }
        }
        else if (mode == "bulk")
        {
            // --n jobs added with DispatchMany from --calls callers side by side; worker never started.
            Phase("timed-start");
            var watch = Stopwatch.StartNew();
            var callMs = new System.Collections.Concurrent.ConcurrentBag<double>();
            var nextChunk = -1;
            await Task.WhenAll(Enumerable.Range(0, calls).Select(_ => Task.Run(async () =>
            {
                for (var c = Interlocked.Increment(ref nextChunk); c * chunk < n; c = Interlocked.Increment(ref nextChunk))
                {
                    var items = Enumerable.Range(0, Math.Min(chunk, n - c * chunk))
                        .Select(_ => new BatchDispatchItem(LoadJob.Create(Interlocked.Increment(ref sequence), payload))).ToList();
                    var started = Stopwatch.GetTimestamp();
                    var receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
                    callMs.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                    Interlocked.Add(ref failedDispatch, receipt.Items.Count(x => x.Receipt is null));
                }
            }))).ConfigureAwait(false);
            watch.Stop();
            Phase("timed-end");
            var sorted = callMs.OrderBy(x => x).ToArray();
            double At(double q) => sorted.Length == 0 ? 0 : Math.Round(sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(q * sorted.Length) - 1)], 2);
            result["batchItems"] = chunk;
            result["callLatencyMs"] = new JsonObject { ["p50"] = At(0.5), ["p95"] = At(0.95), ["p99"] = At(0.99), ["max"] = At(1) };
            result["jobs"] = n;
            result["calls"] = calls;
            result["failedDispatch"] = failedDispatch;
            result["insertsPerSec"] = Math.Round((n - failedDispatch) / watch.Elapsed.TotalSeconds, 1);
        }
        else if (mode == "drain")
        {
            // --calls n: n callers send DispatchMany chunks side by side (the producer admits n concurrent calls).
            Phase("seed-start");
            var insertWatch = Stopwatch.StartNew();
            var nextChunk = -1;
            await Task.WhenAll(Enumerable.Range(0, calls).Select(_ => Task.Run(async () =>
            {
                for (var c = Interlocked.Increment(ref nextChunk); c * chunk < n; c = Interlocked.Increment(ref nextChunk))
                {
                    var items = Enumerable.Range(0, Math.Min(chunk, n - c * chunk))
                        .Select(_ => new BatchDispatchItem(LoadJob.Create(Interlocked.Increment(ref sequence), payload))).ToList();
                    BatchDispatchReceipt? receipt = null;
                    try
                    {
                        receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
                    }
                    catch (DispatchOutcomeUnknownException ex) when (check)
                    {
                        // --check: record the exact error, then ask the store what happened to that operation.
                        Interlocked.Increment(ref unknownChunks);
                        unknownCodes.AddOrUpdate($"{ex.Code}: {ex.InnerException?.GetType().Name}: {ex.InnerException?.Message}", 1, (_, v) => v + 1);
                        DispatchResolution? resolution = null;
                        for (var attempt = 0; attempt < 40 && resolution is null; attempt++)
                        {
                            try
                            {
                                resolution = await provider.GetRequiredService<IPreparedJobDispatcher>().ResolveAsync(ex.OperationId).ConfigureAwait(false);
                            }
                            catch (Exception rex) when (rex is not OperationCanceledException)
                            {
                                resolveErrors.AddOrUpdate($"{rex.GetType().Name}: {rex.Message}", 1, (_, v) => v + 1);
                                await Task.Delay(500).ConfigureAwait(false); // the store may still be finishing a pending write
                            }
                        }

                        resolutions.AddOrUpdate(resolution?.Status.ToString() ?? "StillUnresolvedAfter20s", 1, (_, v) => v + 1);
                        receipt = resolution?.BatchReceipt;
                        if (receipt is null)
                        {
                            Interlocked.Add(ref unresolvedItems, items.Count);
                        }
                    }

                    if (receipt is not null)
                    {
                        var rejected = receipt.Items.Count(x => x.Receipt is null);
                        Interlocked.Add(ref failedDispatch, rejected);
                        Interlocked.Add(ref acceptedItems, receipt.Items.Count - rejected);
                    }
                }
            }))).ConfigureAwait(false);

            if (check)
            {
                result["check"] = new JsonObject
                {
                    ["chunks"] = (n + chunk - 1) / chunk,
                    ["chunksWithUnknownOutcome"] = Interlocked.Read(ref unknownChunks),
                    ["errors"] = new JsonObject([.. unknownCodes.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                    ["resolveAttemptErrors"] = new JsonObject([.. resolveErrors.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                    ["resolutions"] = new JsonObject([.. resolutions.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                    ["itemsAccepted"] = Interlocked.Read(ref acceptedItems),
                    ["itemsInUnresolvedChunks"] = Interlocked.Read(ref unresolvedItems),
                };
            }

            result["insertSeconds"] = Math.Round(insertWatch.Elapsed.TotalSeconds, 3);
            result["insertCalls"] = calls;
            if (snapshotConnection is not null)
            {
                snapshotConnection.Open();
                using var pin = snapshotConnection.CreateCommand();
                pin.CommandText = "BEGIN; SELECT EXISTS(SELECT 1 FROM runwick_jobs WHERE application = 'bench')";
                _ = pin.ExecuteScalar();
            }

            var commitsBeforeDrain = sqlite?.Runner.Commits;
            var statementsBeforeDrain = Interlocked.Read(ref sqliteStatements);
            sqliteStatementProfile?.Clear();
            var expected = check ? Interlocked.Read(ref acceptedItems) : n - failedDispatch;
            Phase("seed-end");
            // Count the backlog in the store before the worker exists, so a short seed fails the round instead of inflating the rate.
            result["readyBeforeStart"] = await StoreJobCountsAsync(provider, sqliteDirectory?.DatabasePath).ConfigureAwait(false);
            var startTicks = clock.ElapsedTicks;
            var allocatedBeforeDrain = trace ? GC.GetTotalAllocatedBytes(precise: true) : 0;
            Phase("timed-start");
            await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
            using var sampling = new CancellationTokenSource();
            var sampler = trace ? Task.Run(async () =>
            {
                // Every 10 ms: jobs held by the worker (reserved, not yet confirmed finished) and the settlement queue depth.
                while (!sampling.IsCancellationRequested)
                {
                    stages.Observe("sample:heldJobs", Interlocked.Read(ref reservedCount) - Interlocked.Read(ref finished));
                    stages.Observe("sample:settlementQueue", engine.Completion.Pending);
                    await Task.Delay(10).ConfigureAwait(false);
                }
            }) : Task.CompletedTask;
            var deadline = Stopwatch.StartNew();
            var progress = new JsonArray();
            var nextProgress = 1.0;
            while (Interlocked.Read(ref finished) < expected && deadline.Elapsed < TimeSpan.FromSeconds(a.Double("max-seconds", 1800)))
            {
                await Task.Delay(5).ConfigureAwait(false);
                if (deadline.Elapsed.TotalSeconds >= nextProgress)
                {
                    progress.Add(new JsonArray(Math.Round(deadline.Elapsed.TotalSeconds, 1), Interlocked.Read(ref finished)));
                    nextProgress = Math.Floor(deadline.Elapsed.TotalSeconds) + 1;
                }
            }

            Phase("timed-end");
            result["progress"] = progress; // [seconds since worker start, confirmed finishes], once a second: an incomplete round still gives time to 50/95/99%
            result["clocks"] = new JsonObject { ["workerStartSeconds"] = Math.Round(deadline.Elapsed.TotalSeconds, 3) };

            if (check)
            {
                await Task.Delay(5000).ConfigureAwait(false); // grace: any extra job run beyond the accepted count shows up here
                (result["check"] as JsonObject)!["jobsRun"] = Interlocked.Read(ref finished);
                (result["check"] as JsonObject)!["jobsRunMinusAccepted"] = Interlocked.Read(ref finished) - expected;
            }

            await sampling.CancelAsync().ConfigureAwait(false);
            await sampler.ConfigureAwait(false);
            if (trace)
            {
                result["drainManagedAllocatedBytes"] = GC.GetTotalAllocatedBytes(precise: true) - allocatedBeforeDrain;
            }

            var done = Interlocked.Read(ref finished);
            var first = Interlocked.Read(ref firstClaimTicks);
            var elapsed = Ms(Interlocked.Read(ref lastFinishTicks) - (first < 0 ? startTicks : first)) / 1000;
            result["jobs"] = n;
            result["accepted"] = expected;
            result["failedDispatch"] = failedDispatch;
            result["finished"] = done;
            result["drained"] = done == expected;
            result["drainWriteCommits"] = sqlite?.Runner.Commits - commitsBeforeDrain;
            if (countSqliteStatements)
            {
                result["drainWriterStatements"] = Interlocked.Read(ref sqliteStatements) - statementsBeforeDrain;
            }
            result["startToFirstClaimMs"] = first < 0 ? null : Math.Round(Ms(first - startTicks), 1);
            result["drainSeconds"] = Math.Round(elapsed, 3);
            result["completionsPerSec"] = elapsed > 0 ? Math.Round(done / elapsed, 1) : 0;
            // Two clocks: first claim to last confirmed finish (the published figure), and worker start to the same moment.
            var clocks = (JsonObject)result["clocks"]!;
            var fromStart = clocks["workerStartSeconds"]!.GetValue<double>();
            clocks["firstClaimSeconds"] = Math.Round(elapsed, 3);
            clocks["firstClaimJobsPerSec"] = elapsed > 0 ? Math.Round(done / elapsed, 1) : 0;
            clocks["workerStartJobsPerSec"] = fromStart > 0 ? Math.Round(done / fromStart, 1) : 0;
        }
        else if (mode == "work")
        {
            // --mode work (the second worker of a --lease-crash run): no dispatch; work what the store holds until --n load jobs
            // have finished or --seconds have passed.
            await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
            var until = clock.Elapsed + TimeSpan.FromSeconds(seconds);
            while (Interlocked.Read(ref finished) < n && clock.Elapsed < until)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }

            result["jobs"] = n;
            result["finished"] = Interlocked.Read(ref finished);
            result["seconds"] = Math.Round(clock.Elapsed.TotalSeconds - leaseStartSeconds, 1);
        }
        else
        {
            await hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
            var session = storeViewers > 0 || retentionHours is not null ? provider.GetRequiredService<IManagementSession>() : null;
            var caller = session is not null ? new ManagementCaller(new ManagementActor("svc:bench-store", null, "host"),
                [new(ManagementPermissions.SettingsRead), new(ManagementPermissions.StoreDiagnosticsRead), new(ManagementPermissions.RetentionManage)]) : null;
            if (retentionHours is { } hours)
            {
                // A short horizon for every class the policy allows (audit has a 7-day floor), so the retained fixture is eligible.
                var h = TimeSpan.FromHours(hours);
                // A crash-run segment reopens a large, busy store; its first management query can run past its budget, so retry.
                var edit = await RetryBudgetAsync(async () =>
                {
                    var current = await session!.GetRetentionAsync(caller!).ConfigureAwait(false);
                    return await session.EditRetentionPolicyAsync(caller!, Guid.NewGuid(), current.Revision,
                        new RetentionPolicy(h, h, h, TimeSpan.FromDays(7), h, h, h)).ConfigureAwait(false);
                }).ConfigureAwait(false);
                if (!edit.Succeeded && edit.Code != "no_change")
                {
                    throw new InvalidOperationException("retention policy edit failed: " + edit.Code);
                }
            }
            if (storeWarmupSeconds > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(storeWarmupSeconds)).ConfigureAwait(false);
                if (session is not null)
                {
                    _ = await SerializeStoreSnapshotAsync().ConfigureAwait(false);
                }
            }

            var interval = TimeSpan.FromSeconds(1 / rate);
            var startTicks = clock.ElapsedTicks;
            var stopAt = clock.Elapsed + TimeSpan.FromSeconds(seconds);
            long snapshots = 0, snapshotsWithDiagnostics = 0, dispatched = 0;
            var viewers = Task.WhenAll(Enumerable.Range(0, storeViewers).Select(i => Task.Run(() => PollStoreAsync(i))));
            long outboxStaged = 0, outboxStageErrors = 0;
            var outboxStager = outboxSourceCs is null ? Task.CompletedTask : Task.Run(StageOutboxAsync);
            var samples = new JsonArray();
            using var sampling = new CancellationTokenSource();
            var sampler = sampleSeconds > 0 ? Task.Run(SampleAsync) : Task.CompletedTask;
            // G5 --outage-container NAME --outage-at s --outage-seconds d: docker pause the store container for d seconds,
            // s seconds into the offers, while jobs run.
            long pausedAtMs = 0, resumedAtMs = 0;
            var outage = a.OptStr("outage-container") is { } container ? Task.Run(() => OutageAsync(container)) : Task.CompletedTask;
            var next = clock.Elapsed;
            var pending = new List<Task>();
            var failures = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
            // --calls 1 (default): one Dispatch at a time on a fixed schedule, like the River and pg-boss steady modes
            // (a slow call delays the next arrival; the schedule catches up). --calls n: open loop, each arrival sent on
            // schedule with up to n calls in flight (beyond that the producer's local admission refuses it).
            Phase("timed-start");
            await OfferAsync(() => clock.Elapsed < stopAt).ConfigureAwait(false);
            var completedByEndOfOffers = Interlocked.Read(ref finished);
            // A crashing segment keeps offering until it is killed, so the kill lands with jobs queued and (with --job-ms)
            // inside their handler; otherwise the queue drains while the segment writes its result and no job crosses a crash.
            _ = crash ? OfferAsync(() => true) : Task.CompletedTask;
            await outage.ConfigureAwait(false);
            await outboxStager.ConfigureAwait(false);
            if (!crash) // a crashing segment dies with its dispatch calls still in flight
            {
                await Task.WhenAll(pending).ConfigureAwait(false);
            }

            async Task OfferAsync(Func<bool> keepGoing)
            {
                while (keepGoing())
                {
                    var task = DispatchOneAsync(LoadJob.Create(Interlocked.Increment(ref sequence), payload));
                    if (calls == 1)
                    {
                        await task.ConfigureAwait(false);
                    }
                    else
                    {
                        pending.Add(task);
                        if (pending.Count >= 256)
                        {
                            pending.RemoveAll(t => t.IsCompleted);
                        }
                    }

                    next += interval;
                    if (next - clock.Elapsed is { Ticks: > 0 } wait)
                    {
                        await Task.Delay(wait).ConfigureAwait(false);
                    }
                }
            }

            async Task DispatchOneAsync(LoadJob job)
            {
                var started = clock.ElapsedTicks;
                try
                {
                    var offeredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    var receipt = await dispatcher.DispatchAsync(job, delayMs > 0 ? new DispatchOptions { NotBefore = DateTimeOffset.UtcNow.AddMilliseconds(delayMs) } : null).ConfigureAwait(false);
                    enqueueCallLatency.Add(Ms(clock.ElapsedTicks - started));
                    Interlocked.Increment(ref dispatched);
                    var id = receipt.JobId.Value;
                    Ledger('a', id, offeredAtMs);
                    if (finishedAt.TryRemove(id, out var done))
                    {
                        latency.Add(Ms(done - started));
                        return;
                    }

                    dispatchedAt[id] = started;
                    if (finishedAt.TryRemove(id, out done) && dispatchedAt.TryRemove(id, out _))
                    {
                        latency.Add(Ms(done - started)); // the finish landed between the two checks
                    }
                }
                catch (Exception ex) when (ex is RunwickException or NpgsqlException or TimeoutException)
                {
                    Interlocked.Increment(ref failedDispatch);
                    failures.AddOrUpdate(ex is RunwickException rx ? rx.Code : ex.GetType().Name, 1, (_, c) => c + 1);
                }
            }

            var drainUntil = clock.Elapsed + TimeSpan.FromSeconds(crash ? 0 : 60);
            var offersEndedTicks = clock.ElapsedTicks;
            while (Interlocked.Read(ref finished) < Interlocked.Read(ref dispatched) && clock.Elapsed < drainUntil)
            {
                await Task.Delay(5).ConfigureAwait(false);
            }

            Phase("timed-end");
            result["clocks"] = new JsonObject
            {
                ["offerWindowSeconds"] = Math.Round(Ms(offersEndedTicks - startTicks) / 1000, 2),
                ["completedDuringOffers"] = completedByEndOfOffers,
                ["secondsToClearAfterOffers"] = Math.Round(Ms(clock.ElapsedTicks - offersEndedTicks) / 1000, 2),
            };

            // --drain-store-seconds (last crash-run segment): jobs a killed segment held come back after their lease expires,
            // so wait until the store has no unfinished load or outbox job and the outbox source has nothing left to deliver.
            if (a.OptInt("drain-store-seconds") is { } storeDrain)
            {
                var until = clock.Elapsed + TimeSpan.FromSeconds(storeDrain);
                while (await UnfinishedAsync().ConfigureAwait(false) > 0 && clock.Elapsed < until)
                {
                    await Task.Delay(500).ConfigureAwait(false);
                }

                result["unfinishedAfterStoreDrain"] = await UnfinishedAsync().ConfigureAwait(false);
            }

            await sampling.CancelAsync().ConfigureAwait(false);
            await sampler.ConfigureAwait(false);
            if (outageResult is not null)
            {
                // The jobs a handler was running when the store went away, and what became of them.
                var running = outageJobs!.Values.Where(j => j.Starts.Count > 0 && j.Starts[0] < pausedAtMs && !(j.FinishedMs < pausedAtMs)).ToList();
                outageResult["runningAtPause"] = running.Count;
                outageResult["keptLeaseAndFinishedOnce"] = running.Count(j => j.LostMs is null && j.Starts.Count == 1 && j.FinishedMs is not null);
                outageResult["lostLease"] = running.Count(j => j.LostMs is not null);
                outageResult["ranTwice"] = running.Count(j => j.Starts.Count > 1);
                outageResult["finishedAfterStoreBack"] = running.Count(j => j.FinishedMs >= resumedAtMs);
                outageResult["notFinished"] = running.Count(j => j.FinishedMs is null);
                outageResult["allJobsRunMoreThanOnce"] = outageJobs.Values.Count(j => j.Starts.Count > 1);
                result["outage"] = outageResult;
            }

            async Task OutageAsync(string name)
            {
                var at = a.Double("outage-at", 30);
                var pause = a.Double("outage-seconds", 30);
                outageResult = new JsonObject { ["container"] = name, ["atSeconds"] = at, ["seconds"] = pause };
                await Task.Delay(TimeSpan.FromSeconds(at)).ConfigureAwait(false);
                pausedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                try
                {
                    outageResult["pauseExit"] = await DockerAsync("pause", name).ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromSeconds(pause)).ConfigureAwait(false);
                }
                finally
                {
                    outageResult["unpauseExit"] = await DockerAsync("unpause", name).ConfigureAwait(false);
                    resumedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    outageResult["pausedForMs"] = resumedAtMs - pausedAtMs;
                }
            }

            var elapsed = Ms(clock.ElapsedTicks - startTicks) / 1000;
            await viewers.ConfigureAwait(false);
            result["storeViewers"] = storeViewers;
            result["storeWarmupSeconds"] = storeWarmupSeconds;
            result["storeSnapshotPolls"] = snapshots;
            result["storeSnapshotPollsWithDiagnostics"] = snapshotsWithDiagnostics;
            var s = latency.Summarize();
            result["offeredPerSec"] = rate;
            result["offered"] = sequence;
            result["seconds"] = seconds;
            result["inserted"] = dispatched;
            result["dispatchCalls"] = calls;
            result["failedDispatch"] = failedDispatch;
            result["failedDispatchByCode"] = new JsonObject([.. failures.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]);
            result["completed"] = Interlocked.Read(ref finished);
            result["completedByEndOfOffers"] = completedByEndOfOffers;
            result["completionsPerSec"] = Math.Round(Interlocked.Read(ref finished) / elapsed, 1);
            var ec = enqueueCallLatency.Summarize();
            result["delayMs"] = delayMs;
            result["enqueueLatencyMs"] = new JsonObject { ["p50"] = Math.Round(ec.P50Ms, 2), ["p95"] = Math.Round(ec.P95Ms, 2), ["p99"] = Math.Round(ec.P99Ms, 2) };
            result["latencyMs"] = new JsonObject
            {
                ["p50"] = Math.Round(s.P50Ms, 2),
                ["p95"] = Math.Round(s.P95Ms, 2),
                ["p99"] = Math.Round(s.P99Ms, 2),
                ["max"] = Math.Round(s.MaxMs, 2),
            };

            async Task PollStoreAsync(int index)
            {
                var interval = TimeSpan.FromSeconds(5);
                await Task.Delay(interval * (index / (double)storeViewers)).ConfigureAwait(false);
                while (clock.Elapsed < stopAt)
                {
                    var started = Stopwatch.GetTimestamp();
                    var snapshot = await SerializeStoreSnapshotAsync().ConfigureAwait(false);
                    stages.Observe("management:storeSnapshot", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                    Interlocked.Increment(ref snapshots);
                    if (snapshot.Sqlite?.DatabaseFile.Status == StoreSnapshotStatus.Available ||
                        snapshot.Postgres?.Sessions.Status == StoreSnapshotStatus.Available ||
                        snapshot.Redis?.Memory.Status == StoreSnapshotStatus.Available)
                    {
                        Interlocked.Increment(ref snapshotsWithDiagnostics);
                    }

                    if (stopAt - clock.Elapsed <= interval)
                    {
                        break;
                    }

                    await Task.Delay(interval).ConfigureAwait(false);
                }
            }

            async Task<ManagementStoreSnapshot> SerializeStoreSnapshotAsync()
            {
                var snapshot = await session!.GetStoreSnapshotAsync(caller!).ConfigureAwait(false);
                _ = JsonSerializer.SerializeToUtf8Bytes(snapshot, JsonSerializerOptions.Web);
                return snapshot;
            }
            result["latencySamples"] = s.Count;
            if (sampleSeconds > 0)
            {
                result["samples"] = samples;
            }

            if (schedule)
            {
                var scheduler = provider.GetRequiredService<Runwick.Scheduling.SchedulerService>();
                result["schedule"] = new JsonObject
                {
                    ["plannedOccurrences"] = Interlocked.Read(ref scheduler.PlannedOccurrences),
                    ["materializations"] = Interlocked.Read(ref scheduler.Materializations),
                    ["jobsFinished"] = sideFinished.GetValueOrDefault(SideQueues.Schedule),
                };
            }

            if (outboxSourceCs is not null)
            {
                result["outbox"] = new JsonObject
                {
                    ["staged"] = outboxStaged,
                    ["stageErrors"] = outboxStageErrors,
                    ["jobsFinished"] = sideFinished.GetValueOrDefault(SideQueues.Outbox),
                    ["sourceByState"] = await CountOutboxSourceAsync(outboxSourceCs).ConfigureAwait(false),
                };
            }

            if (retentionHours is not null)
            {
                var view = await session!.GetRetentionAsync(caller!).ConfigureAwait(false);
                result["cleanup"] = new JsonObject
                {
                    ["policyRevision"] = view.Revision,
                    ["classes"] = new JsonArray([.. view.Classes.Select(c => (JsonNode?)new JsonObject
                    {
                        ["recordClass"] = c.RecordClass,
                        ["kind"] = c.Kind,
                        ["totalRows"] = c.TotalRows,
                        ["todayRows"] = c.TodayRows,
                        ["todayBatches"] = c.TodayBatches,
                        ["todayFailures"] = c.TodayFailures,
                        ["caughtUpAt"] = c.CaughtUpAt?.ToString("O"),
                        ["lastErrorCode"] = c.LastErrorCode,
                    })]),
                };
            }

            async Task StageOutboxAsync()
            {
                // --outbox-rate r: stage r messages/s, each in its own source transaction, as an application would.
                var outbox = provider.GetRequiredService<Runwick.Outbox.Sqlite.ISqliteOutbox>();
                using var source = new SqliteConnection(outboxSourceCs);
                source.Open();
                var every = TimeSpan.FromSeconds(1 / outboxRate);
                var due = clock.Elapsed;
                while (clock.Elapsed < stopAt)
                {
                    try
                    {
                        using var tx = source.BeginTransaction(deferred: false);
                        var number = Interlocked.Increment(ref outboxStaged).ToString(System.Globalization.CultureInfo.InvariantCulture);
                        await outbox.StageAsync(new G2Out(number, "x"), tx).ConfigureAwait(false);
                        tx.Commit();
                    }
                    catch (Exception ex) when (ex is SqliteException or RunwickException)
                    {
                        Interlocked.Decrement(ref outboxStaged);
                        Interlocked.Increment(ref outboxStageErrors);
                    }

                    due += every;
                    if (due - clock.Elapsed is { Ticks: > 0 } wait)
                    {
                        await Task.Delay(wait).ConfigureAwait(false);
                    }
                }
            }

            async Task SampleAsync()
            {
                // --sample-seconds s: completions, refusals and checkpoint progress over time. A separate connection reads the
                // WAL counters with a NOOP checkpoint, so sampling never queues behind the store's writer.
                using var probe = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = sqliteDirectory!.DatabasePath, Pooling = false }.ConnectionString);
                probe.Open();
                var wal = new FileInfo(sqliteDirectory.DatabasePath + "-wal");
                while (!sampling.IsCancellationRequested)
                {
                    var sample = new JsonObject
                    {
                        ["t"] = Math.Round(Ms(clock.ElapsedTicks - startTicks) / 1000, 1),
                        ["completed"] = Interlocked.Read(ref finished),
                        ["accepted"] = Interlocked.Read(ref dispatched),
                        ["refused"] = Interlocked.Read(ref failedDispatch),
                    };
                    try
                    {
                        using var cmd = probe.CreateCommand();
                        cmd.CommandText = "PRAGMA wal_checkpoint(NOOP)";
                        using var row = cmd.ExecuteReader();
                        row.Read();
                        sample["walBusy"] = row.GetInt32(0) != 0;
                        sample["walFrames"] = row.GetInt32(1);
                        sample["checkpointedFrames"] = row.GetInt32(2);
                    }
                    catch (SqliteException ex)
                    {
                        sample["walError"] = ex.SqliteErrorCode;
                    }

                    wal.Refresh();
                    sample["walBytes"] = wal.Exists ? wal.Length : 0;
                    samples.Add(sample);
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(sampleSeconds), sampling.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }

            async Task<long> UnfinishedAsync()
            {
                using var probe = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = sqliteDirectory!.DatabasePath, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
                await probe.OpenAsync().ConfigureAwait(false);
                using var cmd = probe.CreateCommand();
                cmd.CommandText = $"SELECT count(*) FROM runwick_jobs WHERE application = 'bench' AND queue IN ('{LoadRegistration.Queue}', '{SideQueues.Outbox}') AND terminal_at IS NULL";
                var jobs = (long)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
                return jobs + (outboxSourceCs is null ? 0 : (await CountOutboxSourceAsync(outboxSourceCs).ConfigureAwait(false))
                    .Where(kv => kv.Key is "pending" or "leased").Sum(kv => (long)kv.Value!));
            }
        }

        if (driver is RedisStore groupStore && a.Has("redis-group"))
        {
            result["redisGroup"] = new JsonObject
            {
                ["calls"] = groupStore.GroupedCalls, ["members"] = groupStore.GroupedMembers, ["fallbacks"] = groupStore.GroupFallbacks,
                ["meanGroupSize"] = groupStore.GroupedCalls == 0 ? 0 : Math.Round(groupStore.GroupedMembers / (double)groupStore.GroupedCalls, 2),
            };
        }

        result["finishReplayed"] = Interlocked.Read(ref replayed);
        result["batchClaims"] = engine.Pools.Sum(p => Interlocked.Read(ref p.BatchClaims));
        result["reservesSent"] = engine.Pools.Sum(p => Interlocked.Read(ref p.ReservesSent));
        result["claimsInFlightHighWater"] = engine.Pools.Max(p => p.ClaimsInFlightHighWater);
        result["invocations"] = Interlocked.Read(ref engine.Invocations);
        result["finishesSent"] = Interlocked.Read(ref engine.FinishesSent);
        result["finishManyCalls"] = Interlocked.Read(ref engine.Completion.BatchesSent);
        result["combinedCalls"] = Interlocked.Read(ref engine.Completion.CombinedCalls);
        result["combinedClaimed"] = Interlocked.Read(ref engine.Completion.CombinedClaimed);
        result["combinedFallbacks"] = Interlocked.Read(ref engine.Completion.CombinedFallbacks);
        result["inlineDeclined"] = new JsonObject
        {
            ["busy"] = engine.Pools.Sum(p => Interlocked.Read(ref p.InlineDeclinedBusy)),
            ["closed"] = engine.Pools.Sum(p => Interlocked.Read(ref p.InlineDeclinedClosed)),
            ["staleHint"] = engine.Pools.Sum(p => Interlocked.Read(ref p.InlineDeclinedStale)),
            ["laneShape"] = engine.Pools.Sum(p => Interlocked.Read(ref p.InlineDeclinedLane)),
        };
        if (sqlite is not null)
        {
            result["sqlite"] = new JsonObject
            {
                ["profile"] = sqlite.AcknowledgementProfile,
                ["engine"] = new JsonObject([.. sqlite.EngineFacts.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                ["writeCommits"] = sqlite.Runner.Commits,
                ["busyConflicts"] = sqlite.Runner.BusyConflicts,
                ["writerConnectionsOpened"] = sqlite.Executor.WriterConnectionsOpened,
                ["writerStatements"] = countSqliteStatements ? Interlocked.Read(ref sqliteStatements) : null,
            };
            if (sqliteStatementProfile is not null)
            {
                result["sqlite"]!["statementProfile"] = new JsonArray([.. sqliteStatementProfile.ToArray()
                    .OrderByDescending(item => item.Value.Nanoseconds).Take(25)
                    .Select(item => new JsonObject
                    {
                        ["statementPrefix"] = item.Key,
                        ["calls"] = item.Value.Count,
                        ["elapsedMs"] = item.Value.Nanoseconds / 1_000_000.0,
                    })]);
            }

            if (keepSqlite)
            {
                result["sqlite"]!["databasePath"] = Path.GetFullPath(sqliteDirectory!.DatabasePath);
            }
        }

        using (var process = Process.GetCurrentProcess())
        {
            result["peakWorkingSetBytes"] = process.PeakWorkingSet64;
        }

        if (trace)
        {
            result["finishesSent"] = Interlocked.Read(ref engine.FinishesSent);
            result["finishManyCalls"] = Interlocked.Read(ref engine.Completion.BatchesSent);
            var traced = stages.LatenciesJson();
            result["trace"] = traced;
            result["traceCounters"] = stages.CountersJson();
            // Short views of the trace: how long store calls waited for the single SQLite writer, and due-to-handler pickup.
            result["writerQueueMs"] = new JsonObject([.. traced.Where(kv => kv.Key.EndsWith(":writerQueue", StringComparison.Ordinal))
                .Select(kv => KeyValuePair.Create(kv.Key["phase:".Length..^":writerQueue".Length], (JsonNode?)new JsonObject
                {
                    ["count"] = kv.Value!["count"]!.DeepClone(), ["p99"] = kv.Value["p99Ms"]!.DeepClone(), ["max"] = kv.Value["maxMs"]!.DeepClone(),
                }))]);
            if (traced["stage:due->handlerInvoked"] is { } pickup)
            {
                result["pickupMs"] = new JsonObject { ["p50"] = pickup["p50Ms"]!.DeepClone(), ["p99"] = pickup["p99Ms"]!.DeepClone(), ["max"] = pickup["maxMs"]!.DeepClone() };
            }
        }

        // Whole-process GC cost over the run, to tell GC pauses apart from store stalls.
        result["gc"] = new JsonObject
        {
            ["pauseTotalMs"] = Math.Round(GC.GetTotalPauseDuration().TotalMilliseconds, 1),
            ["pauseTimePercentage"] = GC.GetGCMemoryInfo(GCKind.Any).PauseTimePercentage,
            ["gen0"] = GC.CollectionCount(0), ["gen1"] = GC.CollectionCount(1), ["gen2"] = GC.CollectionCount(2),
            ["allocatedMB"] = Math.Round(GC.GetTotalAllocatedBytes() / 1048576.0, 1),
            ["serverGC"] = System.Runtime.GCSettings.IsServerGC,
        };
        result["diagnostics"] = new JsonArray([.. engine.Health().Diagnostics.Select(x => (JsonNode?)x)]);
        if (leaseReport)
        {
            // G5: what the lease settings cost on this store over the whole run (worker start to here).
            var renews = Interlocked.Read(ref engine.Lease.RenewsSent);
            var done = Interlocked.Read(ref finished);
            var runSeconds = clock.Elapsed.TotalSeconds - leaseStartSeconds;
            var countersAfter = countersBefore is not null ? await storeCounters!().ConfigureAwait(false) : null;
            result["lease"] = new JsonObject
            {
                ["leaseSeconds"] = leaseOptions?.LeaseDuration.TotalSeconds,
                ["renewSeconds"] = leaseOptions?.RenewalInterval.TotalSeconds,
                ["renewJitterSeconds"] = leaseOptions?.RenewalJitter.TotalSeconds,
                ["jobMs"] = a.Double("job-ms", 0),
                ["runSeconds"] = Math.Round(runSeconds, 1),
                ["jobsFinished"] = done,
                ["completionsPerSec"] = Math.Round(done / runSeconds, 2),
                ["renewsSent"] = renews,
                ["renewManyCalls"] = Interlocked.Read(ref engine.Lease.RenewWavesSent),
                ["renewsPerJob"] = done == 0 ? null : Math.Round((double)renews / done, 2),
                ["renewsPerSec"] = Math.Round(renews / runSeconds, 2),
                ["lostByCause"] = new JsonObject([.. lostByCause.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]),
                ["storeCounters"] = countersAfter is null ? null
                    : new JsonObject([.. countersAfter.Select(kv => KeyValuePair.Create(kv.Key,
                        (JsonNode?)Math.Round(kv.Value!.GetValue<double>() - (countersBefore![kv.Key]?.GetValue<double>() ?? 0), 3)))]),
            };
        }

        if (crash)
        {
            // Crash-run segment: write what this process measured, then die without a graceful stop. Leases, the writer
            // connection and the WAL are left as a killed process leaves them; the next segment reopens the file.
            result["crashed"] = true;
            await File.WriteAllTextAsync(a.Str("out"), result.ToJsonString(Json.Indented)).ConfigureAwait(false);
            Process.GetCurrentProcess().Kill();
        }

        if (mode is not ("enqueue" or "bulk")) // those never started the worker
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await hosted.StopAsync(stop.Token).ConfigureAwait(false);
        }

        if (trace && sqlite is not null && sqliteDirectory is not null)
        {
            var wal = new FileInfo(sqliteDirectory.DatabasePath + "-wal");
            result["sqlite"]!["walBytesAfterRun"] = wal.Exists ? wal.Length : 0;
            if (Version.TryParse(sqlite.EngineFacts.GetValueOrDefault("sqlite_version"), out var version) && version >= new Version(3, 51))
            {
                result["sqlite"]!["walAfterRun"] = await ReadCheckpointAsync("PRAGMA wal_checkpoint(NOOP)").ConfigureAwait(false);
                if (snapshotConnection is not null)
                {
                    snapshotConnection.Close();
                    result["sqlite"]!["walAfterReaderRelease"] = await ReadCheckpointAsync("PRAGMA wal_checkpoint(PASSIVE)").ConfigureAwait(false);
                }
            }

            Task<JsonObject> ReadCheckpointAsync(string pragma) =>
                sqlite.Executor.WriteAsync(SqLane.Background, conn =>
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "PRAGMA wal_autocheckpoint; " + pragma;
                    var elapsed = Stopwatch.StartNew();
                    using var rows = cmd.ExecuteReader();
                    rows.Read();
                    var checkpointPages = rows.GetInt32(0);
                    rows.NextResult();
                    rows.Read();
                    return new JsonObject
                    {
                        ["autoCheckpointPages"] = checkpointPages,
                        ["busy"] = rows.GetInt32(0) != 0,
                        ["logFrames"] = rows.GetInt32(1),
                        ["checkpointedFrames"] = rows.GetInt32(2),
                        ["elapsedMs"] = elapsed.Elapsed.TotalMilliseconds,
                    };
                }, CancellationToken.None);
        }

        // The result contract (eng/bench-compare/ci/README.md): every effective setting by name, and the outcome counted in the store.
        result["settings"] = new JsonObject
        {
            ["store"] = store, ["mode"] = mode, ["payloadBytes"] = payload, ["callers"] = calls, ["workerSlots"] = workers,
            ["producerSlots"] = a.OptInt("producer-calls") ?? Math.Max(2, calls), ["bulkChunk"] = chunk,
            ["pgGroup"] = store == "postgres" && !a.Has("no-pg-group"), ["bufferedFinish"] = a.Has("buffered-finish") && !a.Has("no-buffered-finish"),
            ["claimAhead"] = a.OptInt("claim-ahead"), ["claimsInFlight"] = a.OptInt("claims-in-flight"), ["usage"] = !a.Has("no-usage"),
            ["jobMs"] = a.Double("job-ms", 0), ["serverGC"] = System.Runtime.GCSettings.IsServerGC, ["cores"] = Environment.ProcessorCount,
        };
        result["outcome"] = await StoreJobCountsAsync(provider, sqliteDirectory?.DatabasePath).ConfigureAwait(false);

        var json = result.ToJsonString(Json.Indented);
        Console.WriteLine(json);
        if (a.OptStr("out") is { } outFile)
        {
            await File.WriteAllTextAsync(outFile, json).ConfigureAwait(false);
        }

        return 0;
    }

    /// <summary>A phase marker on stderr (stdout is the result JSON); ci/run.py reads the store and process counters when it sees one.</summary>
    internal static void Phase(string name) => Console.Error.WriteLine($"@@PHASE {name} {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");

    /// <summary>
    /// Load-queue jobs by state as the store itself reports them (management job stats, the same for all three stores). States count jobs created in the
    /// last 12 hours; "now:" keys are live. Null when management is not available; an error object when the read fails.
    /// </summary>
    static async Task<JsonObject?> StoreJobCountsAsync(IServiceProvider provider, string? sqliteFile = null)
    {
        if (sqliteFile is not null)
        {
            // SQLite: count straight from the file. The management read has a time budget that 20,000-job runs exceed, and a count must not depend on it.
            try
            {
                using var read = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = sqliteFile, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
                await read.OpenAsync().ConfigureAwait(false);
                using var cmd = read.CreateCommand();
                cmd.CommandText = $"SELECT state, count(*) FROM runwick_jobs WHERE application = 'bench' AND queue = '{LoadRegistration.Queue}' GROUP BY state";
                var counts = new JsonObject();
                foreach (var state in new[] { "pending", "reserved", "running", "retrying", "succeeded", "failed", "cancelled" })
                {
                    counts[state] = 0L;
                }

                using var rows = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await rows.ReadAsync().ConfigureAwait(false))
                {
                    counts[rows.GetString(0)] = rows.GetInt64(1);
                }

                return counts;
            }
            catch (Exception ex) when (ex is SqliteException or InvalidOperationException)
            {
                return new JsonObject { ["error"] = ex.GetType().Name + ": " + ex.Message };
            }
        }

        if (provider.GetService<IManagementSession>() is not { } session)
        {
            return null;
        }

        try
        {
            var caller = new ManagementCaller(new ManagementActor("svc:bench-count", null, "host"), [new(ManagementPermissions.JobsRead)]);
            var now = DateTimeOffset.UtcNow;
            var stats = await RetryBudgetAsync(() => session.GetJobStatsAsync(caller,
                new JobStatsQuery(now.AddHours(-12), now.AddMinutes(5), 1, Queue: LoadRegistration.Queue))).ConfigureAwait(false);
            var counts = new JsonObject();
            foreach (var (state, count) in stats.States)
            {
                counts[state] = count;
            }

            foreach (var (state, count) in stats.Now)
            {
                counts["now:" + state] = count;
            }

            return counts;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new JsonObject { ["error"] = ex.GetType().Name + ": " + ex.Message };
        }
    }

    static double Ms(long stopwatchTicks) => stopwatchTicks * 1000.0 / Stopwatch.Frequency;

    static async Task<T> RetryBudgetAsync<T>(Func<Task<T>> call)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await call().ConfigureAwait(false);
            }
            catch (Runwick.Management.ManagementQueryBudgetException) when (attempt < 10)
            {
                await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
            }
        }
    }

    static async Task<int> DockerAsync(string verb, string container)
    {
        using var docker = Process.Start(new ProcessStartInfo("docker", [verb, container]) { UseShellExecute = false })!;
        await docker.WaitForExitAsync().ConfigureAwait(false);
        return docker.ExitCode;
    }

    /// <summary>Redis's own cumulative counters, summed over every primary: command calls and time, and server CPU seconds.</summary>
    internal static async Task<JsonObject> RedisCountersAsync(string configuration)
    {
        using var redis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(configuration + ",allowAdmin=true").ConfigureAwait(false);
        var totals = new SortedDictionary<string, double>(StringComparer.Ordinal);
        void Add(string key, double value) => totals[key] = totals.GetValueOrDefault(key) + value;
        foreach (var server in redis.GetServers().Where(s => s.IsConnected && !s.IsReplica))
        {
            foreach (var section in await server.InfoAsync("all").ConfigureAwait(false))
            {
                foreach (var (key, value) in section)
                {
                    if (key is "used_cpu_sys" or "used_cpu_user")
                    {
                        Add("cpuSeconds", double.Parse(value, System.Globalization.CultureInfo.InvariantCulture));
                    }
                    else if (key.StartsWith("cmdstat_", StringComparison.Ordinal))
                    {
                        // calls=12,usec=345,usec_per_call=...
                        var fields = value.Split(',').Select(f => f.Split('=')).ToDictionary(f => f[0], f => f[1], StringComparer.Ordinal);
                        var calls = double.Parse(fields["calls"], System.Globalization.CultureInfo.InvariantCulture);
                        Add("commandCalls", calls);
                        Add("commandUsec", double.Parse(fields["usec"], System.Globalization.CultureInfo.InvariantCulture));
                        Add("calls:" + key[8..], calls);
                    }
                }
            }
        }

        return new JsonObject([.. totals.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))]);
    }

    /// <summary>
    /// PostgreSQL's own cumulative counters for the bench database: transactions, rows written, and (with --trace, which turns on
    /// track_functions) calls and server time of each runwick function. PostgreSQL does not report its CPU.
    /// </summary>
    internal static async Task<JsonObject> PostgresCountersAsync(string connectionString, string database)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("""
            SELECT 'walBytes', pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::float8
            UNION ALL SELECT 'transactions', (xact_commit + xact_rollback)::float8 FROM pg_stat_database WHERE datname = @db
            UNION ALL SELECT 'rowsWritten', (tup_inserted + tup_updated + tup_deleted)::float8 FROM pg_stat_database WHERE datname = @db
            UNION ALL SELECT 'functionCalls:' || funcname, calls::float8 FROM pg_stat_user_functions WHERE schemaname = 'runwick'
            UNION ALL SELECT 'functionMs:' || funcname, total_time FROM pg_stat_user_functions WHERE schemaname = 'runwick'
            """, connection);
        cmd.Parameters.AddWithValue("db", database);
        var counters = new JsonObject();
        await using var rows = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        while (await rows.ReadAsync().ConfigureAwait(false))
        {
            counters[rows.GetString(0)] = rows.GetDouble(1);
        }

        return counters;
    }

    internal static async Task<JsonObject> CountOutboxSourceAsync(string sourceCs)
    {
        using var source = new SqliteConnection(new SqliteConnectionStringBuilder(sourceCs) { Mode = SqliteOpenMode.ReadOnly }.ConnectionString);
        await source.OpenAsync().ConfigureAwait(false);
        using var cmd = source.CreateCommand();
        cmd.CommandText = "SELECT state, count(*) FROM runwick_outbox_messages GROUP BY state ORDER BY state";
        using var rows = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        var counts = new JsonObject();
        while (await rows.ReadAsync().ConfigureAwait(false))
        {
            counts[rows.GetString(0)] = rows.GetInt64(1);
        }

        return counts;
    }

    sealed class HostedGroup(IReadOnlyList<IHostedService> services)
    {
        public async Task StartAsync(CancellationToken ct)
        {
            foreach (var service in services)
            {
                await service.StartAsync(ct).ConfigureAwait(false);
            }
        }

        public async Task StopAsync(CancellationToken ct)
        {
            foreach (var service in services.Reverse())
            {
                await service.StopAsync(ct).ConfigureAwait(false);
            }
        }
    }
}

/// <summary>One load job's handler starts, first lost lease and confirmed finish (wall-clock ms), for the outage report.</summary>
internal sealed class OutageJob
{
    public List<long> Starts { get; } = [];
    public long? LostMs { get; set; }
    public long? FinishedMs { get; set; }
}

internal static class SideQueues
{
    public const string Schedule = "schedule";
    public const string Outbox = "outbox";
}
