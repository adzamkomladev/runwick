using System.Diagnostics;
using System.Reflection;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Sqlite.Store;

namespace Runwick.Sqlite.Tests;

/// <summary>Multi-process and timing cases share the host's disk and CPU: they run one at a time.</summary>
[CollectionDefinition(nameof(SqliteProcesses), DisableParallelization = true)]
public sealed class SqliteProcesses;

/// <summary>SQLite cases of 20 §12 that need separate OS processes or measure timing (SQL-01, 04, 05, 07, rest of 12).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
[Collection(nameof(SqliteProcesses))]
public sealed class SqliteProcessTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static void Report(string line) => TestContext.Current.SendDiagnosticMessage(line);

    static async Task Until(Func<Task<bool>> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("timed out waiting for " + what);
            }

            await Task.Delay(100, Ct);
        }
    }

    static SqliteConnection Raw(DriverFixture f)
    {
        var conn = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False;Default Timeout=30");
        conn.Open();
        return conn;
    }

    static List<T> Query<T>(SqliteConnection conn, string sql, Func<SqliteDataReader, T> map)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var rows = new List<T>();
        while (r.Read())
        {
            rows.Add(map(r));
        }

        return rows;
    }

    static long WalBytes(DriverFixture f) => File.Exists(f.SqlitePath + "-wal") ? new FileInfo(f.SqlitePath + "-wal").Length : 0;

    static async Task<PeerClient> WorkerPeer(DriverFixture f, string identity, JsonObject? options = null)
    {
        var peer = await f.StartPeer(identity);
        var args = options ?? [];
        args["app"] = f.Application;
        await peer.SendAsync("startWorker", args);
        return peer;
    }

    static async Task<(long Invoked, long Renewed, long Lost)> Stats(IEnumerable<PeerClient> peers)
    {
        long invoked = 0, renewed = 0, lost = 0;
        foreach (var p in peers)
        {
            var s = await p.SendAsync("workerStats");
            invoked += s["invoked"]!.AsObject().Sum(kv => kv.Value!.GetValue<int>());
            renewed += s["renewed"]!.GetValue<int>();
            lost += s["lost"]!.GetValue<int>();
        }

        return (invoked, renewed, lost);
    }

    // ------------------------------------------------------------------ SQL-01 three workers and a producer on one file

    [Fact]
    [Trait("Case", "SQL-01")]
    public async Task Three_worker_processes_and_a_producer_process_on_one_file_keep_one_valid_history()
    {
        const int Jobs = 2000;
        const int Resources = 16;
        await using var f = await DriverFixture.StartAsync(Ns("sql01"), StoreKind.Sqlite);
        var c = await f.OpenClient(configure: r => WorkerHost.RegisterWork(r, jobs: WorkerHost.Contended));
        var activated = await ((IRateControlStore)c.Driver).ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, new ControlActor("user:ops", null, "host", "sql01"))), Ct);
        Assert.Equal(StoreStatus.Applied, activated.Status);

        var workers = await Task.WhenAll(Enumerable.Range(0, 3).Select(i => WorkerPeer(f, "w" + i, new JsonObject { ["contended"] = true, ["concurrency"] = 8 })));
        var producer = await f.StartPeer("producer");
        await producer.SendAsync("open", new JsonObject { ["app"] = f.Application, ["work"] = true, ["contended"] = true });

        var clock = Stopwatch.StartNew();
        await producer.SendAsync("dispatchWork", new JsonObject { ["prefix"] = "r", ["count"] = Jobs, ["resources"] = Resources }, TimeSpan.FromMinutes(5));
        var dispatchedIn = clock.Elapsed;
        await Until(async () => await f.CountAsync("SELECT count(*) FROM runwick_jobs WHERE state = 'succeeded'") == Jobs, TimeSpan.FromMinutes(6),
            "every job terminal");
        var doneIn = clock.Elapsed;
        var (invoked, _, lost) = await Stats(workers);

        // One valid history: invariants, the ReferenceModel replay of every committed job, one successful attempt each.
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Empty(await f.HistoryViolations());
        Assert.Equal(Jobs, snapshot.Model.Jobs.Count);
        Assert.Equal(Jobs, snapshot.Model.Attempts.Count(a => a.Outcome == "succeeded"));
        Assert.Empty(snapshot.Model.Permits);

        using var conn = Raw(f);

        // No double grant of a capacity-1 permit: the holding intervals [reserved, finished] of one scope never overlap.
        var holds = Query(conn, """
            SELECT q.scope_id, a.reserved_at, a.finished_at FROM runwick_job_attempts AS a
            JOIN runwick_job_concurrency_requirements AS q ON q.application = a.application AND q.job_id = a.job_id
            ORDER BY q.scope_id, a.reserved_at, a.finished_at
            """, r => (Scope: Convert.ToHexString((byte[])r.GetValue(0)), Reserved: r.GetInt64(1), Finished: r.GetInt64(2)));
        Assert.Equal(Resources, holds.Select(h => h.Scope).Distinct().Count());
        var overlaps = holds.Zip(holds.Skip(1)).Count(p => p.First.Scope == p.Second.Scope && p.Second.Reserved < p.First.Finished);
        Assert.Equal(0, overlaps);

        // No rate over-grant: authorized begins in any 1 s window stay within the type rate.
        var begins = Query(conn, """
            SELECT authorized_at FROM runwick_job_attempts
            WHERE authorized_at IS NOT NULL AND (outcome IS NULL OR outcome <> 'admission_deferred') ORDER BY authorized_at
            """, r => r.GetInt64(0));
        var busiest = 0;
        for (int i = 0, j = 0; i < begins.Count; i++)
        {
            while (begins[j] <= begins[i] - 1000)
            {
                j++;
            }

            busiest = Math.Max(busiest, i - j + 1);
        }

        Assert.InRange(busiest, 1, WorkerHost.ContendedRate);
        var deferred = Query(conn, "SELECT count(*) FROM runwick_job_attempts WHERE outcome = 'admission_deferred'", r => r.GetInt64(0))[0];
        Report($"SQL-01: {Jobs} jobs, {Resources} capacity-1 scopes, rate {WorkerHost.ContendedRate}/s; dispatched in {dispatchedIn.TotalSeconds:F1} s, " +
            $"all terminal in {doneIn.TotalSeconds:F1} s; {snapshot.Model.Attempts.Count} attempts ({deferred} rate-deferred); handler calls {invoked}; " +
            $"busiest 1 s window {busiest} begins; lost leases {lost}");
    }

    // ------------------------------------------------------------------ SQL-04 kill -9 at every fault point

    [Theory]
    [Trait("Case", "SQL-04")]
    [InlineData("AfterLocksBeforeFreshTime")]
    [InlineData("BeforeFirstMutation")]
    [InlineData("AfterMutation")]
    [InlineData("BeforeCommit")]
    [InlineData("AfterCommitBeforeReply")]
    public async Task A_writer_killed_at_a_fault_point_leaves_job_receipt_and_permits_in_agreement(string pointName)
    {
        var point = Enum.Parse<FaultPoint>(pointName);
        await using var f = await DriverFixture.StartAsync(Ns("sql04"), StoreKind.Sqlite);
        var peer = await f.OpenPeer("writer");
        var jobId = Guid.Parse((await peer.SendAsync("dispatch", new JsonObject { ["job"] = "resource", ["jobKey"] = "k", ["key"] = "r" }))["jobId"]!.GetValue<string>());
        var reserved = await peer.SendAsync("reserve");
        Assert.Equal("Applied", reserved["status"]!.GetValue<string>());
        var lease = PeerJson.Lease(reserved["lease"]!);
        Assert.Equal("Applied", (await peer.SendAsync("begin", new JsonObject { ["lease"] = reserved["lease"]!.DeepClone() }))["status"]!.GetValue<string>());

        // The in-flight command: Finish with an ID and NotAfter this fixture knows, so it can resend it after the crash.
        var commandId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds());
        await peer.SendAsync("armFault", new JsonObject { ["point"] = point.ToString(), ["action"] = "KillProcess", ["operation"] = Operations.Finish });
        _ = peer.SendInBackground("finish", new JsonObject
        {
            ["lease"] = reserved["lease"]!.DeepClone(), ["commandId"] = commandId.ToString("D"), ["notAfterMs"] = notAfter.ToUnixTimeMilliseconds(),
        });
        await peer.WaitForEventAsync("atBarrier");
        peer.Kill();

        // Restart: a fresh process view of the file (WAL recovery on open).
        var committed = point == FaultPoint.AfterCommitBeforeReply;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var after = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(after.Model));
        Assert.Equal(committed ? "succeeded" : "running", after.Model.Job(jobId).State);
        Assert.Equal(committed ? 0 : 1, after.Model.Permits.Count);
        Assert.Equal(committed ? 1 : 0, after.Model.Receipts.Count(r => r.CommandId == commandId));
        var acknowledged = after.Model.Receipts.Select(r => r.Operation).ToHashSet();
        Assert.Superset(new HashSet<string> { Operations.Dispatch, Operations.Reserve, Operations.Begin }, acknowledged);

        // Resending the in-flight command resolves it once; a second resend replays.
        var first = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: commandId, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, first.Status);
        Assert.Equal(committed, first.Replayed);
        var again = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: commandId, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, again.Status);
        Assert.True(again.Replayed);

        var final = await f.ReadCommittedSnapshot();
        Assert.Equal("succeeded", final.Model.Job(jobId).State);
        Assert.Empty(final.Model.Permits);
        Assert.Single(final.Model.Receipts, r => r.CommandId == commandId);
        Assert.Single(final.Model.AttemptsOf(jobId));
        Assert.Empty(SnapshotInvariants.Violations(final.Model));
        Assert.Empty(await f.HistoryViolations());
    }

    // ------------------------------------------------------------------ SQL-05 long readers, WAL growth, checkpoint

    [Fact]
    [Trait("Case", "SQL-05")]
    public async Task Long_readers_grow_the_wal_and_are_interrupted_while_renewals_keep_their_margin()
    {
        const int Held = 24;
        await using var f = await DriverFixture.StartAsync(Ns("sql05"), StoreKind.Sqlite);
        var c = await f.OpenClient(configure: r =>
        {
            WorkerHost.RegisterWork(r);
            r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20));
        });

        // Divisor 10: lease 6 s, safety margin 1 s, renewal every 1.5 s.
        var workers = await Task.WhenAll(Enumerable.Range(0, 3).Select(i => WorkerPeer(f, "w" + i, new JsonObject { ["concurrency"] = 8, ["divisor"] = 10 })));
        for (var i = 0; i < Held; i++)
        {
            await c.Dispatcher.DispatchAsync(new WorkJob("hold-" + i), cancellationToken: Ct);
        }

        await Until(async () => (await Stats(workers)).Invoked == Held, TimeSpan.FromSeconds(60), "every held job running");
        var walStart = WalBytes(f);
        var walPeak = walStart;
        using var sampling = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            while (!sampling.IsCancellationRequested)
            {
                walPeak = Math.Max(walPeak, WalBytes(f));
                await Task.Delay(50, CancellationToken.None);
            }
        }, CancellationToken.None);
        var finishes = Task.Run(async () =>
        {
            for (var i = 0; i < 400; i++)
            {
                await c.Dispatcher.DispatchAsync(new WorkJob("ok-" + i), cancellationToken: Ct);
            }
        }, Ct);

        // Two store reads whose query never ends: the 1 s budget interrupts them and releases their snapshots (§5.3).
        var runner = c.Services.GetRequiredService<SqRunner>();
        async Task<(double Seconds, string? Code)> EndlessRead()
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await runner.ReadAsync(conn =>
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = """
                        SELECT count(*) FROM runwick_jobs;
                        WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n) SELECT count(*) FROM n;
                        """;
                    using var r = cmd.ExecuteReader();
                    while (r.NextResult())
                    {
                    }

                    return 0;
                }, TimeSpan.FromSeconds(1), Ct);
                return (sw.Elapsed.TotalSeconds, null);
            }
            catch (StoreNotAppliedException ex)
            {
                return (sw.Elapsed.TotalSeconds, ex.Code);
            }
        }

        var interrupted = await Task.WhenAll(EndlessRead(), EndlessRead());
        Assert.All(interrupted, r => Assert.Equal("statement_timeout", r.Code));
        Assert.All(interrupted, r => Assert.InRange(r.Seconds, 0.9, 2.5));

        // Two readers outside the store hold snapshots for 4 s: a checkpoint cannot pass them, the WAL grows.
        var readers = Enumerable.Range(0, 2).Select(_ => Raw(f)).ToList();
        var snapshots = readers.Select(r =>
        {
            var tx = r.BeginTransaction(deferred: true);
            using var cmd = r.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "SELECT count(*) FROM runwick_jobs";
            cmd.ExecuteScalar();
            return tx;
        }).ToList();
        var walAtHold = WalBytes(f);
        await Task.Delay(4000, Ct);
        using var checkpointer = Raw(f);
        (long Busy, long Log, long Done) Checkpoint() =>
            Query(checkpointer, "PRAGMA wal_checkpoint(TRUNCATE)", r => (r.GetInt64(0), r.GetInt64(1), r.GetInt64(2)))[0];
        var blocked = Checkpoint();
        var walHeld = WalBytes(f);
        foreach (var tx in snapshots)
        {
            tx.Commit();
            tx.Dispose();
        }

        readers.ForEach(r => r.Dispose());
        var tries = 0;
        var done = Checkpoint();
        while (done.Busy != 0 && ++tries < 50)
        {
            await Task.Delay(100, Ct);
            done = Checkpoint();
        }

        Assert.Equal(0, done.Busy);
        var walAfter = WalBytes(f);
        await finishes;

        foreach (var w in workers)
        {
            await w.SendAsync("releaseHeld");
        }

        await Until(async () => await f.CountAsync("SELECT count(*) FROM runwick_jobs WHERE state = 'succeeded'") == Held + 400, TimeSpan.FromSeconds(90),
            "every job succeeded");
        await sampling.CancelAsync();
        await sampler;
        var (_, renewed, lost) = await Stats(workers);
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Equal(0, lost);
        Assert.True(renewed > 0);
        Assert.Equal(Held + 400, snapshot.Model.Attempts.Count);
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Report($"SQL-05: store reads interrupted after {interrupted[0].Seconds:F2} s and {interrupted[1].Seconds:F2} s; " +
            $"WAL {walStart / 1024} KiB at start, {walAtHold / 1024} KiB when readers took snapshots, {walHeld / 1024} KiB after 4 s held, peak {walPeak / 1024} KiB; " +
            $"checkpoint while held: busy={blocked.Busy} log={blocked.Log} done={blocked.Done}; after release: busy={done.Busy} after {tries} retries, " +
            $"WAL {walAfter / 1024} KiB; renewals {renewed}, lost leases {lost}, {Held} held jobs each one attempt");
    }

    // ------------------------------------------------------------------ SQL-07 executor timing

    [Fact]
    [Trait("Case", "SQL-07")]
    public async Task While_another_process_holds_the_writer_the_engine_keeps_its_timing_and_renewal_goes_first()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sql07a"), StoreKind.Sqlite);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var host = WorkerHost.Create(f.AdminConnectionString, f.Application, f.Faults, concurrency: 8, divisor: 10,
            configure: r => r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20)));
        host.Script.Default = (_, _, ct) => release.Task.WaitAsync(ct);
        await host.StartAsync();
        for (var i = 0; i < 8; i++)
        {
            await host.DispatchAsync("run-" + i);
        }

        await WorkerHost.WaitUntil(() => host.Script.Invocations.Count == 8, TimeSpan.FromSeconds(30), "8 running jobs");
        var holder = await f.StartPeer("holder");

        // A lease of our own for the ordering check (the producer client shares the file, not the engine).
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20));
        });
        await c.Dispatcher.DispatchAsync(new PingJob("mine"), cancellationToken: Ct);
        var mine = (await c.ReserveAsync(lease: TimeSpan.FromMinutes(1))).Value!.Lease.Handle;

        var threadsBefore = ThreadPool.ThreadCount;
        var renewedBefore = host.EventCount("renewed");
        double lateMs = 0;
        using var sampling = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            while (!sampling.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                await Task.Delay(20, CancellationToken.None);
                lateMs = Math.Max(lateMs, sw.Elapsed.TotalMilliseconds - 20);
            }
        }, CancellationToken.None);

        var hold = holder.SendAsync("holdWriter", new JsonObject { ["cs"] = f.AdminConnectionString, ["ms"] = 1500 });
        await holder.WaitForEventAsync("writerHeld");
        var order = new List<string>();
        var dispatches = Enumerable.Range(0, 6).Select(async i =>
        {
            await c.Dispatcher.DispatchAsync(new PingJob("queued-" + i), cancellationToken: Ct);
            lock (order)
            {
                order.Add("dispatch");
            }
        }).ToList();
        await Task.Delay(150, Ct);
        var renew = Task.Run(async () =>
        {
            var reply = await c.RenewAsync(mine, TimeSpan.FromMinutes(1));
            lock (order)
            {
                order.Add("renew");
            }

            return reply;
        }, Ct);
        await hold;
        Assert.Equal(StoreStatus.Applied, (await renew).Status);
        await Task.WhenAll(dispatches);
        await Task.Delay(2000, Ct);   // past one more renewal interval
        await sampling.CancelAsync();
        await sampler;

        var dispatchesBeforeRenew = order.IndexOf("renew");
        Assert.InRange(dispatchesBeforeRenew, 0, 1);   // at most the one already waiting on the lock when the renewal queued
        Assert.True(lateMs < 500, $"thread pool tick late by {lateMs:F0} ms");
        Assert.True(host.EventCount("renewed") > renewedBefore);
        Assert.DoesNotContain(host.Events, e => e.Name.StartsWith("lost:", StringComparison.Ordinal));
        release.TrySetResult();
        Report($"SQL-07: writer held 1.5 s by another process; worst thread-pool tick lateness {lateMs:F0} ms, pool threads {threadsBefore}→{ThreadPool.ThreadCount}, " +
            $"engine renewals {renewedBefore}→{host.EventCount("renewed")}, no lost lease; renewal served after {dispatchesBeforeRenew} of 6 earlier-queued dispatches");
    }

    [Fact]
    [Trait("Case", "SQL-07")]
    public async Task An_open_caller_staging_transaction_delays_renewals_and_the_watchdog_expires_the_lease_on_time()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sql07b"), StoreKind.Sqlite);
        await using var host = WorkerHost.Create(f.AdminConnectionString, f.Application, f.Faults, concurrency: 1, divisor: 10);
        host.Script.Default = (_, _, ct) => Task.Delay(Timeout.InfiniteTimeSpan, ct);
        await host.StartAsync();
        await host.DispatchAsync("long-1");
        await WorkerHost.WaitUntil(() => host.Script.Invocations.Count == 1, TimeSpan.FromSeconds(30), "the job running");
        await WorkerHost.WaitUntil(() => host.EventCount("renewed") > 0, TimeSpan.FromSeconds(30), "a first renewal");

        using var business = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False;Foreign Keys=True;Default Timeout=1");
        business.Open();
        using var tx = business.BeginTransaction(deferred: false);
        await host.Services.GetRequiredService<ISqliteJobDispatcher>().StageAsync(new WorkJob("staged"), tx, cancellationToken: Ct);
        var renewedAtOpen = host.EventCount("renewed");

        TimeSpan? deadline = null;
        var watch = Stopwatch.StartNew();
        while (!host.Events.Any(e => e.Name.StartsWith("lost:", StringComparison.Ordinal)) && watch.Elapsed < TimeSpan.FromSeconds(20))
        {
            deadline = host.Engine.Entries.FirstOrDefault()?.Deadline ?? deadline;
            await Task.Delay(5, Ct);
        }

        var lost = Assert.Single(host.Events, e => e.Name.StartsWith("lost:", StringComparison.Ordinal));
        tx.Rollback();
        Assert.Equal("lost:" + "SafetyDeadline", lost.Name);
        Assert.Equal(renewedAtOpen, host.EventCount("renewed"));
        Assert.NotNull(deadline);
        var late = lost.At - deadline!.Value;
        var watchdog = TimeSpan.FromSeconds(1) / 10;
        Assert.InRange(late, TimeSpan.Zero, watchdog + TimeSpan.FromMilliseconds(250));
        Report($"SQL-07: caller transaction open {watch.Elapsed.TotalSeconds:F1} s; no renewal committed meanwhile; local lease expired {late.TotalMilliseconds:F0} ms " +
            $"after its deadline (watchdog interval {watchdog.TotalMilliseconds:F0} ms)");
    }

    // ------------------------------------------------------------------ SQL-12 batched vs single, renewal priority

    /// <summary>The store with only IRunwickStore: the engine then sends single operations (no ReserveAndBegin, RenewMany, FinishMany).</summary>
    public class SingleOperationStore : DispatchProxy
    {
        internal object Inner { get; set; } = null!;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            try
            {
                return targetMethod!.Invoke(Inner, args);
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(ex.InnerException);
                throw;
            }
        }
    }

    static async Task<List<string>> RunSeeded(bool batched)
    {
        await using var f = await DriverFixture.StartAsync(Ns(batched ? "sql12b" : "sql12s"), StoreKind.Sqlite);
        await using var host = WorkerHost.Create(f.AdminConnectionString, "seeded", f.Faults, concurrency: 16,
            overrides: batched ? null : s => s.AddSingleton<IRunwickStore>(sp =>
            {
                var proxy = DispatchProxy.Create<IRunwickStore, SingleOperationStore>();
                ((SingleOperationStore)(object)proxy).Inner = sp.GetRequiredService<SqliteStore>();
                return proxy;
            }));
        Assert.Equal(batched, host.Engine.Store is ILeaseBatchStore);
        host.Script.Default = (job, ctx, _) =>
            job.Id.StartsWith("fail", StringComparison.Ordinal) || (job.Id.StartsWith("retry", StringComparison.Ordinal) && ctx.ExecutionNumber == 1)
                ? throw new InvalidOperationException("scripted failure")
                : Task.CompletedTask;
        await host.StartAsync();
        var random = new Random(1912);
        var keys = Enumerable.Range(0, 300).Select(i => random.Next(10) switch { < 7 => "ok", < 9 => "retry", _ => "fail" } + "-" + i).ToList();
        await host.Dispatcher.DispatchManyAsync([.. keys.Select(k => new BatchDispatchItem(new WorkJob(k)))]);
        await Until(async () => await f.CountAsync("SELECT count(*) FROM runwick_jobs WHERE state IN ('succeeded', 'dead')") == keys.Count,
            TimeSpan.FromMinutes(3), "every seeded job terminal");
        await host.StopAsync();
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
        Assert.Empty(await f.HistoryViolations());

        // Normalized: per job (by payload), its state and counters, its attempts' outcomes and its event kinds, in order.
        using var conn = Raw(f);
        return Query(conn, """
            SELECT CAST(p.payload_utf8 AS TEXT) || ' ' || j.state || ' g' || j.generation || ' x' || j.execution_count || ' ' || coalesce(j.terminal_reason, '-')
                || ' [' || coalesce((SELECT group_concat(o, ',') FROM (
                       SELECT coalesce(a.outcome, '-') || ':' || coalesce(a.execution_number, 0) AS o FROM runwick_job_attempts AS a
                       WHERE a.application = j.application AND a.job_id = j.job_id ORDER BY a.reservation_number)), '')
                || '] [' || coalesce((SELECT group_concat(k, ',') FROM (
                       SELECT e.event_kind AS k FROM runwick_queue_events AS e
                       WHERE e.application = j.application AND e.job_id = j.job_id ORDER BY e.event_id)), '') || ']'
            FROM runwick_jobs AS j JOIN runwick_job_payloads AS p ON p.application = j.application AND p.job_id = j.job_id
            ORDER BY 1
            """, r => r.GetString(0));
    }

    [Fact]
    [Trait("Case", "SQL-12")]
    public async Task Batched_and_single_operation_runs_of_one_seeded_workload_leave_identical_snapshots()
    {
        var batched = await RunSeeded(batched: true);
        var single = await RunSeeded(batched: false);
        Assert.Equal(300, batched.Count);
        Assert.Equal(batched, single);
        Report($"SQL-12: 300 seeded jobs, normalized snapshots equal; e.g. {batched[0]}");
    }

    [Fact]
    [Trait("Case", "SQL-12")]
    public async Task A_RenewMany_queued_behind_a_DispatchMany_of_1000_is_served_before_the_next_queued_dispatch()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sql12r"), StoreKind.Sqlite);
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20));
        });
        await c.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, 4).Select(i => new BatchDispatchItem(new PingJob("l" + i)))], cancellationToken: Ct);
        var leases = (await c.ReserveAndBeginAsync(4)).Value!.Outcomes.Select(o => o.Reserved!.Lease.Handle).ToList();

        var barrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.DispatchMany));
        var many = c.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, 1000).Select(i => new BatchDispatchItem(new PingJob("m" + i)))], cancellationToken: Ct);
        await DriverFixture.WaitAtBarrier(barrier);
        var order = new List<string>();
        var next = Task.Run(async () =>
        {
            await c.Dispatcher.DispatchAsync(new PingJob("next"), cancellationToken: Ct);
            lock (order)
            {
                order.Add("dispatch");
            }
        }, Ct);
        await Task.Delay(300, Ct);
        var renew = Task.Run(async () =>
        {
            var reply = await c.RenewManyAsync(leases);
            lock (order)
            {
                order.Add("renewMany");
            }

            return reply;
        }, Ct);
        await Task.Delay(300, Ct);
        Assert.Empty(order);
        DriverFixture.ReleaseBarrier(barrier);
        Assert.Equal(1000, (await many).Items.Count);
        Assert.All((await renew).Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        await next;
        Assert.Equal(["renewMany", "dispatch"], order);
    }
}
