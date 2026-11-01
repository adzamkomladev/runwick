using System.Collections.Concurrent;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Worker;
using Runwick.Worker.Scaling;

namespace Runwick.Postgres.Tests;

/// <summary>
/// SP-014 owed cases on the real store (15 §7, 11 §7): the two-worker share, per-lane truncation, the estimate deadline,
/// the automatic-mode parts of CAP-01 and CAP-10, and the capacity-control §6 scaler rows (SCL-02 … SCL-05, 15 I-1).
/// </summary>
public sealed class ScalerCaseTests
{
    static readonly ControlActor Actor = new("user:ops", null, "host", "scaling");

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(120);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static Task<StoreReply<PoolOverride>> OverrideAsync(TestClient c, WorkerHost w, int? target, long expected) =>
        ((IScalingStore)w.Engine.Store).SetPoolOverrideAsync(
            c.Command(new PoolOverrideRequest(w.Engine.WorkerInstanceId, "main", target, null, expected, Actor)), CancellationToken.None);

    /// <summary>Records every distinct scaler decision of a pool, with the pressure state and target seen at the time.</summary>
    sealed class DecisionLog : IAsyncDisposable
    {
        readonly CapacityFaultTests.Sampler _sampler;
        ScalerDecision? _last;

        public DecisionLog(WorkerHost w)
        {
            var pool = w.Engine.Pools.Single();
            _sampler = new CapacityFaultTests.Sampler(() =>
            {
                var stopped = w.Engine.PressureStopped;
                Targets.Enqueue((DateTime.UtcNow, pool.Gate.Target, stopped, w.Engine.Entries.Count));
                if (pool.LastDecision is { } d && !ReferenceEquals(d, _last))
                {
                    _last = d;
                    Decisions.Enqueue((DateTime.UtcNow, d, stopped));
                }
            }, periodMs: 10);
        }

        public ConcurrentQueue<(DateTime At, ScalerDecision Decision, bool Stopped)> Decisions { get; } = new();

        public ConcurrentQueue<(DateTime At, int Target, bool Stopped, int Entries)> Targets { get; } = new();

        bool _disposed;

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                await _sampler.DisposeAsync();
            }
        }
    }

    // ------------------------------------------------------------------ two-worker share (CAP-08, SCL-02)

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-014")]
    [Trait("Case", "CAP-08")]
    [Trait("Case", "SCL-02")]
    public async Task Two_workers_on_one_queue_share_the_backlog_and_the_survivor_takes_it_all()
    {
        // Each worker holds one hung test.ping in an Auto(1, 1) pool, so 240 due test.work rows stay due and the service
        // estimate for test.work stays cold (1 s, no live test.work ticket). After one full occupancy window (mean 1.0):
        // shared, each worker's queued work is 240 × ½ × 1 s → desired ceil((1 + 120/30)/0.8) = 7; the two shares add
        // up to the whole backlog. Alone, the survivor's is 240 s → ceil((1 + 240/30)/0.8) = 12.
        await using var f = await DriverFixture.StartAsync(Ns("share"));
        WorkerHost Host(string id) => WorkerHost.Create(f.ClientConnectionString(id), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(1, 1), configure: r => r.AddHandler<PingJob, ScriptedPingHandler>());
        await using var a = Host("w1");
        await using var b = Host("w2");
        foreach (var w in new[] { a, b })
        {
            w.Script.Default = (_, _, ct) => Task.Delay(Timeout.InfiniteTimeSpan, ct);
        }

        await a.Dispatcher.DispatchAsync(new PingJob("p1"));
        await a.Dispatcher.DispatchAsync(new PingJob("p2"));
        for (var batch = 0; batch < 3; batch++)
        {
            await a.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, 80).Select(i => new BatchDispatchItem(new WorkJob($"w{batch}-{i}")))]);
        }

        await a.StartAsync();
        await WorkerHost.WaitUntil(() => a.Script.Invocations.Count == 1, Wait, "worker 1 holds a ping");
        await b.StartAsync();
        await WorkerHost.WaitUntil(() => b.Script.Invocations.Count == 1, Wait, "worker 2 holds a ping");
        Assert.All(new[] { a, b }, w => Assert.StartsWith("ping:", w.Script.Invocations.Single().JobKey, StringComparison.Ordinal));

        var ct = TestContext.Current.CancellationToken;
        foreach (var w in new[] { a, b })
        {
            var e = await ((IScalingStore)w.Engine.Store).EstimateDemandAsync(f.Application, w.Engine.WorkerInstanceId, ["default"], w.Engine.Contracts, ct);
            Assert.Equal(2, e.LiveWorkers);
            var row = Assert.Single(e.Backlog);
            Assert.Equal(("test.work", 240L, 2), (row.Contract.Name, row.Due, row.CapableInstances));
        }

        // A full 30 s occupancy window at one ticket, then a fresh decision from each worker.
        await Task.Delay(ScalerRules.OccupancyWindow + TimeSpan.FromSeconds(2), ct);
        var shared = new[] { a, b }.Select(w => w.Engine.Pools.Single()).ToList();
        var marks = shared.Select(p => p.LastDecision).ToList();
        await WorkerHost.WaitUntil(() => shared.Select((p, i) => p.LastDecision is { Desired: not null } d && !ReferenceEquals(d, marks[i])).All(x => x),
            Wait, "a fresh decision from each worker");
        Assert.Equal(7, PoolScaler.Desired(1, 120));
        Assert.All(shared, p => Assert.Equal(7, p.LastDecision!.Desired));
        Assert.All(shared, p => Assert.Equal(1, p.Gate.Target));

        // The second worker leaves: the survivor's share becomes the whole backlog. Its interrupted ping becomes due again;
        // it is cancelled so that only test.work demand remains (a due ping would add the survivor's live ping duration).
        await b.StopAsync(ct);
        var c = await f.OpenClient();
        var ping = await WaitingPingAsync(f);
        var position = (await DispatchSupport.TextAsync(f, "SELECT generation || '/' || revision FROM runwick.jobs WHERE job_id = @j", ("j", ping)))
            .Single().Split('/');
        var cancel = await c.Store.CancelAsync(c.Command(new CancelRequest(ping, int.Parse(position[0], System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(position[1], System.Globalization.CultureInfo.InvariantCulture), Actor)), ct);
        Assert.Equal(StoreStatus.Applied, cancel.Status);
        var survivor = shared[0];
        await WorkerHost.WaitUntil(() => survivor.LastDecision is { Desired: 12 }, Wait, "the survivor's demand doubles");
        Assert.Equal(12, PoolScaler.Desired(1, 240));
        var alone = await ((IScalingStore)a.Engine.Store).EstimateDemandAsync(f.Application, a.Engine.WorkerInstanceId, ["default"], a.Engine.Contracts, ct);
        Assert.Equal((1, 1), (alone.LiveWorkers, alone.Backlog.Single(r => r.Contract.Name == "test.work").CapableInstances));

        // Max 1 is a durable bound of this pool: no worker ever ran test.work.
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_name = 'test.work' AND state <> 'pending'"));
    }

    static async Task<Guid> WaitingPingAsync(DriverFixture f)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (true)
        {
            var ids = await DispatchSupport.TextAsync(f, "SELECT job_id::text FROM runwick.jobs WHERE job_name = 'test.ping' AND state IN ('pending', 'retrying')");
            if (ids.Count == 1)
            {
                return Guid.Parse(ids[0]);
            }

            Assert.True(DateTime.UtcNow < deadline, "the stopped worker's ping is not waiting");
            await Task.Delay(100);
        }
    }

    // ------------------------------------------------------------------ per-lane truncation (CAP-06)

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-014")]
    [Trait("Case", "CAP-06")]
    public async Task Each_lane_counts_at_most_2000_due_rows_and_a_full_lane_sets_truncated()
    {
        await using var f = await DriverFixture.StartAsync(Ns("trunc"));
        var c = await f.OpenClient(configure: r => WorkerHost.RegisterWork(r));
        var ct = TestContext.Current.CancellationToken;
        async Task DispatchAsync(int count, string priority, DateTimeOffset? notBefore = null, string tag = "")
        {
            var options = new DispatchOptions { Priority = priority, NotBefore = notBefore };
            for (var done = 0; done < count; done += 100)
            {
                await c.Dispatcher.DispatchManyAsync([.. Enumerable.Range(done, Math.Min(100, count - done))
                    .Select(i => new BatchDispatchItem(new WorkJob($"{priority}-{notBefore is null}-{tag}{i}"), options))], cancellationToken: ct);
            }
        }

        Task<DemandEstimate> EstimateAsync() => c.Store.EstimateDemandAsync(f.Application, Guid.CreateVersion7(), ["default"], c.Contracts, ct);

        // Future-eligible rows are never due; 1,999 + 3 due rows in two lanes are below the cap.
        await DispatchAsync(1_999, "high");
        await DispatchAsync(3, "low");
        await DispatchAsync(5, "medium", DateTimeOffset.UtcNow + TimeSpan.FromHours(1));
        var below = await EstimateAsync();
        Assert.False(below.Truncated);
        Assert.Equal(2_002, Assert.Single(below.Backlog).Due);

        // A lane that reaches the cap sets Truncated: the read cannot tell 2,000 from more, so its count is a lower bound.
        await DispatchAsync(1, "high");
        var full = await EstimateAsync();
        Assert.True(full.Truncated);
        Assert.Equal(2_003, Assert.Single(full.Backlog).Due);

        // 2,001 rows in the high lane: that lane still reports 2,000. The cap is per lane, not per estimate: the low
        // lane's three rows are still counted.
        await DispatchAsync(1, "high", null, "more");
        var over = await EstimateAsync();
        Assert.True(over.Truncated);
        Assert.Equal(2_003, Assert.Single(over.Backlog).Due);
        Assert.Equal(2_001, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE priority = 'high'"));
    }

    // ------------------------------------------------------------------ estimate deadline (CAP-07, CAP-11)

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-014")]
    [Trait("Case", "CAP-07")]
    [Trait("Case", "CAP-11")]
    public async Task A_late_estimate_is_unknown_demand_and_never_overlaps_the_next()
    {
        // The estimate reads pool_controls; an exclusive lock on it (nothing else in the engine touches the table) makes
        // every estimate outlive its 1 s deadline. Demand is then unknown: the target holds although the backlog would
        // grow it at the third sample, and the timed-out statement is gone before the next estimate starts. (The watcher
        // matches the estimate by its first line: pg_stat_activity truncates query text at 1 KiB by default.)
        await using var f = await DriverFixture.StartAsync(Ns("late"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(1, 4));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.Default = (_, _, _) => release.Task;
        await w.DispatchManyAsyncFor(Enumerable.Range(0, 60).Select(i => "late" + i));

        await using var holder = new NpgsqlConnection(f.AdminConnectionString);
        await holder.OpenAsync();
        var tx = await holder.BeginTransactionAsync();
        await using (var l = new NpgsqlCommand("LOCK TABLE runwick.pool_controls IN ACCESS EXCLUSIVE MODE", holder, tx))
        {
            await l.ExecuteNonQueryAsync();
        }

        await using var watcher = new NpgsqlConnection(f.AdminConnectionString);
        await watcher.OpenAsync();
        var maxWaiting = 0L;
        var watching = new CancellationTokenSource();
        var watch = Task.Run(async () =>
        {
            await using var q = new NpgsqlCommand("""
                SELECT count(*) FROM pg_stat_activity
                WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE 'WITH gate AS%'
                """, watcher);
            while (!watching.IsCancellationRequested)
            {
                maxWaiting = Math.Max(maxWaiting, (long)(await q.ExecuteScalarAsync())!);

                await Task.Delay(50);
            }
        });

        var pool = w.Engine.Pools.Single();
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => Interlocked.Read(ref w.Engine.ScalerEstimateFailures) >= 5, Wait, "five late estimates");
        Assert.Equal(1, pool.Gate.Target);
        Assert.Equal("demand_unknown", pool.LastDecision!.Reason);
        Assert.Single(w.Script.Invocations);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.scaling_events"));

        // Known demand again: growth resumes by normal rules (three fresh samples), and late estimates stop.
        await tx.RollbackAsync();
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var failures = Interlocked.Read(ref w.Engine.ScalerEstimateFailures);
        await WorkerHost.WaitUntil(() => pool.Gate.Target >= 2, Wait, "growth after the estimate recovers");
        Assert.Equal(failures, Interlocked.Read(ref w.Engine.ScalerEstimateFailures));
        await watching.CancelAsync();
        await watch;
        Assert.InRange(maxWaiting, 1, 1);

        release.SetResult();
        await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'", 60, "backlog drained");
        await w.StopAsync();
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    // ------------------------------------------------------------------ CAP-01 auto, CAP-12, WRK-02, SCL-03

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-014")]
    [Trait("Case", "CAP-01")]
    [Trait("Case", "CAP-12")]
    [Trait("Case", "WRK-02")]
    [Trait("Case", "SCL-03")]
    public async Task Automatic_targets_bound_hung_handlers_and_reductions_cancel_nothing()
    {
        // CAP-01 automatic-mode runtime part (07 C-8, deferred to SP-014): an Auto(1, 4) pool with hung handlers never holds
        // more than its target or its maximum; a target reduction and a target of 0 stop refill only.
        await using var f = await DriverFixture.StartAsync(Ns("cap01a"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(1, 4));
        var gates = new ConcurrentDictionary<string, TaskCompletionSource>(StringComparer.Ordinal);
        var cancelled = 0;
        w.Script.Default = async (job, _, ct) =>
        {
            using var reg = ct.Register(() => Interlocked.Increment(ref cancelled));
            await gates.GetOrAdd(job.Id, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        };
        await w.DispatchManyAsyncFor(Enumerable.Range(0, 10).Select(i => "hung" + i));
        var c = await f.OpenClient();
        var pool = w.Engine.Pools.Single();
        var maxEntries = 0;
        var maxOutstanding = 0;
        var sampler = new CapacityFaultTests.Sampler(() =>
        {
            maxEntries = Math.Max(maxEntries, w.Engine.Entries.Count);
            maxOutstanding = Math.Max(maxOutstanding, pool.Gate.Outstanding);
        });

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 1, Wait, "the initial target admits one");
        // An override above the maximum is clamped to it: four admitted, never more.
        Assert.Equal(StoreStatus.Applied, (await OverrideAsync(c, w, 50, 0)).Status);
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 4, Wait, "the clamped target admits four");
        // The applying sample reports the clamp; any later sample while the override holds reports `override_active`.
        Assert.Contains(pool.LastDecision!.Reason, new[] { "override_clamped_to_max", "override_active" });
        Assert.Equal(4, pool.Gate.Target);
        await Task.Delay(TimeSpan.FromSeconds(6));
        Assert.Equal((4, 4), (pool.Gate.Target, w.Script.Invocations.Count));

        // Reduction to 1 with four hung handlers: nothing is cancelled; outstanding exceeds the target until holders retire.
        Assert.Equal(StoreStatus.Applied, (await OverrideAsync(c, w, 1, 1)).Status);
        await WorkerHost.WaitUntil(() => pool.Gate.Target == 1, Wait, "reduction applied");
        Assert.Equal(4, pool.Gate.Outstanding);
        var running = w.Script.Invocations.Select(i => i.JobKey).ToList();
        foreach (var key in running.Take(3))
        {
            gates[key].SetResult();
        }

        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 3, Wait, "three holders retire");
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(4, w.Script.Invocations.Count);
        gates[running[3]].SetResult();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 5, Wait, "refill below the reduced target");
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(5, w.Script.Invocations.Count);

        // Target 0 suspends admission: the running handler finishes normally and nothing replaces it.
        Assert.Equal(StoreStatus.Applied, (await OverrideAsync(c, w, 0, 2)).Status);
        await WorkerHost.WaitUntil(() => pool.Gate.Target == 0, Wait, "target 0 applied");
        gates[w.Script.Invocations.Last().JobKey].SetResult();
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 5, Wait, "the fifth completes");
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(5, w.Script.Invocations.Count);
        Assert.Equal(5, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));

        await sampler.DisposeAsync();
        Assert.Equal((4, 4), (maxEntries, maxOutstanding));
        Assert.Equal(0, cancelled);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state IN ('cancelled', 'failed')"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        // History: 4 → 1 and 1 → 0 always. The clamped override writes a row only if it changed the target: when the
        // automatic loop had already reached 4 it is no transition (11 §5), so that row may be absent. Never above max.
        Assert.Equal(["0/1", "1/4"], await DispatchSupport.TextAsync(f, """
            SELECT new_target || '/' || old_target FROM runwick.scaling_events WHERE reason LIKE 'override%' AND new_target < 4 ORDER BY new_target
            """));
        Assert.InRange(await f.CountAsync("SELECT count(*) FROM runwick.scaling_events WHERE reason LIKE 'override%'"), 2, 3);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.scaling_events WHERE new_target > 4"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    // ------------------------------------------------------------------ CAP-10 auto (growth freeze)

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-014")]
    [Trait("Case", "CAP-10")]
    public async Task A_pressure_stop_freezes_growth_and_resets_its_evidence()
    {
        // CAP-10 autoscaler growth-freeze part (07, deferred to SP-014). Default timing. The first job's handler takes a
        // row lock on its own job and returns, so its Finish waits behind the lock and admission stops (the waiting Finish
        // is reported as control wait or, after 5 s, as the oldest published Finish). Meanwhile 40 due rows ask an Auto(1, 8)
        // pool to grow. While stopped every growth sample is frozen and resets evidence; once admission resumes, growth needs
        // three fresh samples.
        await using var f = await DriverFixture.StartAsync(Ns("cap10a"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(1, 8), divisor: 1);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var holder = new NpgsqlConnection(f.AdminConnectionString);
        NpgsqlTransaction? held = null;
        w.Script.On("first", async (_, ctx, _) =>
        {
            await holder.OpenAsync(CancellationToken.None);
            held = await holder.BeginTransactionAsync(CancellationToken.None);
            await using var l = new NpgsqlCommand("SELECT 1 FROM runwick.jobs WHERE job_id = @j FOR UPDATE", holder, held);
            l.Parameters.AddWithValue("j", ctx.JobId.Value);
            await l.ExecuteNonQueryAsync(CancellationToken.None);
        });
        w.Script.Default = (_, _, _) => release.Task;
        await w.DispatchAsync("first");
        await w.DispatchManyAsyncFor(Enumerable.Range(0, 40).Select(i => "due" + i));

        var pool = w.Engine.Pools.Single();
        await using var log = new DecisionLog(w);
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Events.Any(e => e.Name.StartsWith("pressureStop:", StringComparison.Ordinal)), Wait,
            "the slow Finish stops admission");
        await WorkerHost.WaitUntil(() => log.Decisions.Any(d => d.Decision.Reason == "frozen:pressure"), Wait, "a frozen growth sample");
        await Task.Delay(TimeSpan.FromSeconds(6));
        var releasedAt = DateTime.UtcNow;
        await held!.RollbackAsync();
        await WorkerHost.WaitUntil(() => w.EventCount("pressureResume") >= 1, Wait, "admission resumes");
        await WorkerHost.WaitUntil(() => log.Decisions.Any(d => d.At > releasedAt && d.Decision.Action == ScalerAction.Grow), Wait,
            "growth after the stop");
        await log.DisposeAsync();

        // A short stop can come and go before the long one (a transient control wait); growth outside a stop is allowed.
        // What CAP-10 requires: no growth while stopped, and growth after the stop only on fresh evidence.
        var entries = log.Decisions.ToList();
        var decisions = entries.Select(d => d.Decision).ToList();
        var trace = string.Join("; ", entries.Select(d => $"{d.Decision.Action}/{d.Decision.Reason}/{(d.Stopped ? "stopped" : "open")}"));
        Assert.All(log.Targets.Where(t => t.Stopped && t.At < releasedAt), t => Assert.Equal(1, t.Target));
        Assert.DoesNotContain(entries, d => d.Stopped && d.Decision.Action == ScalerAction.Grow);
        var firstGrow = entries.FindIndex(d => d.At > releasedAt && d.Decision.Action == ScalerAction.Grow);
        var lastFrozen = decisions.FindLastIndex(firstGrow, d => d.Reason == "frozen:pressure");
        Assert.True(lastFrozen >= 0, "no frozen sample before the growth: " + trace);
        Assert.Equal(["grow_evidence", "grow_evidence"], decisions.Skip(firstGrow - 2).Take(2).Select(d => d.Reason));
        Assert.True(firstGrow - lastFrozen >= 3, "growth did not wait for three fresh samples after the freeze: " + trace);

        release.SetResult();
        await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'", 41, "all succeeded");
        await w.StopAsync();
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}

/// <summary>CAP-11 automatic part and SCL-04: restarts the shared server, so it runs alone (07 §2.6, collection StoreRestart).</summary>
[Collection("StoreRestart")]
public sealed class ScalerOutageTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-014")]
    [Trait("Case", "CAP-11")]
    [Trait("Case", "SCL-04")]
    [Trait("RequiresDocker", "true")]
    public async Task A_store_outage_freezes_automatic_targets_with_pending_finishes()
    {
        // capacity-control §6 "storage outage and pending finishes": during RestartStore(Kill, 60 s) every estimate fails,
        // so demand is unknown and the target holds; handlers released during the outage leave Finishes pending, and the
        // pool never admits past its maximum or starts a scale-up feedback loop. After the outage the backlog drains.
        await using var f = await DriverFixture.StartAsync(Ns("scl04"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(2, 4));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        w.Script.Default = (_, _, _) => release.Task;
        await w.DispatchManyAsyncFor(Enumerable.Range(0, 12).Select(i => "out" + i));
        var pool = w.Engine.Pools.Single();
        var samples = new ConcurrentQueue<(DateTime At, int Target, int Entries)>();
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 2, TimeSpan.FromSeconds(60), "two running");

        DateTime from, to;
        long failuresBefore;
        await using (new CapacityFaultTests.Sampler(() => samples.Enqueue((DateTime.UtcNow, pool.Gate.Target, w.Engine.Entries.Count)), periodMs: 20))
        {
            failuresBefore = Interlocked.Read(ref w.Engine.ScalerEstimateFailures);
            from = DateTime.UtcNow;
            var releaseLater = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                release.SetResult();
            });
            await f.RestartStore(RestartMode.Kill, TimeSpan.FromSeconds(60));
            to = DateTime.UtcNow;
            await releaseLater;
            await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state NOT IN ('succeeded', 'failed')", 0, "drained after the outage");
        }

        await w.StopAsync();

        // One sample interval after the kill, no estimate can have succeeded: the target is constant until the restart.
        var during = samples.Where(s => s.At >= from + TimeSpan.FromSeconds(6) && s.At <= to).ToList();
        Assert.NotEmpty(during);
        Assert.Single(during.Select(s => s.Target).Distinct());
        Assert.All(samples, s => Assert.InRange(s.Entries, 0, 4));
        Assert.All(samples, s => Assert.InRange(s.Target, 2, 4));
        Assert.True(Interlocked.Read(ref w.Engine.ScalerEstimateFailures) - failuresBefore >= 5, "the outage produced no unknown estimates");
        Assert.Equal(12, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
        Assert.InRange(await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"), 12, 12 * 3);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}

internal static class ScalerCaseExtensions
{
    /// <summary>Dispatches one <see cref="WorkJob"/> per key, in batches of at most 100.</summary>
    public static async Task DispatchManyAsyncFor(this WorkerHost w, IEnumerable<string> keys)
    {
        foreach (var chunk in keys.Chunk(100))
        {
            await w.Dispatcher.DispatchManyAsync([.. chunk.Select(k => new BatchDispatchItem(new WorkJob(k)))]);
        }
    }
}
