using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Worker;

namespace Runwick.Postgres.Tests;

/// <summary>
/// SP-012 readiness, saturation evidence, presence and the unsupported-backlog diagnostic against the real store
/// (09 §6, 15 §5).
/// </summary>
[Trait("Item", "SP-012")]
public sealed class ReadinessTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static WorkerHost Host(DriverFixture f, int concurrency = 4, Action<RunwickBuilder>? configure = null, Action<PoolBuilder>? pool = null) =>
        WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency, configure: configure, pool: pool);

    static Task<ReadySnapshot> ProbeAsync(WorkerHost w) =>
        w.Engine.Store.ProbeAsync(w.Engine.Application, w.Engine.Readiness.Lanes, w.Engine.Contracts, TestContext.Current.CancellationToken);

    static async Task<long> ReserveReceiptsAsync(DriverFixture f) =>
        await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Reserve'");

    static async Task AssertInvariants(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "RDY-01")]
    [Trait("Case", "WRK-15")]
    public async Task A_saturated_queue_sends_no_reserves_and_writes_nothing_per_job()
    {
        // WRK-15 / RDY-01: a queue scope of 2 held by two hung handlers, a blocked backlog behind it.
        await using var f = await DriverFixture.StartAsync(Ns("rdy01"));
        var release = new TaskCompletionSource();
        await using var w = Host(f, concurrency: 4, configure: r => r.Queue("default", q => q.MaxConcurrent(2)));
        w.Script.Default = (_, _, _) => release.Task;
        const int backlog = 500;
        for (var i = 0; i < backlog; i += 100)
        {
            await w.Dispatcher.DispatchManyAsync([.. Enumerable.Range(i, 100).Select(n => new BatchDispatchItem(new WorkJob("b" + n)))]);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 2, Wait, "the queue scope is full");

        // Let the first CapacityBlocked reply and the probe that observes saturation pass, then measure a window.
        var snapshot = await ProbeAsync(w);
        Assert.Contains(snapshot.Queues, q => q.Queue == "default" && q.Saturated && q.Capacity == 2 && q.Live == 2);
        await Task.Delay(TimeSpan.FromSeconds(3));
        var pool = w.Engine.Pools.Single();
        var reserves = Interlocked.Read(ref pool.ReservesSent);
        var receipts = await ReserveReceiptsAsync(f);
        var probes = Interlocked.Read(ref w.Engine.Readiness.Probes);
        var events = await f.CountAsync("SELECT count(*) FROM runwick.queue_events");
        var window = TimeSpan.FromSeconds(10);
        await Task.Delay(window);

        Assert.Equal(reserves, Interlocked.Read(ref pool.ReservesSent));
        Assert.Equal(receipts, await ReserveReceiptsAsync(f));
        Assert.Equal(events, await f.CountAsync("SELECT count(*) FROM runwick.queue_events"));
        Assert.True(Interlocked.Read(ref pool.SaturationSkips) > 0);

        // Idle backoff caps probing at about one per 2 s ± 20 %; the hard floor is the 50 ms spacing (CAP-04).
        var probed = Interlocked.Read(ref w.Engine.Readiness.Probes) - probes;
        Assert.InRange(probed, 1, (long)(window.TotalSeconds / 1.6) + 3);
        Assert.Equal(2, w.Script.Invocations.Count);
        Assert.Equal(backlog - 2, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));

        release.SetResult();
        // The drain is bounded by one Reserve round trip per admission (selection is serial per pool); on the shared
        // G-2 host that measured about 2.6 jobs/s (2026-11-01), so 500 jobs need more than the earlier 3 minutes.
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == backlog,
            TimeSpan.FromMinutes(8), "the backlog drains once the scope frees");
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "RDY-01")]
    [Trait("Case", "WRK-15")]
    public async Task A_full_type_scope_over_ten_thousand_jobs_costs_no_per_job_transaction()
    {
        // RDY-01 type-scope half (15 §5): test.work has a job-type scope of 2, held by two hung handlers, with 10,000
        // due jobs behind it. Probe/2 reports no queue saturation here (the queue has no scope), so Reserve stays the
        // authority: each blocked Reserve is one rolled-back transaction that examines one candidate, and the lane is
        // single-type, so it cools (250 ms doubling to 2 s, 05 §8.6). The cost is bounded by the cooldown, not the backlog.
        await using var f = await DriverFixture.StartAsync(Ns("rdy01t"));
        var release = new TaskCompletionSource();
        await using var typed = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 4, jobs: j => j.MaxConcurrent(2),
            configure: r => r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(30)));
        typed.Script.Default = (_, _, _) => release.Task;
        const int backlog = 10_000;
        await Task.WhenAll(Enumerable.Range(0, 2).Select(async lane =>
        {
            for (var i = lane * 100; i < backlog; i += 200)
            {
                await typed.Dispatcher.DispatchManyAsync([.. Enumerable.Range(i, 100).Select(n => new BatchDispatchItem(new WorkJob("t" + n)))]);
            }
        }));
        Assert.Equal(backlog, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));

        await typed.StartAsync();
        await WorkerHost.WaitUntil(() => typed.Script.Invocations.Count == 2, Wait, "the type scope is full");
        await Task.Delay(TimeSpan.FromSeconds(4)); // the lane cooldown reaches its 2 s cap

        const string rows = "SELECT md5(string_agg(job_id::text || ':' || xmin::text, ',' ORDER BY job_id)) FROM runwick.jobs WHERE state = 'pending'";
        var pool = typed.Engine.Pools.Single();
        var reserves = Interlocked.Read(ref pool.ReservesSent);
        var receipts = await ReserveReceiptsAsync(f);
        var events = await f.CountAsync("SELECT count(*) FROM runwick.queue_events");
        var attempts = await f.CountAsync("SELECT count(*) FROM runwick.job_attempts");
        var before = (await DispatchSupport.TextAsync(f, rows)).Single();
        var window = TimeSpan.FromSeconds(10);
        await Task.Delay(window);

        // At most one Reserve per cooldown (2 s − 20 %) plus slack; nothing durable is written for the blocked
        // backlog: no pending job row version changes, no receipt, no event, no attempt.
        var sent = Interlocked.Read(ref pool.ReservesSent) - reserves;
        Assert.InRange(sent, 0, (long)(window.TotalSeconds / 1.6) + 4);
        Assert.Equal(receipts, await ReserveReceiptsAsync(f));
        Assert.Equal(events, await f.CountAsync("SELECT count(*) FROM runwick.queue_events"));
        Assert.Equal(attempts, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));
        Assert.Equal(before, (await DispatchSupport.TextAsync(f, rows)).Single());
        Assert.Equal(2, typed.Script.Invocations.Count);
        Assert.Equal(backlog - 2, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));

        // Released, the backlog flows again (two at a time); 50 more are enough to show it, the rest is not drained.
        release.SetResult();
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result >= 50,
            TimeSpan.FromMinutes(3), "the backlog moves once the scope frees");
        await typed.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-04")]
    public async Task A_saturated_queue_does_not_delay_another_queue_and_a_control_change_rechecks_at_once()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap04"));
        var release = new TaskCompletionSource();
        await using var w = Host(f, concurrency: 4, configure: r =>
        {
            r.Queue("default", q => q.MaxConcurrent(1));
            r.Queue("other");
        }, pool: p => p.Queues("other"));
        w.Script.Default = (job, _, _) => job.Id.StartsWith("hold", StringComparison.Ordinal) ? release.Task : Task.CompletedTask;
        await w.Dispatcher.DispatchAsync(new WorkJob("hold"));
        for (var i = 0; i < 20; i++)
        {
            await w.Dispatcher.DispatchAsync(new WorkJob("blocked" + i));
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => !w.Script.Invocations.IsEmpty, Wait, "default saturated");

        // Work on an unrelated queue is discovered while "default" stays excluded.
        var started = DateTime.UtcNow;
        await w.Dispatcher.DispatchAsync(new WorkJob("free"), new DispatchOptions { Queue = "other" });
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("free"), Wait, "the other queue's job ran");
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(2.4 + 2 + 2), "saturation of one queue must not delay another");
        Assert.Equal(1, w.Script.Invocations.Count(i => i.JobKey.StartsWith("hold", StringComparison.Ordinal) || i.JobKey.StartsWith("blocked", StringComparison.Ordinal)));

        // A probe storm (thousands of wakes) still spaces probes ≥ 50 ms apart.
        var before = Interlocked.Read(ref w.Engine.Readiness.Probes);
        var stormStart = DateTime.UtcNow;
        for (var i = 0; i < 5_000; i++)
        {
            w.Engine.Readiness.Invalidate();
            w.Engine.WakePools();
        }

        await Task.Delay(TimeSpan.FromSeconds(1));
        var elapsed = DateTime.UtcNow - stormStart;
        Assert.True(Interlocked.Read(ref w.Engine.Readiness.Probes) - before <= (long)(elapsed.TotalMilliseconds / 50) + 1);

        release.SetResult();
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 22,
            TimeSpan.FromMinutes(2), "all succeeded");
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "RDY-03")]
    public async Task Unsupported_backlog_stays_visible_and_is_diagnosed_without_claims()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rdy03"));
        await using var w = Host(f, configure: r => r.Queue("other"));

        // test.ping is registered but has no handler here: no live worker advertises its tuple. test.work on queue
        // "other" is advertised by this worker, which does not subscribe to "other".
        var ping = (await w.Dispatcher.DispatchAsync(new PingJob("p"))).JobId.Value;
        var parked = (await w.Dispatcher.DispatchAsync(new WorkJob("parked"), new DispatchOptions { Queue = "other" })).JobId.Value;
        await w.StartAsync();
        var supported = (await w.Dispatcher.DispatchAsync(new WorkJob("later"))).JobId.Value;
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("later"), Wait, "a later supported job runs past the unsupported head");

        var snapshot = await ProbeAsync(w);
        Assert.Contains(snapshot.UnsupportedLanes, l => l.Queue == "default");
        var diagnostics = w.Services.GetRequiredService<IReadinessDiagnostics>();
        var backlog = await diagnostics.ListUnsupportedBacklogAsync(f.Application, 256, TestContext.Current.CancellationToken);
        Assert.Equal(1, backlog.LiveWorkers);
        Assert.Contains(backlog.Entries, e => e.Queue == "default" && e.Contract.Name == "test.ping" && e.Reason == "no_live_worker_advertises_tuple");
        Assert.Contains(backlog.Entries, e => e.Queue == "other" && e.Contract.Name == "test.work" && e.Reason == "advertised_but_not_subscribed");
        Assert.DoesNotContain(backlog.Entries, e => e.Queue == "default" && e.Contract.Name == "test.work");

        foreach (var job in new[] { ping, parked })
        {
            Assert.Equal(0, await f.CountAsync($"SELECT count(*) FROM runwick.job_attempts WHERE job_id = '{job}'"));
            Assert.Equal(1, await f.CountAsync($"SELECT count(*) FROM runwick.jobs WHERE job_id = '{job}' AND state = 'pending'"));
        }

        Assert.NotEqual(supported, Guid.Empty);
        Assert.NotEqual(WorkerReadiness.Unhealthy, w.Engine.Health().Readiness);
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "RDY-02")]
    [Trait("Case", "WRK-19")]
    public async Task Expired_or_pruned_presence_changes_no_ownership_and_renewals_continue()
    {
        // WRK-19 / RDY-02: presence expires (publication failing) while a job lease is healthy; then rows are pruned.
        await using var f = await DriverFixture.StartAsync(Ns("rdy02"));
        var release = new TaskCompletionSource();
        await using var w = Host(f, concurrency: 1);
        w.Script.Default = (_, _, _) => release.Task;
        var job = (await w.DispatchAsync("held")).JobId.Value;
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 1, Wait, "running");
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_subscriptions WHERE queue = 'default'"));

        f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Throw(), FaultTarget.Op("PublishPresence"), times: 1_000);
        await f.ExecAsync("""
            UPDATE runwick.worker_instances
            SET started_at = clock_timestamp() - interval '3 seconds', heartbeat_at = clock_timestamp() - interval '2 seconds',
                expires_at = clock_timestamp() - interval '1 second'
            """);
        var ownership = await OwnershipAsync(f, job);
        var lease = await LeaseAsync(f, job);
        await WorkerHost.WaitUntil(() => LeaseAsync(f, job).GetAwaiter().GetResult() > lease, Wait, "a renewal after presence expired");
        await WorkerHost.WaitUntil(() => w.Engine.Presence.Failing, Wait, "publication failing");
        // Failing publication alone is Degraded. On the loaded shared host a slow renewal can briefly add pressure
        // (Unhealthy); the state the case is about is the one the worker settles in while publication keeps failing.
        await WorkerHost.WaitUntil(() => w.Engine.Health().Readiness == WorkerReadiness.Degraded, Wait, "degraded while publication fails");

        await f.ExecAsync("DELETE FROM runwick.worker_instances");
        lease = await LeaseAsync(f, job);
        await WorkerHost.WaitUntil(() => LeaseAsync(f, job).GetAwaiter().GetResult() > lease, Wait, "a renewal after presence was pruned");
        Assert.Equal(ownership, await OwnershipAsync(f, job));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Recover'"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.worker_subscriptions"));

        release.SetResult();
        await WorkerHost.WaitUntil(() => f.CountAsync($"SELECT count(*) FROM runwick.jobs WHERE job_id = '{job}' AND state = 'succeeded'").Result == 1,
            Wait, "the held job settles normally");
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "OBS-01")]
    public async Task With_every_wake_dropped_polling_finds_work_controls_and_recovery()
    {
        // driver-contract-suite:OBS-01. Notifications are not implemented in M3 (LISTEN is decided after G1-M1), and
        // local wakes are suppressed here, so everything below is found by polling alone.
        await using var f = await DriverFixture.StartAsync(Ns("obs01"));
        await using var w = Host(f, configure: r => r.Queue("other"));
        await w.StartAsync();
        foreach (var pool in w.Engine.Pools)
        {
            pool.Wake.Suppressed = true;
        }

        var producer = await f.OpenClient(configure: r =>
        {
            WorkerHost.RegisterWork(r);
            r.Queue("other");
        });

        // Due work from another process.
        var first = (await producer.Dispatcher.DispatchAsync(new WorkJob("polled"))).JobId.Value;
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("polled"), TimeSpan.FromSeconds(2.4 + 2 + 4), "discovered by polling");

        // Pause then resume, observed through the probe.
        await f.SetQueuePaused(f.Application, "default", true);
        var paused = (await producer.Dispatcher.DispatchAsync(new WorkJob("while-paused"))).JobId.Value;
        await Task.Delay(TimeSpan.FromSeconds(5));
        Assert.Equal(0, await f.CountAsync($"SELECT count(*) FROM runwick.job_attempts WHERE job_id = '{paused}'"));
        await f.SetQueuePaused(f.Application, "default", false);
        await WorkerHost.WaitUntil(() => w.Script.Completed.ContainsKey("while-paused"), TimeSpan.FromSeconds(2.4 + 2 + 4), "resume discovered by polling");

        // Recovery: a reservation whose owner vanished (a 1 s lease on queue "other", which this worker does not
        // subscribe to) is found by the timer-driven recovery loop (05 §13), which never waits on a wake signal.
        var orphan = (await producer.Dispatcher.DispatchAsync(new WorkJob("orphan"), new DispatchOptions { Queue = "other" })).JobId.Value;
        Assert.Equal(StoreStatus.Applied, (await producer.ReserveAsync(queue: "other", lease: TimeSpan.FromSeconds(1))).Status);
        await WorkerHost.WaitUntil(
            () => f.CountAsync($"SELECT count(*) FROM runwick.job_attempts WHERE job_id = '{orphan}' AND outcome = 'abandoned_before_start'").Result == 1,
            Wait, "the dead owner's lease recovered by polling");
        Assert.NotEqual(first, orphan);
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "MODE-01")]
    [Trait("Case", "MODE-02")]
    [Trait("Case", "MODE-03")]
    [Trait("Case", "MODE-04")]
    [Trait("Case", "MODE-05")]
    public async Task Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none()
    {
        // MODE-01 … 05 presence repeat (15 §5): a worker row with exact subscriptions; a producer-only client has none.
        await using var f = await DriverFixture.StartAsync(Ns("modep"));
        await using var w = Host(f, configure: r => r.Queue("other"));
        await w.StartAsync();
        var producer = await f.OpenClient(configure: r => WorkerHost.RegisterWork(r));
        await producer.Dispatcher.DispatchAsync(new WorkJob("x"));

        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_instances WHERE status IN ('starting', 'ready')"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_subscriptions"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_subscriptions WHERE queue = 'default'"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.worker_capabilities WHERE job_name = 'test.work'"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.worker_capabilities WHERE job_name = 'test.ping'"));

        // The draining publication is best-effort presence (05 §14), awaited by StopAsync only within its bound.
        await w.StopAsync();
        await WorkerHost.WaitUntil(
            () => f.CountAsync("SELECT count(*) FROM runwick.worker_instances WHERE status IN ('draining', 'stopping')").Result == 1,
            TimeSpan.FromSeconds(10), "the draining presence row");
    }

    static async Task<long> LeaseAsync(DriverFixture f, Guid job) =>
        await f.CountAsync($"SELECT (extract(epoch FROM lease_expires_at) * 1000)::bigint FROM runwick.jobs WHERE job_id = '{job}'");

    static async Task<string> OwnershipAsync(DriverFixture f, Guid job) =>
        string.Join("/", await f.CountAsync($"SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = '{job}'"),
            await f.CountAsync($"SELECT count(*) FROM runwick.job_attempts WHERE job_id = '{job}' AND finished_at IS NULL"),
            await f.CountAsync($"SELECT count(*) FROM runwick.jobs WHERE job_id = '{job}' AND state = 'running'"));
}
