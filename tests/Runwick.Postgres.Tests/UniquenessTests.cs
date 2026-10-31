using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>UNI-01..04 (07 §7) and PGX-04 subset (07 §13).</summary>
public sealed class UniquenessTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static DispatchOptions Window(string key, TimeSpan window, string? active = null) =>
        new() { Uniqueness = new DispatchUniqueness(active, key, window) };

    static DispatchOptions Active(string key) => new() { Uniqueness = new DispatchUniqueness(key, null, null) };

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "UNI-01")]
    public async Task Parallel_same_active_key_creates_one_job()
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni01"));
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20));
        });
        var peer = await f.OpenPeer("peer-1");

        var local = Enumerable.Range(0, 4).Select(i => c.Dispatcher.DispatchAsync(new KeyedJob("L" + i, "k"))
            .ContinueWith(t => (Id: "L" + i, JobId: t.Result.JobId.Value, Disposition: t.Result.Disposition.ToString(), Match: t.Result.DuplicateMatch.ToString()),
                TaskScheduler.Default)).ToList();
        var remote = Enumerable.Range(0, 4).Select(i => peer.SendInBackground("dispatch",
                new JsonObject { ["job"] = "keyed", ["jobKey"] = "P" + i, ["key"] = "k" })
            .ContinueWith(t => (Id: "P" + i, JobId: Guid.Parse(t.Result["jobId"]!.GetValue<string>()),
                Disposition: t.Result["disposition"]!.GetValue<string>(), Match: t.Result["match"]!.GetValue<string>()), TaskScheduler.Default)).ToList();
        var results = await WithTimeout(Task.WhenAll(local.Concat(remote)));

        var created = Assert.Single(results, r => r.Disposition == "Created");
        Assert.All(results.Where(r => r.Disposition != "Created"), r =>
        {
            Assert.Equal("Reused", r.Disposition);
            Assert.Equal("Active", r.Match);
            Assert.Equal(created.JobId, r.JobId);
        });
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key_kind = 'active' AND bound_job_id IS NOT NULL"));
        var payload = Assert.Single(await TextAsync(f, "SELECT convert_from(payload_utf8, 'UTF8') FROM runwick.job_payloads"));
        Assert.Contains($"\"{created.Id}\"", payload, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "UNI-02")]
    public async Task Window_key_reuse_conflict_policy_drift_and_trace()
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var w = Window("w", TimeSpan.FromHours(1));
        var first = await c.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        Assert.Equal(DispatchDisposition.Created, first.Disposition);
        var pre = await f.ReadCommittedSnapshot();

        // (a) identical intent.
        var same = await c.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, false, first.JobId), (same.Disposition, same.DuplicateMatch, same.PolicyDrift, same.JobId));

        // (b) changed payload.
        var conflict = await Assert.ThrowsAsync<UniquenessConflictException>(() => c.Dispatcher.DispatchAsync(new PingJob("p2"), w));
        Assert.Equal(first.JobId, conflict.ConflictingJobId);

        // (c) policy-only change: another process registers the contract with a different MaxExecutions default.
        var c5 = await f.OpenClient(configure: r => TestJobs.Register(r, maxExecutions: 5));
        var drift = await c5.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, true, first.JobId), (drift.Disposition, drift.DuplicateMatch, drift.PolicyDrift, drift.JobId));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'policy_drift'"));

        // (d) diagnostic-only change (traceparent).
        DispatchReceipt traced;
        using (new System.Diagnostics.Activity("uni02").SetIdFormat(System.Diagnostics.ActivityIdFormat.W3C).Start())
        {
            traced = await c.Dispatcher.DispatchAsync(new PingJob("p1"), w);
        }

        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, false), (traced.Disposition, traced.DuplicateMatch, traced.PolicyDrift));

        // Payload, binding, generation policy and original trace unchanged; binding stores both digests and their version.
        var post = await f.ReadCommittedSnapshot();
        foreach (var table in new[] { "jobs", "job_payloads", "job_generations", "dispatch_keys" })
        {
            Assert.Equal(pre[table], post[table]);
        }

        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.dispatch_keys
            WHERE key_kind = 'window' AND business_fingerprint IS NOT NULL AND policy_fingerprint IS NOT NULL AND fingerprint_version IS NOT NULL
            """));
        Assert.Equal(1, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "UNI-03")]
    public async Task Active_and_window_bindings_to_different_jobs_conflict()
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var j1 = await c.Dispatcher.DispatchAsync(new PingJob("1"), Active("a"));
        var j2 = await c.Dispatcher.DispatchAsync(new PingJob("2"), Window("w", TimeSpan.FromHours(1)));
        var pre = await f.ReadCommittedSnapshot();

        var ex = await Assert.ThrowsAsync<UniquenessConflictException>(() =>
            c.Dispatcher.DispatchAsync(new PingJob("3"), Window("w", TimeSpan.FromHours(1), active: "a")));
        Assert.Contains(ex.ConflictingJobId, new JobId?[] { j1.JobId, j2.JobId });
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "UNI-04")]
    public async Task Terminal_job_releases_active_key_but_window_history_survives_until_expiry()
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        // Long enough that steps (a) finish inside the window on a loaded host; (b) waits for its expiry.
        var window = TimeSpan.FromSeconds(15);
        var j = await c.Dispatcher.DispatchAsync(new PingJob("j"), Window("w", window, active: "k"));
        var lease = await RunAsync(c, j.JobId.Value);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key_kind = 'active' AND bound_job_id IS NOT NULL"));

        // (a) window w with identical intent → the terminal J (04 §7.2 case d: the cleared active key is not
        // reclaimed); then active k only → a new job. The identical intent goes first: once k is bound to the new
        // job, the same intent is a composition conflict (case c) — see 07 I-15.
        var windowed = await c.Dispatcher.DispatchAsync(new PingJob("j"), Window("w", window, active: "k"));
        Assert.Equal((DispatchDisposition.Reused, DuplicateMatch.Window, j.JobId), (windowed.Disposition, windowed.DuplicateMatch, windowed.JobId));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key_kind = 'active' AND bound_job_id IS NOT NULL"));
        var active = await c.Dispatcher.DispatchAsync(new PingJob("n"), Active("k"));
        Assert.Equal(DispatchDisposition.Created, active.Disposition);
        Assert.NotEqual(j.JobId, active.JobId);

        // (b) after the window expires (store time) → a new job.
        var expires = DateTimeOffset.Parse(Assert.Single(await TextAsync(f,
            "SELECT to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM runwick.dispatch_keys WHERE key_kind = 'window'")),
            System.Globalization.CultureInfo.InvariantCulture);
        while (await f.StoreNow() <= expires)
        {
            await Task.Delay(100);
        }

        var after = await c.Dispatcher.DispatchAsync(new PingJob("j2"), Window("w", window));
        Assert.Equal(DispatchDisposition.Created, after.Disposition);
        Assert.NotEqual(j.JobId, after.JobId);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "UNI-04")]
    public async Task Permanent_failure_releases_the_active_key()
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni04c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var j3 = await c.Dispatcher.DispatchAsync(new KeyedJob("j3", "k3"));
        var lease = await RunAsync(c, j3.JobId.Value);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.PermanentFailure,
            error: new AttemptError("boom", "System.Exception", "boom", null))).Status);
        var again = await c.Dispatcher.DispatchAsync(new KeyedJob("j4", "k3"));
        Assert.Equal(DispatchDisposition.Created, again.Disposition);
        Assert.NotEqual(j3.JobId, again.JobId);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "PGX-04")]
    public async Task Finish_versus_duplicate_dispatch_in_both_orders()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pgx04a"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var j1 = await c.Dispatcher.DispatchAsync(new KeyedJob("j1", "k1"));
        var j2 = await c.Dispatcher.DispatchAsync(new KeyedJob("j2", "k2"));
        var lease1 = await RunAsync(c, j1.JobId.Value);
        var lease2 = await RunAsync(c, j2.JobId.Value);
        var deadlocksBefore = Deadlocks(c);

        // Finish first (holding job then key) → Dispatch waits, then Created.
        var finishBarrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.Finish));
        var finish = c.FinishAsync(lease1, AttemptOutcome.Succeeded);
        await DriverFixture.WaitAtBarrier(finishBarrier, Wait);
        var dispatch = c.Dispatcher.DispatchAsync(new KeyedJob("n1", "k1"));
        await Task.Delay(300);
        Assert.False(dispatch.IsCompleted, "dispatch did not wait for the key held by Finish");
        DriverFixture.ReleaseBarrier(finishBarrier);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(finish)).Status);
        var created = await WithTimeout(dispatch);
        Assert.Equal(DispatchDisposition.Created, created.Disposition);
        Assert.NotEqual(j1.JobId, created.JobId);

        // Dispatch first (holding key) → Reused J2; Finish then clears the binding.
        var dispatchBarrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.Dispatch));
        var dispatch2 = c.Dispatcher.DispatchAsync(new KeyedJob("n2", "k2"));
        await DriverFixture.WaitAtBarrier(dispatchBarrier, Wait);
        var finish2 = c.FinishAsync(lease2, AttemptOutcome.Succeeded);
        await Task.Delay(300);
        Assert.False(finish2.IsCompleted, "Finish did not wait for the key held by Dispatch");
        DriverFixture.ReleaseBarrier(dispatchBarrier);
        var reused = await WithTimeout(dispatch2);
        Assert.Equal((DispatchDisposition.Reused, j2.JobId), (reused.Disposition, reused.JobId));
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(finish2)).Status);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'k2' AND bound_job_id IS NOT NULL"));
        Assert.Equal(deadlocksBefore, Deadlocks(c));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "PGX-04")]
    public async Task Opposite_order_scope_batches_never_deadlock()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pgx04b"));
        var a = await f.OpenClient(configure: r => TestJobs.Register(r));
        var b = await f.OpenClient(configure: r => TestJobs.Register(r));
        const int iterations = 100;
        for (var i = 0; i < iterations; i++)
        {
            var r1 = $"r1-{i}";
            var r2 = $"r2-{i}";
            var x = a.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new ResourceJob($"a1-{i}", r1)), new BatchDispatchItem(new ResourceJob($"a2-{i}", r2))]);
            var y = b.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new ResourceJob($"b2-{i}", r2)), new BatchDispatchItem(new ResourceJob($"b1-{i}", r1))]);
            var both = await WithTimeout(Task.WhenAll(x, y));
            Assert.All(both, batch => Assert.All(batch.Items, item => Assert.NotNull(item.Receipt)));
        }

        Assert.Equal(0, Deadlocks(a) + Deadlocks(b));
        Assert.Equal(2 * iterations, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_scopes WHERE scope_kind = 'resource'"));
        Assert.Equal(4 * iterations, await Jobs(f));
    }
}
