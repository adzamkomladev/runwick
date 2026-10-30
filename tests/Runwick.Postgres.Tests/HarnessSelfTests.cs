using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Postgres.Tests;

/// <summary>SP-003: the harness itself (fault points, barriers, proxy, peers, snapshots, model).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-003")]
public sealed class HarnessSelfTests
{
    static string Ns(string p) => p + "_" + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    [Trait("Case", "HARNESS-barrier")]
    public async Task Barrier_holds_a_command_after_its_locks_until_released()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_barrier"));
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"));

        var h = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Op(Operations.Reserve));
        var reserve = c.ReserveAsync();
        await DriverFixture.WaitAtBarrier(h);
        Assert.False(reserve.IsCompleted);

        // A second claimant skips the locked row instead of waiting (SKIP LOCKED).
        var other = await c.ReserveAsync();
        Assert.Equal(StoreStatus.NoWork, other.Status);

        DriverFixture.ReleaseBarrier(h);
        Assert.Equal(StoreStatus.Applied, (await reserve).Status);
    }

    [Fact]
    [Trait("Case", "HARNESS-throw")]
    public async Task Throw_rolls_back_and_is_classified_not_applied()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_throw"));
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"));
        var pre = await f.ReadCommittedSnapshot();

        var h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Throw("Io"), FaultTarget.Op(Operations.Reserve));
        await Assert.ThrowsAsync<StoreNotAppliedException>(() => c.ReserveAsync());
        Assert.Equal(1, h.Fired);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // Deadlock-kind faults are retried transparently with the same command identity.
        var d = f.ArmFault(FaultPoint.BeforeFirstMutation, FaultSpec.Throw("Deadlock40P01"), FaultTarget.Op(Operations.Reserve), times: 2);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
        Assert.Equal(2, d.Fired);
    }

    [Fact]
    [Trait("Case", "HARNESS-proxy")]
    public async Task Proxy_drops_a_commit_reply_after_the_server_committed()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_proxy"), StoreProfile.DirectPrimaryViaProxy);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"));

        var dropped = f.Proxy!.ArmCommitReplyDrop(1, PgFaultProxy.Role("work", "local"));
        var commandId = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(50)).ToUnixTimeMilliseconds());
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAsync(commandId: commandId, notAfter: notAfter));
        await dropped;
        Assert.Equal(["COMMIT"], f.Proxy.DroppedCommitTags);

        var replay = await c.ReserveAsync(commandId: commandId, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, replay.Status);
        Assert.True(replay.Replayed);
        Assert.Single((await f.ReadCommittedSnapshot()).Model.Attempts);
    }

    [Fact]
    [Trait("Case", "HARNESS-proxy-latency")]
    public async Task Proxy_latency_applies_only_to_the_filtered_role()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_latency"), StoreProfile.DirectPrimaryViaProxy);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);

        f.Proxy!.SetLatency(TimeSpan.FromMilliseconds(20), LatencyDirection.ServerToClient, PgFaultProxy.Role("control"));
        var sw = System.Diagnostics.Stopwatch.StartNew();
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease)).Status);   // control data source
        var slow = sw.Elapsed;
        sw.Restart();
        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAsync()).Status);       // work data source
        var fast = sw.Elapsed;
        Assert.True(slow > TimeSpan.FromMilliseconds(200), $"control call took {slow}");
        Assert.True(fast < slow, $"work {fast} vs control {slow}");

        TestContext.Current.SendDiagnosticMessage($"control {slow.TotalMilliseconds} ms, work {fast.TotalMilliseconds} ms");
        f.Proxy.SetLatency(TimeSpan.Zero);
        Assert.True(f.Proxy.Sever(PgFaultProxy.Role("control")) >= 1);

        // The severed pooled connection fails its next use as known-not-applied; the following call opens a new one.
        var first = await Record.ExceptionAsync(() => c.RenewAsync(lease));
        Assert.True(first is null or StoreNotAppliedException, first?.ToString());
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease)).Status);
    }

    [Fact]
    [Trait("Case", "HARNESS-peer")]
    public async Task Peer_process_dispatches_and_can_be_killed_at_a_barrier()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_peer"));
        await using var local = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("p1");

        var dispatched = await peer.SendAsync("dispatch", new JsonObject { ["job"] = "ping" });
        Assert.Equal("Created", dispatched["disposition"]!.GetValue<string>());

        var reserved = await peer.SendAsync("reserve", new JsonObject { ["leaseMs"] = 30_000 });
        Assert.Equal("Applied", reserved["status"]!.GetValue<string>());
        await peer.SendAsync("armFault", new JsonObject { ["point"] = "BeforeCommit", ["action"] = "KillProcess", ["operation"] = "Begin" });
        _ = peer.SendInBackground("begin", new JsonObject { ["lease"] = reserved["lease"]!.DeepClone() });
        var at = await peer.WaitForEventAsync("atBarrier");
        Assert.Equal("Begin", at.Body["operation"]!.GetValue<string>());
        peer.Kill();
        Assert.True(peer.HasExited);

        // The Begin never committed: the job is still reserved by the dead peer.
        var snapshot = await f.ReadCommittedSnapshot();
        var job = Assert.Single(snapshot.Model.Jobs);
        Assert.Equal(States.Reserved, job.State);
        Assert.Equal(0, job.StartedCount);
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
    }

    [Fact]
    [Trait("Case", "HARNESS-snapshot")]
    public async Task Snapshots_are_stable_and_normalized()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_snap"));
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"));
        var a = await f.ReadCommittedSnapshot();
        var b = await f.ReadCommittedSnapshot();
        Assert.Empty(a.DiffTables(b));
        Assert.Contains(a["job_payloads"], row => row.Contains("payload_utf8=len:", StringComparison.Ordinal));
        Assert.DoesNotContain(a["job_payloads"], row => row.Contains("\"Id\"", StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Case", "HARNESS-model")]
    public void Reference_model_rejects_illegal_histories()
    {
        var job = Guid.NewGuid();
        var attempt = Guid.NewGuid();
        var token = Guid.NewGuid();
        var t0 = DateTimeOffset.UnixEpoch;
        var legal = HistoryChecker.Check(JobModel.Dispatched(job), [
            new ModelCommand(ModelOp.Reserve, job, t0, attempt, token, Lease: TimeSpan.FromSeconds(30)),
            new ModelCommand(ModelOp.Begin, job, t0.AddSeconds(1), attempt, token),
            new ModelCommand(ModelOp.Finish, job, t0.AddSeconds(2), attempt, token, ModelOutcome.Succeeded),
        ]);
        Assert.True(legal.Legal, legal.Explanation);

        var illegal = HistoryChecker.Check(JobModel.Dispatched(job), [
            new ModelCommand(ModelOp.Begin, job, t0, attempt, token),
        ]);
        Assert.False(illegal.Legal);
    }

    [Fact]
    [Trait("Case", "HARNESS-controls")]
    public async Task Queue_pause_receipt_pruning_and_store_clock()
    {
        await using var f = await DriverFixture.StartAsync(Ns("h_ctl"));
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("a"));

        await f.SetQueuePaused(f.Application, "default", paused: true);
        Assert.Equal(StoreStatus.QueuePaused, (await c.ReserveAsync()).Status);
        await f.SetQueuePaused(f.Application, "default", paused: false);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);

        var now = await f.StoreNow();
        Assert.True(await f.StoreNow() >= now);
        Assert.Equal(0, await f.PruneReceipts("durable", now));
        Assert.True(await f.PruneReceipts("durable", now.AddDays(2)) >= 1);
        Assert.True(await f.PruneReceipts("ownership", now.AddHours(1)) >= 1);
    }

    [Fact]
    [Trait("Case", "HARNESS-restart")]
    [Trait("RequiresDocker", "true")]
    public async Task Store_restart_keeps_data_and_clients_reconnect()
    {
        if (TestDatabase.ExternalConnectionString is { Length: > 0 })
        {
            Assert.Skip("requires-docker");
        }

        var db = await TestDatabase.StartAsync();
        await using var _ = db;
        var before = Guid.NewGuid();
        await StoreServer.ExecAsync(db.ConnectionString, $"CREATE TABLE t (id uuid); INSERT INTO t VALUES ('{before}')");
        await db.RestartAsync(RestartMode.Kill, TimeSpan.FromMilliseconds(500));
        await using var conn = new Npgsql.NpgsqlConnection(db.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand("SELECT id FROM t", conn);
        Assert.Equal(before, (Guid)(await cmd.ExecuteScalarAsync())!);
    }
}

[Trait("Category", "Pure")]
[Trait("Item", "SP-003")]
[Trait("Case", "HARNESS-timing")]
public sealed class TestTimingTests
{
    [Fact]
    public void Fast_profile_divides_every_default_by_ten_and_keeps_the_margins()
    {
        var o = new Runwick.WorkerOptions();
        Runwick.Harness.Timing.TestTiming.Fast(o);
        Assert.Equal(TimeSpan.FromSeconds(6), o.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(1), o.LeaseSafetyMargin);
        Assert.Equal(TimeSpan.FromSeconds(1.5), o.RenewalInterval);
        Assert.Equal(TimeSpan.FromSeconds(3), o.OwnershipCommandLifetime);
        Assert.Equal(TimeSpan.FromSeconds(2.5), o.ShutdownBudget);

        // 07 §2.7 validation: 1.8 + 2×0.5 + 0.1 + 0.2 = 3.1 s < 5 s (lease − safety margin).
        var worstRenewal = o.RenewalInterval + o.RenewalJitter / 1.5 + 2 * o.ControlRequestTimeout + o.LeaseWatchdogInterval + o.SchedulerAllowance;
        Assert.True(worstRenewal < o.LeaseDuration - o.LeaseSafetyMargin, worstRenewal.ToString());
    }
}
