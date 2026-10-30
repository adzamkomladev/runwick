using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>CMD-01..05 for Dispatch (07 §4); the ownership-command variants are SP-008.</summary>
public sealed class CommandIdentityTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "CMD-01")]
    public async Task Same_dispatch_concurrently_and_sequentially_creates_one_job()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("peer-1");
        var p = c.Prepared.Prepare(new PingJob("x"));

        // (a) Local holds the receipt row at BeforeCommit; the peer submits the same prepared dispatch and waits on it.
        var barrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(p.OperationId.Value));
        var local = c.Prepared.DispatchAsync(p);
        await DriverFixture.WaitAtBarrier(barrier, Wait);
        var remote = peer.SendInBackground("dispatchPrepared", new JsonObject { ["envelope"] = Convert.ToBase64String(p.Export().Span) });
        await Task.Delay(500);
        Assert.False(remote.IsCompleted, "the peer's dispatch did not wait for the in-flight receipt");
        DriverFixture.ReleaseBarrier(barrier);
        var a = await WithTimeout(local);
        var b = await WithTimeout(remote);

        Assert.Equal(DispatchDisposition.Created, a.Disposition);
        Assert.Equal(a.OperationId, p.OperationId);
        Assert.Equal(a.JobId.Value.ToString("D"), b["jobId"]!.GetValue<string>());
        Assert.Equal("Created", b["disposition"]!.GetValue<string>());

        // (b) Sequential store-level replays: Replayed = true with the original decision.
        var replies = new List<StoreReply<DispatchResult>>();
        for (var i = 0; i < 3; i++)
        {
            replies.Add(await SendAsync(c, p));
        }

        Assert.All(replies, r =>
        {
            Assert.Equal(StoreStatus.Applied, r.Status);
            Assert.Equal(StoreEffect.Applied, r.Effect);
            Assert.True(r.Replayed);
            Assert.Equal(a.JobId.Value, r.Value!.JobId);
            Assert.Equal(DispatchDisposition.Created, r.Value.Disposition);
        });
        Assert.Single(replies.Select(r => r.DecidedAt).Distinct());

        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_generations WHERE application = @a", ("a", f.Application)));
        Assert.Equal(1, await Receipts(f, p.OperationId.Value));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "CMD-02")]
    public async Task Same_id_with_a_changed_body_conflicts_and_namespaces_are_independent()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var p = c.Prepared.Prepare(new PingJob("x"));
        var applied = await SendAsync(c, p);
        Assert.Equal(StoreStatus.Applied, applied.Status);
        var x = p.OperationId.Value;
        var pre = await f.ReadCommittedSnapshot();

        // (i) changed payload, (ii) changed NotAfter, (iii) changed diagnostic metadata only.
        var changedPayload = c.Prepared.Prepare(new PingJob("y"));
        PreparedDispatch changedDiagnostic;
        using (var activity = new System.Diagnostics.Activity("cmd02").SetIdFormat(System.Diagnostics.ActivityIdFormat.W3C).Start())
        {
            changedDiagnostic = c.Prepared.Prepare(new PingJob("x"));
        }

        var attempts = new[]
        {
            await SendAsync(c, changedPayload, commandId: x, notAfter: p.NotAfter),
            await SendAsync(c, p, notAfter: p.NotAfter - TimeSpan.FromSeconds(1)),
            await SendAsync(c, changedDiagnostic, commandId: x, notAfter: p.NotAfter),
        };
        foreach (var r in attempts)
        {
            Assert.Equal(StoreStatus.CommandConflict, r.Status);
            Assert.Equal(StoreEffect.ObservedNoEffect, r.Effect);
        }

        // (iv) a different operation (Reserve) under the same ID.
        var reserve = await c.ReserveAsync(commandId: x);
        Assert.Equal(StoreStatus.CommandConflict, reserve.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, reserve.Effect);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // Application b applies X independently.
        var appB = f.Application + "_b";
        var cb = await f.OpenClient(configure: r => TestJobs.Register(r), application: appB);
        var inB = await SendAsync(cb, cb.Prepared.Prepare(new PingJob("x")), commandId: x);
        Assert.Equal(StoreStatus.Applied, inB.Status);
        Assert.False(inB.Replayed);
        Assert.Equal(DispatchDisposition.Created, inB.Value!.Disposition);
        Assert.NotEqual(applied.Value!.JobId, inB.Value.JobId);
        Assert.Equal(1, await Jobs(f, appB));
        Assert.Equal(2, await Receipts(f, x));
        Assert.Equal(1, await f.CountAsync(
            "SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND retention_class = 'durable' AND command_id = @c",
            ("a", appB), ("c", x)));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "CMD-03")]
    public async Task Lost_reply_in_process_is_outcome_unknown_and_retry_replays()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd03a"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var p = c.Prepared.Prepare(new PingJob("x"));
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(p.OperationId.Value));

        var unknown = await Assert.ThrowsAsync<DispatchOutcomeUnknownException>(() => c.Prepared.DispatchAsync(p));
        Assert.Equal(1, drop.Fired);
        Assert.Equal(ErrorDisposition.OutcomeUnknown, unknown.Disposition);
        Assert.Same(p, unknown.Prepared);

        var retried = await c.Prepared.DispatchAsync(unknown.Prepared!);
        var replay = await SendAsync(c, p);
        Assert.True(replay.Replayed);
        Assert.Equal(retried.JobId.Value, replay.Value!.JobId);
        Assert.Equal(DispatchDisposition.Created, retried.Disposition);
        var resolution = await c.Prepared.ResolveAsync(p.OperationId);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal(retried.JobId, resolution.Receipt!.JobId);
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await Receipts(f, p.OperationId.Value));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "CMD-03")]
    public async Task Lost_commit_reply_on_the_wire_is_outcome_unknown_and_retry_replays()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd03w"), StoreProfile.DirectPrimaryViaProxy);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var p = c.Prepared.Prepare(new PingJob("x"));
        var dropped = f.Proxy!.ArmCommitReplyDrop(1, PgFaultProxy.Role("work", "local"));

        var unknown = await Assert.ThrowsAsync<DispatchOutcomeUnknownException>(() => c.Prepared.DispatchAsync(p));
        await dropped.WaitAsync(Wait);
        Assert.Contains("COMMIT", f.Proxy.DroppedCommitTags);
        Assert.Equal(ErrorDisposition.OutcomeUnknown, unknown.Disposition);

        // The broken pooled connection may surface once as a known-not-applied store error before the retry runs.
        DispatchReceipt? retried = null;
        for (var i = 0; i < 3 && retried is null; i++)
        {
            try
            {
                retried = await c.Prepared.DispatchAsync(unknown.Prepared!);
            }
            catch (StoreUnavailableException)
            {
            }
        }

        Assert.NotNull(retried);
        Assert.True((await SendAsync(c, p)).Replayed);
        var resolution = await c.Prepared.ResolveAsync(p.OperationId);
        Assert.Equal(DispatchResolutionStatus.Confirmed, resolution.Status);
        Assert.Equal(retried.JobId, resolution.Receipt!.JobId);
        Assert.Equal(1, await Jobs(f));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "CMD-04")]
    public async Task Deadline_expiring_during_a_lock_wait_is_command_expired_and_replay_reads_first()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        // (a) A business transaction stages a dispatch holding key K; D waits on K past its NotAfter.
        await using var business = await OpenBusinessAsync(f);
        await using var tx = await business.BeginTransactionAsync();
        await Staging(c).StageAsync(new KeyedJob("held", "K"), tx);
        var d = c.Prepared.Prepare(new KeyedJob("d", "K"));
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((await f.StoreNow() + TimeSpan.FromSeconds(2)).ToUnixTimeMilliseconds());
        var pending = SendAsync(c, d, notAfter: notAfter);
        while (await f.StoreNow() <= notAfter)
        {
            await Task.Delay(100);
        }

        await Task.Delay(200);
        var releasedAt = await f.StoreNow();
        await tx.RollbackAsync();
        var expired = await WithTimeout(pending);
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, expired.Effect);
        Assert.True(expired.DecidedAt >= notAfter, $"decision {expired.DecidedAt:O} precedes NotAfter {notAfter:O} (released {releasedAt:O})");
        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys"));
        Assert.Equal(0, await Receipts(f, d.OperationId.Value));

        // (c) An applied command replayed after its NotAfter returns the recorded receipt.
        var e = c.Prepared.Prepare(new PingJob("e"));
        var eNotAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(1)).ToUnixTimeMilliseconds());
        var first = await SendAsync(c, e, notAfter: eNotAfter);
        Assert.Equal(StoreStatus.Applied, first.Status);
        while (await f.StoreNow() <= eNotAfter)
        {
            await Task.Delay(100);
        }

        var replay = await SendAsync(c, e, notAfter: eNotAfter);
        Assert.Equal(StoreStatus.Applied, replay.Status);
        Assert.True(replay.Replayed);
        Assert.Equal(first.DecidedAt, replay.DecidedAt);
        Assert.Equal(first.Value!.JobId, replay.Value!.JobId);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-005")]
    [Trait("Item", "SP-006")]
    [Trait("Case", "CMD-05")]
    public async Task Pruned_receipt_replay_is_expired_and_never_redispatches()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd05"));
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.Producer(o => o.CommandLifetime = TimeSpan.FromSeconds(2));
        });
        var p = c.Prepared.Prepare(new PingJob("x"));
        var receipt = await c.Prepared.DispatchAsync(p);
        while (await f.StoreNow() <= p.NotAfter)
        {
            await Task.Delay(100);
        }

        Assert.True(await f.PruneReceipts("durable", DateTimeOffset.UtcNow + TimeSpan.FromDays(3650)) >= 1);
        Assert.Equal(0, await Receipts(f, p.OperationId.Value));
        var pre = await f.ReadCommittedSnapshot();

        var replay = await SendAsync(c, p);
        Assert.Equal(StoreStatus.CommandExpired, replay.Status);
        var thrown = await Assert.ThrowsAsync<CommandExpiredException>(() => c.Prepared.DispatchAsync(p));
        Assert.Equal(p.OperationId, thrown.OperationId);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(1, await Jobs(f));

        // PruneReceipts simulates the 24 h retention passing (I-3); the arbiter's classification is a function of
        // store time minus NotAfter, so the same simulated offset is applied to the bound it is given.
        var simulatedBound = p.NotAfter - TimeSpan.FromHours(24) - TimeSpan.FromSeconds(1);
        var resolution = await c.Store.ResolveOperationAsync(c.Application, p.OperationId.Value, simulatedBound, CancellationToken.None);
        Assert.IsType<ReceiptResolution.ExpiredUnknown>(resolution);
        Assert.NotEqual(Guid.Empty, receipt.JobId.Value);
    }
}
