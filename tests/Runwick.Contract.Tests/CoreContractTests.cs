using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Contract.Tests;

/// <summary>
/// Driver-suite cases that use only the protocol interfaces, the fixture and the committed snapshot (20 §12), run as theories
/// over <see cref="StoreKind"/>. PostgreSQL rows need RUNWICK_TEST_PG (or Docker); SQLite rows run on a temp file.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class CoreContractTests
{
    /// <summary>Both kinds; RUNWICK_TEST_STORES (for example "Sqlite") narrows the run.</summary>
    public static TheoryData<StoreKind> Kinds =>
        [.. Enum.GetValues<StoreKind>().Where(k => Environment.GetEnvironmentVariable("RUNWICK_TEST_STORES") is not { Length: > 0 } only
            || only.Split(',').Contains(k.ToString(), StringComparer.OrdinalIgnoreCase))];

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name, int maxExecutions = 3)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r, maxExecutions));
        return (f, c);
    }

    static async Task NoViolations(DriverFixture f)
    {
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RUN-01")]
    public async Task Dispatch_reserve_begin_renew_finish_round_trip(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "run01");
        await using var _ = f;
        var receipt = await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        Assert.Equal(DispatchDisposition.Created, receipt.Disposition);

        var probe = await c.Driver.ProbeAsync(c.Application, [new LaneKey("default", "medium"), new LaneKey("default", "high")], c.Contracts, Ct);
        Assert.Equal(new LaneKey("default", "medium"), Assert.Single(probe.Lanes).Lane);
        Assert.False(Assert.Single(probe.Queues).Paused);
        var ack = await c.Driver.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, 1, 1, "ready", Environment.ProcessId,
            "host", "build", ["default"], new Dictionary<string, int> { ["main"] = 1 }, c.Contracts, TimeSpan.FromSeconds(30)), Ct);
        Assert.True(ack.Accepted);

        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        Assert.Equal(receipt.JobId.Value, reserved.Value!.Position.JobId);
        Assert.Equal("{\"id\":\"a\"}", System.Text.Encoding.UTF8.GetString(reserved.Value.PayloadUtf8));

        var begun = await c.BeginAsync(reserved.Value.Lease.Handle);
        Assert.Equal(StoreStatus.Applied, begun.Reply.Status);
        Assert.Equal(1, begun.Reply.Value!.ExecutionNumber);
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(reserved.Value.Lease.Handle)).Status);

        var finished = await c.FinishAsync(reserved.Value.Lease.Handle, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, finished.Status);
        Assert.Equal("succeeded", finished.Value!.Disposition);

        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal("succeeded", snapshot.Model.Job(receipt.JobId.Value).State);
        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAsync()).Status);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CMD-01")]
    public async Task A_resent_command_replays_its_receipt_and_a_changed_body_conflicts(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "cmd01");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct);

        // The fingerprint covers NotAfter, so a resend carries the same one.
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var first = await c.ReserveAsync(commandId: id, notAfter: notAfter);
        var again = await c.ReserveAsync(commandId: id, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, again.Status);
        Assert.True(again.Replayed);
        Assert.Equal(first.Value!.Position.JobId, again.Value!.Position.JobId);
        Assert.Equal(first.Value.Lease.Handle, again.Value.Lease.Handle);
        Assert.Equal(first.Value.PayloadUtf8, again.Value.PayloadUtf8);

        var conflict = await c.ReserveAsync(commandId: id, priority: "high", notAfter: notAfter);
        Assert.Equal(StoreStatus.CommandConflict, conflict.Status);

        var begin = Guid.CreateVersion7();
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(first.Value.Lease.Handle, begin, notAfter)).Reply.Status);
        var replayed = await c.BeginAsync(first.Value.Lease.Handle, begin, notAfter);
        Assert.True(replayed.Reply.Replayed);
        Assert.Equal(1, replayed.Reply.Value!.ExecutionNumber);

        var expired = await c.ReserveAsync(notAfter: DateTimeOffset.UtcNow.AddMilliseconds(-1));
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OWN-01")]
    public async Task Only_the_current_owner_changes_a_lease(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "own01");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct);
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;

        var wrongToken = lease with { OwnershipToken = Guid.NewGuid() };
        Assert.Equal("ownership_mismatch", (await c.RenewAsync(wrongToken)).Error!.Code);
        Assert.Equal("stale_lease", (await c.RenewAsync(lease with { AttemptId = Guid.NewGuid() })).Error!.Code);
        Assert.Equal("job_not_found", (await c.RenewAsync(lease with { JobId = Guid.NewGuid() })).Error!.Code);
        Assert.Equal(StoreStatus.InvalidState, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);   // not running yet

        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        Assert.Equal("job_terminal", (await c.FinishAsync(lease with { AttemptId = Guid.NewGuid() }, AttemptOutcome.Succeeded)).Error!.Code);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "END-01")]
    public async Task Retry_then_budget_exhaustion_and_recovery_of_an_expired_lease(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "end01", maxExecutions: 2);
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId.Value;

        var first = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(first);
        var retry = await c.FinishAsync(first, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(1));
        Assert.Equal("retrying", retry.Value!.Disposition);
        Assert.NotNull(retry.Value.EligibleAt);

        StoreReply<ReservedJob> second;
        do
        {
            await Task.Delay(20, Ct);
            second = await c.ReserveAsync(lease: TimeSpan.FromSeconds(1));
        }
        while (second.Status == StoreStatus.NoWork);

        await c.BeginAsync(second.Value!.Lease.Handle);
        await Task.Delay(1200, Ct);
        var expired = await c.Driver.ListExpiredLeasesAsync(c.Application, 10, Ct);
        var e = Assert.Single(expired);
        Assert.Equal(job, e.JobId);

        var recovered = await c.RecoverAsync(e);
        Assert.Equal(StoreStatus.Applied, recovered.Status);
        Assert.Equal("failed", recovered.Value!.Disposition);
        Assert.Equal("execution_budget_exhausted", recovered.Value.TerminalReason);
        Assert.Equal("job_terminal", (await c.FinishAsync(second.Value.Lease.Handle, AttemptOutcome.Succeeded)).Error!.Code);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "END-03")]
    public async Task Released_reservation_returns_the_job_to_pending(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "end03");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new CappedJob("a"), cancellationToken: Ct);
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        var released = await c.ReleaseAsync(lease);
        Assert.Equal(StoreStatus.Applied, released.Status);
        Assert.Equal("pending", released.Value!.Disposition);
        var again = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, again.Status);
        Assert.Equal(2, again.Value!.ReservationNumber);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UNI-01")]
    public async Task Unique_while_active_returns_the_existing_job_until_it_ends(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "uni01");
        await using var _ = f;
        var a = await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k"), cancellationToken: Ct);
        var b = await c.Dispatcher.DispatchAsync(new KeyedJob("b", "k"), cancellationToken: Ct);
        Assert.Equal(DispatchDisposition.Created, a.Disposition);
        Assert.NotEqual(DispatchDisposition.Created, b.Disposition);
        Assert.Equal(a.JobId, b.JobId);

        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        await c.FinishAsync(lease, AttemptOutcome.Succeeded);
        var d = await c.Dispatcher.DispatchAsync(new KeyedJob("d", "k"), cancellationToken: Ct);
        Assert.Equal(DispatchDisposition.Created, d.Disposition);
        Assert.NotEqual(a.JobId, d.JobId);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RUN-02")]
    public async Task A_full_scope_blocks_the_next_reservation(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "run02");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new CappedJob("a"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new CappedJob("b"), cancellationToken: Ct);
        var first = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, first.Status);
        var blocked = await c.ReserveAsync();
        Assert.Equal(StoreStatus.CapacityBlocked, blocked.Status);
        Assert.NotNull(blocked.Error!.BlockedScopeId);
        Assert.NotNull(blocked.Error.ResumeAfter);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OWN-04")]
    public async Task RenewMany_refuses_one_member_and_renews_the_others(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "own04");
        await using var _ = f;
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 4; i++)
        {
            await c.Dispatcher.DispatchAsync(new ResourceJob("j" + i, "r" + i), cancellationToken: Ct);
        }

        for (var i = 0; i < 4; i++)
        {
            leases.Add((await c.ReserveAsync()).Value!.Lease.Handle);
        }

        leases[2] = leases[2] with { OwnershipToken = Guid.NewGuid() };
        var reply = await c.RenewManyAsync(leases);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal([StoreStatus.Applied, StoreStatus.Applied, StoreStatus.StaleLease, StoreStatus.Applied],
            reply.Value!.Outcomes.Select(o => o.Status));
        Assert.Equal("ownership_mismatch", reply.Value.Outcomes[2].Code);

        var none = await c.RenewManyAsync([leases[2]]);
        Assert.Equal(StoreStatus.Applied, none.Status);
        Assert.Equal(StoreStatus.StaleLease, Assert.Single(none.Value!.Outcomes).Status);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RAB-01")]
    public async Task ReserveAndBegin_claims_and_begins_several_jobs_and_replays_once(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "rab01");
        await using var _ = f;
        for (var i = 0; i < 5; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct);
        }

        await c.Dispatcher.DispatchAsync(new CappedJob("c1"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new CappedJob("c2"), cancellationToken: Ct);

        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        var claimed = await c.ReserveAndBeginAsync(32, commandId: id, notAfter: notAfter);
        Assert.Equal(StoreStatus.Applied, claimed.Status);
        var outcomes = claimed.Value!.Outcomes;
        Assert.Equal(7, outcomes.Count);
        Assert.Equal(6, outcomes.Count(o => o.Status == StoreStatus.Applied));
        Assert.Equal(StoreStatus.CapacityBlocked, Assert.Single(outcomes, o => o.Status != StoreStatus.Applied).Status);
        Assert.All(outcomes.Where(o => o.Status == StoreStatus.Applied), o => Assert.Equal(1, o.Begun!.ExecutionNumber));

        var replay = await c.ReserveAndBeginAsync(32, commandId: id, notAfter: notAfter);
        Assert.True(replay.Replayed);
        Assert.Equal(outcomes.Select(o => (o.JobId, o.Status)), replay.Value!.Outcomes.Select(o => (o.JobId, o.Status)));
        Assert.Equal(outcomes.First(o => o.Reserved is not null).Reserved!.PayloadUtf8,
            replay.Value.Outcomes.First(o => o.Reserved is not null).Reserved!.PayloadUtf8);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "FINMANY-01")]
    public async Task FinishMany_settles_each_item_with_its_own_receipt(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "fm01");
        await using var _ = f;
        for (var i = 0; i < 3; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct);
        }

        var claimed = (await c.ReserveAndBeginAsync(3)).Value!.Outcomes.Select(o => o.Reserved!.Lease.Handle).ToList();
        var items = new List<StoreCommand<FinishRequest>>
        {
            c.FinishCommand(claimed[0], AttemptOutcome.Succeeded),
            c.FinishCommand(claimed[1] with { OwnershipToken = Guid.NewGuid() }, AttemptOutcome.Succeeded),
            c.FinishCommand(claimed[2], AttemptOutcome.PermanentFailure),
        };

        var reply = await c.FinishManyAsync(items);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal([StoreStatus.Applied, StoreStatus.StaleLease, StoreStatus.Applied], reply.Value!.Outcomes.Select(o => o.Status));

        // Each item's receipt is an ordinary Finish receipt: a single Finish/1 resubmission replays it.
        var single = await c.FinishAsync(items[2]);
        Assert.True(single.Replayed);
        Assert.Equal("failed", single.Value!.Disposition);

        var again = await c.FinishManyAsync(items);
        Assert.Equal([true, false, true], again.Value!.Outcomes.Select(o => o.Replayed));
        await NoViolations(f);
    }
}
