using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>TIME-05 (persistence half) and TIME-06 (07 §5): the store's retry and unstarted-recovery arithmetic, SP-008.</summary>
public sealed class RetryBudgetTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<LeaseHandle> ReserveWhenEligibleAsync(TestClient c, TimeSpan? lease = null)
    {
        var deadline = DateTime.UtcNow + Wait + Wait;
        while (true)
        {
            var reply = await c.ReserveAsync(lease: lease);
            if (reply.Status == StoreStatus.Applied)
            {
                return reply.Value!.Lease.Handle;
            }

            Assert.True(DateTime.UtcNow < deadline, $"no eligible job: {reply.Status}");
            await Task.Delay(100);
        }
    }

    static async Task<StoreReply<FinishedAttempt>> RecoverWhenExpiredAsync(TestClient c, LeaseHandle lease)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (true)
        {
            var expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).SingleOrDefault(e => e.AttemptId == lease.AttemptId);
            if (expired is not null)
            {
                return await c.RecoverAsync(expired);
            }

            Assert.True(DateTime.UtcNow < deadline, "lease never expired");
            await Task.Delay(100);
        }
    }

    static Task<long> DelayOf(DriverFixture f, Guid attemptId) =>
        f.CountAsync("SELECT retry_delay_ms FROM runwick.job_attempts WHERE attempt_id = @a", ("a", attemptId));

    static Task<long> Unstarted(DriverFixture f, Guid jobId) => f.CountAsync("""
        SELECT g.consecutive_unstarted_count FROM runwick.jobs j JOIN runwick.job_generations g USING (application, job_id, generation)
        WHERE j.job_id = @j
        """, ("j", jobId));

    static Task<long> Executions(DriverFixture f, Guid jobId) =>
        f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", jobId));

    /// <summary>eligible_at is the deciding instant plus the persisted delay (no redraw).</summary>
    static async Task AssertEligibleFromDelay(DriverFixture f, Guid attemptId) =>
        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.job_attempts a JOIN runwick.jobs j USING (application, job_id)
            WHERE a.attempt_id = @a AND j.eligible_at = a.finished_at + a.retry_delay_ms * interval '1 millisecond'
            """, ("a", attemptId)));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "TIME-05")]
    public async Task Retry_delays_are_drawn_once_persisted_and_replayed()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time05"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"))).JobId.Value;

        // n = 1 under DropReply: outcome unknown, the replay returns the persisted eligible_at.
        var lease = await ReserveWhenEligibleAsync(c);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.FinishAsync(lease, AttemptOutcome.RetryableFailure, commandId: id, notAfter: notAfter));
        Assert.Equal(1, drop.Fired);
        var replay1 = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, commandId: id, notAfter: notAfter);
        var replay2 = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, commandId: id, notAfter: notAfter);
        Assert.Equal((StoreStatus.Applied, true, JobStates.Retrying), (replay1.Status, replay1.Replayed, replay1.Value!.Disposition));
        Assert.Equal((replay1.DecidedAt, replay1.Value.EligibleAt), (replay2.DecidedAt, replay2.Value!.EligibleAt));
        Assert.InRange(await DelayOf(f, lease.AttemptId), 2_500, 5_000);
        await AssertEligibleFromDelay(f, lease.AttemptId);
        Assert.Equal(replay1.DecidedAt + TimeSpan.FromMilliseconds(await DelayOf(f, lease.AttemptId)), replay1.Value.EligibleAt);

        // n = 2.
        lease = await ReserveWhenEligibleAsync(c);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var second = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure);
        Assert.Equal(JobStates.Retrying, second.Value!.Disposition);
        Assert.InRange(await DelayOf(f, lease.AttemptId), 5_000, 10_000);
        await AssertEligibleFromDelay(f, lease.AttemptId);

        // n = 3 = MaxExecutions: terminal, no eligible instant.
        lease = await ReserveWhenEligibleAsync(c);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var third = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure);
        Assert.Equal((JobStates.Failed, "execution_budget_exhausted", (DateTimeOffset?)null),
            (third.Value!.Disposition, third.Value.TerminalReason, third.Value.EligibleAt));
        Assert.Equal(3, await Executions(f, job));

        // Requested delays: rounded up, floored at 1 ms, capped at the maximum, no jitter; the execution is charged.
        // All four jobs are dispatched first: a job retried after 1 ms would otherwise be reserved ahead of a later dispatch.
        var requests = new Dictionary<Guid, (double Requested, long Expected)>();
        foreach (var (requested, expected) in new[] { (0.0, 1L), (1.5, 2L), (120_000.0, 60_000L) })
        {
            requests[(await c.Dispatcher.DispatchAsync(new ResourceJob("q" + requested, "rq" + requested))).JobId.Value] = (requested, expected);
        }

        var neg = (await c.Dispatcher.DispatchAsync(new ResourceJob("neg", "rneg"))).JobId.Value;
        for (var i = 0; i < requests.Count; i++)
        {
            var l = await ReserveWhenEligibleAsync(c);
            var (requested, expected) = requests[l.JobId];
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(l)).Reply.Status);
            var reply = await c.FinishAsync(l, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(requested));
            Assert.Equal(JobStates.Retrying, reply.Value!.Disposition);
            Assert.Equal(expected, await DelayOf(f, l.AttemptId));
            Assert.Equal(1, await Executions(f, l.JobId));
            await AssertEligibleFromDelay(f, l.AttemptId);
        }

        // Negative: refused before any write (the engine maps it to a contract fault, SP-009).
        var nl = await ReserveWhenEligibleAsync(c);
        Assert.Equal(neg, nl.JobId);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(nl)).Reply.Status);
        var pre = await f.ReadCommittedSnapshot();
        var refused = await c.FinishAsync(nl, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(-1));
        Assert.Equal((StoreStatus.InvalidRequest, "requested_delay_invalid"), (refused.Status, refused.Error!.Code));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "TIME-06")]
    public async Task Consecutive_unstarted_abandonments_back_off_and_exhaust()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time06a"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"))).JobId.Value;
        (long Low, long High)[] bands = [(500, 1_000), (1_000, 2_000), (2_000, 4_000), (4_000, 8_000)];
        for (var k = 1; k <= 4; k++)
        {
            var lease = await ReserveWhenEligibleAsync(c, TimeSpan.FromSeconds(1));
            var recovered = await RecoverWhenExpiredAsync(c, lease);
            Assert.Equal((StoreStatus.Applied, JobStates.Pending), (recovered.Status, recovered.Value!.Disposition));
            Assert.InRange(await DelayOf(f, lease.AttemptId), bands[k - 1].Low, bands[k - 1].High);
            await AssertEligibleFromDelay(f, lease.AttemptId);
            Assert.Equal(k, await Unstarted(f, job));
            Assert.Equal(0, await Executions(f, job));
        }

        var last = await ReserveWhenEligibleAsync(c, TimeSpan.FromSeconds(1));
        var exhausted = await RecoverWhenExpiredAsync(c, last);
        Assert.Equal((JobStates.Failed, "reservation_abandonment_exhausted", (DateTimeOffset?)null),
            (exhausted.Value!.Disposition, exhausted.Value.TerminalReason, exhausted.Value.EligibleAt));
        Assert.Equal(5, await Unstarted(f, job));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "TIME-06")]
    public async Task Begin_resets_the_unstarted_count_and_release_changes_no_budget()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time06b"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"))).JobId.Value;

        // Two abandonments, then ReleaseReservation: neither k nor the execution count moves.
        for (var k = 1; k <= 2; k++)
        {
            await RecoverWhenExpiredAsync(c, await ReserveWhenEligibleAsync(c, TimeSpan.FromSeconds(1)));
        }

        Assert.Equal(2, await Unstarted(f, job));
        var released = await c.ReleaseAsync(await ReserveWhenEligibleAsync(c));
        Assert.Equal(StoreStatus.Applied, released.Status);
        Assert.Equal((2L, 0L), (await Unstarted(f, job), await Executions(f, job)));

        // A committed Begin resets k to 0.
        var lease = await ReserveWhenEligibleAsync(c);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.Equal((0L, 1L), (await Unstarted(f, job), await Executions(f, job)));
        Assert.Equal(JobStates.Retrying, (await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.Zero)).Value!.Disposition);

        // A further reserved expiry gives k = 1 and, the job having started, returns it to retrying.
        var again = await ReserveWhenEligibleAsync(c, TimeSpan.FromSeconds(1));
        var recovered = await RecoverWhenExpiredAsync(c, again);
        Assert.Equal(JobStates.Retrying, recovered.Value!.Disposition);
        Assert.InRange(await DelayOf(f, again.AttemptId), 500, 1_000);
        Assert.Equal((1L, 1L), (await Unstarted(f, job), await Executions(f, job)));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
