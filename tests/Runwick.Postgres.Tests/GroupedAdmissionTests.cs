using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Serialization;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// PF2-B1 redesign: grouped producer admission groups only callers that wait for a producer slot anyway. The client has one
/// producer slot; a first dispatch takes it and is held at its commit by a fault barrier, so the next callers wait for the
/// slot, and the first of them to get it carries the others in one transaction.
/// </summary>
public sealed class GroupedAdmissionTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<(DriverFixture F, TestClient C)> StartAsync(string name, TimeSpan? admissionWait = null)
    {
        var f = await DriverFixture.StartAsync(Ns(name));
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.Producer(o =>
            {
                o.MaxConcurrentCalls = 1;
                o.AdmissionWait = admissionWait ?? TimeSpan.FromSeconds(30);
            });
        }, postgres: o => o.GroupProducerAdmission = true);
        return (f, c);
    }

    static LocalAdmission Admission(TestClient c) => c.Services.GetRequiredService<LocalAdmission>();

    /// <summary>Takes the only producer slot with a dispatch held at BeforeCommit.</summary>
    static async Task<(FaultHandle Barrier, Task<DispatchReceipt> Holder, PreparedDispatch Prepared)> HoldSlotAsync(DriverFixture f, TestClient c)
    {
        var p = c.Prepared.Prepare(new PingJob("holder"));
        var barrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(p.OperationId.Value));
        var holder = c.Prepared.DispatchAsync(p);
        await DriverFixture.WaitAtBarrier(barrier, Wait);
        return (barrier, holder, p);
    }

    static async Task WaitQueuedAsync(TestClient c, int count)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (Admission(c).Waiting != count)
        {
            Assert.True(DateTime.UtcNow < deadline, "the callers did not queue");
            await Task.Delay(10);
        }
    }

    static async Task<FaultHandle> WaitFiredAsync(IReadOnlyList<FaultHandle> handles)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (true)
        {
            if (handles.FirstOrDefault(h => h.Fired > 0) is { } fired)
            {
                return fired;
            }

            Assert.True(DateTime.UtcNow < deadline, "no group reached the fault point");
            await Task.Delay(10);
        }
    }

    static Task<T> Within<T>(Task<T> task) => task.WaitAsync(Wait);

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task Waiting_callers_commit_as_one_group_with_the_rows_and_receipts_of_single_dispatches()
    {
        var (f, c) = await StartAsync("b1shape");
        await using var _ = f;
        var (barrier, holder, holderPrepared) = await HoldSlotAsync(f, c);
        var members = Enumerable.Range(0, 4).Select(i => c.Prepared.Prepare(new PingJob("holder"))).ToList();
        var sends = members.Select(m => c.Prepared.DispatchAsync(m)).ToList();
        await WaitQueuedAsync(c, 4);
        DriverFixture.ReleaseBarrier(barrier);

        var receipts = await Within(Task.WhenAll(sends));
        var holderReceipt = await Within(holder);

        Assert.Equal(1, c.Store.DispatchGroups);
        Assert.Equal(4, c.Store.GroupedDispatches);
        Assert.Equal(0, c.Store.GroupFallbacks);
        for (var i = 0; i < members.Count; i++)
        {
            Assert.Equal(members[i].OperationId, receipts[i].OperationId);
            Assert.Equal(DispatchDisposition.Created, receipts[i].Disposition);
            Assert.Equal(1, await Receipts(f, members[i].OperationId.Value));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE command_id = @c AND event_kind = 'dispatched' AND (details ->> 'ordinal') = '0'",
                ("c", members[i].OperationId.Value)));
        }

        Assert.Equal(5, await Jobs(f));

        // The grouped rows equal a single dispatch's, apart from IDs and times.
        const string Shape = """
            SELECT j.job_name || '|' || j.queue || '|' || j.priority || '|' || j.state || '|' || j.payload_version || '|' ||
                   j.serializer_profile_id || '|' || j.serializer_profile_version || '|' || encode(p.payload_utf8, 'hex') || '|' ||
                   p.payload_encoding_version || '|' || p.headers::text || '|' || g.generation || '|' || g.created_reason || '|' ||
                   g.max_executions || '|' || g.max_unstarted_abandonments || '|' || g.execution_timeout_ms || '|' ||
                   g.retry_policy::text || '|' || g.unstarted_retry_policy::text || '|' || e.event_kind || '|' || e.details::text || '|' ||
                   c.status || '|' || c.operation || '|' || c.retention_class || '|' || (c.retain_until - c.not_after)::text
            FROM runwick.jobs j
            JOIN runwick.job_payloads p ON p.application = j.application AND p.job_id = j.job_id
            JOIN runwick.job_generations g ON g.application = j.application AND g.job_id = j.job_id
            JOIN runwick.queue_events e ON e.application = j.application AND e.job_id = j.job_id
            JOIN runwick.storage_commands c ON c.application = j.application AND c.command_id = e.command_id
            WHERE j.job_id = @id
            """;
        var single = await TextAsync(f, Shape, ("id", holderReceipt.JobId.Value));
        Assert.Single(single);
        foreach (var r in receipts)
        {
            Assert.Equal(single, await TextAsync(f, Shape, ("id", r.JobId.Value)));
        }

        // Every command resends alone to its own original receipt.
        for (var i = 0; i < members.Count; i++)
        {
            var replay = await SendAsync(c, members[i]);
            Assert.True(replay.Replayed);
            Assert.Equal(receipts[i].JobId.Value, replay.Value!.JobId);
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(members[i].OperationId)).Status);
        }

        Assert.Equal(5, await Jobs(f));
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(holderPrepared.OperationId)).Status);
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_lone_caller_is_sent_at_once_and_never_waits_or_groups()
    {
        var (f, c) = await StartAsync("b1lone");
        await using var _ = f;

        for (var i = 0; i < 5; i++)
        {
            var receipt = await Within(c.Dispatcher.DispatchAsync(new PingJob("lone" + i)));
            Assert.Equal(DispatchDisposition.Created, receipt.Disposition);
        }

        Assert.Equal(0, c.Store.DispatchGroups);
        Assert.Equal(0, Admission(c).Waiting);
        Assert.Equal(5, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task Mixed_outcomes_in_one_group_each_get_the_reply_a_single_dispatch_would()
    {
        var (f, c) = await StartAsync("b1mixed");
        await using var _ = f;
        var earlier = c.Prepared.Prepare(new PingJob("earlier"));
        var original = await SendAsync(c, earlier);
        var taken = c.Prepared.Prepare(new PingJob("taken"));
        await SendAsync(c, taken);

        var fresh1 = c.Prepared.Prepare(new PingJob("fresh1"));
        var other = c.Prepared.Prepare(new PingJob("other"));
        var late = c.Prepared.Prepare(new PingJob("late"));
        var nowhere = c.Prepared.Prepare(new PingJob("nowhere"));
        var fresh2 = c.Prepared.Prepare(new PingJob("fresh2"));
        var unregistered = Edited(c, nowhere, nowhere.Intent with { Queue = "not-a-queue" });
        StoreCommand<DispatchRequest>[] group =
        [
            Command(fresh1, application: c.Application),
            Command(earlier, application: c.Application),                                     // replay
            Command(other, commandId: taken.OperationId.Value, application: c.Application),  // same ID, other request
            Command(late, notAfter: DateTimeOffset.UtcNow.AddMilliseconds(-1), application: c.Application),
            unregistered,
            Command(fresh2, application: c.Application),
        ];

        var r = await c.Store.DispatchGroupAsync(group, CancellationToken.None);

        Assert.Equal(1, c.Store.DispatchGroups);
        Assert.All(r, x => Assert.Null(x.Failure));
        Assert.Equal((StoreStatus.Applied, false), (r[0].Reply!.Status, r[0].Reply!.Replayed));
        Assert.Equal((StoreStatus.Applied, true), (r[1].Reply!.Status, r[1].Reply!.Replayed));
        Assert.Equal(original.Value!.JobId, r[1].Reply!.Value!.JobId);
        Assert.Equal(original.DecidedAt, r[1].Reply!.DecidedAt);
        Assert.Equal(StoreStatus.CommandConflict, r[2].Reply!.Status);
        Assert.Equal(StoreStatus.CommandExpired, r[3].Reply!.Status);
        Assert.Equal((StoreStatus.InvalidRequest, "queue_not_registered"), (r[4].Reply!.Status, r[4].Reply!.Error!.Code));
        Assert.Equal((StoreStatus.Applied, false), (r[5].Reply!.Status, r[5].Reply!.Replayed));
        Assert.Equal(r[0].Reply!.DecidedAt, r[5].Reply!.DecidedAt);

        // earlier, taken, fresh1, fresh2; the refused commands left no receipt and no row.
        Assert.Equal(4, await Jobs(f));
        Assert.Equal(1, await Receipts(f, earlier.OperationId.Value));
        Assert.Equal(0, await Receipts(f, late.OperationId.Value));
        Assert.Equal(0, await Receipts(f, nowhere.OperationId.Value));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND result IS NULL", ("a", f.Application)));
    }

    static StoreCommand<DispatchRequest> Edited(TestClient c, PreparedDispatch p, DispatchIntent intent)
    {
        var digests = Fingerprints.ForDispatch(p.StoreId, c.Application, p.OperationId.Value, p.NotAfter.ToUnixTimeMilliseconds(), intent);
        return new(c.Application, p.OperationId.Value, p.NotAfter, new DispatchRequest(new DispatchItem(intent, p.Scopes, digests), p.StoreId, digests.Command));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_group_the_server_refuses_rolls_back_and_each_command_is_sent_alone()
    {
        var (f, c) = await StartAsync("b1fallback");
        await using var _ = f;
        var existing = await SendAsync(c, c.Prepared.Prepare(new PingJob("existing")));

        var a = c.Prepared.Prepare(new PingJob("a"));
        var collide = c.Prepared.Prepare(new PingJob("collide"));
        var b = c.Prepared.Prepare(new PingJob("b"));
        var r = await c.Store.DispatchGroupAsync(
            [Command(a, application: c.Application), Edited(c, collide, collide.Intent with { ProposedJobId = existing.Value!.JobId }),
             Command(b, application: c.Application)], CancellationToken.None);

        Assert.Equal(0, c.Store.DispatchGroups);
        Assert.Equal(3, c.Store.GroupFallbacks);
        Assert.Equal(StoreStatus.Applied, r[0].Reply!.Status);
        Assert.Equal((StoreStatus.InvariantFault, "id_collision"), (r[1].Reply!.Status, r[1].Reply!.Error!.Code));
        Assert.Equal(StoreStatus.Applied, r[2].Reply!.Status);
        Assert.Equal(3, await Jobs(f)); // existing, a, b
        Assert.Equal(0, await Receipts(f, collide.OperationId.Value));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_caller_that_cancels_while_waiting_leaves_nothing_and_the_rest_still_send()
    {
        var (f, c) = await StartAsync("b1cancelq");
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var a = c.Prepared.Prepare(new PingJob("a"));
        var b = c.Prepared.Prepare(new PingJob("b"));
        using var cts = new CancellationTokenSource();
        var sendA = c.Prepared.DispatchAsync(a);
        var sendB = c.Prepared.DispatchAsync(b, cts.Token);
        await WaitQueuedAsync(c, 2);

        await cts.CancelAsync();
        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sendB);
        Assert.IsNotType<DispatchCanceledAfterSendException>(cancelled);
        await WaitQueuedAsync(c, 1);
        DriverFixture.ReleaseBarrier(barrier);

        Assert.Equal(DispatchDisposition.Created, (await Within(sendA)).Disposition);
        await Within(holder);
        Assert.Equal(2, await Jobs(f));
        Assert.Equal(0, await Receipts(f, b.OperationId.Value));
        Assert.Equal(0, c.Store.DispatchGroups);
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_caller_that_cancels_after_its_group_was_sent_can_resolve_and_resend_its_operation()
    {
        var (f, c) = await StartAsync("b1cancelsent");
        await using var _ = f;
        var (holdBarrier, holder, _) = await HoldSlotAsync(f, c);
        var members = Enumerable.Range(0, 3).Select(i => c.Prepared.Prepare(new PingJob("m" + i))).ToList();
        var barriers = members.Select(m => f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(m.OperationId.Value))).ToList();
        using var cts = new CancellationTokenSource();
        var sends = new[] { c.Prepared.DispatchAsync(members[0]), c.Prepared.DispatchAsync(members[1], cts.Token), c.Prepared.DispatchAsync(members[2]) };
        await WaitQueuedAsync(c, 3);
        DriverFixture.ReleaseBarrier(holdBarrier);
        var groupBarrier = await WaitFiredAsync(barriers); // the group travels under its leader's ID

        await cts.CancelAsync();
        var unknown = await Assert.ThrowsAsync<DispatchCanceledAfterSendException>(() => sends[1]);
        Assert.Equal(members[1].OperationId, unknown.OperationId);
        DriverFixture.ReleaseBarrier(groupBarrier);

        Assert.Equal(DispatchDisposition.Created, (await Within(sends[0])).Disposition);
        Assert.Equal(DispatchDisposition.Created, (await Within(sends[2])).Disposition);
        await Within(holder);
        Assert.Equal(1, c.Store.DispatchGroups);
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(members[1].OperationId)).Status);
        Assert.True((await SendAsync(c, members[1])).Replayed);
        Assert.Equal(4, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_lost_group_reply_is_outcome_unknown_for_each_member_under_its_own_ID_and_each_resends_to_its_receipt()
    {
        var (f, c) = await StartAsync("b1lost");
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var members = Enumerable.Range(0, 3).Select(i => c.Prepared.Prepare(new PingJob("m" + i))).ToList();
        var drops = members.Select(m => f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(m.OperationId.Value))).ToList();
        var sends = members.Select(m => c.Prepared.DispatchAsync(m)).ToList();
        await WaitQueuedAsync(c, 3);
        DriverFixture.ReleaseBarrier(barrier);
        await Within(holder);

        foreach (var (send, m) in sends.Zip(members))
        {
            var unknown = await Assert.ThrowsAsync<DispatchOutcomeUnknownException>(() => Within(send));
            Assert.Equal(m.OperationId, unknown.OperationId);
        }

        Assert.Equal(1, drops.Sum(d => d.Fired));
        Assert.Equal(4, await Jobs(f));
        foreach (var m in members)
        {
            Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(m.OperationId)).Status);
            Assert.True((await SendAsync(c, m)).Replayed);
        }

        Assert.Equal(4, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_waiting_caller_refused_by_local_admission_sends_nothing()
    {
        var (f, c) = await StartAsync("b1refused", admissionWait: TimeSpan.FromMilliseconds(200));
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var p = c.Prepared.Prepare(new PingJob("refused"));

        var refused = await Assert.ThrowsAsync<LocalCapacityExceededException>(() => Within(c.Prepared.DispatchAsync(p)));
        Assert.Same(p, refused.Prepared);
        DriverFixture.ReleaseBarrier(barrier);
        await Within(holder);
        Assert.Equal(0, await Receipts(f, p.OperationId.Value));
        Assert.Equal(1, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task Keyed_dispatches_and_calls_on_a_caller_transaction_take_the_ordinary_path()
    {
        var (f, c) = await StartAsync("b1plain");
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var keyed = Enumerable.Range(0, 3).Select(i => c.Dispatcher.DispatchAsync(new KeyedJob("k" + i, "key" + i),
            new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "w" + i, TimeSpan.FromHours(1)) })).ToList();
        await WaitQueuedAsync(c, 3);

        // A call on the caller's own transaction never waits for a producer slot and is never grouped.
        var staged = c.Prepared.Prepare(new PingJob("staged"));
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync())
        {
            Assert.Equal(DispatchDisposition.Created, (await Within(Staging(c).StageAsync(staged, tx))).Disposition);
            await tx.CommitAsync();
        }

        DriverFixture.ReleaseBarrier(barrier);
        Assert.All(await Within(Task.WhenAll(keyed)), r => Assert.Equal(DispatchDisposition.Created, r.Disposition));
        await Within(holder);
        Assert.Equal(0, c.Store.DispatchGroups);
        Assert.Equal(5, await Jobs(f));
    }
}
