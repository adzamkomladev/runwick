using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Serialization;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// PF2-B1: opt-in grouped producer admission. A group forms behind one held send: the first caller finds a free slot and
/// is held at its commit by a fault barrier, the others queue, and they leave together when the barrier opens.
/// </summary>
public sealed class GroupedAdmissionTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<(DriverFixture F, TestClient C)> StartAsync(string name)
    {
        var f = await DriverFixture.StartAsync(Ns(name));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r), postgres: o => o.GroupProducerAdmission = true);
        return (f, c);
    }

    /// <summary>The same command with an edited intent, with honest digests.</summary>
    static StoreCommand<DispatchRequest> Edited(TestClient c, PreparedDispatch p, Func<DispatchIntent, DispatchIntent> edit,
        Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var id = commandId ?? p.OperationId.Value;
        var na = notAfter ?? p.NotAfter;
        var intent = edit(p.Intent);
        var digests = Fingerprints.ForDispatch(p.StoreId, c.Application, id, na.ToUnixTimeMilliseconds(), intent);
        return new StoreCommand<DispatchRequest>(c.Application, id, na, new DispatchRequest(new DispatchItem(intent, p.Scopes, digests), p.StoreId, digests.Command));
    }

    /// <summary>Sends a command that finds a free slot and holds it at BeforeCommit.</summary>
    static async Task<(FaultHandle Barrier, Task<StoreReply<DispatchResult>> Holder, PreparedDispatch Prepared)> HoldSlotAsync(DriverFixture f, TestClient c)
    {
        var p = c.Prepared.Prepare(new PingJob("holder"));
        var barrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(p.OperationId.Value));
        var holder = SendAsync(c, p);
        await DriverFixture.WaitAtBarrier(barrier, Wait);
        return (barrier, holder, p);
    }

    static async Task WaitQueuedAsync(TestClient c, int count)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (c.Store.Group!.Queued < count)
        {
            Assert.True(DateTime.UtcNow < deadline, "the commands did not queue");
            await Task.Delay(10);
        }
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task Concurrent_callers_commit_as_one_group_with_the_rows_of_a_single_dispatch()
    {
        var (f, c) = await StartAsync("b1shape");
        await using var _ = f;
        var (barrier, holder, holderPrepared) = await HoldSlotAsync(f, c);
        var members = Enumerable.Range(0, 4).Select(i => c.Prepared.Prepare(new PingJob("holder"))).ToList();
        var sends = members.Select(m => SendAsync(c, m)).ToList();
        await WaitQueuedAsync(c, 4);
        DriverFixture.ReleaseBarrier(barrier);

        var replies = await WithTimeout(Task.WhenAll(sends));
        var holderReply = await WithTimeout(holder);

        Assert.Equal(1, c.Store.Group!.Groups);
        Assert.Equal(4, c.Store.Group.GroupedCommands);
        Assert.All(replies, r =>
        {
            Assert.Equal(StoreStatus.Applied, r.Status);
            Assert.False(r.Replayed);
            Assert.Equal(DispatchDisposition.Created, r.Value!.Disposition);
        });
        Assert.Single(replies.Select(r => r.DecidedAt).Distinct());
        Assert.Equal(5, await Jobs(f));
        foreach (var m in members)
        {
            Assert.Equal(1, await Receipts(f, m.OperationId.Value));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE command_id = @c AND event_kind = 'dispatched' AND (details ->> 'ordinal') = '0'",
                ("c", m.OperationId.Value)));
        }

        // The grouped rows equal a single dispatch's, apart from ids and times.
        const string Shape = """
            SELECT j.job_name || '|' || j.queue || '|' || j.priority || '|' || j.state || '|' || j.payload_version || '|' ||
                   j.serializer_profile_id || '|' || j.serializer_profile_version || '|' || encode(p.payload_utf8, 'hex') || '|' ||
                   p.payload_encoding_version || '|' || p.headers::text || '|' || g.generation || '|' || g.created_reason || '|' ||
                   g.max_executions || '|' || g.max_unstarted_abandonments || '|' || g.execution_timeout_ms || '|' ||
                   g.retry_policy::text || '|' || g.unstarted_retry_policy::text || '|' || e.event_kind || '|' || e.details::text
            FROM runwick.jobs j
            JOIN runwick.job_payloads p ON p.application = j.application AND p.job_id = j.job_id
            JOIN runwick.job_generations g ON g.application = j.application AND g.job_id = j.job_id
            JOIN runwick.queue_events e ON e.application = j.application AND e.job_id = j.job_id
            WHERE j.job_id = @id
            """;
        var single = await TextAsync(f, Shape, ("id", holderReply.Value!.JobId));
        foreach (var r in replies)
        {
            Assert.Equal(single, await TextAsync(f, Shape, ("id", r.Value!.JobId)));
        }

        // Every command resends alone to its own original receipt.
        for (var i = 0; i < members.Count; i++)
        {
            var replay = await SendAsync(c, members[i]);
            Assert.True(replay.Replayed);
            Assert.Equal(replies[i].Value!.JobId, replay.Value!.JobId);
            Assert.Equal(replies[i].DecidedAt, replay.DecidedAt);
        }

        Assert.Equal(5, await Jobs(f));
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(holderPrepared.OperationId)).Status);
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_lone_caller_is_sent_at_once_and_never_forms_a_group()
    {
        var (f, c) = await StartAsync("b1lone");
        await using var _ = f;

        for (var i = 0; i < 5; i++)
        {
            var reply = await WithTimeout(c.Dispatcher.DispatchAsync(new PingJob("lone" + i)));
            Assert.Equal(DispatchDisposition.Created, reply.Disposition);
        }

        Assert.Equal(0, c.Store.Group!.Groups);
        Assert.Equal(5, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task Mixed_outcomes_in_one_group_each_get_their_own_reply()
    {
        var (f, c) = await StartAsync("b1mixed");
        await using var _ = f;
        var earlier = c.Prepared.Prepare(new PingJob("earlier"));
        var original = await SendAsync(c, earlier);

        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var fresh1 = c.Prepared.Prepare(new PingJob("fresh1"));
        var replay = earlier;
        var conflict = Edited(c, c.Prepared.Prepare(new PingJob("other")), i => i, commandId: earlier.OperationId.Value);
        var expired = Edited(c, c.Prepared.Prepare(new PingJob("late")), i => i, notAfter: DateTimeOffset.UtcNow.AddMilliseconds(500));
        var unregistered = Edited(c, c.Prepared.Prepare(new PingJob("nowhere")), i => i with { Queue = "not-a-queue" });
        var fresh2 = c.Prepared.Prepare(new PingJob("fresh2"));

        var sends = new[]
        {
            SendAsync(c, fresh1), SendAsync(c, replay), c.Store.DispatchAsync(conflict, CancellationToken.None),
            c.Store.DispatchAsync(expired, CancellationToken.None), c.Store.DispatchAsync(unregistered, CancellationToken.None),
            SendAsync(c, fresh2),
        };
        await WaitQueuedAsync(c, 6);
        await Task.Delay(900); // past the short member's deadline while the slot is held
        DriverFixture.ReleaseBarrier(barrier);
        var r = await WithTimeout(Task.WhenAll(sends));
        await WithTimeout(holder);

        Assert.Equal(1, c.Store.Group!.Groups);
        Assert.Equal((StoreStatus.Applied, false), (r[0].Status, r[0].Replayed));
        Assert.Equal((StoreStatus.Applied, true), (r[1].Status, r[1].Replayed));
        Assert.Equal(original.Value!.JobId, r[1].Value!.JobId);
        Assert.Equal(original.DecidedAt, r[1].DecidedAt);
        Assert.Equal(StoreStatus.CommandConflict, r[2].Status);
        Assert.Equal(StoreStatus.CommandExpired, r[3].Status);
        Assert.Equal((StoreStatus.InvalidRequest, "queue_not_registered"), (r[4].Status, r[4].Error!.Code));
        Assert.Equal((StoreStatus.Applied, false), (r[5].Status, r[5].Replayed));

        // earlier, holder, fresh1, fresh2; the refused members left no receipt and no row.
        Assert.Equal(4, await Jobs(f));
        Assert.Equal(1, await Receipts(f, earlier.OperationId.Value));
        Assert.Equal(0, await Receipts(f, expired.CommandId));
        Assert.Equal(0, await Receipts(f, unregistered.CommandId));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE application = @a AND result IS NULL", ("a", f.Application)));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_late_failure_rolls_the_group_back_and_each_command_retries_alone()
    {
        var (f, c) = await StartAsync("b1fallback");
        await using var _ = f;
        var existing = c.Prepared.Prepare(new PingJob("existing"));
        var existingReply = await SendAsync(c, existing);

        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var a = c.Prepared.Prepare(new PingJob("a"));
        var collide = Edited(c, c.Prepared.Prepare(new PingJob("collide")), i => i with { ProposedJobId = existingReply.Value!.JobId });
        var b = c.Prepared.Prepare(new PingJob("b"));
        var sends = new[] { SendAsync(c, a), c.Store.DispatchAsync(collide, CancellationToken.None), SendAsync(c, b) };
        await WaitQueuedAsync(c, 3);
        DriverFixture.ReleaseBarrier(barrier);
        var r = await WithTimeout(Task.WhenAll(sends));
        await WithTimeout(holder);

        Assert.Equal(1, c.Store.Group!.Groups);
        Assert.Equal(3, c.Store.Group.Fallbacks);
        Assert.Equal(StoreStatus.Applied, r[0].Status);
        Assert.Equal(StoreStatus.InvariantFault, r[1].Status);
        Assert.Equal("id_collision", r[1].Error!.Code);
        Assert.Equal(StoreStatus.Applied, r[2].Status);
        Assert.Equal(4, await Jobs(f)); // existing, holder, a, b
        Assert.Equal(0, await Receipts(f, collide.CommandId));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_caller_that_cancels_while_queued_leaves_nothing_and_the_rest_still_send()
    {
        var (f, c) = await StartAsync("b1cancelq");
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var a = c.Prepared.Prepare(new PingJob("a"));
        var b = c.Prepared.Prepare(new PingJob("b"));
        using var cts = new CancellationTokenSource();
        var sendA = SendAsync(c, a);
        var sendB = c.Store.DispatchAsync(Command(b, notAfter: null, application: c.Application), cts.Token);
        await WaitQueuedAsync(c, 2);

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sendB);
        Assert.Equal(1, c.Store.Group!.Queued);
        DriverFixture.ReleaseBarrier(barrier);

        Assert.Equal(StoreStatus.Applied, (await WithTimeout(sendA)).Status);
        await WithTimeout(holder);
        Assert.Equal(2, await Jobs(f));
        Assert.Equal(0, await Receipts(f, b.OperationId.Value));
        Assert.Equal(0, c.Store.Group.Groups);
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_caller_that_cancels_after_submission_can_resolve_its_operation_id()
    {
        var (f, c) = await StartAsync("b1cancelsent");
        await using var _ = f;
        var (holdBarrier, holder, _) = await HoldSlotAsync(f, c);
        var a = c.Prepared.Prepare(new PingJob("a"));
        var b = c.Prepared.Prepare(new PingJob("b"));
        var d = c.Prepared.Prepare(new PingJob("d"));
        using var cts = new CancellationTokenSource();
        var sendA = SendAsync(c, a);
        await WaitQueuedAsync(c, 1);
        var sendB = c.Store.DispatchAsync(Command(b, application: c.Application), cts.Token);
        await WaitQueuedAsync(c, 2);
        var sendD = SendAsync(c, d);
        await WaitQueuedAsync(c, 3);
        var groupBarrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(a.OperationId.Value));
        DriverFixture.ReleaseBarrier(holdBarrier);
        await DriverFixture.WaitAtBarrier(groupBarrier, Wait);

        await cts.CancelAsync();
        var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => sendB);
        Assert.Equal(b.OperationId.Value, unknown.CommandId);
        DriverFixture.ReleaseBarrier(groupBarrier);

        Assert.Equal(StoreStatus.Applied, (await WithTimeout(sendA)).Status);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(sendD)).Status);
        await WithTimeout(holder);
        Assert.Equal(DispatchResolutionStatus.Confirmed, (await c.Prepared.ResolveAsync(b.OperationId)).Status);
        var again = await SendAsync(c, b);
        Assert.True(again.Replayed);
        Assert.Equal(4, await Jobs(f));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task A_lost_group_reply_is_outcome_unknown_for_every_member_and_each_resends_to_its_receipt()
    {
        var (f, c) = await StartAsync("b1lost");
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var members = Enumerable.Range(0, 3).Select(i => c.Prepared.Prepare(new PingJob("m" + i))).ToList();
        var sends = new List<Task<StoreReply<DispatchResult>>>();
        foreach (var m in members)
        {
            sends.Add(SendAsync(c, m));
            await WaitQueuedAsync(c, sends.Count);
        }

        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(members[0].OperationId.Value));
        DriverFixture.ReleaseBarrier(barrier);
        await WithTimeout(holder);

        foreach (var (send, m) in sends.Zip(members))
        {
            var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => send);
            Assert.Equal(m.OperationId.Value, unknown.CommandId);
        }

        Assert.Equal(1, drop.Fired);
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
    public async Task Disposing_the_group_settles_queued_commands_as_not_applied()
    {
        await using var f = await DriverFixture.StartAsync(Ns("b1dispose"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await using var store = f.NewStore(options: new PostgresOptions { GroupProducerAdmission = true });
        var holderPrepared = c.Prepared.Prepare(new PingJob("holder"));
        var barrier = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(holderPrepared.OperationId.Value));
        var holder = store.DispatchAsync(Command(holderPrepared, application: c.Application), CancellationToken.None);
        await DriverFixture.WaitAtBarrier(barrier, Wait);
        var queued = Enumerable.Range(0, 2).Select(i => store.DispatchAsync(Command(c.Prepared.Prepare(new PingJob("q" + i)), application: c.Application), CancellationToken.None)).ToList();
        var deadline = DateTime.UtcNow + Wait;
        while (store.Group!.Queued < 2)
        {
            Assert.True(DateTime.UtcNow < deadline);
            await Task.Delay(10);
        }

        await store.Group.DisposeAsync();
        foreach (var q in queued)
        {
            var ex = await Assert.ThrowsAsync<StoreNotAppliedException>(() => q);
            Assert.Equal("store_unavailable", ex.Code);
        }

        DriverFixture.ReleaseBarrier(barrier);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(holder)).Status);
        Assert.Equal(1, await Jobs(f));
        await Assert.ThrowsAsync<StoreNotAppliedException>(() => store.DispatchAsync(Command(c.Prepared.Prepare(new PingJob("after")), application: c.Application), CancellationToken.None));
    }

    [Fact]
    [Trait("Item", "PF2-B1")]
    public async Task Commands_that_are_not_plain_take_the_ordinary_path()
    {
        var (f, c) = await StartAsync("b1plain");
        await using var _ = f;
        var (barrier, holder, _) = await HoldSlotAsync(f, c);
        var keyed = Enumerable.Range(0, 3).Select(i => c.Prepared.Prepare(new KeyedJob("k" + i, "key" + i),
            new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "w" + i, TimeSpan.FromHours(1)) })).ToList();
        var sends = keyed.Select(k => SendAsync(c, k)).ToList();
        await Task.Delay(300);
        Assert.Equal(0, c.Store.Group!.Queued);
        DriverFixture.ReleaseBarrier(barrier);

        Assert.All(await WithTimeout(Task.WhenAll(sends)), r => Assert.Equal(StoreStatus.Applied, r.Status));
        await WithTimeout(holder);
        Assert.Equal(0, c.Store.Group.Groups);
        Assert.Equal(4, await Jobs(f));
    }
}
