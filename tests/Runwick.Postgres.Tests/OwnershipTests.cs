using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Proxy;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>RUN-01/02, OWN-01/02/03/05 and the ownership variants of CMD-01/03/04/05 (07 §4, §6): SP-008.</summary>
public sealed class OwnershipTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    sealed record Outcome(StoreStatus Status, StoreEffect Effect, bool Replayed, DateTimeOffset DecidedAt, string Value);

    static Outcome Of<T>(StoreReply<T> r, Func<T, string> value) where T : class =>
        new(r.Status, r.Effect, r.Replayed, r.DecidedAt, r.Value is { } v ? value(v) : "");

    static DateTimeOffset NotAfter(TimeSpan lifetime) =>
        DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + lifetime).ToUnixTimeMilliseconds());

    public static TheoryData<string> OwnershipOperations() =>
        [Operations.Reserve, Operations.Begin, Operations.Finish, Operations.Recover, Operations.ReleaseReservation];

    /// <summary>Arranges the pre-state for one ownership command; the returned call sends the same StoreCommand through any store.</summary>
    static async Task<(Guid CommandId, Func<PgStore, Task<Outcome>> Call)> ArrangeAsync(string operation, TestClient c)
    {
        var id = Guid.CreateVersion7();
        var na = NotAfter(TimeSpan.FromSeconds(50));
        await c.Dispatcher.DispatchAsync(new ResourceJob("x", "r1"));
        if (operation == Operations.Reserve)
        {
            var cmd = c.Command(new ReserveRequest(c.WorkerInstanceId, "default", "medium", c.Contracts, null, TimeSpan.FromSeconds(30)), id, na);
            return (id, async s => Of(await s.TryReserveAsync(cmd, CancellationToken.None),
                v => $"{v.Position.JobId}/{v.Lease.Handle.AttemptId}/{v.Lease.Handle.OwnershipToken}/{v.Lease.ExpiresAt:O}"));
        }

        var reserved = (await c.ReserveAsync(lease: operation == Operations.Recover ? TimeSpan.FromSeconds(1) : null)).Value!;
        var lease = reserved.Lease.Handle;
        switch (operation)
        {
            case Operations.Begin:
                var begin = c.Command(new BeginRequest(lease), id, na);
                return (id, async s => Of((await s.BeginExecutionAsync(begin, CancellationToken.None)).Reply, v => v.ToString()));
            case Operations.ReleaseReservation:
                var release = c.Command(new ReleaseRequest(lease), id, na);
                return (id, async s => Of(await s.ReleaseReservationAsync(release, CancellationToken.None), v => v.ToString()));
            case Operations.Finish:
                await c.BeginAsync(lease);
                var finish = c.Command(new FinishRequest(lease, AttemptOutcome.Succeeded, TimeSpan.FromMilliseconds(5), null, null), id, na);
                return (id, async s => Of(await s.FinishAsync(finish, CancellationToken.None), v => v.ToString()));
            case Operations.Recover:
                await Task.Delay(1100);
                var expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).Single();
                var recover = c.Command(new RecoverRequest(expired.JobId, expired.AttemptId, expired.OwnershipToken), id, na);
                return (id, async s => Of(await s.RecoverAsync(recover, CancellationToken.None), v => v.ToString()));
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    static async Task AssertSingleEffect(DriverFixture f, Guid commandId)
    {
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal(1, await Receipts(f, commandId));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));
        Assert.True(await f.CountAsync("SELECT max(execution_count) FROM runwick.jobs") <= 1);
        Assert.True(await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits") <= 1);
    }

    [Theory]
    [MemberData(nameof(OwnershipOperations))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "CMD-01")]
    public async Task Same_ownership_command_concurrently_and_sequentially_has_one_effect(string operation)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd01o"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var other = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (id, call) = await ArrangeAsync(operation, c);

        var h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(id));
        var first = call(c.Store);
        await DriverFixture.WaitAtBarrier(h, Wait);
        var second = call(other.Store);
        await Task.Delay(300);
        Assert.False(second.IsCompleted, "the second connection did not wait for the in-flight receipt");
        DriverFixture.ReleaseBarrier(h);
        var a = await WithTimeout(first);
        var b = await WithTimeout(second);

        Assert.Equal((StoreStatus.Applied, StoreEffect.Applied, false), (a.Status, a.Effect, a.Replayed));
        Assert.Equal(a with { Replayed = true }, b);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(a with { Replayed = true }, await call(c.Store));
        }

        await AssertSingleEffect(f, id);
    }

    [Theory]
    [MemberData(nameof(OwnershipOperations))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "CMD-03")]
    public async Task Lost_ownership_reply_is_outcome_unknown_and_the_same_command_replays(string operation)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd03o"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (id, call) = await ArrangeAsync(operation, c);

        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        var unknown = await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => call(c.Store));
        Assert.Equal(1, drop.Fired);
        Assert.Equal(id, unknown.CommandId);

        var replay = await call(c.Store);
        Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        await AssertSingleEffect(f, id);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "RUN-01")]
    public async Task Crash_before_begin_versus_after_begin()
    {
        await using var f = await DriverFixture.StartAsync(Ns("run01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        async Task<(Guid Job, LeaseHandle Lease)> PeerCrashesInBegin(string identity, FaultPoint point)
        {
            var peer = await f.OpenPeer(identity);
            await c.Dispatcher.DispatchAsync(new ResourceJob(identity, "r-" + identity));
            var reserved = await peer.SendAsync("reserve", new JsonObject { ["leaseMs"] = 3000 });
            var lease = PeerJson.Lease(reserved["lease"]!);
            await peer.SendAsync("armFault", new JsonObject { ["point"] = point.ToString(), ["action"] = "KillProcess", ["operation"] = Operations.Begin });
            var begin = peer.SendInBackground("begin", new JsonObject { ["lease"] = PeerJson.Lease(lease) });
            var barrier = peer.WaitForEventAsync("atBarrier", timeout: Wait);
            if (await Task.WhenAny(barrier, begin) == begin)
            {
                Assert.Fail($"Begin returned before {point}: {await Record.ExceptionAsync(() => begin)} {(begin.IsCompletedSuccessfully ? begin.Result.ToJsonString() : "")}");
            }

            await barrier;
            peer.Kill();
            await Record.ExceptionAsync(() => begin);

            // The job the peer actually reserved: (a)'s recovered job becomes eligible again within its unstarted delay.
            return (lease.JobId, lease);
        }

        async Task<(StoreReply<FinishedAttempt> First, StoreReply<FinishedAttempt> Replay, StoreReply<FinishedAttempt> Competing)> RecoverAsync(Guid job)
        {
            ExpiredLease? expired = null;
            var deadline = DateTime.UtcNow + Wait;
            while (expired is null)
            {
                Assert.True(DateTime.UtcNow < deadline, "the crashed peer's lease never expired");
                await Task.Delay(100);
                expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).SingleOrDefault(e => e.JobId == job);
            }

            var id = Guid.CreateVersion7();
            var na = NotAfter(TimeSpan.FromSeconds(30));
            var first = await c.RecoverAsync(expired, id, na);
            var replay = c.RecoverAsync(expired, id, na);
            var competing = c.RecoverAsync(expired);
            return (first, await replay, await competing);
        }

        // (a) Killed before Begin's gate lock: the reservation is abandoned unstarted.
        var (j1, l1) = await PeerCrashesInBegin("peer-a", FaultPoint.BeforeGateLock);
        var (first, replay, competing) = await RecoverAsync(j1);
        Assert.Equal(StoreStatus.Applied, first.Status);
        Assert.Equal((StoreStatus.Applied, true, first.DecidedAt), (replay.Status, replay.Replayed, replay.DecidedAt));
        Assert.Equal(StoreStatus.StaleLease, competing.Status);
        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs j JOIN runwick.job_generations g USING (application, job_id, generation)
            WHERE j.job_id = @j AND j.state = 'pending' AND j.execution_count = 0 AND g.started_count = 0 AND g.consecutive_unstarted_count = 1
            """, ("j", j1)));
        Assert.Equal(1, await f.CountAsync(
            "SELECT count(*) FROM runwick.job_attempts WHERE attempt_id = @a AND outcome = 'abandoned_before_start' AND execution_number IS NULL",
            ("a", l1.AttemptId)));

        // (b) Killed after Begin committed: the execution counts once; recovery schedules a retry.
        var (j2, l2) = await PeerCrashesInBegin("peer-b", FaultPoint.AfterCommitBeforeReply);
        var (first2, replay2, competing2) = await RecoverAsync(j2);
        Assert.Equal(StoreStatus.Applied, first2.Status);
        Assert.Equal((StoreStatus.Applied, true), (replay2.Status, replay2.Replayed));
        Assert.Equal(StoreStatus.StaleLease, competing2.Status);
        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs j JOIN runwick.job_generations g USING (application, job_id, generation)
            WHERE j.job_id = @j AND j.state = 'retrying' AND j.execution_count = 1 AND g.started_count = 1 AND g.consecutive_unstarted_count = 0
            """, ("j", j2)));
        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.job_attempts a JOIN runwick.jobs j USING (application, job_id)
            WHERE a.attempt_id = @a AND a.outcome = 'abandoned_running' AND a.execution_number = 1
              AND j.eligible_at = a.finished_at + a.retry_delay_ms * interval '1 millisecond'
            """, ("a", l2.AttemptId)));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "RUN-02")]
    public async Task Concurrent_and_replayed_begin()
    {
        await using var f = await DriverFixture.StartAsync(Ns("run02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var other = await f.OpenClient(configure: r => TestJobs.Register(r));

        // (a) The same Begin on two connections.
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("a", "ra"))).JobId.Value;
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        var cmd = c.Command(new BeginRequest(lease), Guid.CreateVersion7(), NotAfter(TimeSpan.FromSeconds(30)));
        var h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(cmd.CommandId));
        var first = c.Store.BeginExecutionAsync(cmd, CancellationToken.None);
        await DriverFixture.WaitAtBarrier(h, Wait);
        var second = other.Store.BeginExecutionAsync(cmd, CancellationToken.None);
        await Task.Delay(300);
        DriverFixture.ReleaseBarrier(h);
        var a = (await WithTimeout(first)).Reply;
        var b = (await WithTimeout(second)).Reply;
        Assert.Equal((StoreStatus.Applied, false), (a.Status, a.Replayed));
        Assert.Equal((StoreStatus.Applied, true), (b.Status, b.Replayed));
        Assert.Equal(a.Value, b.Value);
        Assert.Equal((1, 1L), (a.Value!.ExecutionNumber, a.Value.TotalExecutionNumber));

        // (b) Two Begin commands with different IDs for one lease.
        var job2 = (await c.Dispatcher.DispatchAsync(new ResourceJob("b", "rb"))).JobId.Value;
        var lease2 = (await c.ReserveAsync()).Value!.Lease.Handle;
        var id1 = Guid.CreateVersion7();
        var h2 = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(id1));
        var b1 = c.BeginAsync(lease2, id1);
        await DriverFixture.WaitAtBarrier(h2, Wait);
        var b2 = other.BeginAsync(lease2);
        await Task.Delay(300);
        DriverFixture.ReleaseBarrier(h2);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(b1)).Reply.Status);
        Assert.Equal(StoreStatus.InvalidState, (await WithTimeout(b2)).Reply.Status);

        foreach (var (j, l) in new[] { (job, lease), (job2, lease2) })
        {
            Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", j)));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j AND authorized_at IS NOT NULL", ("j", j)));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE attempt_id = @a", ("a", l.AttemptId)));
        }

        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_started'"));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "OWN-01")]
    public async Task Renew_wins_over_a_stale_recovery_discovery()
    {
        await using var f = await DriverFixture.StartAsync(Ns("own01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        // The discovery snapshot names A's original expiry. The Renew commits well inside that lease (a 1.5 s margin
        // was too small on a loaded host: G-1 run 3), and the Recover is released only once the original expiry has
        // passed, so its StaleLease comes from rechecking the fresh expiry under its locks.
        var reserved = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(10))).Value!;
        var lease = reserved.Lease.Handle;
        await c.BeginAsync(lease);
        var stale = new ExpiredLease(lease.JobId, lease.AttemptId, lease.OwnershipToken, JobStates.Running, reserved.Lease.ExpiresAt);
        var h = f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Barrier, FaultTarget.Op(Operations.Recover));
        var recover = c.RecoverAsync(stale);
        await DriverFixture.WaitAtBarrier(h, Wait);
        var renewed = await c.RenewAsync(lease, TimeSpan.FromSeconds(30));
        Assert.Equal(StoreStatus.Applied, renewed.Status);
        while (await f.StoreNow() <= reserved.Lease.ExpiresAt + TimeSpan.FromMilliseconds(100))
        {
            await Task.Delay(50);
        }

        DriverFixture.ReleaseBarrier(h);
        var result = await WithTimeout(recover);
        Assert.Equal((StoreStatus.StaleLease, StoreEffect.ObservedNoEffect), (result.Status, result.Effect));

        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs j JOIN runwick.concurrency_permits p USING (application, job_id)
            WHERE j.job_id = @j AND j.state = 'running' AND j.current_attempt_id = @a
              AND j.lease_expires_at = @e AND p.lease_expires_at = @e
            """, ("j", lease.JobId), ("a", lease.AttemptId), ("e", renewed.Value!.Lease.ExpiresAt.UtcDateTime)));
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease)).Status);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "OWN-02")]
    public async Task Late_commands_from_an_expired_owner_are_stale()
    {
        await using var f = await DriverFixture.StartAsync(Ns("own02"));
        var a = await f.OpenClient(configure: r => TestJobs.Register(r));
        var b = await f.OpenClient(configure: r => TestJobs.Register(r));
        await a.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        var aLease = (await a.ReserveAsync(lease: TimeSpan.FromSeconds(1))).Value!.Lease.Handle;
        await a.BeginAsync(aLease);

        ExpiredLease? expired = null;
        while (expired is null)
        {
            await Task.Delay(100);
            expired = (await b.Store.ListExpiredLeasesAsync(b.Application, 10, CancellationToken.None)).SingleOrDefault();
        }

        Assert.Equal(StoreStatus.Applied, (await b.RecoverAsync(expired)).Status);
        StoreReply<ReservedJob> bReserved;
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        while ((bReserved = await b.ReserveAsync()).Status != StoreStatus.Applied && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(200);
        }

        var bLease = bReserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await b.BeginAsync(bLease)).Reply.Status);
        var pre = await f.ReadCommittedSnapshot();

        foreach (var reply in new (StoreStatus, StoreEffect)[]
        {
            Pair(await a.RenewAsync(aLease)),
            Pair(await a.FinishAsync(aLease, AttemptOutcome.Succeeded)),
            Pair(await a.ReleaseAsync(aLease)),
        })
        {
            Assert.Equal((StoreStatus.StaleLease, StoreEffect.ObservedNoEffect), reply);
        }

        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(["abandoned_running"], await TextAsync(f, "SELECT coalesce(outcome, '<null>') FROM runwick.job_attempts WHERE attempt_id = @a", ("a", aLease.AttemptId)));

        static (StoreStatus, StoreEffect) Pair<T>(StoreReply<T> r) where T : class => (r.Status, r.Effect);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "OWN-03")]
    public async Task Lease_expiring_during_a_lock_wait_is_stale()
    {
        await using var f = await DriverFixture.StartAsync(Ns("own03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new SharedJob("1", "s"));
        await c.Dispatcher.DispatchAsync(new SharedJob("2", "s"));
        var r1 = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!;
        var r2 = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!;
        await c.BeginAsync(r1.Lease.Handle);
        await c.BeginAsync(r2.Lease.Handle);
        var pre = await f.ReadCommittedSnapshot();

        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync();
        await ExecAsync(conn, tx, "SELECT 1 FROM runwick.concurrency_scopes WHERE scope_kind = 'resource' AND scope_key = 's' FOR UPDATE");
        var renew = c.RenewAsync(r1.Lease.Handle);
        var finish = c.FinishAsync(r2.Lease.Handle, AttemptOutcome.Succeeded);
        await Task.Delay(300);
        Assert.False(renew.IsCompleted || finish.IsCompleted, "Renew/Finish did not wait for the scope lock");
        var expiry = new[] { r1.Lease.ExpiresAt, r2.Lease.ExpiresAt }.Max() + TimeSpan.FromMilliseconds(100);
        while (await f.StoreNow() <= expiry)
        {
            await Task.Delay(100);
        }

        var releasedAt = await f.StoreNow();
        await tx.RollbackAsync();
        var renewed = await WithTimeout(renew);
        var finished = await WithTimeout(finish);
        Assert.Equal((StoreStatus.StaleLease, StoreStatus.StaleLease), (renewed.Status, finished.Status));
        Assert.True(renewed.DecidedAt >= PgTime.Floor(releasedAt) && finished.DecidedAt >= PgTime.Floor(releasedAt),
            $"decisions {renewed.DecidedAt:O}/{finished.DecidedAt:O} precede the blocker release {releasedAt:O}");
        var post = await f.ReadCommittedSnapshot();
        Assert.Equal(pre["jobs"], post["jobs"]);
        Assert.Equal(pre["concurrency_permits"], post["concurrency_permits"]);
        Assert.Equal(pre["job_attempts"], post["job_attempts"]);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "OWN-05")]
    public async Task Missing_or_mismatched_permits_fault_renew_without_changes()
    {
        await using var f = await DriverFixture.StartAsync(Ns("own05"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PairJob("p", "r-a", "r-b"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE attempt_id = @a", ("a", lease.AttemptId)));

        // (a) One permit row deleted.
        await f.ExecAsync("""
            CREATE TABLE public.saved AS SELECT * FROM runwick.concurrency_permits WHERE attempt_id = @a ORDER BY scope_id LIMIT 1;
            DELETE FROM runwick.concurrency_permits p USING public.saved s WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.attempt_id = s.attempt_id;
            """, ("a", lease.AttemptId));
        var pre = await f.ReadCommittedSnapshot();
        var a = await c.RenewAsync(lease);
        Assert.Equal((StoreStatus.InvariantFault, "permit_set_mismatch"), (a.Status, a.Error!.Code));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // (b) Restored, with one permit's expiry different from the job's.
        await f.ExecAsync("""
            INSERT INTO runwick.concurrency_permits SELECT * FROM public.saved;
            UPDATE runwick.concurrency_permits p SET lease_expires_at = p.lease_expires_at + interval '1 second'
            FROM public.saved s WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.attempt_id = s.attempt_id;
            """);
        var pre2 = await f.ReadCommittedSnapshot();
        var b = await c.RenewAsync(lease);
        Assert.Equal((StoreStatus.InvariantFault, "permit_set_mismatch"), (b.Status, b.Error!.Code));
        Assert.Empty(pre2.DiffTables(await f.ReadCommittedSnapshot()));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "CMD-04")]
    public async Task Finish_whose_deadline_expires_during_a_lock_wait_is_expired()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd04b"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        var pre = await f.ReadCommittedSnapshot();

        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync();
        await ExecAsync(conn, tx, "SELECT 1 FROM runwick.concurrency_scopes WHERE scope_kind = 'resource' FOR UPDATE");
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((await f.StoreNow() + TimeSpan.FromSeconds(1)).ToUnixTimeMilliseconds());
        var finish = c.FinishAsync(lease, AttemptOutcome.Succeeded, notAfter: notAfter);
        while (await f.StoreNow() <= notAfter)
        {
            await Task.Delay(100);
        }

        await Task.Delay(200);
        await tx.RollbackAsync();
        var reply = await WithTimeout(finish);
        Assert.Equal((StoreStatus.CommandExpired, StoreEffect.ObservedNoEffect), (reply.Status, reply.Effect));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-008")]
    [Trait("Case", "CMD-05")]
    public async Task Finish_replayed_after_receipt_pruning_is_expired()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd05f"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        var id = Guid.CreateVersion7();
        var notAfter = NotAfter(TimeSpan.FromSeconds(3));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter)).Status);
        while (await f.StoreNow() <= notAfter)
        {
            await Task.Delay(100);
        }

        Assert.True(await f.PruneReceipts(Operations.RetentionClass(Operations.Finish), DateTimeOffset.UtcNow + TimeSpan.FromDays(3650)) >= 1);
        var pre = await f.ReadCommittedSnapshot();
        var replay = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
        Assert.Equal(StoreStatus.CommandExpired, replay.Status);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'"));
    }
    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-008")]
    [Trait("Item", "SP-005")]
    [Trait("Case", "DEP-04")]
    public async Task Lost_commit_cancel_and_disconnect_resolve_by_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dep04"), StoreProfile.DirectPrimaryViaProxy);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var proxy = f.Proxy!;
        var control = Harness.Proxy.PgFaultProxy.Role("control", "local");

        async Task<(StoreCommand<FinishRequest> Cmd, CommandIdentity Identity)> RunningFinishAsync(string name)
        {
            await c.Dispatcher.DispatchAsync(new ResourceJob(name, "r-" + name));
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
            var cmd = c.Command(new FinishRequest(lease, AttemptOutcome.Succeeded, TimeSpan.FromMilliseconds(5), null, null),
                Guid.CreateVersion7(), NotAfter(TimeSpan.FromSeconds(50)));
            var fp = CommandEnvelopes.Fingerprint(c.Store.StoreId, c.Application, Operations.Finish, cmd.CommandId, cmd.NotAfter, CommandEnvelopes.Finish(cmd.Body));
            return (cmd, new CommandIdentity(c.Application, cmd.CommandId, Operations.Finish, 1, fp, cmd.NotAfter));
        }

        async Task<ReceiptResolution> ResolveAsync(CommandIdentity identity)
        {
            // The broken pooled connection may surface once as a not-applied store error.
            for (var i = 0; ; i++)
            {
                try
                {
                    return await c.Store.ResolveCommandAsync(identity, CancellationToken.None);
                }
                catch (StoreNotAppliedException) when (i < 3)
                {
                }
            }
        }

        async Task AssertOneEffect(StoreCommand<FinishRequest> cmd)
        {
            Assert.Equal(1, await Receipts(f, cmd.CommandId));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'succeeded'", ("j", cmd.Body.Lease.JobId)));
            Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", cmd.Body.Lease.JobId)));
        }

        // (a) COMMIT reply dropped: outcome unknown; the receipt resolves it to the one recorded effect.
        var (a, aId) = await RunningFinishAsync("a");
        var dropped = proxy.ArmCommitReplyDrop(1, control);
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.Store.FinishAsync(a, CancellationToken.None));
        await dropped.WaitAsync(Wait);
        Assert.IsType<ReceiptResolution.Recorded>(await ResolveAsync(aId));
        await AssertOneEffect(a);

        // (b) Caller cancels while COMMIT is in flight (reply delayed 2 s): outcome unknown, never rollback evidence.
        var (b, bId) = await RunningFinishAsync("b");
        var hb = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(b.CommandId));
        using (var cts = new CancellationTokenSource())
        {
            var pending = c.Store.FinishAsync(b, cts.Token);
            await DriverFixture.WaitAtBarrier(hb, Wait);
            proxy.SetLatency(TimeSpan.FromSeconds(2), LatencyDirection.ServerToClient, control);
            DriverFixture.ReleaseBarrier(hb);
            await Task.Delay(300);
            await cts.CancelAsync();
            await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => pending);
        }

        proxy.SetLatency(TimeSpan.Zero);
        Assert.IsType<ReceiptResolution.Recorded>(await ResolveAsync(bId));
        await AssertOneEffect(b);

        // (c) Severed before COMMIT is sent: nothing committed; the arbiter finds no receipt (known not applied)
        // and the same command then applies once.
        var (cc, cId) = await RunningFinishAsync("c");
        var hc = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(cc.CommandId));
        var severed = c.Store.FinishAsync(cc, CancellationToken.None);
        await DriverFixture.WaitAtBarrier(hc, Wait);
        Assert.True(proxy.Sever(control) >= 1);
        await Task.Delay(200);
        DriverFixture.ReleaseBarrier(hc);
        var severedError = await Record.ExceptionAsync(() => severed);
        Assert.True(severedError is StoreOutcomeUnknownException or StoreNotAppliedException, $"unexpected {severedError}");
        Assert.IsType<ReceiptResolution.NotObserved>(await ResolveAsync(cId));
        Assert.Equal(0, await Receipts(f, cc.CommandId));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'running'", ("j", cc.Body.Lease.JobId)));
        var retried = await c.Store.FinishAsync(cc, CancellationToken.None);
        Assert.Equal((StoreStatus.Applied, false), (retried.Status, retried.Replayed));
        await AssertOneEffect(cc);

        // (d) Severed after COMMIT was forwarded (reply held by latency): outcome unknown, resolved to the effect.
        var (d, dId) = await RunningFinishAsync("d");
        var hd = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Command(d.CommandId));
        var lost = c.Store.FinishAsync(d, CancellationToken.None);
        await DriverFixture.WaitAtBarrier(hd, Wait);
        proxy.SetLatency(TimeSpan.FromSeconds(2), LatencyDirection.ServerToClient, control);
        DriverFixture.ReleaseBarrier(hd);
        await Task.Delay(500);
        proxy.Sever(control);
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => lost);
        proxy.SetLatency(TimeSpan.Zero);
        Assert.IsType<ReceiptResolution.Recorded>(await ResolveAsync(dId));
        await AssertOneEffect(d);
    }
}
