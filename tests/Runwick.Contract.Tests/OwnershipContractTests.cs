using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// OWN-02, OWN-03, OWN-05, CMD-04, CMD-05, RENEWMANY-01 and RENEWMANY-02 (07 §4, §6; PF-06) as theories over <see cref="StoreKind"/>, ported from the
/// PostgreSQL suite. Lock waits come from <see cref="WriteHold"/>; admin writes (a deleted or shifted permit, aged receipts) use the store's own admin
/// path, and only to set up the state under test.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class OwnershipContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static IRetentionStore Retention(TestClient c) => (IRetentionStore)c.Driver;

    static readonly CleanupLimits Limits = new(100, 1 << 20, TimeSpan.FromSeconds(5));

    /// <summary>One full retention pass over every record class (what ages a receipt out of the store).</summary>
    static async Task FullCleanupAsync(TestClient c)
    {
        foreach (var recordClass in RecordClasses.All)
        {
            for (var i = 0; i < 10_000; i++)
            {
                var r = await Retention(c).RunCleanupBatchAsync(c.Application, recordClass, Limits, CancellationToken.None);
                Assert.True(r.Status == "done", $"{recordClass}: {r.Status} {r.ErrorCode}");
                if (r.PassComplete)
                {
                    break;
                }
            }
        }
    }

    /// <summary>Moves every receipt 400 days into the past (their retention ended), then runs cleanup, as the retention cases do.</summary>
    static async Task PruneReceiptsAsync(DriverFixture f, TestClient c)
    {
        switch (f.Kind)
        {
            case StoreKind.Redis:
                await RedisAdmin.ExecAsync(f, "UPDATE runwick.storage_commands SET not_after = not_after - interval '400 days', retain_until = retain_until - interval '400 days'", []);
                break;
            case StoreKind.Sqlite:
                await f.ExecAsync("UPDATE runwick_storage_commands SET not_after = not_after - @by, retain_until = retain_until - @by", ("by", (long)TimeSpan.FromDays(400).TotalMilliseconds));
                break;
            default:
                await f.ExecAsync("UPDATE runwick.storage_commands SET not_after = not_after - interval '400 days', retain_until = retain_until - interval '400 days'");
                break;
        }

        await FullCleanupAsync(c);
    }

    /// <summary>Takes one permit of an attempt out of the store (setup); the returned action puts it back with its expiry moved by a second.</summary>
    static async Task<Func<Task>> TakePermitAsync(DriverFixture f, Guid attempt)
    {
        if (f.Kind == StoreKind.Redis)
        {
            var taken = await RedisAdmin.TakePermit(f.Application, attempt);
            return () => RedisAdmin.PutPermitShifted(f.Application, taken, 1000);
        }

        var sqlite = f.Kind == StoreKind.Sqlite;
        await f.ExecAsync(sqlite
            ? """
              CREATE TABLE saved AS SELECT * FROM runwick_concurrency_permits WHERE attempt_id = @a ORDER BY scope_id LIMIT 1;
              DELETE FROM runwick_concurrency_permits WHERE attempt_id = @a AND scope_id = (SELECT scope_id FROM saved);
              """
            : """
              CREATE TABLE public.saved AS SELECT * FROM runwick.concurrency_permits WHERE attempt_id = @a ORDER BY scope_id LIMIT 1;
              DELETE FROM runwick.concurrency_permits p USING public.saved s WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.attempt_id = s.attempt_id;
              """, ("a", attempt));
        return () => f.ExecAsync(sqlite
            ? """
              INSERT INTO runwick_concurrency_permits SELECT * FROM saved;
              UPDATE runwick_concurrency_permits SET lease_expires_at = lease_expires_at + 1000
                WHERE attempt_id = @a AND scope_id = (SELECT scope_id FROM saved);
              """
            : """
              INSERT INTO runwick.concurrency_permits SELECT * FROM public.saved;
              UPDATE runwick.concurrency_permits p SET lease_expires_at = p.lease_expires_at + interval '1 second'
              FROM public.saved s WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.attempt_id = s.attempt_id;
              """, ("a", attempt));
    }

    /// <summary>
    /// Whether the jobs, permits and attempts that existed in <paramref name="before"/> are unchanged in <paramref name="after"/>, and nothing was
    /// added except <paramref name="extraJobs"/> jobs and as many receipts: the SQLite lock hold is itself a dispatch that commits when released.
    /// </summary>
    static bool Same(StoreSnapshot before, StoreSnapshot after, int extraJobs = 0) =>
        after.Jobs.Where(j => before.Jobs.Any(b => b.JobId == j.JobId)).SequenceEqual(before.Jobs)
        && after.Jobs.Count == before.Jobs.Count + extraJobs
        && after.Receipts.Count == before.Receipts.Count + extraJobs
        && after.Permits.SequenceEqual(before.Permits) && after.Attempts.SequenceEqual(before.Attempts);

    /// <summary>The jobs the lock hold itself adds when it is released (<see cref="WriteHold"/>).</summary>
    static int HoldJobs(DriverFixture f) => f.Kind == StoreKind.Sqlite ? 1 : 0;

    // ------------------------------------------------------------------ OWN-02, OWN-03, OWN-05

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OWN-02")]
    public async Task Late_commands_from_an_expired_owner_are_stale(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("own02"), kind);
        var a = await f.OpenClient(configure: r => TestJobs.Register(r));
        var b = await f.OpenClient(configure: r => TestJobs.Register(r));
        await a.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        // A must have begun before its lease expires, or its attempt is abandoned before start (07 I-18).
        var aLease = (await a.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await a.BeginAsync(aLease)).Reply.Status);

        ExpiredLease? expired = null;
        while (expired is null)
        {
            await Task.Delay(100);
            expired = (await b.Driver.ListExpiredLeasesAsync(b.Application, 10, CancellationToken.None)).SingleOrDefault();
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
        Assert.Equal("abandoned_running", (await Model(f)).Attempts.Single(x => x.AttemptId == aLease.AttemptId).Outcome);
        Assert.Equal(1, await Permits(f));

        static (StoreStatus, StoreEffect) Pair<T>(StoreReply<T> r) where T : class => (r.Status, r.Effect);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OWN-03")]
    public async Task Lease_expiring_during_a_lock_wait_is_stale(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("own03"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new SharedJob("1", "s"));
        await c.Dispatcher.DispatchAsync(new SharedJob("2", "s"));
        var r1 = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!;
        var r2 = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(3))).Value!;
        await c.BeginAsync(r1.Lease.Handle);
        await c.BeginAsync(r2.Lease.Handle);
        var pre = await f.ReadCommittedSnapshot();

        await using var hold = await WriteHold.AcquireAsync(f, c);
        var renew = c.RenewAsync(r1.Lease.Handle);
        var finish = c.FinishAsync(r2.Lease.Handle, AttemptOutcome.Succeeded);
        await Task.Delay(300);
        Assert.False(renew.IsCompleted || finish.IsCompleted, "Renew/Finish did not wait for the lock");
        var expiry = new[] { r1.Lease.ExpiresAt, r2.Lease.ExpiresAt }.Max() + TimeSpan.FromMilliseconds(100);
        while (await WriteHold.NowAsync(f) <= expiry)
        {
            await Task.Delay(100);
        }

        var releasedAt = await WriteHold.NowAsync(f);
        await hold.ReleaseAsync();
        var renewed = await WithTimeout(renew);
        var finished = await WithTimeout(finish);
        Assert.Equal((StoreStatus.StaleLease, StoreStatus.StaleLease), (renewed.Status, finished.Status));
        var floor = DateTimeOffset.FromUnixTimeMilliseconds(releasedAt.ToUnixTimeMilliseconds());
        Assert.True(renewed.DecidedAt >= floor && finished.DecidedAt >= floor,
            $"decisions {renewed.DecidedAt:O}/{finished.DecidedAt:O} precede the blocker release {releasedAt:O}");
        Assert.True(Same(pre.Model, (await f.ReadCommittedSnapshot()).Model, HoldJobs(f)), "jobs, permits or attempts changed");
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "OWN-05")]
    public async Task Missing_or_mismatched_permits_fault_renew_without_changes(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("own05"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PairJob("p", "r-a", "r-b"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        Assert.Equal(2, (await Model(f)).Permits.Count(p => p.AttemptId == lease.AttemptId));

        // (a) One permit row deleted.
        var restore = await TakePermitAsync(f, lease.AttemptId);
        var pre = await f.ReadCommittedSnapshot();
        var a = await c.RenewAsync(lease);
        Assert.Equal((StoreStatus.InvariantFault, "permit_set_mismatch"), (a.Status, a.Error!.Code));
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));

        // (b) Restored, with one permit's expiry different from the job's.
        await restore();
        var pre2 = await f.ReadCommittedSnapshot();
        var b = await c.RenewAsync(lease);
        Assert.Equal((StoreStatus.InvariantFault, "permit_set_mismatch"), (b.Status, b.Error!.Code));
        Assert.Empty(pre2.DiffTables(await f.ReadCommittedSnapshot()));
    }

    // ------------------------------------------------------------------ CMD-04, CMD-05

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CMD-04")]
    public async Task Deadline_expiring_during_a_lock_wait_is_command_expired_and_replay_reads_first(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd04"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("seed", "r"));
        var d = c.Prepared.Prepare(new ResourceJob("d", "r"));
        var pre = await f.ReadCommittedSnapshot();

        // (a) D waits on the lock past its NotAfter: no decision is made after the deadline.
        await using var hold = await WriteHold.AcquireAsync(f, c);
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((await WriteHold.NowAsync(f) + TimeSpan.FromSeconds(2)).ToUnixTimeMilliseconds());
        var pending = SendAsync(c, d, notAfter: notAfter);
        while (await WriteHold.NowAsync(f) <= notAfter)
        {
            await Task.Delay(100);
        }

        await Task.Delay(200);
        await hold.ReleaseAsync();
        var expired = await WithTimeout(pending);
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        Assert.Equal(StoreEffect.ObservedNoEffect, expired.Effect);
        Assert.True(expired.DecidedAt >= notAfter, $"decision {expired.DecidedAt:O} precedes NotAfter {notAfter:O}");
        Assert.Equal(0, await Receipts(f, d.OperationId.Value));
        Assert.True(Same(pre.Model, (await f.ReadCommittedSnapshot()).Model, HoldJobs(f)), "the expired dispatch changed the store");

        // (c) An applied command replayed after its NotAfter returns the recorded receipt. The first send needs headroom
        // to apply before NotAfter on a loaded host; the replay still comes after NotAfter.
        var e = c.Prepared.Prepare(new PingJob("e"));
        var eNotAfter = NotAfter(TimeSpan.FromSeconds(6));
        var first = await SendAsync(c, e, notAfter: eNotAfter);
        Assert.Equal(StoreStatus.Applied, first.Status);
        while (await WriteHold.NowAsync(f) <= eNotAfter)
        {
            await Task.Delay(100);
        }

        var replay = await SendAsync(c, e, notAfter: eNotAfter);
        Assert.Equal(StoreStatus.Applied, replay.Status);
        Assert.True(replay.Replayed);
        Assert.Equal(first.DecidedAt, replay.DecidedAt);
        Assert.Equal(first.Value!.JobId, replay.Value!.JobId);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CMD-04")]
    public async Task Finish_whose_deadline_expires_during_a_lock_wait_is_expired(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd04b"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        var pre = await f.ReadCommittedSnapshot();

        await using var hold = await WriteHold.AcquireAsync(f, c);
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((await WriteHold.NowAsync(f) + TimeSpan.FromSeconds(1)).ToUnixTimeMilliseconds());
        var finish = c.FinishAsync(lease, AttemptOutcome.Succeeded, notAfter: notAfter);
        while (await WriteHold.NowAsync(f) <= notAfter)
        {
            await Task.Delay(100);
        }

        await Task.Delay(200);
        await hold.ReleaseAsync();
        var reply = await WithTimeout(finish);
        Assert.Equal((StoreStatus.CommandExpired, StoreEffect.ObservedNoEffect), (reply.Status, reply.Effect));
        Assert.True(Same(pre.Model, (await f.ReadCommittedSnapshot()).Model, HoldJobs(f)), "the expired finish changed the store");
        Assert.Equal(0, await Receipts(f, c.History.Last().CommandId));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CMD-05")]
    public async Task Finish_replayed_after_receipt_pruning_is_expired(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd05f"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("j", "r"));
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.BeginAsync(lease);
        var id = Guid.CreateVersion7();
        var notAfter = NotAfter(TimeSpan.FromSeconds(3));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter)).Status);
        while (await WriteHold.NowAsync(f) <= notAfter)
        {
            await Task.Delay(100);
        }

        await PruneReceiptsAsync(f, c);
        Assert.Equal(0, await Receipts(f, id));
        var pre = await f.ReadCommittedSnapshot();
        var replay = await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: notAfter);
        Assert.Equal(StoreStatus.CommandExpired, replay.Status);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(1, (await Model(f)).Jobs.Count(j => j.State == "succeeded"));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CMD-05")]
    public async Task Pruned_receipt_replay_is_expired_and_never_redispatches(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd05"), kind);
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            // Long enough for the first dispatch to apply on a loaded host; the test then waits past NotAfter.
            r.Producer(o => o.CommandLifetime = TimeSpan.FromSeconds(10));
        });
        var p = c.Prepared.Prepare(new PingJob("x"));
        var receipt = await c.Prepared.DispatchAsync(p);
        while (await WriteHold.NowAsync(f) <= p.NotAfter)
        {
            await Task.Delay(100);
        }

        await PruneReceiptsAsync(f, c);
        Assert.Equal(0, await Receipts(f, p.OperationId.Value));
        var pre = await f.ReadCommittedSnapshot();

        var replay = await SendAsync(c, p);
        Assert.Equal(StoreStatus.CommandExpired, replay.Status);
        var thrown = await Assert.ThrowsAsync<CommandExpiredException>(() => c.Prepared.DispatchAsync(p));
        Assert.Equal(p.OperationId, thrown.OperationId);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(1, await Jobs(f));

        // The cleanup simulates the retention passing; the arbiter's classification is a function of store time minus NotAfter,
        // so the same simulated offset is applied to the bound it is given.
        var simulatedBound = p.NotAfter - TimeSpan.FromHours(24) - TimeSpan.FromSeconds(1);
        var resolution = await c.Driver.ResolveOperationAsync(c.Application, p.OperationId.Value, simulatedBound, CancellationToken.None);
        Assert.IsType<ReceiptResolution.ExpiredUnknown>(resolution);
        Assert.NotEqual(Guid.Empty, receipt.JobId.Value);
    }

    // ------------------------------------------------------------------ RENEWMANY-01, RENEWMANY-02

    static DateTimeOffset? Expiry(StoreSnapshot m, Guid job) => m.Job(job).LeaseExpiresAt;

    static List<DateTimeOffset> PermitExpiries(StoreSnapshot m, Guid job) =>
        [.. m.Permits.Where(p => p.JobId == job).OrderBy(p => p.ScopeId).Select(p => p.ExpiresAt)];

    static async Task<List<LeaseHandle>> ReserveResourceJobsAsync(TestClient c, int n, string prefix, TimeSpan? lease = null)
    {
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < n; i++)
        {
            await c.Dispatcher.DispatchAsync(new ResourceJob(prefix + i, prefix + "-r" + i));
        }

        for (var i = 0; i < n; i++)
        {
            var reserved = await c.ReserveAsync(lease: lease);
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            leases.Add(reserved.Value!.Lease.Handle);
        }

        return leases;
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RENEWMANY-01")]
    public async Task Each_lease_is_fenced_by_its_own_row(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rm01"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await ReserveResourceJobsAsync(c, 4, "f");
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(leases[1])).Reply.Status);

        // Wrong token, wrong worker, wrong generation, unknown job: each refused exactly as Renew/1 refuses it.
        var wrongToken = leases[2] with { OwnershipToken = Guid.NewGuid() };
        var wrongGeneration = leases[3] with { Generation = leases[3].Generation + 1 };
        var wrongWorker = leases[0] with { WorkerInstanceId = Guid.NewGuid() };
        var unknown = leases[0] with { JobId = Guid.NewGuid() };
        var model = await Model(f);
        var before = leases.ToDictionary(l => l.JobId, l => (Job: Expiry(model, l.JobId), Permits: PermitExpiries(model, l.JobId)));

        var reply = await c.RenewManyAsync([leases[1], wrongToken, wrongGeneration, unknown], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal(4, o.Count);
        Assert.Equal((StoreStatus.Applied, (string?)null), (o[0].Status, o[0].Code));
        Assert.Equal((StoreStatus.StaleLease, "ownership_mismatch"), (o[1].Status, o[1].Code));
        Assert.Equal((StoreStatus.StaleLease, "ownership_mismatch"), (o[2].Status, o[2].Code));
        Assert.Equal((StoreStatus.NotFound, "job_not_found"), (o[3].Status, o[3].Code));
        Assert.All(o.Skip(1), x => Assert.Null(x.Value));

        // The renewed lease: the job and its permit carry the new expiry; the refused leases' rows are untouched.
        var renewed = o[0].Value!.Lease.ExpiresAt;
        Assert.True(renewed > reply.DecidedAt + TimeSpan.FromSeconds(59));
        var after = await Model(f);
        var afterExpiry = Expiry(after, leases[1].JobId);
        Assert.NotEqual(before[leases[1].JobId].Job, afterExpiry);
        Assert.All(PermitExpiries(after, leases[1].JobId), p => Assert.Equal(afterExpiry, p));
        foreach (var l in new[] { leases[0], leases[2], leases[3] })
        {
            Assert.Equal(before[l.JobId].Job, Expiry(after, l.JobId));
            Assert.Equal(before[l.JobId].Permits, PermitExpiries(after, l.JobId));
        }

        // A wrong worker instance on an otherwise valid lease is also a per-lease refusal.
        var worker = await c.RenewManyAsync([wrongWorker, leases[2]], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, worker.Status);
        Assert.Equal(StoreStatus.StaleLease, worker.Value!.Outcomes[0].Status);
        Assert.Equal(StoreStatus.Applied, worker.Value.Outcomes[1].Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.RenewManyAsync([leases[0], leases[0]])).Status);

        Assert.Empty(await History(f));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "RENEWMANY-02")]
    public async Task A_refused_lease_never_affects_the_others(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rm02"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var healthy = await ReserveResourceJobsAsync(c, 3, "h");
        var expiring = (await ReserveResourceJobsAsync(c, 1, "x", TimeSpan.FromSeconds(1)))[0];
        var broken = (await ReserveResourceJobsAsync(c, 1, "b"))[0];
        var finished = (await ReserveResourceJobsAsync(c, 1, "d"))[0];
        await c.BeginAsync(healthy[0]);
        await c.BeginAsync(finished);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(finished, AttemptOutcome.Succeeded)).Status);

        // A missing permit (the invariant Renew/1 reports as permit_set_mismatch), a finished attempt and a lapsed
        // lease sit between healthy leases in the same command.
        _ = await TakePermitAsync(f, broken.AttemptId);
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var m0 = await Model(f);
        var brokenBefore = Expiry(m0, broken.JobId);
        var expiringBefore = Expiry(m0, expiring.JobId);

        var reply = await c.RenewManyAsync([healthy[0], broken, healthy[1], finished, expiring, healthy[2]], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal(StoreStatus.Applied, o[0].Status);
        Assert.Equal((StoreStatus.InvariantFault, "permit_set_mismatch"), (o[1].Status, o[1].Code));
        Assert.Equal(StoreStatus.Applied, o[2].Status);
        Assert.Contains(o[3].Status, new[] { StoreStatus.InvalidState, StoreStatus.StaleLease });
        Assert.Equal((StoreStatus.StaleLease, "lease_expired"), (o[4].Status, o[4].Code));
        Assert.Equal(StoreStatus.Applied, o[5].Status);

        var m1 = await Model(f);
        foreach (var (lease, outcome) in new[] { (healthy[0], o[0]), (healthy[1], o[2]), (healthy[2], o[5]) })
        {
            var expiry = Expiry(m1, lease.JobId);
            Assert.All(PermitExpiries(m1, lease.JobId), p => Assert.Equal(expiry, p));
            Assert.True(outcome.Value!.Lease.ExpiresAt > reply.DecidedAt + TimeSpan.FromSeconds(59));
        }

        Assert.Equal(brokenBefore, Expiry(m1, broken.JobId));
        Assert.Equal(expiringBefore, Expiry(m1, expiring.JobId));

        // Every lease refused: Applied with no effect.
        var none = await c.RenewManyAsync([broken, expiring], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, none.Status);
        Assert.DoesNotContain(none.Value!.Outcomes, x => x.Status == StoreStatus.Applied);

        // A whole-command refusal (expired NotAfter) applies nothing to any lease.
        var healthyBefore = Expiry(await Model(f), healthy[1].JobId);
        var expired = await c.RenewManyAsync([healthy[1], healthy[2]], TimeSpan.FromSeconds(60), notAfter: DateTimeOffset.UtcNow.AddSeconds(-1));
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        var m2 = await Model(f);
        Assert.Equal(healthyBefore, Expiry(m2, healthy[1].JobId));
        Assert.Equal(0, m2.Receipts.Count(r => r.Operation == "RenewMany"));

        // The healthy leases still settle normally afterwards.
        foreach (var l in healthy.Skip(1))
        {
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(l)).Reply.Status);
        }

        foreach (var l in healthy)
        {
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(l, AttemptOutcome.Succeeded)).Status);
        }

        Assert.Empty(await History(f));
    }
}
