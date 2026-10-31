using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Serialization;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>ADM-01..05 (07 §5), CMD-06 subset (07 §4) and ISO-01 (07 §9): Reserve/1.</summary>
public sealed class AdmissionTests
{
    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static Task<long> Attempts(DriverFixture f, Guid jobId) =>
        f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE job_id = @j", ("j", jobId));

    static Task<long> Permits(DriverFixture f) => f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits");

    static async Task PeerArmBarrier(PeerClient peer, FaultPoint point, string operation) =>
        await peer.SendAsync("armFault", new JsonObject { ["point"] = point.ToString(), ["action"] = "Barrier", ["operation"] = operation });

    static Task PeerRelease(PeerClient peer, FaultPoint point) =>
        peer.SendAsync("releaseBarrier", new JsonObject { ["point"] = point.ToString() });

    static async Task<long> PeerDeadlocks(PeerClient peer) => (await peer.SendAsync("deadlocks"))["count"]!.GetValue<long>();

    /// <summary>
    /// One process holds its Reserve at <paramref name="point"/> (after its locks); the other reserves meanwhile.
    /// Returns (holder reply status, holder lease or null, contender status, contender lease or null).
    /// </summary>
    static async Task<(string Holder, LeaseHandle? HolderLease, string Contender, LeaseHandle? ContenderLease)> RaceAsync(
        DriverFixture f, TestClient c, PeerClient peer, bool localHolds, FaultPoint point, int contenderDelayMs = 0)
    {
        if (localHolds)
        {
            var h = f.ArmFault(point, FaultSpec.Barrier, FaultTarget.Op(Operations.Reserve));
            var held = c.ReserveAsync();
            await DriverFixture.WaitAtBarrier(h, Wait);
            var contender = peer.SendInBackground("reserve");
            await Task.Delay(contenderDelayMs);
            DriverFixture.ReleaseBarrier(h);
            var a = await WithTimeout(held);
            var b = await WithTimeout(contender);
            return (a.Status.ToString(), a.Value?.Lease.Handle, b["status"]!.GetValue<string>(), b["lease"] is { } l ? PeerJson.Lease(l) : null);
        }
        else
        {
            await PeerArmBarrier(peer, point, Operations.Reserve);
            var held = peer.SendInBackground("reserve");
            await peer.WaitForEventAsync("atBarrier", timeout: Wait);
            var contender = c.ReserveAsync();
            await Task.Delay(contenderDelayMs);
            await PeerRelease(peer, point);
            var a = await WithTimeout(held);
            var b = await WithTimeout(contender);
            return (a["status"]!.GetValue<string>(), a["lease"] is { } l ? PeerJson.Lease(l) : null, b.Status.ToString(), b.Value?.Lease.Handle);
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "ADM-01")]
    public async Task Two_processes_claim_one_ready_job_and_unsupported_tuples_are_never_reserved()
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("peer-1");

        for (var i = 0; i < 100; i++)
        {
            var j = await c.Dispatcher.DispatchAsync(new PingJob("j" + i));
            var (holder, holderLease, contender, contenderLease) = await RaceAsync(f, c, peer, localHolds: i % 2 == 0, FaultPoint.AfterLocksBeforeFreshTime);
            Assert.Equal(("Applied", "NoWork"), (holder, contender));
            Assert.Equal(j.JobId.Value, holderLease!.JobId);
            Assert.Null(contenderLease);
            Assert.Equal(1, await Attempts(f, j.JobId.Value));
        }

        Assert.Equal(100, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'reserved' AND ownership_token IS NOT NULL"));

        // Workers advertising a different payload version or serializer-profile version never see the job.
        var u = await c.Dispatcher.DispatchAsync(new PingJob("u"));
        var json = RunwickJsonProfile.ProfileId;
        foreach (var contract in new[] { new JobContract("test.ping", 2, json, 1), new JobContract("test.ping", 1, json, 2) })
        {
            for (var k = 0; k < 3; k++)
            {
                Assert.Equal(StoreStatus.NoWork, (await c.ReserveAsync(contracts: [contract])).Status);
            }
        }

        Assert.Equal(0, await Attempts(f, u.JobId.Value));
        Assert.Equal(1, await f.CountAsync(
            "SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'pending' AND reservation_count = 0 AND execution_count = 0", ("j", u.JobId.Value)));
        Assert.Equal(u.JobId.Value, (await c.ReserveAsync(contracts: [new JobContract("test.ping", 1, json, 1)])).Value!.Position.JobId);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "ADM-02")]
    public async Task Two_jobs_contend_for_the_last_type_permit()
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("peer-1");
        await c.Dispatcher.DispatchAsync(new CappedJob("j1"));
        await c.Dispatcher.DispatchAsync(new CappedJob("j2"));

        for (var i = 0; i < 100; i++)
        {
            var attemptsBefore = await f.CountAsync("SELECT count(*) FROM runwick.job_attempts");
            var localHolds = i % 2 == 0;
            var (holder, holderLease, contender, contenderLease) = await RaceAsync(f, c, peer, localHolds, FaultPoint.AfterLocksBeforeFreshTime, contenderDelayMs: 20);
            Assert.Equal("Applied", holder);
            Assert.Contains(contender, new[] { "CapacityBlocked", "NoWork" });
            Assert.Null(contenderLease);
            Assert.Equal(1, await Permits(f));
            Assert.Equal(attemptsBefore + 1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));

            // Return the permit for the next iteration.
            if (localHolds)
            {
                Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(holderLease!)).Status);
            }
            else
            {
                Assert.Equal("Applied", (await peer.SendAsync("release", new JsonObject { ["lease"] = PeerJson.Lease(holderLease!) }))["status"]!.GetValue<string>());
            }

            Assert.Equal(0, await Permits(f));
        }
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "ADM-03")]
    public async Task Opposite_declaration_order_scopes_never_deadlock_and_grants_are_all_or_none()
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("peer-1");
        var x = await c.Dispatcher.DispatchAsync(new PairJob("x", "r-a", "r-b"));
        var y = await c.Dispatcher.DispatchAsync(new PairJob("y", "r-b", "r-a"));
        var before = Deadlocks(c) + await PeerDeadlocks(peer);

        for (var i = 0; i < 200; i++)
        {
            // Barrier after the holder's first mutation (it holds both scope rows), in both process orders.
            var (holder, holderLease, contender, contenderLease) =
                await RaceAsync(f, c, peer, localHolds: i % 2 == 0, FaultPoint.AfterMutation, contenderDelayMs: 10);
            Assert.Equal("Applied", holder);
            Assert.Contains(contender, new[] { "CapacityBlocked", "NoWork" });
            Assert.Null(contenderLease);
            Assert.Equal(2, await Permits(f));
            Assert.Equal(1, await f.CountAsync("SELECT count(DISTINCT job_id) FROM runwick.concurrency_permits"));
            Assert.Contains(holderLease!.JobId, new[] { x.JobId.Value, y.JobId.Value });

            if (i % 2 == 0)
            {
                Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(holderLease)).Status);
            }
            else
            {
                Assert.Equal("Applied", (await peer.SendAsync("release", new JsonObject { ["lease"] = PeerJson.Lease(holderLease) }))["status"]!.GetValue<string>());
            }
        }

        Assert.Equal(before, Deadlocks(c) + await PeerDeadlocks(peer));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "ADM-04")]
    public async Task Queue_pause_races_claim()
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var j1 = await c.Dispatcher.DispatchAsync(new PingJob("1"));
        await c.Dispatcher.DispatchAsync(new PingJob("2"));

        // (a) Reserve holds the queue row FOR SHARE; the pause waits for it; R commits first.
        var h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.Reserve));
        var r = c.ReserveAsync();
        await DriverFixture.WaitAtBarrier(h, Wait);
        var pause = f.SetQueuePaused(c.Application, "default", true);
        await Task.Delay(300);
        Assert.False(pause.IsCompleted, "the pause did not wait for the in-flight Reserve");
        DriverFixture.ReleaseBarrier(h);
        var reserved = await WithTimeout(r);
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        Assert.Equal(j1.JobId.Value, reserved.Value!.Position.JobId);
        await WithTimeout(pause);
        Assert.Equal(StoreStatus.QueuePaused, (await c.ReserveAsync()).Status);

        // (b) The unpause → pause cycle: a Reserve arriving while the pause commits waits and sees it.
        await f.SetQueuePaused(c.Application, "default", false);
        var hp = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.SetQueueState));
        var pause2 = f.SetQueuePaused(c.Application, "default", true);
        await DriverFixture.WaitAtBarrier(hp, Wait);
        var r3 = c.ReserveAsync();
        await Task.Delay(300);
        Assert.False(r3.IsCompleted, "Reserve did not wait for the pause holding the queue row");
        DriverFixture.ReleaseBarrier(hp);
        await WithTimeout(pause2);
        var blocked = await WithTimeout(r3);
        Assert.Equal((StoreStatus.QueuePaused, StoreEffect.ObservedNoEffect), (blocked.Status, blocked.Effect));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND state = 'reserved'", ("j", j1.JobId.Value)));

        // (c) Pause never blocks dispatch.
        Assert.Equal(DispatchDisposition.Created, (await c.Dispatcher.DispatchAsync(new PingJob("3"))).Disposition);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "ADM-05")]
    public async Task Blocked_lane_cursor_reaches_a_later_type_and_wraps()
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm05"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));

        // A's type scope (capacity 1) is occupied; 20 more A jobs are eligible before one B job.
        await c.Dispatcher.DispatchAsync(new CappedJob("held"));
        var held = (await c.ReserveAsync()).Value!;
        var a = new List<Guid>();
        for (var i = 0; i < 20; i++)
        {
            a.Add((await c.Dispatcher.DispatchAsync(new CappedJob("a" + i))).JobId.Value);
        }

        var b = await c.Dispatcher.DispatchAsync(new PingJob("b"));

        // Lane visits as the engine runs them (05 §4): Reserve with After = cursor; a visit ends after 8 blocked
        // candidates; each Reserve inspects exactly one candidate in its own transaction.
        CandidateCursor? cursor = null;
        var visits = 0;
        var seen = new List<CandidateCursor>();
        Guid? reservedB = null;
        while (reservedB is null && visits < 10)
        {
            visits++;
            for (var blocked = 0; blocked < 8;)
            {
                var reply = await c.ReserveAsync(after: cursor);
                if (reply.Status == StoreStatus.Applied)
                {
                    reservedB = reply.Value!.Position.JobId;
                    cursor = reply.Value.Cursor;
                    break;
                }

                Assert.Equal(StoreStatus.CapacityBlocked, reply.Status);
                cursor = reply.Error!.ResumeAfter!;
                seen.Add(cursor);
                blocked++;
            }
        }

        Assert.Equal(b.JobId.Value, reservedB);
        Assert.True(visits <= 4, $"B reserved after {visits} lane visits");
        Assert.Equal(a, seen.Select(s => s.JobId));
        Assert.True(seen.Zip(seen.Skip(1)).All(p => (p.First.EligibleAt, Runwick.Internal.UuidOrder.Instance.Compare(p.First.JobId, p.Second.JobId)) switch
        {
            var (e, _) when e < p.Second.EligibleAt => true,
            var (e, cmp) => e == p.Second.EligibleAt && cmp < 0,
        }));

        // A B job eligible before the cursor is found once the cursor wraps.
        var early = await c.Dispatcher.DispatchAsync(new PingJob("early"));
        await f.ExecAsync("UPDATE runwick.jobs SET eligible_at = eligible_at - interval '1 hour' WHERE job_id = @j", ("j", early.JobId.Value));
        var tail = await c.ReserveAsync(after: cursor);
        Assert.Equal(StoreStatus.NoWork, tail.Status);
        var wrapped = await c.ReserveAsync(after: null);
        Assert.Equal(StoreStatus.Applied, wrapped.Status);
        Assert.Equal(early.JobId.Value, wrapped.Value!.Position.JobId);

        // No A job was reserved while its scope was full.
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts a JOIN runwick.jobs j USING (application, job_id) WHERE j.job_name = 'test.capped'"));
        Assert.Equal(held.Position.JobId, Guid.Parse(Assert.Single(await TextAsync(f,
            "SELECT job_id::text FROM runwick.concurrency_permits p JOIN runwick.concurrency_scopes s USING (application, scope_id) WHERE s.scope_kind = 'job-type'"))));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "CMD-06")]
    public async Task Pure_observations_leave_no_receipt_and_the_same_command_later_applies()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd06"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(50)).ToUnixTimeMilliseconds());
        Task<StoreReply<ReservedJob>> R() => c.ReserveAsync(commandId: id, notAfter: notAfter);

        async Task AssertPure(StoreStatus expected)
        {
            var reply = await R();
            Assert.Equal((expected, StoreEffect.ObservedNoEffect, false), (reply.Status, reply.Effect, reply.Replayed));
            Assert.Equal(0, await Receipts(f, id));
        }

        await AssertPure(StoreStatus.NoWork);

        await c.Dispatcher.DispatchAsync(new CappedJob("first"));
        var occupying = (await c.ReserveAsync()).Value!.Lease.Handle;
        await c.Dispatcher.DispatchAsync(new CappedJob("second"));
        await f.SetQueuePaused(c.Application, "default", true);
        await AssertPure(StoreStatus.QueuePaused);
        await f.SetQueuePaused(c.Application, "default", false);
        await AssertPure(StoreStatus.CapacityBlocked);

        Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(occupying)).Status);
        var applied = await R();
        Assert.Equal((StoreStatus.Applied, StoreEffect.Applied, false), (applied.Status, applied.Effect, applied.Replayed));
        Assert.Equal(1, await Receipts(f, id));
        Assert.True((await R()).Replayed);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-007")]
    [Trait("Case", "ISO-01")]
    public async Task Application_namespaces_are_isolated()
    {
        await using var f = await DriverFixture.StartAsync(Ns("iso01"));
        var a = await f.OpenClient(configure: r => TestJobs.Register(r));
        var appB = f.Application + "_b";
        var b = await f.OpenClient(configure: r => TestJobs.Register(r), application: appB);

        // Identical intents and keys in both applications.
        var ka = await a.Dispatcher.DispatchAsync(new KeyedJob("k", "same-key"));
        var kb = await b.Dispatcher.DispatchAsync(new KeyedJob("k", "same-key"));
        Assert.Equal((DispatchDisposition.Created, DispatchDisposition.Created), (ka.Disposition, kb.Disposition));
        Assert.NotEqual(ka.JobId, kb.JobId);
        var ca = await a.Dispatcher.DispatchAsync(new CappedJob("c"));
        var cb = await b.Dispatcher.DispatchAsync(new CappedJob("c"));

        // Fill a's type scope; b is neither blocked by a's permit nor shown a's jobs.
        var seenA = new List<Guid>();
        StoreReply<ReservedJob> next;
        while ((next = await a.ReserveAsync()).Status == StoreStatus.Applied)
        {
            seenA.Add(next.Value!.Position.JobId);
        }

        Assert.Contains(ca.JobId.Value, seenA);
        var seenB = new List<Guid>();
        while ((next = await b.ReserveAsync()).Status == StoreStatus.Applied)
        {
            seenB.Add(next.Value!.Position.JobId);
        }

        Assert.Equal(new[] { kb.JobId.Value, cb.JobId.Value }.Order(), seenB.Order());
        Assert.Empty(seenA.Intersect(seenB));
        Assert.Equal(1, await f.CountAsync(
            "SELECT count(*) FROM runwick.concurrency_permits p JOIN runwick.concurrency_scopes s USING (application, scope_id) WHERE s.scope_kind = 'job-type' AND p.application = @a", ("a", appB)));

        // a's command ID replayed in b applies independently; conflicts never cross namespaces.
        var pa = a.Prepared.Prepare(new PingJob("cmd"));
        await a.Prepared.DispatchAsync(pa);
        var inB = await SendAsync(b, b.Prepared.Prepare(new PingJob("other")), commandId: pa.OperationId.Value);
        Assert.Equal((StoreStatus.Applied, false, DispatchDisposition.Created), (inB.Status, inB.Replayed, inB.Value!.Disposition));
    }
}
