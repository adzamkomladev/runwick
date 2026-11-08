using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Serialization;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// ADM-01, ADM-02, ADM-03 and ADM-05 (07 §5) as theories over <see cref="StoreKind"/>, ported from the PostgreSQL suite. Two OS processes race
/// for one store. Where the driver has fault points (PostgreSQL, SQLite) one process is held at a barrier inside its Reserve; Redis has none
/// (22 §12 lists only write-prefix points), so there both Reserve calls are started together and the store's own serialization decides.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class AdmissionContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static async Task PeerArmBarrier(PeerClient peer, FaultPoint point, string operation) =>
        await peer.SendAsync("armFault", new JsonObject { ["point"] = point.ToString(), ["action"] = "Barrier", ["operation"] = operation });

    static Task PeerRelease(PeerClient peer, FaultPoint point) =>
        peer.SendAsync("releaseBarrier", new JsonObject { ["point"] = point.ToString() });

    static async Task<long> PeerDeadlocks(PeerClient peer) => (await peer.SendAsync("deadlocks"))["count"]!.GetValue<long>();

    /// <summary>
    /// Two processes reserve at once. With fault points, one holds its Reserve at <paramref name="point"/> after its locks (and, with
    /// <paramref name="localHolds"/>, the local process is the holder). Without them (Redis) the two calls start together and the holder is
    /// whichever the store applied. Returns (holder status, holder lease, contender status, contender lease).
    /// </summary>
    static async Task<(string Holder, LeaseHandle? HolderLease, string Contender, LeaseHandle? ContenderLease)> RaceAsync(
        DriverFixture f, TestClient c, PeerClient peer, bool localHolds, FaultPoint point, int contenderDelayMs = 0)
    {
        if (f.Kind == StoreKind.Redis)
        {
            var local = c.ReserveAsync();
            var remote = peer.SendInBackground("reserve");
            var (l, r) = (await WithTimeout(local), await WithTimeout(remote));
            LeaseHandle? rl = r["lease"] is { } rj ? PeerJson.Lease(rj) : null;
            var rs = r["status"]!.GetValue<string>();
            return l.Status == StoreStatus.Applied
                ? (l.Status.ToString(), l.Value!.Lease.Handle, rs, rl)
                : (rs, rl, l.Status.ToString(), l.Value?.Lease.Handle);
        }

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

    static async Task ReleaseAsync(TestClient c, PeerClient peer, bool viaLocal, LeaseHandle lease)
    {
        if (viaLocal)
        {
            Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(lease)).Status);
        }
        else
        {
            Assert.Equal("Applied", (await peer.SendAsync("release", new JsonObject { ["lease"] = PeerJson.Lease(lease) }))["status"]!.GetValue<string>());
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ADM-01")]
    public async Task Two_processes_claim_one_ready_job_and_unsupported_tuples_are_never_reserved(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm01"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("peer-1");

        for (var i = 0; i < 100; i++)
        {
            var j = await c.Dispatcher.DispatchAsync(new PingJob("j" + i));
            var (holder, holderLease, contender, contenderLease) = await RaceAsync(f, c, peer, localHolds: i % 2 == 0, FaultPoint.AfterLocksBeforeFreshTime);
            Assert.Equal(("Applied", "NoWork"), (holder, contender));
            Assert.Equal(j.JobId.Value, holderLease!.JobId);
            Assert.Null(contenderLease);
            Assert.Single((await Model(f)).AttemptsOf(j.JobId.Value));
        }

        var reserved = await Model(f);
        Assert.Equal(100, reserved.Jobs.Count(j => j.State == "reserved" && j.OwnershipToken is not null));

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

        var untouched = (await Model(f)).Job(u.JobId.Value);
        Assert.Empty((await Model(f)).AttemptsOf(u.JobId.Value));
        Assert.Equal("pending", untouched.State);
        Assert.Equal(u.JobId.Value, (await c.ReserveAsync(contracts: [new JobContract("test.ping", 1, json, 1)])).Value!.Position.JobId);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ADM-02")]
    public async Task Two_jobs_contend_for_the_last_type_permit(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm02"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var peer = await f.OpenPeer("peer-1");
        await c.Dispatcher.DispatchAsync(new CappedJob("j1"));
        await c.Dispatcher.DispatchAsync(new CappedJob("j2"));

        for (var i = 0; i < 100; i++)
        {
            var attemptsBefore = await Attempts(f);
            var localHolds = i % 2 == 0;
            var (holder, holderLease, contender, contenderLease) = await RaceAsync(f, c, peer, localHolds, FaultPoint.AfterLocksBeforeFreshTime, contenderDelayMs: 20);
            Assert.Equal("Applied", holder);
            Assert.Contains(contender, new[] { "CapacityBlocked", "NoWork" });
            Assert.Null(contenderLease);
            Assert.Equal(1, await Permits(f));
            Assert.Equal(attemptsBefore + 1, await Attempts(f));

            // Return the permit for the next iteration; with no fault points (Redis) either process may have won, so release through the one that holds it.
            await ReleaseAsync(c, peer, f.Kind == StoreKind.Redis || localHolds, holderLease!);
            Assert.Equal(0, await Permits(f));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ADM-03")]
    public async Task Opposite_declaration_order_scopes_never_deadlock_and_grants_are_all_or_none(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm03"), kind);
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
            var permits = (await Model(f)).Permits;
            Assert.Equal(2, permits.Count);
            Assert.Single(permits.Select(p => p.JobId).Distinct());
            Assert.Contains(holderLease!.JobId, new[] { x.JobId.Value, y.JobId.Value });

            await ReleaseAsync(c, peer, f.Kind == StoreKind.Redis || i % 2 == 0, holderLease);
        }

        Assert.Equal(before, Deadlocks(c) + await PeerDeadlocks(peer));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ADM-05")]
    public async Task Blocked_lane_cursor_reaches_a_later_type_and_wraps(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm05"), kind);
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
        await MakeEligibleAnHourEarlier(f, early.JobId.Value);
        var tail = await c.ReserveAsync(after: cursor);
        Assert.Equal(StoreStatus.NoWork, tail.Status);
        var wrapped = await c.ReserveAsync(after: null);
        Assert.Equal(StoreStatus.Applied, wrapped.Status);
        Assert.Equal(early.JobId.Value, wrapped.Value!.Position.JobId);

        // No A job was reserved while its scope was full.
        var model = await Model(f);
        var capped = a.Append(held.Position.JobId).ToHashSet();
        Assert.Equal(1, model.Attempts.Count(x => capped.Contains(x.JobId)));
        var permit = model.Permits.Where(p => p.JobId == held.Position.JobId).ToList();
        Assert.Single(permit);
    }

    /// <summary>Setup write through the store's admin path: moves one job's eligibility an hour back (no protocol operation does that).</summary>
    static Task MakeEligibleAnHourEarlier(DriverFixture f, Guid job) => f.Kind switch
    {
        StoreKind.Redis => RedisAdmin.MakeEligibleEarlier(f.Application, job, 3_600_000),
        StoreKind.Sqlite => f.ExecAsync("UPDATE runwick_jobs SET eligible_at = eligible_at - 3600000 WHERE job_id = @j", ("j", job)),
        _ => f.ExecAsync("UPDATE runwick.jobs SET eligible_at = eligible_at - interval '1 hour' WHERE job_id = @j", ("j", job)),
    };
}
