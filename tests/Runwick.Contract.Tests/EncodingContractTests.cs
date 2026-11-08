using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Internal;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Serialization;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// ENC-01 (07 §3; 20 SQL-09; 22 REDIS-08/11) as theories over <see cref="StoreKind"/>: instants round the same way (up for a not-before, down
/// for a lease expiry), job IDs order as <see cref="UuidOrder"/> where the store orders them, and a fence at the top of its range is exact and
/// an overflow commits nothing. The PostgreSQL case that freezes the server clock with libfaketime stays in the PostgreSQL project (it needs Docker
/// and a clock the other stores do not have).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class EncodingContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ENC-01")]
    public async Task Sub_millisecond_instants_round_up_for_not_before_and_down_for_a_lease_expiry(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("enc01a"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var ms = TimeSpan.FromMilliseconds(1);

        // NotBefore rounds up to the millisecond.
        var whole = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) + TimeSpan.FromSeconds(60);
        var later = await c.Dispatcher.DispatchAsync(new PingJob("later"), new DispatchOptions { NotBefore = whole + TimeSpan.FromTicks(4) });
        var exact = await c.Dispatcher.DispatchAsync(new PingJob("exact"), new DispatchOptions { NotBefore = whole });
        var model = await Model(f);
        Assert.Equal(whole + ms, model.Job(later.JobId.Value).EligibleAt);
        Assert.Equal(whole, model.Job(exact.JobId.Value).EligibleAt);

        // A lease expiry landing on a sub-millisecond instant rounds down: the decision time is whole milliseconds, so adding a lease with a
        // fractional millisecond leaves exactly decision time + the whole part.
        var now = await c.Dispatcher.DispatchAsync(new PingJob("now"));
        var reply = await c.ReserveAsync(lease: TimeSpan.FromSeconds(90) + TimeSpan.FromTicks(6_000));
        var reserved = reply.Value!;
        Assert.Equal(now.JobId.Value, reserved.Position.JobId);
        Assert.Equal(reply.DecidedAt + TimeSpan.FromSeconds(90), reserved.Lease.ExpiresAt);
        Assert.Equal(reserved.Lease.ExpiresAt, (await Model(f)).Job(now.JobId.Value).LeaseExpiresAt);

        // A command deadline that has passed is expired whatever its sub-millisecond part: NotAfter 1 ms ago plus half a millisecond floors to 1 ms ago.
        var past = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1_000) + TimeSpan.FromTicks(5_000);
        Assert.Equal(StoreStatus.CommandExpired, (await c.BeginAsync(reserved.Lease.Handle, notAfter: past)).Reply.Status);
    }

    /// <summary>A dispatch command that proposes the given job ID for the prepared dispatch (other fields as Prepare made them).</summary>
    static StoreCommand<DispatchRequest> CommandFor(PreparedDispatch p, Guid jobId)
    {
        var intent = p.Intent with { ProposedJobId = jobId };
        var id = Guid.CreateVersion7();
        var digests = Fingerprints.ForDispatch(p.StoreId, p.Application, id, p.NotAfterMs, intent);
        return new StoreCommand<DispatchRequest>(p.Application, id, p.NotAfter, new DispatchRequest(new DispatchItem(intent, p.Scopes, digests), p.StoreId, digests.Command));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ENC-01")]
    public async Task Uuid_order_of_the_driver_comparer_matches_the_stores_candidate_order(StoreKind kind)
    {
        // v7, v4 and byte patterns on which the little-endian Guid.ToByteArray() order and big-endian order differ.
        var random = new Random(20261031);
        var ids = new List<Guid>();
        for (var i = 0; i < 100; i++)
        {
            ids.Add(Guid.CreateVersion7(DateTimeOffset.UnixEpoch + TimeSpan.FromMilliseconds(random.NextInt64(0, 1L << 47))));
            ids.Add(Guid.NewGuid());
        }

        for (var i = 0; i < 100; i++)
        {
            var bytes = new byte[16];
            bytes[random.Next(16)] = (byte)random.Next(256);
            bytes[random.Next(16)] = (byte)random.Next(256);
            ids.Add(new Guid(bytes, bigEndian: true));
        }

        ids = [.. ids.Distinct()];
        var expected = ids.OrderBy(g => g, UuidOrder.Instance).ToList();
        Assert.NotEqual(expected, ids.OrderBy(g => g.ToByteArray(), Comparer<byte[]>.Create((x, y) => x.AsSpan().SequenceCompareTo(y))).ToList());

        // Every job has the same eligible_at, so the store's candidate order is decided by the job ID alone.
        await using var f = await DriverFixture.StartAsync(Ns("enc01b"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var at = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 4_000);
        var p = c.Prepared.Prepare(new PingJob("x"), new DispatchOptions { NotBefore = at });
        foreach (var id in ids.OrderBy(_ => random.Next()))
        {
            Assert.Equal(StoreStatus.Applied, (await c.Driver.DispatchAsync(CommandFor(p, id), CancellationToken.None)).Status);
        }

        while (DateTimeOffset.UtcNow <= at + TimeSpan.FromMilliseconds(200))
        {
            await Task.Delay(100);
        }

        var order = new List<Guid>();
        StoreReply<ReservedJob> next;
        while ((next = await c.ReserveAsync()).Status == StoreStatus.Applied)
        {
            order.Add(next.Value!.Position.JobId);
        }

        Assert.True(expected.SequenceEqual(order), $"count {order.Count}/{expected.Count}; missing [{string.Join(",", expected.Except(order).Take(5))}]; first difference at {expected.Zip(order).TakeWhile(p => p.First == p.Second).Count()}");
    }

    /// <summary>Setup: puts the resource scopes' fencing counter at <paramref name="value"/> with fencing enabled (the SQL stores update the scope row).</summary>
    static Task SetFenceAsync(DriverFixture f, long value) => f.Kind switch
    {
        StoreKind.Redis => RedisAdmin.SetFence(f.Application, value),
        StoreKind.Sqlite => f.ExecAsync("UPDATE runwick_concurrency_scopes SET fencing_enabled = 1, fencing_counter = @v WHERE scope_kind = 'resource'", ("v", value)),
        _ => f.ExecAsync("UPDATE runwick.concurrency_scopes SET fencing_enabled = true, fencing_counter = @v WHERE scope_kind = 'resource'", ("v", value)),
    };

    static async Task<long> FenceAsync(DriverFixture f) => f.Kind == StoreKind.Redis
        ? await RedisAdmin.Fence(f.Application)
        : await f.CountAsync(Q(f, "SELECT fencing_counter FROM runwick.concurrency_scopes WHERE scope_kind = 'resource'"));

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ENC-01")]
    public async Task Fence_at_the_top_of_its_range_is_exact_and_overflow_commits_nothing(StoreKind kind)
    {
        // A fenced resource scope whose counter starts at long.MaxValue - 1.
        await using var f = await DriverFixture.StartAsync(Ns("enc01c"), kind);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("x", "r1"))).JobId.Value;
        await SetFenceAsync(f, long.MaxValue - 1);

        var first = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, first.Status);
        Assert.Equal(long.MaxValue, Assert.Single(first.Value!.Fences).Value);
        Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(first.Value.Lease.Handle)).Status);
        var before = await f.ReadCommittedSnapshot();

        // The second increment overflows: an explicit error (PostgreSQL raises it from the database, SQLite and Redis answer InvariantFault),
        // and nothing is committed.
        var thrown = await Record.ExceptionAsync(async () =>
        {
            var reply = await c.ReserveAsync();
            Assert.NotEqual(StoreStatus.Applied, reply.Status);
            Assert.Equal(StoreStatus.InvariantFault, reply.Status);
            throw new InvalidOperationException($"{reply.Status}: {reply.Error?.Code}");
        });
        Assert.NotNull(thrown);
        Assert.IsNotType<Xunit.Sdk.EqualException>(thrown);
        Assert.Matches("overflow|out of range|fence|limit", thrown.ToString());
        Assert.Equal(long.MaxValue, await FenceAsync(f));
        var after = await f.ReadCommittedSnapshot();
        Assert.Equal("pending", after.Model.Jobs.Single(j => j.JobId == job).State);
        Assert.Single(after.Model.Attempts);
        Assert.Equal(before.Model.Jobs.Single().Revision, after.Model.Jobs.Single().Revision);
        Assert.Empty(before.DiffTables(after));
    }
}
