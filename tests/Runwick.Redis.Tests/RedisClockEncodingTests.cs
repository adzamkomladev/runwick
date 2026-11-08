using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>REDIS-09 (server clock) and the remaining parts of REDIS-11 (encodings) of 22 §12.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed partial class RedisClockEncodingTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    /// <summary>A producer or worker whose clock is wrong by <paramref name="offset"/>; everything else is the system clock.</summary>
    sealed class SkewedTime : TimeProvider
    {
        /// <summary>Zero while the client registers (a skewed handshake is refused by NotAfter); set afterwards.</summary>
        public TimeSpan Offset { get; set; }

        public override DateTimeOffset GetUtcNow() => base.GetUtcNow() + Offset;
    }

    static async Task<DateTimeOffset> ServerNow()
    {
        var t = (RedisResult[])(await (await RedisServer.AdminAsync()).GetDatabase().ExecuteAsync("TIME"))!;
        return DateTimeOffset.FromUnixTimeMilliseconds(long.Parse((string)t[0]!, CultureInfo.InvariantCulture) * 1000 + long.Parse((string)t[1]!, CultureInfo.InvariantCulture) / 1000);
    }

    static async Task<TestClient> OpenAsync(string name, TimeProvider? time = null) =>
        await TestClient.CreateAsync(RedisServer.Prefix + RedisServer.Configuration, name + "_" + Guid.NewGuid().ToString("N")[..8],
            new HarnessFaultInjector(), r => TestJobs.Register(r), time);

    static StoreCommand<DispatchBatchRequest> Many(TestClient c, IReadOnlyList<Guid>? ids, int count, DateTimeOffset notAfter)
    {
        var preparer = c.Services.GetRequiredService<DispatchPreparer>();
        var batch = preparer.PrepareMany(c.Runtime.StoreId, [.. Enumerable.Range(0, count).Select(i => new BatchDispatchItem(new PingJob("e" + i)))], null);
        var items = batch.Items.Select((intent, i) => new DispatchItem(ids is null ? intent : intent with { ProposedJobId = ids[i] }, batch.Scopes[i], batch.ItemDigests[i])
        { Rates = preparer.RatesFor(intent) }).ToList();
        return new StoreCommand<DispatchBatchRequest>(c.Application, Guid.CreateVersion7(), notAfter,
            new DispatchBatchRequest(items, false, c.Runtime.StoreId, batch.CommandDigest));
    }

    // ------------------------------------------------------------------ REDIS-09

    [Theory]
    [InlineData(3600)]
    [InlineData(-3600)]
    [Trait("Case", "REDIS-09")]
    public async Task Decision_times_leases_and_eligibility_follow_Redis_TIME_when_the_client_clock_is_an_hour_off(int skewSeconds)
    {
        var clock = new SkewedTime();
        await using var c = await OpenAsync("redis09", clock);
        clock.Offset = TimeSpan.FromSeconds(skewSeconds);
        try
        {
            Assert.True(Math.Abs((c.Services.GetRequiredService<TimeProvider>().GetUtcNow() - DateTimeOffset.UtcNow).TotalSeconds - skewSeconds) < 5);
            void Within(DateTimeOffset decided, DateTimeOffset low, DateTimeOffset high) =>
                Assert.True(decided >= low.AddSeconds(-1) && decided <= high.AddSeconds(1), $"decided {decided:O} outside server window {low:O}..{high:O}");

            var t0 = await ServerNow();
            var dispatched = await c.Driver.DispatchManyAsync(Many(c, null, 2, t0.AddSeconds(30)), Ct);
            Assert.Equal(StoreStatus.Applied, dispatched.Status);
            Within(dispatched.DecidedAt, t0, await ServerNow());

            var t1 = await ServerNow();
            var reserved = await c.ReserveAsync(lease: TimeSpan.FromSeconds(30), notAfter: t1.AddSeconds(30));
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            Within(reserved.DecidedAt, t1, await ServerNow());
            Assert.Equal(TimeSpan.FromSeconds(30), reserved.Value!.Lease.ExpiresAt - reserved.DecidedAt);
            var lease = reserved.Value.Lease.Handle;

            var t2 = await ServerNow();
            var begun = await c.BeginAsync(lease, notAfter: t2.AddSeconds(30));
            Within(begun.Reply.DecidedAt, t2, await ServerNow());

            var t3 = await ServerNow();
            var renewed = await c.RenewAsync(lease, TimeSpan.FromSeconds(40), notAfter: t3.AddSeconds(30));
            Assert.Equal(StoreStatus.Applied, renewed.Status);
            Within(renewed.DecidedAt, t3, await ServerNow());
            Assert.Equal(TimeSpan.FromSeconds(40), renewed.Value!.Lease.ExpiresAt - renewed.DecidedAt);

            // Eligibility: a retry delayed by 0.8 s is not reservable for 0.8 s by server time, whatever the client thinks.
            var t4 = await ServerNow();
            var finished = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(800), notAfter: t4.AddSeconds(30));
            Assert.Equal(StoreStatus.Applied, finished.Status);
            var wait = finished.Value!.EligibleAt!.Value;
            Within(wait, t4.AddMilliseconds(800), (await ServerNow()).AddMilliseconds(800));
            var other = await c.ReserveAsync(notAfter: (await ServerNow()).AddSeconds(30));
            Assert.Equal(StoreStatus.Applied, other.Status); // the second dispatched job
            Assert.NotEqual(lease.JobId, other.Value!.Position.JobId);
            var early = await c.ReserveAsync(notAfter: (await ServerNow()).AddSeconds(30));
            Assert.Equal(StoreStatus.NoWork, early.Status);
            await Task.Delay(wait - await ServerNow() + TimeSpan.FromMilliseconds(300), Ct);
            var later = await c.ReserveAsync(notAfter: (await ServerNow()).AddSeconds(30));
            Assert.Equal((StoreStatus.Applied, lease.JobId), (later.Status, later.Value!.Position.JobId));
        }
        finally
        {
            await RedisServer.DropApplicationAsync(c.Application);
        }
    }

    [Fact]
    [Trait("Case", "REDIS-09")]
    public async Task An_intent_recovered_after_its_NotAfter_publishes_the_original_decision_time_ids_and_reply()
    {
        var clock = new SkewedTime();
        await using var c = await OpenAsync("redis09r", clock);
        clock.Offset = TimeSpan.FromHours(1);
        try
        {
            Assert.Equal(StoreStatus.Applied, (await c.Driver.DispatchManyAsync(Many(c, null, 1, (await ServerNow()).AddSeconds(30)), Ct)).Status);
            var store = Store(c);
            var id = Guid.CreateVersion7();
            var accepted = await ServerNow();
            var notAfter = accepted.AddSeconds(3);
            store.Runner.Client.FailAfter = 1; // the intent is stored (decision time, IDs, reply) and nothing else
            try
            {
                await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.ReserveAsync(lease: TimeSpan.FromSeconds(30), commandId: id, notAfter: notAfter));
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            var settled = await ServerNow();
            await Task.Delay(notAfter - settled + TimeSpan.FromMilliseconds(700), Ct);
            Assert.True(await ServerNow() > notAfter);

            // The resend is past NotAfter, but the stored decision is published and returned, not a new one.
            // The caller's budget is spent, so its own call may answer Recovering; the background class (1 s retries, 22 §4.2) publishes
            // the intent regardless of the deadline, and the next resend reads the stored decision.
            StoreReply<ReservedJob>? again = null;
            for (var i = 0; i < 20 && again is null; i++)
            {
                try
                {
                    again = await c.ReserveAsync(lease: TimeSpan.FromSeconds(30), commandId: id, notAfter: notAfter);
                }
                catch (StoreOutcomeUnknownException ex) when (ex.Code == "store_recovering")
                {
                    await Task.Delay(500, Ct);
                }
            }

            Assert.NotNull(again);
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.True(again.DecidedAt >= accepted.AddSeconds(-1) && again.DecidedAt <= settled.AddSeconds(1), $"decided {again.DecidedAt:O}, accepted {accepted:O}..{settled:O}");
            Assert.Equal(TimeSpan.FromSeconds(30), again.Value!.Lease.ExpiresAt - again.DecidedAt);
            await using var mux = await ConnectionMultiplexer.ConnectAsync(RedisServer.Configuration + ",allowAdmin=true");
            var snapshot = await RedisSnapshotReader.ReadAsync(mux, c.Application);
            Assert.Equal("reserved", snapshot.Model.Jobs.Single(j => j.JobId == again.Value.Position.JobId).State);
            Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
            Assert.Equal(1, snapshot.Model.Attempts.Count(a => a.AttemptId == again.Value.Lease.Handle.AttemptId));
        }
        finally
        {
            await RedisServer.DropApplicationAsync(c.Application);
        }
    }

    // ------------------------------------------------------------------ REDIS-11

    [Fact]
    [Trait("Case", "REDIS-11")]
    public async Task Jobs_with_equal_eligibility_are_reserved_in_UuidOrder_which_is_the_byte_order_of_the_job_ID()
    {
        var f = await DriverFixture.StartAsync("redis11u_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        // IDs on which the little-endian Guid.ToByteArray() order and the big-endian (RFC 4122) order differ.
        var random = new Random(20261108);
        var ids = new List<Guid>();
        while (ids.Count < 40)
        {
            var bytes = new byte[16];
            random.NextBytes(bytes);
            ids.Add(new Guid(bytes, bigEndian: true));
        }

        var expected = ids.Order(Internal.UuidOrder.Instance).ToList();
        Assert.NotEqual(expected, ids.Order(Comparer<Guid>.Create((x, y) => x.ToByteArray().AsSpan().SequenceCompareTo(y.ToByteArray()))).ToList());

        // One command, so one decision time: every job gets the same eligibility score and only the ID bytes order them.
        var reply = await c.Driver.DispatchManyAsync(Many(c, ids, 40, DateTimeOffset.UtcNow.AddSeconds(30)), Ct);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var order = new List<Guid>();
        for (var i = 0; i < ids.Count; i++)
        {
            var reserved = await c.ReserveAsync();
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            order.Add(reserved.Value!.Position.JobId);
        }

        Assert.Equal(expected, order);
    }

    [Fact]
    [Trait("Case", "REDIS-11")]
    public async Task Millisecond_conversions_floor_a_lease_and_ceil_a_requested_delay_and_leave_exact_values_alone()
    {
        var f = await DriverFixture.StartAsync("redis11m_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        foreach (var (lease, expectedMs) in new[] { (TimeSpan.FromTicks(15_009_999), 1500L), (TimeSpan.FromMilliseconds(1500), 1500L), (TimeSpan.FromTicks(15_000_001), 1500L) })
        {
            await c.Dispatcher.DispatchAsync(new PingJob("l" + expectedMs), cancellationToken: Ct);
            var reserved = await c.ReserveAsync(lease: lease);
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            Assert.Equal(expectedMs, (long)(reserved.Value!.Lease.ExpiresAt - reserved.DecidedAt).TotalMilliseconds);
            Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(reserved.Value.Lease.Handle)).Status);
            while ((await c.ReserveAsync()) is { Status: StoreStatus.Applied } drain)
            {
                await c.ReleaseAsync(drain.Value!.Lease.Handle);
                break;
            }
        }

        foreach (var (delay, expectedMs) in new[] { (TimeSpan.FromTicks(5_000_001), 501L), (TimeSpan.FromMilliseconds(500), 500L) })
        {
            await c.Dispatcher.DispatchAsync(new PingJob("d" + expectedMs), cancellationToken: Ct);
            ReservedJob job;
            while (true)
            {
                var r = await c.ReserveAsync();
                Assert.Equal(StoreStatus.Applied, r.Status);
                job = r.Value!;
                if (job.Contract.Name != null && (await c.BeginAsync(job.Lease.Handle)).Reply.Status == StoreStatus.Applied)
                {
                    break;
                }
            }

            var finished = await c.FinishAsync(job.Lease.Handle, AttemptOutcome.RetryableFailure, requestedDelay: delay);
            Assert.Equal(StoreStatus.Applied, finished.Status);
            Assert.Equal(expectedMs, (long)(finished.Value!.EligibleAt!.Value - finished.DecidedAt).TotalMilliseconds);
        }
    }

    [GeneratedRegex(@"table\.sort\(([^()]|\([^()]*\))*\)")]
    private static partial Regex SortCall();

    [Fact]
    [Trait("Case", "REDIS-11")]
    public void No_script_orders_text_with_the_Lua_less_than_operator_which_follows_the_server_locale()
    {
        var sources = RxScript.AllSources().ToList();
        Assert.True(sources.Count >= 40);
        var sorts = 0;
        foreach (var source in sources)
        {
            foreach (Match m in SortCall().Matches(source))
            {
                sorts++;
                var call = m.Value;
                var comparator = call[(call.IndexOf(',', StringComparison.Ordinal) + 1)..];
                Assert.True(call.Contains(',', StringComparison.Ordinal), "table.sort without a comparator orders strings with Lua '<': " + call);
                // A comparator is either the bytewise helper or a function over numeric fields only (n, oldest, hb are integers).
                Assert.True(comparator.Contains("U.blt", StringComparison.Ordinal)
                    || Regex.IsMatch(comparator, @"function\(x, y\) return x\.n < y\.n end|function\(a, b\) return a\.oldest < b\.oldest end|if x\.hb ~= y\.hb then return x\.hb > y\.hb end"),
                    "unexpected sort comparator: " + call);
            }
        }

        Assert.True(sorts >= 10, $"only {sorts} table.sort calls found");
    }

    [Fact]
    [Trait("Case", "REDIS-11")]
    public async Task A_forged_key_prefix_is_refused_before_any_write()
    {
        var f = await DriverFixture.StartAsync("redis11p_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = Store(c);
        var mux = await RedisServer.AdminAsync();
        var db = mux.GetDatabase();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        var before = await server.DatabaseSizeAsync();
        foreach (var forged in new[] { "rw:{4A}:meta", "rw:{zz}:meta", "rw:{6162}:meta:x", "other:{6162}:meta", "rw:{6162}:meta\nrw:{6162}:" })
        {
            var op = new RxOp("forged", Guid.NewGuid(), Operations.Dispatch, DateTimeOffset.UtcNow.AddSeconds(30), new byte[32], RxLane.Producer, 'c');
            var frame = store.Runner.Client.Frame(op, store.StoreId, "0", "0", "initial", "", 0);
            var a = await store.Runner.Client.EvalAsync(RxLane.Producer, RxScript.Write("dispatch"), forged, frame, null, Ct);
            Assert.Equal(["R", "InvariantFault", "N", "0", "invalid_transition"], a.Take(5).Select(x => (string)x!));
            Assert.False(await db.KeyExistsAsync(forged));
        }

        Assert.Equal(before, await server.DatabaseSizeAsync());
    }
}
