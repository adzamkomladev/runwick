using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;

namespace Runwick.Redis.Tests;

/// <summary>REDIS-01: every write prefix of the ownership, batch, recovery, uniqueness and rate operations (22 §12).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisPrefixTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    /// <summary>
    /// Interrupts one command after each write n = 1, 2, … (the script stops after its n-th write) and resends it with the same
    /// identity, until n passes the last write. <paramref name="prepare"/> builds fresh state and returns the resendable call;
    /// <paramref name="check"/> sees the resend and the interrupted call's reply, if it had one. Returns the interrupted count.
    /// </summary>
    static async Task<int> Sweep<T>(TestClient c, Func<Task<Func<Task<T>>>> prepare, Action<T, T?> check) where T : class
    {
        var store = Store(c);
        var interrupted = 0;
        for (var n = 1; n <= 120; n++)
        {
            var call = await prepare();
            store.Runner.Client.FailAfter = n;
            T? first = null;
            var thrown = false;
            try
            {
                first = await call();
            }
            catch (Exception ex) when (ex is StoreOutcomeUnknownException or DispatchOutcomeUnknownException)
            {
                thrown = true;
                interrupted++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            check(await call(), first);
            if (!thrown)
            {
                return interrupted;
            }
        }

        throw new InvalidOperationException("the command never completed");
    }

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Every_write_prefix_of_Reserve_Begin_Release_ReserveAndBegin_FinishMany_Recover_a_windowed_Dispatch_and_a_rate_grant_resends_to_one_effect()
    {
        var f = await DriverFixture.StartAsync("redis01ops_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            RateJobs.Register(r);
        });
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        await db.HashSetAsync(RxClient.Meta(f.Application), "caps", "{\"rate_admission\":1}");
        var seq = 0;
        var counts = new Dictionary<string, int>();

        // Reserve, on a job under a concurrency scope (the scope fence moves with it).
        counts["Reserve"] = await Sweep<StoreReply<ReservedJob>>(c, async () =>
        {
            await c.Dispatcher.DispatchAsync(new ResourceJob("rv" + seq++, "res" + seq), cancellationToken: Ct);
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            return () => c.ReserveAsync(commandId: id, notAfter: notAfter);
        }, (again, first) =>
        {
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal(first?.Value!.Lease.Handle ?? again.Value!.Lease.Handle, again.Value!.Lease.Handle);
        });

        // Begin on a job with rate scopes: the rate grants are written with it.
        counts["Begin+rate"] = await Sweep<BeginReply>(c, async () =>
        {
            await c.Dispatcher.DispatchAsync(new RatedOrderJob("bg" + seq++, "fa" + seq, "fb" + seq), cancellationToken: Ct);
            var lease = (await c.ReserveAsync(queue: "default")).Value!.Lease.Handle;
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            return () => c.BeginAsync(lease, id, notAfter);
        }, (again, first) =>
        {
            Assert.Equal(StoreStatus.Applied, again.Reply.Status);
            if (first is not null)
            {
                Assert.Equal(first.Reply.Value, again.Reply.Value);
            }
        });

        counts["Release"] = await Sweep<StoreReply<ReleasedReservation>>(c, async () =>
        {
            await c.Dispatcher.DispatchAsync(new PingJob("rl" + seq++), cancellationToken: Ct);
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            return () => c.ReleaseAsync(lease, id, notAfter);
        }, (again, first) =>
        {
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal(first?.Value!.Disposition ?? again.Value!.Disposition, again.Value!.Disposition);
        });

        counts["ReserveAndBegin"] = await Sweep<StoreReply<ClaimedJobs>>(c, async () =>
        {
            await c.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new PingJob("rb" + seq++)), new BatchDispatchItem(new PingJob("rb" + seq++))], cancellationToken: Ct);
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            return () => c.ReserveAndBeginAsync(2, commandId: id, notAfter: notAfter);
        }, (again, first) =>
        {
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal(2, again.Value!.Outcomes.Count);
            Assert.All(again.Value.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
            if (first is not null)
            {
                Assert.Equal(first.Value!.Outcomes.Select(o => o.JobId), again.Value.Outcomes.Select(o => o.JobId));
            }
        });

        counts["FinishMany"] = await Sweep<StoreReply<FinishedMany>>(c, async () =>
        {
            await c.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new PingJob("fm" + seq++)), new BatchDispatchItem(new PingJob("fm" + seq++))], cancellationToken: Ct);
            var claimed = (await c.ReserveAndBeginAsync(2)).Value!.Outcomes.Select(o => o.Reserved!.Lease.Handle).ToList();
            var items = claimed.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
            var id = Guid.CreateVersion7();
            return () => c.FinishManyAsync(items, id);
        }, (again, first) =>
        {
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.All(again.Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        });

        for (var i = 0; i < 24; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("rc" + i), cancellationToken: Ct);
            var lease = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(1))).Value!.Lease.Handle;
            await c.BeginAsync(lease);
        }

        await Task.Delay(1300, Ct);
        var expired = (await c.Driver.ListExpiredLeasesAsync(c.Application, 100, Ct)).ToList();
        Assert.Equal(24, expired.Count);
        var next = 0;
        counts["Recover"] = await Sweep<StoreReply<FinishedAttempt>>(c, () =>
        {
            var e = expired[next++];
            var id = Guid.CreateVersion7();
            var notAfter = DateTimeOffset.UtcNow.AddSeconds(30);
            return Task.FromResult<Func<Task<StoreReply<FinishedAttempt>>>>(() => c.RecoverAsync(e, id, notAfter));
        }, (again, first) => Assert.Equal(StoreStatus.Applied, again.Status));

        // A Dispatch binding a uniqueness window key (the window row, the reverse link and the receipt are separate writes).
        var windowSeq = 0;
        counts["Dispatch+window"] = await Sweep<DispatchReceipt>(c, () =>
        {
            var p = c.Prepared.Prepare(new PingJob("wd" + windowSeq), new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "wk" + windowSeq++, TimeSpan.FromHours(1)) });
            return Task.FromResult<Func<Task<DispatchReceipt>>>(() => c.Prepared.DispatchAsync(p, Ct));
        }, (again, first) =>
        {
            Assert.Equal(DispatchDisposition.Created, again.Disposition);
            if (first is not null)
            {
                Assert.Equal(first.JobId, again.JobId);
            }
        });

        foreach (var (op, n) in counts)
        {
            Assert.True(n >= 2, $"{op}: only {n} interrupted prefixes");
        }

        TestContext.Current.SendDiagnosticMessage("REDIS-01 prefixes interrupted per operation: " + string.Join(", ", counts.Select(p => $"{p.Key}={p.Value}")));
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal("", (string?)await db.HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");
    }
}
