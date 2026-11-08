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
        for (var n = 1; n <= 400; n++)
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

        // A batch of 60 jobs is larger than one call (100 after-images): prefixes fall between its projection batches too.
        var bigSeq = 0;
        counts["DispatchMany60"] = await Sweep<BatchDispatchReceipt>(c, () =>
        {
            var p = c.Prepared.PrepareMany([.. Enumerable.Range(0, 60).Select(i => new BatchDispatchItem(new PingJob($"big{bigSeq}-{i}")))]);
            bigSeq++;
            return Task.FromResult<Func<Task<BatchDispatchReceipt>>>(() => c.Prepared.DispatchAsync(p, Ct));
        }, (again, first) =>
        {
            Assert.Equal(60, again.Items.Count);
            Assert.All(again.Items, i => Assert.Equal(DispatchDisposition.Created, i.Receipt!.Disposition));
            if (first is not null)
            {
                Assert.Equal(first.Items, again.Items);
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

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Every_write_prefix_of_retention_cleanup_batches_recovers_and_the_pass_still_completes()
    {
        var f = await DriverFixture.StartAsync("redis01cl_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < 40; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct);
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            await c.BeginAsync(lease);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        }

        var mux = await RedisServer.AdminAsync();
        var db = mux.GetDatabase();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        await foreach (var k in server.KeysAsync(db.Database, RxClient.KeyPrefix(f.Application) + "job:*"))
        {
            var at = long.Parse((string)(await db.HashGetAsync(k, "terminalat"))!, System.Globalization.CultureInfo.InvariantCulture);
            await db.HashSetAsync(k, "terminalat", at - (long)TimeSpan.FromDays(400).TotalMilliseconds);
        }

        var retention = (IRetentionStore)c.Driver;
        var limits = new CleanupLimits(10, 1 << 20, TimeSpan.FromSeconds(5));
        var store = Store(c);
        var interrupted = 0;
        var step = 0;
        foreach (var recordClass in RecordClasses.All)
        {
            for (var i = 0; i < 200; i++)
            {
                store.Runner.Client.FailAfter = 1 + step++ % 9;
                try
                {
                    var attempt = await retention.RunCleanupBatchAsync(c.Application, recordClass, limits, Ct);
                    if (attempt.Status != "done")
                    {
                        interrupted++;
                    }
                }
                catch (StoreOutcomeUnknownException)
                {
                    interrupted++;
                }
                finally
                {
                    store.Runner.Client.FailAfter = 0;
                }

                var r = await retention.RunCleanupBatchAsync(c.Application, recordClass, limits, Ct);
                Assert.True(r.Status == "done", $"{recordClass}: {r.Status} {r.ErrorCode}");
                if (r.PassComplete)
                {
                    break;
                }

                Assert.True(i < 199, "pass did not complete");
            }
        }

        Assert.True(interrupted >= 5, $"only {interrupted} interrupted cleanup batches");
        Assert.Equal("", (string?)await db.HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");
        Assert.Equal(0, await db.SortedSetLengthAsync(RxClient.KeyPrefix(f.Application) + "idx:created"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
