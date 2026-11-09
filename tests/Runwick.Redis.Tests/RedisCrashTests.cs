using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Dispatching;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>One AOF-always server per test class (<c>runwick-redis-r01-*</c>), removed when the class ends.</summary>
public sealed class R01Server : IAsyncLifetime
{
    internal RedisNode Node { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        Node = await RedisNode.StartAsync(namePrefix: "runwick-redis-r01-");
        await RunwickRedisMigrations.MigrateAsync(Node.Configuration, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync() => await Node.DisposeAsync();
}

/// <summary>
/// REDIS-01 (22 §12) with kill -9: every write prefix of each operation is interrupted, the server is killed and restarted from the
/// AOF, reads must answer Recovering, and the resend must give the state and reply of a clean run byte for byte. The clean run is the
/// same stored decision (an intent fixes the decision time and every ID) projected with no interruption on a copy of the state the
/// command started from, under a second application prefix of the same server.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
public sealed class RedisCrashTests(R01Server server) : IClassFixture<R01Server>
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ControlActor Actor = new("user:r01", null, "host", "test");

    RedisNode Node => server.Node;

    /// <summary>One operation: builds fresh state and returns the call to interrupt and resend.</summary>
    sealed record Operation(bool Receipt, Func<TestClient, RedisCrashTests, Task<Func<Task<object?>>>> Prepare);

    static readonly Dictionary<string, Operation> Operations = new()
    {
        ["Finish"] = new(true, async (c, t) =>
        {
            var lease = await t.ReserveBegin(c, new PingJob("f" + Guid.NewGuid().ToString("N")[..6]));
            var (id, na) = Identity();
            return async () => await c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: id, notAfter: na);
        }),
        // Reserve under a concurrency scope: the permit and the scope fence are written with the lease.
        ["Reserve+permit+fence"] = new(true, async (c, _) =>
        {
            await c.Dispatcher.DispatchAsync(new ResourceJob("rv" + Guid.NewGuid().ToString("N")[..6], "res" + Guid.NewGuid().ToString("N")[..6]), cancellationToken: Ct);
            var (id, na) = Identity();
            return async () => await c.ReserveAsync(commandId: id, notAfter: na);
        }),
        // Begin of a job with two rate scopes: the rate grants are written with it.
        ["Begin+rate-grants"] = new(true, async (c, _) =>
        {
            var s = Guid.NewGuid().ToString("N")[..6];
            await c.Dispatcher.DispatchAsync(new RatedOrderJob("bg" + s, "fa" + s, "fb" + s), cancellationToken: Ct);
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            var (id, na) = Identity();
            return async () => await c.BeginAsync(lease, id, na);
        }),
        ["Release"] = new(true, async (c, _) =>
        {
            await c.Dispatcher.DispatchAsync(new PingJob("rl" + Guid.NewGuid().ToString("N")[..6]), cancellationToken: Ct);
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            var (id, na) = Identity();
            return async () => await c.ReleaseAsync(lease, id, na);
        }),
        ["ReserveAndBegin"] = new(true, async (c, _) =>
        {
            await c.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new PingJob("rb1" + Guid.NewGuid())), new BatchDispatchItem(new PingJob("rb2" + Guid.NewGuid()))],
                cancellationToken: Ct);
            var (id, na) = Identity();
            return async () => await c.ReserveAndBeginAsync(2, commandId: id, notAfter: na);
        }),
        ["FinishMany"] = new(true, async (c, _) =>
        {
            await c.Dispatcher.DispatchManyAsync([new BatchDispatchItem(new PingJob("fm1" + Guid.NewGuid())), new BatchDispatchItem(new PingJob("fm2" + Guid.NewGuid()))],
                cancellationToken: Ct);
            var claimed = (await c.ReserveAndBeginAsync(2)).Value!.Outcomes.Select(o => o.Reserved!.Lease.Handle).ToList();
            var items = claimed.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
            var id = Guid.CreateVersion7();
            return async () => await c.FinishManyAsync(items, id);
        }),
        ["Recover"] = new(true, async (c, _) =>
        {
            await c.Dispatcher.DispatchAsync(new PingJob("rc" + Guid.NewGuid().ToString("N")[..6]), cancellationToken: Ct);
            var lease = (await c.ReserveAsync(lease: TimeSpan.FromSeconds(1))).Value!.Lease.Handle;
            await c.BeginAsync(lease);
            await Task.Delay(1300, Ct);
            var expired = (await c.Driver.ListExpiredLeasesAsync(c.Application, 10, Ct)).Single(e => e.JobId == lease.JobId);
            var (id, na) = Identity();
            return async () => await c.RecoverAsync(expired, id, na);
        }),
        // A Dispatch binding a uniqueness window key.
        ["Dispatch+window"] = new(true, (c, _) =>
        {
            var s = Guid.NewGuid().ToString("N")[..6];
            var p = c.Prepared.Prepare(new PingJob("wd" + s), new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "wk" + s, TimeSpan.FromHours(1)) });
            return Task.FromResult<Func<Task<object?>>>(async () => await c.Prepared.DispatchAsync(p, Ct));
        }),
        // 60 jobs: more than 100 after-images, so prefixes also fall between projection batches.
        ["DispatchMany60"] = new(true, (c, _) =>
        {
            var s = Guid.NewGuid().ToString("N")[..6];
            var p = c.Prepared.PrepareMany([.. Enumerable.Range(0, 60).Select(i => new BatchDispatchItem(new PingJob($"big{s}-{i}")))]);
            return Task.FromResult<Func<Task<object?>>>(async () => await c.Prepared.DispatchAsync(p, Ct));
        }),
        ["Cancel"] = new(true, async (c, _) =>
        {
            var s = Guid.NewGuid().ToString("N")[..6];
            var job = (await c.Dispatcher.DispatchAsync(new KeyedJob("c" + s, "k" + s), cancellationToken: Ct)).JobId.Value;
            var detail = (await c.Services.GetRequiredService<IManagementQueryStore>().GetJobAsync(c.Application, job, Ct)).Value!.Job;
            var cmd = c.Command(new CancelRequest(job, detail.Generation, detail.Revision, Actor));
            return async () => await c.Services.GetRequiredService<IJobControlStore>().CancelAsync(cmd, Ct);
        }),
        ["ReceiveOutbox"] = new(true, (c, _) =>
        {
            var prepared = c.Services.GetRequiredService<DispatchPreparer>().Prepare(c.Runtime.StoreId, new PingJob("o" + Guid.NewGuid()), null);
            var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
            var cut = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeMilliseconds());
            var fingerprint = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, cut.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);
            var command = new StoreCommand<ReceiveOutboxRequest>(c.Application, Guid.CreateVersion7(),
                DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds()),
                new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), cut, fingerprint, prepared.StoreId));
            return Task.FromResult<Func<Task<object?>>>(async () => await ((IOutboxDestinationStore)c.Driver).ReceiveOutboxAsync(command, Ct));
        }),
        // No receipt (03 §8.1): there is no resend, the next writer publishes the stored decision.
        ["PublishPresence"] = new(false, (c, t) =>
        {
            var seq = ++t._presenceSeq;
            var record = new PresenceRecord(c.WorkerInstanceId, seq, 1, "ready", Environment.ProcessId, "host", "build", ["default"],
                new Dictionary<string, int>(), c.Contracts, TimeSpan.FromMinutes(5));
            return Task.FromResult<Func<Task<object?>>>(async () => await c.Driver.PublishPresenceAsync(c.Application, record, Ct));
        }),
        ["RenewMany"] = new(false, async (c, t) =>
        {
            var leases = new List<LeaseHandle>();
            for (var i = 0; i < 3; i++)
            {
                await c.Dispatcher.DispatchAsync(new PingJob("rn" + Guid.NewGuid()), cancellationToken: Ct);
                leases.Add((await c.ReserveAsync()).Value!.Lease.Handle);
            }

            return async () => await c.RenewManyAsync(leases, TimeSpan.FromMinutes(5));
        }),
        // Receipt cleanup: three ownership receipts are aged past their retention, then one cleanup batch deletes them.
        ["Cleanup receipts"] = new(false, async (c, t) =>
        {
            for (var i = 0; i < 3; i++)
            {
                await c.Dispatcher.DispatchAsync(new PingJob("cl" + Guid.NewGuid()), cancellationToken: Ct);
                await c.ReserveAsync();
            }

            var db = await t.DbAsync();
            var index = RxClient.KeyPrefix(c.Application) + "rcpt:exp:ownership";
            foreach (var m in await db.SortedSetRangeByRankAsync(index))
            {
                await db.SortedSetAddAsync(index, m, 1);
            }

            var store = Store(c);
            return async () => await store.Runner.WriteAsync(
                new RxOp(c.Application, Guid.CreateVersion7(), Runwick.Protocol.Operations.CleanupBatch, DateTimeOffset.UtcNow.AddSeconds(30), null, RxLane.Background, 'n'),
                RxScript.Write("cleanup"), [RecordClasses.ReceiptsOwnership, "batch", 100, 1 << 20, "", RedisStore.RetDefaultHours], Ct);
        }),
    };

    public static TheoryData<string> Names => [.. Operations.Keys];

    long _presenceSeq;
    ConnectionMultiplexer? _admin;

    static (Guid Id, DateTimeOffset NotAfter) Identity() => (Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddSeconds(60));

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    async Task<IDatabase> DbAsync()
    {
        _admin ??= await ConnectionMultiplexer.ConnectAsync(Node.Configuration + ",allowAdmin=true,abortConnect=false,connectTimeout=5000");
        return _admin.GetDatabase();
    }

    async Task<LeaseHandle> ReserveBegin(TestClient c, IQueueJob job)
    {
        await c.Dispatcher.DispatchAsync(job, cancellationToken: Ct);
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        return lease;
    }

    /// <summary>Retries while the client reconnects after the crash (calls fail fast meanwhile, nothing is queued).</summary>
    static async Task<T> Retry<T>(Func<Task<T>> call)
    {
        var until = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            try
            {
                return await call();
            }
            catch (Exception ex) when (DateTime.UtcNow < until && ex is StoreNotAppliedException or StoreUnavailableException or RedisException
                or TimeoutException or StoreOutcomeUnknownException { Code: "commit_outcome_unknown" })
            {
                await Task.Delay(200, Ct);
            }
        }
    }

    /// <summary>The write counts to interrupt after: every one up to 30, then every 25th and each projection-batch boundary.</summary>
    static IEnumerable<int> Prefixes()
    {
        for (var n = 1; n <= 1200; n++)
        {
            if (n <= 30 || n % 25 == 0 || (n % 100) is >= 98 or <= 3)
            {
                yield return n;
            }
        }
    }

    async Task<List<(string Key, byte[] Dump)>> DumpAsync(string app)
    {
        var db = await DbAsync();
        var keys = await KeysAsync(app);
        var list = new List<(string, byte[])>();
        foreach (var k in keys)
        {
            list.Add((k[RxClient.KeyPrefix(app).Length..], (await db.KeyDumpAsync(k))!));
        }

        return list;
    }

    async Task<List<string>> KeysAsync(string app)
    {
        var mux = _admin!;
        var srv = mux.GetServer(mux.GetEndPoints()[0]);
        var keys = new List<string>();
        await foreach (var k in srv.KeysAsync(pattern: RxClient.KeyPrefix(app) + "*"))
        {
            keys.Add(k!);
        }

        return keys;
    }

    /// <summary>
    /// Every key of the application as sorted lines <c>key|field|value</c> (values hex, so the comparison is on bytes). The receipt's
    /// <c>confirmed</c> marker of <paramref name="command"/> is left out: 22 §4.5 makes it a fresh write by each resend, holding its time.
    /// </summary>
    async Task<List<string>> StateAsync(string app, string? command)
    {
        var db = await DbAsync();
        var prefix = RxClient.KeyPrefix(app);
        var lines = new List<string>();
        foreach (var key in await KeysAsync(app))
        {
            var k = key[prefix.Length..];
            static string H(RedisValue v) => Convert.ToHexString((byte[])v!);
            switch (await db.KeyTypeAsync(key))
            {
                case RedisType.Hash:
                    foreach (var e in await db.HashGetAllAsync(key))
                    {
                        if (!(e.Name == "confirmed" && (k == "rcpt:" + command || (command == "*" && k.StartsWith("rcpt:", StringComparison.Ordinal)))))
                        {
                            lines.Add($"{k}|{H(e.Name)}|{H(e.Value)}");
                        }
                    }

                    break;
                case RedisType.SortedSet:
                    foreach (var e in await db.SortedSetRangeByRankWithScoresAsync(key))
                    {
                        lines.Add($"{k}|{H(e.Element)}|{e.Score:R}");
                    }

                    break;
                case RedisType.String:
                    lines.Add($"{k}||{H(await db.StringGetAsync(key))}");
                    break;
                case var other:
                    lines.Add($"{k}|type {other}");
                    break;
            }
        }

        lines.Sort(StringComparer.Ordinal);
        return lines;
    }

    static string FirstDifference(List<string> a, List<string> b)
    {
        var onlyA = a.Except(b).Take(3);
        var onlyB = b.Except(a).Take(3);
        return $"recovered only: [{string.Join("; ", onlyA)}] clean only: [{string.Join("; ", onlyB)}]";
    }

    static async Task<string> RawRead(TestClient c)
    {
        var store = Store(c);
        var a = await store.Runner.Client.EvalAsync(RxLane.Background, RxScript.Read("describe"), RxClient.Meta(c.Application),
            RxClient.ReadFrame(store.Runner.StoreId), null, Ct);
        return (string)a[0]!;
    }

    static byte[] B(RedisResult r) => r.IsNull ? [] : (byte[])r!;

    [Theory]
    [MemberData(nameof(Names))]
    [Trait("Case", "REDIS-01")]
    public async Task Kill_9_in_every_interrupted_state_restarts_from_the_AOF_hides_the_partial_state_and_resends_to_the_clean_runs_bytes(string name)
    {
        var op = Operations[name];
        await using var c = await TestClient.CreateAsync(RedisServer.Prefix + Node.Configuration, "r01_" + Guid.NewGuid().ToString("N")[..8],
            new HarnessFaultInjector(), r =>
            {
                TestJobs.Register(r);
                RateJobs.Register(r);
            });
        var store = Store(c);
        var db = await DbAsync();
        var meta = RxClient.Meta(c.Application);
        await db.HashSetAsync(meta, "caps", "{\"rate_admission\":1}");
        var trace = new List<(RxScript Script, RedisValue[] Args, RedisResult[] Reply)>();
        store.Runner.Client.Traced = (s, a, r) =>
        {
            lock (trace)
            {
                trace.Add((s, a, r));
            }
        };

        int pending = 0, compared = 0, last = 0;
        foreach (var n in Prefixes())
        {
            last = n;
            var call = await op.Prepare(c, this);
            var before = await DumpAsync(c.Application);
            store.Runner.Client.FailAfter = n;
            var thrown = false;
            try
            {
                await call();
            }
            catch (Exception ex) when (ex is StoreOutcomeUnknownException or DispatchOutcomeUnknownException)
            {
                thrown = true;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            if (!thrown)
            {
                break; // n is past the last write: every earlier prefix was interrupted
            }

            var held = await db.HashGetAsync(meta, ["guard", "intent", "intentsha"]);
            var guard = (string?)held[0] ?? "";
            await Node.CrashAndRestartAsync();
            await Retry(() => db.PingAsync());
            if (guard.Length == 0)
            {
                // Published, only the reply was lost: the resend replays and changes nothing but the confirmation marker.
                if (op.Receipt)
                {
                    var published = await Retry(() => StateAsync(c.Application, "*"));
                    await Retry(call);
                    Assert.Equal(published, await StateAsync(c.Application, "*"));
                }

                continue;
            }

            pending++;
            Assert.Equal("RECOVERING", await Retry(() => RawRead(c))); // reads hide the partial state after the restart too

            RedisResult[]? recoveredReply = null;
            RedisValue[]? resendArgs = null;
            RxScript? script = null;
            if (op.Receipt)
            {
                lock (trace)
                {
                    trace.Clear();
                }

                await Retry(call);
                lock (trace)
                {
                    (script, resendArgs, recoveredReply) = trace.Last(e => (string)e.Reply[0]! == "R" && (string?)e.Args[3] == guard);
                }
            }
            else
            {
                Assert.True(await Retry(() => store.Runner.RecoverAsync(c.Application, "", RxLane.Background, Ct)));
            }

            Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
            var recovered = await StateAsync(c.Application, guard);

            // The clean run: the start state under a second prefix, the same intent stored (write 1 exactly), projected uninterrupted.
            var reference = "r01ref_" + Guid.NewGuid().ToString("N")[..8];
            var refMeta = RxClient.Meta(reference);
            foreach (var (k, dump) in before)
            {
                await db.KeyRestoreAsync(RxClient.KeyPrefix(reference) + k, dump);
            }

            await db.HashSetAsync(refMeta, [new("guard", held[0]), new("intent", held[1]), new("intentsha", held[2]), new("ipos", "1")]);
            var cleanReply = (RedisResult[])(await db.ScriptEvaluateAsync("return cmsgpack.unpack(redis.call('HGET', KEYS[1], 'intent')).reply", [refMeta]))!;
            for (var i = 0; i < 64; i++)
            {
                var r = await store.Runner.Client.EvalAsync(RxLane.Background, RxScript.Write("recovery"), refMeta, [0, "", 100], null, Ct);
                if ((string)r[0]! != "MORE")
                {
                    Assert.Equal("PUBLISHED", (string)r[0]!);
                    break;
                }
            }

            if (op.Receipt)
            {
                var replay = await store.Runner.Client.EvalAsync(RxLane.Background, script!, refMeta, resendArgs!, null, Ct);
                var got = recoveredReply!;
                Assert.Equal(replay.Select(B), got.Select(B));
                // Against the reply the uninterrupted call returns: every field equal, except that a resend is marked replayed (03 §8.2).
                for (var i = 0; i < cleanReply.Length; i++)
                {
                    if (i != 3)
                    {
                        Assert.True(B(cleanReply[i]).AsSpan().SequenceEqual(B(got[i])), $"{name} n={n}: reply field {i} differs");
                    }
                }

                Assert.Equal("1", (string?)got[3]);
            }

            var clean = await StateAsync(reference, guard);
            Assert.True(clean.SequenceEqual(recovered), $"{name} n={n}: {FirstDifference(recovered, clean)}");
            compared++;
            foreach (var k in await KeysAsync(reference))
            {
                await db.KeyDeleteAsync(k);
            }
        }

        store.Runner.Client.Traced = null;
        TestContext.Current.SendDiagnosticMessage($"REDIS-01 kill -9 {name}: completed after {last} writes; {pending} pending states killed, {compared} compared with the clean run");
        Assert.True(pending >= 2, $"{name}: only {pending} pending states");
        Assert.Equal(pending, compared);
        Assert.Empty(SnapshotInvariants.Violations((await RedisSnapshotReader.ReadAsync(_admin!, c.Application)).Model));
    }
}
