using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

/// <summary>REDIS-06 (mixed concurrent soak with injected failures) and REDIS-07 (renewal wait) of 22 §12.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisSoakTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ControlActor Actor = new("user:soak", null, "host", "redis");

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<RedisTickJob>().Named("test.redis.tick").ToQueue("default");
    }

    /// <summary>Measured numbers are kept next to the test binary (reports/NAME.txt) as well as in the test output.</summary>
    static void Report(string name, string text)
    {
        TestContext.Current.SendDiagnosticMessage(text);
        var dir = Path.Combine(AppContext.BaseDirectory, "reports");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, name + ".txt"), text + Environment.NewLine);
    }

    /// <summary>
    /// Stops <paramref name="send"/> right after it stores its intent and measures that intent: after-images (as the 1,024 limit counts
    /// them), distinct keys written other than <c>meta</c> (as the 256 limit counts them), serialized bytes. Null when nothing was stored
    /// (the call was refused before any write). The caller resends the same command, which publishes it.
    /// </summary>
    static async Task<(int Ops, int Keys, int Bytes)?> IntentShapeAsync(TestClient c, Func<Task> send)
    {
        var store = Store(c);
        store.Runner.Client.FailAfter = 1;
        try
        {
            await send();
            return null;
        }
        catch (Exception ex) when (ex is StoreOutcomeUnknownException or DispatchOutcomeUnknownException)
        {
            // interrupted after the intent write
        }
        finally
        {
            store.Runner.Client.FailAfter = 0;
        }

        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var r = (RedisResult[])(await db.ScriptEvaluateAsync("""
            local b = redis.call('HGET', KEYS[1], 'intent')
            local it = cmsgpack.unpack(b)
            local seen, n = {}, 0
            for _, op in ipairs(it.ops) do
              if op[2] ~= 'meta' and not seen[op[2]] then seen[op[2]] = true; n = n + 1 end
            end
            return { #it.ops, n, #b }
            """, [RxClient.Meta(c.Application)]))!;
        return ((int)r[0], (int)r[1], (int)r[2]);
    }

    // ------------------------------------------------------------------ REDIS-06

    [Fact]
    [Trait("Case", "REDIS-06")]
    public async Task Eight_connections_run_1200_mixed_commands_with_injected_failures_and_every_count_agrees()
    {
        const int Workers = 8;
        const int Steps = 100;
        var f = await DriverFixture.StartAsync("redis06_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var clients = new List<TestClient>();
        for (var i = 0; i < Workers; i++)
        {
            clients.Add(await f.OpenClient(configure: Jobs));
        }

        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var meta = RxClient.Meta(f.Application);
        await db.HashSetAsync(meta, "caps", "{\"scheduler\":1}");

        var key = "soak";
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration(key, new RedisTickJob(key)) { Cron = "* * * * *" },
            clients[0].Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
        var store0 = Store(clients[0]);
        var cutoff0 = await store0.StoreNowAsync(Ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff0)?.At;
        Assert.Equal(StoreStatus.Applied, (await store0.RegisterScheduleAsync(clients[0].Command(new RegisterScheduleRequest(key, definition, cutoff0, first)), Ct)).Status);
        var startCursor = (await store0.ReadScheduleAsync(f.Application, key, Ct))!.CursorRevision;

        long logical = 0, injected = 0, unknown = 0, created = 0, cancelsApplied = 0, firstDeliveries = 0, fireWins = 0, finished = 0, readerSamples = 0, recovering = 0;
        var violations = new List<string>();
        var barrier = new Barrier(Workers);
        MaterializeRequest?[] shared = new MaterializeRequest?[Steps / 10];
        using var stop = new CancellationTokenSource();

        // Reads (22 §4.6): each is a whole revision or Recovering, and the revisions a reader sees never go back.
        var reader = Task.Run(async () =>
        {
            var store = Store(clients[0]);
            System.Numerics.BigInteger last = 0;
            while (!stop.IsCancellationRequested)
            {
                var a = await store.Runner.Client.EvalAsync(RxLane.Background, RxScript.Read("describe"), meta, RxClient.ReadFrame(store.Runner.StoreId), null, CancellationToken.None);
                var kind = (string)a[0]!;
                Interlocked.Increment(ref readerSamples);
                if (kind == "RECOVERING")
                {
                    Interlocked.Increment(ref recovering);
                }
                else if (kind != "OK")
                {
                    lock (violations) violations.Add("read returned " + kind);
                }
                else
                {
                    var rev = System.Numerics.BigInteger.Parse((string)a[1]!, CultureInfo.InvariantCulture);
                    if (rev < last)
                    {
                        lock (violations) violations.Add($"revision went back {last} -> {rev}");
                    }

                    last = rev;
                }

                await Task.Delay(5);
            }
        }, Ct);

        // One logical command: sent once (a failure may be injected), then resent with the same identity until it is decided.
        async Task<T> Send<T>(TestClient c, Random rng, Func<Task<T>> call)
        {
            Interlocked.Increment(ref logical);
            var inject = rng.Next(5) == 0;
            for (var attempt = 0; ; attempt++)
            {
                if (inject && attempt == 0)
                {
                    Store(c).Runner.Client.FailAfter = rng.Next(1, 5);
                    Interlocked.Increment(ref injected);
                }

                try
                {
                    return await call();
                }
                catch (Exception ex) when (attempt < 40 && (ex is StoreOutcomeUnknownException or StoreNotAppliedException or RunwickException))
                {
                    Interlocked.Increment(ref unknown);
                    await Task.Delay(20, Ct);
                }
                finally
                {
                    Store(c).Runner.Client.FailAfter = 0;
                }
            }
        }

        async Task Worker(int w)
        {
            var c = clients[w];
            var store = Store(c);
            var rng = new Random(1000 + w);
            for (var step = 0; step < Steps; step++)
            {
                switch (step % 10)
                {
                    case 9: // a scheduled fire racing for one slot across all eight connections
                    {
                        var round = step / 10;
                        if (w == 0)
                        {
                            var s = (await store.ReadScheduleAsync(f.Application, key, Ct))!;
                            var now = await store.StoreNowAsync(Ct);
                            var at = s.EvaluatedThrough!.Value.AddMilliseconds(1);
                            shared[round] = new MaterializeRequest(s.ScheduleId, s.DefinitionRevision, s.ControlRevision, s.CursorRevision, null, now,
                                [new PlannedOccurrence(at, Guid.CreateVersion7())], new ScheduleCursor(at, now.AddMinutes(1), now.AddMinutes(1), null), [], false);
                        }

                        await Task.Run(barrier.SignalAndWait, Ct);
                        var plan = shared[round]! with { Occurrences = [shared[round]!.Occurrences[0] with { ProposedJobId = Guid.CreateVersion7() }] };
                        var command = c.Command(plan);
                        var reply = await Send(c, rng, () => store.MaterializeOccurrencesAsync(command, Ct));
                        if (reply.Status == StoreStatus.Applied)
                        {
                            Interlocked.Increment(ref fireWins);
                        }
                        else if (reply.Error?.Code != "revision_conflict")
                        {
                            lock (violations) violations.Add($"fire {reply.Status}/{reply.Error?.Code}");
                        }

                        await Task.Run(barrier.SignalAndWait, Ct);
                        break;
                    }

                    case 0:
                    case 4:
                    {
                        var p = c.Prepared.Prepare(new PingJob($"d{w}-{step}"));
                        await Send(c, rng, () => c.Prepared.DispatchAsync(p, Ct));
                        Interlocked.Increment(ref created);
                        break;
                    }

                    case 1:
                    case 5: // dispatch, then a management control with its audit row
                    {
                        var p = c.Prepared.Prepare(new PingJob($"c{w}-{step}"));
                        var receipt = await Send(c, rng, () => c.Prepared.DispatchAsync(p, Ct));
                        Interlocked.Increment(ref created);
                        var read = await c.Services.GetRequiredService<IManagementQueryStore>().GetJobAsync(f.Application, receipt.JobId.Value, Ct);
                        var job = read.Value!.Job;
                        var cancel = c.Command(new CancelRequest(receipt.JobId.Value, job.Generation, job.Revision, Actor));
                        var reply = await Send(c, rng, () => c.Services.GetRequiredService<IJobControlStore>().CancelAsync(cancel, Ct));
                        if (reply.Status == StoreStatus.Applied)
                        {
                            Interlocked.Increment(ref cancelsApplied);
                        }

                        break;
                    }

                    case 2:
                    case 6:
                    case 8: // reserve, begin, finish
                    {
                        if (step % 10 == 2)
                        {
                            var p = c.Prepared.Prepare(new PingJob($"r{w}-{step}"));
                            await Send(c, rng, () => c.Prepared.DispatchAsync(p, Ct));
                            Interlocked.Increment(ref created);
                        }

                        var reserveId = Guid.CreateVersion7();
                        var reserved = await Send(c, rng, () => c.ReserveAsync(commandId: reserveId));
                        if (reserved.Status != StoreStatus.Applied)
                        {
                            break;
                        }

                        var lease = reserved.Value!.Lease.Handle;
                        var beginId = Guid.CreateVersion7();
                        var begun = await Send(c, rng, async () => (await c.BeginAsync(lease, beginId)).Reply);
                        if (begun.Status != StoreStatus.Applied)
                        {
                            break;
                        }

                        var finishId = Guid.CreateVersion7();
                        var done = await Send(c, rng, () => c.FinishAsync(lease, AttemptOutcome.Succeeded, commandId: finishId));
                        if (done.Status == StoreStatus.Applied)
                        {
                            Interlocked.Increment(ref finished);
                        }

                        break;
                    }

                    default: // 3, 7: an outbox message received by the destination
                    {
                        var prepared = c.Services.GetRequiredService<DispatchPreparer>().Prepare(c.Runtime.StoreId, new PingJob($"o{w}-{step}"), null);
                        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
                        var cut = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeMilliseconds());
                        var fingerprint = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, cut.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);
                        var command = new StoreCommand<ReceiveOutboxRequest>(f.Application, Guid.CreateVersion7(), DateTimeOffset.UtcNow.AddSeconds(20),
                            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), cut, fingerprint, prepared.StoreId));
                        var reply = await Send(c, rng, () => ((IOutboxDestinationStore)c.Driver).ReceiveOutboxAsync(command, Ct));
                        if (reply.Status == StoreStatus.Applied && reply.Value!.FirstDelivery)
                        {
                            Interlocked.Increment(ref firstDeliveries);
                        }

                        break;
                    }
                }
            }
        }

        var clock = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, Workers).Select(w => Task.Run(() => Worker(w), Ct)));
        await stop.CancelAsync();
        await reader;

        // Settle: anything still pending is published by the next write of any process.
        for (var i = 0; i < 20; i++)
        {
            if (((string?)await db.HashGetAsync(meta, "guard") ?? "") == "")
            {
                break;
            }

            try
            {
                await clients[0].ReserveAsync();
            }
            catch (StoreOutcomeUnknownException)
            {
                await Task.Delay(300, Ct);
            }
        }

        Assert.Equal("", (string?)await db.HashGetAsync(meta, "guard") ?? "");
        Assert.Empty(violations);
        var rounds = Steps / 10;
        Assert.True(logical >= 1200, $"only {logical} logical commands ran");
        Assert.True(injected >= 100 && unknown >= 20, $"injected {injected}, interrupted {unknown}");

        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal(rounds, fireWins);
        var schedule = (await store0.ReadScheduleAsync(f.Application, key, Ct))!;
        Assert.Equal(startCursor + rounds, schedule.CursorRevision);
        Assert.Equal(created + firstDeliveries + fireWins, snapshot.Model.Jobs.Count);
        Assert.Equal(cancelsApplied, await f.CountAsync("SELECT count(*) FROM runwick.management_audit"));
        Assert.Equal(firstDeliveries, snapshot.Tables.GetValueOrDefault("outbox", []).Count(r => r.Contains('{', StringComparison.Ordinal)));
        Assert.True(snapshot.Model.Jobs.Count(j => j.State == "succeeded") >= finished);
        Report("redis-06", string.Create(CultureInfo.InvariantCulture,
            $"REDIS-06: {logical} commands, {injected} injected failures, {unknown} interrupted calls, {readerSamples} reads ({recovering} Recovering), {snapshot.Model.Jobs.Count} jobs, {fireWins} fires, {cancelsApplied} audit rows, {firstDeliveries} deliveries, {clock.Elapsed.TotalSeconds:F1} s"));
    }

    // ------------------------------------------------------------------ REDIS-07

    [Fact]
    [Trait("Case", "REDIS-07")]
    public async Task Renewal_wait_while_the_largest_intents_run_back_to_back_is_measured_and_a_maximal_DispatchMany_is_accepted()
    {
        var f = await DriverFixture.StartAsync("redis07_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        await using var _ = f;
        var producer = await f.OpenClient(configure: Jobs);
        var control = await f.OpenClient(configure: Jobs);

        // A maximal public DispatchMany: 100 items mixing active keys, window keys and 33 concurrency scopes, measured, then accepted.
        var maximalPrepared = producer.Prepared.PrepareMany([.. Enumerable.Range(0, 100).Select(i => (i % 3) switch
        {
            0 => new BatchDispatchItem(new KeyedJob("m" + i, "k" + i)),
            1 => new BatchDispatchItem(new PingJob("m" + i), new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "w" + i, TimeSpan.FromHours(1)) }),
            _ => new BatchDispatchItem(new ResourceJob("m" + i, "r" + i)),
        })]);
        var maximalShape = await IntentShapeAsync(producer, () => producer.Prepared.DispatchAsync(maximalPrepared, Ct));
        var maximal = await producer.Prepared.DispatchAsync(maximalPrepared, Ct);
        Assert.NotNull(maximalShape);
        Assert.Equal(100, maximal.Items.Count);
        Assert.All(maximal.Items, i => Assert.Equal(DispatchDisposition.Created, i.Receipt!.Disposition));

        // 64 leases held by the control connection.
        await producer.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, 64).Select(i => new BatchDispatchItem(new PingJob("lease" + i)))], cancellationToken: Ct);
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 64; i++)
        {
            var reserved = await control.ReserveAsync(queue: "default", lease: TimeSpan.FromMinutes(10));
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            leases.Add(reserved.Value!.Lease.Handle);
        }

        // The largest intents a real operation builds run back to back: a driver-level DispatchMany of 120 jobs (two keys each, close
        // to the 256-key limit). 1,024 after-images cannot be reached by any operation: an intent writes at most two after-images per
        // hash or sorted-set key (field deletions, then field sets) and one per string, so 256 keys bound it near 512 (22 §4.3).
        var preparer = producer.Services.GetRequiredService<DispatchPreparer>();
        StoreCommand<DispatchBatchRequest> Batch(string tag)
        {
            var batches = Enumerable.Range(0, 2).Select(b => preparer.PrepareMany(producer.Runtime.StoreId,
                [.. Enumerable.Range(0, 60).Select(i => new BatchDispatchItem(new PingJob($"w{tag}-{b}-{i}")))], null)).ToList();
            var items = batches.SelectMany(p => p.Items.Select((intent, i) => new DispatchItem(intent, p.Scopes[i], p.ItemDigests[i]) { Rates = preparer.RatesFor(intent) })).ToList();
            return producer.Command(new DispatchBatchRequest(items, false, producer.Runtime.StoreId, batches[0].CommandDigest));
        }

        var probe = Batch("probe");
        var writerShape = await IntentShapeAsync(producer, () => producer.Driver.DispatchManyAsync(probe, CancellationToken.None));
        Assert.Equal(StoreStatus.Applied, (await producer.Driver.DispatchManyAsync(probe, CancellationToken.None)).Status);
        Assert.NotNull(writerShape);
        Assert.True(writerShape.Value.Keys <= 256, $"background intent writes {writerShape.Value.Keys} keys");

        using var stop = new CancellationTokenSource();
        var intents = 0;
        var writer = Task.Run(async () =>
        {
            for (var round = 0; !stop.IsCancellationRequested; round++)
            {
                var reply = await producer.Driver.DispatchManyAsync(Batch(round.ToString(CultureInfo.InvariantCulture)), CancellationToken.None);
                Assert.Equal(StoreStatus.Applied, reply.Status);
                Interlocked.Increment(ref intents);
            }
        }, Ct);

        var manyLatency = new List<double>();
        var oneLatency = new List<double>();
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < TimeSpan.FromSeconds(20) && manyLatency.Count < 2000)
        {
            var t = Stopwatch.StartNew();
            var reply = await control.RenewManyAsync(leases, TimeSpan.FromMinutes(10));
            manyLatency.Add(t.Elapsed.TotalMilliseconds);
            Assert.Equal(StoreStatus.Applied, reply.Status);
            t.Restart();
            Assert.Equal(StoreStatus.Applied, (await control.RenewAsync(leases[0], TimeSpan.FromMinutes(10))).Status);
            oneLatency.Add(t.Elapsed.TotalMilliseconds);
        }

        await stop.CancelAsync();
        await writer;
        static double P(List<double> l, double q) => l.Order().ElementAt(Math.Min(l.Count - 1, (int)(l.Count * q)));
        var report = string.Create(CultureInfo.InvariantCulture,
            $"REDIS-07 (non-reference host, shared and loaded; informational): background intent {writerShape.Value.Ops} after-images, {writerShape.Value.Keys} keys, {writerShape.Value.Bytes} bytes; maximal public DispatchMany {maximalShape.Value.Ops} after-images, {maximalShape.Value.Keys} keys, {maximalShape.Value.Bytes} bytes (accepted); {intents} background intents during {clock.Elapsed.TotalSeconds:F0} s; RenewMany(64) n={manyLatency.Count} p50={P(manyLatency, 0.5):F1} ms p99={P(manyLatency, 0.99):F1} ms max={manyLatency.Max():F1} ms; Renew n={oneLatency.Count} p50={P(oneLatency, 0.5):F1} ms p99={P(oneLatency, 0.99):F1} ms max={oneLatency.Max():F1} ms");
        Report("redis-07", report);

        Assert.True(intents >= 5, $"only {intents} large intents ran");
        Assert.True(manyLatency.Count >= 100);
        // The 250 ms bound is a reference-host requirement (22 §4.3); on any other host the numbers above are the evidence.
        if (Environment.GetEnvironmentVariable("RUNWICK_REFERENCE_HOST") == "1")
        {
            Assert.True(P(oneLatency, 0.99) <= 250 && P(manyLatency, 0.99) <= 250, report);
        }
    }
}
