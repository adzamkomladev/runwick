using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

public sealed record RedisTickJob(string Name) : IQueueJob;

/// <summary>Redis-only cases of 22 §11 for schedules (REDIS-01 write prefixes) and retention (REDIS-13).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
[Collection(nameof(RedisServerState))]
public sealed class RedisSchedulesRetentionTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ControlActor Actor = new("user:ops", null, "host", "redis");

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<RedisTickJob>().Named("test.redis.tick").ToQueue("default");
        });
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        await db.HashSetAsync(RxClient.Meta(f.Application), "caps", "{\"scheduler\":1}");
        return (f, c);
    }

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    static async Task<ScheduleSnapshot> RegisterAsync(TestClient c, string key)
    {
        var d = new ScheduleDeclaration(key, new RedisTickJob(key)) { Cron = "* * * * *" };
        var definition = ScheduleCompiler.Compile(d, c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
        var store = Store(c);
        var cutoff = await store.StoreNowAsync(Ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), Ct)).Status);
        return (await store.ReadScheduleAsync(c.Application, key, Ct))!;
    }

    static async Task<long> JobCount(DriverFixture f) =>
        await (await RedisServer.AdminAsync()).GetDatabase().SortedSetLengthAsync(RxClient.KeyPrefix(f.Application) + "idx:created");

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Every_write_prefix_of_a_materialization_recovers_and_the_resend_applies_it_once()
    {
        var (f, c) = await OpenAsync("redis01s");
        await using var _ = f;
        var store = Store(c);
        var interrupted = 0;
        long jobs = 0;
        foreach (var k in new[] { 1, 2, 3, 5, 8, 13, 21, 34, 55 })
        {
            var s = await RegisterAsync(c, "tick" + k);
            var cutoff = await store.StoreNowAsync(Ct);
            var occurrences = Enumerable.Range(1, 3)
                .Select(i => new PlannedOccurrence(s.EvaluatedThrough!.Value.AddMilliseconds(i), Guid.CreateVersion7())).ToList();
            var plan = new MaterializeRequest(s.ScheduleId, s.DefinitionRevision, s.ControlRevision, s.CursorRevision, null, cutoff, occurrences,
                new ScheduleCursor(occurrences[^1].ScheduledAt, cutoff.AddMinutes(1), cutoff.AddMinutes(1), null), [], false);
            var command = c.Command(plan);

            store.Runner.Client.FailAfter = k;
            StoreReply<ScheduleMaterialization>? first = null;
            try
            {
                first = await store.MaterializeOccurrencesAsync(command, Ct);
            }
            catch (StoreOutcomeUnknownException)
            {
                interrupted++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            // The resend publishes any pending intent first and then answers from the receipt: applied exactly once.
            var again = await store.MaterializeOccurrencesAsync(command, Ct);
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal(occurrences.Select(o => (Guid?)o.ProposedJobId), again.Value!.Occurrences.Select(o => o.JobId));
            if (first is not null)
            {
                Assert.Equal(first.Value!.Occurrences, again.Value.Occurrences);
            }

            jobs += 3;
            Assert.Equal(jobs, await JobCount(f));
            Assert.Equal(s.CursorRevision + 1, (await store.ReadScheduleAsync(c.Application, s.Key, Ct))!.CursorRevision);
        }

        Assert.True(interrupted >= 4, $"only {interrupted} interrupted calls");
        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(ReferenceModel.SnapshotInvariants.Violations(snapshot.Model));
    }

    [Fact]
    [Trait("Case", "REDIS-13")]
    public async Task Cleanup_intents_stay_within_the_limits_and_a_full_pass_leaves_no_key_with_a_TTL()
    {
        var (f, c) = await OpenAsync("redis13r");
        await using var _ = f;
        const int N = 130;
        for (var i = 0; i < N; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct);
            var reserved = await c.ReserveAsync();
            var lease = reserved.Value!.Lease.Handle;
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
            var error = i % 2 == 0 ? new AttemptError("boom", "System.Exception", "secret detail " + i, null) : null;
            Assert.Equal(StoreStatus.Applied,
                (await c.FinishAsync(lease, error is null ? AttemptOutcome.Succeeded : AttemptOutcome.PermanentFailure, error: error)).Status);
        }

        var mux = await RedisServer.AdminAsync();
        var db = mux.GetDatabase();
        var prefix = RxClient.KeyPrefix(f.Application);
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        await foreach (var k in server.KeysAsync(db.Database, prefix + "job:*"))
        {
            var at = long.Parse((string)(await db.HashGetAsync(k, "terminalat"))!, CultureInfo.InvariantCulture);
            await db.HashSetAsync(k, "terminalat", at - (long)TimeSpan.FromDays(400).TotalMilliseconds);
        }

        var retention = (IRetentionStore)c.Driver;
        var limits = new CleanupLimits(1_000, 1 << 20, TimeSpan.FromSeconds(5));
        var full = 0;
        for (var round = 0; round < 2; round++)
        {
            foreach (var recordClass in RecordClasses.All)
            {
                for (var i = 0; i < 100; i++)
                {
                    var r = await retention.RunCleanupBatchAsync(c.Application, recordClass, limits, Ct);
                    // A batch whose intent broke 22 §4.3 would come back failed (intent_too_many_keys and friends).
                    Assert.True(r.Status == "done", $"{recordClass}: {r.Status} {r.ErrorCode}");
                    full += r.Rows == 100 ? 1 : 0;
                    if (r.PassComplete)
                    {
                        break;
                    }

                    Assert.True(i < 99, "pass did not complete");
                }
            }
        }

        Assert.True(full >= 1, "the row limit of 100 should have shortened the job history batch");
        Assert.Equal(0, await JobCount(f));
        Assert.Equal("", (string?)await db.HashGetAsync(RxClient.Meta(f.Application), "guard") ?? "");
        var keys = new List<RedisKey>();
        await foreach (var k in server.KeysAsync(db.Database, prefix + "*"))
        {
            keys.Add(k);
        }

        Assert.NotEmpty(keys);
        Assert.DoesNotContain(keys, k => ((string)k!).Contains(":job:", StringComparison.Ordinal) || ((string)k!).Contains(":pay:", StringComparison.Ordinal));
        foreach (var k in keys)
        {
            Assert.Null(await db.KeyTimeToLiveAsync(k));
        }
    }
}
