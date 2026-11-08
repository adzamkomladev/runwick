using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.ReferenceModel;
using StackExchange.Redis;

namespace Runwick.Redis.Tests;

public sealed record OtherQueueJob(string Id) : IQueueJob;

/// <summary>Redis-only cases for controls, audit, disclosure, events and the bounded management projections (22 §7.1, §7.2).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-025")]
public sealed class RedisControlTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ControlActor Actor = new("user:42", "Ada", "host", "test");

    static readonly ManagementCaller Admin = new(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);

    static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherQueueJob>().Named("test.other").ToQueue("other");
    }

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        return (f, await f.OpenClient(configure: Jobs));
    }

    static RedisStore Store(TestClient c) => c.Services.GetRequiredService<RedisStore>();

    static IJobControlStore Controls(TestClient c) => c.Services.GetRequiredService<IJobControlStore>();

    static IManagementQueryStore Reads(TestClient c) => c.Services.GetRequiredService<IManagementQueryStore>();

    static async Task<JobPosition> PositionAsync(TestClient c, Guid id)
    {
        var j = (await Reads(c).GetJobAsync(c.Application, id, Ct)).Value!.Job;
        return new JobPosition(id, j.Generation, j.Revision, j.State);
    }

    static Task<long> Count(DriverFixture f, string sql, params (string, object)[] p) => f.CountAsync(sql, p);

    // ------------------------------------------------------------------ REDIS-01 form: every write prefix of a control

    [Fact]
    [Trait("Case", "REDIS-01")]
    public async Task Every_write_prefix_of_a_control_resends_to_one_effect_one_audit_row_and_one_receipt()
    {
        var (f, c) = await OpenAsync("redis01ctl");
        await using var _ = f;
        var store = Store(c);
        var interrupted = 0;
        for (var n = 1; n <= 30; n++)
        {
            var job = (await c.Dispatcher.DispatchAsync(new KeyedJob("c" + n, "k" + n), cancellationToken: Ct)).JobId.Value;
            var cmd = c.Command(new CancelRequest(job, 1, (await PositionAsync(c, job)).Revision, Actor));
            store.Runner.Client.FailAfter = n;
            try
            {
                await Controls(c).CancelAsync(cmd, Ct);
            }
            catch (StoreOutcomeUnknownException)
            {
                interrupted++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            var again = await Controls(c).CancelAsync(cmd, Ct);
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal(("cancelled", "cancelled_while_waiting"), (again.Value!.Effect, again.Value.TerminalReason));
            Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", cmd.CommandId)));
            Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", cmd.CommandId)));
        }

        Assert.True(interrupted > 5, $"only {interrupted} interrupted calls");
        Assert.Equal(30, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'job_cancelled'"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Fact]
    [Trait("Case", "REDIS-12")]
    public async Task A_disclosure_whose_write_fails_returns_nothing()
    {
        var (f, c) = await OpenAsync("redis12disc");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("secret"), cancellationToken: Ct)).JobId.Value;
        var request = new DisclosureRequest("RevealPayload", job, null, Actor);
        Store(c).Runner.Client.FailAfter = 2;
        try
        {
            await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => Reads(c).RecordDisclosureAsync(c.Command(request), Ct));
        }
        finally
        {
            Store(c).Runner.Client.FailAfter = 0;
        }

        // The stored decision is published by the next writer: one audit row, and the data only through a fresh reveal.
        var ok = await Reads(c).RecordDisclosureAsync(c.Command(request), Ct);
        Assert.Equal("disclosed", ok.Value!.Status);
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'RevealPayload'"));
    }

    // ------------------------------------------------------------------ REDIS-12: events and projections

    [Fact]
    [Trait("Case", "REDIS-12")]
    public async Task Three_writers_and_a_tailing_reader_see_every_event_once_while_old_events_are_deleted()
    {
        var (f, c) = await OpenAsync("redis12ev");
        await using var _ = f;
        var writers = new[] { c, await f.OpenClient(configure: Jobs), await f.OpenClient(configure: Jobs) };
        var admin = await RedisServer.AdminAsync();
        var db = admin.GetDatabase();
        var prefix = RxClient.KeyPrefix(f.Application);
        var seen = new List<long>();
        var done = false;
        var reader = Task.Run(async () =>
        {
            EventCursor? cursor = null;
            while (true)
            {
                var stop = Volatile.Read(ref done);
                var page = await Controls(c).EventsSinceAsync(c.Application, cursor, 7, Ct);
                seen.AddRange(page.Events.Select(e => e.EventId));
                cursor = page.Next;
                if (page.Events.Count > 0)
                {
                    // Cleanup of events already read (17): the reader's position is unaffected.
                    var old = page.Events[0].EventId;
                    await db.SortedSetRemoveRangeByScoreAsync(prefix + "events", 0, old - 1);
                }
                else if (stop)
                {
                    return;
                }
                else
                {
                    await Task.Delay(5, Ct);
                }
            }
        }, Ct);
        await Task.WhenAll(writers.Select((w, i) => Task.Run(async () =>
        {
            for (var k = 0; k < 25; k++)
            {
                await w.Dispatcher.DispatchAsync(new PingJob($"w{i}-{k}"), cancellationToken: Ct);
            }
        }, Ct)));
        Volatile.Write(ref done, true);
        await reader;

        Assert.Equal(75, seen.Count);
        Assert.Equal(seen.Distinct().Count(), seen.Count);
        Assert.Equal(seen.Order(), seen);
        Assert.Equal(Enumerable.Range((int)seen[0], 75).Select(i => (long)i), seen); // no gaps
    }

    [Fact]
    [Trait("Case", "REDIS-12")]
    public async Task ListJobs_pages_match_a_reference_filter_for_every_filter_combination_and_over_budget_is_a_timeout()
    {
        var (f, c) = await OpenAsync("redis12list");
        await using var _ = f;
        for (var i = 0; i < 9; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct);
        }

        for (var i = 0; i < 5; i++)
        {
            await c.Dispatcher.DispatchAsync(new OtherQueueJob("o" + i), cancellationToken: Ct);
        }

        // Some succeed, one retries, two are cancelled.
        for (var i = 0; i < 3; i++)
        {
            var reserved = await c.ReserveAsync();
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(reserved.Value!.Lease.Handle)).Reply.Status);
            await c.FinishAsync(reserved.Value.Lease.Handle, i == 0 ? AttemptOutcome.RetryableFailure : AttemptOutcome.Succeeded,
                requestedDelay: i == 0 ? TimeSpan.FromMinutes(5) : null);
        }

        var all = (await Reads(c).ListJobsAsync(c.Application, null, new JobListFilter(null, null, null, DateTimeOffset.UnixEpoch, DateTimeOffset.UtcNow.AddDays(1)), null, 100, Ct)).Value;
        foreach (var victim in all.Where(j => j.State == JobStates.Pending).Take(2))
        {
            await Controls(c).CancelAsync(c.Command(new CancelRequest(victim.JobId, victim.Generation, victim.Revision, Actor)), Ct);
        }

        all = (await Reads(c).ListJobsAsync(c.Application, null, new JobListFilter(null, null, null, DateTimeOffset.UnixEpoch, DateTimeOffset.UtcNow.AddDays(1)), null, 100, Ct)).Value;
        Assert.Equal(14, all.Count);
        Assert.Equal(all.OrderByDescending(j => j.CreatedAt).ThenByDescending(j => j.JobId).Select(j => j.JobId), all.Select(j => j.JobId));

        foreach (var queue in new string?[] { null, "default", "other" })
        {
            foreach (var state in new string?[] { null, JobStates.Pending, JobStates.Succeeded, JobStates.Retrying, JobStates.Cancelled })
            {
                foreach (var name in new string?[] { null, "test.ping", "test.other" })
                {
                    var expected = all.Where(j => (queue is null || j.Queue == queue) && (state is null || j.State == state)
                        && (name is null || j.JobName == name)).Select(j => j.JobId).ToList();
                    var got = new List<Guid>();
                    (DateTimeOffset, Guid)? cursor = null;
                    while (true)
                    {
                        var page = (await Reads(c).ListJobsAsync(c.Application, null,
                            new JobListFilter(queue, state, name, DateTimeOffset.UnixEpoch, DateTimeOffset.UtcNow.AddDays(1)), cursor, 3, Ct)).Value;
                        got.AddRange(page.Select(j => j.JobId));
                        if (page.Count < 3)
                        {
                            break;
                        }

                        cursor = (page[^1].CreatedAt, page[^1].JobId);
                    }

                    Assert.Equal(expected, got);
                }
            }
        }

        // A caller restricted to one queue never sees another (the check is per index entry).
        var scoped = (await Reads(c).ListJobsAsync(c.Application, ["other"], new JobListFilter(null, null, null, DateTimeOffset.UnixEpoch, DateTimeOffset.UtcNow.AddDays(1)), null, 100, Ct)).Value;
        Assert.Equal(5, scoped.Count);

        // More than 5,000 index entries to examine: a statement timeout (the session's query_budget_exceeded), never a partial page.
        var mux = await RedisServer.AdminAsync();
        var key = RxClient.KeyPrefix(f.Application) + "idx:created";
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await mux.GetDatabase().SortedSetAddAsync(key, [.. Enumerable.Range(0, 5100).Select(i => new SortedSetEntry("ff" + i.ToString("x30"), now))]);
        var timeout = await Assert.ThrowsAsync<StoreNotAppliedException>(() => Reads(c).ListJobsAsync(c.Application, null,
            new JobListFilter(null, null, "no.such.job", DateTimeOffset.UnixEpoch, DateTimeOffset.UtcNow.AddDays(1)), null, 10, Ct));
        Assert.Equal("statement_timeout", timeout.Code);
        await Assert.ThrowsAsync<ManagementQueryBudgetException>(() => c.Services.GetRequiredService<IManagementSession>()
            .ListJobsAsync(Admin, new JobListQuery(JobName: "no.such.job"), Ct));
    }
}
