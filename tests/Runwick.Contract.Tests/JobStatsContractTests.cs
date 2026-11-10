using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.ControlContractTests;

namespace Runwick.Contract.Tests;

/// <summary>
/// JST-01..04 (16 Changes 2026-11-10) on every store: GetJobStats counts exactly (both range ends, first and last bucket, groups,
/// live counts, error codes, worker and queue-coverage filters); ListJobTypes merges the registry with the stored job names;
/// RetryJobs reports each item as CancelJobs does; GetAuditStats counts operations and actors.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "JST")]
public sealed class JobStatsContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ManagementCaller Admin = new(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);

    static readonly IReadOnlySet<string> DefaultOnly = new HashSet<string>(StringComparer.Ordinal) { "default" };

    static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherQueueJob>().Named("test.other").ToQueue("other");
    }

    static IManagementSession Session(TestClient c) => c.Services.GetRequiredService<IManagementSession>();

    static async Task<Guid> DispatchAsync(TestClient c, IQueueJob job)
    {
        // Distinct store milliseconds, so the range-edge cases below separate every job.
        await Task.Delay(5, Ct);
        return (await c.Dispatcher.DispatchAsync(job, cancellationToken: Ct)).JobId.Value;
    }

    /// <summary>
    /// Six jobs: succeeded, failed ("boom"), retried once then succeeded ("flaky" first), cancelled in queue other, running,
    /// and pending, created in that order.
    /// </summary>
    static async Task<(Guid Ok, Guid Failed, Guid Retried, Guid Cancelled, Guid Running, Guid Pending)> SeedAsync(TestClient c)
    {
        var ok = await DispatchAsync(c, new PingJob("ok"));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(await RunAsync(c, ok), AttemptOutcome.Succeeded)).Status);
        var failed = await DispatchAsync(c, new PingJob("failed"));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(await RunAsync(c, failed), AttemptOutcome.PermanentFailure,
            error: new AttemptError("boom", "System.Exception", "m", null))).Status);
        var retried = await DispatchAsync(c, new PingJob("retried"));
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(await RunAsync(c, retried), AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(1),
            error: new AttemptError("flaky", "System.Exception", "m", null))).Status);
        StoreReply<ReservedJob> again;
        do
        {
            await Task.Delay(20, Ct);
            again = await c.ReserveAsync();
        }
        while (again.Status == StoreStatus.NoWork);

        Assert.Equal(retried, again.Value!.Position.JobId);
        await c.BeginAsync(again.Value.Lease.Handle);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(again.Value.Lease.Handle, AttemptOutcome.Succeeded)).Status);

        var cancelled = await DispatchAsync(c, new OtherQueueJob("cancelled"));
        var position = await PositionAsync(c, cancelled);
        Assert.True((await Session(c).CancelJobAsync(Admin, cancelled, new JobControlRequest(Guid.CreateVersion7(), position.Generation, position.Revision),
            Ct)).Succeeded);
        var running = await DispatchAsync(c, new PingJob("running"));
        await RunAsync(c, running);
        var pending = await DispatchAsync(c, new PingJob("pending"));
        return (ok, failed, retried, cancelled, running, pending);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "JST-01")]
    public async Task Job_stats_count_exactly_by_state_bucket_group_and_live_kind(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "jst01", Jobs);
        await using var _ = f;
        var jobs = await SeedAsync(c);
        var s = Session(c);
        var detail = new Dictionary<Guid, JobSummary>();
        foreach (var id in (Guid[])[jobs.Ok, jobs.Failed, jobs.Retried, jobs.Cancelled, jobs.Running, jobs.Pending])
        {
            detail[id] = (await s.GetJobAsync(Admin, id, Ct)).Job;
        }

        var stats = await s.GetJobStatsAsync(Admin, new JobStatsQuery(ByQueue: true, ByJobName: true), Ct);
        Assert.Equal(TimeSpan.FromDays(1), stats.To - stats.From);
        Assert.Equal((2L, 1L, 1L, 1L, 1L, 0L, 0L), (stats.States["succeeded"], stats.States["failed"], stats.States["cancelled"], stats.States["running"],
            stats.States["pending"], stats.States["reserved"], stats.States["retrying"]));
        Assert.Equal(1L, stats.Retries);
        Assert.Equal((1L, 0L, 1L, 0L), (stats.Now["pending"], stats.Now["scheduled"], stats.Now["running"], stats.Now["reserved"]));
        Assert.Equal(detail[jobs.Pending].EligibleAt, stats.OldestPendingEligibleAt);
        Assert.Equal(24, stats.Buckets.Count);
        Assert.Equal((6L, 2L, 1L, 1L), (stats.Buckets.Sum(b => b.Created), stats.Buckets.Sum(b => b.Succeeded), stats.Buckets.Sum(b => b.Failed),
            stats.Buckets.Sum(b => b.Cancelled)));
        Assert.Equal(3L, stats.Duration.Count); // the cancelled job never ran
        Assert.True(stats.Duration.MedianMs >= 0 && stats.Duration.P95Ms >= stats.Duration.MedianMs && stats.Duration.MaxMs >= stats.Duration.P95Ms);
        Assert.Equal([new JobErrorCount("boom", 1), new JobErrorCount("flaky", 1)], stats.ErrorCodes);

        var queues = stats.Queues!.ToDictionary(g => g.Key);
        Assert.Equal(["default", "other"], queues.Keys.Order(StringComparer.Ordinal));
        Assert.Equal((5L, 1L, 1L), (queues["default"].States.Values.Sum(), queues["other"].States.Values.Sum(), queues["other"].States["cancelled"]));
        Assert.Equal((jobs.Pending, "pending"), (queues["default"].LastJobId!.Value, queues["default"].LastState));
        Assert.Equal((1L, 1L, 0L), (queues["default"].Now["pending"], queues["default"].Now["running"], queues["other"].Now["pending"]));
        Assert.Equal((3L, 0L), (queues["default"].Duration.Count, queues["other"].Duration.Count));
        Assert.Null(queues["other"].OldestPendingEligibleAt);
        var names = stats.JobNames!.ToDictionary(g => g.Key);
        Assert.Equal((5L, 1L, 1L), (names["test.ping"].States.Values.Sum(), names["test.other"].States.Values.Sum(), names["test.ping"].Retries));
        Assert.Equal(1L, names["test.ping"].Buckets.Sum(b => b.Failed));

        // Filters: queue, job name, and the worker that ran an attempt (no groups then).
        Assert.Equal(1L, (await s.GetJobStatsAsync(Admin, new JobStatsQuery(Queue: "other"), Ct)).States.Values.Sum());
        Assert.Equal(5L, (await s.GetJobStatsAsync(Admin, new JobStatsQuery(JobName: "test.ping"), Ct)).States.Values.Sum());
        var worker = (await s.GetJobAsync(Admin, jobs.Ok, Ct)).Attempts[0].WorkerInstanceId;
        var byWorker = await s.GetJobStatsAsync(Admin, new JobStatsQuery(WorkerInstanceId: worker, ByQueue: true), Ct);
        Assert.Equal((4L, 0L, 1L), (byWorker.States.Values.Sum(), byWorker.Now["pending"], byWorker.Now["running"]));
        Assert.Null(byWorker.Queues);
        Assert.Equal(0L, (await s.GetJobStatsAsync(Admin, new JobStatsQuery(WorkerInstanceId: Guid.NewGuid()), Ct)).States.Values.Sum());

        // Jobs.Read on default only: other is not counted, not grouped, and reads as empty when asked for.
        var scoped = new ManagementCaller(new ManagementActor("scoped", null, "test"), [new ManagementGrant(ManagementPermissions.JobsRead, DefaultOnly)]);
        var mine = await s.GetJobStatsAsync(scoped, new JobStatsQuery(ByQueue: true), Ct);
        Assert.Equal((5L, 0L), (mine.States.Values.Sum(), mine.States["cancelled"]));
        Assert.Equal(["default"], mine.Queues!.Select(g => g.Key));
        Assert.Equal(["boom", "flaky"], mine.ErrorCodes.Select(e => e.Code));
        var hidden = await s.GetJobStatsAsync(scoped, new JobStatsQuery(Queue: "other"), Ct);
        Assert.Equal((0L, 0L), (hidden.States.Values.Sum(), hidden.Now.Values.Sum()));
        await Assert.ThrowsAsync<ManagementDeniedException>(() => s.GetJobStatsAsync(
            new ManagementCaller(new ManagementActor("none", null, "test"), [new ManagementGrant(ManagementPermissions.QueuesRead)]), new JobStatsQuery(), Ct));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "JST-02")]
    public async Task Job_stats_ranges_include_From_exclude_To_and_fill_the_first_and_last_bucket(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "jst02", Jobs);
        await using var _ = f;
        var jobs = await SeedAsync(c);
        var s = Session(c);
        var all = new List<JobSummary>();
        foreach (var id in (Guid[])[jobs.Ok, jobs.Failed, jobs.Retried, jobs.Cancelled, jobs.Running, jobs.Pending])
        {
            all.Add((await s.GetJobAsync(Admin, id, Ct)).Job);
        }

        var (first, last) = (all[0].CreatedAt, all[^1].CreatedAt);
        long Created(JobStats x) => x.States.Values.Sum();
        Assert.Equal(5L, Created(await s.GetJobStatsAsync(Admin, new JobStatsQuery(first, last), Ct)));
        Assert.Equal(4L, Created(await s.GetJobStatsAsync(Admin, new JobStatsQuery(first.AddMilliseconds(1), last), Ct)));
        Assert.Equal(6L, Created(await s.GetJobStatsAsync(Admin, new JobStatsQuery(first, last.AddMilliseconds(1)), Ct)));

        // 96 buckets over [first, last + 1 ms): the first job opens bucket 0, the last closes bucket 95, and every job lands in the
        // bucket whose start is the latest at or before its creation.
        var fine = await s.GetJobStatsAsync(Admin, new JobStatsQuery(first, last.AddMilliseconds(1), 96), Ct);
        Assert.Equal((96, first), (fine.Buckets.Count, fine.Buckets[0].From));
        Assert.True(fine.Buckets[0].Created >= 1 && fine.Buckets[^1].Created >= 1);
        var expected = new long[96];
        foreach (var j in all)
        {
            expected[fine.Buckets.Select((b, i) => (b, i)).Last(x => x.b.From <= j.CreatedAt).i]++;
        }

        Assert.Equal(expected, fine.Buckets.Select(b => b.Created));

        // Ended jobs count by end time, whenever created: the first to end is counted from its end and not up to it.
        var ended = all.Where(j => j.TerminalAt is not null).OrderBy(j => j.TerminalAt).ToList();
        var end = ended[0].TerminalAt!.Value;
        Assert.Equal(4L, (await s.GetJobStatsAsync(Admin, new JobStatsQuery(end, end.AddHours(1)), Ct)).Buckets.Sum(b => b.Succeeded + b.Failed + b.Cancelled));
        Assert.Equal(0L, (await s.GetJobStatsAsync(Admin, new JobStatsQuery(end.AddHours(-1), end), Ct)).Buckets.Sum(b => b.Succeeded + b.Failed + b.Cancelled));
        var one = await s.GetJobStatsAsync(Admin, new JobStatsQuery(end, end.AddMilliseconds(1), 1), Ct);
        Assert.Equal((1L, 1L), (one.Buckets[0].Succeeded + one.Buckets[0].Failed + one.Buckets[0].Cancelled, one.Duration.Count));
        Assert.Equal(one.Duration.MedianMs, one.Buckets[0].MedianMs);

        // A range before every job is empty but still sees the live jobs.
        var early = await s.GetJobStatsAsync(Admin, new JobStatsQuery(first.AddDays(-2), first.AddDays(-1)), Ct);
        Assert.Equal((0L, 1L, 1L), (Created(early), early.Now["pending"], early.Now["running"]));

        foreach (var bad in (JobStatsQuery[])[new(Buckets: 0), new(Buckets: 97), new(last, first), new(first, first), new(first.AddDays(-32), first)])
        {
            await Assert.ThrowsAsync<ManagementValidationException>(() => s.GetJobStatsAsync(Admin, bad, Ct));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "JST-03")]
    public async Task Job_types_merge_the_registry_with_stored_names_by_queue_coverage(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "jst03", Jobs);
        await using var _ = f;
        await DispatchAsync(c, new OtherQueueJob("o"));
        await DispatchAsync(c, new PingJob("p"));

        // This host registers test.ping (and the other test contracts) but not test.other.
        var host = await f.OpenClient(configure: r => TestJobs.Register(r));
        var types = (await Session(host).ListJobTypesAsync(Admin, Ct)).ToDictionary(t => t.Name);
        Assert.Equal(types.Keys.Order(StringComparer.Ordinal), types.Keys);
        var ping = types["test.ping"];
        Assert.Equal((true, "default", "medium", 1, true), (ping.Registered, ping.Queue, ping.Priority, ping.PayloadVersion, ping.SeenInStore));
        Assert.Equal(typeof(PingJob).FullName, ping.Definition!["payloadType"]!.GetValue<string>());
        Assert.Equal(3, ping.Definition["maxExecutions"]!.GetValue<int>());
        var keyed = types["test.keyed"];
        Assert.Equal((true, false, true), (keyed.Registered, keyed.SeenInStore, keyed.Definition!["unique"]!.GetValue<bool>()));
        var other = types["test.other"];
        Assert.Equal((false, null, null, true), (other.Registered, other.Queue, other.Definition, other.SeenInStore));
        Assert.Empty(other.Schedules);

        // Jobs.Read on default only: test.other's only queue is not covered.
        var scoped = new ManagementCaller(new ManagementActor("scoped", null, "test"), [new ManagementGrant(ManagementPermissions.JobsRead, DefaultOnly)]);
        Assert.DoesNotContain("test.other", (await Session(host).ListJobTypesAsync(scoped, Ct)).Select(t => t.Name));
        await Assert.ThrowsAsync<ManagementDeniedException>(() => Session(host).ListJobTypesAsync(
            new ManagementCaller(new ManagementActor("none", null, "test"), []), Ct));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "JST-04")]
    public async Task Bulk_retry_reports_each_item_and_audit_stats_count_operations_and_actors(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "jst04", Jobs);
        await using var _ = f;
        var jobs = await SeedAsync(c);
        var s = Session(c);
        var failed = await PositionAsync(c, jobs.Failed);
        var cancelled = await PositionAsync(c, jobs.Cancelled);
        var ok = await PositionAsync(c, jobs.Ok);
        var retryDefault = new ManagementCaller(new ManagementActor("ops", null, "test"),
            [new ManagementGrant(ManagementPermissions.JobsRead), new ManagementGrant(ManagementPermissions.JobsRetry, DefaultOnly)]);

        var results = await s.RetryJobsAsync(retryDefault,
        [
            new BulkRetryItem(jobs.Failed, Guid.CreateVersion7(), failed.Generation, failed.Revision),
            new BulkRetryItem(jobs.Ok, Guid.CreateVersion7(), ok.Generation, ok.Revision - 1),
            new BulkRetryItem(jobs.Cancelled, Guid.CreateVersion7(), cancelled.Generation, cancelled.Revision),
            new BulkRetryItem(Guid.CreateVersion7(), Guid.CreateVersion7(), 1, 1),
        ], "bulk", Ct);
        Assert.Equal(["applied", "rejected", "forbidden", "not_found"], results.Select(r => r.Status));
        Assert.Equal("pending", (await PositionAsync(c, jobs.Failed)).State);
        Assert.NotNull(results[1].Code);
        await Assert.ThrowsAsync<ManagementValidationException>(() => s.RetryJobsAsync(Admin, [], null, Ct));
        await Assert.ThrowsAsync<ManagementValidationException>(() => s.RetryJobsAsync(Admin,
            [.. Enumerable.Range(0, ManagementSession.MaxBulkItems + 1).Select(_ => new BulkRetryItem(Guid.CreateVersion7(), Guid.CreateVersion7(), 1, 1))], null, Ct));

        // Audit: as many rows as the audit list shows, among them the seed's cancel by admin and the one applied retry by ops.
        var audit = await s.GetAuditStatsAsync(Admin, cancellationToken: Ct);
        var listed = (await s.ListAuditAsync(Admin, new AuditQuery(Limit: 200), Ct)).Items;
        Assert.Equal(listed.Count, audit.Total);
        Assert.Equal([.. listed.GroupBy(a => a.Operation).Select(g => new AuditOperationCount(g.Key, g.LongCount()))
            .OrderByDescending(o => o.Count).ThenBy(o => o.Operation, StringComparer.Ordinal)], audit.Operations);
        Assert.Contains(new AuditOperationCount("ManualRetry", 1), audit.Operations);
        Assert.Contains(new AuditActorCount("ops", 1), audit.Actors);
        Assert.Contains(audit.Actors, a => a.Actor == "admin");
        Assert.Null(audit.Denied);
        Assert.Equal(0L, (await s.GetAuditStatsAsync(Admin, audit.From.AddDays(-2), audit.From.AddDays(-1), Ct)).Total);
        await Assert.ThrowsAsync<ManagementDeniedException>(() => s.GetAuditStatsAsync(retryDefault, cancellationToken: Ct));
    }

    public static TheoryData<StoreKind> RedisOnly => [.. CoreContractTests.Kinds.Where(r => r.Data == StoreKind.Redis)];

    [Theory]
    [MemberData(nameof(RedisOnly))]
    [Trait("Case", "JST-05")]
    public async Task Redis_builds_its_stats_indexes_for_jobs_written_before_it_kept_them(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "jst05", Jobs);
        await using var _ = f;
        await SeedAsync(c);
        var query = new JobStatsQuery(ByJobName: true);
        var before = await Session(c).GetJobStatsAsync(Admin, query, Ct);

        // As a store written by an older binary: no idx:terminal, no jobtypes counts, no walk cursor. A job dispatched then
        // is left to the walk, which counts it once.
        var db = (await RedisServer.AdminAsync()).GetDatabase();
        var p = Runwick.Redis.Store.RxClient.KeyPrefix(c.Application);
        await db.KeyDeleteAsync([p + "idx:terminal", p + "jobtypes"]);
        await db.HashDeleteAsync(p + "meta", "bf");
        await DispatchAsync(c, new PingJob("late"));

        var fresh = await f.OpenClient(configure: Jobs);
        var after = await Session(fresh).GetJobStatsAsync(Admin, query, Ct);
        Assert.Equal(before.States["pending"] + 1, after.States["pending"]);
        Assert.Equal(before.Buckets.Select(b => (b.Succeeded, b.Failed, b.Cancelled)), after.Buckets.Select(b => (b.Succeeded, b.Failed, b.Cancelled)));
        Assert.Equal(before.Duration, after.Duration);
        Assert.Equal("done", (string?)await db.HashGetAsync(p + "meta", "bf"));
        Assert.Equal(6L, (long)await db.HashGetAsync(p + "jobtypes", "default\0test.ping"));
        Assert.Equal(1L, (long)await db.HashGetAsync(p + "jobtypes", "other\0test.other"));
        Assert.True((await Session(fresh).ListJobTypesAsync(Admin, Ct)).Single(t => t.Name == "test.other").SeenInStore);
    }
}
