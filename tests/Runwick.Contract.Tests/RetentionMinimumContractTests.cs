using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.RetentionContractTests;

namespace Runwick.Contract.Tests;

/// <summary>
/// OBS-10 (17 §8) on SQLite and Redis, ported from Runwick.Postgres.Tests.RetentionTests: with the minimum policy and many passes,
/// cleanup never deletes the fixed records still in force (live window keys, rate grants, receipts, live jobs) and changes no table
/// outside the deletable set.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class RetentionMinimumContractTests
{
    public static TheoryData<StoreKind> NonPostgres => UpgradeContractTests.NonPostgresKinds;

    static readonly ControlActor Actor = new("user:42", "Ada", "host", "retention");

    /// <summary>Everything the policy and the fixed rules may delete; every other table must be unchanged by cleanup.</summary>
    static readonly HashSet<string> Deletable = new(StringComparer.Ordinal)
    {
        "jobs", "job_payloads", "job_generations", "job_attempts", "job_concurrency_requirements", "dispatch_keys", "storage_commands",
        "outbox_deliveries", "schedule_occurrences", "schedule_audit_ranges", "scaling_events", "queue_events", "rate_grants", "management_audit",
        "worker_instances", "cleanup_progress",
        "job_rate_requirements", // SQLite: the purged job's own rows, deleted with it
    };

    static readonly HashSet<string> RedisFixed = new(StringComparer.Ordinal) { "scopes", "queues", "pools", "ratescopes", "permits" };

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(NonPostgres))]
    [Trait("Case", "OBS-10")]
    public async Task Obs10_the_minimum_policy_never_deletes_fixed_records_still_in_force(StoreKind kind)
    {
        var f = await DriverFixture.StartAsync("obs10_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var _ = f;
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<RetentionWindowJob>().Named("test.window").ToQueue("default").UniqueWithinWindow(j => j.Key, TimeSpan.FromMinutes(10));
            r.AddJob<RatedJob>().Named("test.rated").ToQueue("default").RollingWindow(100, TimeSpan.FromMinutes(1));
        });
        Assert.Equal(StoreStatus.Applied, (await RateScalingContractTests.Rates(c).ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), Ct)).Status);

        var minimum = new RetentionPolicy(TimeSpan.FromHours(1), TimeSpan.FromHours(1), TimeSpan.FromHours(1), TimeSpan.FromDays(7), TimeSpan.FromHours(1),
            TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        var retention = Retention(c);
        var refused = await retention.EditRetentionPolicyAsync(c.Command(new RetentionPolicyRequest(minimum with { JobHistory = TimeSpan.FromMinutes(30) }, 0, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidRequest, "invalid_retention_policy"), (refused.Status, refused.Error?.Code));
        Assert.Equal(StoreStatus.Applied, (await retention.EditRetentionPolicyAsync(c.Command(new RetentionPolicyRequest(minimum, 0, Actor)), Ct)).Status);

        var plain = await TerminalAsync(c, new PingJob("plain"));
        var windowed = await TerminalAsync(c, new RetentionWindowJob("w", "k"), AttemptOutcome.Succeeded);
        var rated = await TerminalAsync(c, new RatedJob("r"), AttemptOutcome.Succeeded);
        var active = (await c.Dispatcher.DispatchAsync(new KeyedJob("active", "key"), cancellationToken: Ct)).JobId.Value;
        await AgeJobsAsync(f, TimeSpan.FromHours(2));
        var receipts = await f.CountAsync(kind == StoreKind.Sqlite ? "SELECT count(*) FROM runwick_storage_commands" : "SELECT count(*) FROM runwick.storage_commands");
        var before = await f.ReadCommittedSnapshot();

        // OBS-10: many passes over the same fixture.
        for (var pass = 0; pass < 5; pass++)
        {
            await FullPassAsync(c);
        }

        Assert.Equal(0, await Exists(f, plain));
        Assert.Equal(0, await Exists(f, rated));
        Assert.Equal((1, 1L), (await Exists(f, windowed), await Expired(f, windowed)));
        Assert.Equal(1, await Exists(f, active));
        Assert.Equal(receipts, await f.CountAsync(kind == StoreKind.Sqlite ? "SELECT count(*) FROM runwick_storage_commands" : "SELECT count(*) FROM runwick.storage_commands"));
        Assert.Equal(2, await CountAsync(f, $"SELECT count(*) FROM {T(f, "dispatch_keys")}"));
        Assert.Equal(1, await RateScalingContractTests.Grants(f));
        Assert.DoesNotContain(before.DiffTables(await f.ReadCommittedSnapshot()), t => kind == StoreKind.Redis ? RedisFixed.Contains(t) : !Deletable.Contains(t));
        Assert.Equal(1, await CountAsync(f, $"SELECT count(*) FROM {T(f, "management_audit")} WHERE operation = 'EditRetentionPolicy'"));
    }
}
