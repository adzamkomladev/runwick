using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.ControlContractTests;

namespace Runwick.Contract.Tests;

/// <summary>
/// WDT-01..02 (16 Changes 2026-11-10) on every store: the worker detail published with presence comes back from ListWorkers
/// (null when a publication carried none), and its running and recent attempts are cut to the queues the caller may read jobs in.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "WDT")]
public sealed class WorkerDetailContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ManagementCaller Admin = new(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);

    static ManagementCaller Caller(params ManagementGrant[] grants) => new(new ManagementActor("scoped", null, "test"), grants);

    static readonly IReadOnlySet<string> DefaultOnly = new HashSet<string>(StringComparer.Ordinal) { "default" };

    static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherQueueJob>().Named("test.other").ToQueue("other");
    }

    static IManagementSession Session(TestClient c) => c.Services.GetRequiredService<IManagementSession>();

    static PresenceRecord Presence(TestClient c, Guid instance, long sequence, JsonObject? detail) =>
        new(instance, sequence, 1, "ready", Environment.ProcessId, "host", "build", ["default"], new Dictionary<string, int> { ["main"] = 1 },
            c.Contracts, TimeSpan.FromSeconds(30)) { Detail = detail };

    static JsonObject Detail() => new()
    {
        ["v"] = 1,
        ["host"] = new JsonObject { ["os"] = "test-os", ["cpus"] = 8, ["memoryTotalBytes"] = 17_179_869_184L, ["processCpuPercent"] = 3.5 },
        ["pools"] = new JsonArray(new JsonObject { ["name"] = "main", ["target"] = 4, ["inFlight"] = 2 }),
        ["settings"] = new JsonObject { ["leaseDurationMs"] = 60_000, ["recordResourceUsage"] = true },
        ["running"] = new JsonArray(new JsonObject { ["jobId"] = "a", ["queue"] = "default" }, new JsonObject { ["jobId"] = "b", ["queue"] = "other" }),
        ["runningCount"] = 2,
        ["recent"] = new JsonArray(new JsonObject { ["jobId"] = "c", ["queue"] = "other", ["outcome"] = "succeeded" }),
    };

    static async Task<WorkerSummary> WorkerAsync(TestClient c, ManagementCaller caller, Guid instance) =>
        (await Session(c).ListWorkersAsync(caller, cancellationToken: Ct)).Items.Single(w => w.InstanceId == instance);

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "WDT-01")]
    public async Task Presence_detail_round_trips_and_a_publication_without_one_reads_as_null(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "wdt01", Jobs);
        await using var _ = f;
        var with = Guid.CreateVersion7();
        var without = Guid.CreateVersion7();
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, Presence(c, with, 1, Detail()), Ct)).Accepted);
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, Presence(c, without, 1, null), Ct)).Accepted);

        Assert.True(JsonNode.DeepEquals(Detail(), (await WorkerAsync(c, Admin, with)).Detail));
        Assert.Null((await WorkerAsync(c, Admin, without)).Detail);

        // A later publication replaces the detail; one without detail, or with one over 16 KiB, clears it (the presence still applies).
        var next = Detail();
        next["runningCount"] = 0;
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, Presence(c, with, 2, next), Ct)).Accepted);
        Assert.Equal(0, (await WorkerAsync(c, Admin, with)).Detail!["runningCount"]!.GetValue<int>());
        var huge = Detail();
        huge["pad"] = new string('x', PresenceRecord.MaxDetailBytes);
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, Presence(c, with, 3, huge), Ct)).Accepted);
        Assert.Null((await WorkerAsync(c, Admin, with)).Detail);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "WDT-02")]
    public async Task Running_and_recent_attempts_are_listed_only_for_queues_the_caller_may_read(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "wdt02", Jobs);
        await using var _ = f;
        var id = Guid.CreateVersion7();
        Assert.True((await c.Driver.PublishPresenceAsync(c.Application, Presence(c, id, 1, Detail()), Ct)).Accepted);

        var scoped = (await WorkerAsync(c, Caller(new(ManagementPermissions.CapacityRead), new(ManagementPermissions.JobsRead, DefaultOnly)), id)).Detail!;
        Assert.Equal(["a"], scoped["running"]!.AsArray().Select(x => x!["jobId"]!.GetValue<string>()));
        Assert.Empty(scoped["recent"]!.AsArray());
        Assert.Equal(2, scoped["runningCount"]!.GetValue<int>());

        var capacityOnly = (await WorkerAsync(c, Caller(new ManagementGrant(ManagementPermissions.CapacityRead)), id)).Detail!.AsObject();
        Assert.False(capacityOnly.ContainsKey("running"));
        Assert.False(capacityOnly.ContainsKey("recent"));
        Assert.Equal("test-os", capacityOnly["host"]!["os"]!.GetValue<string>());
        Assert.Equal(60_000, capacityOnly["settings"]!["leaseDurationMs"]!.GetValue<int>());

        Assert.Equal(2, (await WorkerAsync(c, Admin, id)).Detail!["running"]!.AsArray().Count);
    }
}
