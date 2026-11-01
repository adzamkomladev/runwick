using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using Runwick.Serialization;

namespace Runwick.Postgres.Tests;

/// <summary>
/// driver-contract-suite:CFG-01 for the operation versions M3 added (15 §3, §4): every new version is in the manifest
/// of the revision that introduced it and in no earlier one, and every one of them is rejected before any write when
/// the store's schema revision is not the binary's (03 §10.1 G1). No silent downgrade: nothing commits.
/// </summary>
public sealed class OperationVersionTests
{
    static readonly ControlActor Actor = new("user:cfg", null, "host", "cfg-01");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>The revision that introduced each M3 operation version, as each M3 spec states it (08, 14, 10, 09, 13, 11).</summary>
    static readonly (long Revision, string Operation, int Version)[] M3Versions =
    [
        (2, "Cancel", 1), (2, "ManualRetry", 1), (2, "RerunSucceeded", 1), (2, "EditScopePolicy", 1), (2, "EventsSince", 1),
        (3, "ReceiveOutbox", 1),
        (4, "ActivateCapabilities", 1), (4, "EditRatePolicy", 1),
        (5, "ReadinessProbe", 2), (5, "ListUnsupportedBacklog", 1),
        (6, "RegisterSchedule", 1), (6, "MaterializeOccurrences", 1), (6, "SetScheduleState", 1), (6, "UpdateSchedule", 1),
        (6, "TriggerSchedule", 1), (6, "ResolveCatchUp", 1), (6, "RecordScheduleEvaluationFailure", 1), (6, "RevalidateSchedule", 1),
        (7, "SetPoolOverride", 1), (7, "EstimateDemand", 1), (7, "RecordScalingEvent", 1),
    ];

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CFG-01")]
    public async Task Each_m3_operation_version_is_listed_from_its_revision_and_never_before()
    {
        foreach (var (revision, operation, version) in M3Versions)
        {
            for (var r = 1L; r <= PgMigrator.CurrentRevision; r++)
            {
                var listed = PgMigrator.VersionsAt(r).TryGetValue(operation, out var versions) && versions.Contains(version);
                Assert.True(listed == (r >= revision), $"{operation}/{version} listed={listed} at revision {r}; introduced at {revision}");
            }
        }

        // The M1/M2 versions are unchanged: ReadinessProbe keeps version 1 beside 2, SetQueueState stays at 1 (public path, 08 §3).
        Assert.Equal([1, 2], PgMigrator.VersionsAt(PgMigrator.CurrentRevision)["ReadinessProbe"]);
        Assert.Equal([1], PgMigrator.VersionsAt(PgMigrator.CurrentRevision)["SetQueueState"]);

        // The installed store carries the current revision's manifest and digest.
        await using var f = await DriverFixture.StartAsync(Ns("cfg01m"));
        var manifest = (await DispatchSupport.TextAsync(f, "SELECT compatibility_manifest::text FROM runwick.store_metadata")).Single();
        var operations = System.Text.Json.Nodes.JsonNode.Parse(manifest)!["operations"]!.AsObject();
        foreach (var (_, operation, version) in M3Versions)
        {
            Assert.Contains(version, operations[operation]!.AsArray().Select(v => v!.GetValue<int>()));
        }

        Assert.Equal(Convert.ToHexStringLower(PgMigrator.Manifest().Digest),
            (await DispatchSupport.TextAsync(f, "SELECT encode(manifest_digest, 'hex') FROM runwick.store_metadata")).Single());
    }

    [Theory]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CFG-01")]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task Every_m3_operation_is_rejected_before_any_write_at_another_schema_revision(int offset)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cfg01g"));
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<TickJob>().Named("test.tick").ToQueue("default");
        });
        var ct = CancellationToken.None;

        // Valid requests, built while the store is at the binary's revision.
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Actor)), ct)).Status);
        var waiting = (await c.Dispatcher.DispatchAsync(new PingJob("w"))).JobId.Value;
        await c.Dispatcher.DispatchAsync(new ResourceJob("r", "r1"));
        var preparer = c.Services.GetRequiredService<DispatchPreparer>();
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration("nightly", new TickJob("n")) { Cron = "* * * * *" }, preparer, TzdbArtifact.Pinned);
        var cutoff = await c.Store.StoreNowAsync(ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await c.Store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest("nightly", definition, cutoff, first)), ct)).Status);
        var schedule = (await c.Store.ReadScheduleAsync(c.Application, "nightly", ct))!;
        var position = (await DispatchSupport.TextAsync(f, "SELECT generation || '/' || revision FROM runwick.jobs WHERE job_id = @j", ("j", waiting))).Single().Split('/');
        var (generation, revision) = (int.Parse(position[0], System.Globalization.CultureInfo.InvariantCulture), long.Parse(position[1], System.Globalization.CultureInfo.InvariantCulture));
        var prepared = preparer.Prepare(c.Runtime.StoreId, new PingJob("outbox"), null);
        var acceptUntil = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeMilliseconds());
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var delivery = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, acceptUntil.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);
        var receive = new StoreCommand<ReceiveOutboxRequest>(c.Application, Guid.CreateVersion7(),
            DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddSeconds(20).ToUnixTimeMilliseconds()),
            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), acceptUntil, delivery, prepared.StoreId));
        var instance = Guid.CreateVersion7();
        var contracts = c.Contracts;

        // Receipted operations: the reply must be the G1 rejection itself.
        var receipted = new (string Operation, Func<Task<(StoreStatus Status, string? Code)>> Call)[]
        {
            ("Cancel/1", () => Shape(c.Store.CancelAsync(c.Command(new CancelRequest(waiting, generation, revision, Actor)), ct))),
            ("ManualRetry/1", () => Shape(c.Store.ManualRetryAsync(c.Command(new RetryRequest(waiting, generation, revision, Actor)), ct))),
            ("RerunSucceeded/1", () => Shape(c.Store.RerunSucceededAsync(c.Command(new RetryRequest(waiting, generation, revision, Actor)), ct))),
            ("SetQueueState/1 (public)", () => Shape(c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, 1, Actor)), ct))),
            ("EditScopePolicy/1", () => Shape(c.Store.EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "r1", 2, 1, Actor)), ct))),
            ("ReceiveOutbox/1", () => Shape(c.Store.ReceiveOutboxAsync(receive, ct))),
            ("ActivateCapabilities/1", () => Shape(c.Store.ActivateCapabilitiesAsync(
                c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), ct))),
            ("EditRatePolicy/1", () => Shape(c.Store.EditRatePolicyAsync(c.Command(new RatePolicyRequest("resource", "r1", 2, 1000, 1, Actor)), ct))),
            ("RegisterSchedule/1", () => Shape(c.Store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest("other", definition, cutoff, first)), ct))),
            ("MaterializeOccurrences/1", () => Shape(c.Store.MaterializeOccurrencesAsync(c.Command(new MaterializeRequest(
                schedule.ScheduleId, schedule.DefinitionRevision, schedule.ControlRevision, schedule.CursorRevision, null, cutoff, [],
                new ScheduleCursor(cutoff, first, cutoff, null), [], false)), ct))),
            ("SetScheduleState/1", () => Shape(c.Store.SetScheduleStateAsync(c.Command(new ScheduleStateRequest("nightly", true, null, schedule.ControlRevision, Actor)), ct))),
            ("UpdateSchedule/1", () => Shape(c.Store.UpdateScheduleAsync(c.Command(new UpdateScheduleRequest(
                "nightly", definition, schedule.DefinitionRevision, cutoff, first, Actor)), ct))),
            ("TriggerSchedule/1", () => Shape(c.Store.TriggerScheduleAsync(c.Command(new TriggerScheduleRequest(
                "nightly", schedule.DefinitionRevision, Guid.CreateVersion7(), Guid.CreateVersion7(), Actor)), ct))),
            ("ResolveCatchUp/1", () => Shape(c.Store.ResolveCatchUpAsync(c.Command(new ResolveCatchUpRequest(
                "nightly", Guid.CreateVersion7(), "skip", 0, null, Guid.CreateVersion7(), Actor)), ct))),
            ("RecordScheduleEvaluationFailure/1", () => Shape(c.Store.RecordEvaluationFailureAsync(c.Command(new EvaluationFailureRequest(
                schedule.ScheduleId, schedule.DefinitionRevision, "schedule_budget_elapsed", null, Actor)), ct))),
            ("RevalidateSchedule/1", () => Shape(c.Store.RevalidateScheduleAsync(c.Command(new RevalidateScheduleRequest("nightly", schedule.DefinitionRevision, Actor)), ct))),
            ("SetPoolOverride/1", () => Shape(c.Store.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 0, Actor)), ct))),
        };

        // Unreceipted reads: they must fail as incompatible, never return data or write.
        var unreceipted = new (string Operation, Func<Task> Call)[]
        {
            ("EventsSince/1", () => c.Store.EventsSinceAsync(c.Application, null, 10, ct)),
            ("ReadinessProbe/2", () => c.Store.ProbeAsync(c.Application, [new LaneKey("default", "medium")], contracts, ct)),
            ("ListUnsupportedBacklog/1", () => c.Store.ListUnsupportedBacklogAsync(c.Application, 4, ct)),
            ("EstimateDemand/1", () => c.Store.EstimateDemandAsync(c.Application, instance, ["default"], contracts, ct)),
        };

        await f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = @r", ("r", PgMigrator.CurrentRevision + offset));
        var before = await f.ReadCommittedSnapshot();

        foreach (var (operation, call) in receipted)
        {
            var shape = await call();
            Assert.True((StoreStatus.UnsupportedVersion, "schema_incompatible") == shape, $"{operation}: {shape}");
        }

        foreach (var (operation, call) in unreceipted)
        {
            var error = await Record.ExceptionAsync(call);
            Assert.True(error is StoreIncompatibleException or StoreNotAppliedException, $"{operation}: {error?.GetType().Name ?? "no error"} {error?.Message}");
        }

        // RecordScalingEvent/1 is unreceipted best-effort history (11 §6): a rejection is a false return, not an exception.
        Assert.False(await c.Store.RecordScalingEventAsync(c.Application, new ScalingEventRecord(instance, "main", 4, 5, 5, 5, "grow", null, null), ct));

        Assert.Empty(before.DiffTables(await f.ReadCommittedSnapshot()));

        // Back at the binary's revision the same requests are admitted again (the rejection was the gate, not the request).
        await f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = @r", ("r", PgMigrator.CurrentRevision));
        Assert.Equal(StoreStatus.Applied, (await receipted[0].Call()).Status);
        Assert.Equal(StoreStatus.Applied, (await receipted[5].Call()).Status);
        await c.Store.EventsSinceAsync(c.Application, null, 10, ct);
        Assert.True(await c.Store.RecordScalingEventAsync(c.Application, new ScalingEventRecord(instance, "main", 4, 5, 5, 5, "grow", null, null), ct));
        Assert.Empty(await f.HistoryViolations());
    }

    static async Task<(StoreStatus Status, string? Code)> Shape<T>(Task<StoreReply<T>> call) where T : class
    {
        var reply = await call;
        return (reply.Status, reply.Error?.Code);
    }
}
