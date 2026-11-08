using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using static Runwick.Contract.Tests.Cx;

namespace Runwick.Contract.Tests;

/// <summary>
/// CFG-01 (07 §9, 03 §10.1): the store carries the manifest and digest of the binary's schema revision, and when its stored revision is not the binary's,
/// every operation is rejected before any write (no silent downgrade). Ported from the PostgreSQL suite as theories over <see cref="StoreKind"/>;
/// the PostgreSQL case that also pins which revision introduced each operation version stays PostgreSQL-only (it reads that driver's migration history).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class ConfigContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    public static TheoryData<StoreKind, int> KindsAndOffsets =>
        [.. CoreContractTests.Kinds.Select(k => (StoreKind)k.Data).SelectMany(k => new[] { -1, 1 }.Select(o => (k, o)))];

    static readonly ControlActor Actor = new("user:cfg", null, "host", "cfg-01");

    /// <summary>Operation versions every driver's installed manifest lists (the M3 set of 15 §3, §4).</summary>
    static readonly (string Operation, int Version)[] Operations =
    [
        ("Cancel", 1), ("ManualRetry", 1), ("RerunSucceeded", 1), ("EditScopePolicy", 1), ("EventsSince", 1), ("ReceiveOutbox", 1),
        ("ActivateCapabilities", 1), ("EditRatePolicy", 1), ("ReadinessProbe", 2), ("ListUnsupportedBacklog", 1), ("RegisterSchedule", 1),
        ("MaterializeOccurrences", 1), ("SetScheduleState", 1), ("UpdateSchedule", 1), ("TriggerSchedule", 1), ("ResolveCatchUp", 1),
        ("RecordScheduleEvaluationFailure", 1), ("RevalidateSchedule", 1), ("SetPoolOverride", 1), ("EstimateDemand", 1), ("RecordScalingEvent", 1),
        ("RenewMany", 1), ("ReserveAndBegin", 1), ("FinishMany", 1),
    ];

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CFG-01")]
    public async Task The_installed_store_carries_the_binarys_manifest_and_every_operation_version(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cfg01m"), kind);
        string manifest;
        string digest;
        string expectedDigest;
        switch (kind)
        {
            case StoreKind.Postgres:
                manifest = (await DispatchTextAsync(f, "SELECT compatibility_manifest::text FROM runwick.store_metadata")).Single();
                digest = (await DispatchTextAsync(f, "SELECT encode(manifest_digest, 'hex') FROM runwick.store_metadata")).Single();
                expectedDigest = Convert.ToHexStringLower(PgMigrator.Manifest().Digest);
                break;
            case StoreKind.Sqlite:
                manifest = SqliteScalar(f, "SELECT compatibility_manifest FROM runwick_store_metadata");
                digest = SqliteScalar(f, "SELECT lower(hex(manifest_digest)) FROM runwick_store_metadata");
                expectedDigest = Convert.ToHexStringLower(Runwick.Sqlite.Migrations.SqliteMigrator.Manifest().Digest);
                break;
            default:
                var store = await RedisAdmin.StoreHash();
                manifest = store["manifest"];
                digest = store["digest"];
                expectedDigest = Convert.ToHexStringLower(Runwick.Redis.RunwickRedisMigrations.Manifest().Digest);
                break;
        }

        var operations = JsonNode.Parse(manifest)!["operations"]!.AsObject();
        foreach (var (operation, version) in Operations)
        {
            Assert.Contains(version, operations[operation]!.AsArray().Select(v => v!.GetValue<int>()));
        }

        Assert.Equal(expectedDigest, digest);
    }

    static async Task<List<string>> DispatchTextAsync(DriverFixture f, string sql)
    {
        await using var conn = new Npgsql.NpgsqlConnection(f.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand(sql, conn);
        var list = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)!);
        }

        return list;
    }

    static string SqliteScalar(DriverFixture f, string sql)
    {
        using var conn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={f.SqlitePath};Mode=ReadOnly");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(cmd.ExecuteScalar(), CultureInfo.InvariantCulture)!;
    }

    /// <summary>Setup: makes the store's stored schema revision differ from the binary's (<paramref name="offset"/> below or above), or restores it.</summary>
    static Task SetRevisionAsync(DriverFixture f, int offset) => f.Kind switch
    {
        StoreKind.Postgres => f.ExecAsync("UPDATE runwick.store_metadata SET schema_revision = @r", ("r", PgMigrator.CurrentRevision + offset)),
        StoreKind.Sqlite => f.ExecAsync("UPDATE runwick_store_metadata SET schema_revision = @r", ("r", Runwick.Sqlite.Migrations.SqliteMigrator.CurrentRevision + offset)),
        _ => RedisAdmin.SetLayout(f.Application, (1 + offset).ToString(CultureInfo.InvariantCulture)),
    };

    [Theory]
    [MemberData(nameof(KindsAndOffsets))]
    [Trait("Case", "CFG-01")]
    public async Task Every_operation_is_rejected_before_any_write_at_another_schema_revision(StoreKind kind, int offset)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cfg01g"), kind);
        var c = await f.OpenClient(configure: r =>
        {
            TestJobs.Register(r);
            r.AddJob<TickJob>().Named("test.tick").ToQueue("default");
        });
        var ct = CancellationToken.None;
        var driver = c.Driver;
        var control = (IJobControlStore)driver;
        var rates = (IRateControlStore)driver;
        var schedules = c.Services.GetRequiredService<IScheduleStore>();
        var scaling = (IScalingStore)driver;
        var outbox = (IOutboxDestinationStore)driver;
        var diagnostics = (IReadinessDiagnostics)driver;

        // Valid requests, built while the store is at the binary's revision.
        Assert.Equal(StoreStatus.Applied, (await rates.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Actor)), ct)).Status);
        var waiting = (await c.Dispatcher.DispatchAsync(new PingJob("w"))).JobId.Value;
        await c.Dispatcher.DispatchAsync(new ResourceJob("r", "r1"));
        var preparer = c.Services.GetRequiredService<DispatchPreparer>();
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration("nightly", new TickJob("n")) { Cron = "* * * * *" }, preparer, TzdbArtifact.Pinned);
        var cutoff = await schedules.StoreNowAsync(ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await schedules.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest("nightly", definition, cutoff, first)), ct)).Status);
        var schedule = (await schedules.ReadScheduleAsync(c.Application, "nightly", ct))!;
        var position = (await Model(f)).Job(waiting);
        var (generation, revision) = (position.Generation, position.Revision);
        var prepared = preparer.Prepare(c.Runtime.StoreId, new PingJob("outbox"), null);
        var acceptUntil = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeMilliseconds());
        var identity = new OutboxSourceIdentity(Guid.NewGuid(), Guid.CreateVersion7());
        var delivery = OutboxFingerprints.Delivery(prepared.StoreId, prepared.Application, identity, acceptUntil.ToUnixTimeMilliseconds(), prepared.Intent, prepared.Digests);
        var receive = new StoreCommand<ReceiveOutboxRequest>(c.Application, Guid.CreateVersion7(), NotAfter(TimeSpan.FromSeconds(20)),
            new ReceiveOutboxRequest(identity, new DispatchItem(prepared.Intent, prepared.Scopes, prepared.Digests), acceptUntil, delivery, prepared.StoreId));
        var instance = Guid.CreateVersion7();
        var contracts = c.Contracts;

        static async Task<(StoreStatus Status, string? Code)> Shape<T>(Task<StoreReply<T>> call) where T : class
        {
            var reply = await call;
            return (reply.Status, reply.Error?.Code);
        }

        // Receipted operations: the reply must be the G1 rejection itself.
        var receipted = new (string Operation, Func<Task<(StoreStatus Status, string? Code)>> Call)[]
        {
            ("Cancel/1", () => Shape(control.CancelAsync(c.Command(new CancelRequest(waiting, generation, revision, Actor)), ct))),
            ("ManualRetry/1", () => Shape(control.ManualRetryAsync(c.Command(new RetryRequest(waiting, generation, revision, Actor)), ct))),
            ("RerunSucceeded/1", () => Shape(control.RerunSucceededAsync(c.Command(new RetryRequest(waiting, generation, revision, Actor)), ct))),
            ("SetQueueState/1 (public)", () => Shape(control.SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, 1, Actor)), ct))),
            ("EditScopePolicy/1", () => Shape(control.EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "r1", 2, 1, Actor)), ct))),
            ("ReceiveOutbox/1", () => Shape(outbox.ReceiveOutboxAsync(receive, ct))),
            ("ActivateCapabilities/1", () => Shape(rates.ActivateCapabilitiesAsync(
                c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), ct))),
            ("EditRatePolicy/1", () => Shape(rates.EditRatePolicyAsync(c.Command(new RatePolicyRequest("resource", "r1", 2, 1000, 1, Actor)), ct))),
            ("RegisterSchedule/1", () => Shape(schedules.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest("other", definition, cutoff, first)), ct))),
            ("MaterializeOccurrences/1", () => Shape(schedules.MaterializeOccurrencesAsync(c.Command(new MaterializeRequest(
                schedule.ScheduleId, schedule.DefinitionRevision, schedule.ControlRevision, schedule.CursorRevision, null, cutoff, [],
                new ScheduleCursor(cutoff, first, cutoff, null), [], false)), ct))),
            ("SetScheduleState/1", () => Shape(schedules.SetScheduleStateAsync(c.Command(new ScheduleStateRequest("nightly", true, null, schedule.ControlRevision, Actor)), ct))),
            ("UpdateSchedule/1", () => Shape(schedules.UpdateScheduleAsync(c.Command(new UpdateScheduleRequest(
                "nightly", definition, schedule.DefinitionRevision, cutoff, first, Actor)), ct))),
            ("TriggerSchedule/1", () => Shape(schedules.TriggerScheduleAsync(c.Command(new TriggerScheduleRequest(
                "nightly", schedule.DefinitionRevision, Guid.CreateVersion7(), Guid.CreateVersion7(), Actor)), ct))),
            ("ResolveCatchUp/1", () => Shape(schedules.ResolveCatchUpAsync(c.Command(new ResolveCatchUpRequest(
                "nightly", Guid.CreateVersion7(), "skip", 0, null, Guid.CreateVersion7(), Actor)), ct))),
            ("RecordScheduleEvaluationFailure/1", () => Shape(schedules.RecordEvaluationFailureAsync(c.Command(new EvaluationFailureRequest(
                schedule.ScheduleId, schedule.DefinitionRevision, "schedule_budget_elapsed", null, Actor)), ct))),
            ("RevalidateSchedule/1", () => Shape(schedules.RevalidateScheduleAsync(c.Command(new RevalidateScheduleRequest("nightly", schedule.DefinitionRevision, Actor)), ct))),
            ("SetPoolOverride/1", () => Shape(scaling.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 0, Actor)), ct))),
        };

        // Unreceipted reads: they must fail as incompatible, never return data or write.
        var unreceipted = new (string Operation, Func<Task> Call)[]
        {
            ("EventsSince/1", () => control.EventsSinceAsync(c.Application, null, 10, ct)),
            ("ReadinessProbe/2", () => driver.ProbeAsync(c.Application, [new LaneKey("default", "medium")], contracts, ct)),
            ("ListUnsupportedBacklog/1", () => diagnostics.ListUnsupportedBacklogAsync(c.Application, 4, ct)),
            ("EstimateDemand/1", () => scaling.EstimateDemandAsync(c.Application, instance, ["default"], contracts, ct)),
        };

        await SetRevisionAsync(f, offset);
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
        Assert.False(await scaling.RecordScalingEventAsync(c.Application, new ScalingEventRecord(instance, "main", 4, 5, 5, 5, "grow", null, null), ct));

        Assert.Empty(before.DiffTables(await f.ReadCommittedSnapshot()));

        // Back at the binary's revision the same requests are admitted again (the rejection was the gate, not the request).
        await SetRevisionAsync(f, 0);
        Assert.Equal(StoreStatus.Applied, (await receipted[0].Call()).Status);
        Assert.Equal(StoreStatus.Applied, (await receipted[5].Call()).Status);
        await control.EventsSinceAsync(c.Application, null, 10, ct);
        Assert.True(await scaling.RecordScalingEventAsync(c.Application, new ScalingEventRecord(instance, "main", 4, 5, 5, 5, "grow", null, null), ct));
        Assert.Empty(await History(f));
    }
}
