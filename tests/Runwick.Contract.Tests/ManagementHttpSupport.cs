using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Runwick.AspNetCore.Tests;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Contract.Tests;

/// <summary>
/// Store-side setup and reads for the management host cases (MGMT, 16 §8) on every store: the PostgreSQL cases of
/// Runwick.AspNetCore.Tests, run here over <see cref="MgmtHost"/> with each driver behind it.
/// </summary>
internal static class ManagementHttpSupport
{
    static readonly ControlActor Ops = new("user:ops", null, "host", "setup");

    sealed class FrozenBudgetClock : TimeProvider
    {
        public static readonly FrozenBudgetClock Instance = new();

        public override long GetTimestamp() => 0;
    }

    public static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    public static Task<long> Count(DriverFixture f, string sql, params (string Name, object Value)[] parameters) => ControlContractTests.Count(f, sql, parameters);

    public static object Control(Guid commandId, (int Generation, long Revision, string State) p, string? reason = null) =>
        new { commandId, expectedGeneration = p.Generation, expectedRevision = p.Revision, reason };

    public static Task<long> Audits(DriverFixture f, Guid? commandId = null) => commandId is { } id
        ? Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", id))
        : Count(f, "SELECT count(*) FROM runwick.management_audit");

    public static Task<long> Receipts(DriverFixture f, Guid commandId) =>
        Count(f, "SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", commandId));

    public static async Task<(int Generation, long Revision, string State)> PositionAsync(MgmtHost h, Guid job)
    {
        var (status, body, _) = await h.GetAsync($"jobs/{job}", Users.Admin);
        Assert.Equal(HttpStatusCode.OK, status);
        var j = body!["job"]!;
        return (j["generation"]!.GetValue<int>(), j["revision"]!.GetValue<long>(), j["state"]!.GetValue<string>());
    }

    /// <summary>Activates the scheduler capability and registers a daily schedule of <see cref="PingJob"/>, as a scheduler host would.</summary>
    public static async Task RegisterScheduleAsync(this DriverFixture f, string key)
    {
        var c = await f.OpenClient("local", MgmtHost.Jobs);
        var ct = CancellationToken.None;
        Assert.Equal(StoreStatus.Applied, (await ((IRateControlStore)c.Driver).ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Ops)), ct)).Status);
        var schedules = (IScheduleStore)c.Driver;
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration(key, new PingJob(key)) { Cron = "0 3 * * *" },
            c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
        var cutoff = await schedules.StoreNowAsync(ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await schedules.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), ct)).Status);
    }

    /// <summary>Runs the next job and fails it permanently with an error whose message is sensitive.</summary>
    public static async Task<Guid> FailNextAsync(this DriverFixture f, string message)
    {
        var c = await f.OpenClient("local", MgmtHost.Jobs);
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        var finished = await c.FinishAsync(lease, AttemptOutcome.PermanentFailure, error: new AttemptError("boom", "System.Exception", message, null));
        Assert.Equal(StoreStatus.Applied, finished.Status);
        return lease.AttemptId;
    }
}
