using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;

namespace Runwick.AspNetCore.Tests;

/// <summary>Store-side setup that is not under test: schedules, owned attempts and failed attempts.</summary>
internal static class StoreSupport
{
    static readonly ControlActor Ops = new("user:ops", null, "host", "setup");

    sealed class FrozenBudgetClock : TimeProvider
    {
        public static readonly FrozenBudgetClock Instance = new();

        public override long GetTimestamp() => 0;
    }

    /// <summary>Activates the scheduler capability and registers a daily schedule of <see cref="PingJob"/>, as a scheduler host would.</summary>
    public static async Task RegisterScheduleAsync(this DriverFixture f, string key)
    {
        var c = await f.OpenClient("local", MgmtHost.Jobs);
        var ct = CancellationToken.None;
        Assert.Equal(StoreStatus.Applied, (await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Ops)), ct)).Status);
        var definition = ScheduleCompiler.Compile(new ScheduleDeclaration(key, new PingJob(key)) { Cron = "0 3 * * *" },
            c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
        var cutoff = await c.Store.StoreNowAsync(ct);
        var first = ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance)(cutoff)?.At;
        Assert.Equal(StoreStatus.Applied, (await c.Store.RegisterScheduleAsync(c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), ct)).Status);
    }

    /// <summary>Reserves and begins the next job of queue <c>default</c> on a store-level client: the job is owned and running.</summary>
    public static async Task<(TestClient Client, LeaseHandle Lease)> RunNextAsync(this DriverFixture f)
    {
        var c = await f.OpenClient("local", MgmtHost.Jobs);
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        var lease = reserved.Value!.Lease.Handle;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        return (c, lease);
    }

    /// <summary>Runs the next job and fails it permanently with an error whose message is sensitive.</summary>
    public static async Task<Guid> FailNextAsync(this DriverFixture f, string message)
    {
        var (c, lease) = await f.RunNextAsync();
        var finished = await c.FinishAsync(lease, AttemptOutcome.PermanentFailure, error: new AttemptError("boom", "System.Exception", message, null));
        Assert.Equal(StoreStatus.Applied, finished.Status);
        return lease.AttemptId;
    }
}
