using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// The harness's committed-history check (<see cref="CommittedHistory"/>, 15 §12.2 rule 4) accepts a real run that
/// uses retries, controls and new generations, and rejects a history whose committed state the ReferenceModel would
/// not reach. Without the second half the check could pass vacuously.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-011")]
[Trait("Case", "HARNESS-history")]
public sealed class CommittedHistoryTests
{
    static readonly ControlActor Actor = new("user:history", null, "host", "harness");

    [Fact]
    public async Task Real_histories_are_accepted_and_altered_ones_are_rejected()
    {
        await using var f = await DriverFixture.StartAsync("hist_" + Guid.NewGuid().ToString("N")[..8]);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r, maxExecutions: 2));

        // Retried then succeeded.
        var retried = (await c.Dispatcher.DispatchAsync(new PingJob("retried"))).JobId.Value;
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(await RunAsync(c, retried), AttemptOutcome.RetryableFailure)).Status);
        await Task.Delay(1200);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(await RunAsync(c, retried), AttemptOutcome.Succeeded)).Status);

        // Failed permanently, then a second generation by ManualRetry that is cancelled while waiting.
        var failed = (await c.Dispatcher.DispatchAsync(new PingJob("failed"))).JobId.Value;
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(await RunAsync(c, failed), AttemptOutcome.PermanentFailure)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.Store.ManualRetryAsync(c.Command(new RetryRequest(failed, 1, await Revision(f, failed), Actor)), CancellationToken.None)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.Store.CancelAsync(c.Command(new CancelRequest(failed, 2, await Revision(f, failed), Actor)), CancellationToken.None)).Status);

        // Cancel requested while running, then an unsuccessful Finish closes it as cancelled.
        var running = (await c.Dispatcher.DispatchAsync(new PingJob("running"))).JobId.Value;
        var lease = await RunAsync(c, running);
        Assert.Equal(StoreStatus.Applied, (await c.Store.CancelAsync(c.Command(new CancelRequest(running, 1, await Revision(f, running), Actor)), CancellationToken.None)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.RetryableFailure)).Status);

        // Reserved and released, still waiting.
        var released = (await c.Dispatcher.DispatchAsync(new PingJob("released"))).JobId.Value;
        var reserved = await c.ReserveAsync();
        Assert.Equal(released, reserved.Value!.Position.JobId);
        Assert.Equal(StoreStatus.Applied, (await c.ReleaseAsync(reserved.Value.Lease.Handle)).Status);

        Assert.Empty(await f.HistoryViolations());

        // A counter the model would not reach, and a cancellation with no committed record of it.
        await f.ExecAsync("UPDATE runwick.job_generations SET started_count = 1 WHERE job_id = @j AND generation = 1", ("j", retried));
        await f.ExecAsync("DELETE FROM runwick.queue_events WHERE job_id = @j AND event_kind = 'job_cancel_requested'", ("j", running));
        var violations = await f.HistoryViolations();
        Assert.Contains(violations, v => v.Contains(retried.ToString("N"), StringComparison.Ordinal) && v.Contains("started_count", StringComparison.Ordinal));
        Assert.Contains(violations, v => v.Contains(running.ToString("N"), StringComparison.Ordinal) && v.Contains("state model=retrying store=cancelled", StringComparison.Ordinal));
        Assert.Equal(2, violations.Count);
    }

    static async Task<long> Revision(DriverFixture f, Guid job) =>
        long.Parse((await TextAsync(f, "SELECT revision FROM runwick.jobs WHERE job_id = @j", ("j", job))).Single(), System.Globalization.CultureInfo.InvariantCulture);
}
