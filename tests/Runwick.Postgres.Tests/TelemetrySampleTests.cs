using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Postgres.Tests;

/// <summary>The collector's bounded sample (18 §4): due count, oldest due instant and schedule review numbers in one statement.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-022")]
[Trait("Case", "OBS-06")]
public sealed class TelemetrySampleTests
{
    [Fact]
    public async Task Sample_counts_due_jobs_per_queue_and_reports_the_oldest()
    {
        await using var f = await DriverFixture.StartAsync("tel_" + Guid.NewGuid().ToString("N")[..8]);
        await using var client = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = (ITelemetrySampleStore)client.Store;
        var ct = TestContext.Current.CancellationToken;

        var empty = await store.SampleAsync(client.Application, ["default", "other"], ct);
        Assert.Equal([("default", 0L, (DateTimeOffset?)null), ("other", 0L, null)], empty.Queues.Select(q => (q.Queue, q.Due, q.OldestDueAt)).Order());
        Assert.Equal(0, empty.NeedsReview);
        Assert.Null(empty.OldestOverdueAt);

        for (var i = 0; i < 3; i++)
        {
            await client.Dispatcher.DispatchAsync(new PingJob("s" + i), cancellationToken: ct);
        }

        var sample = await store.SampleAsync(client.Application, ["default"], ct);
        var q = Assert.Single(sample.Queues);
        Assert.Equal(3, q.Due);
        Assert.NotNull(q.OldestDueAt);
        Assert.True(q.OldestDueAt <= sample.ObservedAt);

        // Another application's sample sees none of these jobs.
        var other = await store.SampleAsync(client.Application + "_x", ["default"], ct);
        Assert.Equal(0, Assert.Single(other.Queues).Due);
    }
}
