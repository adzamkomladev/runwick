using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// 18 §2.1 and §3.1 on the real store: every driver call records <c>runwick.storage.operation.duration</c> with its
/// operation, result and I/O class; staging inside the caller's transaction is a <c>stage {queue}</c> span that never
/// reports <c>created</c>. The instruments are process-wide, so the assertions look only for this test's records.
/// </summary>
public sealed class TelemetryStoreTests
{
    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-022")]
    [Trait("Case", "TEL-03")]
    public async Task Driver_calls_are_timed_and_staging_is_a_stage_span()
    {
        var storage = new ConcurrentQueue<(string Operation, string Result, string IoClass)>();
        var spans = new ConcurrentQueue<Activity>();
        using var meters = new MeterListener
        {
            InstrumentPublished = (i, l) =>
            {
                if (i.Meter.Name == "Runwick" && i.Name == "runwick.storage.operation.duration")
                {
                    l.EnableMeasurementEvents(i);
                }
            },
        };
        meters.SetMeasurementEventCallback<double>((_, _, tags, _) =>
        {
            string? op = null, result = null, io = null;
            foreach (var t in tags)
            {
                switch (t.Key)
                {
                    case "runwick.operation": op = t.Value as string; break;
                    case "runwick.result": result = t.Value as string; break;
                    case "runwick.io_class": io = t.Value as string; break;
                }
            }

            storage.Enqueue((op!, result!, io!));
        });
        meters.Start();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Runwick",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);

        await using var f = await DriverFixture.StartAsync("tel3_" + Guid.NewGuid().ToString("N")[..8]);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var dispatched = await c.Dispatcher.DispatchAsync(new PingJob("timed"));
        Assert.Contains(("Dispatch", "applied", "producer"), storage);

        await using var conn = await OpenBusinessAsync(f);
        await using var tx = await conn.BeginTransactionAsync();
        var staged = await Staging(c).StageAsync(new PingJob("staged"), tx);
        await tx.CommitAsync();

        var stage = Assert.Single(spans, s => s.DisplayName == "stage default" && (string?)s.GetTagItem("runwick.job.id") == staged.JobId.Value.ToString("D"));
        Assert.Equal(ActivityKind.Producer, stage.Kind);
        Assert.Equal("staged", stage.GetTagItem("runwick.dispatch.kind"));
        Assert.Equal("stage", stage.GetTagItem("messaging.operation.name"));
        Assert.Null(stage.GetTagItem("runwick.dispatch.result"));
        Assert.NotEqual(dispatched.JobId, staged.JobId);
    }
}
