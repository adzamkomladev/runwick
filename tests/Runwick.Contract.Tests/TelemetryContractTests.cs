using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Sqlite;

namespace Runwick.Contract.Tests;

/// <summary>TEL-03 (18 §3.1, §2.1) on every driver: each driver call is timed, and staging is a stage span.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class TelemetryContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    /// <summary>Staging on the caller's transaction exists on SQLite here (PostgreSQL has its own form; Redis declares it unsupported, 22 §8).</summary>
    public static TheoryData<StoreKind> SqliteOnly =>
        [.. CoreContractTests.Kinds.Where(r => r.Data == StoreKind.Sqlite)];

    sealed class Listeners : IDisposable
    {
        readonly MeterListener _meters;
        readonly ActivityListener _spans;

        public ConcurrentQueue<(string Operation, string Result, string IoClass)> Storage { get; } = new();

        public ConcurrentQueue<Activity> Spans { get; } = new();

        public Listeners()
        {
            _meters = new MeterListener
            {
                InstrumentPublished = (i, l) =>
                {
                    if (i.Meter.Name == "Runwick" && i.Name == "runwick.storage.operation.duration")
                    {
                        l.EnableMeasurementEvents(i);
                    }
                },
            };
            _meters.SetMeasurementEventCallback<double>((_, _, tags, _) =>
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

                Storage.Enqueue((op!, result!, io!));
            });
            _meters.Start();
            _spans = new ActivityListener
            {
                ShouldListenTo = s => s.Name == "Runwick",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = Spans.Enqueue,
            };
            ActivitySource.AddActivityListener(_spans);
        }

        public void Dispose()
        {
            _meters.Dispose();
            _spans.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "TEL-03")]
    public async Task Driver_calls_are_timed(StoreKind kind)
    {
        using var listeners = new Listeners();
        await using var f = await DriverFixture.StartAsync("tel3_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new PingJob("timed"), cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains(("Dispatch", "applied", "producer"), listeners.Storage);

        var reserved = await c.ReserveAsync();
        Assert.Equal(Runwick.Protocol.StoreStatus.Applied, reserved.Status);
        Assert.Contains(("Reserve", "applied", "admission"), listeners.Storage);
    }

    [Theory]
    [MemberData(nameof(SqliteOnly))]
    [Trait("Case", "TEL-03")]
    public async Task Staging_is_a_stage_span(StoreKind kind)
    {
        using var listeners = new Listeners();
        await using var f = await DriverFixture.StartAsync("tel3s_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var dispatched = await c.Dispatcher.DispatchAsync(new PingJob("timed"), cancellationToken: TestContext.Current.CancellationToken);

        using var conn = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False;Foreign Keys=True;Default Timeout=5");
        conn.Open();
        using (var sync = conn.CreateCommand())
        {
            sync.CommandText = "PRAGMA synchronous = FULL";
            sync.ExecuteNonQuery();
        }

        using var tx = conn.BeginTransaction(deferred: false);
        var staged = await c.Services.GetRequiredService<ISqliteJobDispatcher>().StageAsync(new PingJob("staged"), tx, cancellationToken: TestContext.Current.CancellationToken);
        tx.Commit();

        var stage = Assert.Single(listeners.Spans, s => s.DisplayName == "stage default" && (string?)s.GetTagItem("runwick.job.id") == staged.JobId.Value.ToString("D"));
        Assert.Equal(ActivityKind.Producer, stage.Kind);
        Assert.Equal("staged", stage.GetTagItem("runwick.dispatch.kind"));
        Assert.Equal("stage", stage.GetTagItem("messaging.operation.name"));
        Assert.Null(stage.GetTagItem("runwick.dispatch.result"));
        Assert.NotEqual(dispatched.JobId, staged.JobId);
    }
}
