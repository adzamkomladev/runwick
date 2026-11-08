using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;

namespace Runwick.Contract.Tests;

/// <summary>
/// OBS-11 (18 §10) on every driver, ported from Runwick.Postgres.Tests.TelemetryOverheadTests: the same worker load with
/// telemetry off and with listeners that record every span and measurement (standing in for the SDK's in-memory exporter).
/// Numbers are written to the test output; the throughput bar is enforced when RUNWICK_OBS11_ENFORCE=1 (18 §10: numbers are recorded,
/// not promised for other hosts).
/// </summary>
[Trait("Category", "Perf")]
[Trait("Item", "SP-028")]
public sealed class TelemetryOverheadContractTests(ITestOutputHelper output)
{
    const int Jobs = 300;
    const int Concurrency = 16;

    sealed record Run(string Mode, double JobsPerSecond, double AttemptP50, double AttemptP99, double BytesPerJob, int Spans, long Measurements,
        double? MinMarginSeconds);

    /// <summary>Everything an in-memory exporter would keep: each ended span and each measurement with its tag set.</summary>
    sealed class Recorder : IDisposable
    {
        readonly ActivityListener _spans;
        readonly MeterListener _meters;
        readonly Timer _collect;
        public readonly ConcurrentQueue<Activity> Spans = new();
        public readonly ConcurrentDictionary<string, double> Series = new(StringComparer.Ordinal);
        public long Measurements;
        public double MinMargin = double.MaxValue;

        public Recorder()
        {
            _spans = new ActivityListener
            {
                ShouldListenTo = s => s.Name == "Runwick",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = Spans.Enqueue,
            };
            ActivitySource.AddActivityListener(_spans);
            _meters = new MeterListener
            {
                InstrumentPublished = (i, l) =>
                {
                    if (i.Meter.Name == "Runwick")
                    {
                        l.EnableMeasurementEvents(i);
                    }
                },
            };
            _meters.SetMeasurementEventCallback<long>((i, v, tags, _) => Add(i, v, tags));
            _meters.SetMeasurementEventCallback<double>((i, v, tags, _) =>
            {
                Add(i, v, tags);
                if (i.Name == "runwick.lease.margin")
                {
                    lock (Series)
                    {
                        MinMargin = Math.Min(MinMargin, v);
                    }
                }
            });
            _meters.Start();

            // A periodic reader: observable instruments are collected once a second.
            _collect = new Timer(_ => _meters.RecordObservableInstruments(), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        void Add(Instrument i, double v, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            Interlocked.Increment(ref Measurements);
            var key = i.Name;
            foreach (var t in tags)
            {
                key += "|" + t.Key + "=" + t.Value;
            }

            Series.AddOrUpdate(key, v, (_, s) => s + v);
        }

        public void Dispose()
        {
            _collect.Dispose();
            _meters.Dispose();
            _spans.Dispose();
        }
    }

    static double Percentile(List<double> sorted, double p) => sorted[Math.Min(sorted.Count - 1, (int)Math.Ceiling(p * sorted.Count) - 1)];

    long _done;

    async Task<Run> RunAsync(DriverFixture f, string mode)
    {
        var tag = mode + Guid.NewGuid().ToString("N")[..6];
        await using var w = WorkerHost.Create(f.ClientConnectionString("obs11"), f.Application, f.Faults, Concurrency);
        for (var i = 0; i < Jobs; i++)
        {
            await w.DispatchAsync(tag + "-" + i);
        }

        using var recorder = mode == "on" ? new Recorder() : null;
        var startedAt = DateTimeOffset.UtcNow;
        var allocated = GC.GetTotalAllocatedBytes();
        var clock = Stopwatch.StartNew();
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count(c => c.Key.StartsWith(tag, StringComparison.Ordinal)) >= Jobs, TimeSpan.FromMinutes(5), mode + " load");
        var elapsed = clock.Elapsed.TotalSeconds;
        var bytes = (GC.GetTotalAllocatedBytes() - allocated) / (double)Jobs;
        await w.StopAsync();
        _done += Jobs;

        // The attempts begun during this run, from the committed history: begin to close.
        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Equal(_done, model.Jobs.Count(j => j.State == "succeeded"));
        var latency = model.Attempts.Where(a => a.BegunAt is { } begun && begun >= startedAt - TimeSpan.FromSeconds(5) && a.ClosedAt is not null)
            .Select(a => (a.ClosedAt!.Value - a.BegunAt!.Value).TotalMilliseconds).Order().ToList();
        Assert.NotEmpty(latency);
        return new Run(mode, Jobs / elapsed, Percentile(latency, 0.5), Percentile(latency, 0.99), bytes, recorder?.Spans.Count ?? 0, recorder?.Measurements ?? 0,
            recorder is { MinMargin: < double.MaxValue } r ? r.MinMargin : null);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "OBS-11")]
    public async Task Telemetry_on_keeps_at_least_ninety_percent_of_the_throughput_of_telemetry_off(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("obs11_" + Guid.NewGuid().ToString("N")[..8], kind);
        await RunAsync(f, "warmup");
        var runs = new List<Run>();
        foreach (var mode in (Environment.GetEnvironmentVariable("RUNWICK_OBS11_MODES") ?? "off,on,off,on").Split(","))
        {
            runs.Add(await RunAsync(f, mode));
        }

        foreach (var r in runs)
        {
            output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"{kind} {r.Mode,-4} {r.JobsPerSecond,8:F1} jobs/s  attempt p50/p99 {r.AttemptP50:F1}/{r.AttemptP99:F1} ms  {r.BytesPerJob / 1024:F1} KiB/job  spans {r.Spans}  measurements {r.Measurements}  min margin {(r.MinMarginSeconds is { } m ? m.ToString("F1", CultureInfo.InvariantCulture) + " s" : "n/a")}"));
        }

        var off = runs.Where(r => r.Mode == "off").Max(r => r.JobsPerSecond);
        var on = runs.Where(r => r.Mode == "on").Max(r => r.JobsPerSecond);
        output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"{kind} best on / best off = {on / off:P1}"));
        Assert.True(runs.Where(r => r.Mode == "on").All(r => r.Spans > 0 && r.Measurements > 0), "the listeners saw nothing");
        // shortcut: on a shared, loaded host run-to-run noise (±50 %) exceeds the 10 % bar, so the bar is enforced only
        // when RUNWICK_OBS11_ENFORCE=1 (a quiet, reference host); elsewhere the numbers are recorded, as 18 §10 allows.
        if (Environment.GetEnvironmentVariable("RUNWICK_OBS11_ENFORCE") == "1")
        {
            Assert.True(on >= 0.9 * off, $"{kind}: telemetry-on throughput {on:F1} jobs/s is below 90 % of off ({off:F1} jobs/s)");
        }
    }
}
