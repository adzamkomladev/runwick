using System.Collections.Immutable;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Runwick.Hosting;
using Runwick.Protocol;

namespace Runwick.Telemetry;

/// <summary>
/// The designated collector of application-wide numbers (18 §4): one bounded sample per period on the background class
/// with a 1 s budget, at most 64 queues per sample (round-robin beyond). Gauge callbacks only read the immutable state;
/// a queue whose sample is older than 3 periods reports nothing, while <c>runwick.collector.snapshot.age</c> keeps growing.
/// </summary>
internal sealed class ApplicationMetricsCollector(RunwickRuntime runtime, RunwickTelemetryOptions options, TimeProvider time, ILogger logger)
    : IRunwickComponent, IDisposable
{
    public const int QueuesPerSample = 64;
    static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);

    readonly CancellationTokenSource _stop = new();
    State _state = new(ImmutableDictionary<string, Sampled>.Empty, null, null);
    long _startedAt;
    int _offset;
    Task? _loop;

    sealed record Sampled(long Due, double OldestAgeSeconds, long LocalAt);

    sealed record State(ImmutableDictionary<string, Sampled> Queues, Sampled? Schedules, long? LastSuccess);

    public long Samples;

    public Task StartAsync(CancellationToken ct)
    {
        if (runtime.Store is not ITelemetrySampleStore store)
        {
            return Task.CompletedTask;
        }

        _startedAt = time.GetTimestamp();
        Telemetry.AddCollectorSource(this, Gauges);
        _loop = Task.Run(() => LoopAsync(store), CancellationToken.None);
        return Task.CompletedTask;
    }

    async Task LoopAsync(ITelemetrySampleStore store)
    {
        while (!_stop.IsCancellationRequested)
        {
            await SampleOnceAsync(store).ConfigureAwait(false);
            try
            {
                await Task.Delay(options.ApplicationSamplePeriod, time, _stop.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    internal async Task SampleOnceAsync(ITelemetrySampleStore store)
    {
        var all = runtime.Registry.Queues.Order(StringComparer.Ordinal).ToList();
        var take = Math.Min(QueuesPerSample, all.Count);
        var batch = Enumerable.Range(0, take).Select(i => all[(_offset + i) % all.Count]).ToList();
        _offset = all.Count == 0 ? 0 : (_offset + take) % all.Count;
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        budget.CancelAfter(Budget);
        try
        {
            var sample = await store.SampleAsync(runtime.Registry.Application, batch, budget.Token).ConfigureAwait(false);
            var now = time.GetTimestamp();
            var queues = _state.Queues;
            foreach (var q in sample.Queues)
            {
                queues = queues.SetItem(q.Queue, new Sampled(q.Due, q.OldestDueAt is { } at ? Math.Max(0, (sample.ObservedAt - at).TotalSeconds) : 0, now));
            }

            var overdue = sample.OldestOverdueAt is { } o ? Math.Max(0, (sample.ObservedAt - o).TotalSeconds) : 0;
            _state = new State(queues, new Sampled(sample.NeedsReview, overdue, now), now);
            Interlocked.Increment(ref Samples);
        }
        catch (Exception ex) when (!_stop.IsCancellationRequested)
        {
            RunwickLog.CollectorSampleFailed(logger, ex is RunwickException r ? r.Code : ex is OperationCanceledException ? "budget_exceeded" : ex.GetType().Name);
        }
    }

    /// <summary>The gauge callback body: in-memory state and the clock only (18 §3.4, OBS-04).</summary>
    CollectorGauges Gauges()
    {
        var state = _state;
        var now = time.GetTimestamp();
        var staleAfter = 3 * options.ApplicationSamplePeriod;
        var collector = new KeyValuePair<string, object?>("runwick.collector", options.CollectorName);
        List<Measurement<double>> due = [], oldest = [], review = [], overdue = [];
        foreach (var (queue, s) in state.Queues)
        {
            if (time.GetElapsedTime(s.LocalAt, now) >= staleAfter)
            {
                continue; // absent, never zero (00 §9 rule 9)
            }

            var q = new KeyValuePair<string, object?>("runwick.queue", queue);
            due.Add(new(s.Due, q, collector));
            if (s.Due > 0)
            {
                oldest.Add(new(s.OldestAgeSeconds, q, collector));
            }
        }

        if (state.Schedules is { } sch && time.GetElapsedTime(sch.LocalAt, now) < staleAfter)
        {
            review.Add(new(sch.Due, collector));
            overdue.Add(new(sch.OldestAgeSeconds, collector));
        }

        var age = time.GetElapsedTime(state.LastSuccess ?? _startedAt, now).TotalSeconds;
        return new CollectorGauges(due, oldest, review, overdue, [new(age, collector)]);
    }

    public async Task StopAsync(CancellationToken ct)
    {
        Telemetry.RemoveCollectorSource(this);
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is { } loop)
        {
            await loop.ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        Telemetry.RemoveCollectorSource(this);
        _stop.Dispose();
    }
}
