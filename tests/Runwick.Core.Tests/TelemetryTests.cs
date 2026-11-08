using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Runwick.Core.Tests.Support;
using Runwick.Harness.Engine;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Telemetry;
using T = Runwick.Telemetry.Telemetry;

namespace Runwick.Core.Tests;

/// <summary>The instruments, spans and listeners are process-wide, so telemetry cases never run beside other engines.</summary>
[CollectionDefinition(nameof(TelemetryCollection), DisableParallelization = true)]
public sealed class TelemetryCollection;

/// <summary>Listens to the <c>Runwick</c> source and meter and keeps everything it sees (18 §10 test support).</summary>
internal sealed class TelemetryCapture : IDisposable
{
    readonly ActivityListener _activities;
    readonly MeterListener _meters;

    public TelemetryCapture(Action<string>? onCall = null, params string[] extraSources)
    {
        _activities = new ActivityListener
        {
            ShouldListenTo = s => s.Name == RunwickTelemetry.SourceName || extraSources.Contains(s.Name),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = a => onCall?.Invoke("start " + a.OperationName),
            ActivityStopped = a =>
            {
                Spans.Enqueue(a);
                onCall?.Invoke("stop " + a.OperationName);
            },
        };
        ActivitySource.AddActivityListener(_activities);
        _meters = new MeterListener
        {
            InstrumentPublished = (i, l) =>
            {
                if (i.Meter.Name == RunwickTelemetry.SourceName)
                {
                    l.EnableMeasurementEvents(i);
                }
            },
        };
        _meters.SetMeasurementEventCallback<long>((i, v, tags, _) =>
        {
            Measurements.Enqueue((i.Name, v, Tags(tags)));
            onCall?.Invoke(i.Name);
        });
        _meters.SetMeasurementEventCallback<double>((i, v, tags, _) =>
        {
            Measurements.Enqueue((i.Name, v, Tags(tags)));
            onCall?.Invoke(i.Name);
        });
        _meters.Start();
    }

    static Dictionary<string, string?> Tags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var d = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var t in tags)
        {
            d[t.Key] = t.Value?.ToString();
        }

        return d;
    }

    public ConcurrentQueue<Activity> Spans { get; } = new();

    public ConcurrentQueue<(string Name, double Value, Dictionary<string, string?> Tags)> Measurements { get; } = new();

    public void Observe() => _meters.RecordObservableInstruments();

    public double Sum(string instrument) => Measurements.Where(m => m.Name == instrument).Sum(m => m.Value);

    public IEnumerable<Activity> ForJob(Guid jobId) => Spans.Where(s => s.GetTagItem("runwick.job.id") as string == jobId.ToString("D"));

    public void Dispose()
    {
        _activities.Dispose();
        _meters.Dispose();
    }
}

/// <summary>Keeps every log record with its event ID, formatted message and structured values.</summary>
internal sealed class LogCapture : ILoggerProvider, ILogger
{
    public ConcurrentQueue<(int EventId, LogLevel Level, string Text)> Records { get; } = new();

    public ILogger CreateLogger(string categoryName) => this;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var values = state is IEnumerable<KeyValuePair<string, object?>> kv ? string.Join("|", kv.Select(p => p.Value)) : "";
        Records.Enqueue((eventId.Id, logLevel, formatter(state, exception) + "|" + values + "|" + exception));
    }

    public void Dispose()
    {
    }
}

/// <summary>SP-022 telemetry cases in core (18 §10): listeners, bounds, redaction, collector, health and log limits.</summary>
[Collection(nameof(TelemetryCollection))]
[Trait("Category", "Pure")]
[Trait("Item", "SP-022")]
public sealed class TelemetryTests
{
    static readonly TimeSpan Long = TimeSpan.FromMinutes(5);

    // ------------------------------------------------------------------ TEL-01 name freeze

    /// <summary>Instrument name → (kind, unit) from the frozen table of spec 18 §3.1.</summary>
    static Dictionary<string, (string Kind, string Unit)> SpecInstruments()
    {
        var spec = File.ReadAllText(Path.Combine(Repo.Root, "docs", "spec", "18-telemetry.md"));
        var table = spec[spec.IndexOf("### 3.1", StringComparison.Ordinal)..spec.IndexOf("### 3.2", StringComparison.Ordinal)];
        var rows = Regex.Matches(table, @"^\| `(runwick\.[a-z_.]+)` \| (\w+) `?([^`|]+?)`? \|", RegexOptions.Multiline);
        return rows.ToDictionary(m => m.Groups[1].Value, m => (m.Groups[2].Value, m.Groups[3].Value.Trim()));
    }

    static string KindOf(Instrument i) => i switch
    {
        Counter<long> => "Counter",
        Histogram<double> => "Histogram",
        ObservableGauge<double> => "ObservableGauge",
        _ => i.GetType().Name,
    };

    [Fact]
    [Trait("Case", "TEL-01")]
    public async Task The_meter_and_source_list_exactly_the_frozen_names()
    {
        var spec = SpecInstruments();
        Assert.Equal(33, spec.Count);
        var seen = new ConcurrentDictionary<string, (string, string)>();
        System.Runtime.CompilerServices.RuntimeHelpers.RunClassConstructor(typeof(T).TypeHandle);
        using (var listener = new MeterListener
        {
            InstrumentPublished = (i, _) =>
            {
                if (i.Meter.Name == RunwickTelemetry.SourceName)
                {
                    seen[i.Name] = (KindOf(i), i.Unit ?? "");
                }
            },
        })
        {
            listener.Start();
        }

        Assert.Equal(spec.Keys.Order(StringComparer.Ordinal), seen.Keys.Order(StringComparer.Ordinal));
        foreach (var (name, expected) in spec)
        {
            Assert.True(seen[name] == expected, $"{name}: {seen[name]} != {expected}");
        }

        Assert.Equal(spec.Keys.Order(StringComparer.Ordinal), T.Instruments().Select(i => i.Name).Order(StringComparer.Ordinal));
        Assert.Equal(RunwickTelemetry.SourceName, T.Meter.Name);
        Assert.Equal("https://opentelemetry.io/schemas/1.44.0", T.Meter.TelemetrySchemaUrl);
        Assert.Equal("https://opentelemetry.io/schemas/1.44.0", T.Source.TelemetrySchemaUrl);
        Assert.Contains(T.Meter.Tags!, t => t.Key == "runwick.telemetry.schema" && Equals(t.Value, 1));

        // Span names of one attempt are a subset of §2.1.
        using var capture = new TelemetryCapture();
        await using var e = EngineFixture.Create();
        var id = e.Add("tel01");
        await e.StartAsync();
        await e.RunUntilAsync(() => capture.ForJob(id).Any(s => s.Kind == ActivityKind.Consumer), Long, "process span ended");
        var names = capture.Spans.Select(s => s.DisplayName).Distinct().ToList();
        string[] allowed = ["process default", "runwick.begin", "runwick.execute", "runwick.attempt.cleanup", "settle default", "runwick.recover",
            "runwick.schedule.materialize", "runwick.management.command", "runwick.retention.batch", "send default", "stage default"];
        Assert.All(names, n => Assert.Contains(n, allowed));
        Assert.Contains("process default", names);
        Assert.Contains("runwick.execute", names);
        Assert.Contains("settle default", names);
    }

    // ------------------------------------------------------------------ OBS-01 listeners never steer ownership

    [Theory]
    [Trait("Case", "OBS-01")]
    [InlineData("none")]
    [InlineData("blocking")]
    [InlineData("throwing")]
    public async Task Jobs_settle_once_with_no_slow_or_broken_listener(string mode)
    {
        Action<string>? call = mode switch
        {
            "blocking" => _ => Thread.Sleep(2000),
            "throwing" => _ => throw new InvalidOperationException("listener broke"),
            _ => null,
        };
        using var capture = mode == "none" ? null : new TelemetryCapture(call);
        await using var e = EngineFixture.Create();

        // Blocking: 2 s of real time per listener call; fake time moves slowly so the lease is judged by Runwick alone.
        e.Step = mode == "blocking" ? TimeSpan.FromMilliseconds(1) : TimeSpan.FromMilliseconds(50);
        var hold = mode == "blocking" ? TimeSpan.Zero : TimeSpan.FromSeconds(150);
        e.Script.Default = (_, _, ct) => Task.Delay(hold, e.Time, ct);
        e.Add("a");
        e.Add("b");
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Store.Jobs.All(j => j.State == JobStates.Succeeded), TimeSpan.FromMinutes(10), "both jobs succeeded");
        Assert.All(e.Store.Jobs, j => Assert.Equal(AttemptOutcome.Succeeded, Assert.Single(j.Finishes).Outcome));
        Assert.DoesNotContain(e.Events, x => x.Name.StartsWith("lost:", StringComparison.Ordinal));
        Assert.DoesNotContain(e.Events, x => x.Name.StartsWith("pressureStop", StringComparison.Ordinal));
        if (mode != "blocking")
        {
            Assert.True(e.EventCount("renewed") >= 2, "the leases renewed while the handlers ran");
        }

        if (mode == "throwing")
        {
            Assert.True(T.ListenerFailures > 0);
        }

        await e.StopAsync();
    }

    // ------------------------------------------------------------------ OBS-02 honest outcomes

    [Fact]
    [Trait("Case", "OBS-02")]
    public async Task Success_with_a_replayed_begin_and_finish_is_succeeded_confirmed_and_counted_once()
    {
        using var capture = new TelemetryCapture();
        await using var e = EngineFixture.Create();
        e.Store.DropReply = c => c.Submission == 1 && c.Operation is Operations.Begin or Operations.Finish;
        var id = e.Add("obs02");
        await e.StartAsync();
        await e.RunUntilAsync(() => capture.ForJob(id).Any(s => s.Kind == ActivityKind.Consumer), Long, "process span ended");
        var process = capture.ForJob(id).Single(s => s.Kind == ActivityKind.Consumer);
        Assert.Equal("succeeded", process.GetTagItem("runwick.outcome"));
        Assert.Equal("confirmed", process.GetTagItem("runwick.settlement.result"));
        Assert.Contains(e.Store.Calls, c => c.Operation == Operations.Begin && c.Submission == 2);
        Assert.Equal(1, capture.Sum("runwick.execution.authorized"));
        Assert.Contains(capture.Measurements, m => m.Name == "runwick.settlement.duration" && m.Tags["runwick.settlement.result"] == "confirmed");
        await e.StopAsync();
    }

    [Fact]
    [Trait("Case", "OBS-02")]
    public async Task Success_whose_finish_never_resolves_is_succeeded_unresolved()
    {
        using var capture = new TelemetryCapture();
        await using var e = EngineFixture.Create();
        e.Store.Before = c => Task.FromResult(c.Operation == Operations.Finish ? CallEffect.Hang : CallEffect.Proceed);
        var id = e.Add("obs02b");
        await e.StartAsync();
        await e.RunUntilAsync(() => capture.Spans.Any(s => s.DisplayName == "settle default"), Long, "settle span ended");
        await e.StopAsync();
        await e.RunUntilAsync(() => capture.ForJob(id).Any(s => s.Kind == ActivityKind.Consumer), Long, "process span ended");
        var process = capture.ForJob(id).Single(s => s.Kind == ActivityKind.Consumer);
        Assert.Equal("succeeded", process.GetTagItem("runwick.outcome"));
        Assert.Equal("unresolved", process.GetTagItem("runwick.settlement.result"));
        Assert.Equal(ActivityStatusCode.Error, process.Status);
        Assert.Equal("unresolved", capture.Spans.Single(s => s.DisplayName == "settle default").GetTagItem("runwick.settlement.result"));
    }

    // ------------------------------------------------------------------ OBS-03 cardinality

    [Fact]
    [Trait("Case", "OBS-03")]
    public void A_million_measurements_stay_within_the_series_bound_without_allocating()
    {
        using var meter = new Meter("obs03.local");
        var series = new Series(meter.CreateCounter<long>("obs03.dispatches"), ["runwick.queue"]);
        var registered = Enumerable.Range(0, 5_000).Select(i => "q-" + i).ToArray();
        T.RegisterNames(registered, []);
        var random = Enumerable.Range(0, 1_000).Select(_ => Guid.NewGuid().ToString("N")).ToArray();

        var combos = new HashSet<string>(StringComparer.Ordinal);
        long overflow = 0;
        using (var listener = new MeterListener())
        {
            listener.InstrumentPublished = (i, l) =>
            {
                if (i.Meter == meter || i.Name == "runwick.telemetry.overflow")
                {
                    l.EnableMeasurementEvents(i);
                }
            };
            listener.SetMeasurementEventCallback<long>((i, v, tags, _) =>
            {
                if (i.Meter == meter)
                {
                    var key = string.Join(",", tags.ToArray().Select(t => t.Key + "=" + t.Value));
                    lock (combos)
                    {
                        combos.Add(key);
                    }
                }
                else if (tags.ToArray().Any(t => Equals(t.Value, "obs03.dispatches")))
                {
                    Interlocked.Add(ref overflow, v);
                }
            });
            listener.Start();
            for (var i = 0; i < 1_000_000; i++)
            {
                series.Add(1, T.Queue(i % 2 == 0 ? registered[(i / 2) % registered.Length] : random[i % random.Length]));
            }
        }

        Assert.True(combos.Count <= 1_001, $"{combos.Count} series");
        Assert.Contains("runwick.overflow=True", combos);
        Assert.Contains("runwick.queue=_other", combos);
        Assert.True(series.Admitted <= T.MaxSeries);
        Assert.True(overflow > 0);

        // Allocation: a warmed admission set records with no per-measurement allocation.
        using (var quiet = new MeterListener())
        {
            quiet.InstrumentPublished = (i, l) => l.EnableMeasurementEvents(i);
            quiet.SetMeasurementEventCallback<long>((_, _, _, _) => { });
            quiet.Start();
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 1_000_000; i++)
            {
                series.Add(1, T.Queue(i % 2 == 0 ? registered[(i / 2) % registered.Length] : random[i % random.Length]));
            }

            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(allocated < 64 * 1024, $"{allocated} bytes allocated for 1,000,000 measurements");
        }
    }

    [Fact]
    [Trait("Case", "OBS-03")]
    public void The_sdk_exports_at_most_the_limit_and_never_its_own_overflow()
    {
        T.RetentionRows.ResetForTests();
        var exported = new List<MetricSnapshot>();
        using (var provider = Sdk.CreateMeterProviderBuilder()
            .AddRunwickInstrumentation(o => o.MaxSeriesPerInstrument = 1_000)
            .AddInMemoryExporter(exported)
            .Build())
        {
            for (var i = 0; i < 5_000; i++)
            {
                T.RetentionRows.Add(1, "class-" + i);
            }

            provider.ForceFlush();
        }

        var rows = exported.Last(m => m.Name == "runwick.retention.rows");
        Assert.True(rows.MetricPoints.Count <= 1_001, $"{rows.MetricPoints.Count} points");
        var keys = rows.MetricPoints.SelectMany(p => ToList(p.Tags)).Select(t => t.Key).ToHashSet();
        Assert.DoesNotContain("otel.metric.overflow", keys);
        Assert.Contains("runwick.overflow", keys);
        Assert.Equal(5_000, rows.MetricPoints.Sum(p => p.GetSumLong()));
        T.RetentionRows.ResetForTests();
    }

    static List<KeyValuePair<string, object?>> ToList(ReadOnlyTagCollection tags)
    {
        var list = new List<KeyValuePair<string, object?>>();
        foreach (var t in tags)
        {
            list.Add(t);
        }

        return list;
    }

    // ------------------------------------------------------------------ OBS-04 callbacks never touch the store

    sealed class HangingSampleStore : ITelemetrySampleStore
    {
        public int Calls;

        public async Task<ApplicationSample> SampleAsync(string application, IReadOnlyList<string> queues, CancellationToken ct)
        {
            Interlocked.Increment(ref Calls);
            await Task.Delay(Timeout.Infinite, CancellationToken.None);
            throw new UnreachableException();
        }
    }

    [Fact]
    [Trait("Case", "OBS-04")]
    public async Task Observable_callbacks_return_at_once_while_the_store_hangs()
    {
        await using var e = EngineFixture.Create();
        e.Script.Default = (_, _, ct) => Task.Delay(TimeSpan.FromMinutes(10), e.Time, ct);
        e.Add("busy");
        await e.StartAsync();
        await e.RunUntilAsync(() => e.Script.Invocations.Count == 1, Long, "running");
        await e.AdvanceAsync(TimeSpan.FromSeconds(2));

        // Every store call now hangs; collector samples hang too.
        e.Store.Before = _ => Task.FromResult(CallEffect.Hang);
        var store = new HangingSampleStore();
        using var collector = new ApplicationMetricsCollector(e.Services.GetRequiredService<RunwickRuntime>(), new RunwickTelemetryOptions(), e.Time,
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, store);
        await collector.StartAsync(CancellationToken.None);
        await EngineFixture.SettleAsync();
        using var capture = new TelemetryCapture();
        capture.Observe(); // warm-up: JIT of the callbacks is not what OBS-04 measures
        var calls = (store.Calls, e.Store.Calls.Count);
        var sw = Stopwatch.StartNew();
        capture.Observe();
        sw.Stop();
        Assert.True(sw.Elapsed < TimeSpan.FromMilliseconds(10), $"callbacks took {sw.Elapsed.TotalMilliseconds} ms");
        Assert.Equal(calls, (store.Calls, e.Store.Calls.Count));
        Assert.Contains(capture.Measurements, m => m.Name == "runwick.worker.tickets" && m.Tags["runwick.ticket.phase"] == "running");
        Assert.Contains(capture.Measurements, m => m.Name == "runwick.collector.snapshot.age");
    }

    // ------------------------------------------------------------------ OBS-05 context isolation

    [Fact]
    [Trait("Case", "OBS-05")]
    public async Task Each_handler_sees_its_own_execute_span_with_no_baggage_or_ambient_state()
    {
        using var parents = new ActivitySource("obs05.dispatch");
        using var capture = new TelemetryCapture(null, "obs05.dispatch");
        var ambient = new AsyncLocal<string>();
        await using var e = EngineFixture.Create();
        var seen = new ConcurrentDictionary<string, (string? Name, ActivitySpanId Parent, ActivityTraceId Trace, int Baggage, string? Local)>();
        var gate = new TaskCompletionSource();
        e.Script.Default = async (job, _, _) =>
        {
            var a = Activity.Current;
            seen[job.Id] = (a?.OperationName, a?.ParentSpanId ?? default, a?.TraceId ?? default, a?.Baggage.Count() ?? 0, ambient.Value);
            if (seen.Count == 2)
            {
                gate.TrySetResult();
            }

            await gate.Task;
        };

        var contexts = new Dictionary<string, ActivityContext>();
        foreach (var key in new[] { "x", "y" })
        {
            using var parent = parents.StartActivity("dispatch " + key)!;
            parent.AddBaggage("tenant", "t-" + key);
            ambient.Value = "leak-" + key;
            e.Add(key);
            contexts[key] = parent.Context;
        }

        using (var outer = parents.StartActivity("ambient")!)
        {
            outer.AddBaggage("ambient", "yes");
            await e.StartAsync();
        }

        await e.RunUntilAsync(() => capture.Spans.Count(s => s.Kind == ActivityKind.Consumer) == 2, Long, "both attempts ended");
        foreach (var key in new[] { "x", "y" })
        {
            var (name, parentId, trace, baggage, local) = seen[key];
            Assert.Equal("runwick.execute", name);
            Assert.Equal(0, baggage);
            Assert.Null(local);
            var process = capture.Spans.Single(s => s.Kind == ActivityKind.Consumer && s.SpanId == parentId);
            Assert.Equal(trace, process.TraceId);
            Assert.Equal(default, process.ParentSpanId);
            Assert.NotEqual(contexts[key].TraceId, process.TraceId);
            Assert.Equal(contexts[key].TraceId, Assert.Single(process.Links).Context.TraceId);
        }

        Assert.NotEqual(seen["x"].Trace, seen["y"].Trace);
        await e.StopAsync();
    }

    // ------------------------------------------------------------------ OBS-09 redaction

    const string Password = "hunter2-secret";
    const string Card = "4111111111111111";

    [Theory]
    [Trait("Case", "OBS-09")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Secrets_never_reach_logs_spans_or_metrics(bool details)
    {
        var logs = new LogCapture();
        using var capture = new TelemetryCapture();
        await using var e = EngineFixture.Create(
            configure: r => r.Telemetry(t => t.RecordExceptionDetails = details),
            services: s => s.AddLogging(b => b.SetMinimumLevel(LogLevel.Trace).AddProvider(logs)));
        try
        {
            e.Script.Default = (_, _, _) => throw new InvalidOperationException(
                $"Host=db;Username=app;Password={Password}\u0007 " + new string('x', 2_000));
            var id = e.Add("pay-" + Card);
            await e.StartAsync();
            await e.RunUntilAsync(() => capture.ForJob(id).Any(s => s.Kind == ActivityKind.Consumer), Long, "process span ended");
            await e.StopAsync();

            var spanText = capture.Spans.SelectMany(s => s.TagObjects.Select(t => (s.OperationName, t.Key, Value: t.Value?.ToString() ?? ""))
                .Concat(s.Events.SelectMany(ev => ev.Tags.Select(t => (s.OperationName, t.Key, Value: t.Value?.ToString() ?? ""))))).ToList();
            Assert.DoesNotContain(spanText, t => t.Value.Contains(Card, StringComparison.Ordinal));
            Assert.DoesNotContain(capture.Measurements.SelectMany(m => m.Tags.Values), v => v?.Contains(Password, StringComparison.Ordinal) == true);
            Assert.DoesNotContain(capture.Measurements.SelectMany(m => m.Tags.Values), v => v?.Contains(Card, StringComparison.Ordinal) == true);
            Assert.DoesNotContain(logs.Records, r => r.Text.Contains(Card, StringComparison.Ordinal));

            var leaking = spanText.Where(t => t.Value.Contains(Password, StringComparison.Ordinal)).ToList();
            var leakingLogs = logs.Records.Where(r => r.Text.Contains(Password, StringComparison.Ordinal)).ToList();
            Assert.Contains(logs.Records, r => r.EventId == 1011);
            if (!details)
            {
                Assert.Empty(leaking);
                Assert.Empty(leakingLogs);
                return;
            }

            // Only the execute span's exception.message and event 1011, sanitized and bounded.
            var (_, key, value) = Assert.Single(leaking);
            Assert.Equal("exception.message", key);
            Assert.DoesNotContain('\u0007', value);
            Assert.True(System.Text.Encoding.UTF8.GetByteCount(value) <= 1024);
            Assert.EndsWith("…", value, StringComparison.Ordinal);
            Assert.All(leakingLogs, r => Assert.Equal(1011, r.EventId));
            Assert.NotEmpty(leakingLogs);
        }
        finally
        {
            T.RecordExceptionDetails = false;
        }
    }

    // ------------------------------------------------------------------ OBS-06 collectors

    sealed class FakeSampleStore(TimeProvider time) : ITelemetrySampleStore
    {
        public volatile bool Down;

        public Task<ApplicationSample> SampleAsync(string application, IReadOnlyList<string> queues, CancellationToken ct)
        {
            if (Down)
            {
                throw new StoreUnavailableException("down");
            }

            var now = time.GetUtcNow();
            return Task.FromResult(new ApplicationSample(now, [.. queues.Select(q => new QueueSample(q, 5, now - TimeSpan.FromSeconds(30)))], 1,
                now - TimeSpan.FromSeconds(12)));
        }
    }

    [Fact]
    [Trait("Case", "OBS-06")]
    public async Task Two_collectors_differ_only_by_name_and_stale_queues_report_nothing()
    {
        await using var e = EngineFixture.Create();
        await e.StartAsync();
        var runtime = e.Services.GetRequiredService<RunwickRuntime>();
        var store = new FakeSampleStore(e.Time);
        var period = TimeSpan.FromSeconds(15);
        using var a = new ApplicationMetricsCollector(runtime, new RunwickTelemetryOptions { CollectorName = "a" }, e.Time,
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, store);
        using var b = new ApplicationMetricsCollector(runtime, new RunwickTelemetryOptions { CollectorName = "b" }, e.Time,
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, store);
        await a.StartAsync(CancellationToken.None);
        await b.StartAsync(CancellationToken.None);
        await e.RunUntilAsync(() => a.Samples > 0 && b.Samples > 0, Long, "first samples");

        using var capture = new TelemetryCapture();
        capture.Observe();
        var due = capture.Measurements.Where(m => m.Name == "runwick.queue.due").ToList();
        Assert.Equal(["a", "b"], due.Select(m => m.Tags["runwick.collector"]).Order(StringComparer.Ordinal));
        Assert.All(due, m => Assert.Equal(5, m.Value));
        var maxByQueue = due.GroupBy(m => m.Tags["runwick.queue"]).Select(g => g.Max(m => m.Value)).Single();
        Assert.Equal(5, maxByQueue);
        var oldest = capture.Measurements.Where(m => m.Name == "runwick.queue.oldest_due.age").ToList();
        Assert.All(oldest, m => Assert.Equal(30, m.Value));
        Assert.Equal(2, capture.Measurements.Count(m => m.Name == "runwick.schedule.needs_review" && m.Value == 1));

        // Store outage: after 3 periods the queue series disappear; the snapshot age keeps rising; nothing reports 0.
        store.Down = true;
        await e.AdvanceAsync(3 * period + TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        var stale = new TelemetryCapture();
        stale.Observe();
        Assert.DoesNotContain(stale.Measurements, m => m.Name is "runwick.queue.due" or "runwick.queue.oldest_due.age" or "runwick.schedule.needs_review");
        var ages = stale.Measurements.Where(m => m.Name == "runwick.collector.snapshot.age").ToList();
        Assert.Equal(["a", "b"], ages.Select(m => m.Tags["runwick.collector"]).Order(StringComparer.Ordinal));
        var age1 = ages.Min(m => m.Value);
        Assert.True(age1 >= 3 * period.TotalSeconds, $"snapshot age {age1}");
        stale.Dispose();
        await e.AdvanceAsync(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1));
        using var later = new TelemetryCapture();
        later.Observe();
        Assert.True(later.Measurements.Where(m => m.Name == "runwick.collector.snapshot.age").Min(m => m.Value) > age1);
        await a.StopAsync(CancellationToken.None);
        await b.StopAsync(CancellationToken.None);
        await e.StopAsync();
    }

    // ------------------------------------------------------------------ OBS-07 health

    [Fact]
    [Trait("Case", "OBS-07")]
    public async Task Readiness_per_role_and_liveness_follow_the_table()
    {
        await using var e = EngineFixture.Create();
        await e.StartAsync();
        var health = e.Services.GetRequiredService<IRunwickHealth>();
        await e.AdvanceAsync(TimeSpan.FromSeconds(2));
        var ready = health.Readiness().ToDictionary(r => r.Role);
        Assert.Equal(["producer", "worker"], ready.Keys);
        Assert.Equal(RunwickHealthStatus.Healthy, ready["producer"].Status);
        Assert.Equal(RunwickHealthStatus.Healthy, ready["worker"].Status);
        Assert.Equal(RunwickHealthStatus.Healthy, health.Liveness().Status);

        // A zero backlog changes nothing; drain makes worker readiness Unhealthy while liveness stays Healthy.
        e.Engine.SignalStop();
        await e.AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Equal((RunwickHealthStatus.Unhealthy, "draining"), health.Readiness().Where(r => r.Role == "worker").Select(r => (r.Status, r.Reason)).Single());
        Assert.Equal(RunwickHealthStatus.Healthy, health.Liveness().Status);
        await e.StopAsync();
    }

    [Fact]
    [Trait("Case", "OBS-07")]
    public void A_role_tracker_degrades_then_fails_and_never_shows_stale_data_as_healthy()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var window = TimeSpan.FromSeconds(30);
        var stale = TimeSpan.FromSeconds(30);
        var tracker = new RoleTracker(time);
        Assert.Equal((RunwickHealthStatus.Degraded, "stale"), tracker.Evaluate(window, stale));
        tracker.Ok();
        Assert.Equal((RunwickHealthStatus.Healthy, "ok"), tracker.Evaluate(window, stale));
        tracker.Failed(new StoreUnavailableException("outage"));
        time.Advance(TimeSpan.FromSeconds(29));
        Assert.Equal((RunwickHealthStatus.Degraded, "store_unavailable"), tracker.Evaluate(window, stale));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal((RunwickHealthStatus.Unhealthy, "store_unavailable"), tracker.Evaluate(window, stale));
        tracker.Ok();
        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal((RunwickHealthStatus.Degraded, "stale"), tracker.Evaluate(window, stale));
        tracker.Failed(new StoreIncompatibleException("profile mismatch"));
        Assert.Equal((RunwickHealthStatus.Unhealthy, "incompatible"), tracker.Evaluate(window, stale));
    }

    // ------------------------------------------------------------------ TEL-02 log bounds

    [Fact]
    [Trait("Case", "TEL-02")]
    public void Ten_thousand_authentication_failures_log_ten_events_and_one_summary()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var logs = new LogCapture();
        var previous = RunwickLog.Limiter;
        RunwickLog.Limiter = new LogLimiter(time);
        try
        {
            for (var i = 0; i < 10_000; i++)
            {
                RunwickLog.ManagementAuthenticationFailed(logs, "basic", "ab12");
                if (i % 1_000 == 0)
                {
                    time.Advance(TimeSpan.FromSeconds(5));
                }
            }

            Assert.Equal(10, logs.Records.Count(r => r.EventId == 1401));
            Assert.DoesNotContain(logs.Records, r => r.EventId == 1603);
            time.Advance(TimeSpan.FromSeconds(30));
            RunwickLog.Limiter.Flush(logs);
            var summary = Assert.Single(logs.Records, r => r.EventId == 1603);
            Assert.Contains("9990", summary.Text, StringComparison.Ordinal);
        }
        finally
        {
            RunwickLog.Limiter = previous;
        }
    }
}
