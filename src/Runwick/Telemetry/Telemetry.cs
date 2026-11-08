using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Runwick.Telemetry;

/// <summary>
/// The process-wide source, meter and frozen instruments of spec 18 (§1, §3.1). Nothing is measured unless a listener is
/// enabled; every attribute combination goes through the per-instrument admission set (§3.3).
/// </summary>
internal static class Telemetry
{
    static readonly string Version = typeof(Telemetry).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0";

    static readonly KeyValuePair<string, object?>[] ScopeTags = [new("runwick.telemetry.schema", RunwickTelemetry.SchemaVersion)];

    public static readonly ActivitySource Source = new(new ActivitySourceOptions(RunwickTelemetry.SourceName)
    {
        Version = Version,
        Tags = ScopeTags,
        TelemetrySchemaUrl = RunwickTelemetry.TelemetrySchemaUrl,
    });

    public static readonly Meter Meter = new(new MeterOptions(RunwickTelemetry.SourceName)
    {
        Version = Version,
        Tags = ScopeTags,
        TelemetrySchemaUrl = RunwickTelemetry.TelemetrySchemaUrl,
    });

    static readonly double[] Fast = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
    static readonly double[] LongBuckets = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600, 14400];
    static readonly double[] MarginBuckets = [0, 1, 2, 5, 10, 15, 20, 25, 30, 45, 60, 120];

    // Boxed once: an overflow measurement allocates nothing.
    internal static readonly object True = true;

    static int _maxSeries = 1_000;

    /// <summary>The admission-set size of every instrument (18 §3.3); set once from the options at host start.</summary>
    public static int MaxSeries
    {
        get => Volatile.Read(ref _maxSeries);
        set => Volatile.Write(ref _maxSeries, value);
    }

    /// <summary>18 §6: sanitized exception messages on spans and log 1011; set once from the options at host start.</summary>
    public static bool RecordExceptionDetails { get; set; }

    /// <summary>Logger for events 1602/1603; set at host start, null until then.</summary>
    public static ILogger? Logger { get; set; }

    // ---- 3.1 local instruments
    public static readonly Series DispatchOperations = Counter("runwick.dispatch.operations", "{operation}", "runwick.dispatch.result", "runwick.dispatch.kind", "runwick.queue");
    public static readonly Series DispatchDuration = Histogram("runwick.dispatch.duration", Fast, "runwick.dispatch.result");
    public static readonly Series ExecutionAuthorized = Counter("runwick.execution.authorized", "{execution}", "runwick.queue", "runwick.job.name", "runwick.pool");
    public static readonly Series AdmissionDeferred = Counter("runwick.admission.deferred", "{begin}", "runwick.reason");
    public static readonly Series ExecutionDuration = Histogram("runwick.execution.duration", LongBuckets, "runwick.queue", "runwick.job.name", "runwick.outcome");
    public static readonly Series ExecutionStartDelay = Histogram("runwick.execution.start_delay", LongBuckets, "runwick.queue");
    public static readonly Series AttemptCleanupDuration = Histogram("runwick.attempt.cleanup.duration", Fast, "runwick.result");
    public static readonly Series SettlementDuration = Histogram("runwick.settlement.duration", Fast, "runwick.settlement.result");
    public static readonly Series StorageOperationDuration = Histogram("runwick.storage.operation.duration", Fast, "runwick.operation", "runwick.result", "runwick.io_class");
    public static readonly Series LeaseRenewals = Counter("runwick.lease.renewals", "{renewal}", "runwick.result");
    public static readonly Series LeaseMargin = Histogram("runwick.lease.margin", MarginBuckets, "runwick.pool");
    public static readonly Series LeaseAuthorityLost = Counter("runwick.lease.authority_lost", "{attempt}", "runwick.loss.reason");
    public static readonly Series Recoveries = Counter("runwick.recoveries", "{recovery}", "runwick.result");
    public static readonly Series ScalingDecisions = Counter("runwick.scaling.decisions", "{decision}", "runwick.pool", "runwick.scaling.direction", "runwick.reason");
    public static readonly Series ScheduleMaterializationDelay = Histogram("runwick.schedule.materialization.delay", LongBuckets);
    public static readonly Series ScheduleOccurrences = Counter("runwick.schedule.occurrences", "{occurrence}", "runwick.schedule.disposition");
    public static readonly Series ManagementRequests = Counter("runwick.management.requests", "{request}", "runwick.management.operation", "runwick.result");
    public static readonly Series ManagementDuration = Histogram("runwick.management.duration", Fast, "runwick.management.operation");
    public static readonly Series RetentionRows = Counter("runwick.retention.rows", "{row}", "runwick.retention.class");
    public static readonly Series RetentionBatches = Counter("runwick.retention.batches", "{batch}", "runwick.retention.class", "runwick.result");
    public static readonly Series RetentionBatchDuration = Histogram("runwick.retention.batch.duration", Fast, "runwick.retention.class");
    public static readonly Series TelemetryDropped = Counter("runwick.telemetry.dropped", "{record}", "runwick.buffer");

    // Not limited: it is the overflow sink itself, one series per instrument name.
    static readonly Counter<long> TelemetryOverflow = Meter.CreateCounter<long>("runwick.telemetry.overflow", "{measurement}");

    // ---- observable gauges: callbacks read immutable snapshots only (18 §3.4)
    static readonly ConcurrentDictionary<object, Func<WorkerGauges?>> WorkerSources = new();
    static readonly ConcurrentDictionary<object, Func<CollectorGauges?>> CollectorSources = new();
    static readonly ConcurrentDictionary<string, DateTimeOffset> RetentionCaughtUp = new(StringComparer.Ordinal);

    static Telemetry()
    {
        Meter.CreateObservableGauge("runwick.worker.tickets", () => Workers(w => w.Tickets), "{ticket}");
        Meter.CreateObservableGauge("runwick.worker.target", () => Workers(w => w.Targets), "{ticket}");
        Meter.CreateObservableGauge("runwick.worker.oldest_unsettled.age", () => Workers(w => w.OldestUnsettled), "s");
        Meter.CreateObservableGauge("runwick.worker.admission", () => Workers(w => w.Admission), "{state}");
        Meter.CreateObservableGauge("runwick.retention.caught_up.age", RetentionAges, "s");
        Meter.CreateObservableGauge("runwick.queue.due", () => Collectors(c => c.Due), "{job}");
        Meter.CreateObservableGauge("runwick.queue.oldest_due.age", () => Collectors(c => c.OldestDue), "s");
        Meter.CreateObservableGauge("runwick.schedule.needs_review", () => Collectors(c => c.NeedsReview), "{schedule}");
        Meter.CreateObservableGauge("runwick.schedule.oldest_overdue.age", () => Collectors(c => c.OldestOverdue), "s");
        Meter.CreateObservableGauge("runwick.collector.snapshot.age", () => Collectors(c => c.SnapshotAge), "s");
    }

    /// <summary>Attribute keys of the instruments that do not go through <see cref="Series"/>.</summary>
    static readonly Dictionary<string, string[]> OtherKeys = new(StringComparer.Ordinal)
    {
        ["runwick.telemetry.overflow"] = ["runwick.instrument"],
        ["runwick.worker.tickets"] = ["runwick.pool", "runwick.ticket.phase"],
        ["runwick.worker.target"] = ["runwick.pool"],
        ["runwick.worker.oldest_unsettled.age"] = ["runwick.pool"],
        ["runwick.worker.admission"] = ["runwick.pool"],
        ["runwick.retention.caught_up.age"] = ["runwick.retention.class"],
        ["runwick.queue.due"] = ["runwick.queue", "runwick.collector"],
        ["runwick.queue.oldest_due.age"] = ["runwick.queue", "runwick.collector"],
        ["runwick.schedule.needs_review"] = ["runwick.collector"],
        ["runwick.schedule.oldest_overdue.age"] = ["runwick.collector"],
        ["runwick.collector.snapshot.age"] = ["runwick.collector"],
    };

    /// <summary>Every frozen instrument: name, allowed attribute keys and histogram buckets (null for others).</summary>
    public static IEnumerable<(string Name, IReadOnlyList<string> Keys, IReadOnlyList<double>? Buckets)> Instruments() =>
        typeof(Telemetry).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(Series))
            .Select(f => (Series)f.GetValue(null)!)
            .Select(s => (s.Instrument.Name, s.Keys, (s.Instrument as Histogram<double>)?.Advice?.HistogramBucketBoundaries))
            .Concat(OtherKeys.Select(k => (k.Key, (IReadOnlyList<string>)k.Value, (IReadOnlyList<double>?)null)));

    static Series Counter(string name, string unit, params string[] keys) => new(Meter.CreateCounter<long>(name, unit), keys);

    static Series Histogram(string name, double[] buckets, params string[] keys) =>
        new(Meter.CreateHistogram<double>(name, "s", null, null, new InstrumentAdvice<double> { HistogramBucketBoundaries = buckets }), keys);

    static IEnumerable<Measurement<double>> Workers(Func<WorkerGauges, IEnumerable<Measurement<double>>> pick) =>
        WorkerSources.Values.Select(s => s()).OfType<WorkerGauges>().SelectMany(pick).ToList();

    static IEnumerable<Measurement<double>> Collectors(Func<CollectorGauges, IEnumerable<Measurement<double>>> pick) =>
        CollectorSources.Values.Select(s => s()).OfType<CollectorGauges>().SelectMany(pick).ToList();

    static IEnumerable<Measurement<double>> RetentionAges()
    {
        var now = DateTimeOffset.UtcNow;
        return [.. RetentionCaughtUp.Select(p => new Measurement<double>((now - p.Value).TotalSeconds, new KeyValuePair<string, object?>("runwick.retention.class", p.Key)))];
    }

    public static void AddWorkerSource(object owner, Func<WorkerGauges?> source) => WorkerSources[owner] = source;

    public static void RemoveWorkerSource(object owner) => WorkerSources.TryRemove(owner, out _);

    public static void AddCollectorSource(object owner, Func<CollectorGauges?> source) => CollectorSources[owner] = source;

    public static void RemoveCollectorSource(object owner) => CollectorSources.TryRemove(owner, out _);

    // ---- name bounds (18 §3.2): unregistered queue and job names become _other before the admission set
    static FrozenSet<string> _queues = FrozenSet<string>.Empty;
    static FrozenSet<string> _jobs = FrozenSet<string>.Empty;

    public static void RegisterNames(IEnumerable<string> queues, IEnumerable<string> jobs)
    {
        lock (Source)
        {
            _queues = _queues.Union(queues).ToFrozenSet(StringComparer.Ordinal);
            _jobs = _jobs.Union(jobs).ToFrozenSet(StringComparer.Ordinal);
        }
    }

    public static string Queue(string? name) => name is not null && Volatile.Read(ref _queues).Contains(name) ? name : "_other";

    public static string JobName(string? name) => name is not null && Volatile.Read(ref _jobs).Contains(name) ? name : "_other";

    // ---- spans: a listener that throws never changes Runwick's control flow (18 §1, OBS-01)

    /// <summary>Starts a span; null when nothing listens or a listener threw. The parent defaults to <c>Activity.Current</c>.</summary>
    public static Activity? StartActivity(string name, ActivityKind kind = ActivityKind.Internal, ActivityContext parent = default,
        IEnumerable<ActivityLink>? links = null)
    {
        if (!Source.HasListeners())
        {
            return null;
        }

        try
        {
            return Source.StartActivity(name, kind, parent, null, links);
        }
        catch (Exception ex)
        {
            ListenerFailed(ex);
            return null;
        }
    }

    /// <summary>A disposable span whose stop never throws.</summary>
    public static Span Span(string name, ActivityKind kind = ActivityKind.Internal, ActivityContext parent = default) => new(StartActivity(name, kind, parent));

    public static void Stop(Activity? activity)
    {
        try
        {
            activity?.Dispose();
        }
        catch (Exception ex)
        {
            ListenerFailed(ex);
        }
    }

    static int _listenerFailures;

    /// <summary>Listener exceptions caught at the instrumentation boundary (tests and diagnostics).</summary>
    public static int ListenerFailures => Volatile.Read(ref _listenerFailures);

    /// <summary>
    /// A listener threw. Runwick's decision was already taken; the exception is counted and logged once at Debug so the
    /// renewal, settlement or recovery path that made the call continues unchanged.
    /// </summary>
    internal static void ListenerFailed(Exception ex)
    {
        if (Interlocked.Increment(ref _listenerFailures) == 1)
        {
            Logger?.LogDebug(ex, "A telemetry listener threw; Runwick continues without that measurement.");
        }
    }

    // ---- retention (SP-021 calls these; 17 §4.1)

    /// <summary>One retention cleanup batch (18 §3.1): span attributes and metrics after the batch committed or failed.</summary>
    public static void RetentionBatch(string recordClass, string result, long rows, TimeSpan elapsed)
    {
        RetentionBatches.Add(1, recordClass, result);
        if (rows > 0)
        {
            RetentionRows.Add(rows, recordClass);
        }

        RetentionBatchDuration.Record(elapsed.TotalSeconds, recordClass);
    }

    /// <summary>A retention class completed a pass with nothing to delete at <paramref name="at"/>.</summary>
    public static void RetentionCaughtUpAt(string recordClass, DateTimeOffset at) => RetentionCaughtUp[recordClass] = at;

    internal static void ResetRetentionForTests() => RetentionCaughtUp.Clear();

    // ---- storage (18 §3.1 runwick.storage.operation.duration): one driver call, measured by the store runners

    static readonly string[] IoClasses = ["renewal", "settlement", "admission", "producer", "background"];
    static readonly Dictionary<Protocol.StoreStatus, string> StatusNames =
        Enum.GetValues<Protocol.StoreStatus>().ToDictionary(v => v, v => Worker.WorkerEngine.Snake(v.ToString()));

    /// <summary>
    /// Runs one driver call and records its duration once it has returned or failed; <paramref name="lane"/> is the
    /// runner's lane (renewal … background, the 04 §9.1 order). Without a listener the call runs unwrapped.
    /// </summary>
    public static Task<Protocol.StoreReply<T>> MeasureStorage<T>(string operation, int lane, Func<Task<Protocol.StoreReply<T>>> call) where T : class =>
        MeasureStorage(operation, lane, call, reply => StatusNames[reply.Status]);

    /// <summary>As the reply form, for a driver whose call returns its own result type; <paramref name="result"/> names the outcome tag.</summary>
    public static Task<TResult> MeasureStorage<TResult>(string operation, int lane, Func<Task<TResult>> call, Func<TResult, string> result) =>
        StorageOperationDuration.Instrument.Enabled ? MeasuredAsync(operation, IoClasses[lane], call, result) : call();

    static async Task<TResult> MeasuredAsync<TResult>(string operation, string ioClass, Func<Task<TResult>> call, Func<TResult, string> resultOf)
    {
        var start = Stopwatch.GetTimestamp();
        var result = "error";
        try
        {
            var reply = await call().ConfigureAwait(false);
            result = resultOf(reply);
            return reply;
        }
        catch (Protocol.StoreOutcomeUnknownException)
        {
            result = "outcome_unknown";
            throw;
        }
        catch (Protocol.StoreNotAppliedException)
        {
            result = "not_applied";
            throw;
        }
        finally
        {
            StorageOperationDuration.Record(Stopwatch.GetElapsedTime(start).TotalSeconds, operation, result, ioClass);
        }
    }

    // ---- 3.3 overflow

    static readonly ConcurrentDictionary<string, byte> OverflowLogged = new(StringComparer.Ordinal);

    internal static void Overflowed(string instrument)
    {
        TelemetryOverflow.Add(1, new KeyValuePair<string, object?>("runwick.instrument", instrument));
        if (OverflowLogged.TryAdd(instrument, 0) && Logger is { } log)
        {
            RunwickLog.TelemetryOverflow(log, instrument);
        }
    }

    // ---- 18 §6 redaction

    /// <summary>Control characters removed, at most 1 KiB of UTF-8 with a truncation marker.</summary>
    public static string Sanitize(string message)
    {
        var sb = new StringBuilder(Math.Min(message.Length, 1024));
        var bytes = 0;
        foreach (var rune in message.EnumerateRunes())
        {
            if (Rune.IsControl(rune))
            {
                continue;
            }

            if (bytes + rune.Utf8SequenceLength > 1024 - 3)
            {
                return sb.Append('…').ToString();
            }

            bytes += rune.Utf8SequenceLength;
            sb.Append(rune.ToString());
        }

        return sb.ToString();
    }

    /// <summary><c>error.type</c>: a code or exception type's full name, at most 128 characters.</summary>
    public static string ErrorType(string type) => type.Length <= 128 ? type : type[..128];
}

/// <summary>One frozen instrument with its attribute names and admission set (18 §3.3).</summary>
internal sealed class Series(Instrument instrument, string[] keys)
{
    readonly ConcurrentDictionary<(string?, string?, string?), byte> _admitted = new();
    int _count;

    public Instrument Instrument => instrument;

    public IReadOnlyList<string> Keys => keys;

    /// <summary>Admitted combinations (tests: bounded by the limit).</summary>
    public int Admitted => _admitted.Count;

    public void Add(long value, string? a = null, string? b = null, string? c = null)
    {
        if (!instrument.Enabled)
        {
            return;
        }

        try
        {
            ((Counter<long>)instrument).Add(value, Tags(a, b, c));
        }
        catch (Exception ex)
        {
            Telemetry.ListenerFailed(ex);
        }
    }

    public void Record(double value, string? a = null, string? b = null, string? c = null)
    {
        if (!instrument.Enabled)
        {
            return;
        }

        try
        {
            ((Histogram<double>)instrument).Record(value, Tags(a, b, c));
        }
        catch (Exception ex)
        {
            Telemetry.ListenerFailed(ex);
        }
    }

    /// <summary>Clears the admission set (tests only: the instruments are process-wide).</summary>
    internal void ResetForTests()
    {
        _admitted.Clear();
        Volatile.Write(ref _count, 0);
    }

    TagList Tags(string? a, string? b, string? c)
    {
        TagList tags = default;
        var key = (a, b, c);
        if (!_admitted.ContainsKey(key) && !Admit(key))
        {
            tags.Add("runwick.overflow", Telemetry.True);
            Telemetry.Overflowed(instrument.Name);
            return tags;
        }

        if (keys.Length > 0)
        {
            tags.Add(keys[0], a);
        }

        if (keys.Length > 1)
        {
            tags.Add(keys[1], b);
        }

        if (keys.Length > 2)
        {
            tags.Add(keys[2], c);
        }

        return tags;
    }

    bool Admit((string?, string?, string?) key)
    {
        if (Interlocked.Increment(ref _count) > Telemetry.MaxSeries)
        {
            Interlocked.Decrement(ref _count);
            return _admitted.ContainsKey(key);
        }

        if (!_admitted.TryAdd(key, 0))
        {
            Interlocked.Decrement(ref _count);
        }

        return true;
    }
}

/// <summary>A span scope for <c>using</c>; its stop never throws (<see cref="Telemetry.Stop"/>).</summary>
internal readonly struct Span(Activity? activity) : IDisposable
{
    public Activity? Activity => activity;

    public void Dispose() => Telemetry.Stop(activity);
}

/// <summary>A worker's local gauge values, rebuilt by its supervisor tick (18 §3.4).</summary>
internal sealed record WorkerGauges(
    IReadOnlyList<Measurement<double>> Tickets,
    IReadOnlyList<Measurement<double>> Targets,
    IReadOnlyList<Measurement<double>> OldestUnsettled,
    IReadOnlyList<Measurement<double>> Admission);

/// <summary>The collector's global gauge values; stale per-queue values are already left out (18 §4).</summary>
internal sealed record CollectorGauges(
    IReadOnlyList<Measurement<double>> Due,
    IReadOnlyList<Measurement<double>> OldestDue,
    IReadOnlyList<Measurement<double>> NeedsReview,
    IReadOnlyList<Measurement<double>> OldestOverdue,
    IReadOnlyList<Measurement<double>> SnapshotAge);
