using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Runwick.Telemetry;

/// <summary>
/// Runwick's fixed log events (18 §5): source-generated, structured, never interpolating user data. Events at Warning
/// or above and 1401–1403 pass the per-process <see cref="LogLimiter"/> first.
/// </summary>
internal static partial class RunwickLog
{
    public static LogLimiter Limiter { get; set; } = new(TimeProvider.System);

    [LoggerMessage(EventId = 1001, EventName = "AdmissionClosed", Level = LogLevel.Warning, Message = "Admission closed in pool {Pool}: {Reason}.")]
    static partial void AdmissionClosedCore(ILogger logger, string pool, string reason);

    public static void AdmissionClosed(ILogger logger, string pool, string reason)
    {
        if (Limiter.Allow(logger, 1001))
        {
            AdmissionClosedCore(logger, pool, reason);
        }
    }

    [LoggerMessage(EventId = 1003, EventName = "AuthorityLost", Level = LogLevel.Warning,
        Message = "Attempt {AttemptId} of job {JobId} lost authority ({LossReason}); handler still running: {HandlerStillRunning}.")]
    static partial void AuthorityLostCore(ILogger logger, Guid jobId, Guid attemptId, string lossReason, bool handlerStillRunning);

    public static void AuthorityLost(ILogger logger, Guid jobId, Guid attemptId, string lossReason, bool handlerStillRunning)
    {
        if (Limiter.Allow(logger, 1003))
        {
            AuthorityLostCore(logger, jobId, attemptId, lossReason, handlerStillRunning);
        }
    }

    [LoggerMessage(EventId = 1004, EventName = "SettlementUnresolved", Level = LogLevel.Warning,
        Message = "Finish of attempt {AttemptId} of job {JobId} ({Outcome}) unresolved after {ElapsedMs} ms.")]
    static partial void SettlementUnresolvedCore(ILogger logger, Guid jobId, Guid attemptId, string outcome, long elapsedMs);

    public static void SettlementUnresolved(ILogger logger, Guid jobId, Guid attemptId, string outcome, long elapsedMs)
    {
        if (Limiter.Allow(logger, 1004))
        {
            SettlementUnresolvedCore(logger, jobId, attemptId, outcome, elapsedMs);
        }
    }

    [LoggerMessage(EventId = 1005, EventName = "AttemptCleanupBudgetExceeded", Level = LogLevel.Warning,
        Message = "Scope disposal of attempt {AttemptId} of job {JobId} exceeded {BudgetMs} ms.")]
    static partial void CleanupBudgetExceededCore(ILogger logger, Guid jobId, Guid attemptId, long budgetMs);

    public static void CleanupBudgetExceeded(ILogger logger, Guid jobId, Guid attemptId, long budgetMs)
    {
        if (Limiter.Allow(logger, 1005))
        {
            CleanupBudgetExceededCore(logger, jobId, attemptId, budgetMs);
        }
    }

    [LoggerMessage(EventId = 1007, EventName = "PressureStop", Level = LogLevel.Warning, Message = "Worker pressure stop: {Trigger} ({ValueMs} ms).")]
    static partial void PressureStopCore(ILogger logger, string trigger, long valueMs);

    public static void PressureStop(ILogger logger, string trigger, long valueMs)
    {
        if (Limiter.Allow(logger, 1007))
        {
            PressureStopCore(logger, trigger, valueMs);
        }
    }

    [LoggerMessage(EventId = 1008, EventName = "PressureResume", Level = LogLevel.Information, Message = "Worker pressure resumed.")]
    public static partial void PressureResume(ILogger logger);

    [LoggerMessage(EventId = 1011, EventName = "HandlerFailed", Level = LogLevel.Information,
        Message = "Handler of {JobName} failed for attempt {AttemptId} of job {JobId}: {Outcome} {ErrorType} {ExceptionMessage}")]
    public static partial void HandlerFailed(ILogger logger, Guid jobId, Guid attemptId, string jobName, string outcome, string errorType, string? exceptionMessage);

    [LoggerMessage(EventId = 1012, EventName = "DrainStarted", Level = LogLevel.Information, Message = "Worker drain started with {Outstanding} attempts.")]
    public static partial void DrainStarted(ILogger logger, int outstanding);

    [LoggerMessage(EventId = 1013, EventName = "DrainCompleted", Level = LogLevel.Information, Message = "Worker drain completed; {Outstanding} attempts unresolved.")]
    public static partial void DrainCompleted(ILogger logger, int outstanding);

    [LoggerMessage(EventId = 1101, EventName = "DispatchOutcomeUnknown", Level = LogLevel.Warning,
        Message = "Dispatch {CommandId} to {Queue}: outcome unknown; resolve it with the same operation ID.")]
    static partial void DispatchOutcomeUnknownCore(ILogger logger, Guid commandId, string queue);

    public static void DispatchOutcomeUnknown(ILogger logger, Guid commandId, string queue)
    {
        if (Limiter.Allow(logger, 1101))
        {
            DispatchOutcomeUnknownCore(logger, commandId, queue);
        }
    }

    [LoggerMessage(EventId = 1401, EventName = "ManagementAuthenticationFailed", Level = LogLevel.Warning,
        Message = "Management authentication failed ({Scheme}, source {SourceBucket}).")]
    static partial void ManagementAuthenticationFailedCore(ILogger logger, string scheme, string sourceBucket);

    public static void ManagementAuthenticationFailed(ILogger logger, string scheme, string sourceBucket)
    {
        if (Limiter.Allow(logger, 1401))
        {
            ManagementAuthenticationFailedCore(logger, scheme, sourceBucket);
        }
    }

    [LoggerMessage(EventId = 1501, EventName = "RetentionBatchFailed", Level = LogLevel.Warning, Message = "Retention batch for {RecordClass} failed: {ErrorCode}.")]
    static partial void RetentionBatchFailedCore(ILogger logger, string recordClass, string errorCode);

    public static void RetentionBatchFailed(ILogger logger, string recordClass, string errorCode)
    {
        if (Limiter.Allow(logger, 1501))
        {
            RetentionBatchFailedCore(logger, recordClass, errorCode);
        }
    }

    [LoggerMessage(EventId = 1601, EventName = "CollectorSampleFailed", Level = LogLevel.Warning, Message = "Application metrics sample failed: {ErrorCode}.")]
    static partial void CollectorSampleFailedCore(ILogger logger, string errorCode);

    public static void CollectorSampleFailed(ILogger logger, string errorCode)
    {
        if (Limiter.Allow(logger, 1601))
        {
            CollectorSampleFailedCore(logger, errorCode);
        }
    }

    // 1602 is logged once per instrument per process by its caller, so it bypasses the limiter.
    [LoggerMessage(EventId = 1602, EventName = "TelemetryOverflow", Level = LogLevel.Warning,
        Message = "Instrument {Instrument} reached its series limit; further combinations go to the overflow series.")]
    public static partial void TelemetryOverflow(ILogger logger, string instrument);

    [LoggerMessage(EventId = 1603, EventName = "LogsSuppressed", Level = LogLevel.Warning, Message = "{Suppressed} events {EventId} suppressed in the last minute.")]
    public static partial void LogsSuppressed(ILogger logger, int eventId, long suppressed);

    [LoggerMessage(EventId = 1701, EventName = "StoreUnavailable", Level = LogLevel.Warning, Message = "Store unavailable for role {Role}: {ErrorCode}.")]
    static partial void StoreUnavailableCore(ILogger logger, string role, string errorCode);

    public static void StoreUnavailable(ILogger logger, string role, string errorCode)
    {
        if (Limiter.Allow(logger, 1701))
        {
            StoreUnavailableCore(logger, role, errorCode);
        }
    }
}

/// <summary>
/// 10 events per minute per event ID per process (18 §5, OPS-06). The excess is counted in <c>runwick.telemetry.dropped</c>
/// and summarized by one event 1603 when the next minute's first event of that ID arrives.
/// </summary>
internal sealed class LogLimiter(TimeProvider time)
{
    public const int PerMinute = 10;

    readonly ConcurrentDictionary<int, Window> _windows = new();

    sealed class Window
    {
        public long Start;
        public int Count;
        public long Suppressed;
    }

    public bool Allow(ILogger logger, int eventId)
    {
        var now = time.GetTimestamp();
        var w = _windows.GetOrAdd(eventId, _ => new Window { Start = now });
        long suppressed = 0;
        bool allowed;
        lock (w)
        {
            if (time.GetElapsedTime(w.Start, now) >= TimeSpan.FromMinutes(1))
            {
                suppressed = w.Suppressed;
                (w.Start, w.Count, w.Suppressed) = (now, 0, 0);
            }

            allowed = ++w.Count <= PerMinute;
            if (!allowed)
            {
                w.Suppressed++;
            }
        }

        if (suppressed > 0)
        {
            RunwickLog.LogsSuppressed(logger, eventId, suppressed);
        }

        if (!allowed)
        {
            Telemetry.TelemetryDropped.Add(1, "log");
        }

        return allowed;
    }

    /// <summary>Summarizes windows that ended without a later event (called once a minute by a host loop or a test).</summary>
    public void Flush(ILogger logger)
    {
        var now = time.GetTimestamp();
        foreach (var (eventId, w) in _windows)
        {
            long suppressed = 0;
            lock (w)
            {
                if (time.GetElapsedTime(w.Start, now) >= TimeSpan.FromMinutes(1) && w.Suppressed > 0)
                {
                    suppressed = w.Suppressed;
                    (w.Start, w.Count, w.Suppressed) = (now, 0, 0);
                }
            }

            if (suppressed > 0)
            {
                RunwickLog.LogsSuppressed(logger, eventId, suppressed);
            }
        }
    }
}
