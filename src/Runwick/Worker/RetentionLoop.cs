using Microsoft.Extensions.Logging;
using Runwick.Protocol;

namespace Runwick.Worker;

internal sealed partial class WorkerEngine
{
    static readonly TimeSpan CleanupBackoffBase = TimeSpan.FromSeconds(5), CleanupBackoffCap = TimeSpan.FromMinutes(5);

    public long CleanupBatches;

    public long CleanupRows;

    public long CleanupSkipped;

    public long CleanupFailures;

    /// <summary>
    /// The non-critical cleanup loop (17 §4.4): classes round-robin, one batch at a time on a background slot. No batch
    /// starts under the pressure stop or while draining; a batch in progress finishes. Processes cooperate through the
    /// store's progress rows; there is no leader.
    /// </summary>
    async Task RetentionLoopAsync()
    {
        var options = Settings.Retention;
        if (!options.Enabled || Store is not IRetentionStore store)
        {
            return;
        }

        var target = options.BatchTarget * 4;
        var limits = new CleanupLimits(options.BatchRows, options.BatchDetailBytes, target < TimeSpan.FromSeconds(2) ? TimeSpan.FromSeconds(2) : target);
        var due = RecordClasses.All.ToDictionary(c => c, _ => Clock.Now, StringComparer.Ordinal);
        var failures = new Dictionary<string, int>(StringComparer.Ordinal);
        while (!ControlStop.IsCancellationRequested && !Draining)
        {
            // Earliest due first; equal times keep the class order, so a busy store is visited round-robin.
            var (recordClass, at) = due.MinBy(d => d.Value);
            var now = Clock.Now;
            if (at > now || PressureStopped)
            {
                await Clock.Delay(at > now ? Min(at - now, TimeSpan.FromSeconds(1)) : TimeSpan.FromSeconds(1), ControlStop).ConfigureAwait(false);
                continue;
            }

            // A new root span per batch (18 §2.1); tagged and measured once the outcome is known.
            System.Diagnostics.Activity.Current = null;
            using var span = Telemetry.Telemetry.Span("runwick.retention.batch");
            var started = Clock.Now;
            CleanupBatchResult result;
            try
            {
                result = await CallAsync(BackgroundIo, 2, limits.StatementTimeout + TimeSpan.FromSeconds(10),
                    t => store.RunCleanupBatchAsync(Application, recordClass, limits, t), ControlStop).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ControlStop.IsCancellationRequested)
            {
                Logger.LogDebug(ex, "Runwick cleanup batch of {RecordClass} failed.", recordClass);
                result = new CleanupBatchResult("failed", 0, false, false, ex.GetType().Name);
            }

            Interlocked.Increment(ref CleanupBatches);
            Interlocked.Add(ref CleanupRows, result.Rows);
            now = Clock.Now;
            var outcome = result.Status == "done" ? (result.Rows > 0 ? "completed" : "empty") : result.Status;
            span.Activity?.SetTag("runwick.retention.class", recordClass);
            span.Activity?.SetTag("runwick.result", outcome);
            span.Activity?.SetTag("runwick.rows", result.Rows);
            Telemetry.Telemetry.RetentionBatch(recordClass, outcome, result.Rows, now - started);
            if (result.CaughtUp)
            {
                Telemetry.Telemetry.RetentionCaughtUpAt(recordClass, Clock.WallNow);
            }

            switch (result.Status)
            {
                case "failed":
                    Interlocked.Increment(ref CleanupFailures);
                    var n = failures[recordClass] = failures.GetValueOrDefault(recordClass) + 1;
                    var backoff = CleanupBackoffBase * Math.Pow(2, Math.Min(n - 1, 10));
                    due[recordClass] = now + Min(backoff, CleanupBackoffCap);
                    Telemetry.RunwickLog.RetentionBatchFailed(Logger, recordClass, result.ErrorCode ?? "unknown");
                    break;
                case "skipped_locked":
                    Interlocked.Increment(ref CleanupSkipped);
                    due[recordClass] = now + options.BatchPause;
                    break;
                default:
                    failures.Remove(recordClass);
                    due[recordClass] = now + (result.CaughtUp ? options.IdlePoll : options.BatchPause);
                    break;
            }
        }
    }
}
