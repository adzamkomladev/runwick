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
            switch (result.Status)
            {
                case "failed":
                    Interlocked.Increment(ref CleanupFailures);
                    var n = failures[recordClass] = failures.GetValueOrDefault(recordClass) + 1;
                    var backoff = CleanupBackoffBase * Math.Pow(2, Math.Min(n - 1, 10));
                    due[recordClass] = now + Min(backoff, CleanupBackoffCap);
                    Logger.LogDebug("Runwick cleanup of {RecordClass} failed ({Code}); backing off.", recordClass, result.ErrorCode);
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
