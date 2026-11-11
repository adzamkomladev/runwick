using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Runwick.Internal;

/// <summary>
/// Per-phase durations of one store call (engine I/O lane wait, driver lane wait, pool open, statements, COMMIT) on
/// the <c>Runwick.Diagnostics</c> meter. Recording costs nothing unless a listener is attached (perf-floor diagnosis).
/// </summary>
internal static class PhaseTimings
{
    static readonly Meter Meter = new("Runwick.Diagnostics");
    static readonly Histogram<double> Phase = Meter.CreateHistogram<double>("runwick.store.phase", "ms");
    static readonly Histogram<double> DispatchGroup = Meter.CreateHistogram<double>("runwick.sqlite.dispatch.commit_size", "{command}");

    public static bool Enabled => Phase.Enabled;

    public static void DispatchCommit(int count) => DispatchGroup.Record(count);

    public static long Start() => Phase.Enabled ? Stopwatch.GetTimestamp() : 0;

    /// <summary>Records the time since <paramref name="started"/> and returns a new start for the next phase.</summary>
    public static long Lap(long started, string operation, string phase)
    {
        if (started == 0 || !Phase.Enabled)
        {
            return 0;
        }

        var now = Stopwatch.GetTimestamp();
        Phase.Record(Stopwatch.GetElapsedTime(started, now).TotalMilliseconds,
            new KeyValuePair<string, object?>("operation", operation), new KeyValuePair<string, object?>("phase", phase));
        return now;
    }
}
