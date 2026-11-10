using Runwick.Management;

namespace Runwick.Protocol;

// 16 §2.4: the exact job stats read. A store answers with grouped cells (Postgres groups in SQL); SQLite and Redis read one
// row per job and group them here with JobStatsCells.From, so the three stores share one definition of every figure.

/// <summary>
/// [From, To) at millisecond precision, cut into <see cref="Buckets"/> slices: a time t falls in slice
/// <c>(t - From) * Buckets / (To - From)</c> (integer milliseconds), so both ends are exact. <see cref="Queues"/> null = every queue.
/// <see cref="Groups"/> asks for the per-queue and per-job-name duration percentiles and newest jobs.
/// </summary>
internal sealed record JobStatsFilter(
    DateTimeOffset From, DateTimeOffset To, int Buckets, string? Queue, string? JobName, Guid? Worker, IReadOnlyList<string>? Queues, bool Groups)
{
    public long FromMs => From.ToUnixTimeMilliseconds();

    public long SpanMs => To.ToUnixTimeMilliseconds() - FromMs;

    public int Slice(DateTimeOffset t) => (int)((t.ToUnixTimeMilliseconds() - FromMs) * Buckets / SpanMs);

    public bool Contains(DateTimeOffset t) => t >= From && t < To;

    public DateTimeOffset SliceStart(int i) => DateTimeOffset.FromUnixTimeMilliseconds(FromMs + ((i * SpanMs) + Buckets - 1) / Buckets);

    public bool Allows(string queue) =>
        (Queue is null || Queue == queue) && (Queues is null || Queues.Contains(queue, StringComparer.Ordinal));
}

/// <summary>Jobs created in the range by queue, name, current state and creation slice; retries = executions beyond the first.</summary>
internal readonly record struct JobStatsCreated(string Queue, string JobName, string State, int Slice, long Jobs, long Retries);

/// <summary>Jobs that ended in the range by queue, name, final state and end slice.</summary>
internal readonly record struct JobStatsEnded(string Queue, string JobName, string State, int Slice, long Jobs);

/// <summary>
/// Duration percentiles of jobs that ended in the range: <see cref="Queue"/>/<see cref="JobName"/> null = across them,
/// <see cref="Slice"/> null = the whole range. Only the overall, per-slice, and (when grouped) per-queue and per-name sets.
/// </summary>
internal readonly record struct JobStatsDuration(string? Queue, string? JobName, int? Slice, long Count, double? MedianMs, double? P95Ms, double? MaxMs);

/// <summary>The newest job created in the range of one queue and name.</summary>
internal readonly record struct JobStatsLast(string Queue, string JobName, DateTimeOffset CreatedAt, string State, Guid JobId);

/// <summary>Live jobs by queue, name and kind (pending, scheduled, reserved, running); oldest eligible time for pending.</summary>
internal readonly record struct JobStatsLive(string Queue, string JobName, string Kind, long Jobs, DateTimeOffset? OldestEligibleAt);

internal sealed record JobStatsCells(
    IReadOnlyList<JobStatsCreated> Created, IReadOnlyList<JobStatsEnded> Ended, IReadOnlyList<JobStatsDuration> Durations,
    IReadOnlyList<JobStatsLast> Last, IReadOnlyList<JobStatsLive> Live, IReadOnlyList<JobErrorCount> ErrorCodes)
{
    public const int TopErrorCodes = 10;

    /// <summary>
    /// Groups per-job rows. <paramref name="created"/>: jobs created in the range; <paramref name="ended"/>: jobs that ended in it, with
    /// their duration when they ran; <paramref name="live"/>: live jobs with their kind; <paramref name="errorCodes"/>: one code per
    /// attempt that finished in the range with an error. Callers filter by queue, name and worker first.
    /// </summary>
    public static JobStatsCells From(
        JobStatsFilter f, IEnumerable<(string Queue, string JobName, string State, DateTimeOffset CreatedAt, long ExecutionCount, Guid JobId)> created,
        IEnumerable<(string Queue, string JobName, string State, DateTimeOffset TerminalAt, double? DurationMs)> ended,
        IEnumerable<(string Queue, string JobName, string Kind, DateTimeOffset EligibleAt)> live, IEnumerable<string> errorCodes)
    {
        var c = new Dictionary<(string, string, string, int), (long Jobs, long Retries)>();
        var last = new Dictionary<(string, string), JobStatsLast>();
        foreach (var j in created)
        {
            var k = (j.Queue, j.JobName, j.State, f.Slice(j.CreatedAt));
            var v = c.GetValueOrDefault(k);
            c[k] = (v.Jobs + 1, v.Retries + Math.Max(0, j.ExecutionCount - 1));
            if (!last.TryGetValue((j.Queue, j.JobName), out var l) || (j.CreatedAt, j.JobId).CompareTo((l.CreatedAt, l.JobId)) > 0)
            {
                last[(j.Queue, j.JobName)] = new JobStatsLast(j.Queue, j.JobName, j.CreatedAt, j.State, j.JobId);
            }
        }

        var e = new Dictionary<(string, string, string, int), long>();
        var d = new Dictionary<(string?, string?, int?), List<double>>();
        void Add((string?, string?, int?) key, double ms) => (d.TryGetValue(key, out var list) ? list : d[key] = []).Add(ms);
        foreach (var j in ended)
        {
            var slice = f.Slice(j.TerminalAt);
            var k = (j.Queue, j.JobName, j.State, slice);
            e[k] = e.GetValueOrDefault(k) + 1;
            if (j.DurationMs is { } ms)
            {
                Add((null, null, null), ms);
                Add((null, null, slice), ms);
                if (f.Groups)
                {
                    Add((j.Queue, null, null), ms);
                    Add((j.Queue, null, slice), ms);
                    Add((null, j.JobName, null), ms);
                    Add((null, j.JobName, slice), ms);
                }
            }
        }

        var n = new Dictionary<(string, string, string), (long Jobs, DateTimeOffset? Oldest)>();
        foreach (var j in live)
        {
            var k = (j.Queue, j.JobName, j.Kind);
            var v = n.GetValueOrDefault(k);
            n[k] = (v.Jobs + 1, j.Kind == "pending" && (v.Oldest is null || j.EligibleAt < v.Oldest) ? j.EligibleAt : v.Oldest);
        }

        return new JobStatsCells(
            [.. c.Select(x => new JobStatsCreated(x.Key.Item1, x.Key.Item2, x.Key.Item3, x.Key.Item4, x.Value.Jobs, x.Value.Retries))],
            [.. e.Select(x => new JobStatsEnded(x.Key.Item1, x.Key.Item2, x.Key.Item3, x.Key.Item4, x.Value))],
            [.. d.Select(x =>
            {
                x.Value.Sort();
                return new JobStatsDuration(x.Key.Item1, x.Key.Item2, x.Key.Item3, x.Value.Count, Percentile(x.Value, 0.5), Percentile(x.Value, 0.95), x.Value[^1]);
            })],
            [.. last.Values],
            [.. n.Select(x => new JobStatsLive(x.Key.Item1, x.Key.Item2, x.Key.Item3, x.Value.Jobs, x.Value.Oldest))],
            TopCodes(errorCodes.GroupBy(x => x, StringComparer.Ordinal).Select(g => new JobErrorCount(g.Key, g.LongCount()))));
    }

    public static IReadOnlyList<JobErrorCount> TopCodes(IEnumerable<JobErrorCount> codes) =>
        [.. codes.OrderByDescending(x => x.Count).ThenBy(x => x.Code, StringComparer.Ordinal).Take(TopErrorCodes)];

    /// <summary>Postgres <c>percentile_cont</c>: linear interpolation at <c>p * (n - 1)</c> over the sorted values.</summary>
    public static double Percentile(List<double> sorted, double p)
    {
        var pos = p * (sorted.Count - 1);
        var lo = (int)Math.Floor(pos);
        var hi = Math.Min(lo + 1, sorted.Count - 1);
        return sorted[lo] + ((sorted[hi] - sorted[lo]) * (pos - lo));
    }
}
