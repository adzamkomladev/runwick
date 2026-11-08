using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

internal sealed partial class SqliteStore : ITelemetrySampleStore
{
    /// <summary>
    /// The collector sample (18 §4) as PgStore.Telemetry, in one read snapshot: per queue a capped due count and the oldest due
    /// instant (one <c>jobs_ready</c> lookup per priority), plus the application's schedule review count and oldest overdue instant.
    /// </summary>
    public Task<ApplicationSample> SampleAsync(string application, IReadOnlyList<string> queues, CancellationToken ct) =>
        runner.ReadAsync(conn =>
        {
            var observed = Now();
            var samples = queues.Select(queue => Rows(Cmd(conn, application, """
                SELECT (SELECT count(*) FROM (
                            SELECT 1 FROM runwick_jobs
                            WHERE application = @application AND queue = @queue
                              AND state IN ('pending', 'retrying') AND eligible_at <= @now
                            LIMIT @max_due)),
                       (SELECT min(o) FROM (
                            SELECT (SELECT eligible_at FROM runwick_jobs
                                    WHERE application = @application AND queue = @queue AND priority = p.value
                                      AND state IN ('pending', 'retrying') AND eligible_at <= @now
                                    ORDER BY eligible_at LIMIT 1) AS o
                            FROM json_each('["high","medium","low"]') AS p))
                """).P("queue", queue).P("now", observed).P("max_due", ITelemetrySampleStore.MaxDue),
                r => new QueueSample(queue, r.GetInt64(0), r.TimeN(1)))[0]).ToList();
            var (needsReview, overdue) = Rows(Cmd(conn, application, """
                SELECT count(*) FILTER (WHERE needs_review),
                       min(next_due_at) FILTER (WHERE enabled AND NOT paused AND NOT needs_review AND next_due_at <= @now)
                FROM runwick_schedules WHERE application = @application
                """).P("now", observed), r => (r.GetInt64(0), r.TimeN(1)))[0];
            return new ApplicationSample(observed, samples, needsReview, overdue);
        }, ct);
}
