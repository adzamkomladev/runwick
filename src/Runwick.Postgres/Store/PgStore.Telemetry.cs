using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : ITelemetrySampleStore
{
    /// <summary>
    /// The collector sample (18 §4): per queue a due count capped at 10,000 and the oldest due instant (one <c>jobs_ready</c>
    /// lookup per priority), plus the application's schedule review count and oldest overdue instant.
    /// </summary>
    public Task<ApplicationSample> SampleAsync(string application, IReadOnlyList<string> queues, CancellationToken ct) =>
        ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand("""
                SELECT now() AS observed_at, q.queue,
                       (SELECT count(*) FROM (
                            SELECT 1 FROM runwick.jobs AS j
                            WHERE j.application = @application AND j.queue = q.queue
                              AND j.state IN ('pending', 'retrying') AND j.eligible_at <= now()
                            LIMIT @max_due) AS d) AS due,
                       (SELECT min(o.eligible_at)
                        FROM unnest(ARRAY['high', 'medium', 'low']) AS p(priority)
                        CROSS JOIN LATERAL (
                            SELECT j.eligible_at FROM runwick.jobs AS j
                            WHERE j.application = @application AND j.queue = q.queue AND j.priority = p.priority
                              AND j.state IN ('pending', 'retrying') AND j.eligible_at <= now()
                            ORDER BY j.eligible_at
                            LIMIT 1) AS o) AS oldest,
                       s.needs_review, s.oldest_overdue
                FROM (SELECT count(*) FILTER (WHERE x.needs_review) AS needs_review,
                             min(x.next_due_at) FILTER (WHERE x.enabled AND NOT x.paused AND NOT x.needs_review AND x.next_due_at <= now())
                                 AS oldest_overdue
                      FROM runwick.schedules AS x WHERE x.application = @application) AS s
                LEFT JOIN unnest(@queues::text[]) AS q(queue) ON true
                """, conn);
            cmd.P("application", application).P("max_due", ITelemetrySampleStore.MaxDue).Arr("queues", [.. queues], NpgsqlDbType.Text);
            var samples = new List<QueueSample>();
            DateTimeOffset observed = default;
            long needsReview = 0;
            DateTimeOffset? overdue = null;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    observed = PgTime.Read(reader, 0);
                    needsReview = reader.GetInt64(4);
                    overdue = await reader.IsDBNullAsync(5, ct).ConfigureAwait(false) ? null : PgTime.Read(reader, 5);
                    if (!await reader.IsDBNullAsync(1, ct).ConfigureAwait(false))
                    {
                        samples.Add(new QueueSample(reader.GetString(1), reader.GetInt64(2),
                            await reader.IsDBNullAsync(3, ct).ConfigureAwait(false) ? null : PgTime.Read(reader, 3)));
                    }
                }
            }

            return new ApplicationSample(observed, samples, needsReview, overdue);
        }, ct);
}
