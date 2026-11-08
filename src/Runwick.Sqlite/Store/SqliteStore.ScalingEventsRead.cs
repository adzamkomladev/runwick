using System.Text;
using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

// SP-024 (21 §6): IManagementQueryStore.ListScalingEventsAsync on SQLite. The interface is declared on the management part
// (SqliteStore.Management.cs), which lands separately; this part only carries the method.
internal sealed partial class SqliteStore
{
    public Task<StoreRead<IReadOnlyList<ScalingEventSummary>>> ListScalingEventsAsync(string application, ScalingEventFilter filter,
        (DateTimeOffset CreatedAt, Guid EventId)? after, int limit, CancellationToken ct) =>
        runner.ReadAsync(conn =>
        {
            var observed = Now();
            var sql = new StringBuilder("""
                SELECT event_id, instance_id, pool_name, old_target, new_target, desired, candidate, reason, snapshot_at, created_at
                FROM runwick_scaling_events WHERE application = @application AND created_at >= @from AND created_at <= @to
                """)
                .Append(filter.InstanceId is null ? "" : " AND instance_id = @instance")
                .Append(filter.Pool is null ? "" : " AND pool_name = @pool")
                .Append(after is null ? "" : " AND (created_at < @after_at OR (created_at = @after_at AND event_id < @after_id))")
                .Append(" ORDER BY created_at DESC, event_id DESC LIMIT @limit");
            var cmd = Cmd(conn, application, sql.ToString()).P("from", filter.From).P("to", filter.To).P("limit", limit);
            if (filter.InstanceId is { } instance)
            {
                cmd.P("instance", instance);
            }

            if (filter.Pool is { } pool)
            {
                cmd.P("pool", pool);
            }

            if (after is { } a)
            {
                cmd.P("after_at", a.CreatedAt).P("after_id", a.EventId);
            }

            IReadOnlyList<ScalingEventSummary> rows = Rows(cmd, r => new ScalingEventSummary(r.Uuid(0), r.Uuid(1), r.GetString(2), r.GetInt32(3),
                r.GetInt32(4), r.IsDBNull(5) ? null : r.GetInt32(5), r.IsDBNull(6) ? null : r.GetInt32(6), r.GetString(7),
                r.IsDBNull(8) ? null : r.Time(8), r.Time(9)));
            return new StoreRead<IReadOnlyList<ScalingEventSummary>>(rows, observed);
        }, ct);
}
