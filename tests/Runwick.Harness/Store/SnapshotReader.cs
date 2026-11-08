using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Npgsql;
using Runwick.ReferenceModel;

namespace Runwick.Harness.Store;

/// <summary>A committed snapshot: the reference-model view plus every table's normalized rows (07 §2.2).</summary>
internal sealed record HarnessSnapshot(StoreSnapshot Model, IReadOnlyDictionary<string, IReadOnlyList<string>> Tables)
{
    public IReadOnlyList<string> this[string table] => Tables[table];

    /// <summary>Tables whose normalized rows differ between two snapshots (a Redis key that exists in only one counts as changed).</summary>
    public IReadOnlyList<string> DiffTables(HarnessSnapshot other) =>
        [.. Tables.Keys.Union(other.Tables.Keys).Where(t => !Tables.TryGetValue(t, out var a) || !other.Tables.TryGetValue(t, out var b) || !a.SequenceEqual(b))];
}

internal static class SnapshotReader
{
    static readonly (string Table, string OrderBy)[] Tables =
    [
        ("store_metadata", "singleton_id"),
        ("application_controls", "application"),
        ("queue_controls", "application, queue"),
        ("jobs", "application, job_id"),
        ("job_payloads", "application, job_id"),
        ("job_generations", "application, job_id, generation"),
        ("job_attempts", "application, attempt_id"),
        ("concurrency_scopes", "application, scope_id"),
        ("job_concurrency_requirements", "application, job_id, scope_id"),
        ("concurrency_permits", "application, scope_id, attempt_id"),
        ("dispatch_keys", "application, job_name, key_kind, key"),
        ("storage_commands", "application, retention_class, command_id"),
        ("outbox_deliveries", "application, source_store_id, message_id"),
        ("schedules", "application, schedule_id"),
        ("schedule_definitions", "application, schedule_id, revision"),
        ("schedule_occurrences", "application, schedule_id, occurrence_key"),
        ("schedule_audit_ranges", "application, audit_id"),
        ("pool_controls", "application, instance_id, pool_name"),
        ("scaling_events", "application, event_id"),
        ("queue_events", "event_id"),
    ];

    public static async Task<HarnessSnapshot> ReadAsync(string connectionString)
    {
        var conn = new NpgsqlConnection(connectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead).ConfigureAwait(false);
            await using (tx.ConfigureAwait(false))
            {
                using (var ro = new NpgsqlCommand("SET TRANSACTION READ ONLY", conn, tx))
                {
                    await ro.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                DateTimeOffset readAt;
                using (var now = new NpgsqlCommand("SELECT clock_timestamp()", conn, tx))
                {
                    readAt = new DateTimeOffset(DateTime.SpecifyKind((DateTime)(await now.ExecuteScalarAsync().ConfigureAwait(false))!, DateTimeKind.Utc));
                }

                var tables = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
                foreach (var (table, order) in Tables)
                {
                    using var cmd = new NpgsqlCommand($"SELECT * FROM runwick.{table} ORDER BY {order}", conn, tx);
                    var rows = new List<string>();
                    var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            var sb = new StringBuilder();
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                sb.Append(reader.GetName(i)).Append('=').Append(Format(reader.GetName(i), await reader.IsDBNullAsync(i).ConfigureAwait(false) ? null : reader.GetValue(i))).Append(';');
                            }

                            rows.Add(sb.ToString());
                        }
                    }

                    tables[table] = rows;
                }

                var model = await ReadModelAsync(conn, tx, readAt).ConfigureAwait(false);
                return new HarnessSnapshot(model, tables);
            }
        }
    }

    static string Format(string column, object? value) => value switch
    {
        null => "∅",
        byte[] b when column == "payload_utf8" => $"len:{b.Length},sha256:{Convert.ToHexStringLower(SHA256.HashData(b))}",
        byte[] b => Convert.ToHexStringLower(b),
        DateTime d => new DateTimeOffset(DateTime.SpecifyKind(d, DateTimeKind.Utc)).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
        Guid g => g.ToString("D"),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>Only the ReferenceModel view, in one REPEATABLE READ snapshot (workload end checks over large stores).</summary>
    public static async Task<StoreSnapshot> ReadModelAsync(string connectionString)
    {
        var conn = new NpgsqlConnection(connectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead).ConfigureAwait(false);
            await using (tx.ConfigureAwait(false))
            {
                using var now = new NpgsqlCommand("SELECT clock_timestamp()", conn, tx);
                var readAt = new DateTimeOffset(DateTime.SpecifyKind((DateTime)(await now.ExecuteScalarAsync().ConfigureAwait(false))!, DateTimeKind.Utc));
                return await ReadModelAsync(conn, tx, readAt).ConfigureAwait(false);
            }
        }
    }

    static async Task<StoreSnapshot> ReadModelAsync(NpgsqlConnection conn, NpgsqlTransaction tx, DateTimeOffset readAt)
    {
        static DateTimeOffset T(NpgsqlDataReader r, int i) => new(DateTime.SpecifyKind(r.GetDateTime(i), DateTimeKind.Utc));
        static DateTimeOffset? Tn(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : T(r, i);
        static Guid? Gn(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetGuid(i);

        var jobs = await QueryAsync(conn, tx, """
            SELECT j.job_id, j.state, j.generation, j.revision, g.started_count, j.execution_count, g.consecutive_unstarted_count,
                   g.max_executions, g.max_unstarted_abandonments, j.current_attempt_id, j.ownership_token, j.worker_instance_id,
                   j.lease_expires_at, j.cancel_requested_at IS NOT NULL, j.eligible_at, j.queue, j.terminal_reason
            FROM runwick.jobs AS j
            JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
            ORDER BY j.job_id
            """, r => new JobRow(r.GetGuid(0), r.GetString(1), r.GetInt32(2), r.GetInt64(3), r.GetInt32(4), r.GetInt64(5), r.GetInt32(6),
                r.GetInt32(7), r.GetInt32(8), Gn(r, 9), Gn(r, 10), Gn(r, 11), Tn(r, 12), r.GetBoolean(13), T(r, 14), r.GetString(15),
                r.IsDBNull(16) ? null : r.GetString(16))).ConfigureAwait(false);
        var attempts = await QueryAsync(conn, tx, """
            SELECT attempt_id, job_id, generation, reservation_number, execution_number, outcome, reserved_at, authorized_at, finished_at
            FROM runwick.job_attempts ORDER BY job_id, reservation_number
            """, r => new AttemptRow(r.GetGuid(0), r.GetGuid(1), r.GetInt32(2), r.GetInt64(3), r.IsDBNull(4) ? null : r.GetInt32(4),
                r.IsDBNull(5) ? null : r.GetString(5), T(r, 6), Tn(r, 7), Tn(r, 8))).ConfigureAwait(false);
        var permits = await QueryAsync(conn, tx,
            "SELECT scope_id, job_id, attempt_id, lease_expires_at FROM runwick.concurrency_permits ORDER BY scope_id, attempt_id",
            r => new PermitRow(r.GetGuid(0), r.GetGuid(1), r.GetGuid(2), T(r, 3))).ConfigureAwait(false);
        var keys = await QueryAsync(conn, tx, """
            SELECT key_kind, key, bound_job_id, key_kind = 'active', expires_at
            FROM runwick.dispatch_keys WHERE bound_job_id IS NOT NULL ORDER BY job_name, key_kind, key
            """, r => new DispatchKeyRow(r.GetString(0), r.GetString(1), r.GetGuid(2), r.GetBoolean(3), Tn(r, 4))).ConfigureAwait(false);
        var receipts = await QueryAsync(conn, tx,
            "SELECT command_id, operation, retention_class, result IS NOT NULL FROM runwick.storage_commands ORDER BY retention_class, command_id",
            r => new ReceiptRow(r.GetGuid(0), r.GetString(1), r.GetString(2), r.GetBoolean(3))).ConfigureAwait(false);
        return new StoreSnapshot(readAt, jobs, attempts, permits, keys, receipts);
    }

    static async Task<List<T>> QueryAsync<T>(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, Func<NpgsqlDataReader, T> map)
    {
        using var cmd = new NpgsqlCommand(sql, conn, tx);
        var list = new List<T>();
        var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                list.Add(map(reader));
            }
        }

        return list;
    }
}
