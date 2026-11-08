using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Runwick.ReferenceModel;

namespace Runwick.Harness.Store;

/// <summary>
/// The SQLite form of <see cref="SnapshotReader"/> (20 §12): every <c>runwick_</c> table in one read transaction, normalized
/// the same way (UUID blobs as UUIDs, instants as epoch milliseconds), plus the ReferenceModel view.
/// </summary>
internal static class SqliteSnapshotReader
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
        ("rate_scopes", "application, rate_scope_id"),
        ("job_rate_requirements", "application, job_id, rate_scope_id"),
        ("rate_grants", "application, rate_scope_id, attempt_id"),
        ("dispatch_keys", "application, job_name, key_kind, key"),
        ("storage_commands", "application, retention_class, command_id"),
        ("queue_events", "event_id"),
    ];

    static SqliteConnection Open(string path)
    {
        var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadWrite, Pooling = false }.ConnectionString);
        conn.Open();
        return conn;
    }

    public static HarnessSnapshot Read(string path)
    {
        using var conn = Open(path);
        using var tx = conn.BeginTransaction(deferred: true);
        var readAt = DateTimeOffset.UtcNow;
        var tables = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        foreach (var (table, order) in Tables)
        {
            tables[table] = Query(conn, tx, $"SELECT * FROM runwick_{table} ORDER BY {order}", r =>
            {
                var sb = new StringBuilder();
                for (var i = 0; i < r.FieldCount; i++)
                {
                    sb.Append(r.GetName(i)).Append('=').Append(Format(r.GetName(i), r.IsDBNull(i) ? null : r.GetValue(i))).Append(';');
                }

                return sb.ToString();
            });
        }

        static Guid G(SqliteDataReader r, int i) => new((byte[])r.GetValue(i), bigEndian: true);
        static Guid? Gn(SqliteDataReader r, int i) => r.IsDBNull(i) ? null : G(r, i);
        static DateTimeOffset T(SqliteDataReader r, int i) => DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(i));
        static DateTimeOffset? Tn(SqliteDataReader r, int i) => r.IsDBNull(i) ? null : T(r, i);

        var jobs = Query(conn, tx, """
            SELECT j.job_id, j.state, j.generation, j.revision, g.started_count, j.execution_count, g.consecutive_unstarted_count,
                   g.max_executions, g.max_unstarted_abandonments, j.current_attempt_id, j.ownership_token, j.worker_instance_id,
                   j.lease_expires_at, j.cancel_requested_at IS NOT NULL, j.eligible_at, j.queue, j.terminal_reason
            FROM runwick_jobs AS j
            JOIN runwick_job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
            ORDER BY j.job_id
            """, r => new JobRow(G(r, 0), r.GetString(1), r.GetInt32(2), r.GetInt64(3), r.GetInt32(4), r.GetInt64(5), r.GetInt32(6),
                r.GetInt32(7), r.GetInt32(8), Gn(r, 9), Gn(r, 10), Gn(r, 11), Tn(r, 12), r.GetInt64(13) != 0, T(r, 14), r.GetString(15),
                r.IsDBNull(16) ? null : r.GetString(16)));
        var attempts = Query(conn, tx, """
            SELECT attempt_id, job_id, generation, reservation_number, execution_number, outcome, reserved_at, authorized_at, finished_at
            FROM runwick_job_attempts ORDER BY job_id, reservation_number
            """, r => new AttemptRow(G(r, 0), G(r, 1), r.GetInt32(2), r.GetInt64(3), r.IsDBNull(4) ? null : r.GetInt32(4),
                r.IsDBNull(5) ? null : r.GetString(5), T(r, 6), Tn(r, 7), Tn(r, 8)));
        var permits = Query(conn, tx, "SELECT scope_id, job_id, attempt_id, lease_expires_at FROM runwick_concurrency_permits ORDER BY scope_id, attempt_id",
            r => new PermitRow(G(r, 0), G(r, 1), G(r, 2), T(r, 3)));
        var keys = Query(conn, tx, """
            SELECT key_kind, key, bound_job_id, key_kind = 'active', expires_at
            FROM runwick_dispatch_keys WHERE bound_job_id IS NOT NULL ORDER BY job_name, key_kind, key
            """, r => new DispatchKeyRow(r.GetString(0), r.GetString(1), G(r, 2), r.GetInt64(3) != 0, Tn(r, 4)));
        var receipts = Query(conn, tx,
            "SELECT command_id, operation, retention_class, result IS NOT NULL FROM runwick_storage_commands ORDER BY retention_class, command_id",
            r => new ReceiptRow(G(r, 0), r.GetString(1), r.GetString(2), r.GetInt64(3) != 0));
        tx.Commit();
        return new HarnessSnapshot(new StoreSnapshot(readAt, jobs, attempts, permits, keys, receipts), tables);
    }

    /// <summary>One scalar from SQLite admin SQL (assertions only); UUID parameters bind as 16-byte big-endian blobs.</summary>
    public static long Count(string path, string sql, params (string Name, object Value)[] parameters)
    {
        using var conn = Open(path);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name.StartsWith('@') ? name : "@" + name, value is Guid g ? g.ToByteArray(bigEndian: true) : value);
        }

        return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    /// <summary>Admin SQL against the SQLite file (setup only); UUID parameters bind as 16-byte big-endian blobs.</summary>
    public static void Exec(string path, string sql, params (string Name, object Value)[] parameters)
    {
        using var conn = Open(path);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name.StartsWith('@') ? name : "@" + name, value is Guid g ? g.ToByteArray(bigEndian: true) : value);
        }

        cmd.ExecuteNonQuery();
    }

    static string Format(string column, object? value) => value switch
    {
        null => "∅",
        byte[] b when column == "payload_utf8" => $"len:{b.Length},sha256:{Convert.ToHexStringLower(SHA256.HashData(b))}",
        byte[] { Length: 16 } b when column != "fingerprint" => new Guid(b, bigEndian: true).ToString("D"),
        byte[] b => Convert.ToHexStringLower(b),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    static List<T> Query<T>(SqliteConnection conn, SqliteTransaction tx, string sql, Func<SqliteDataReader, T> map)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var list = new List<T>();
        while (r.Read())
        {
            list.Add(map(r));
        }

        return list;
    }
}
