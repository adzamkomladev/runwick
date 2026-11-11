using System.Data;
using Npgsql;
using Runwick.Management;
using static Runwick.Management.StoreSnapshotScope;
using static Runwick.Management.StoreSnapshotStatus;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IPostgresStoreDiagnosticsSource
{
    async Task<PostgresStoreDiagnostics> IPostgresStoreDiagnosticsSource.ObserveDiagnosticsAsync(CancellationToken ct)
    {
        var gate = sources.Gate(PgLane.Background);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var conn = await sources.For(PgLane.Background).OpenConnectionAsync(ct).ConfigureAwait(false);
            await using (conn.ConfigureAwait(false))
            {
                return new(
                    await ReadAsync(StoreSnapshotScope.Store,
                        "SELECT numbackends FROM pg_catalog.pg_stat_database WHERE datname = pg_catalog.current_database()",
                        r => new PostgresSessionCount(r.GetInt32(0))).ConfigureAwait(false),
                    await ReadAsync(StoreSnapshotScope.Store, """
                        SELECT CASE WHEN pg_catalog.current_setting('track_activities') = 'on' THEN
                            (SELECT count(*) FROM pg_catalog.pg_stat_activity
                             WHERE datname = pg_catalog.current_database() AND wait_event_type = 'Lock')
                        END
                        WHERE pg_catalog.pg_has_role('pg_read_all_stats', 'USAGE')
                        """, r => new PostgresLockWaitCount(r.GetInt64(0)), PermissionDenied).ConfigureAwait(false),
                    await ReadAsync(StoreSnapshotScope.Store, "SELECT pg_catalog.pg_database_size(pg_catalog.current_database())",
                        r => new StoreByteCount(r.GetInt64(0))).ConfigureAwait(false),
                    await ReadAsync(StoreSnapshotScope.Store, """
                        SELECT pg_catalog.pg_total_relation_size(relid), pg_catalog.pg_stat_get_dead_tuples(relid),
                               pg_catalog.pg_stat_get_autovacuum_count(relid)
                        FROM (SELECT pg_catalog.to_regclass('runwick.jobs') AS relid) AS jobs
                        WHERE relid IS NOT NULL AND pg_catalog.current_setting('track_counts') = 'on'
                        """, r => new PostgresJobTableStats(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2))).ConfigureAwait(false),
                    await ReadAsync(ProviderNode, "SELECT wal_bytes, stats_reset FROM pg_catalog.pg_stat_wal",
                        r => new PostgresWalStats(r.GetDecimal(0), r.IsDBNull(1) ? null : r.GetFieldValue<DateTimeOffset>(1))).ConfigureAwait(false),
                    await ReadAsync(ProviderNode, "SELECT pg_catalog.pg_is_in_recovery()",
                        r => new PostgresRecoveryState(r.GetBoolean(0))).ConfigureAwait(false),
                    await ReadAsync(ProviderNode, """
                        SELECT senders, backlog
                        FROM (SELECT count(*) AS senders, max(pg_catalog.pg_wal_lsn_diff(sent_lsn, replay_lsn)) AS backlog
                              FROM pg_catalog.pg_stat_replication) AS replication
                        WHERE pg_catalog.pg_has_role('pg_read_all_stats', 'USAGE')
                        """, r => new PostgresReplicationStats(r.GetInt64(0), r.IsDBNull(1) ? null : r.GetDecimal(1)),
                        PermissionDenied).ConfigureAwait(false));
            }

            async Task<StoreSnapshotFact<T>> ReadAsync<T>(StoreSnapshotScope scope, string sql, Func<NpgsqlDataReader, T> map,
                StoreSnapshotStatus noRow = Unavailable) where T : class
            {
                try
                {
                    using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = 1 };
                    var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow, ct).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                        {
                            return new(scope, noRow, null);
                        }

                        return await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)
                            ? new(scope, Unavailable, null) : new(scope, Available, map(reader));
                    }
                }
                catch (PostgresException ex) when (ex.SqlState is "42501" or "42P01" or "42703" or "42883")
                {
                    return new(scope, ex.SqlState == "42501" ? PermissionDenied : Unsupported, null);
                }
            }
        }
        finally
        {
            gate.Release();
        }
    }
}
