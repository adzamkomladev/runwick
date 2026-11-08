using System.Globalization;
using Microsoft.Data.Sqlite;
using Runwick.Sqlite.Store;

namespace Runwick;

/// <summary>Operator admission, status and identity commands of the SQLite store (23). They run in one <c>BEGIN IMMEDIATE</c>, like the migrator.</summary>
public static partial class RunwickSqliteMigrations
{
    /// <summary>
    /// Sets one application's (or, with <paramref name="application"/> null, every registered application's) admission mode.
    /// <c>BEGIN IMMEDIATE</c> waits for every writer, including a caller's open staging transaction, at most 5 s, then
    /// fails with <c>migration_lock_timeout</c>.
    /// </summary>
    public static Task<IReadOnlyList<AdmissionChange>> SetAdmissionModeAsync(
        string connectionString, string? application, string mode, string actor, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        return Task.Run<IReadOnlyList<AdmissionChange>>(() => SqliteAdmin.InTransaction(connectionString, true, (conn, now) =>
        {
            var rows = new List<(string App, long Revision, string Mode)>();
            using (var select = conn.CreateCommand())
            {
                select.CommandText = "SELECT application, revision, admission_mode FROM runwick_application_controls WHERE @app IS NULL OR application = @app ORDER BY application";
                select.P("app", application);
                using var r = select.ExecuteReader();
                while (r.Read())
                {
                    rows.Add((r.GetString(0), r.GetInt64(1), r.GetString(2)));
                }
            }

            if (application is not null && rows.Count == 0)
            {
                throw new SqliteMigrationException("application_not_registered", $"Application {application} is not registered.");
            }

            var changes = new List<AdmissionChange>();
            foreach (var (app, revision, previous) in rows)
            {
                if (previous == mode)
                {
                    changes.Add(new AdmissionChange(app, previous, mode, revision, false));
                    continue;
                }

                if (StoreAdminRules.Check(previous, mode) is { } code)
                {
                    throw new SqliteMigrationException(code, code == "drain_first"
                        ? $"Application {app} is open: drain it and let work settle before maintenance."
                        : "The mode must be open, drain or maintenance.");
                }

                using var update = conn.CreateCommand();
                update.CommandText = "UPDATE runwick_application_controls SET admission_mode = @mode, revision = revision + 1, updated_at = @now, actor_subject = @actor WHERE application = @app RETURNING revision";
                update.P("mode", mode).P("now", now).P("actor", actor).P("app", app);
                changes.Add(new AdmissionChange(app, previous, mode, (long)update.ExecuteScalar()!, true));
            }

            return changes;
        }), cancellationToken);
    }

    /// <summary>
    /// Raises one capability (<c>scheduler</c> or <c>rate_admission</c>) for one application, or every registered application when
    /// <paramref name="application"/> is null (10 §6.1, 23 §3.6). Never lowers one; a repeat reports <c>Changed = false</c>. Unlike the
    /// store operation this writes no audit row or event, like <see cref="SetAdmissionModeAsync"/>; it records <paramref name="actor"/>.
    /// </summary>
    public static Task<IReadOnlyList<CapabilityChange>> ActivateCapabilityAsync(
        string connectionString, string? application, string capability, string actor, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        if (StoreAdminRules.CheckCapability(capability) is { } bad)
        {
            throw new SqliteMigrationException(bad, "The capability must be scheduler or rate_admission.");
        }

        var version = StoreAdminRules.CapabilityVersion(capability);
        return Task.Run<IReadOnlyList<CapabilityChange>>(() => SqliteAdmin.InTransaction(connectionString, true, (conn, now) =>
        {
            var rows = new List<(string App, long Revision, string Required)>();
            using (var select = conn.CreateCommand())
            {
                select.CommandText = "SELECT application, revision, required_capabilities FROM runwick_application_controls WHERE @app IS NULL OR application = @app ORDER BY application";
                select.P("app", application);
                using var r = select.ExecuteReader();
                while (r.Read())
                {
                    rows.Add((r.GetString(0), r.GetInt64(1), r.GetString(2)));
                }
            }

            if (application is not null && rows.Count == 0)
            {
                throw new SqliteMigrationException("application_not_registered", $"Application {application} is not registered.");
            }

            var changes = new List<CapabilityChange>();
            foreach (var (app, revision, required) in rows)
            {
                if (StoreAdminRules.Raise(required, capability, version) is not { } raised)
                {
                    changes.Add(new CapabilityChange(app, capability, version, revision, false));
                    continue;
                }

                using var update = conn.CreateCommand();
                update.CommandText = "UPDATE runwick_application_controls SET required_capabilities = @caps, revision = revision + 1, updated_at = @now, actor_subject = @actor WHERE application = @app RETURNING revision";
                update.P("caps", raised).P("now", now).P("actor", actor).P("app", app);
                changes.Add(new CapabilityChange(app, capability, version, (long)update.ExecuteScalar()!, true));
            }

            return changes;
        }), cancellationToken);
    }

    /// <summary>Reads the store's revision, mode, application gates and the lease and presence rows an operator checks before acting (23 §6).</summary>
    public static Task<StoreAdminStatus> GetStatusAsync(string connectionString, CancellationToken cancellationToken = default) =>
        Task.Run(() => SqliteAdmin.InTransaction(connectionString, false, (conn, now) =>
        {
            Guid id;
            long revision;
            string mode;
            using (var meta = conn.CreateCommand())
            {
                meta.CommandText = "SELECT store_id, schema_revision, migration_mode FROM runwick_store_metadata WHERE singleton_id = 1";
                using var r = meta.ExecuteReader();
                r.Read();
                (id, revision, mode) = (SqUuid.Read((byte[])r.GetValue(0)), r.GetInt64(1), r.GetString(2));
            }

            var apps = new List<ApplicationAdmissionState>();
            using (var list = conn.CreateCommand())
            {
                list.CommandText = "SELECT application, admission_mode, revision, required_capabilities FROM runwick_application_controls ORDER BY application";
                using var r = list.ExecuteReader();
                while (r.Read())
                {
                    apps.Add(new ApplicationAdmissionState(r.GetString(0), r.GetString(1), r.GetInt64(2), r.GetString(3)));
                }
            }

            using var counts = conn.CreateCommand();
            counts.CommandText = """
                SELECT (SELECT count(*) FROM runwick_jobs WHERE lease_expires_at IS NOT NULL),
                       (SELECT count(*) FROM runwick_worker_instances),
                       (SELECT count(*) FROM runwick_worker_instances WHERE expires_at <= @now)
                """;
            counts.P("now", now);
            using var c = counts.ExecuteReader();
            c.Read();
            return new StoreAdminStatus(id, revision, Runwick.Sqlite.Migrations.SqliteMigrator.CurrentRevision, mode, apps,
                (int)c.GetInt64(0), (int)c.GetInt64(1), (int)c.GetInt64(2), StoreAdminRules.QuiescenceNotice);
        }), cancellationToken);

    /// <summary>
    /// Gives a copied or restored file a new logical store identity (23 §7). Refused unless every application is in
    /// <c>maintenance</c> and no unexpired lease or worker presence is recorded.
    /// </summary>
    public static Task<StoreIdentityChange> ReassignStoreIdAsync(string connectionString, CancellationToken cancellationToken = default) =>
        Task.Run(() => SqliteAdmin.InTransaction(connectionString, true, (conn, now) =>
        {
            var open = new List<string>();
            using (var apps = conn.CreateCommand())
            {
                apps.CommandText = "SELECT application FROM runwick_application_controls WHERE admission_mode <> 'maintenance' ORDER BY application";
                using var r = apps.ExecuteReader();
                while (r.Read())
                {
                    open.Add(r.GetString(0));
                }
            }

            if (open.Count > 0)
            {
                throw new SqliteMigrationException("store_not_quiescent", "Put every application in maintenance first; not in maintenance: " + string.Join(", ", open));
            }

            using (var live = conn.CreateCommand())
            {
                live.CommandText = "SELECT (SELECT count(*) FROM runwick_worker_instances WHERE expires_at > @now), (SELECT count(*) FROM runwick_jobs WHERE lease_expires_at > @now)";
                live.P("now", now);
                using var r = live.ExecuteReader();
                r.Read();
                if (r.GetInt64(0) + r.GetInt64(1) > 0)
                {
                    throw new SqliteMigrationException("store_not_quiescent",
                        $"The store records {r.GetInt64(0).ToString(CultureInfo.InvariantCulture)} live worker presences and {r.GetInt64(1).ToString(CultureInfo.InvariantCulture)} live leases; stop those processes first.");
                }
            }

            using var oldId = conn.CreateCommand();
            oldId.CommandText = "SELECT store_id FROM runwick_store_metadata WHERE singleton_id = 1";
            var old = SqUuid.Read((byte[])oldId.ExecuteScalar()!);
            var fresh = Guid.NewGuid();
            using var update = conn.CreateCommand();
            update.CommandText = "UPDATE runwick_store_metadata SET store_id = @id, revision = revision + 1, updated_at = @now WHERE singleton_id = 1";
            update.P("id", fresh).P("now", now);
            update.ExecuteNonQuery();
            return new StoreIdentityChange(old, fresh);
        }), cancellationToken);
}

internal static class SqliteAdmin
{
    /// <summary>One transaction on a dedicated connection with a 5 s busy timeout; <paramref name="write"/> takes the write lock up front.</summary>
    internal static T InTransaction<T>(string connectionString, bool write, Func<SqliteConnection, long, T> body)
    {
        SqliteConnection conn;
        try
        {
            var cs = SqFile.Normalize(connectionString, SqliteOpenMode.ReadWrite);
            SqFile.CheckTopology(SqFile.FilePath(cs));
            conn = SqFile.Open(cs, 5_000);
        }
        catch (SqProfileException ex)
        {
            throw new SqliteMigrationException(ex.Code is "store_profile_mismatch" ? "store_unavailable" : ex.Code, ex.Message);
        }
        catch (SqliteException ex)
        {
            throw new SqliteMigrationException("store_unavailable", "Could not open the SQLite database.", ex);
        }

        using (conn)
        {
            try
            {
                SqFile.Exec(conn, write ? "BEGIN IMMEDIATE" : "BEGIN");
                if (SqFile.Scalar(conn, "SELECT schema_revision FROM runwick_store_metadata WHERE singleton_id = 1") is not { } revision || revision == "0")
                {
                    throw new SqliteMigrationException("store_not_initialized", "The store is not installed; run migrate first.");
                }

                var result = body(conn, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                SqFile.Exec(conn, "COMMIT");
                return result;
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5)
            {
                Rollback(conn);
                throw new SqliteMigrationException("migration_lock_timeout", "A writer held the database for more than 5 s; nothing changed.", ex);
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 1 && ex.Message.Contains("no such table", StringComparison.Ordinal))
            {
                Rollback(conn);
                throw new SqliteMigrationException("store_not_initialized", "The store is not installed; run migrate first.", ex);
            }
            catch
            {
                Rollback(conn);
                throw;
            }
        }
    }

    static void Rollback(SqliteConnection conn)
    {
        if (SqExecutor.InTransaction(conn))
        {
            SqFile.Exec(conn, "ROLLBACK");
        }
    }
}
