using Npgsql;

namespace Runwick.Postgres.Migrations;

/// <summary>Operator admission, status and identity commands of the PostgreSQL store (23). Run as the migration role, like the migrator.</summary>
public static partial class RunwickPostgresMigrations
{
    /// <summary>
    /// Sets one application's (or, with <paramref name="application"/> null, every registered application's) admission mode with the
    /// 04 §7.15 statements: waits for in-flight transactions at the gate, at most 5 s, then fails with <c>migration_lock_timeout</c>.
    /// </summary>
    public static async Task<IReadOnlyList<AdmissionChange>> SetAdmissionModeAsync(
        string connectionString, string? application, string mode, string actor, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        return await PgAdmin.InTransactionAsync(connectionString, "admission", async (conn, tx) =>
        {
            await PgAdmin.LockStoreAsync(conn, tx, cancellationToken).ConfigureAwait(false);
            var rows = new List<(string App, long Revision, string Mode)>();
            using (var select = new NpgsqlCommand("""
                SELECT application, revision, admission_mode FROM runwick.application_controls
                WHERE @app::text IS NULL OR application = @app ORDER BY application FOR UPDATE
                """, conn, tx))
            {
                select.Parameters.AddWithValue("app", (object?)application ?? DBNull.Value);
                var r = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (r.ConfigureAwait(false))
                {
                    while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        rows.Add((r.GetString(0), r.GetInt64(1), r.GetString(2)));
                    }
                }
            }

            if (application is not null && rows.Count == 0)
            {
                throw new PostgresMigrationException("application_not_registered", $"Application {application} is not registered.");
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
                    throw new PostgresMigrationException(code, code == "drain_first"
                        ? $"Application {app} is open: drain it and let work settle before maintenance."
                        : "The mode must be open, drain or maintenance.");
                }

                using var update = new NpgsqlCommand("""
                    UPDATE runwick.application_controls
                    SET admission_mode = @mode, revision = revision + 1, updated_at = runwick.ms_floor(clock_timestamp()), actor_subject = @actor
                    WHERE application = @app
                    RETURNING revision
                    """, conn, tx);
                update.Parameters.AddWithValue("mode", mode);
                update.Parameters.AddWithValue("actor", actor);
                update.Parameters.AddWithValue("app", app);
                changes.Add(new AdmissionChange(app, previous, mode, (long)(await update.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!, true));
            }

            return changes;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Raises one capability (<c>scheduler</c> or <c>rate_admission</c>) for one application, or every registered application when
    /// <paramref name="application"/> is null (10 §6.1, 23 §3.6). Never lowers one; a repeat reports <c>Changed = false</c>. Unlike the
    /// store operation this writes no audit row or event, like <see cref="SetAdmissionModeAsync"/>; it records <paramref name="actor"/>.
    /// </summary>
    public static async Task<IReadOnlyList<CapabilityChange>> ActivateCapabilityAsync(
        string connectionString, string? application, string capability, string actor, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        if (StoreAdminRules.CheckCapability(capability) is { } bad)
        {
            throw new PostgresMigrationException(bad, "The capability must be scheduler or rate_admission.");
        }

        var version = StoreAdminRules.CapabilityVersion(capability);
        return await PgAdmin.InTransactionAsync(connectionString, "capability", async (conn, tx) =>
        {
            await PgAdmin.LockStoreAsync(conn, tx, cancellationToken).ConfigureAwait(false);
            var rows = new List<(string App, long Revision, string Required)>();
            using (var select = new NpgsqlCommand("""
                SELECT application, revision, required_capabilities::text FROM runwick.application_controls
                WHERE @app::text IS NULL OR application = @app ORDER BY application FOR UPDATE
                """, conn, tx))
            {
                select.Parameters.AddWithValue("app", (object?)application ?? DBNull.Value);
                var r = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (r.ConfigureAwait(false))
                {
                    while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        rows.Add((r.GetString(0), r.GetInt64(1), r.GetString(2)));
                    }
                }
            }

            if (application is not null && rows.Count == 0)
            {
                throw new PostgresMigrationException("application_not_registered", $"Application {application} is not registered.");
            }

            var changes = new List<CapabilityChange>();
            foreach (var (app, revision, required) in rows)
            {
                if (StoreAdminRules.Raise(required, capability, version) is not { } raised)
                {
                    changes.Add(new CapabilityChange(app, capability, version, revision, false));
                    continue;
                }

                using var update = new NpgsqlCommand("""
                    UPDATE runwick.application_controls
                    SET required_capabilities = @caps::jsonb, revision = revision + 1, updated_at = runwick.ms_floor(clock_timestamp()), actor_subject = @actor
                    WHERE application = @app
                    RETURNING revision
                    """, conn, tx);
                update.Parameters.AddWithValue("caps", raised);
                update.Parameters.AddWithValue("actor", actor);
                update.Parameters.AddWithValue("app", app);
                changes.Add(new CapabilityChange(app, capability, version, (long)(await update.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!, true));
            }

            return changes;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads the store's revision, mode, application gates and the lease and presence rows an operator checks before acting (23 §6).</summary>
    public static async Task<StoreAdminStatus> GetStatusAsync(string connectionString, CancellationToken cancellationToken = default) =>
        await PgAdmin.InTransactionAsync(connectionString, "status", async (conn, tx) =>
        {
            Guid id;
            long revision;
            string mode;
            using (var meta = new NpgsqlCommand("SELECT store_id, schema_revision, migration_mode FROM runwick.store_metadata WHERE singleton_id = 1", conn, tx))
            {
                var r = await meta.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (r.ConfigureAwait(false))
                {
                    await r.ReadAsync(cancellationToken).ConfigureAwait(false);
                    (id, revision, mode) = (r.GetGuid(0), r.GetInt64(1), r.GetString(2));
                }
            }

            var apps = new List<ApplicationAdmissionState>();
            using (var list = new NpgsqlCommand("SELECT application, admission_mode, revision, required_capabilities::text FROM runwick.application_controls ORDER BY application", conn, tx))
            {
                var r = await list.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (r.ConfigureAwait(false))
                {
                    while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        apps.Add(new ApplicationAdmissionState(r.GetString(0), r.GetString(1), r.GetInt64(2), r.GetString(3)));
                    }
                }
            }

            using var counts = new NpgsqlCommand("""
                SELECT (SELECT count(*) FROM runwick.jobs WHERE lease_expires_at IS NOT NULL),
                       (SELECT count(*) FROM runwick.worker_instances),
                       (SELECT count(*) FROM runwick.worker_instances WHERE expires_at <= clock_timestamp())
                """, conn, tx);
            var c = await counts.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (c.ConfigureAwait(false))
            {
                await c.ReadAsync(cancellationToken).ConfigureAwait(false);
                return new StoreAdminStatus(id, revision, PgMigrator.CurrentRevision, mode, apps, (int)c.GetInt64(0), (int)c.GetInt64(1), (int)c.GetInt64(2),
                    StoreAdminRules.QuiescenceNotice);
            }
        }, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Gives a copied or restored database a new logical store identity (23 §7). Refused unless every application is in
    /// <c>maintenance</c> and no unexpired lease or worker presence is recorded. Commands and prepared envelopes bound to the
    /// old identity then fail with <c>store_identity_mismatch</c>.
    /// </summary>
    public static async Task<StoreIdentityChange> ReassignStoreIdAsync(string connectionString, CancellationToken cancellationToken = default) =>
        await PgAdmin.InTransactionAsync(connectionString, "reidentify", async (conn, tx) =>
        {
            Guid old;
            using (var meta = new NpgsqlCommand("SELECT store_id FROM runwick.store_metadata WHERE singleton_id = 1 FOR UPDATE", conn, tx))
            {
                old = (Guid)(await meta.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
            }

            var open = new List<string>();
            using (var apps = new NpgsqlCommand("SELECT application FROM runwick.application_controls WHERE admission_mode <> 'maintenance' ORDER BY application FOR UPDATE", conn, tx))
            {
                var r = await apps.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (r.ConfigureAwait(false))
                {
                    while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        open.Add(r.GetString(0));
                    }
                }
            }

            if (open.Count > 0)
            {
                throw new PostgresMigrationException("store_not_quiescent", "Put every application in maintenance first; not in maintenance: " + string.Join(", ", open));
            }

            using (var live = new NpgsqlCommand("""
                SELECT (SELECT count(*) FROM runwick.worker_instances WHERE expires_at > clock_timestamp()),
                       (SELECT count(*) FROM runwick.jobs WHERE lease_expires_at > clock_timestamp())
                """, conn, tx))
            {
                var r = await live.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (r.ConfigureAwait(false))
                {
                    await r.ReadAsync(cancellationToken).ConfigureAwait(false);
                    if (r.GetInt64(0) + r.GetInt64(1) > 0)
                    {
                        throw new PostgresMigrationException("store_not_quiescent",
                            $"The store records {r.GetInt64(0)} live worker presences and {r.GetInt64(1)} live leases; stop those processes first.");
                    }
                }
            }

            var fresh = Guid.NewGuid();
            using var update = new NpgsqlCommand("""
                UPDATE runwick.store_metadata SET store_id = @id, revision = revision + 1, updated_at = runwick.ms_floor(clock_timestamp())
                WHERE singleton_id = 1
                """, conn, tx);
            update.Parameters.AddWithValue("id", fresh);
            await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return new StoreIdentityChange(old, fresh);
        }, cancellationToken, serialize: true).ConfigureAwait(false);
}

internal static class PgAdmin
{
    /// <summary>One transaction on a dedicated unpooled migration-role connection, with the migrator's timeouts (04 §8.1).</summary>
    internal static async Task<T> InTransactionAsync<T>(
        string connectionString, string purpose, Func<NpgsqlConnection, NpgsqlTransaction, Task<T>> body, CancellationToken ct, bool serialize = false)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false, Enlist = false, ApplicationName = "runwick:" + purpose, IncludeErrorDetail = false, MaxAutoPrepare = 0,
        };
        var conn = new NpgsqlConnection(builder.ConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            try
            {
                await conn.OpenAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is NpgsqlException or IOException or TimeoutException)
            {
                throw new PostgresMigrationException("store_unavailable", "Could not connect to PostgreSQL.", ex);
            }

            if (serialize)
            {
                await Migrations.PgMigrator.AcquireLockAsync(conn, ct).ConfigureAwait(false);
            }

            try
            {
                var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);
                await using (tx.ConfigureAwait(false))
                {
                    try
                    {
                        using (var set = new NpgsqlCommand("SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '60s'", conn, tx))
                        {
                            await set.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        }

                        var result = await body(conn, tx).ConfigureAwait(false);
                        await tx.CommitAsync(ct).ConfigureAwait(false);
                        return result;
                    }
                    catch (PostgresException ex)
                    {
                        await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                        throw new PostgresMigrationException(
                            ex.SqlState switch { "55P03" => "migration_lock_timeout", "42P01" or "3F000" => "store_not_initialized", _ => "migration_phase_failed" },
                            ex.SqlState == "55P03" ? "A transaction holding the store or application gate did not finish in 5 s; nothing changed."
                                : $"The {purpose} command failed ({ex.SqlState}).", ex);
                    }
                    catch (PostgresMigrationException)
                    {
                        await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                        throw;
                    }
                }
            }
            finally
            {
                if (serialize && conn.State == System.Data.ConnectionState.Open)
                {
                    using var unlock = new NpgsqlCommand($"SELECT pg_advisory_unlock({Migrations.PgMigrator.AdvisoryLock})", conn);
                    await unlock.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>04 §7.15 step 1: the store gate row, shared (the application change then waits for every in-flight holder).</summary>
    internal static async Task LockStoreAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("SELECT schema_revision FROM runwick.store_metadata WHERE singleton_id = 1 FOR SHARE", conn, tx);
        if (await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) is not long revision || revision < 1)
        {
            throw new PostgresMigrationException("store_not_initialized", "The store is not installed; run migrate first.");
        }
    }
}
