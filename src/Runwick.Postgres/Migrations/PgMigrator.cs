using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Npgsql;
using Runwick.Serialization;

namespace Runwick.Postgres.Migrations;

/// <summary>Outcome of one migration run (04 §8).</summary>
public sealed record PostgresMigrationResult(Guid StoreId, long SchemaRevision, IReadOnlyList<string> AppliedPhases);

/// <summary>A migration step failed; <see cref="Code"/> is the runner diagnostic (04 §8.1–8.3).</summary>
public sealed class PostgresMigrationException(string code, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public string Code { get; } = code;
}

/// <summary>The explicit deployment migrator for the PostgreSQL store. Workers never migrate on startup.</summary>
public static class RunwickPostgresMigrations
{
    /// <summary>Applies <c>0001_initial</c> through validate and activate. Re-running a fully applied migration is a no-op.</summary>
    public static Task<PostgresMigrationResult> MigrateAsync(string connectionString, CancellationToken cancellationToken = default) =>
        new PgMigrator(connectionString).RunAsync(cancellationToken);
}

internal sealed partial class PgMigrator(string connectionString, Func<string, NpgsqlConnection, Task>? afterPhase = null)
{
    internal const long AdvisoryLock = 5941776742533524273;
    const string MigrationId = "0001_initial";
    const string ManifestDomain = "runwick.manifest.v1";
    static readonly string RunnerBuild = "runwick/" + (typeof(PgMigrator).Assembly.GetName().Version?.ToString() ?? "0");

    internal static string InitialSql { get; } = LoadSql("0001_initial.sql");

    /// <summary>SHA-256 of the UTF-8 bytes after normalizing line endings to LF.</summary>
    internal static byte[] Checksum { get; } = SHA256.HashData(Encoding.UTF8.GetBytes(InitialSql.Replace("\r\n", "\n", StringComparison.Ordinal)));

    static string LoadSql(string name)
    {
        using var stream = typeof(PgMigrator).Assembly.GetManifestResourceStream("Runwick.Postgres.Sql." + name)
            ?? throw new InvalidOperationException($"Embedded migration {name} is missing.");
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    public async Task<PostgresMigrationResult> RunAsync(CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false,
            Enlist = false,
            ApplicationName = "runwick:migrate",
            IncludeErrorDetail = false,
            MaxAutoPrepare = 0,
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

            await PreflightServerAsync(conn, ct).ConfigureAwait(false);
            await AcquireLockAsync(conn, ct).ConfigureAwait(false);
            try
            {
                return await RunPhasesAsync(conn, ct).ConfigureAwait(false);
            }
            finally
            {
                if (conn.State == System.Data.ConnectionState.Open)
                {
                    using var unlock = new NpgsqlCommand($"SELECT pg_advisory_unlock({AdvisoryLock})", conn);
                    await unlock.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
    }

    static async Task PreflightServerAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("""
            SELECT current_setting('server_version_num')::int, pg_encoding_to_char(d.encoding), current_setting('standard_conforming_strings')
            FROM pg_database AS d WHERE d.datname = current_database()
            """, conn);
        var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            await reader.ReadAsync(ct).ConfigureAwait(false);
            if (reader.GetInt32(0) < 160000)
            {
                throw new PostgresMigrationException("server_version_unsupported", "PostgreSQL 16 or later is required.");
            }

            if (reader.GetString(1) != "UTF8")
            {
                throw new PostgresMigrationException("encoding_unsupported", "The database encoding must be UTF8.");
            }

            if (reader.GetString(2) != "on")
            {
                throw new PostgresMigrationException("standard_conforming_strings_off", "standard_conforming_strings must be on.");
            }
        }
    }

    static async Task AcquireLockAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        var delay = TimeSpan.FromMilliseconds(250);
        while (true)
        {
            using var cmd = new NpgsqlCommand($"SELECT pg_try_advisory_lock({AdvisoryLock})", conn);
            if ((bool)(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!)
            {
                return;
            }

            if (DateTime.UtcNow + delay > deadline)
            {
                throw new PostgresMigrationException("migration_lock_unavailable", "Another migration runner holds the migration lock.");
            }

            await Task.Delay(delay, ct).ConfigureAwait(false);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(2).Ticks));
        }
    }

    async Task<PostgresMigrationResult> RunPhasesAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        var applied = new List<string>();
        var ledger = await ReadLedgerAsync(conn, ct).ConfigureAwait(false);
        foreach (var (phase, checksum) in ledger)
        {
            if (!checksum.AsSpan().SequenceEqual(Checksum))
            {
                throw new PostgresMigrationException("migration_checksum_mismatch",
                    $"Applied migration {MigrationId} phase {phase} has a different checksum than this binary.");
            }
        }

        if (!ledger.ContainsKey("expand"))
        {
            await PhaseAsync(conn, "expand", async tx =>
            {
                using var ddl = new NpgsqlCommand(InitialSql, conn, tx);
                await ddl.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                await LedgerAsync(conn, tx, "preflight", ct).ConfigureAwait(false);
                await LedgerAsync(conn, tx, "expand", ct).ConfigureAwait(false);
            }, ct).ConfigureAwait(false);
            applied.Add("expand");
            await AfterPhaseAsync("expand", conn).ConfigureAwait(false);
        }

        if (!ledger.ContainsKey("validate"))
        {
            await PhaseAsync(conn, "validate", async tx =>
            {
                using (var lockRow = new NpgsqlCommand("SELECT revision FROM runwick.store_metadata WHERE singleton_id = 1 FOR UPDATE", conn, tx))
                {
                    await lockRow.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                await ValidateCatalogAsync(conn, tx, ct).ConfigureAwait(false);
                var (manifest, digest) = Manifest();
                using (var update = new NpgsqlCommand("""
                    UPDATE runwick.store_metadata
                    SET schema_revision = 1, migration_mode = 'normal', compatibility_manifest = @manifest,
                        compatibility_manifest_version = 1, manifest_digest = @digest, revision = revision + 1,
                        updated_at = runwick.ms_floor(clock_timestamp())
                    WHERE singleton_id = 1
                    """, conn, tx))
                {
                    update.Parameters.AddWithValue("manifest", NpgsqlTypes.NpgsqlDbType.Jsonb, manifest.ToJsonString());
                    update.Parameters.AddWithValue("digest", NpgsqlTypes.NpgsqlDbType.Bytea, digest);
                    await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                await LedgerAsync(conn, tx, "validate", ct).ConfigureAwait(false);
            }, ct).ConfigureAwait(false);
            applied.Add("validate");
            await AfterPhaseAsync("validate", conn).ConfigureAwait(false);
        }

        if (!ledger.ContainsKey("activate"))
        {
            await PhaseAsync(conn, "activate", async tx =>
            {
                using (var check = new NpgsqlCommand("SELECT schema_revision, migration_mode FROM runwick.store_metadata WHERE singleton_id = 1", conn, tx))
                {
                    var reader = await check.ExecuteReaderAsync(ct).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        if (!await reader.ReadAsync(ct).ConfigureAwait(false) || reader.GetInt64(0) != 1 || reader.GetString(1) != "normal")
                        {
                            throw new PostgresMigrationException("schema_validation_failed", "Activate found the store not validated.");
                        }
                    }
                }

                await LedgerAsync(conn, tx, "activate", ct).ConfigureAwait(false);
            }, ct).ConfigureAwait(false);
            applied.Add("activate");
            await AfterPhaseAsync("activate", conn).ConfigureAwait(false);
        }

        using var meta = new NpgsqlCommand("SELECT store_id, schema_revision FROM runwick.store_metadata WHERE singleton_id = 1", conn);
        var r = await meta.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (r.ConfigureAwait(false))
        {
            await r.ReadAsync(ct).ConfigureAwait(false);
            return new PostgresMigrationResult(r.GetGuid(0), r.GetInt64(1), applied);
        }
    }

    async Task AfterPhaseAsync(string phase, NpgsqlConnection conn)
    {
        if (afterPhase is not null)
        {
            await afterPhase(phase, conn).ConfigureAwait(false);
        }
    }

    static async Task<Dictionary<string, byte[]>> ReadLedgerAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        using var exists = new NpgsqlCommand("""
            SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'runwick'),
                   to_regclass('runwick.schema_migrations') IS NOT NULL
            """, conn);
        bool schema, table;
        var e = await exists.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (e.ConfigureAwait(false))
        {
            await e.ReadAsync(ct).ConfigureAwait(false);
            (schema, table) = (e.GetBoolean(0), e.GetBoolean(1));
        }

        if (!schema)
        {
            return [];
        }

        if (!table)
        {
            throw new PostgresMigrationException("schema_not_empty", "Schema runwick exists but has no migration ledger.");
        }

        using var cmd = new NpgsqlCommand(
            "SELECT phase, checksum FROM runwick.schema_migrations WHERE migration_id = @id AND state = 'applied'", conn);
        cmd.Parameters.AddWithValue("id", MigrationId);
        var ledger = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                ledger[reader.GetString(0)] = (byte[])reader[1];
            }
        }

        return ledger;
    }

    static async Task PhaseAsync(NpgsqlConnection conn, string phase, Func<NpgsqlTransaction, Task> body, CancellationToken ct)
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

                await body(tx).ConfigureAwait(false);
                await tx.CommitAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is PostgresException or PostgresMigrationException)
            {
                await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                var code = ex is PostgresMigrationException m ? m.Code : ((PostgresException)ex).SqlState;
                if (phase != "expand")
                {
                    await RecordFailureAsync(conn, phase, code).ConfigureAwait(false);
                }

                throw ex as PostgresMigrationException
                    ?? new PostgresMigrationException(code == "55P03" ? "migration_lock_timeout" : "migration_phase_failed",
                        $"Migration {MigrationId} phase {phase} failed ({code}).", ex);
            }
        }
    }

    static async Task LedgerAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string phase, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("""
            INSERT INTO runwick.schema_migrations (
                migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at)
            VALUES (@id, @phase, @checksum, 1, 'applied', @build, clock_timestamp(), clock_timestamp())
            ON CONFLICT (migration_id, phase) DO UPDATE
            SET state = 'applied', checksum = EXCLUDED.checksum, completed_at = EXCLUDED.completed_at, diagnostic_code = NULL
            """, conn, tx);
        cmd.Parameters.AddWithValue("id", MigrationId);
        cmd.Parameters.AddWithValue("phase", phase);
        cmd.Parameters.AddWithValue("checksum", Checksum);
        cmd.Parameters.AddWithValue("build", RunnerBuild);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    static async Task RecordFailureAsync(NpgsqlConnection conn, string phase, string code)
    {
        try
        {
            using var cmd = new NpgsqlCommand("""
                INSERT INTO runwick.schema_migrations (
                    migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at, diagnostic_code)
                VALUES (@id, @phase, @checksum, 1, 'failed', @build, clock_timestamp(), clock_timestamp(), @code)
                ON CONFLICT (migration_id, phase) DO UPDATE
                SET state = 'failed', completed_at = EXCLUDED.completed_at, diagnostic_code = EXCLUDED.diagnostic_code
                """, conn);
            cmd.Parameters.AddWithValue("id", MigrationId);
            cmd.Parameters.AddWithValue("phase", phase);
            cmd.Parameters.AddWithValue("checksum", Checksum);
            cmd.Parameters.AddWithValue("build", RunnerBuild);
            cmd.Parameters.AddWithValue("code", code[..Math.Min(code.Length, 64)]);
            await cmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (PostgresException)
        {
            // Best effort: the failure is still reported to the caller.
        }
    }

    // ------------------------------------------------------------------ validation (04 §8.3)

    [GeneratedRegex(@"CREATE TABLE runwick\.([a-z_]+)")]
    private static partial Regex TablePattern();

    [GeneratedRegex(@"CREATE (?:UNIQUE )?INDEX ([a-z_]+)")]
    private static partial Regex IndexPattern();

    [GeneratedRegex(@"CONSTRAINT ([a-z_]+) (?:PRIMARY KEY|UNIQUE|FOREIGN KEY|CHECK|EXCLUDE)")]
    private static partial Regex ConstraintPattern();

    internal static IReadOnlySet<string> ExpectedRelations { get; } =
        TablePattern().Matches(InitialSql).Select(m => m.Groups[1].Value)
            .Concat(IndexPattern().Matches(InitialSql).Select(m => m.Groups[1].Value)).ToHashSet(StringComparer.Ordinal);

    internal static IReadOnlySet<string> ExpectedConstraints { get; } =
        ConstraintPattern().Matches(InitialSql).Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);

    static readonly string[] DeferredForeignKeys = ["jobs_current_generation_fk", "jobs_current_attempt_fk"];
    static readonly string[] ReceiptTriggers = ["storage_commands_ownership_decided", "storage_commands_durable_decided"];

    static async Task ValidateCatalogAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        var problems = new List<string>();
        var relations = await ListAsync(conn, tx, """
            SELECT c.relname FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'runwick' AND c.relkind IN ('r', 'p', 'i', 'I')
            """, ct).ConfigureAwait(false);
        problems.AddRange(ExpectedRelations.Where(x => !relations.Contains(x)).Select(x => "missing relation " + x));

        var invalid = await ListAsync(conn, tx, """
            SELECT c.relname FROM pg_index AS i
            JOIN pg_class AS c ON c.oid = i.indexrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'runwick' AND (NOT i.indisvalid OR NOT i.indisready)
            """, ct).ConfigureAwait(false);
        problems.AddRange(invalid.Select(x => "invalid index " + x));

        var constraints = await ListAsync(conn, tx, """
            SELECT conname || ':' || convalidated || ':' || condeferrable || ':' || condeferred
            FROM pg_constraint AS k JOIN pg_namespace AS n ON n.oid = k.connamespace WHERE n.nspname = 'runwick'
            """, ct).ConfigureAwait(false);
        var byName = constraints.Select(c => c.Split(':')).GroupBy(p => p[0]).ToDictionary(g => g.Key, g => g.First());
        problems.AddRange(ExpectedConstraints.Where(x => !byName.ContainsKey(x)).Select(x => "missing constraint " + x));
        problems.AddRange(byName.Values.Where(p => p[1] != "true").Select(p => "unvalidated constraint " + p[0]));
        problems.AddRange(DeferredForeignKeys.Where(x => !byName.TryGetValue(x, out var p) || p[2] != "true" || p[3] != "true")
            .Select(x => "constraint not deferred " + x));

        var triggers = await ListAsync(conn, tx, """
            SELECT tgname FROM pg_trigger
            WHERE tgname IN ('storage_commands_ownership_decided', 'storage_commands_durable_decided')
              AND tgenabled = 'O' AND tgdeferrable AND tginitdeferred
            """, ct).ConfigureAwait(false);
        problems.AddRange(ReceiptTriggers.Where(x => !triggers.Contains(x)).Select(x => "missing trigger " + x));

        if (problems.Count > 0)
        {
            throw new PostgresMigrationException("schema_validation_failed", "Schema validation failed: " + string.Join("; ", problems));
        }
    }

    static async Task<HashSet<string>> ListAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string sql, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(sql, conn, tx);
        var set = new HashSet<string>(StringComparer.Ordinal);
        var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                set.Add(reader.GetString(0));
            }
        }

        return set;
    }

    // ------------------------------------------------------------------ manifest (04 §8.5)

    internal static readonly string[] ManifestOperations =
    [
        "Describe", "RegisterApplication", "RegisterQueue", "RegisterScope", "Dispatch", "DispatchMany", "ResolveCommand",
        "ReadinessProbe", "PublishPresence", "ListExpiredLeases", "Reserve", "Begin", "Renew", "Finish", "Recover",
        "ReleaseReservation", "SetQueueState",
    ];

    internal static (JsonObject Json, byte[] Digest) Manifest()
    {
        var operations = new JsonObject();
        var canonicalOps = CanonicalValue.Object();
        foreach (var op in ManifestOperations)
        {
            operations[op] = new JsonArray(1);
            canonicalOps.Add(op, CanonicalValue.Array(CanonicalValue.Int(1)));
        }

        var json = new JsonObject
        {
            ["manifestVersion"] = 1,
            ["schemaRevision"] = 1,
            ["protocolEpochs"] = new JsonArray(1),
            ["receiptCodecs"] = new JsonArray(1),
            ["commandFingerprintVersions"] = new JsonArray(1),
            ["windowFingerprintVersions"] = new JsonArray(1),
            ["retryEvaluators"] = new JsonArray("exponential-v1"),
            ["operations"] = operations,
            ["expandCompatibleBinaries"] = new JsonArray(),
        };
        var one = CanonicalValue.Array(CanonicalValue.Int(1));
        var canonical = CanonicalValue.Object()
            .Add("manifestVersion", 1)
            .Add("schemaRevision", 1)
            .Add("protocolEpochs", one)
            .Add("receiptCodecs", one)
            .Add("commandFingerprintVersions", one)
            .Add("windowFingerprintVersions", one)
            .Add("retryEvaluators", CanonicalValue.Array(CanonicalValue.Str("exponential-v1")))
            .Add("operations", canonicalOps.Build())
            .Add("expandCompatibleBinaries", CanonicalValue.Array())
            .Build();
        return (json, CanonicalEncoder.Digest(ManifestDomain, canonical));
    }
}
