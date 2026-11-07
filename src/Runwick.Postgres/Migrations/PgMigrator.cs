using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Npgsql;
using Runwick.Protocol;
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
    /// <summary>Applies every migration this binary knows, in order, each through expand, validate and activate. Re-running is a no-op.</summary>
    public static Task<PostgresMigrationResult> MigrateAsync(string connectionString, CancellationToken cancellationToken = default) =>
        new PgMigrator(connectionString).RunAsync(cancellationToken);
}

internal sealed partial class PgMigrator(string connectionString, Func<string, NpgsqlConnection, Task>? afterPhase = null)
{
    internal const long AdvisoryLock = 5941776742533524273;
    const string ManifestDomain = "runwick.manifest.v1";
    static readonly string RunnerBuild = "runwick/" + (typeof(PgMigrator).Assembly.GetName().Version?.ToString() ?? "0");

    /// <summary>One numbered migration: its SQL, checksum and the schema revision its validate phase installs (04 §8.4).</summary>
    internal sealed record Migration(string Id, string Sql, long TargetRevision)
    {
        /// <summary>SHA-256 of the UTF-8 bytes after normalizing line endings to LF.</summary>
        public byte[] Checksum { get; } = SHA256.HashData(Encoding.UTF8.GetBytes(Sql.Replace("\r\n", "\n", StringComparison.Ordinal)));

        /// <summary>
        /// The fault-hooks variant's checksum (04 §8.1, PF-05): the LF-normalized bytes followed by 0x00 and
        /// <c>rw:fault-hooks</c>. Equal to <see cref="Checksum"/> for a migration without hook lines.
        /// </summary>
        public byte[] HooksChecksum { get; } = HasHooks(Sql)
            ? SHA256.HashData([.. Encoding.UTF8.GetBytes(Sql.Replace("\r\n", "\n", StringComparison.Ordinal)), 0, .. "rw:fault-hooks"u8])
            : SHA256.HashData(Encoding.UTF8.GetBytes(Sql.Replace("\r\n", "\n", StringComparison.Ordinal)));

        public byte[] ChecksumFor(bool hooks) => hooks ? HooksChecksum : Checksum;

        /// <summary>The SQL a variant executes: the production variant strips every hook line and hook block.</summary>
        public string SqlFor(bool hooks) => hooks ? Sql : StripHooks(Sql);
    }

    const string HookLine = "-- rw:fault";

    static bool HasHooks(string sql) => sql.Contains(HookLine, StringComparison.Ordinal);

    /// <summary>Removes every line ending in <c>-- rw:fault</c> and every block from <c>-- rw:fault-begin</c> to <c>-- rw:fault-end</c>.</summary>
    internal static string StripHooks(string sql)
    {
        var kept = new StringBuilder(sql.Length);
        var inBlock = false;
        foreach (var raw in sql.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'))
        {
            var line = raw.TrimEnd();
            if (line.EndsWith(HookLine + "-begin", StringComparison.Ordinal))
            {
                inBlock = true;
                continue;
            }

            if (line.EndsWith(HookLine + "-end", StringComparison.Ordinal))
            {
                inBlock = false;
                continue;
            }

            if (inBlock || line.EndsWith(HookLine, StringComparison.Ordinal))
            {
                continue;
            }

            kept.Append(raw).Append('\n');
        }

        return kept.ToString(0, Math.Max(0, kept.Length - 1));
    }

    /// <summary>
    /// Set only by the test harness (<c>InternalsVisibleTo</c>): migrate the fault-hooks variant (04 §8.1, PF-05). The
    /// public <see cref="RunwickPostgresMigrations.MigrateAsync"/> never sets it.
    /// </summary>
    internal bool FaultHooks { get; init; }

    [GeneratedRegex(@"CREATE FUNCTION runwick\.([a-z0-9_]+)\(.*?AS \$fn\$(.*?)\$fn\$|^DROP FUNCTION runwick\.([a-z0-9_]+)\(", RegexOptions.Singleline | RegexOptions.Multiline)]
    private static partial Regex FunctionPattern();

    /// <summary>
    /// The server functions the migrations create and the MD5 of each body as the variant executes it (<c>prosrc</c>),
    /// compared with <c>pg_proc</c> at validate and at Describe (04 §8.3, PF-05).
    /// </summary>
    internal static IReadOnlyDictionary<string, string> ExpectedFunctions(IEnumerable<Migration> migrations, bool hooks)
    {
        var map = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var m in migrations)
        {
            foreach (Match f in FunctionPattern().Matches(m.SqlFor(hooks)))
            {
                // A later migration drops a superseded function (0010: reserve_v1, begin_v1): it is no longer expected.
                if (f.Groups[3].Success)
                {
                    map.Remove(f.Groups[3].Value);
                    continue;
                }

#pragma warning disable CA5351 // Matches PostgreSQL md5(prosrc) for drift detection; not a security use.
                map[f.Groups[1].Value] = Convert.ToHexStringLower(MD5.HashData(Encoding.UTF8.GetBytes(f.Groups[2].Value)));
#pragma warning restore CA5351
            }
        }

        return map;
    }

    /// <summary>The functions of every migration this binary knows, for a store of the given variant.</summary>
    internal static IReadOnlyDictionary<string, string> CurrentFunctions(bool hooks) => hooks ? HooksFunctions.Value : ProductionFunctions.Value;

    static readonly Lazy<IReadOnlyDictionary<string, string>> ProductionFunctions = new(() => ExpectedFunctions(Migrations!, false));
    static readonly Lazy<IReadOnlyDictionary<string, string>> HooksFunctions = new(() => ExpectedFunctions(Migrations!, true));

    /// <summary>
    /// Lists the differences between the store's runwick functions and <paramref name="expected"/>: a missing or changed
    /// body (an operator's CREATE OR REPLACE) or a function no migration of this variant creates (rw_fault in production).
    /// </summary>
    internal static List<string> FunctionProblems(IReadOnlyDictionary<string, string> expected, IReadOnlyDictionary<string, string> actual, IEnumerable<string> allExpected)
    {
        var problems = new List<string>();
        foreach (var (name, digest) in expected)
        {
            if (!actual.TryGetValue(name, out var got))
            {
                problems.Add("missing function " + name);
            }
            else if (got != digest)
            {
                problems.Add("changed function " + name);
            }
        }

        var known = allExpected.ToHashSet(StringComparer.Ordinal);
        problems.AddRange(actual.Keys.Where(n => !expected.ContainsKey(n) && known.Contains(n)).Select(n => "unexpected function " + n));
        return problems;
    }

    /// <summary>Every function name either variant creates, including ones a later migration drops (still unexpected if present).</summary>
    internal static IEnumerable<string> AllFunctionNames => AllCreated.Value;

    static readonly Lazy<IReadOnlyList<string>> AllCreated = new(() =>
        [.. Migrations!.SelectMany(m => FunctionPattern().Matches(m.Sql)).Where(f => f.Groups[1].Success).Select(f => f.Groups[1].Value).Distinct()]);

    internal const string FunctionDigestSql = """
        SELECT p.proname || ':' || md5(p.prosrc)
        FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = 'runwick'
        """;

    /// <summary>All migrations in apply order; numbers are assigned in merge order (08 Changes, plan "Migration numbering").</summary>
    internal static IReadOnlyList<Migration> Migrations { get; } =
    [
        new("0001_initial", LoadSql("0001_initial.sql"), 1),
        new("0002_controls", LoadSql("0002_controls.sql"), 2),
        new("0003_outbox", LoadSql("0003_outbox.sql"), 3),
        new("0004_rates", LoadSql("0004_rates.sql"), 4),
        new("0005_readiness", LoadSql("0005_readiness.sql"), 5),
        new("0006_scheduler", LoadSql("0006_scheduler.sql"), 6),
        new("0007_scaling", LoadSql("0007_scaling.sql"), 7),
        new("0008_renew_many", LoadSql("0008_renew_many.sql"), 8),
        new("0009_functions", LoadSql("0009_functions.sql"), 9),
        new("0010_reserve_and_begin", LoadSql("0010_reserve_and_begin.sql"), 10),
        new("0011_finish_many", LoadSql("0011_finish_many.sql"), 11),
        new("0012_set_claim", LoadSql("0012_set_claim.sql"), 12),
    ];

    /// <summary>The schema revision this binary writes to (03 §10.1): the last migration's target.</summary>
    internal static long CurrentRevision => Migrations[^1].TargetRevision;

    internal static string InitialSql => Migrations[0].Sql;

    /// <summary>Checksum of <c>0001_initial</c>.</summary>
    internal static byte[] Checksum => Migrations[0].Checksum;

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
                // The schema may have changed under statements this process's stores prepared: drop their idle
                // connections so no prepared statement outlives a migration here (04 §9.2, PF-03). Other processes
                // meet the change as a 0A000 transient retry.
                Store.PgDataSources.ClearAll(connectionString);
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
        foreach (var ((id, phase), checksum) in ledger)
        {
            var known = Migrations.FirstOrDefault(m => m.Id == id)
                ?? throw new PostgresMigrationException("migration_unknown", $"Applied migration {id} is not known to this binary (newer store).");
            if (!checksum.AsSpan().SequenceEqual(known.ChecksumFor(FaultHooks)))
            {
                throw new PostgresMigrationException("migration_checksum_mismatch",
                    $"Applied migration {id} phase {phase} has a different checksum than this binary.");
            }
        }

        for (var index = 0; index < Migrations.Count; index++)
        {
            var migration = Migrations[index];
            var cumulative = Migrations.Take(index + 1).ToList();
            bool Done(string phase) => ledger.ContainsKey((migration.Id, phase));

            if (!Done("expand"))
            {
                await PhaseAsync(conn, migration, "expand", async tx =>
                {
                    using var ddl = new NpgsqlCommand(migration.SqlFor(FaultHooks), conn, tx);
                    await ddl.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    await LedgerAsync(conn, tx, migration, "preflight", ct).ConfigureAwait(false);
                    await LedgerAsync(conn, tx, migration, "expand", ct).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);
                applied.Add(migration.Id + ":expand");
                await AfterPhaseAsync(migration.Id + ":expand", conn).ConfigureAwait(false);
            }

            if (!Done("validate"))
            {
                await PhaseAsync(conn, migration, "validate", async tx =>
                {
                    using (var lockRow = new NpgsqlCommand("SELECT revision FROM runwick.store_metadata WHERE singleton_id = 1 FOR UPDATE", conn, tx))
                    {
                        await lockRow.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }

                    await ValidateCatalogAsync(conn, tx, cumulative, FaultHooks, ct).ConfigureAwait(false);
                    var (manifest, digest) = Manifest(migration.TargetRevision);
                    using (var update = new NpgsqlCommand("""
                        UPDATE runwick.store_metadata
                        SET schema_revision = @revision, migration_mode = 'normal', compatibility_manifest = @manifest,
                            compatibility_manifest_version = 1, manifest_digest = @digest, revision = revision + 1,
                            updated_at = runwick.ms_floor(clock_timestamp())
                        WHERE singleton_id = 1
                        """, conn, tx))
                    {
                        update.Parameters.AddWithValue("revision", migration.TargetRevision);
                        update.Parameters.AddWithValue("manifest", NpgsqlTypes.NpgsqlDbType.Jsonb, manifest.ToJsonString());
                        update.Parameters.AddWithValue("digest", NpgsqlTypes.NpgsqlDbType.Bytea, digest);
                        await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }

                    await LedgerAsync(conn, tx, migration, "validate", ct).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);
                applied.Add(migration.Id + ":validate");
                await AfterPhaseAsync(migration.Id + ":validate", conn).ConfigureAwait(false);
            }

            if (!Done("activate"))
            {
                await PhaseAsync(conn, migration, "activate", async tx =>
                {
                    using (var check = new NpgsqlCommand("SELECT schema_revision, migration_mode FROM runwick.store_metadata WHERE singleton_id = 1", conn, tx))
                    {
                        var reader = await check.ExecuteReaderAsync(ct).ConfigureAwait(false);
                        await using (reader.ConfigureAwait(false))
                        {
                            if (!await reader.ReadAsync(ct).ConfigureAwait(false) || reader.GetInt64(0) != migration.TargetRevision || reader.GetString(1) != "normal")
                            {
                                throw new PostgresMigrationException("schema_validation_failed", "Activate found the store not validated.");
                            }
                        }
                    }

                    await LedgerAsync(conn, tx, migration, "activate", ct).ConfigureAwait(false);
                }, ct).ConfigureAwait(false);
                applied.Add(migration.Id + ":activate");
                await AfterPhaseAsync(migration.Id + ":activate", conn).ConfigureAwait(false);
            }
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

    Task LedgerAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Migration migration, string phase, CancellationToken ct) =>
        LedgerAsync(conn, tx, migration, migration.ChecksumFor(FaultHooks), phase, ct);

    static async Task<Dictionary<(string Id, string Phase), byte[]>> ReadLedgerAsync(NpgsqlConnection conn, CancellationToken ct)
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
            "SELECT migration_id, phase, checksum FROM runwick.schema_migrations WHERE state = 'applied'", conn);
        var ledger = new Dictionary<(string Id, string Phase), byte[]>();
        var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                ledger[(reader.GetString(0), reader.GetString(1))] = (byte[])reader[2];
            }
        }

        return ledger;
    }

    async Task PhaseAsync(NpgsqlConnection conn, Migration migration, string phase, Func<NpgsqlTransaction, Task> body, CancellationToken ct)
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
                    await RecordFailureAsync(conn, migration, migration.ChecksumFor(FaultHooks), phase, code).ConfigureAwait(false);
                }

                throw ex as PostgresMigrationException
                    ?? new PostgresMigrationException(code == "55P03" ? "migration_lock_timeout" : "migration_phase_failed",
                        $"Migration {migration.Id} phase {phase} failed ({code}).", ex);
            }
        }
    }

    static async Task LedgerAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Migration migration, byte[] checksum, string phase, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("""
            INSERT INTO runwick.schema_migrations (
                migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at)
            VALUES (@id, @phase, @checksum, @target, 'applied', @build, clock_timestamp(), clock_timestamp())
            ON CONFLICT (migration_id, phase) DO UPDATE
            SET state = 'applied', checksum = EXCLUDED.checksum, completed_at = EXCLUDED.completed_at, diagnostic_code = NULL
            """, conn, tx);
        cmd.Parameters.AddWithValue("id", migration.Id);
        cmd.Parameters.AddWithValue("target", migration.TargetRevision);
        cmd.Parameters.AddWithValue("phase", phase);
        cmd.Parameters.AddWithValue("checksum", checksum);
        cmd.Parameters.AddWithValue("build", RunnerBuild);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    static async Task RecordFailureAsync(NpgsqlConnection conn, Migration migration, byte[] checksum, string phase, string code)
    {
        try
        {
            using var cmd = new NpgsqlCommand("""
                INSERT INTO runwick.schema_migrations (
                    migration_id, phase, checksum, target_schema_revision, state, runner_build, started_at, completed_at, diagnostic_code)
                VALUES (@id, @phase, @checksum, @target, 'failed', @build, clock_timestamp(), clock_timestamp(), @code)
                ON CONFLICT (migration_id, phase) DO UPDATE
                SET state = 'failed', completed_at = EXCLUDED.completed_at, diagnostic_code = EXCLUDED.diagnostic_code
                """, conn);
            cmd.Parameters.AddWithValue("id", migration.Id);
        cmd.Parameters.AddWithValue("target", migration.TargetRevision);
            cmd.Parameters.AddWithValue("phase", phase);
            cmd.Parameters.AddWithValue("checksum", checksum);
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

    [GeneratedRegex(@"CONSTRAINT ([a-z0-9_]+) (?:PRIMARY KEY|UNIQUE|FOREIGN KEY|CHECK|EXCLUDE)")]
    private static partial Regex ConstraintPattern();

    [GeneratedRegex(@"DROP CONSTRAINT ([a-z0-9_]+)")]
    private static partial Regex DroppedConstraintPattern();

    /// <summary>Relations every migration up to and including the last one creates.</summary>
    internal static IReadOnlySet<string> ExpectedRelations => RelationsOf(Migrations);

    /// <summary>Constraints after the last migration (created minus dropped, in order).</summary>
    internal static IReadOnlySet<string> ExpectedConstraints => ConstraintsOf(Migrations);

    static HashSet<string> RelationsOf(IEnumerable<Migration> migrations) =>
        migrations.SelectMany(m => TablePattern().Matches(m.Sql).Select(x => x.Groups[1].Value)
            .Concat(IndexPattern().Matches(m.Sql).Select(x => x.Groups[1].Value))).ToHashSet(StringComparer.Ordinal);

    static HashSet<string> ConstraintsOf(IEnumerable<Migration> migrations)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var m in migrations)
        {
            set.ExceptWith(DroppedConstraintPattern().Matches(m.Sql).Select(x => x.Groups[1].Value));
            set.UnionWith(ConstraintPattern().Matches(m.Sql).Select(x => x.Groups[1].Value));
        }

        return set;
    }

    static readonly string[] DeferredForeignKeys = ["jobs_current_generation_fk", "jobs_current_attempt_fk"];
    static readonly string[] ReceiptTriggers = ["storage_commands_ownership_decided", "storage_commands_durable_decided"];

    static async Task ValidateCatalogAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyList<Migration> applied, bool hooks, CancellationToken ct)
    {
        var problems = new List<string>();
        var expectedRelations = RelationsOf(applied);
        var expectedConstraints = ConstraintsOf(applied);
        var relations = await ListAsync(conn, tx, """
            SELECT c.relname FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'runwick' AND c.relkind IN ('r', 'p', 'i', 'I')
            """, ct).ConfigureAwait(false);
        problems.AddRange(expectedRelations.Where(x => !relations.Contains(x)).Select(x => "missing relation " + x));

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
        problems.AddRange(expectedConstraints.Where(x => !byName.ContainsKey(x)).Select(x => "missing constraint " + x));
        problems.AddRange(byName.Values.Where(p => p[1] != "true").Select(p => "unvalidated constraint " + p[0]));
        problems.AddRange(DeferredForeignKeys.Where(x => !byName.TryGetValue(x, out var p) || p[2] != "true" || p[3] != "true")
            .Select(x => "constraint not deferred " + x));

        var triggers = await ListAsync(conn, tx, """
            SELECT tgname FROM pg_trigger
            WHERE tgname IN ('storage_commands_ownership_decided', 'storage_commands_durable_decided')
              AND tgenabled = 'O' AND tgdeferrable AND tginitdeferred
            """, ct).ConfigureAwait(false);
        problems.AddRange(ReceiptTriggers.Where(x => !triggers.Contains(x)).Select(x => "missing trigger " + x));

        // Function bodies (04 §8.3, PF-05): every function of these migrations, with this variant's body digest.
        var digests = (await ListAsync(conn, tx, FunctionDigestSql, ct).ConfigureAwait(false))
            .Select(x => x.Split(':')).ToDictionary(x => x[0], x => x[1], StringComparer.Ordinal);
        problems.AddRange(FunctionProblems(ExpectedFunctions(applied, hooks), digests, AllFunctionNames));

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

    /// <summary>Operations accepted at schema revision 1 (M2).</summary>
    internal static readonly string[] ManifestOperations =
    [
        "Describe", "RegisterApplication", "RegisterQueue", "RegisterScope", "Dispatch", "DispatchMany", "ResolveCommand",
        "ReadinessProbe", "PublishPresence", "ListExpiredLeases", "Reserve", "Begin", "Renew", "Finish", "Recover",
        "ReleaseReservation", "SetQueueState",
    ];

    /// <summary>
    /// Operations each later revision adds (cumulative manifest, 04 §8.5). <c>Name@v</c> adds version v of an operation
    /// that already exists (09 §2: <c>ReadinessProbe@2</c>); a bare name adds version 1.
    /// </summary>
    internal static readonly IReadOnlyDictionary<long, string[]> ManifestAdditions = new Dictionary<long, string[]>
    {
        [2] = ["Cancel", "ManualRetry", "RerunSucceeded", "EditScopePolicy", "EventsSince"],
        [3] = ["ReceiveOutbox"],
        [4] = ["ActivateCapabilities", "EditRatePolicy"],
        [5] = ["ReadinessProbe@2", "ListUnsupportedBacklog"],
        [6] = ScheduleOperations.All,
        [7] = ["SetPoolOverride", "EstimateDemand", "RecordScalingEvent"],
        [8] = ["RenewMany"],
        [9] = [],
        [10] = ["ReserveAndBegin"],
        [11] = ["FinishMany"],
        [12] = [],
    };

    internal static IReadOnlyList<string> OperationsAt(long revision) => [.. VersionsAt(revision).Keys];

    /// <summary>Operation → ascending accepted versions at a revision.</summary>
    internal static SortedDictionary<string, SortedSet<int>> VersionsAt(long revision)
    {
        var map = new SortedDictionary<string, SortedSet<int>>(StringComparer.Ordinal);
        foreach (var entry in ManifestOperations.Concat(ManifestAdditions.Where(a => a.Key <= revision).OrderBy(a => a.Key).SelectMany(a => a.Value)))
        {
            var at = entry.IndexOf('@', StringComparison.Ordinal);
            var (name, version) = at < 0 ? (entry, 1) : (entry[..at], int.Parse(entry[(at + 1)..], System.Globalization.CultureInfo.InvariantCulture));
            if (!map.TryGetValue(name, out var versions))
            {
                map[name] = versions = [];
            }

            versions.Add(version);
        }

        return map;
    }

    internal static (JsonObject Json, byte[] Digest) Manifest() => Manifest(CurrentRevision);

    internal static (JsonObject Json, byte[] Digest) Manifest(long revision)
    {
        var operations = new JsonObject();
        var canonicalOps = CanonicalValue.Object();
        foreach (var (op, versions) in VersionsAt(revision))
        {
            operations[op] = new JsonArray([.. versions.Select(v => (JsonNode?)v)]);
            canonicalOps.Add(op, CanonicalValue.Array(versions.Select(v => CanonicalValue.Int(v))));
        }

        var json = new JsonObject
        {
            ["manifestVersion"] = 1,
            ["schemaRevision"] = revision,
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
            .Add("schemaRevision", revision)
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
