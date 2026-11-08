using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Protocol;
using Runwick.Serialization;
using Runwick.Sqlite.Store;

namespace Runwick
{

/// <summary>Outcome of one SQLite migration run (20 §9.1).</summary>
public sealed record SqliteMigrationResult(Guid StoreId, long SchemaRevision, IReadOnlyList<string> AppliedPhases);

/// <summary>A migration step failed; <see cref="Code"/> is one of the 20 §9.1 codes.</summary>
public sealed class SqliteMigrationException(string code, string message, Exception? innerException = null) : Exception(message, innerException)
{
    public string Code { get; } = code;
}

/// <summary>The explicit deployment migrator for the SQLite store. Workers never migrate on startup.</summary>
public static partial class RunwickSqliteMigrations
{
    /// <summary>Creates the schema in one transaction, or does nothing when it is already applied (20 §9.2).</summary>
    public static Task<SqliteMigrationResult> MigrateAsync(string connectionString, CancellationToken cancellationToken = default) =>
        Task.Run(() => Runwick.Sqlite.Migrations.SqliteMigrator.Run(connectionString, cancellationToken), cancellationToken);
}

namespace Sqlite.Migrations
{
    internal static class SqliteMigrator
    {
        public const long CurrentRevision = 1;
        const string MigrationId = "0001_initial";
        const string ManifestDomain = "runwick.manifest.v1";
        static readonly string RunnerBuild = "runwick/" + (typeof(SqliteMigrator).Assembly.GetName().Version?.ToString() ?? "0");

        /// <summary>Test seam (SQL-10): replaces the migration text, so a changed checksum can be shown.</summary>
        internal static Func<string, string>? SqlOverride { get; set; }

        internal static string Sql
        {
            get
            {
                using var stream = typeof(SqliteMigrator).Assembly.GetManifestResourceStream("Runwick.Sqlite.Sql.0001_initial.sql")
                    ?? throw new InvalidOperationException("Embedded SQLite migration missing.");
                using var reader = new StreamReader(stream, Encoding.UTF8);
                var sql = reader.ReadToEnd().Replace("\r\n", "\n", StringComparison.Ordinal);
                return SqlOverride?.Invoke(sql) ?? sql;
            }
        }

        internal static byte[] Checksum(string sql) => SHA256.HashData(Encoding.UTF8.GetBytes(sql));

        public static SqliteMigrationResult Run(string connectionString, CancellationToken ct)
        {
            string cs;
            try
            {
                cs = SqFile.Normalize(connectionString, SqliteOpenMode.ReadWriteCreate);
                SqFile.CheckTopology(SqFile.FilePath(cs));
            }
            catch (SqProfileException ex)
            {
                throw new SqliteMigrationException(ex.Code, ex.Message);
            }

            SqliteConnection conn;
            try
            {
                conn = SqFile.Open(cs, 60_000, checkWal: false);
                SqFile.CheckEngine(conn);
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
                ct.ThrowIfCancellationRequested();
                try
                {
                    if (!string.Equals(SqFile.Scalar(conn, "PRAGMA journal_mode = WAL"), "wal", StringComparison.OrdinalIgnoreCase))
                    {
                        throw new SqliteMigrationException("store_unavailable", "The database could not be switched to journal_mode=WAL.");
                    }
                }
                catch (SqliteException ex) when (ex.SqliteErrorCode == 5)
                {
                    throw new SqliteMigrationException("migration_lock_timeout", "Another process holds the database while switching to WAL.", ex);
                }

                try
                {
                    SqFile.Exec(conn, "BEGIN IMMEDIATE");
                }
                catch (SqliteException ex) when (ex.SqliteErrorCode == 5)
                {
                    throw new SqliteMigrationException("migration_lock_timeout", "The migration lock (the write lock) was not granted within 60 s.", ex);
                }

                try
                {
                    var result = Apply(conn);
                    SqFile.Exec(conn, "COMMIT");
                    return result;
                }
                catch
                {
                    if (SqExecutor.InTransaction(conn))
                    {
                        SqFile.Exec(conn, "ROLLBACK");
                    }

                    throw;
                }
            }
        }

        static SqliteMigrationResult Apply(SqliteConnection conn)
        {
            var sql = Sql;
            var checksum = Checksum(sql);
            var tables = List(conn, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'runwick\\_%' ESCAPE '\\'");
            if (tables.Contains("runwick_schema_migrations"))
            {
                // A ledger row this binary does not know means the store is ahead of it (23 §5, UPG-06).
                var unknown = Scalar(conn, $"SELECT migration_id FROM runwick_schema_migrations WHERE migration_id <> '{MigrationId}' ORDER BY migration_id LIMIT 1");
                if (unknown is string newer)
                {
                    throw new SqliteMigrationException("migration_unknown", $"Applied migration {newer} is not known to this binary (newer store).");
                }

                using var read = conn.CreateCommand();
                read.CommandText = "SELECT DISTINCT checksum FROM runwick_schema_migrations WHERE migration_id = @id";
                read.Parameters.AddWithValue("@id", MigrationId);
                using (var r = read.ExecuteReader())
                {
                    while (r.Read())
                    {
                        if (!((byte[])r.GetValue(0)).AsSpan().SequenceEqual(checksum))
                        {
                            throw new SqliteMigrationException("migration_checksum_mismatch", $"Migration {MigrationId} was applied with a different checksum.");
                        }
                    }
                }

                var id = SqUuid.Read((byte[])Scalar(conn, "SELECT store_id FROM runwick_store_metadata WHERE singleton_id = 1")!);
                return new SqliteMigrationResult(id, CurrentRevision, []);
            }

            // The outbox source tables (14 §3.3) may share the file; anything else named runwick_ is a foreign schema.
            if (tables.Any(t => t is not ("runwick_outbox_metadata" or "runwick_outbox_messages")))
            {
                throw new SqliteMigrationException("schema_not_empty", "The database has runwick_ tables but no Runwick migration ledger.");
            }

            var now = SqTime.Ms(DateTimeOffset.UtcNow);
            try
            {
                SqFile.Exec(conn, sql);
                var storeId = Guid.NewGuid();
                var (manifest, digest) = Manifest();
                using (var meta = conn.CreateCommand())
                {
                    meta.CommandText = """
                        INSERT INTO runwick_store_metadata (singleton_id, store_id, driver, schema_revision, protocol_epoch, migration_mode,
                            compatibility_manifest_version, compatibility_manifest, manifest_digest, revision, updated_at)
                        VALUES (1, @id, 'sqlite', 1, 1, 'normal', 1, @manifest, @digest, 1, @now)
                        """;
                    meta.P("id", storeId).P("manifest", manifest.ToJsonString()).P("digest", digest).P("now", now);
                    meta.ExecuteNonQuery();
                }

                Validate(conn);
                foreach (var phase in new[] { "expand", "validate", "activate" })
                {
                    using var ledger = conn.CreateCommand();
                    ledger.CommandText = """
                        INSERT INTO runwick_schema_migrations (migration_id, phase, checksum, target_schema_revision, state, runner_build,
                            started_at, completed_at)
                        VALUES (@id, @phase, @checksum, 1, 'applied', @build, @now, @now)
                        """;
                    ledger.P("id", MigrationId).P("phase", phase).P("checksum", checksum).P("build", RunnerBuild).P("now", now);
                    ledger.ExecuteNonQuery();
                }

                return new SqliteMigrationResult(storeId, CurrentRevision, [MigrationId + ":expand", MigrationId + ":validate", MigrationId + ":activate"]);
            }
            catch (SqliteException ex)
            {
                throw new SqliteMigrationException("migration_phase_failed", "The SQLite migration failed and was rolled back.", ex);
            }
        }

        /// <summary>
        /// 20 §9.4: the file's runwick_ objects equal the binary's (the migration run on an in-memory database), foreign keys
        /// hold, and integrity_check is ok.
        /// </summary>
        internal static void Validate(SqliteConnection conn)
        {
            var expected = Objects(Expected.Value);
            var actual = Objects(conn);
            if (!expected.SetEquals(actual))
            {
                var diff = expected.Except(actual).Concat(actual.Except(expected)).Select(o => o.Split('\u0001')[1]).Distinct();
                throw new SqliteMigrationException("schema_validation_failed", "Schema objects differ: " + string.Join(", ", diff));
            }

            if (List(conn, "PRAGMA foreign_key_check").Count > 0 || SqFile.Scalar(conn, "PRAGMA integrity_check") != "ok")
            {
                throw new SqliteMigrationException("schema_validation_failed", "foreign_key_check or integrity_check failed.");
            }
        }

        /// <summary>Whether the file's objects are this binary's (Describe uses it to refuse a changed schema).</summary>
        internal static bool SchemaMatches(SqliteConnection conn) => Objects(Expected.Value).SetEquals(Objects(conn));

        static readonly Lazy<SqliteConnection> Expected = new(() =>
        {
            var mem = new SqliteConnection("Data Source=:memory:");
            mem.Open();
            SqFile.Exec(mem, Sql);
            return mem;
        });

        static HashSet<string> Objects(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                SELECT type, name, tbl_name, sql FROM sqlite_schema
                WHERE name LIKE 'runwick\_%' ESCAPE '\' AND name NOT LIKE 'runwick\_outbox\_%' ESCAPE '\'
                  AND tbl_name NOT LIKE 'runwick\_outbox\_%' ESCAPE '\'
                """;
            var set = new HashSet<string>(StringComparer.Ordinal);
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var text = r.IsDBNull(3) ? "" : string.Join(' ', r.GetString(3).Split((char[])[' ', '\n', '\t', '\r'], StringSplitOptions.RemoveEmptyEntries));
                set.Add(string.Join('\u0001', r.GetString(0), r.GetString(1), r.GetString(2), text));
            }

            return set;
        }

        static HashSet<string> List(SqliteConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            var set = new HashSet<string>(StringComparer.Ordinal);
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                set.Add(Convert.ToString(r.GetValue(0), System.Globalization.CultureInfo.InvariantCulture)!);
            }

            return set;
        }

        static object? Scalar(SqliteConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteScalar();
        }

        // ------------------------------------------------------------------ manifest (20 §9.3, 04 §8.5)

        /// <summary>
        /// The PostgreSQL revision-17 operation set and versions: revision 15's set, CleanupBatch/1 and EditRetentionPolicy/1
        /// (17, revision 16) and ManagementDispatch/1 (19 §6.2).
        /// </summary>
        internal static readonly string[] ManifestOperations =
        [
            "Describe", "RegisterApplication", "RegisterQueue", "RegisterScope", "Dispatch", "DispatchMany", "ResolveCommand",
            "ReadinessProbe", "ReadinessProbe@2", "PublishPresence", "ListExpiredLeases", "Reserve", "Begin", "Renew", "Finish", "Recover",
            "ReleaseReservation", "SetQueueState", "Cancel", "ManualRetry", "RerunSucceeded", "EditScopePolicy", "EventsSince",
            "ReceiveOutbox", "ActivateCapabilities", "EditRatePolicy", "ListUnsupportedBacklog",
            .. ScheduleOperations.All,
            "SetPoolOverride", "EstimateDemand", "RecordScalingEvent", "RenewMany", "ReserveAndBegin", "FinishMany", "RecordDisclosure",
            "CleanupBatch", "EditRetentionPolicy", "ManagementDispatch",
        ];

        internal static SortedDictionary<string, SortedSet<int>> Versions()
        {
            var map = new SortedDictionary<string, SortedSet<int>>(StringComparer.Ordinal);
            foreach (var entry in ManifestOperations)
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

        internal static (JsonObject Json, byte[] Digest) Manifest()
        {
            var operations = new JsonObject();
            var canonicalOps = CanonicalValue.Object();
            foreach (var (op, versions) in Versions())
            {
                operations[op] = new JsonArray([.. versions.Select(v => (JsonNode?)v)]);
                canonicalOps.Add(op, CanonicalValue.Array(versions.Select(v => CanonicalValue.Int(v))));
            }

            var json = new JsonObject
            {
                ["manifestVersion"] = 1,
                ["schemaRevision"] = CurrentRevision,
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
                .Add("schemaRevision", CurrentRevision)
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
}
}
