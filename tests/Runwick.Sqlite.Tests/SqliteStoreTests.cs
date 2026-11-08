using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Internal;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Sqlite.Migrations;
using Runwick.Sqlite.Store;

namespace Runwick.Sqlite.Tests;

/// <summary>The process-wide test seams (version, mountinfo, parameter limit, migration text) are set by these tests only.</summary>
[CollectionDefinition(nameof(SqliteSeams), DisableParallelization = true)]
public sealed class SqliteSeams;

/// <summary>SQLite-only cases of 20 §12 (SQL-02, 06, 08, 09, 10, 12).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
[Collection(nameof(SqliteSeams))]
public sealed class SqliteStoreTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        return (f, c);
    }

    static SqliteConnection Raw(DriverFixture f)
    {
        var conn = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False");
        conn.Open();
        return conn;
    }

    static string Scalar(SqliteConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    // ------------------------------------------------------------------ SQL-06 connection profile and topology

    [Fact]
    [Trait("Case", "SQL-06")]
    public async Task Every_driver_connection_reads_back_the_profile()
    {
        var (f, c) = await OpenAsync("sql06a");
        await using var _ = f;
        var executor = c.Services.GetRequiredService<SqExecutor>();
        string[] Pragmas(SqliteConnection conn) =>
            [Scalar(conn, "PRAGMA journal_mode"), Scalar(conn, "PRAGMA synchronous"), Scalar(conn, "PRAGMA foreign_keys"),
             Scalar(conn, "PRAGMA trusted_schema"), Scalar(conn, "PRAGMA journal_size_limit")];
        Assert.Equal(["wal", "2", "1", "0", "67108864"], await executor.WriteAsync(SqLane.Background, Pragmas, Ct));
        Assert.Equal(["wal", "2", "1", "0", "67108864"], await executor.ReadAsync(Pragmas, Ct));
        Assert.Equal("2000", await executor.WriteAsync(SqLane.Background, conn => Scalar(conn, "PRAGMA busy_timeout"), Ct));
    }

    [Fact]
    [Trait("Case", "SQL-06")]
    public async Task A_file_switched_to_rollback_journal_fails_startup_with_store_profile_mismatch()
    {
        await using var f = await DriverFixture.StartAsync("sql06b_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Sqlite);
        using (var conn = Raw(f))
        {
            Assert.Equal("delete", Scalar(conn, "PRAGMA journal_mode = DELETE"));
        }

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => f.OpenClient(configure: r => TestJobs.Register(r)));
        Assert.Contains("store_profile_mismatch", ex.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [Trait("Case", "SQL-06")]
    [InlineData("Data Source=:memory:")]
    [InlineData("Data Source=")]
    [InlineData("Data Source=x.db;Mode=Memory")]
    [InlineData("Data Source=x.db;Mode=ReadOnly")]
    [InlineData("Data Source=x.db;Cache=Shared")]
    [InlineData("Data Source=x.db;Foreign Keys=False")]
    public void Semantics_changing_connection_strings_are_refused_with_invalid_policy(string cs)
    {
        var ex = Assert.Throws<SqProfileException>(() => SqFile.Normalize(cs, SqliteOpenMode.ReadWrite));
        Assert.Equal("invalid_policy", ex.Code);
    }

    [Fact]
    [Trait("Case", "SQL-06")]
    public async Task UseSqlite_reports_a_refused_connection_string_as_a_configuration_error()
    {
        var services = new ServiceCollection();
        services.AddRunwick(r =>
        {
            r.Application("cfg");
            TestJobs.Register(r);
            r.UseSqlite("Data Source=x.db;Cache=Shared");
        });
        await using var provider = services.BuildServiceProvider();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => provider.GetRequiredService<Hosting.RunwickRuntime>().EnsureReadyAsync(Ct));
        Assert.Contains("invalid_policy", ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "SQL-06")]
    public void An_nfs_mount_is_refused_and_an_unknown_one_is_unverified()
    {
        const string MountInfo = """
            22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
            40 22 0:35 / /mnt/share rw,relatime shared:20 - nfs4 server:/export rw,vers=4.2
            41 22 0:36 / /mnt/odd rw,relatime shared:21 - weirdfs none rw
            """;
        Assert.Equal("nfs4", SqFile.FileSystemOf(MountInfo, "/mnt/share/data/runwick.db"));
        Assert.Equal("ext4", SqFile.FileSystemOf(MountInfo, "/var/lib/runwick.db"));
        SqFile.MountInfoOverride = () => MountInfo;
        try
        {
            Assert.Equal("topology_unsupported", Assert.Throws<SqProfileException>(() => SqFile.CheckTopology("/mnt/share/data/runwick.db")).Code);
            Assert.Equal("unverified", SqFile.CheckTopology("/mnt/odd/runwick.db"));
            Assert.Equal("local", SqFile.CheckTopology("/var/lib/runwick.db"));
        }
        finally
        {
            SqFile.MountInfoOverride = null;
        }
    }

    // ------------------------------------------------------------------ SQL-08 engine

    [Fact]
    [Trait("Case", "SQL-08")]
    public async Task Describe_reports_the_bundled_engine_and_an_old_engine_is_refused()
    {
        var (f, c) = await OpenAsync("sql08");
        await using var _ = f;
        var store = c.Services.GetRequiredService<SqliteStore>();
        await store.DescribeAsync(c.Application, Ct);
        var version = Version.Parse(store.EngineFacts["sqlite_version"]);
        Assert.True(version >= new Version(3, 38, 0), $"bundled SQLite {version}");
        Assert.NotEmpty(store.EngineFacts["sqlite_source_id"]);
        TestContext.Current.SendDiagnosticMessage($"bundled SQLite {version} ({store.EngineFacts["sqlite_source_id"]})");

        // Native assets for both release targets ship with the build (linux-x64 loads here; win-x64 is checked as present).
        var bin = AppContext.BaseDirectory;
        Assert.True(File.Exists(Path.Combine(bin, "runtimes", "linux-x64", "native", "libe_sqlite3.so")));
        Assert.True(File.Exists(Path.Combine(bin, "runtimes", "win-x64", "native", "e_sqlite3.dll")));

        SqFile.VersionOverride = _ => "3.37.2";
        try
        {
            var ex = await Assert.ThrowsAsync<StoreIncompatibleException>(() => store.DescribeAsync(c.Application, Ct));
            Assert.Contains("engine_version_unsupported", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            SqFile.VersionOverride = null;
        }
    }

    // ------------------------------------------------------------------ SQL-09 encoding

    [Fact]
    [Trait("Case", "SQL-09")]
    public void Blob_order_is_uuid_order_and_text_order_is_utf8_byte_order()
    {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using (var create = conn.CreateCommand())
        {
            create.CommandText = "CREATE TABLE t (u BLOB, s TEXT) STRICT";
            create.ExecuteNonQuery();
        }

        var guids = Enumerable.Range(0, 500).Select(_ => Guid.NewGuid()).Concat([Guid.Empty, Guid.AllBitsSet, Guid.CreateVersion7()]).ToList();
        string[] texts = ["a", "B", "é", "z", "ÿ", "Ā", "😀", "￿", "", "aa"];
        foreach (var (g, i) in guids.Select((g, i) => (g, i)))
        {
            using var insert = conn.CreateCommand();
            insert.CommandText = "INSERT INTO t VALUES (@u, @s)";
            insert.Parameters.AddWithValue("@u", SqUuid.Bytes(g));
            insert.Parameters.AddWithValue("@s", texts[i % texts.Length]);
            insert.ExecuteNonQuery();
        }

        var byBlob = new List<Guid>();
        using (var q = conn.CreateCommand())
        {
            q.CommandText = "SELECT u FROM t ORDER BY u";
            using var r = q.ExecuteReader();
            while (r.Read())
            {
                byBlob.Add(SqUuid.Read((byte[])r.GetValue(0)));
            }
        }

        Assert.Equal(guids.Order(UuidOrder.Instance), byBlob);

        var byText = new List<string>();
        using (var q = conn.CreateCommand())
        {
            q.CommandText = "SELECT DISTINCT s FROM t ORDER BY s";
            using var r = q.ExecuteReader();
            while (r.Read())
            {
                byText.Add(r.GetString(0));
            }
        }

        Assert.Equal(texts.Order(Utf8Order.Instance), byText);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1000), SqTime.Floor(DateTimeOffset.FromUnixTimeMilliseconds(1000).AddTicks(9_999)));
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1001), SqTime.Ceil(DateTimeOffset.FromUnixTimeMilliseconds(1000).AddTicks(1)));
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1000), SqTime.Ceil(DateTimeOffset.FromUnixTimeMilliseconds(1000)));
    }

    [Fact]
    [Trait("Case", "SQL-09")]
    public async Task A_fence_at_long_max_value_is_counter_overflow_and_writes_nothing()
    {
        var (f, c) = await OpenAsync("sql09");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("a", "r"), cancellationToken: Ct)).JobId.Value;
        using (var conn = Raw(f))
        {
            Assert.Equal("1", Scalar(conn, "UPDATE runwick_concurrency_scopes SET fencing_counter = 9223372036854775807 WHERE fencing_enabled = 1; SELECT changes();"));
        }

        var before = await f.ReadCommittedSnapshot();
        var reply = await c.ReserveAsync();
        Assert.Equal(StoreStatus.InvariantFault, reply.Status);
        Assert.Equal("counter_overflow", reply.Error!.Code);
        var after = await f.ReadCommittedSnapshot();
        Assert.Empty(before.DiffTables(after));
        Assert.Equal("pending", after.Model.Job(job).State);
    }

    // ------------------------------------------------------------------ SQL-10 migrations

    [Fact]
    [Trait("Case", "SQL-10")]
    public async Task Migration_applies_once_and_refuses_changed_text_and_foreign_tables()
    {
        var dir = Directory.CreateTempSubdirectory("runwick-sqlite-mig-").FullName;
        try
        {
            var cs = $"Data Source={Path.Combine(dir, "a.db")}";
            var first = await RunwickSqliteMigrations.MigrateAsync(cs, Ct);
            Assert.Equal(1, first.SchemaRevision);
            Assert.NotEmpty(first.AppliedPhases);
            var again = await RunwickSqliteMigrations.MigrateAsync(cs, Ct);
            Assert.Equal(first.StoreId, again.StoreId);
            Assert.Empty(again.AppliedPhases);

            SqliteMigrator.SqlOverride = sql => sql + "\n-- changed";
            try
            {
                Assert.Equal("migration_checksum_mismatch",
                    (await Assert.ThrowsAsync<SqliteMigrationException>(() => RunwickSqliteMigrations.MigrateAsync(cs, Ct))).Code);
            }
            finally
            {
                SqliteMigrator.SqlOverride = null;
            }

            // A foreign runwick_ table in an unmigrated file; an outbox source table in the same file is left alone.
            var other = $"Data Source={Path.Combine(dir, "b.db")};Pooling=False";
            using (var conn = new SqliteConnection(other))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE runwick_x (a INTEGER)";
                cmd.ExecuteNonQuery();
            }

            Assert.Equal("schema_not_empty", (await Assert.ThrowsAsync<SqliteMigrationException>(() => RunwickSqliteMigrations.MigrateAsync(other, Ct))).Code);

            var shared = $"Data Source={Path.Combine(dir, "c.db")};Pooling=False";
            using (var conn = new SqliteConnection(shared))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE runwick_outbox_messages (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO runwick_outbox_messages (body) VALUES ('m');";
                cmd.ExecuteNonQuery();
            }

            await RunwickSqliteMigrations.MigrateAsync(shared, Ct);
            using (var conn = new SqliteConnection(shared))
            {
                conn.Open();
                Assert.Equal("m", Scalar(conn, "SELECT body FROM runwick_outbox_messages"));
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    [Trait("Case", "SQL-10")]
    public async Task Concurrent_migrations_apply_once()
    {
        var dir = Directory.CreateTempSubdirectory("runwick-sqlite-mig-").FullName;
        try
        {
            var cs = $"Data Source={Path.Combine(dir, "a.db")}";
            var runs = await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
            {
                try
                {
                    return (Result: await RunwickSqliteMigrations.MigrateAsync(cs, Ct), Code: (string?)null);
                }
                catch (SqliteMigrationException ex)
                {
                    return (Result: (SqliteMigrationResult?)null, Code: ex.Code);
                }
            }));
            Assert.All(runs, r => Assert.True(r.Result is not null || r.Code == "migration_lock_timeout", r.Code));
            Assert.Single(runs, r => r.Result is { AppliedPhases.Count: > 0 });
            Assert.Single(runs.Where(r => r.Result is not null).Select(r => r.Result!.StoreId).Distinct());
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(dir, recursive: true);
        }
    }

    // ------------------------------------------------------------------ SQL-02 another writer holds the lock

    [Fact]
    [Trait("Case", "SQL-02")]
    public async Task A_held_write_lock_delays_the_command_which_applies_once_after_release_or_expires()
    {
        var (f, c) = await OpenAsync("sql02");
        await using var _ = f;
        c.Services.GetRequiredService<SqliteStoreRegistration>().Options.WriterBusyTimeout = TimeSpan.FromMilliseconds(150);
        var runner = c.Services.GetRequiredService<SqRunner>();

        // Shortcut: the lock holder is a second connection in this process, not a peer process; SQLite's file lock is the same.
        using (var holder = Raw(f))
        {
            using (var begin = holder.CreateCommand())
            {
                begin.CommandText = "BEGIN IMMEDIATE";
                begin.ExecuteNonQuery();
            }

            // Microsoft.Data.Sqlite retries a busy statement for its 1 s command timeout, so a hold shorter than that is
            // only a longer wait; 1.5 s makes the runner see SQLITE_BUSY and retry the whole transaction.
            var dispatch = c.Dispatcher.DispatchAsync(new PingJob("held"), cancellationToken: Ct);
            await Task.Delay(1500, Ct);
            Assert.False(dispatch.IsCompleted);
            using (var commit = holder.CreateCommand())
            {
                commit.CommandText = "COMMIT";
                commit.ExecuteNonQuery();
            }

            var receipt = await dispatch;
            Assert.Equal(DispatchDisposition.Created, receipt.Disposition);
            Assert.True(runner.BusyConflicts > 0);
        }

        Assert.Single((await f.ReadCommittedSnapshot()).Model.Jobs);

        using (var holder = Raw(f))
        {
            using (var begin = holder.CreateCommand())
            {
                begin.CommandText = "BEGIN IMMEDIATE";
                begin.ExecuteNonQuery();
            }

            var expired = await c.ReserveAsync(notAfter: DateTimeOffset.UtcNow.AddMilliseconds(300));
            Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        }

        Assert.Equal("pending", Assert.Single((await f.ReadCommittedSnapshot()).Model.Jobs).State);
    }

    // ------------------------------------------------------------------ SQL-12 batches and set-based dispatch

    [Fact]
    [Trait("Case", "SQL-12")]
    public async Task Set_based_DispatchMany_crosses_the_parameter_chunk_boundary()
    {
        var (f, c) = await OpenAsync("sql12a");
        await using var _ = f;
        SqliteStore.ParameterLimit = 64;
        try
        {
            var items = Enumerable.Range(0, 100).Select(i => new BatchDispatchItem(new PingJob("p" + i))).ToList();
            var receipt = await c.Dispatcher.DispatchManyAsync(items, cancellationToken: Ct);
            Assert.Equal(100, receipt.Items.Count);
            Assert.All(receipt.Items, i => Assert.Equal(DispatchDisposition.Created, i.Receipt!.Disposition));
        }
        finally
        {
            SqliteStore.ParameterLimit = SqliteStore.MaxParameters;
        }

        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal(100, snapshot["jobs"].Count);
        Assert.Equal(100, snapshot["job_payloads"].Count);
        Assert.Equal(100, snapshot["job_generations"].Count);
        Assert.Equal(100, snapshot["queue_events"].Count(e => e.Contains("event_kind=dispatched;", StringComparison.Ordinal)));

        // Every job reads back as one dispatched one by one would: reservable with its own payload.
        var claimed = await c.ReserveAndBeginAsync(32);
        Assert.Equal(32, claimed.Value!.Outcomes.Count(o => o.Status == StoreStatus.Applied));
    }

    [Fact]
    [Trait("Case", "SQL-12")]
    public async Task RenewMany_of_64_with_a_stale_member_commits_once()
    {
        var (f, c) = await OpenAsync("sql12b");
        await using var _ = f;
        await c.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, 64).Select(i => new BatchDispatchItem(new ResourceJob("j" + i, "r" + i)))],
            cancellationToken: Ct);
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 2; i++)
        {
            leases.AddRange((await c.ReserveAndBeginAsync(32)).Value!.Outcomes.Select(o => o.Reserved!.Lease.Handle));
        }

        Assert.Equal(64, leases.Count);
        leases[10] = leases[10] with { OwnershipToken = Guid.NewGuid() };
        var runner = c.Services.GetRequiredService<SqRunner>();
        var commits = runner.Commits;
        var reply = await c.RenewManyAsync(leases);
        Assert.Equal(63, reply.Value!.Outcomes.Count(o => o.Status == StoreStatus.Applied));
        Assert.Equal(StoreStatus.StaleLease, reply.Value.Outcomes[10].Status);
        Assert.Equal(commits + 1, runner.Commits);

        var finishCommits = runner.Commits;
        var finished = await c.FinishManyAsync([.. leases.Where((_, i) => i != 10).Take(32).Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded))]);
        Assert.All(finished.Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        Assert.Equal(finishCommits + 1, runner.Commits);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }
}
