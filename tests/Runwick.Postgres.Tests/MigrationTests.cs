using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;

namespace Runwick.Postgres.Tests;

[Trait("Category", "Fault")]
[Trait("Item", "SP-005")]
[Trait("Case", "INIT-01")]
public sealed class MigrationTests
{
    const string SpecManifest = """
        {"manifestVersion":1,"schemaRevision":1,"protocolEpochs":[1],"receiptCodecs":[1],"commandFingerprintVersions":[1],
         "windowFingerprintVersions":[1],"retryEvaluators":["exponential-v1"],
         "operations":{"Describe":[1],"RegisterApplication":[1],"RegisterQueue":[1],"RegisterScope":[1],"Dispatch":[1],
           "DispatchMany":[1],"ResolveCommand":[1],"ReadinessProbe":[1],"PublishPresence":[1],"ListExpiredLeases":[1],
           "Reserve":[1],"Begin":[1],"Renew":[1],"Finish":[1],"Recover":[1],"ReleaseReservation":[1],"SetQueueState":[1]},
         "expandCompatibleBinaries":[]}
        """;

    static string Ns(string prefix) => prefix + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<(long Revision, int Epoch, string Mode, string Manifest, byte[] Digest)> ReadMetadataAsync(string cs)
    {
        var conn = new NpgsqlConnection(cs);
        await using (conn)
        {
            await conn.OpenAsync();
            using var cmd = new NpgsqlCommand(
                "SELECT schema_revision, protocol_epoch, migration_mode, compatibility_manifest::text, manifest_digest FROM runwick.store_metadata", conn);
            await using var r = await cmd.ExecuteReaderAsync();
            await r.ReadAsync();
            return (r.GetInt64(0), r.GetInt32(1), r.GetString(2), r.GetString(3), (byte[])r[4]);
        }
    }

    [Fact]
    public async Task Fresh_run_installs_revision_epoch_and_manifest_and_rerun_is_a_no_op()
    {
        var server = await StoreServer.SharedAsync();
        var db = await server.CreateEmptyDatabaseAsync(Ns("init_fresh"));
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            var first = await RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken);
            Assert.Equal(["expand", "validate", "activate"], first.AppliedPhases);
            Assert.Equal(1, first.SchemaRevision);

            var meta = await ReadMetadataAsync(cs);
            Assert.Equal((1L, 1, "normal"), (meta.Revision, meta.Epoch, meta.Mode));
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(SpecManifest), JsonNode.Parse(meta.Manifest)));
            Assert.Equal(PgMigrator.Manifest().Digest, meta.Digest);

            var second = await RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken);
            Assert.Empty(second.AppliedPhases);
            Assert.Equal(first.StoreId, second.StoreId);
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    [Theory]
    [InlineData("expand")]
    [InlineData("validate")]
    [InlineData("activate")]
    public async Task Session_killed_after_a_phase_resumes_from_catalog_state(string killAfter)
    {
        var server = await StoreServer.SharedAsync();
        var ns = Ns("init_kill");
        var db = await server.CreateEmptyDatabaseAsync(ns);
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            var admin = server.ConnectionStringFor(db, "rw:harness:admin", pooling: false);
            var migrator = new PgMigrator(cs, async (phase, conn) =>
            {
                if (phase == killAfter)
                {
                    await StoreServer.ExecAsync(admin, $"SELECT pg_terminate_backend({conn.ProcessID})");
                }
            });
            await Assert.ThrowsAnyAsync<Exception>(() => migrator.RunAsync(TestContext.Current.CancellationToken));

            var meta = await ReadMetadataAsync(admin);
            if (killAfter == "expand")
            {
                // Never marks readiness falsely: still maintenance at revision 0, and a client rejects the store.
                Assert.Equal((0L, "maintenance"), (meta.Revision, meta.Mode));
                await using var f = new TemplateFreeClient(server, db, ns);
                await Assert.ThrowsAsync<StoreIncompatibleException>(f.EnsureReadyAsync);
            }
            else
            {
                Assert.Equal((1L, "normal"), (meta.Revision, meta.Mode));
            }

            var resumed = await RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken);
            var expectedRemaining = new[] { "expand", "validate", "activate" }.SkipWhile(p => p != killAfter).Skip(1);
            Assert.Equal(expectedRemaining, resumed.AppliedPhases);
            meta = await ReadMetadataAsync(admin);
            Assert.Equal((1L, 1, "normal"), (meta.Revision, meta.Epoch, meta.Mode));
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    [Fact]
    public async Task Concurrent_runners_are_serialized_by_the_advisory_lock()
    {
        var server = await StoreServer.SharedAsync();
        var db = await server.CreateEmptyDatabaseAsync(Ns("init_conc"));
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            var results = await Task.WhenAll(
                RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken),
                RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken));
            Assert.Equal(3, results.Sum(r => r.AppliedPhases.Count));
            Assert.Contains(results, r => r.AppliedPhases.Count == 0);
            Assert.Equal(results[0].StoreId, results[1].StoreId);

            var conn = new NpgsqlConnection(cs);
            await using (conn)
            {
                await conn.OpenAsync();
                using var cmd = new NpgsqlCommand("SELECT count(*) FROM runwick.schema_migrations WHERE state = 'applied'", conn);
                Assert.Equal(4L, (long)(await cmd.ExecuteScalarAsync())!);
            }
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    [Fact]
    public async Task Checksum_mismatch_is_a_hard_error()
    {
        var server = await StoreServer.SharedAsync();
        var db = await server.CreateEmptyDatabaseAsync(Ns("init_sum"));
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            await RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken);
            await StoreServer.ExecAsync(cs, "UPDATE runwick.schema_migrations SET checksum = sha256('tampered') WHERE phase = 'expand'");
            var ex = await Assert.ThrowsAsync<PostgresMigrationException>(() => RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken));
            Assert.Equal("migration_checksum_mismatch", ex.Code);
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }

    /// <summary>A producer-only client against a database that is not a template clone.</summary>
    sealed class TemplateFreeClient(StoreServer server, string database, string application) : IAsyncDisposable
    {
        readonly Microsoft.Extensions.DependencyInjection.ServiceProvider _provider = Build(server, database, application);

        static Microsoft.Extensions.DependencyInjection.ServiceProvider Build(StoreServer server, string database, string application)
        {
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            services.AddLogging();
            services.AddRunwick(r => r.Application(application).UsePostgres(server.ConnectionStringFor(database, "rw:local:work")));
            return services.BuildServiceProvider();
        }

        public Task EnsureReadyAsync() =>
            _provider.GetRequiredService<Runwick.Hosting.RunwickRuntime>().EnsureReadyAsync(CancellationToken.None);

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }
}
