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
        {"manifestVersion":1,"schemaRevision":11,"protocolEpochs":[1],"receiptCodecs":[1],"commandFingerprintVersions":[1],
         "windowFingerprintVersions":[1],"retryEvaluators":["exponential-v1"],
         "operations":{"Describe":[1],"RegisterApplication":[1],"RegisterQueue":[1],"RegisterScope":[1],"Dispatch":[1],
           "DispatchMany":[1],"ResolveCommand":[1],"ReadinessProbe":[1,2],"PublishPresence":[1],"ListExpiredLeases":[1],
           "Reserve":[1],"Begin":[1],"Renew":[1],"Finish":[1],"Recover":[1],"ReleaseReservation":[1],"SetQueueState":[1],
           "Cancel":[1],"ManualRetry":[1],"RerunSucceeded":[1],"EditScopePolicy":[1],"EventsSince":[1],
           "ActivateCapabilities":[1],"EditRatePolicy":[1],"ListUnsupportedBacklog":[1],"ReceiveOutbox":[1],
           "RegisterSchedule":[1],"MaterializeOccurrences":[1],"SetScheduleState":[1],"UpdateSchedule":[1],"TriggerSchedule":[1],
           "ResolveCatchUp":[1],"RecordScheduleEvaluationFailure":[1],"RevalidateSchedule":[1],
           "SetPoolOverride":[1],"EstimateDemand":[1],"RecordScalingEvent":[1],"RenewMany":[1],"ReserveAndBegin":[1],
           "FinishMany":[1]},
         "expandCompatibleBinaries":[]}
        """;

    static string Ns(string prefix) => prefix + "_" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>Every phase of every migration, in apply order (0001_initial, 0002_controls, 0003_outbox, 0004_rates, 0005_readiness, 0006_scheduler, 0007_scaling, 0008_renew_many, 0009_functions, 0010_reserve_and_begin, 0011_finish_many).</summary>
    static readonly string[] AllPhases = [.. PgMigrator.Migrations.SelectMany(m => new[] { "expand", "validate", "activate" }.Select(p => m.Id + ":" + p))];

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
            Assert.Equal(AllPhases, first.AppliedPhases);
            Assert.Equal(PgMigrator.CurrentRevision, first.SchemaRevision);

            var meta = await ReadMetadataAsync(cs);
            Assert.Equal((PgMigrator.CurrentRevision, 1, "normal"), (meta.Revision, meta.Epoch, meta.Mode));
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
    [InlineData("0001_initial:expand", 0L)]
    [InlineData("0001_initial:validate", 1L)]
    [InlineData("0001_initial:activate", 1L)]
    [InlineData("0002_controls:expand", 1L)]
    [InlineData("0002_controls:validate", 2L)]
    [InlineData("0002_controls:activate", 2L)]
    [InlineData("0003_outbox:expand", 2L)]
    [InlineData("0003_outbox:validate", 3L)]
    [InlineData("0003_outbox:activate", 3L)]
    [InlineData("0004_rates:expand", 3L)]
    [InlineData("0004_rates:validate", 4L)]
    [InlineData("0004_rates:activate", 4L)]
    [InlineData("0005_readiness:expand", 4L)]
    [InlineData("0005_readiness:validate", 5L)]
    [InlineData("0005_readiness:activate", 5L)]
    [InlineData("0006_scheduler:expand", 5L)]
    [InlineData("0006_scheduler:validate", 6L)]
    [InlineData("0006_scheduler:activate", 6L)]
    [InlineData("0007_scaling:expand", 6L)]
    [InlineData("0007_scaling:validate", 7L)]
    [InlineData("0007_scaling:activate", 7L)]
    [InlineData("0008_renew_many:expand", 7L)]
    [InlineData("0008_renew_many:validate", 8L)]
    [InlineData("0008_renew_many:activate", 8L)]
    [InlineData("0009_functions:expand", 8L)]
    [InlineData("0009_functions:validate", 9L)]
    [InlineData("0009_functions:activate", 9L)]
    [InlineData("0010_reserve_and_begin:expand", 9L)]
    [InlineData("0010_reserve_and_begin:validate", 10L)]
    [InlineData("0010_reserve_and_begin:activate", 10L)]
    [InlineData("0011_finish_many:expand", 10L)]
    [InlineData("0011_finish_many:validate", 11L)]
    [InlineData("0011_finish_many:activate", 11L)]
    public async Task Session_killed_after_a_phase_resumes_from_catalog_state(string killAfter, long revisionAfterKill)
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
            if (revisionAfterKill == 0)
            {
                // Never marks readiness falsely: still maintenance at revision 0, and a client rejects the store.
                Assert.Equal((0L, "maintenance"), (meta.Revision, meta.Mode));
                await using var f = new TemplateFreeClient(server, db, ns);
                await Assert.ThrowsAsync<StoreIncompatibleException>(f.EnsureReadyAsync);
            }
            else
            {
                Assert.Equal((revisionAfterKill, "normal"), (meta.Revision, meta.Mode));
            }

            var resumed = await RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken);
            var expectedRemaining = AllPhases.SkipWhile(p => p != killAfter).Skip(1);
            Assert.Equal(expectedRemaining, resumed.AppliedPhases);
            meta = await ReadMetadataAsync(admin);
            Assert.Equal((PgMigrator.CurrentRevision, 1, "normal"), (meta.Revision, meta.Epoch, meta.Mode));
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
            Assert.Equal(AllPhases.Length, results.Sum(r => r.AppliedPhases.Count));
            Assert.Contains(results, r => r.AppliedPhases.Count == 0);
            Assert.Equal(results[0].StoreId, results[1].StoreId);

            var conn = new NpgsqlConnection(cs);
            await using (conn)
            {
                await conn.OpenAsync();
                using var cmd = new NpgsqlCommand("SELECT count(*) FROM runwick.schema_migrations WHERE state = 'applied'", conn);
                Assert.Equal(4L * PgMigrator.Migrations.Count, (long)(await cmd.ExecuteScalarAsync())!);
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
            await StoreServer.ExecAsync(cs, "UPDATE runwick.schema_migrations SET checksum = sha256('tampered') WHERE phase = 'expand' AND migration_id = '0002_controls'");
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
