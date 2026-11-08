using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// Declared-unsupported PostgreSQL environments fail with an explicit code and write nothing (driver-capabilities §1, driver-contract-suite §7).
/// Both cases carry the <c>Unsupported</c> trait so the conformance report can tell them from ordinary passes.
/// </summary>
[Trait("Item", "SP-028")]
[Trait("Unsupported", "postgres")]
public sealed class UnsupportedCapabilityTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    [Trait("Case", "UNS-PG-01")]
    public async Task A_database_that_is_not_UTF8_is_refused_by_the_migrator_with_encoding_unsupported()
    {
        var server = await StoreServer.SharedAsync();
        var name = "rw_uns_" + Guid.NewGuid().ToString("N")[..8];
        var admin = server.ConnectionStringFor("postgres", pooling: false);
        await StoreServer.ExecAsync(admin, $"CREATE DATABASE {name} TEMPLATE template0 ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'");
        try
        {
            var cs = server.ConnectionStringFor(name, pooling: false);
            var ex = await Assert.ThrowsAsync<PostgresMigrationException>(() => RunwickPostgresMigrations.MigrateAsync(cs, Ct));
            Assert.Equal("encoding_unsupported", ex.Code);
            await using var conn = new NpgsqlConnection(cs);
            await conn.OpenAsync(Ct);
            using var cmd = new NpgsqlCommand("SELECT count(*) FROM pg_namespace WHERE nspname = 'runwick'", conn);
            Assert.Equal(0L, (long)(await cmd.ExecuteScalarAsync(Ct))!);
        }
        finally
        {
            await server.DropDatabaseAsync(name);
        }
    }

    [Fact]
    [Trait("Case", "UNS-PG-02")]
    public async Task Staging_on_a_read_only_caller_transaction_is_refused_with_transaction_invalid_and_stages_nothing()
    {
        await using var f = await DriverFixture.StartAsync("uns_pg_" + Guid.NewGuid().ToString("N")[..8]);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var p = c.Prepared.Prepare(new KeyedJob("t", "k"));
        await using (var conn = await OpenBusinessAsync(f))
        await using (var tx = await conn.BeginTransactionAsync(Ct))
        {
            await ExecAsync(conn, tx, "SET TRANSACTION READ ONLY");
            var ex = await Assert.ThrowsAsync<TransactionValidationException>(() => Staging(c).StageAsync(p, tx));
            Assert.Equal("transaction_invalid", ex.Code);
            await tx.RollbackAsync(Ct);
        }

        Assert.Equal(0, await Jobs(f));
        Assert.Equal(DispatchResolutionStatus.NotObserved, (await c.Prepared.ResolveAsync(p.OperationId)).Status);
    }
}
