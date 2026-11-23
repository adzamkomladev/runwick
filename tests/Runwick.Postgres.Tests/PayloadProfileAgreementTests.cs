using Npgsql;
using Runwick.Harness.Store;
using Runwick.Postgres.Migrations;

namespace Runwick.Postgres.Tests;

/// <summary>
/// 0028 removed <c>jobs_profile_uq</c> and the composite payload foreign key; triggers keep the payload's serializer profile
/// equal to its job's. Each case runs in a transaction that is rolled back, so the deferred job foreign keys never fire.
/// </summary>
[Trait("Category", "Fault")]
[Trait("Item", "PG-INDEX")]
public sealed class PayloadProfileAgreementTests
{
    const string Job1 = "00000000-0000-0000-0000-000000000001";
    const string Job2 = "00000000-0000-0000-0000-000000000002";

    static string InsertJob(string id, int version) => $"""
        INSERT INTO runwick.jobs (application, job_id, job_name, payload_version, serializer_profile_id, serializer_profile_version,
                                  queue, priority, state, eligible_at, created_at, updated_at)
        VALUES ('profile-app', '{id}', 'test-job', 1, 'json', {version}, 'default', 'medium', 'pending', date_trunc('second', now()), now(), now())
        """;

    static string InsertPayload(string id, int version) => $"""
        INSERT INTO runwick.job_payloads (application, job_id, payload_utf8, serializer_profile_id, serializer_profile_version)
        VALUES ('profile-app', '{id}', '\x7b7d', 'json', {version})
        """;

    static async Task<PostgresException?> TryAsync(NpgsqlConnection conn, params string[] statements)
    {
        await using var tx = await conn.BeginTransactionAsync(TestContext.Current.CancellationToken);
        try
        {
            foreach (var sql in statements)
            {
                await using var cmd = new NpgsqlCommand(sql, conn, tx);
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            return null;
        }
        catch (PostgresException ex)
        {
            return ex;
        }
        finally
        {
            await tx.RollbackAsync(CancellationToken.None);
        }
    }

    static void AssertProfileViolation(PostgresException? ex)
    {
        Assert.NotNull(ex);
        Assert.Equal(PostgresErrorCodes.ForeignKeyViolation, ex.SqlState);
        Assert.Equal("job_payloads_job_profile_fk", ex.ConstraintName);
    }

    [Fact]
    public async Task Payload_profile_must_match_its_job_without_the_composite_key()
    {
        var server = await StoreServer.SharedAsync();
        var db = await server.CreateEmptyDatabaseAsync("pg_profile_" + Guid.NewGuid().ToString("N")[..8]);
        try
        {
            var cs = server.ConnectionStringFor(db, pooling: false);
            await RunwickPostgresMigrations.MigrateAsync(cs, TestContext.Current.CancellationToken);
            await using var conn = new NpgsqlConnection(cs);
            await conn.OpenAsync(TestContext.Current.CancellationToken);

            await using (var c = new NpgsqlCommand(
                "SELECT count(*) FROM pg_constraint WHERE conname IN ('jobs_profile_uq', 'job_payloads_job_profile_fk')", conn))
            {
                Assert.Equal(0L, (long)(await c.ExecuteScalarAsync(TestContext.Current.CancellationToken))!);
            }

            // Matching profile: accepted, single and set inserts.
            Assert.Null(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1)));
            Assert.Null(await TryAsync(conn, InsertJob(Job1, 1), InsertJob(Job2, 1), $"""
                INSERT INTO runwick.job_payloads (application, job_id, payload_utf8, serializer_profile_id, serializer_profile_version)
                VALUES ('profile-app', '{Job1}', '\x7b7d', 'json', 1), ('profile-app', '{Job2}', '\x7b7d', 'json', 1)
                """));

            // A different profile, alone or as one row of a set insert: refused.
            AssertProfileViolation(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 2)));
            AssertProfileViolation(await TryAsync(conn, InsertJob(Job1, 1), InsertJob(Job2, 1), $"""
                INSERT INTO runwick.job_payloads (application, job_id, payload_utf8, serializer_profile_id, serializer_profile_version)
                VALUES ('profile-app', '{Job1}', '\x7b7d', 'json', 1), ('profile-app', '{Job2}', '\x7b7d', 'other', 1)
                """));

            // Changing a stored payload's profile: refused.
            AssertProfileViolation(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1),
                $"UPDATE runwick.job_payloads SET serializer_profile_version = 2 WHERE job_id = '{Job1}'"));

            // Changing the profile of a job that has a payload: refused. Same value, or a job without a payload: accepted.
            AssertProfileViolation(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1),
                $"UPDATE runwick.jobs SET serializer_profile_id = 'other' WHERE job_id = '{Job1}'"));
            Assert.Null(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1),
                $"UPDATE runwick.jobs SET serializer_profile_version = 1 WHERE job_id = '{Job1}'"));
            Assert.Null(await TryAsync(conn, InsertJob(Job2, 1),
                $"UPDATE runwick.jobs SET serializer_profile_version = 2 WHERE job_id = '{Job2}'"));

            // As with the old key (ON UPDATE NO ACTION), agreement is checked at the end of the statement: one statement that
            // moves a payload and its job to a new profile together is accepted, in either order; two statements are not.
            Assert.Null(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1), $"""
                WITH p AS (UPDATE runwick.job_payloads SET serializer_profile_id = 'other' WHERE job_id = '{Job1}' RETURNING job_id)
                UPDATE runwick.jobs SET serializer_profile_id = 'other' WHERE job_id = (SELECT job_id FROM p)
                """));
            Assert.Null(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1), $"""
                WITH j AS (UPDATE runwick.jobs SET serializer_profile_version = 2 WHERE job_id = '{Job1}' RETURNING job_id)
                UPDATE runwick.job_payloads SET serializer_profile_version = 2 WHERE job_id = (SELECT job_id FROM j)
                """));
            AssertProfileViolation(await TryAsync(conn, InsertJob(Job1, 1), InsertPayload(Job1, 1),
                $"UPDATE runwick.jobs SET serializer_profile_version = 2 WHERE job_id = '{Job1}'",
                $"UPDATE runwick.job_payloads SET serializer_profile_version = 2 WHERE job_id = '{Job1}'"));

            // A payload without a job is still refused (now by the primary-key foreign key).
            var orphan = await TryAsync(conn, InsertPayload(Job1, 1));
            Assert.NotNull(orphan);
            Assert.Equal(PostgresErrorCodes.ForeignKeyViolation, orphan.SqlState);
        }
        finally
        {
            await server.DropDatabaseAsync(db);
        }
    }
}
