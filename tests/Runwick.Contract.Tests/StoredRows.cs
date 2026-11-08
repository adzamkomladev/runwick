using System.Globalization;
using Microsoft.Data.Sqlite;
using Runwick.Harness.Store;

namespace Runwick.Contract.Tests;

/// <summary>
/// Raw reads and setup writes of one job's stored payload and serializer profile on every store (SER-06/07). Setup only: the
/// cases use it to read what the store kept and to plant a foreign profile or malformed bytes, never to fake a protocol effect.
/// </summary>
internal static class StoredRows
{
    static byte[] Blob(Guid g) => g.ToByteArray(bigEndian: true);

    static SqliteConnection Sqlite(DriverFixture f)
    {
        var conn = new SqliteConnection(f.AdminConnectionString);
        conn.Open();
        return conn;
    }

    static async Task<StackExchange.Redis.IDatabase> Redis() => (await RedisServer.AdminAsync()).GetDatabase();

    public static async Task<byte[]> PayloadAsync(DriverFixture f, Guid job)
    {
        switch (f.Kind)
        {
            case StoreKind.Redis:
                return (byte[])(await (await Redis()).StringGetAsync(RedisServer.Key(f.Application, "pay:" + job.ToString("N"))))!;
            case StoreKind.Sqlite:
                using (var conn = Sqlite(f))
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "SELECT payload_utf8 FROM runwick_job_payloads WHERE job_id = @j";
                    cmd.Parameters.AddWithValue("@j", Blob(job));
                    return (byte[])cmd.ExecuteScalar()!;
                }

            default:
                await using (var conn = new Npgsql.NpgsqlConnection(f.AdminConnectionString))
                {
                    await conn.OpenAsync();
                    using var cmd = new Npgsql.NpgsqlCommand("SELECT payload_utf8 FROM runwick.job_payloads WHERE job_id = @j", conn);
                    cmd.Parameters.AddWithValue("j", job);
                    return (byte[])(await cmd.ExecuteScalarAsync())!;
                }
        }
    }

    /// <summary>Whether the job row and its payload row both carry this profile (PostgreSQL and SQLite keep it in both; Redis in the job hash).</summary>
    public static async Task<bool> ProfileIsAsync(DriverFixture f, Guid job, string id, int version)
    {
        if (f.Kind == StoreKind.Redis)
        {
            var h = await RedisAdmin.Hash(f.Application, "job:" + job.ToString("N"));
            return h["profile"] == id && h["profver"] == version.ToString(CultureInfo.InvariantCulture);
        }

        var t = f.Kind == StoreKind.Sqlite ? "runwick_" : "runwick.";
        var sql = $"SELECT count(*) FROM {t}jobs AS j JOIN {t}job_payloads AS p USING (application, job_id) WHERE j.job_id = @j " +
                  "AND j.serializer_profile_id = @id AND p.serializer_profile_id = @id AND j.serializer_profile_version = @v AND p.serializer_profile_version = @v";
        return await f.CountAsync(sql, ("j", f.Kind == StoreKind.Sqlite ? Blob(job) : job), ("id", id), ("v", version)) == 1;
    }

    public static async Task SetProfileAsync(DriverFixture f, Guid job, string id)
    {
        switch (f.Kind)
        {
            case StoreKind.Redis:
                await (await Redis()).HashSetAsync(RedisServer.Key(f.Application, "job:" + job.ToString("N")), "profile", id);
                break;
            case StoreKind.Sqlite:
                using (var conn = Sqlite(f))
                {
                    using var cmd = conn.CreateCommand();
                    // The profile reference is a composite foreign key: switch enforcement off so the two rows can change one after the other.
                    cmd.CommandText = "PRAGMA foreign_keys = OFF; UPDATE runwick_job_payloads SET serializer_profile_id = @id WHERE job_id = @j; UPDATE runwick_jobs SET serializer_profile_id = @id WHERE job_id = @j";
                    cmd.Parameters.AddWithValue("@id", id);
                    cmd.Parameters.AddWithValue("@j", Blob(job));
                    cmd.ExecuteNonQuery();
                }

                break;
            default:
                await f.ExecAsync("""
                    WITH p AS (UPDATE runwick.job_payloads SET serializer_profile_id = @id WHERE job_id = @j RETURNING job_id)
                    UPDATE runwick.jobs SET serializer_profile_id = @id WHERE job_id = (SELECT job_id FROM p)
                    """, ("j", job), ("id", id));
                break;
        }
    }

    public static async Task SetPayloadAsync(DriverFixture f, Guid job, byte[] bytes)
    {
        switch (f.Kind)
        {
            case StoreKind.Redis:
                await (await Redis()).StringSetAsync(RedisServer.Key(f.Application, "pay:" + job.ToString("N")), bytes);
                break;
            case StoreKind.Sqlite:
                using (var conn = Sqlite(f))
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "UPDATE runwick_job_payloads SET payload_utf8 = @b WHERE job_id = @j";
                    cmd.Parameters.AddWithValue("@b", bytes);
                    cmd.Parameters.AddWithValue("@j", Blob(job));
                    cmd.ExecuteNonQuery();
                }

                break;
            default:
                await f.ExecAsync("UPDATE runwick.job_payloads SET payload_utf8 = @b WHERE job_id = @j", ("j", job), ("b", bytes));
                break;
        }
    }
}
