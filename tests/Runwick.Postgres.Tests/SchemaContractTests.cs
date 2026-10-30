using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Internal;
using Runwick.Harness.Jobs;

namespace Runwick.Postgres.Tests;

/// <summary>PGX-09 (subset): durations, ordering and policy versions.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-005")]
[Trait("Case", "PGX-09")]
public sealed class SchemaContractTests
{
    static async Task<List<T>> QueryAsync<T>(string cs, string sql, Func<NpgsqlDataReader, T> map)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var r = await cmd.ExecuteReaderAsync();
        var list = new List<T>();
        while (await r.ReadAsync())
        {
            list.Add(map(r));
        }

        return list;
    }

    [Fact]
    public async Task Duration_columns_are_bigint_milliseconds()
    {
        await using var f = await DriverFixture.StartAsync("pgx09_dur_" + Guid.NewGuid().ToString("N")[..6]);
        var columns = await QueryAsync(f.AdminConnectionString, """
            SELECT table_name || '.' || column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'runwick' AND column_name LIKE '%\_ms'
            """, r => (Name: r.GetString(0), Type: r.GetString(1)));
        Assert.NotEmpty(columns);
        Assert.All(columns, c => Assert.Equal("bigint", c.Type));

        var intervals = await QueryAsync(f.AdminConnectionString,
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'runwick' AND data_type = 'interval'", r => r.GetString(0));
        Assert.Empty(intervals);
    }

    [Fact]
    public async Task Text_identities_sort_bytewise_and_uuids_sort_big_endian()
    {
        await using var f = await DriverFixture.StartAsync("pgx09_order_" + Guid.NewGuid().ToString("N")[..6]);
        var collations = await QueryAsync(f.AdminConnectionString, """
            SELECT t.typname, coalesce(c.collname, '') FROM pg_type AS t
            JOIN pg_namespace AS n ON n.oid = t.typnamespace
            LEFT JOIN pg_collation AS c ON c.oid = t.typcollation
            WHERE n.nspname = 'runwick' AND t.typname IN ('rw_identifier', 'rw_job_name', 'rw_key')
            """, r => (Type: r.GetString(0), Collation: r.GetString(1)));
        Assert.Equal(3, collations.Count);
        Assert.All(collations, c => Assert.Equal("C", c.Collation));

        string[] keys = ["b", "a", "Z", "é", "é", "ä", "😀", "~", "aa", "A"];
        var values = string.Join(",", keys.Select(k => "('" + k + "'::runwick.rw_key)"));
        var dbOrder = await QueryAsync(f.AdminConnectionString, $"SELECT k FROM (VALUES {values}) AS v(k) ORDER BY k", r => r.GetString(0));
        Assert.Equal(keys.Order(Utf8Order.Instance), dbOrder);

        var uuids = Enumerable.Range(0, 200).Select(_ => Guid.NewGuid()).ToList();
        var literal = string.Join(",", uuids.Select(u => $"('{u:D}'::uuid)"));
        var dbUuids = await QueryAsync(f.AdminConnectionString, $"SELECT u FROM (VALUES {literal}) AS v(u) ORDER BY u", r => r.GetGuid(0));
        Assert.Equal(uuids.Order(UuidOrder.Instance), dbUuids);
    }

    [Fact]
    public async Task Generation_rows_record_evaluator_version_and_jitter_kind()
    {
        await using var f = await DriverFixture.StartAsync("pgx09_policy_" + Guid.NewGuid().ToString("N")[..6]);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r, maxExecutions: 4));
        await c.Dispatcher.DispatchAsync(new PingJob("p"));
        var rows = await QueryAsync(f.AdminConnectionString, """
            SELECT max_executions, retry_policy::text, unstarted_retry_policy::text, retry_policy_version, unstarted_retry_policy_version,
                   policy_snapshot_version
            FROM runwick.job_generations
            """, r => (Max: r.GetInt32(0), Retry: r.GetString(1), Unstarted: r.GetString(2), V1: r.GetInt32(3), V2: r.GetInt32(4), V3: r.GetInt32(5)));
        var row = Assert.Single(rows);
        Assert.Equal(4, row.Max);
        Assert.Equal((1, 1, 1), (row.V1, row.V2, row.V3));
        foreach (var json in new[] { row.Retry, row.Unstarted })
        {
            var o = JsonNode.Parse(json)!.AsObject();
            Assert.Equal("exponential", o["backoff"]!.GetValue<string>());
            Assert.Equal(1, o["evaluatorVersion"]!.GetValue<int>());
            Assert.Equal("equal", o["jitterKind"]!.GetValue<string>());
        }

        var retry = JsonNode.Parse(row.Retry)!;
        Assert.Equal(100, retry["initialDelay"]!.GetValue<long>());
        Assert.Equal(1000, retry["maximumDelay"]!.GetValue<long>());
    }
}
