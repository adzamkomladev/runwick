using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Runwick.Workloads;

/// <summary>
/// <c>writefloor</c>: the cheapest durable write each store accepts on this disk, with no Runwick code in the path. One 1 KiB insert (PostgreSQL,
/// SQLite) or XADD (Redis) per call, acknowledged only after the store's own durability point, from <c>--callers</c> concurrent callers. The floor a producer
/// cost budget is measured against (docs/plans/queue-throughput-stage-1-plan.md). The stores are the harness's: PostgreSQL 18 (full durability),
/// Redis with AOF appendfsync always, SQLite with WAL + synchronous=FULL (Runwick's mandatory profile).
/// Usage: <c>writefloor --store postgres|redis|sqlite [--callers 1] [--n 2000] [--conn admin-cs] [--redis host:port] [--out file.json]</c>.
/// </summary>
internal static class WriteFloorMode
{
    public static async Task<int> RunAsync(Args a)
    {
        var store = a.OptStr("store") ?? "postgres";
        var callers = a.Int("callers", 1);
        var n = a.Int("n", 2000);
        var payload = new byte[1024];
        Array.Fill(payload, (byte)'x');
        var latencies = new double[n];
        var next = -1;
        Func<Task> write;
        var settings = new JsonObject { ["store"] = store, ["payloadBytes"] = payload.Length, ["callers"] = callers, ["n"] = n };
        string? sqliteDir = null;

        switch (store)
        {
            case "postgres":
            {
                var admin = a.OptStr("conn") ?? "Host=127.0.0.1;Port=55499;Username=postgres;Password=pg;Database=postgres";
                await using (var c = new NpgsqlConnection(admin))
                {
                    await c.OpenAsync().ConfigureAwait(false);
                    foreach (var sql in new[] { "DROP DATABASE IF EXISTS bench_writefloor WITH (FORCE)", "CREATE DATABASE bench_writefloor" })
                    {
                        await new NpgsqlCommand(sql, c).ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }

                var cs = new NpgsqlConnectionStringBuilder(admin) { Database = "bench_writefloor", MaxPoolSize = callers + 2, MinPoolSize = callers }.ConnectionString;
                await using (var c = new NpgsqlConnection(cs))
                {
                    await c.OpenAsync().ConfigureAwait(false);
                    await new NpgsqlCommand("CREATE TABLE t (id bigserial PRIMARY KEY, payload bytea NOT NULL)", c).ExecuteNonQueryAsync().ConfigureAwait(false);
                    settings["synchronous_commit"] = (string?)await new NpgsqlCommand("SHOW synchronous_commit", c).ExecuteScalarAsync().ConfigureAwait(false);
                    settings["fsync"] = (string?)await new NpgsqlCommand("SHOW fsync", c).ExecuteScalarAsync().ConfigureAwait(false);
                }

                write = async () =>
                {
                    await using var c = new NpgsqlConnection(cs);
                    await c.OpenAsync().ConfigureAwait(false);
                    await using var cmd = new NpgsqlCommand("INSERT INTO t (payload) VALUES ($1)", c);
                    cmd.Parameters.AddWithValue(payload);
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                };
                break;
            }

            case "redis":
            {
                var mux = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync((a.OptStr("redis") ?? "127.0.0.1:56392") + ",allowAdmin=true").ConfigureAwait(false);
                var server = mux.GetServers()[0];
                await server.FlushAllDatabasesAsync().ConfigureAwait(false);
                settings["appendfsync"] = (await server.ConfigGetAsync("appendfsync").ConfigureAwait(false)).FirstOrDefault().Value;
                settings["appendonly"] = (await server.ConfigGetAsync("appendonly").ConfigureAwait(false)).FirstOrDefault().Value;
                var db = mux.GetDatabase();
                write = async () => await db.StreamAddAsync("floor", "p", payload).ConfigureAwait(false);
                break;
            }

            case "sqlite":
            {
                sqliteDir = Path.Combine(Path.GetTempPath(), "writefloor-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(sqliteDir);
                var cs = new SqliteConnectionStringBuilder { DataSource = Path.Combine(sqliteDir, "f.db"), Pooling = false }.ConnectionString;
                using (var c = new SqliteConnection(cs))
                {
                    c.Open();
                    using var cmd = c.CreateCommand();
                    cmd.CommandText = "PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)";
                    cmd.ExecuteNonQuery();
                }

                // One connection per caller; the single SQLite writer serializes them, which is the point of the 16-caller row.
                var connections = new System.Collections.Concurrent.ConcurrentBag<SqliteConnection>();
                for (var i = 0; i < callers; i++)
                {
                    var c = new SqliteConnection(cs);
                    c.Open();
                    using var pragma = c.CreateCommand();
                    pragma.CommandText = "PRAGMA synchronous=FULL; PRAGMA busy_timeout=60000";
                    pragma.ExecuteNonQuery();
                    connections.Add(c);
                }

                using (var c = new SqliteConnection(cs))
                {
                    c.Open();
                    using var q = c.CreateCommand();
                    q.CommandText = "PRAGMA journal_mode";
                    settings["journal_mode"] = (string?)q.ExecuteScalar();
                }

                settings["synchronous"] = "FULL";
                write = () =>
                {
                    connections.TryTake(out var c);
                    try
                    {
                        using var cmd = c!.CreateCommand();
                        cmd.CommandText = "INSERT INTO t (payload) VALUES ($p)";
                        cmd.Parameters.AddWithValue("$p", payload);
                        cmd.ExecuteNonQuery();
                    }
                    finally
                    {
                        connections.Add(c!);
                    }

                    return Task.CompletedTask;
                };
                break;
            }

            default:
                Console.Error.WriteLine("--store must be postgres, redis or sqlite");
                return 2;
        }

        for (var i = 0; i < Math.Min(100, n); i++)
        {
            await write().ConfigureAwait(false); // warm the connection and the path
        }

        Console.Error.WriteLine($"@@PHASE timed-start {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");
        var clock = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, callers).Select(_ => Task.Run(async () =>
        {
            for (var i = Interlocked.Increment(ref next); i < n; i = Interlocked.Increment(ref next))
            {
                var started = Stopwatch.GetTimestamp();
                await write().ConfigureAwait(false);
                latencies[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds * 1000;
            }
        }))).ConfigureAwait(false);
        var seconds = clock.Elapsed.TotalSeconds;
        Console.Error.WriteLine($"@@PHASE timed-end {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");
        Array.Sort(latencies);
        double At(double q) => Math.Round(latencies[Math.Clamp((int)Math.Ceiling(q * n) - 1, 0, n - 1)], 1);
        var result = new JsonObject
        {
            ["system"] = "writefloor-" + store, ["mode"] = "writefloor", ["jobs"] = n, ["writesPerSec"] = Math.Round(n / seconds, 1),
            // Per-write wall time at saturation of one caller is the budget that one synchronous caller has; with more callers it is what each call waits.
            ["meanMicrosPerWrite"] = Math.Round(seconds * 1_000_000 / n, 1),
            ["latencyMicros"] = new JsonObject { ["p50"] = At(0.5), ["p95"] = At(0.95), ["p99"] = At(0.99) },
            ["settings"] = settings,
        };
        var json = result.ToJsonString(Json.Indented);
        Console.WriteLine(json);
        if (a.OptStr("out") is { } file)
        {
            await File.WriteAllTextAsync(file, json).ConfigureAwait(false);
        }

        if (sqliteDir is not null)
        {
            SqliteConnection.ClearAllPools();
            try { Directory.Delete(sqliteDir, recursive: true); } catch (IOException) { }
        }

        return 0;
    }
}
