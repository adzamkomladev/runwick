using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick.Harness.Store;

/// <summary>
/// The shared Redis server of the Redis driver-suite rows (22 §11): RUNWICK_TEST_REDIS, else the local AOF-always container on
/// 127.0.0.1:56379. Migrated once per process; each fixture uses its own application namespace and removes it afterwards.
/// </summary>
internal static class RedisServer
{
    /// <summary>The harness connection-string form of a Redis store: this prefix, then the StackExchange.Redis configuration.</summary>
    public const string Prefix = "redis|";

    public static string Configuration { get; } = Environment.GetEnvironmentVariable("RUNWICK_TEST_REDIS") is { Length: > 0 } c ? c : "127.0.0.1:56379";

    static readonly Lazy<Task> Migrated = new(() => RunwickRedisMigrations.MigrateAsync(Configuration));
    static readonly Lazy<Task<ConnectionMultiplexer>> Admin = new(() => ConnectionMultiplexer.ConnectAsync(Configuration + ",allowAdmin=true"));

    public static string Key(string application, string name) => RxClient.KeyPrefix(application) + name;

    public static bool IsRedis(string connectionString) => connectionString.StartsWith(Prefix, StringComparison.Ordinal);

    public static string ConfigurationOf(string connectionString) => connectionString[Prefix.Length..];

    public static Task EnsureMigratedAsync() => Migrated.Value;

    public static async Task<ConnectionMultiplexer> AdminAsync() => await Admin.Value.ConfigureAwait(false);

    public static async Task<HarnessSnapshot> SnapshotAsync(string application) =>
        await RedisSnapshotReader.ReadAsync(await AdminAsync().ConfigureAwait(false), application).ConfigureAwait(false);

    public static async Task DropApplicationAsync(string application)
    {
        var mux = await AdminAsync().ConfigureAwait(false);
        var db = mux.GetDatabase();
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        var batch = new List<RedisKey>();
        await foreach (var k in server.KeysAsync(db.Database, RxClient.KeyPrefix(application) + "*", 1000).ConfigureAwait(false))
        {
            batch.Add(k);
        }

        foreach (var chunk in batch.Chunk(500))
        {
            await db.KeyDeleteAsync(chunk).ConfigureAwait(false);
        }
    }
}
