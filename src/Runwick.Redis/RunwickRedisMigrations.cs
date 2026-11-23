using System.Text.Json.Nodes;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Serialization;
using StackExchange.Redis;

namespace Runwick
{
    /// <summary>Outcome of one Redis migration run (22 §9.1).</summary>
    public sealed record RedisMigrationResult(Guid StoreId, long LayoutRevision, IReadOnlyList<string> AppliedPhases);

    /// <summary>A migration step failed; <see cref="Code"/> is one of the 22 §9.1 codes.</summary>
    public sealed class RedisMigrationException(string code, string message, Exception? innerException = null) : Exception(message, innerException)
    {
        public string Code { get; } = code;
    }

    /// <summary>The explicit deployment migrator for the Redis store (22 §9). Workers never migrate on startup.</summary>
    public static partial class RunwickRedisMigrations
    {
        /// <summary>Writes <c>rw:store</c> once, or does nothing when this layout is already installed (22 §9.2).</summary>
        public static Task<RedisMigrationResult> MigrateAsync(string configuration, CancellationToken cancellationToken = default) =>
            Runwick.Redis.Migrations.RedisMigrator.RunAsync(configuration, cancellationToken);

        /// <summary>
        /// As <see cref="MigrateAsync(string, CancellationToken)"/> for a Redis Cluster (22 §2.6): <paramref name="configuration"/> lists the
        /// cluster's nodes, every primary is checked against the cluster profile, and <c>rw:store</c> is written once.
        /// </summary>
        public static Task<RedisMigrationResult> MigrateClusterAsync(string configuration, CancellationToken cancellationToken = default) =>
            Runwick.Redis.Migrations.RedisMigrator.RunAsync(configuration, cancellationToken, cluster: true);

        /// <summary>As above, checking the server against the given persistence profile (G4).</summary>
        public static Task<RedisMigrationResult> MigrateAsync(string configuration, RedisPersistence persistence, CancellationToken cancellationToken = default) =>
            Runwick.Redis.Migrations.RedisMigrator.RunAsync(configuration, cancellationToken, persistence);
    }
}

namespace Runwick.Redis.Migrations
{
    internal static class RedisMigrator
    {
        public const long CurrentLayout = 2;
        public const string StoreKey = "rw:store";
        const string LockKey = "rw:migration-lock";
        const string ManifestDomain = "runwick.manifest.v1";

        /// <summary>Test seam (REDIS-14): how long to wait for the migration lock.</summary>
        internal static TimeSpan LockWait { get; set; } = TimeSpan.FromSeconds(60);

        public static Task<RedisMigrationResult> RunAsync(string configuration, CancellationToken ct, RedisPersistence persistence = RedisPersistence.AofAlways,
            bool cluster = false) =>
            WithLockAsync(configuration, (db, server) => ApplyAsync(db, server, cluster, ct), ct, persistence, cluster);

        /// <summary>Connects as admin, checks the profile and runs <paramref name="body"/> under the migration lock (22 §9.2).</summary>
        internal static async Task<T> WithLockAsync<T>(string configuration, Func<IDatabase, IServer, Task<T>> body, CancellationToken ct,
            RedisPersistence persistence = RedisPersistence.AofAlways, bool cluster = false)
        {
            ConfigurationOptions options;
            try
            {
                options = cluster ? RxClient.ClusterOptions(configuration, TimeSpan.FromSeconds(5)).Data : RxClient.Options(configuration, TimeSpan.FromSeconds(5));
            }
            catch (RxProfileException ex)
            {
                throw new RedisMigrationException(ex.Code, ex.Message);
            }

            options.AllowAdmin = true;
            options.ClientName = "runwick:migration";
            ConnectionMultiplexer mux;
            try
            {
                mux = await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
            }
            catch (RedisConnectionException ex)
            {
                throw new RedisMigrationException("store_unavailable", "Could not connect to Redis.", ex);
            }

            await using (mux.ConfigureAwait(false))
            {
                if (!mux.IsConnected)
                {
                    throw new RedisMigrationException("store_unavailable", "Could not connect to Redis.");
                }

                var db = mux.GetDatabase();
                var server = cluster ? PrimariesOf(mux).First() : mux.GetServer(mux.GetEndPoints(configuredOnly: true).Single());
                foreach (var node in cluster ? PrimariesOf(mux) : [server]) // a cluster is checked primary by primary
                {
                    RxProfileReport report;
                    try
                    {
                        report = await RxProfile.CheckAsync(node, db, persistence, cluster ? RxTopology.Cluster : RxTopology.Standalone).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is RedisException)
                    {
                        throw new RedisMigrationException("store_unavailable", "Could not read the Redis profile.", ex);
                    }

                    if (!report.Verified)
                    {
                        throw new RedisMigrationException(report.FailureCode!, "The Redis server is outside the profile: " + report.Failure);
                    }
                }

                var token = Guid.NewGuid().ToString("N");
                var until = DateTimeOffset.UtcNow + LockWait;
                while (!await db.StringSetAsync(LockKey, token, TimeSpan.FromSeconds(60), When.NotExists).ConfigureAwait(false))
                {
                    if (DateTimeOffset.UtcNow >= until)
                    {
                        throw new RedisMigrationException("migration_lock_timeout", "The Redis migration lock was not granted in time.");
                    }

                    await Task.Delay(100, ct).ConfigureAwait(false);
                }

                try
                {
                    return await body(db, server).ConfigureAwait(false);
                }
                finally
                {
                    await db.ScriptEvaluateAsync("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
                        [LockKey], [token]).ConfigureAwait(false);
                }
            }
        }

        static IServer[] PrimariesOf(IConnectionMultiplexer mux)
        {
            var primaries = mux.GetServers().Where(n => !n.IsReplica && n.IsConnected).ToArray();
            return primaries.Length > 0 ? primaries : throw new RedisMigrationException("store_unavailable", "No connected cluster primary.");
        }

        static async Task<RedisMigrationResult> ApplyAsync(IDatabase db, IServer server, bool cluster, CancellationToken ct)
        {
            var (manifest, digest) = Manifest();
            var existing = await db.HashGetAllAsync(StoreKey).ConfigureAwait(false);
            if (existing.Length > 0)
            {
                var map = existing.ToDictionary(e => (string)e.Name!, e => (string)e.Value!, StringComparer.Ordinal);
                if (long.TryParse(map.GetValueOrDefault("layout"), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var layout) && layout > CurrentLayout)
                {
                    throw new RedisMigrationException("migration_unknown", $"rw:store holds layout revision {layout}, which this binary does not know (newer store).");
                }

                if (map.GetValueOrDefault("layout") == "1" && map.GetValueOrDefault("digest") == Convert.ToHexStringLower(Manifest(1).Digest))
                {
                    return await UpgradeFromLayout1Async(db, server, cluster, map, manifest, digest, ct).ConfigureAwait(false);
                }

                if (map.GetValueOrDefault("layout") != CurrentLayout.ToString(System.Globalization.CultureInfo.InvariantCulture)
                    || map.GetValueOrDefault("digest") != Convert.ToHexStringLower(digest))
                {
                    throw new RedisMigrationException("schema_validation_failed", "rw:store holds a different layout revision or manifest.");
                }

                return new RedisMigrationResult(Guid.Parse(map["store"]), CurrentLayout, []);
            }

            foreach (var node in cluster ? PrimariesOf(db.Multiplexer) : [server])
            {
                await foreach (var key in node.KeysAsync(db.Database, "rw:*", 1000).WithCancellation(ct).ConfigureAwait(false))
                {
                    if (key != "rw:migration-lock")
                    {
                        throw new RedisMigrationException("schema_not_empty", "The Redis database has rw:* keys but no rw:store.");
                    }
                }
            }

            var storeId = Guid.NewGuid();
            await db.HashSetAsync(StoreKey,
            [
                new("store", RxClient.Hex(storeId)), new("driver", "redis"), new("layout", CurrentLayout), new("epoch", 1), new("mode", "normal"),
                new("mversion", 1), new("manifest", manifest.ToJsonString()), new("digest", Convert.ToHexStringLower(digest)),
                new("created", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
            ]).ConfigureAwait(false);
            return new RedisMigrationResult(storeId, CurrentLayout, ["expand", "validate", "activate"]);
        }

        static readonly LuaScript SetLayout = LuaScript.Prepare(
            "local l = redis.call('HGET', @meta, 'layout') if l == @to then return 'DONE' end " +
            "if l ~= @from then return 'OTHER' end " +
            "local i = redis.call('HGET', @meta, 'intent') if i and i ~= '' then return 'PENDING' end " +
            "redis.call('HSET', @meta, 'layout', @to) return 'OK'");

        /// <summary>
        /// Layout revision 1 to 2 (22 §9.2 step 5, 23 §4): each application's meta copy first, one script each, refused with
        /// <c>intent_pending</c> while that application has a pending intent; then rw:store. A run stopped partway is run again:
        /// rw:store still says 1 until every application is done, so no binary runs on the half-changed store (a layout-2 binary
        /// refuses rw:store, a layout-1 binary the changed meta copies). No data is rewritten: layout-2 readers read both forms.
        /// </summary>
        static async Task<RedisMigrationResult> UpgradeFromLayout1Async(IDatabase db, IServer server, bool cluster, Dictionary<string, string> store,
            JsonObject manifest, byte[] digest, CancellationToken ct)
        {
            foreach (var node in cluster ? PrimariesOf(db.Multiplexer) : [server])
            {
                foreach (var (name, meta) in await RedisOps.ApplicationsAsync(node, db, null, ct).ConfigureAwait(false))
                {
                    var result = (string?)await db.ScriptEvaluateAsync(SetLayout, new { meta, from = "1", to = "2" }).ConfigureAwait(false);
                    if (result == "PENDING")
                    {
                        throw new RedisMigrationException("intent_pending", $"Application {name} has a pending intent; let recovery finish and retry.");
                    }

                    if (result == "OTHER")
                    {
                        throw new RedisMigrationException("schema_validation_failed", $"Application {name} holds a layout revision other than 1 or 2.");
                    }
                }
            }

            await db.HashSetAsync(StoreKey,
            [
                new("layout", CurrentLayout), new("manifest", manifest.ToJsonString()), new("digest", Convert.ToHexStringLower(digest)),
            ]).ConfigureAwait(false);
            return new RedisMigrationResult(Guid.Parse(store["store"]), CurrentLayout, ["layout-2"]);
        }

        // ------------------------------------------------------------------ manifest (22 §9.3)

        /// <summary>The PostgreSQL revision-17 operation set and versions (as SQLite's).</summary>
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

        internal static (JsonObject Json, byte[] Digest) Manifest(long layout = CurrentLayout)
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

            var operations = new JsonObject();
            var canonicalOps = CanonicalValue.Object();
            foreach (var (op, versions) in map)
            {
                operations[op] = new JsonArray([.. versions.Select(v => (JsonNode?)v)]);
                canonicalOps.Add(op, CanonicalValue.Array(versions.Select(v => CanonicalValue.Int(v))));
            }

            var json = new JsonObject
            {
                ["manifestVersion"] = 1,
                ["layoutRevision"] = layout,
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
                .Add("layoutRevision", layout)
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
