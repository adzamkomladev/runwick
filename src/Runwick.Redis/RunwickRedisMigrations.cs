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
    public static class RunwickRedisMigrations
    {
        /// <summary>Writes <c>rw:store</c> once, or does nothing when this layout is already installed (22 §9.2).</summary>
        public static Task<RedisMigrationResult> MigrateAsync(string configuration, CancellationToken cancellationToken = default) =>
            Runwick.Redis.Migrations.RedisMigrator.RunAsync(configuration, cancellationToken);
    }
}

namespace Runwick.Redis.Migrations
{
    internal static class RedisMigrator
    {
        public const long CurrentLayout = 1;
        public const string StoreKey = "rw:store";
        const string LockKey = "rw:migration-lock";
        const string ManifestDomain = "runwick.manifest.v1";

        /// <summary>Test seam (REDIS-14): how long to wait for the migration lock.</summary>
        internal static TimeSpan LockWait { get; set; } = TimeSpan.FromSeconds(60);

        public static async Task<RedisMigrationResult> RunAsync(string configuration, CancellationToken ct)
        {
            ConfigurationOptions options;
            try
            {
                options = RxClient.Options(configuration, TimeSpan.FromSeconds(5));
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
                var server = mux.GetServer(mux.GetEndPoints().Single());
                RxProfileReport report;
                try
                {
                    report = await RxProfile.CheckAsync(server, db).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is RedisException)
                {
                    throw new RedisMigrationException("store_unavailable", "Could not read the Redis profile.", ex);
                }

                if (!report.Verified)
                {
                    throw new RedisMigrationException(report.FailureCode!, "The Redis server is outside the profile: " + report.Failure);
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
                    return await ApplyAsync(db, server, ct).ConfigureAwait(false);
                }
                finally
                {
                    await db.ScriptEvaluateAsync("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
                        [LockKey], [token]).ConfigureAwait(false);
                }
            }
        }

        static async Task<RedisMigrationResult> ApplyAsync(IDatabase db, IServer server, CancellationToken ct)
        {
            var (manifest, digest) = Manifest();
            var existing = await db.HashGetAllAsync(StoreKey).ConfigureAwait(false);
            if (existing.Length > 0)
            {
                var map = existing.ToDictionary(e => (string)e.Name!, e => (string)e.Value!, StringComparer.Ordinal);
                if (map.GetValueOrDefault("layout") != CurrentLayout.ToString(System.Globalization.CultureInfo.InvariantCulture)
                    || map.GetValueOrDefault("digest") != Convert.ToHexStringLower(digest))
                {
                    throw new RedisMigrationException("schema_validation_failed", "rw:store holds a different layout revision or manifest.");
                }

                return new RedisMigrationResult(Guid.Parse(map["store"]), CurrentLayout, []);
            }

            await foreach (var key in server.KeysAsync(db.Database, "rw:*", 1000).WithCancellation(ct).ConfigureAwait(false))
            {
                if (key != "rw:migration-lock")
                {
                    throw new RedisMigrationException("schema_not_empty", "The Redis database has rw:* keys but no rw:store.");
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

        internal static (JsonObject Json, byte[] Digest) Manifest()
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
                ["layoutRevision"] = CurrentLayout,
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
                .Add("layoutRevision", CurrentLayout)
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
