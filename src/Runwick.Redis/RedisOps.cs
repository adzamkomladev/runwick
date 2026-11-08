using System.Globalization;
using System.Text;
using Runwick.Redis;
using Runwick.Redis.Migrations;
using Runwick.Redis.Store;
using StackExchange.Redis;

namespace Runwick
{
    /// <summary>Operator admission, status and identity commands of the Redis store (23). Each runs under the migration lock.</summary>
    public static partial class RunwickRedisMigrations
    {
        /// <summary>
        /// Sets one application's (or, with <paramref name="application"/> null, every registered application's) admission mode in
        /// its <c>meta</c> hash with one script per application. An application with a pending intent is refused with
        /// <c>intent_pending</c> (22 §9.2); nothing is changed for the applications listed after it.
        /// </summary>
        public static Task<IReadOnlyList<AdmissionChange>> SetAdmissionModeAsync(
            string configuration, string? application, string mode, string actor, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(actor);
            return RedisMigrator.WithLockAsync<IReadOnlyList<AdmissionChange>>(configuration, async (db, server) =>
            {
                await RedisOps.RequireStoreAsync(db).ConfigureAwait(false);
                var apps = await RedisOps.ApplicationsAsync(server, db, application, cancellationToken).ConfigureAwait(false);
                if (application is not null && apps.Count == 0)
                {
                    throw new RedisMigrationException("application_not_registered", $"Application {application} is not registered.");
                }

                // Check every application first so a refusal changes nothing.
                var plan = new List<(string Name, RedisKey Meta, string Previous, long Revision)>();
                foreach (var (name, meta) in apps)
                {
                    var previous = (string?)await db.HashGetAsync(meta, "admission").ConfigureAwait(false) ?? "open";
                    var revision = long.Parse((string?)await db.HashGetAsync(meta, "gaterev").ConfigureAwait(false) ?? "1", CultureInfo.InvariantCulture);
                    if (previous != mode && StoreAdminRules.Check(previous, mode) is { } code)
                    {
                        throw new RedisMigrationException(code, code == "drain_first"
                            ? $"Application {name} is open: drain it and let work settle before maintenance."
                            : "The mode must be open, drain or maintenance.");
                    }

                    plan.Add((name, meta, previous, revision));
                }

                var changes = new List<AdmissionChange>();
                foreach (var (name, meta, previous, revision) in plan)
                {
                    if (previous == mode)
                    {
                        changes.Add(new AdmissionChange(name, previous, mode, revision, false));
                        continue;
                    }

                    var result = (RedisResult[])(await db.ScriptEvaluateAsync(RedisOps.SetAdmission, new { meta, mode, actor }).ConfigureAwait(false))!;
                    if ((string?)result[0] == "PENDING")
                    {
                        throw new RedisMigrationException("intent_pending", $"Application {name} has a pending intent; let recovery finish and retry.");
                    }

                    changes.Add(new AdmissionChange(name, previous, mode, (long)result[1], true));
                }

                return changes;
            }, cancellationToken);
        }

        /// <summary>
        /// Raises one capability (<c>scheduler</c> or <c>rate_admission</c>) in each application's <c>meta</c> hash with one script per
        /// application (10 §6.1, 23 §3.6). Never lowers one; a repeat reports <c>Changed = false</c>. An application with a pending intent
        /// is refused with <c>intent_pending</c>. Like admission, no audit row or event is written; <paramref name="actor"/> is recorded.
        /// </summary>
        public static Task<IReadOnlyList<CapabilityChange>> ActivateCapabilityAsync(
            string configuration, string? application, string capability, string actor, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(actor);
            if (StoreAdminRules.CheckCapability(capability) is { } bad)
            {
                throw new RedisMigrationException(bad, "The capability must be scheduler or rate_admission.");
            }

            var version = StoreAdminRules.CapabilityVersion(capability);
            return RedisMigrator.WithLockAsync<IReadOnlyList<CapabilityChange>>(configuration, async (db, server) =>
            {
                await RedisOps.RequireStoreAsync(db).ConfigureAwait(false);
                var apps = await RedisOps.ApplicationsAsync(server, db, application, cancellationToken).ConfigureAwait(false);
                if (application is not null && apps.Count == 0)
                {
                    throw new RedisMigrationException("application_not_registered", $"Application {application} is not registered.");
                }

                var changes = new List<CapabilityChange>();
                foreach (var (name, meta) in apps)
                {
                    var result = (RedisResult[])(await db.ScriptEvaluateAsync(RedisOps.ActivateCapability, new { meta, capability, version, actor }).ConfigureAwait(false))!;
                    if ((string?)result[0] == "PENDING")
                    {
                        throw new RedisMigrationException("intent_pending", $"Application {name} has a pending intent; let recovery finish and retry.");
                    }

                    changes.Add(new CapabilityChange(name, capability, version, (long)result[1], (long)result[2] == 1));
                }

                return changes;
            }, cancellationToken);
        }

        /// <summary>Reads the store's layout revision, mode and application gates (23 §6). Redis reports no lease or presence counts.</summary>
        public static Task<StoreAdminStatus> GetStatusAsync(string configuration, CancellationToken cancellationToken = default) =>
            RedisMigrator.WithLockAsync(configuration, async (db, server) =>
            {
                var store = await RedisOps.RequireStoreAsync(db).ConfigureAwait(false);
                var apps = new List<ApplicationAdmissionState>();
                foreach (var (name, meta) in await RedisOps.ApplicationsAsync(server, db, null, cancellationToken).ConfigureAwait(false))
                {
                    apps.Add(new ApplicationAdmissionState(name, (string?)await db.HashGetAsync(meta, "admission").ConfigureAwait(false) ?? "open",
                        long.Parse((string?)await db.HashGetAsync(meta, "gaterev").ConfigureAwait(false) ?? "1", CultureInfo.InvariantCulture),
                        (string?)await db.HashGetAsync(meta, "caps").ConfigureAwait(false) ?? "{}"));
                }

                return new StoreAdminStatus(Guid.ParseExact(store["store"]!, "N"), long.Parse(store["layout"]!, CultureInfo.InvariantCulture),
                    RedisMigrator.CurrentLayout, store["mode"]!, apps, null, null, null, StoreAdminRules.QuiescenceNotice);
            }, cancellationToken);

        /// <summary>
        /// Gives a restored or copied keyspace a new logical store identity (23 §7). Refused unless every application is in
        /// <c>maintenance</c> and none has a pending intent. Every application's copy is rewritten first and <c>rw:store</c> last, so
        /// a failure part-way leaves the applications refusing with <c>store_identity_mismatch</c>; rerun to finish.
        /// </summary>
        public static Task<StoreIdentityChange> ReassignStoreIdAsync(string configuration, CancellationToken cancellationToken = default) =>
            RedisMigrator.WithLockAsync(configuration, async (db, server) =>
            {
                var store = await RedisOps.RequireStoreAsync(db).ConfigureAwait(false);
                var apps = await RedisOps.ApplicationsAsync(server, db, null, cancellationToken).ConfigureAwait(false);
                var open = new List<string>();
                foreach (var (name, meta) in apps)
                {
                    if (((string?)await db.HashGetAsync(meta, "admission").ConfigureAwait(false) ?? "open") != "maintenance")
                    {
                        open.Add(name);
                    }
                }

                if (open.Count > 0)
                {
                    throw new RedisMigrationException("store_not_quiescent", "Put every application in maintenance first; not in maintenance: " + string.Join(", ", open));
                }

                var old = Guid.ParseExact(store["store"]!, "N");
                var fresh = Guid.NewGuid();
                foreach (var (name, meta) in apps)
                {
                    var done = (RedisResult[])(await db.ScriptEvaluateAsync(RedisOps.SetStore, new { meta, store = RxClient.Hex(fresh) }).ConfigureAwait(false))!;
                    if ((string?)done[0] == "PENDING")
                    {
                        throw new RedisMigrationException("intent_pending", $"Application {name} has a pending intent; let recovery finish and rerun.");
                    }
                }

                await db.HashSetAsync(RedisMigrator.StoreKey, "store", RxClient.Hex(fresh)).ConfigureAwait(false);
                return new StoreIdentityChange(old, fresh);
            }, cancellationToken);
    }
}

namespace Runwick.Redis
{
    internal static class RedisOps
    {

        internal static readonly LuaScript SetAdmission = LuaScript.Prepare(
            "local i = redis.call('HGET', @meta, 'intent') if i and i ~= '' then return {'PENDING', 0} end " +
            "local rev = tonumber(redis.call('HGET', @meta, 'gaterev') or '1') + 1 " +
            "redis.call('HSET', @meta, 'admission', @mode, 'gaterev', rev, 'actor', @actor) return {'OK', rev}");

        internal static readonly LuaScript ActivateCapability = LuaScript.Prepare(
            "local i = redis.call('HGET', @meta, 'intent') if i and i ~= '' then return {'PENDING', 0, 0} end " +
            "local rev = tonumber(redis.call('HGET', @meta, 'gaterev') or '1') " +
            "local caps = cjson.decode(redis.call('HGET', @meta, 'caps') or '{}') " +
            "if (caps[@capability] or 0) >= tonumber(@version) then return {'OK', rev, 0} end " +
            "caps[@capability] = tonumber(@version) rev = rev + 1 " +
            "redis.call('HSET', @meta, 'caps', cjson.encode(caps), 'gaterev', rev, 'actor', @actor) return {'OK', rev, 1}");

        internal static readonly LuaScript SetStore = LuaScript.Prepare(
            "local i = redis.call('HGET', @meta, 'intent') if i and i ~= '' then return {'PENDING', 0} end " +
            "redis.call('HSET', @meta, 'store', @store) return {'OK', 0}");

        internal static async Task<Dictionary<string, string?>> RequireStoreAsync(IDatabase db)
        {
            var fields = await db.HashGetAllAsync(RedisMigrator.StoreKey).ConfigureAwait(false);
            if (fields.Length == 0)
            {
                throw new RedisMigrationException("store_not_initialized", "The store is not installed; run migrate first.");
            }

            return fields.ToDictionary(e => (string)e.Name!, e => (string?)e.Value, StringComparer.Ordinal);
        }

        /// <summary>Every application's meta key (<c>SCAN MATCH rw:{*}:meta</c>), optionally one name; the name is decoded from the hex hash tag.</summary>
        internal static async Task<List<(string Name, RedisKey Meta)>> ApplicationsAsync(IServer server, IDatabase db, string? only, CancellationToken ct)
        {
            var list = new List<(string, RedisKey)>();
            await foreach (var key in server.KeysAsync(db.Database, "rw:{*}:meta", 1000).WithCancellation(ct).ConfigureAwait(false))
            {
                var text = (string)key!;
                var name = Encoding.UTF8.GetString(Convert.FromHexString(text["rw:{".Length..text.IndexOf('}', StringComparison.Ordinal)]));
                if (only is null || only == name)
                {
                    list.Add((name, key));
                }
            }

            list.Sort((a, b) => string.CompareOrdinal(a.Item1, b.Item1));
            return list;
        }
    }
}
