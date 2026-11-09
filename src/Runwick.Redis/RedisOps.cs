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
        /// is refused with <c>intent_pending</c>. A raise also writes the store operation's <c>capabilities_activated</c> event and
        /// <c>ActivateCapabilities</c> audit row, with <paramref name="actor"/> as the subject and scheme <c>operator</c>; a repeat writes nothing.
        /// </summary>
        public static Task<IReadOnlyList<CapabilityChange>> ActivateCapabilityAsync(
            string configuration, string? application, string capability, string actor, CancellationToken cancellationToken = default)
        {
            StoreAdminRules.CheckActor(actor);
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
                    var prefix = ((string)meta!)[..^"meta".Length];
                    var result = (RedisResult[])(await db.ScriptEvaluateAsync(RedisOps.ActivateCapability, new
                    {
                        meta, capability, version, actor, application = name, command = RxClient.Hex(Guid.NewGuid()), scheme = StoreAdminRules.OperatorScheme,
                        events = (RedisKey)(prefix + "events"), eventsData = (RedisKey)(prefix + "events:data"),
                        audit = (RedisKey)(prefix + "audit"), auditData = (RedisKey)(prefix + "audit:data"),
                    }).ConfigureAwait(false))!;
                    if ((string?)result[0] == "PENDING")
                    {
                        throw new RedisMigrationException("intent_pending", $"Application {name} has a pending intent; let recovery finish and retry.");
                    }

                    changes.Add(new CapabilityChange(name, capability, version, (long)result[1], (long)result[2] == 1));
                }

                return changes;
            }, cancellationToken);
        }

        /// <summary>
        /// Reads the store's layout revision, mode, application gates and lease and presence counts (23 §6). Owned leases are the
        /// members of every application's <c>leases</c> index, presences the rows of <c>workers</c>, and expired presences those whose
        /// <c>workers:exp</c> score is at or before the server's clock.
        /// </summary>
        public static Task<StoreAdminStatus> GetStatusAsync(string configuration, CancellationToken cancellationToken = default) =>
            RedisMigrator.WithLockAsync(configuration, async (db, server) =>
            {
                var store = await RedisOps.RequireStoreAsync(db).ConfigureAwait(false);
                var apps = new List<ApplicationAdmissionState>();
                var now = new DateTimeOffset(await server.TimeAsync().ConfigureAwait(false), TimeSpan.Zero).ToUnixTimeMilliseconds();
                long leases = 0, presences = 0, expired = 0;
                foreach (var (name, meta) in await RedisOps.ApplicationsAsync(server, db, null, cancellationToken).ConfigureAwait(false))
                {
                    var prefix = ((string)meta!)[..^"meta".Length];
                    leases += await db.SortedSetLengthAsync(prefix + "leases").ConfigureAwait(false);
                    presences += await db.HashLengthAsync(prefix + "workers").ConfigureAwait(false);
                    expired += await db.SortedSetLengthAsync(prefix + "workers:exp", double.NegativeInfinity, now).ConfigureAwait(false);
                    apps.Add(new ApplicationAdmissionState(name, (string?)await db.HashGetAsync(meta, "admission").ConfigureAwait(false) ?? "open",
                        long.Parse((string?)await db.HashGetAsync(meta, "gaterev").ConfigureAwait(false) ?? "1", CultureInfo.InvariantCulture),
                        (string?)await db.HashGetAsync(meta, "caps").ConfigureAwait(false) ?? "{}"));
                }

                return new StoreAdminStatus(Guid.ParseExact(store["store"]!, "N"), long.Parse(store["layout"]!, CultureInfo.InvariantCulture),
                    RedisMigrator.CurrentLayout, store["mode"]!, apps, (int)leases, (int)presences, (int)expired, StoreAdminRules.QuiescenceNotice);
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
            "caps[@capability] = tonumber(@version) local before = rev rev = rev + 1 " +
            "local enc = cjson.encode(caps) " +
            "redis.call('HSET', @meta, 'caps', enc, 'gaterev', rev, 'actor', @actor) " +
            // The store operation's capabilities_activated event and ActivateCapabilities audit row (activate_capabilities.lua), keys in byte order.
            "local t = redis.call('TIME') local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000) " +
            "local function d(n) return string.format('%d', n) end " +
            "local who, cmd = cjson.encode(@actor), cjson.encode(@command) " +
            "local ev = tonumber(redis.call('HGET', @meta, 'nextevent') or '1') " +
            "redis.call('ZADD', @events, ev, d(ev)) " +
            "redis.call('HSET', @eventsData, d(ev), '{\"actorId\":' .. who .. ',\"attemptId\":null,\"commandId\":' .. cmd .. " +
            "',\"details\":{\"requiredCapabilities\":' .. enc .. ',\"revision\":' .. d(rev) .. '},\"eventId\":' .. d(ev) .. " +
            "',\"jobId\":null,\"kind\":\"capabilities_activated\",\"occurredAt\":' .. d(now) .. '}') " +
            "local au = tonumber(redis.call('HGET', @meta, 'nextaudit') or '1') " +
            "redis.call('ZADD', @audit, au, d(au)) " +
            "redis.call('HSET', @auditData, d(au), '{\"action\":\"ActivateCapabilities\",\"at\":' .. d(now) .. ',\"auditId\":' .. d(au) .. " +
            "',\"commandId\":' .. cmd .. ',\"details\":{\"requiredCapabilities\":' .. enc .. '},\"display\":null,\"expectedRevision\":' .. d(before) .. " +
            "',\"reason\":null,\"resultingRevision\":' .. d(rev) .. ',\"scheme\":' .. cjson.encode(@scheme) .. ',\"subject\":' .. who .. " +
            "',\"targetId\":' .. cjson.encode(@application) .. ',\"targetKind\":\"application\"}') " +
            "redis.call('HSET', @meta, 'nextevent', d(ev + 1), 'nextaudit', d(au + 1)) return {'OK', rev, 1}");

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
