using System.Globalization;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>One run of the 22 §2.3 check: verified, or unavailable with the failing setting; plus the Describe facts.</summary>
internal sealed record RxProfileReport(string? Failure, string? FailureCode, IReadOnlyDictionary<string, string> Facts)
{
    public bool Verified => Failure is null;
}

/// <summary>Which supported topology a check runs for (22 §2.3, §2.5, §2.6).</summary>
internal enum RxTopology
{
    Standalone,
    Sentinel,
    Cluster,
}

/// <summary>The <c>redis-aof-always-single-primary-v1</c> profile and topology check (22 §2.3), on the admin connection.</summary>
internal static class RxProfile
{
    public const string Name = "redis-aof-always-single-primary-v1";

    public const string EverySecondName = "redis-aof-everysec-single-primary-v1";

    public const string SentinelName = "redis-aof-always-sentinel-v1";

    public const string ClusterName = "redis-aof-always-cluster-v1";

    static (string Name, string Value)[] Required(RedisPersistence persistence) =>
    [
        ("appendonly", "yes"), ("appendfsync", persistence == RedisPersistence.AofEverySecond ? "everysec" : "always"), ("no-appendfsync-on-rewrite", "no"),
        ("maxmemory-policy", "noeviction"), ("aof-load-truncated", "no"),
    ];

    public static readonly Version MinimumVersion = new(8, 2, 0);

    /// <summary>The newest version a conformance report covers (00 §9 rule 10); newer ones are accepted, uncertified.</summary>
    public static readonly Version CertifiedVersion = new(8, 2, 11);

    public static async Task<RxProfileReport> CheckAsync(IServer server, IDatabase db, RedisPersistence persistence = RedisPersistence.AofAlways, RxTopology topology = RxTopology.Standalone)
    {
        var facts = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["driver"] = "redis",
            ["profile"] = topology switch
            {
                RxTopology.Sentinel => SentinelName,
                RxTopology.Cluster => ClusterName,
                _ => persistence == RedisPersistence.AofEverySecond ? EverySecondName : Name,
            },
            ["topology"] = topology switch { RxTopology.Sentinel => "sentinel-primary", RxTopology.Cluster => "cluster-primary", _ => "standalone-primary" },
            ["limits"] = "keys=256,afterImages=1024,intentBytes=4194304",
        };
        string? failure = null;
        string? code = null;
        void Fail(string what, string c = "store_profile_mismatch")
        {
            failure ??= what;
            code ??= c;
        }

        foreach (var (name, value) in Required(persistence))
        {
            string actual;
            try
            {
                var r = await server.ConfigGetAsync(name).ConfigureAwait(false);
                actual = r.Length == 1 ? r[0].Value : "<missing>";
            }
            catch (RedisServerException)
            {
                actual = "unverifiable";
            }

            facts["config." + name] = actual;
            if (!string.Equals(actual, value, StringComparison.OrdinalIgnoreCase))
            {
                Fail($"{name}={actual}");
            }
        }

        var cluster = topology == RxTopology.Cluster;
        var info = await InfoAsync(server).ConfigureAwait(false); // this node's own answer, not whichever node a cluster connection picks
        foreach (var (key, expected) in new[] { ("role", "master"), ("redis_mode", cluster ? "cluster" : "standalone"), ("cluster_enabled", cluster ? "1" : "0"), ("aof_enabled", "1"), ("aof_last_write_status", "ok") })
        {
            var actual = info.GetValueOrDefault(key, "<missing>");
            if (actual != expected)
            {
                Fail($"{key}={actual}", key is "redis_mode" or "cluster_enabled" or "role" ? "topology_unsupported" : "store_profile_mismatch");
            }
        }

        foreach (var key in new[] { "redis_version", "run_id", "os", "arch_bits", "connected_slaves", "maxmemory", "loading" })
        {
            facts[key] = info.GetValueOrDefault(key, "");
        }

        facts["replicas"] = facts["connected_slaves"];
        if (topology != RxTopology.Standalone)
        {
            // The lineage fields the authority compares after a promotion (22 §2.5).
            foreach (var key in new[] { "master_replid", "master_replid2", "master_repl_offset", "second_repl_offset" })
            {
                facts[key] = info.GetValueOrDefault(key, "");
            }
        }

        if (!Version.TryParse(facts["redis_version"], out var version) || version < MinimumVersion)
        {
            Fail($"redis_version={facts["redis_version"]}", "engine_version_unsupported");
        }

        facts["engine_certified"] = version is not null && version <= CertifiedVersion ? "true" : "false";
        try
        {
            var modules = (RedisResult[])(await server.ExecuteAsync("MODULE", "LIST").ConfigureAwait(false))!;
            var names = new List<string>();
            foreach (var m in modules)
            {
                var pairs = (RedisResult[])m!;
                string? n = null, v = null;
                for (var i = 0; i + 1 < pairs.Length; i += 2)
                {
                    var k = (string?)pairs[i];
                    if (k == "name")
                    {
                        n = (string?)pairs[i + 1];
                    }
                    else if (k == "ver")
                    {
                        v = pairs[i + 1].ToString();
                    }
                }

                names.Add($"{n}@{v}");
            }

            facts["modules"] = string.Join(',', names.Order(StringComparer.Ordinal));
        }
        catch (RedisServerException)
        {
            facts["modules"] = "unverifiable";
            Fail("MODULE LIST=unverifiable");
        }

        facts["profile_status"] = failure is null ? "verified" : "unavailable: " + failure;
        return new RxProfileReport(failure, code, facts);
    }

    public static async Task<Dictionary<string, string>> InfoAsync(IServer server, string section = "everything")
    {
        var text = (string)(await server.ExecuteAsync("INFO", section).ConfigureAwait(false))!;
        var d = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in text.Split('\n'))
        {
            var i = line.IndexOf(':', StringComparison.Ordinal);
            if (i > 0 && line[0] != '#')
            {
                d[line[..i]] = line[(i + 1)..].Trim();
            }
        }

        return d;
    }

    public static long ParseLong(string s) => long.Parse(s, CultureInfo.InvariantCulture);
}
