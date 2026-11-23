using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Registration;

namespace Runwick;

/// <summary>Redis store options (22 §2.1, §10).</summary>
/// <summary>The persistence profile the Redis server must run and the acknowledgement it earns (22 §2.3, G4).</summary>
public enum RedisPersistence
{
    /// <summary><c>appendfsync always</c>: a reply follows an fsync. The default.</summary>
    AofAlways,

    /// <summary>
    /// <c>appendfsync everysec</c> (opt-in, profile <c>redis-aof-everysec-single-primary-v1</c>): faster writes, but a machine or
    /// operating-system crash can lose about the last second of acknowledged writes. Replicas and failover stay unsupported.
    /// </summary>
    AofEverySecond,
}

/// <summary>The write-authority timing shared by the Sentinel and Cluster modes (22 §2.5, §2.6).</summary>
public abstract class RedisAuthorityOptions
{
    /// <summary>How often the primary is verified again (250 ms – 10 s, default 1 s).</summary>
    public TimeSpan AuthorityRecheckInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How long one successful verification lets commands go out (1 s – 30 s, default 4 s, at least twice the recheck interval). A process
    /// cut off from the Sentinels (or the cluster) stops writing when this runs out, so keep it well under the failure-detection time of
    /// the Sentinels (<c>down-after-milliseconds</c>) or the cluster (<c>cluster-node-timeout</c>).
    /// </summary>
    public TimeSpan AuthorityTtl { get; set; } = TimeSpan.FromSeconds(4);
}

/// <summary>
/// Sentinel high availability (opt-in, profile <c>redis-aof-always-sentinel-v1</c>, 22 §2.5). Runwick writes only to a primary that a
/// majority of the configured Sentinels name and whose profile and history it has just verified; otherwise it stops sending commands.
/// </summary>
public sealed class RedisSentinelOptions : RedisAuthorityOptions
{
}

/// <summary>
/// Redis Cluster (opt-in, profile <c>redis-aof-always-cluster-v1</c>, 22 §2.6). All of an application's keys share one hash slot, so one
/// primary owns the whole application. Runwick writes only while a majority of the listed nodes name the same healthy owner for that slot
/// and its profile and history are verified; while the slot is moving or the owner is failing it stops sending commands.
/// </summary>
public sealed class RedisClusterOptions : RedisAuthorityOptions
{
}

public sealed class RedisOptions
{
    /// <summary>
    /// Opt-in Sentinel mode. The configuration string then lists the Sentinel endpoints and <c>serviceName</c>, <c>user</c>/<c>password</c> are
    /// the data nodes' credentials and <c>sentinelUser</c>/<c>sentinelPassword</c> the Sentinels'. Needs <c>appendfsync always</c>.
    /// </summary>
    public RedisSentinelOptions? Sentinel { get; set; }

    /// <summary>
    /// Opt-in Cluster mode. The configuration string then lists the cluster's nodes (list every primary), <c>user</c>/<c>password</c> are the
    /// nodes' credentials, and the cluster needs <c>appendfsync always</c> on every primary. Cannot be combined with <see cref="Sentinel"/>.
    /// </summary>
    public RedisClusterOptions? Cluster { get; set; }

    /// <summary>Which persistence profile the server must run. A server on the other profile is refused with <c>store_profile_mismatch</c>.</summary>
    public RedisPersistence Persistence { get; set; } = RedisPersistence.AofAlways;

    /// <summary>How often the profile and topology check runs while the process runs (5 s – 5 min).</summary>
    public TimeSpan ProfileRecheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Opt-in producer grouping (default off). Plain dispatches that arrive while a send is in flight wait briefly and go to Redis
    /// in one script call, each with its own command identity, receipt and event. A caller that finds a free send slot goes out alone
    /// at once and never waits for company. Dispatches with uniqueness keys, concurrency or rate scopes always go alone.
    /// </summary>
    public bool GroupDispatches { get; set; }

    /// <summary>
    /// Opt-in Redis Functions for the worker commands (default off, 22 §4.1). Claim, finish and renew are then loaded once per server as a
    /// Functions library and called with <c>FCALL</c>; a server that refuses <c>FUNCTION</c> or <c>FCALL</c> gets <c>EVALSHA</c> instead.
    /// Off by default because drains measured slower with it (0.83× on disk, 0.89× in memory); results are identical either way.
    /// </summary>
    public bool UseFunctions { get; set; }

    /// <summary>Most dispatches in one grouped call (2 – 16, default 8). Only used with <see cref="GroupDispatches"/>.</summary>
    public int MaxGroupItems { get; set; } = 8;

    /// <summary>Most estimated request bytes in one grouped call (16 KiB – 128 KiB, default 128 KiB). Larger dispatches go alone.</summary>
    public int MaxGroupBytes { get; set; } = 131_072;

    /// <summary>Grouped or single sends that may be in flight at once per application (1 – 16, default 2).</summary>
    public int MaxGroupsInFlight { get; set; } = 2;
}

public static class RunwickRedisBuilderExtensions
{
    /// <summary>
    /// Uses one Redis primary under <c>redis-aof-always-single-primary-v1</c> as the store (22 §2.1). Runwick takes only the
    /// endpoint, credentials, TLS settings, <c>defaultDatabase</c> and <c>connectTimeout</c> from the configuration string and
    /// never logs it. Several endpoints or a Sentinel service name are refused with <c>topology_unsupported</c> unless
    /// <see cref="RedisOptions.Sentinel"/> or <see cref="RedisOptions.Cluster"/> is set.
    /// </summary>
    public static RunwickBuilder UseRedis(this RunwickBuilder builder, string configuration, Action<RedisOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new RedisOptions();
        configure?.Invoke(options);
        StackExchange.Redis.ConfigurationOptions? parsed = null;
        RxSentinelSeeds? seeds = null;
        if (string.IsNullOrWhiteSpace(configuration))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.MissingStore, "redis", "The configuration string is empty."));
        }
        else
        {
            try
            {
                if (options.Sentinel is not null && options.Cluster is not null)
                {
                    throw new RxProfileException("invalid_policy", "Sentinel and Cluster cannot both be set.");
                }

                if (options.Cluster is not null)
                {
                    (parsed, seeds) = RxClient.ClusterOptions(configuration, TimeSpan.FromSeconds(5));
                }
                else if (options.Sentinel is null)
                {
                    parsed = RxClient.Options(configuration, TimeSpan.FromSeconds(5));
                }
                else
                {
                    (parsed, seeds) = RxClient.SentinelOptions(configuration, TimeSpan.FromSeconds(5));
                }
            }
            catch (RxProfileException ex)
            {
                builder.EarlyErrors.Add(new ConfigurationError(ex.Code == "invalid_policy" ? RunwickErrorCodes.InvalidPolicy : ex.Code, "redis", ex.Message));
            }
        }

        if (options.ProfileRecheckInterval < TimeSpan.FromSeconds(5) || options.ProfileRecheckInterval > TimeSpan.FromMinutes(5))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "redis", "ProfileRecheckInterval must be 5 s – 5 min."));
        }

        if (((RedisAuthorityOptions?)options.Sentinel ?? options.Cluster) is { } sentinel)
        {
            if (sentinel.AuthorityRecheckInterval < TimeSpan.FromMilliseconds(250) || sentinel.AuthorityRecheckInterval > TimeSpan.FromSeconds(10)
                || sentinel.AuthorityTtl < TimeSpan.FromSeconds(1) || sentinel.AuthorityTtl > TimeSpan.FromSeconds(30)
                || sentinel.AuthorityTtl < 2 * sentinel.AuthorityRecheckInterval)
            {
                builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "redis",
                    "AuthorityRecheckInterval must be 250 ms – 10 s, AuthorityTtl 1 s – 30 s and at least twice the recheck interval."));
            }

            if (options.Persistence != RedisPersistence.AofAlways)
            {
                builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "redis", "The Sentinel and Cluster profiles need appendfsync always."));
            }
        }

        if (options.GroupDispatches && (options.MaxGroupItems is < 2 or > 16 || options.MaxGroupBytes is < 16_384 or > 131_072 || options.MaxGroupsInFlight is < 1 or > 16))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "redis",
                "MaxGroupItems must be 2 – 16, MaxGroupBytes 16 KiB – 128 KiB and MaxGroupsInFlight 1 – 16."));
        }

        builder.AddStoreProvider(new RedisStoreRegistration(parsed, options, seeds));
        return builder;
    }
}

internal sealed class RedisStoreRegistration(StackExchange.Redis.ConfigurationOptions? configuration, RedisOptions options, RxSentinelSeeds? seeds = null) : IStoreProviderRegistration
{
    public string Name => "redis";

    public RedisOptions Options => options;

    public void Register(IServiceCollection services)
    {
        services.AddSingleton(this);
        services.TryAddSingleton<IFaultInjector, NullFaultInjector>();
        services.AddSingleton(sp => new RxClient(configuration ?? new StackExchange.Redis.ConfigurationOptions(),
            sp.GetRequiredService<JobRegistry>().Application, sp.GetRequiredService<ProducerOptions>().MaxConcurrentCalls)
        {
            Cluster = options.Cluster is not null,
            UseFunctions = options.UseFunctions,
            Authority = seeds is null || ((RedisAuthorityOptions?)options.Sentinel ?? options.Cluster) is not { } authority ? null
                : new RxAuthority(seeds, authority, sp.GetRequiredService<TimeProvider>(), configuration!, options.Persistence, options.ProfileRecheckInterval,
                    options.Cluster is null ? null : RxClient.SlotOf(sp.GetRequiredService<JobRegistry>().Application), options.UseFunctions),
        });
        services.AddSingleton(sp => new RxRunner(sp.GetRequiredService<RxClient>(), sp.GetRequiredService<TimeProvider>(), sp.GetRequiredService<IFaultInjector>()));
        services.AddSingleton(sp => new RedisStore(sp.GetRequiredService<RxRunner>(), options, sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IRunwickStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IReadinessDiagnostics>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IJobControlStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IManagementQueryStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IRateControlStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IScalingStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IOutboxDestinationStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IScheduleStore>(sp => sp.GetRequiredService<RedisStore>());
        services.AddSingleton<IRetentionStore>(sp => sp.GetRequiredService<RedisStore>());
    }
}
