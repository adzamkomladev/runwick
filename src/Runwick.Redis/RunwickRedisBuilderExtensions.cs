using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Runwick.Protocol;
using Runwick.Redis.Store;
using Runwick.Registration;

namespace Runwick;

/// <summary>Redis store options (22 §2.1, §10).</summary>
public sealed class RedisOptions
{
    /// <summary>How often the profile and topology check runs while the process runs (5 s – 5 min).</summary>
    public TimeSpan ProfileRecheckInterval { get; set; } = TimeSpan.FromSeconds(30);
}

public static class RunwickRedisBuilderExtensions
{
    /// <summary>
    /// Uses one Redis primary under <c>redis-aof-always-single-primary-v1</c> as the store (22 §2.1). Runwick takes only the
    /// endpoint, credentials, TLS settings, <c>defaultDatabase</c> and <c>connectTimeout</c> from the configuration string and
    /// never logs it. Several endpoints or a Sentinel service name are refused with <c>topology_unsupported</c>.
    /// </summary>
    public static RunwickBuilder UseRedis(this RunwickBuilder builder, string configuration, Action<RedisOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new RedisOptions();
        configure?.Invoke(options);
        StackExchange.Redis.ConfigurationOptions? parsed = null;
        if (string.IsNullOrWhiteSpace(configuration))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.MissingStore, "redis", "The configuration string is empty."));
        }
        else
        {
            try
            {
                parsed = RxClient.Options(configuration, TimeSpan.FromSeconds(5));
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

        builder.AddStoreProvider(new RedisStoreRegistration(parsed, options));
        return builder;
    }
}

internal sealed class RedisStoreRegistration(StackExchange.Redis.ConfigurationOptions? configuration, RedisOptions options) : IStoreProviderRegistration
{
    public string Name => "redis";

    public RedisOptions Options => options;

    public void Register(IServiceCollection services)
    {
        services.AddSingleton(this);
        services.TryAddSingleton<IFaultInjector, NullFaultInjector>();
        services.AddSingleton(sp => new RxClient(configuration ?? new StackExchange.Redis.ConfigurationOptions(),
            sp.GetRequiredService<JobRegistry>().Application, sp.GetRequiredService<ProducerOptions>().MaxConcurrentCalls));
        services.AddSingleton(sp => new RxRunner(sp.GetRequiredService<RxClient>(), sp.GetRequiredService<TimeProvider>()));
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
