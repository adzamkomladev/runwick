using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Runwick;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Worker;

namespace Microsoft.Extensions.DependencyInjection;

public static class RunwickServiceCollectionExtensions
{
    /// <summary>Registers Runwick. Configuration is validated once, at host start or first dispatcher resolution.</summary>
    public static IServiceCollection AddRunwick(this IServiceCollection services, Action<RunwickBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(d => d.ServiceType == typeof(RunwickMarker)))
        {
            throw new RunwickConfigurationException([new ConfigurationError(RunwickErrorCodes.RunwickAlreadyAdded, "AddRunwick",
                "AddRunwick was called twice on one service collection.")]);
        }

        services.AddSingleton<RunwickMarker>();
        var builder = new RunwickBuilder(services);
        configure(builder);
        if (builder.Workers.Count > 1)
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.DuplicateWorker, "worker", "AddWorker may be called once."));
        }

        foreach (var provider in builder.StoreProviders.Take(1))
        {
            provider.Register(services);
        }

        foreach (var (_, handler) in builder.Handlers)
        {
            services.TryAddScoped(handler);
        }

        foreach (var middleware in builder.GlobalMiddleware.Concat(builder.MiddlewareAliases.Values.SelectMany(t => t)).Distinct())
        {
            services.TryAddScoped(middleware);
        }

        var isWorker = builder.Workers.Count > 0;
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton(builder);
        services.AddSingleton(sp =>
        {
            var compiler = new RegistryCompiler(builder, isWorker);
            return compiler.Compile() ?? throw new RunwickConfigurationException(compiler.Errors);
        });
        services.AddSingleton<IRunwickJobRegistry>(sp => sp.GetRequiredService<JobRegistry>());
        services.AddSingleton(sp =>
        {
            var options = new ProducerOptions();
            foreach (var action in builder.ProducerConfiguration)
            {
                action(options);
            }

            return options;
        });
        services.AddSingleton(sp => new RunwickRuntime(
            sp.GetRequiredService<JobRegistry>(),
            sp.GetRequiredService<IRunwickStore>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetService<ILogger<RunwickRuntime>>() ?? NullLogger<RunwickRuntime>.Instance));
        services.AddSingleton(sp =>
        {
            var o = sp.GetRequiredService<ProducerOptions>();
            return new LocalAdmission(o.MaxConcurrentCalls, o.MaxWaitingOperations, o.AdmissionWait, sp.GetRequiredService<TimeProvider>());
        });
        services.AddSingleton(sp => new DispatchPreparer(
            sp.GetRequiredService<JobRegistry>(), sp.GetRequiredService<ProducerOptions>(), sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<JobDispatcher>();
        services.AddSingleton<IJobDispatcher>(sp => sp.GetRequiredService<JobDispatcher>());
        services.AddSingleton<IPreparedJobDispatcher>(sp => sp.GetRequiredService<JobDispatcher>());
        if (isWorker)
        {
            // Worker settings validate with the registry (06 §3); the engine starts after the handshake.
            services.AddSingleton(sp => WorkerSettings.Validate(builder.Workers[0], sp.GetRequiredService<JobRegistry>().Queues));
            services.AddSingleton(sp => new WorkerEngine(
                sp.GetRequiredService<RunwickRuntime>(),
                sp.GetRequiredService<WorkerSettings>(),
                sp.GetRequiredService<IServiceScopeFactory>(),
                sp.GetRequiredService<TimeProvider>(),
                sp.GetService<ILogger<WorkerEngine>>() ?? NullLogger<WorkerEngine>.Instance,
                sp.GetService<IHostApplicationLifetime>()));
            services.AddSingleton<IRunwickComponent>(sp => sp.GetRequiredService<WorkerEngine>());
            services.AddScoped<JobContextHolder>();
            services.AddScoped<IJobContext>(sp => sp.GetRequiredService<JobContextHolder>().Context
                ?? throw new InvalidOperationException("IJobContext is available only inside a job execution scope."));
        }

        services.AddHostedService<RunwickHostedService>();
        return services;
    }

    sealed class RunwickMarker;
}
