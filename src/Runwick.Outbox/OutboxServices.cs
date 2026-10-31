using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Runwick.Dispatching;
using Runwick.Hosting;
using Runwick.Protocol;

namespace Runwick.Outbox;

/// <summary>Registration shared by the source adapters: one source, one destination and one relay group per host (14 §2.3).</summary>
internal static class OutboxServices
{
    public static void Add(IServiceCollection services, OutboxDestination destination, Action<OutboxRelayOptions>? relay,
        Func<IServiceProvider, IOutboxSource> source)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(destination);
        if (services.Any(d => d.ServiceType == typeof(IOutboxSource)))
        {
            throw new InvalidOperationException("An outbox source adapter is already registered; one per host.");
        }

        var options = new OutboxRelayOptions();
        relay?.Invoke(options);
        var errors = options.Validate();
        if (errors.Count > 0)
        {
            throw new InvalidOperationException("Invalid outbox relay options: " + string.Join(" ", errors));
        }

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton(destination);
        services.AddSingleton(options);
        services.AddSingleton(source);
        services.AddSingleton(sp => new OutboxStager(
            sp.GetRequiredService<RunwickRuntime>(), sp.GetRequiredService<DispatchPreparer>(), destination));
        services.AddSingleton(sp => new OutboxRelay(
            sp.GetRequiredService<IOutboxSource>(),
            sp.GetRequiredService<RunwickRuntime>(),
            sp.GetRequiredService<IPreparedJobDispatcher>(),
            sp.GetRequiredService<DispatchPreparer>(),
            sp.GetService<IOutboxDestinationStore>(),
            sp.GetRequiredService<OutboxRelayOptions>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetService<ILogger<OutboxRelay>>() ?? NullLogger<OutboxRelay>.Instance));
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<OutboxRelay>());
    }
}
