using Runwick.Outbox;
using Runwick.Outbox.Postgres;

namespace Microsoft.Extensions.DependencyInjection;

public static class PostgresOutboxServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IPostgresOutbox"/> over the source database at <paramref name="sourceConnectionString"/> and the
    /// relay that delivers to this host's Runwick store (call <c>AddRunwick</c> as well). The relay opens at most two
    /// source connections (14 §4.3). Set <see cref="OutboxRelayOptions.Enabled"/> to false in stage-only processes.
    /// </summary>
    public static IServiceCollection AddRunwickPostgresOutbox(this IServiceCollection services, string sourceConnectionString,
        OutboxDestination? destination = null, Action<OutboxRelayOptions>? relay = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceConnectionString);
        OutboxServices.Add(services, destination ?? new OutboxDestination(), relay, _ => new PostgresOutboxSource(sourceConnectionString));
        services.AddSingleton<IPostgresOutbox>(sp => new PostgresOutbox(sp.GetRequiredService<OutboxStager>()));
        return services;
    }
}
