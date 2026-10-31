using Runwick.Outbox;
using Runwick.Outbox.Sqlite;

namespace Microsoft.Extensions.DependencyInjection;

public static class SqliteOutboxServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="ISqliteOutbox"/> over the same-host SQLite file at <paramref name="sourceConnectionString"/> and the
    /// relay that delivers to this host's Runwick store (call <c>AddRunwick</c> as well). SQLite is an outbox source only,
    /// never a queue provider (14 §1.3). Set <see cref="OutboxRelayOptions.Enabled"/> to false in stage-only processes.
    /// </summary>
    public static IServiceCollection AddRunwickSqliteOutbox(this IServiceCollection services, string sourceConnectionString,
        OutboxDestination? destination = null, Action<OutboxRelayOptions>? relay = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceConnectionString);
        OutboxServices.Add(services, destination ?? new OutboxDestination(), relay, _ => new SqliteOutboxSource(sourceConnectionString));
        services.AddSingleton<ISqliteOutbox>(sp => new SqliteOutbox(sp.GetRequiredService<OutboxStager>()));
        return services;
    }
}
