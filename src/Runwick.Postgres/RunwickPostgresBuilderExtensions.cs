using Microsoft.Extensions.DependencyInjection;
using Runwick.Postgres;
using Runwick.Registration;

namespace Runwick;

public static class RunwickPostgresBuilderExtensions
{
    /// <summary>Uses PostgreSQL as the store. Runwick creates and owns its data sources and never logs the connection string.</summary>
    public static RunwickBuilder UsePostgres(this RunwickBuilder builder, string connectionString, Action<PostgresOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new PostgresOptions();
        configure?.Invoke(options);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.MissingStore, "postgres", "The connection string is empty."));
        }

        if (!string.Equals(options.Schema, "runwick", StringComparison.Ordinal))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "postgres",
                "Only the 'runwick' schema is supported in this release."));
        }

        builder.AddStoreProvider(new PostgresStoreRegistration(connectionString, options));
        return builder;
    }
}

internal sealed class PostgresStoreRegistration(string connectionString, PostgresOptions options) : IStoreProviderRegistration
{
    public string Name => "postgres";

    public string ConnectionString => connectionString;

    public PostgresOptions Options => options;

    public void Register(IServiceCollection services)
    {
        services.AddSingleton(this);
        PostgresServices.Register(services);
    }
}
