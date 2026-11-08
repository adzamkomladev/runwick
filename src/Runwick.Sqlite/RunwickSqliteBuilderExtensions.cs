using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Sqlite.Store;

namespace Runwick;

public static class RunwickSqliteBuilderExtensions
{
    /// <summary>
    /// Uses one local SQLite file as the store (20 §2.1). Runwick opens its own connections from the connection string and
    /// never logs it. In-memory, read-only and shared-cache connection strings are refused with <c>invalid_policy</c>.
    /// </summary>
    public static RunwickBuilder UseSqlite(this RunwickBuilder builder, string connectionString, Action<SqliteOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new SqliteOptions();
        configure?.Invoke(options);
        var normalized = "";
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.MissingStore, "sqlite", "The connection string is empty."));
        }
        else
        {
            try
            {
                normalized = SqFile.Normalize(connectionString, SqliteOpenMode.ReadWrite);
            }
            catch (Exception ex) when (ex is SqProfileException or ArgumentException or FormatException)
            {
                builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "sqlite", ex.Message));
            }
        }

        if (options.WriterBusyTimeout < TimeSpan.FromMilliseconds(100) || options.WriterBusyTimeout > TimeSpan.FromSeconds(30)
            || options.ReaderThreads is < 1 or > 8)
        {
            builder.EarlyErrors.Add(new ConfigurationError(RunwickErrorCodes.InvalidPolicy, "sqlite",
                "WriterBusyTimeout must be 100 ms – 30 s and ReaderThreads 1 – 8."));
        }

        builder.AddStoreProvider(new SqliteStoreRegistration(normalized, options));
        return builder;
    }
}

internal sealed class SqliteStoreRegistration(string connectionString, SqliteOptions options) : IStoreProviderRegistration
{
    public string Name => "sqlite";

    public string ConnectionString => connectionString;

    public SqliteOptions Options => options;

    public void Register(IServiceCollection services)
    {
        services.AddSingleton(this);
        services.TryAddSingleton<IFaultInjector, NullFaultInjector>();
        services.AddSingleton(sp => new SqExecutor(connectionString, options, sp.GetRequiredService<ProducerOptions>().MaxConcurrentCalls));
        services.AddSingleton(sp => new SqRunner(sp.GetRequiredService<SqExecutor>(), options, sp.GetRequiredService<IFaultInjector>(),
            sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton(sp => new SqliteStore(sp.GetRequiredService<SqExecutor>(), sp.GetRequiredService<SqRunner>(), sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IRunwickStore>(sp => sp.GetRequiredService<SqliteStore>());
        services.AddSingleton<IReadinessDiagnostics>(sp => sp.GetRequiredService<SqliteStore>());
        services.AddSingleton<IRateControlStore>(sp => sp.GetRequiredService<SqliteStore>());
        services.AddSingleton<IScalingStore>(sp => sp.GetRequiredService<SqliteStore>());
    }
}
