using Npgsql;
using Runwick.Postgres.Migrations;

namespace Runwick.Harness.Store;

/// <summary>
/// One PostgreSQL server per test assembly with a migrated template database <c>rw_template</c>;
/// each test gets <c>CREATE DATABASE rw_&lt;ns&gt; TEMPLATE rw_template</c> (07 §2.6).
/// </summary>
internal sealed class StoreServer
{
    static readonly SemaphoreSlim Gate = new(1, 1);
    static StoreServer? _shared;

    StoreServer(TestDatabase database) => Database = database;

    public TestDatabase Database { get; }

    public static async Task<StoreServer> SharedAsync()
    {
        await Gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_shared is null)
            {
                var db = await TestDatabase.SharedAsync().ConfigureAwait(false);
                var server = new StoreServer(db);
                await server.CreateTemplateAsync().ConfigureAwait(false);
                _shared = server;
            }

            return _shared;
        }
        finally
        {
            Gate.Release();
        }
    }

    public string ConnectionStringFor(string database, string applicationName = "rw:harness:admin", bool pooling = true) =>
        new NpgsqlConnectionStringBuilder(Database.ConnectionString)
        {
            Database = database,
            ApplicationName = applicationName,
            Pooling = pooling,
        }.ConnectionString;

    async Task CreateTemplateAsync()
    {
        var admin = ConnectionStringFor("postgres", pooling: false);
        await ExecAsync(admin, """
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'rw_template') THEN
                ALTER DATABASE rw_template WITH IS_TEMPLATE false ALLOW_CONNECTIONS true;
              END IF;
            END $$
            """).ConfigureAwait(false);
        await ExecAsync(admin, "DROP DATABASE IF EXISTS rw_template WITH (FORCE)").ConfigureAwait(false);
        await ExecAsync(admin, "CREATE DATABASE rw_template").ConfigureAwait(false);
        await RunwickPostgresMigrations.MigrateAsync(ConnectionStringFor("rw_template", pooling: false)).ConfigureAwait(false);
        await ExecAsync(admin, "ALTER DATABASE rw_template WITH IS_TEMPLATE true ALLOW_CONNECTIONS false").ConfigureAwait(false);
    }

    /// <summary>Creates <c>rw_&lt;ns&gt;</c> from the template and gives it a fresh store identity.</summary>
    public async Task<string> CreateDatabaseAsync(string ns)
    {
        if (!System.Text.RegularExpressions.Regex.IsMatch(ns, "^[a-z0-9_]{1,40}$"))
        {
            throw new ArgumentException("Namespace must match [a-z0-9_]{1,40}.", nameof(ns));
        }

        var name = "rw_" + ns;
        var admin = ConnectionStringFor("postgres", pooling: false);
        await ExecAsync(admin, $"DROP DATABASE IF EXISTS {name} WITH (FORCE)").ConfigureAwait(false);
        await ExecAsync(admin, $"CREATE DATABASE {name} TEMPLATE rw_template").ConfigureAwait(false);
        await ExecAsync(ConnectionStringFor(name, pooling: false),
            "UPDATE runwick.store_metadata SET store_id = gen_random_uuid() WHERE singleton_id = 1").ConfigureAwait(false);
        return name;
    }

    /// <summary>Creates an empty database (no Runwick schema) for migration tests.</summary>
    public async Task<string> CreateEmptyDatabaseAsync(string ns)
    {
        var name = "rw_" + ns;
        var admin = ConnectionStringFor("postgres", pooling: false);
        await ExecAsync(admin, $"DROP DATABASE IF EXISTS {name} WITH (FORCE)").ConfigureAwait(false);
        await ExecAsync(admin, $"CREATE DATABASE {name}").ConfigureAwait(false);
        return name;
    }

    public async Task DropDatabaseAsync(string name)
    {
        NpgsqlConnection.ClearAllPools();
        await ExecAsync(ConnectionStringFor("postgres", pooling: false), $"DROP DATABASE IF EXISTS {name} WITH (FORCE)").ConfigureAwait(false);
    }

    public static async Task ExecAsync(string connectionString, string sql)
    {
        var conn = new NpgsqlConnection(connectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync().ConfigureAwait(false);
            using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
}
