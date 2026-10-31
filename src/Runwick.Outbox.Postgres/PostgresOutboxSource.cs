using System.Reflection;
using Npgsql;
using NpgsqlTypes;

namespace Runwick.Outbox.Postgres;

/// <summary>Installs the PostgreSQL outbox source schema (14 §3.2). Run once per source database with a DDL-capable role.</summary>
public static class PostgresOutboxSchema
{
    /// <summary>The source schema version this package reads and writes.</summary>
    public const int Version = 1;

    internal static string Sql { get; } = Load("source_v1.sql");

    /// <summary>Creates the <c>runwick_outbox</c> schema if absent and returns the (stable) source UUID. Idempotent.</summary>
    public static async Task<Guid> InstallAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        var connection = new NpgsqlConnection(connectionString);
        await using var connectionScope = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var txScope = tx.ConfigureAwait(false);
        using (var ddl = new NpgsqlCommand(Sql, connection, tx))
        {
            await ddl.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        using var read = new NpgsqlCommand("SELECT source_store_id, schema_version FROM runwick_outbox.outbox_metadata WHERE singleton_id = 1", connection, tx);
        var r = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var rScope = r.ConfigureAwait(false);
        await r.ReadAsync(cancellationToken).ConfigureAwait(false);
        var (id, version) = (r.GetGuid(0), r.GetInt32(1));
        await r.DisposeAsync().ConfigureAwait(false);
        if (version != Version)
        {
            throw new OutboxStagingException("outbox_schema_incompatible", $"Outbox source schema version {version} is not {Version}.");
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return id;
    }

    static string Load(string name)
    {
        using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Runwick.Outbox.Postgres.Sql." + name)
            ?? throw new InvalidOperationException($"Embedded SQL {name} is missing.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}

/// <summary>
/// The relay's view of one PostgreSQL source (14 §4). Two source connections per relay group: one lane for claims,
/// one reserved for renewal and marks, so a slow claim never delays a renewal.
/// </summary>
internal sealed class PostgresOutboxSource : IOutboxSource, IAsyncDisposable
{
    readonly NpgsqlDataSource _data;
    readonly SemaphoreSlim _claimLane = new(1, 1);
    readonly SemaphoreSlim _controlLane = new(1, 1);
    Guid? _sourceStoreId;

    public PostgresOutboxSource(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { MaxPoolSize = 2 };
        if (string.IsNullOrEmpty(builder.ApplicationName))
        {
            builder.ApplicationName = "runwick-outbox-relay";
        }

        _data = NpgsqlDataSource.Create(builder.ConnectionString);
    }

    public Guid SourceStoreId => _sourceStoreId ?? throw new InvalidOperationException("The outbox source is not initialized.");

    public async Task InitializeAsync(CancellationToken ct)
    {
        var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var connectionScope = connection.ConfigureAwait(false);
        using var read = new NpgsqlCommand("SELECT source_store_id, schema_version FROM runwick_outbox.outbox_metadata WHERE singleton_id = 1", connection);
        var r = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using var rScope = r.ConfigureAwait(false);
        if (!await r.ReadAsync(ct).ConfigureAwait(false))
        {
            throw new OutboxStagingException("outbox_not_installed", "The outbox source schema has no metadata row.");
        }

        if (r.GetInt32(1) != PostgresOutboxSchema.Version)
        {
            throw new OutboxStagingException("outbox_schema_incompatible", $"Outbox source schema version {r.GetInt32(1)} is not {PostgresOutboxSchema.Version}.");
        }

        _sourceStoreId = r.GetGuid(0);
    }

    public Task<IReadOnlyList<OutboxLease>> ClaimAsync(Guid owner, int max, TimeSpan leaseDuration, CancellationToken ct) =>
        OnLane(_claimLane, async connection =>
        {
            using var claim = new NpgsqlCommand("""
                WITH due AS (
                    SELECT message_id FROM runwick_outbox.outbox_messages
                    WHERE (state = 'pending' AND next_attempt_at <= clock_timestamp())
                       OR (state = 'leased' AND lease_expires_at <= clock_timestamp())
                    ORDER BY next_attempt_at
                    LIMIT @max
                    FOR UPDATE SKIP LOCKED)
                UPDATE runwick_outbox.outbox_messages AS m
                SET state = 'leased', lease_owner = @owner, lease_token = gen_random_uuid(),
                    lease_expires_at = clock_timestamp() + @lease, attempts = m.attempts + 1, revision = m.revision + 1
                FROM due
                WHERE m.message_id = due.message_id
                RETURNING m.message_id, m.lease_token, m.destination_store_id, m.destination_application, m.intent,
                          m.delivery_fingerprint, m.staged_at, m.accept_until, m.attempts, m.transport_command_id, m.transport_not_after
                """, connection);
            claim.Parameters.AddWithValue("max", NpgsqlDbType.Integer, max);
            claim.Parameters.AddWithValue("owner", NpgsqlDbType.Uuid, owner);
            claim.Parameters.AddWithValue("lease", NpgsqlDbType.Interval, leaseDuration);
            var leases = new List<OutboxLease>();
            var r = await claim.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using var rScope = r.ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                leases.Add(new OutboxLease(r.GetGuid(0), r.GetGuid(1), r.GetGuid(2), r.GetString(3), (byte[])r[4], (byte[])r[5],
                    Utc(r.GetDateTime(6)), Utc(r.GetDateTime(7)), r.GetInt32(8),
                    await r.IsDBNullAsync(9, ct).ConfigureAwait(false) ? null : r.GetGuid(9), await r.IsDBNullAsync(10, ct).ConfigureAwait(false) ? null : Utc(r.GetDateTime(10))));
            }

            return (IReadOnlyList<OutboxLease>)leases;
        }, ct);

    public Task<bool> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken ct) =>
        GuardedAsync(lease, "lease_expires_at = clock_timestamp() + @lease", p => p.AddWithValue("lease", NpgsqlDbType.Interval, leaseDuration),
            requireLive: true, ct);

    public Task<bool> RecordTransportAsync(OutboxLease lease, Guid commandId, DateTimeOffset notAfter, CancellationToken ct) =>
        GuardedAsync(lease, "transport_command_id = @command, transport_not_after = @not_after, revision = revision + 1", p =>
        {
            p.AddWithValue("command", NpgsqlDbType.Uuid, commandId);
            p.AddWithValue("not_after", NpgsqlDbType.TimestampTz, notAfter.UtcDateTime);
        }, requireLive: true, ct);

    public Task<bool> MarkDeliveredAsync(OutboxLease lease, OutboxDelivered delivered, CancellationToken ct) =>
        GuardedAsync(lease, """
            state = 'delivered', delivered_job_id = @job, delivered_disposition = @disposition, confirmed_profile = @profile,
            delivered_at = @delivered_at, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, last_error = NULL,
            revision = revision + 1
            """, p =>
        {
            p.AddWithValue("job", NpgsqlDbType.Uuid, delivered.JobId);
            p.AddWithValue("disposition", NpgsqlDbType.Text, delivered.Disposition);
            p.AddWithValue("profile", NpgsqlDbType.Text, delivered.ConfirmedProfile);
            p.AddWithValue("delivered_at", NpgsqlDbType.TimestampTz, delivered.DeliveredAt.UtcDateTime);
        }, requireLive: false, ct);

    public Task<bool> RetryLaterAsync(OutboxLease lease, TimeSpan delay, string error, CancellationToken ct) =>
        GuardedAsync(lease, """
            state = 'pending', next_attempt_at = clock_timestamp() + @delay, last_error = left(@error, 256),
            lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, revision = revision + 1
            """, p =>
        {
            p.AddWithValue("delay", NpgsqlDbType.Interval, delay);
            p.AddWithValue("error", NpgsqlDbType.Text, error);
        }, requireLive: false, ct);

    public Task<bool> BlockAsync(OutboxLease lease, string reason, CancellationToken ct) =>
        GuardedAsync(lease, """
            state = 'blocked', blocked_reason = left(@reason, 256), lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
            revision = revision + 1
            """, p => p.AddWithValue("reason", NpgsqlDbType.Text, reason), requireLive: false, ct);

    public async ValueTask DisposeAsync()
    {
        await _data.DisposeAsync().ConfigureAwait(false);
        _claimLane.Dispose();
        _controlLane.Dispose();
    }

    /// <summary>One autocommit statement guarded by the lease token; <paramref name="requireLive"/> also requires an unexpired lease.</summary>
    Task<bool> GuardedAsync(OutboxLease lease, string set, Action<NpgsqlParameterCollection> parameters, bool requireLive, CancellationToken ct) =>
        OnLane(_controlLane, async connection =>
        {
            using var update = new NpgsqlCommand($"""
                UPDATE runwick_outbox.outbox_messages SET {set}
                WHERE message_id = @m AND state = 'leased' AND lease_token = @token
                {(requireLive ? "AND lease_expires_at > clock_timestamp()" : "")}
                """, connection);
            update.Parameters.AddWithValue("m", NpgsqlDbType.Uuid, lease.MessageId);
            update.Parameters.AddWithValue("token", NpgsqlDbType.Uuid, lease.Token);
            parameters(update.Parameters);
            return await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, ct);

    async Task<T> OnLane<T>(SemaphoreSlim lane, Func<NpgsqlConnection, Task<T>> work, CancellationToken ct)
    {
        await lane.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var connection = await _data.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var connectionScope = connection.ConfigureAwait(false);
            return await work(connection).ConfigureAwait(false);
        }
        finally
        {
            lane.Release();
        }
    }

    static DateTimeOffset Utc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));
}
