using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Runwick.Outbox.Sqlite;

/// <summary>
/// The relay's view of one same-host SQLite source (14 §4). Claims run in <c>BEGIN IMMEDIATE</c> transactions so two
/// relay processes on one file serialize; times come from SQLite's own clock in epoch milliseconds.
/// </summary>
internal sealed class SqliteOutboxSource(string connectionString) : IOutboxSource, IDisposable
{
    readonly SemaphoreSlim _claimLane = new(1, 1);
    readonly SemaphoreSlim _controlLane = new(1, 1);
    Guid? _sourceStoreId;

    public Guid SourceStoreId => _sourceStoreId ?? throw new InvalidOperationException("The outbox source is not initialized.");

    internal static string Text(Guid value) => value.ToString("D", CultureInfo.InvariantCulture);

    internal static async Task<(Guid Id, int Version)> ReadMetadataAsync(SqliteConnection connection, SqliteTransaction? tx, CancellationToken ct)
    {
        using var read = new SqliteCommand("SELECT source_store_id, schema_version FROM runwick_outbox_metadata WHERE singleton_id = 1", connection, tx);
        var r = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using var rScope = r.ConfigureAwait(false);
        return await r.ReadAsync(ct).ConfigureAwait(false)
            ? (Guid.Parse(r.GetString(0), CultureInfo.InvariantCulture), r.GetInt32(1))
            : throw new OutboxStagingException("outbox_not_installed", "The outbox source schema has no metadata row.");
    }

    /// <summary>Source time (14 §6.1): SQLite's clock, millisecond floor.</summary>
    internal static async Task<DateTimeOffset> NowAsync(SqliteConnection connection, SqliteTransaction? tx, CancellationToken ct)
    {
        using var now = new SqliteCommand("SELECT CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)", connection, tx);
        return DateTimeOffset.FromUnixTimeMilliseconds((long)(await now.ExecuteScalarAsync(ct).ConfigureAwait(false))!);
    }

    public async Task InitializeAsync(CancellationToken ct)
    {
        var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var connectionScope = connection.ConfigureAwait(false);
        var (id, version) = await ReadMetadataAsync(connection, null, ct).ConfigureAwait(false);
        if (!SqliteOutboxSchema.Readable(version))
        {
            throw new OutboxStagingException("outbox_schema_incompatible", $"Outbox source schema version {version} is not {SqliteOutboxSchema.Version}.");
        }

        _sourceStoreId = id;
    }

    public Task<IReadOnlyList<OutboxLease>> ClaimAsync(Guid owner, int max, TimeSpan leaseDuration, CancellationToken ct) =>
        ClaimCoreAsync(owner, max, leaseDuration, null, null, ct);

    public Task<IReadOnlyList<OutboxLease>> ClaimWithTransportAsync(Guid owner, int max, TimeSpan leaseDuration, DateTimeOffset transportAsOf,
        DateTimeOffset transportBound, CancellationToken ct) => ClaimCoreAsync(owner, max, leaseDuration, transportAsOf, transportBound, ct);

    async Task<IReadOnlyList<OutboxLease>> ClaimCoreAsync(Guid owner, int max, TimeSpan leaseDuration, DateTimeOffset? transportAsOf,
        DateTimeOffset? transportBound, CancellationToken ct)
    {
        await _claimLane.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var connection = await OpenAsync(ct).ConfigureAwait(false);
            await using var connectionScope = connection.ConfigureAwait(false);
            var tx = (SqliteTransaction)await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            await using var txScope = tx.ConfigureAwait(false);
            var now = (await NowAsync(connection, tx, ct).ConfigureAwait(false)).ToUnixTimeMilliseconds();
            var ids = new List<string>();
            using (var due = new SqliteCommand("""
                SELECT message_id FROM runwick_outbox_messages
                WHERE (state = 'pending' AND next_attempt_at <= $now) OR (state = 'leased' AND lease_expires_at <= $now)
                ORDER BY next_attempt_at
                LIMIT $max
                """, connection, tx))
            {
                due.Parameters.AddWithValue("$now", now);
                due.Parameters.AddWithValue("$max", max);
                var r = await due.ExecuteReaderAsync(ct).ConfigureAwait(false);
                await using var rScope = r.ConfigureAwait(false);
                while (await r.ReadAsync(ct).ConfigureAwait(false))
                {
                    ids.Add(r.GetString(0));
                }
            }

            var leases = new List<OutboxLease>(ids.Count);
            foreach (var id in ids)
            {
                var token = Guid.NewGuid();
                using var claim = new SqliteCommand("""
                    UPDATE runwick_outbox_messages
                    SET state = 'leased', lease_owner = $owner, lease_token = $token, lease_expires_at = $expires,
                        attempts = attempts + 1, revision = revision + 1,
                        transport_command_id = CASE
                          WHEN min($transport_bound, accept_until) > $transport_as_of
                           AND (transport_command_id IS NULL OR transport_not_after IS NULL OR transport_not_after <= $transport_as_of)
                          THEN $transport_command ELSE transport_command_id END,
                        transport_not_after = CASE
                          WHEN min($transport_bound, accept_until) > $transport_as_of
                           AND (transport_command_id IS NULL OR transport_not_after IS NULL OR transport_not_after <= $transport_as_of)
                          THEN min($transport_bound, accept_until) ELSE transport_not_after END
                    WHERE message_id = $m
                    RETURNING message_id, destination_store_id, destination_application, intent, delivery_fingerprint, staged_at,
                              accept_until, attempts, transport_command_id, transport_not_after
                    """, connection, tx);
                claim.Parameters.AddWithValue("$owner", Text(owner));
                claim.Parameters.AddWithValue("$token", Text(token));
                claim.Parameters.AddWithValue("$expires", now + (long)leaseDuration.TotalMilliseconds);
                claim.Parameters.AddWithValue("$m", id);
                claim.Parameters.AddWithValue("$transport_as_of", (object?)transportAsOf?.ToUnixTimeMilliseconds() ?? DBNull.Value);
                claim.Parameters.AddWithValue("$transport_bound", (object?)transportBound?.ToUnixTimeMilliseconds() ?? DBNull.Value);
                claim.Parameters.AddWithValue("$transport_command", Text(Guid.CreateVersion7()));
                var r = await claim.ExecuteReaderAsync(ct).ConfigureAwait(false);
                await using var rScope = r.ConfigureAwait(false);
                if (await r.ReadAsync(ct).ConfigureAwait(false))
                {
                    leases.Add(new OutboxLease(Guid.Parse(r.GetString(0), CultureInfo.InvariantCulture), token,
                        Guid.Parse(r.GetString(1), CultureInfo.InvariantCulture), r.GetString(2), (byte[])r[3], (byte[])r[4],
                        DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(5)), DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(6)), r.GetInt32(7),
                        await r.IsDBNullAsync(8, ct).ConfigureAwait(false) ? null : Guid.Parse(r.GetString(8), CultureInfo.InvariantCulture),
                        await r.IsDBNullAsync(9, ct).ConfigureAwait(false) ? null : DateTimeOffset.FromUnixTimeMilliseconds(r.GetInt64(9))));
                }
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);
            return leases;
        }
        finally
        {
            _claimLane.Release();
        }
    }

    public Task<bool> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken ct) =>
        GuardedAsync(lease, "lease_expires_at = $now + $lease", p => p.AddWithValue("$lease", (long)leaseDuration.TotalMilliseconds), requireLive: true, ct);

    public Task<bool> RecordTransportAsync(OutboxLease lease, Guid commandId, DateTimeOffset notAfter, CancellationToken ct) =>
        GuardedAsync(lease, "transport_command_id = $command, transport_not_after = $not_after, revision = revision + 1", p =>
        {
            p.AddWithValue("$command", Text(commandId));
            p.AddWithValue("$not_after", notAfter.ToUnixTimeMilliseconds());
        }, requireLive: true, ct);

    public Task<bool> MarkDeliveredAsync(OutboxLease lease, OutboxDelivered delivered, CancellationToken ct) =>
        GuardedAsync(lease, """
            state = 'delivered', delivered_job_id = $job, delivered_disposition = $disposition, confirmed_profile = $profile,
            delivered_at = $delivered_at, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, last_error = NULL,
            revision = revision + 1
            """, p =>
        {
            p.AddWithValue("$job", Text(delivered.JobId));
            p.AddWithValue("$disposition", delivered.Disposition);
            p.AddWithValue("$profile", delivered.ConfirmedProfile);
            p.AddWithValue("$delivered_at", delivered.DeliveredAt.ToUnixTimeMilliseconds());
        }, requireLive: false, ct);

    public Task<bool> RetryLaterAsync(OutboxLease lease, TimeSpan delay, string error, CancellationToken ct) =>
        GuardedAsync(lease, """
            state = 'pending', next_attempt_at = $now + $delay, last_error = substr($error, 1, 256),
            lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, revision = revision + 1
            """, p =>
        {
            p.AddWithValue("$delay", (long)delay.TotalMilliseconds);
            p.AddWithValue("$error", error);
        }, requireLive: false, ct);

    public Task<bool> BlockAsync(OutboxLease lease, string reason, CancellationToken ct) =>
        GuardedAsync(lease, """
            state = 'blocked', blocked_reason = substr($reason, 1, 256), lease_owner = NULL, lease_token = NULL,
            lease_expires_at = NULL, revision = revision + 1
            """, p => p.AddWithValue("$reason", reason), requireLive: false, ct);

    /// <summary>17 §6 on SQLite: one write transaction serializes cleanup with claims, so nothing else is locked out.</summary>
    public async Task<int> PurgeDeliveredAsync(int maxRows, long maxBytes, CancellationToken ct)
    {
        await _claimLane.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var connection = await OpenAsync(ct).ConfigureAwait(false);
            await using var connectionScope = connection.ConfigureAwait(false);
            var tx = (SqliteTransaction)await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            await using var txScope = tx.ConfigureAwait(false);
            var now = (await NowAsync(connection, tx, ct).ConfigureAwait(false)).ToUnixTimeMilliseconds();
            using var purge = new SqliteCommand("""
                DELETE FROM runwick_outbox_messages WHERE message_id IN (
                    SELECT message_id FROM (
                        SELECT message_id, sum(bytes) OVER (ORDER BY delivered_at, message_id) - bytes AS before, bytes FROM (
                            SELECT message_id, delivered_at, length(intent) AS bytes FROM runwick_outbox_messages
                            WHERE state = 'delivered' AND delivered_at < $now - 604800000 AND accept_until + 86400000 < $now
                            ORDER BY delivered_at, message_id
                            LIMIT $max))
                    WHERE before = 0 OR before + bytes <= $bytes)
                """, connection, tx);
            purge.Parameters.AddWithValue("$now", now);
            purge.Parameters.AddWithValue("$max", maxRows);
            purge.Parameters.AddWithValue("$bytes", maxBytes);
            var deleted = await purge.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            await tx.CommitAsync(ct).ConfigureAwait(false);
            return deleted;
        }
        finally
        {
            _claimLane.Release();
        }
    }

    public void Dispose()
    {
        _claimLane.Dispose();
        _controlLane.Dispose();
    }

    async Task<bool> GuardedAsync(OutboxLease lease, string set, Action<SqliteParameterCollection> parameters, bool requireLive, CancellationToken ct)
    {
        await _controlLane.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var connection = await OpenAsync(ct).ConfigureAwait(false);
            await using var connectionScope = connection.ConfigureAwait(false);
            var tx = (SqliteTransaction)await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            await using var txScope = tx.ConfigureAwait(false);
            var now = (await NowAsync(connection, tx, ct).ConfigureAwait(false)).ToUnixTimeMilliseconds();
            using var update = new SqliteCommand($"""
                UPDATE runwick_outbox_messages SET {set}
                WHERE message_id = $m AND state = 'leased' AND lease_token = $token
                {(requireLive ? "AND lease_expires_at > $now" : "")}
                """, connection, tx);
            update.Parameters.AddWithValue("$m", Text(lease.MessageId));
            update.Parameters.AddWithValue("$token", Text(lease.Token));
            update.Parameters.AddWithValue("$now", now);
            parameters(update.Parameters);
            var changed = await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
            await tx.CommitAsync(ct).ConfigureAwait(false);
            return changed;
        }
        finally
        {
            _controlLane.Release();
        }
    }

    async Task<SqliteConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new SqliteConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
