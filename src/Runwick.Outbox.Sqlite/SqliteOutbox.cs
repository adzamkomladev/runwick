using System.Globalization;
using System.Reflection;
using Microsoft.Data.Sqlite;

namespace Runwick.Outbox.Sqlite;

/// <summary>
/// Stages a job on the caller's same-host SQLite transaction for later delivery to a Runwick destination (14 §2.1).
/// SQLite here is an outbox source only, never a queue provider (SP-019).
/// </summary>
public interface ISqliteOutbox
{
    /// <summary>
    /// Inserts the immutable intent on <paramref name="transaction"/>'s connection inside a savepoint. It never contacts
    /// the destination and never commits, rolls back or retries the transaction.
    /// </summary>
    Task<StagedOutboxMessage> StageAsync<TJob>(TJob job, SqliteTransaction transaction, OutboxStageOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : class, IQueueJob;
}

/// <summary>Installs the SQLite outbox source schema (14 §3.3).</summary>
public static class SqliteOutboxSchema
{
    public const int Version = 1;

    internal static string Sql { get; } = Load("source_v1.sql");

    /// <summary>Creates the tables if absent and returns the (stable) source UUID. Idempotent.</summary>
    public static async Task<Guid> InstallAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        var connection = new SqliteConnection(connectionString);
        await using var connectionScope = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var tx = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var txScope = tx.ConfigureAwait(false);
        using (var ddl = new SqliteCommand(Sql, connection, tx))
        {
            await ddl.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        using (var seed = new SqliteCommand("""
            INSERT INTO runwick_outbox_metadata (singleton_id, source_store_id, schema_version, created_at)
            VALUES (1, $id, 1, CAST(ROUND((julianday('now') - 2440587.5) * 86400000.0) AS INTEGER))
            ON CONFLICT (singleton_id) DO NOTHING
            """, connection, tx))
        {
            seed.Parameters.AddWithValue("$id", SqliteOutboxSource.Text(Guid.NewGuid()));
            await seed.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var (id, version) = await SqliteOutboxSource.ReadMetadataAsync(connection, tx, cancellationToken).ConfigureAwait(false);
        if (version != Version)
        {
            throw new OutboxStagingException("outbox_schema_incompatible", $"Outbox source schema version {version} is not {Version}.");
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        return id;
    }

    static string Load(string name)
    {
        using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Runwick.Outbox.Sqlite.Sql." + name)
            ?? throw new InvalidOperationException($"Embedded SQL {name} is missing.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}

internal sealed class SqliteOutbox(OutboxStager stager) : ISqliteOutbox
{
    const string Savepoint = "runwick_outbox_stage";

    public async Task<StagedOutboxMessage> StageAsync<TJob>(TJob job, SqliteTransaction transaction, OutboxStageOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : class, IQueueJob
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var prepared = stager.Prepare(job, options);
        var horizon = OutboxStager.Horizon(options);
        var messageId = options?.MessageId ?? Guid.CreateVersion7();
        var connection = transaction.Connection
            ?? throw new OutboxStagingException("transaction_invalid", "The transaction is completed or has no connection.");

        try
        {
            await transaction.SaveAsync(Savepoint, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex)
        {
            throw new OutboxStagingException("transaction_invalid", ex.Message, ex);
        }

        try
        {
            var result = await StageCoreAsync(connection, transaction, prepared, messageId, horizon, cancellationToken).ConfigureAwait(false);
            await transaction.ReleaseAsync(Savepoint, cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch (Exception ex) when (ex is SqliteException or OutboxMessageConflictException or OutboxStagingException)
        {
            await transaction.RollbackAsync(Savepoint, cancellationToken).ConfigureAwait(false);
            if (ex is SqliteException { SqliteErrorCode: 1 } && ex.Message.Contains("no such table", StringComparison.Ordinal))
            {
                throw new OutboxStagingException("outbox_not_installed", "The outbox source schema is not installed (SqliteOutboxSchema.InstallAsync).", ex);
            }

            throw;
        }
    }

    static async Task<StagedOutboxMessage> StageCoreAsync(SqliteConnection connection, SqliteTransaction transaction, PreparedDispatch prepared,
        Guid messageId, TimeSpan horizon, CancellationToken ct)
    {
        var (sourceStoreId, version) = await SqliteOutboxSource.ReadMetadataAsync(connection, transaction, ct).ConfigureAwait(false);
        if (version != SqliteOutboxSchema.Version)
        {
            throw new OutboxStagingException("outbox_schema_incompatible", $"Outbox source schema version {version} is not {SqliteOutboxSchema.Version}.");
        }

        var now = await SqliteOutboxSource.NowAsync(connection, transaction, ct).ConfigureAwait(false);
        var intent = OutboxStager.Freeze(sourceStoreId, messageId, prepared, now, horizon);
        using (var insert = new SqliteCommand("""
            INSERT INTO runwick_outbox_messages (
                message_id, destination_store_id, destination_application, intent, business_digest, policy_digest,
                delivery_fingerprint, proposed_job_id, staged_at, accept_until, next_attempt_at)
            VALUES ($m, $store, $app, $intent, $business, $policy, $delivery, $job, $staged, $accept, $staged)
            ON CONFLICT (message_id) DO NOTHING
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue("$m", SqliteOutboxSource.Text(intent.MessageId));
            insert.Parameters.AddWithValue("$store", SqliteOutboxSource.Text(intent.DestinationStoreId));
            insert.Parameters.AddWithValue("$app", intent.DestinationApplication);
            insert.Parameters.AddWithValue("$intent", intent.Envelope);
            insert.Parameters.AddWithValue("$business", intent.BusinessDigest);
            insert.Parameters.AddWithValue("$policy", intent.PolicyDigest);
            insert.Parameters.AddWithValue("$delivery", intent.DeliveryFingerprint);
            insert.Parameters.AddWithValue("$job", SqliteOutboxSource.Text(intent.ProposedJobId));
            insert.Parameters.AddWithValue("$staged", intent.StagedAt.ToUnixTimeMilliseconds());
            insert.Parameters.AddWithValue("$accept", intent.AcceptUntil.ToUnixTimeMilliseconds());
            if (await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1)
            {
                return OutboxStager.Result(sourceStoreId, intent, existing: false);
            }
        }

        using var read = new SqliteCommand("""
            SELECT destination_store_id, destination_application, business_digest, policy_digest, delivery_fingerprint,
                   proposed_job_id, staged_at, accept_until, intent
            FROM runwick_outbox_messages WHERE message_id = $m
            """, connection, transaction);
        read.Parameters.AddWithValue("$m", SqliteOutboxSource.Text(messageId));
        var row = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using var rowScope = row.ConfigureAwait(false);
        if (!await row.ReadAsync(ct).ConfigureAwait(false)
            || !OutboxStager.SameIntent(intent, Guid.Parse(row.GetString(0), CultureInfo.InvariantCulture), row.GetString(1), (byte[])row[2], (byte[])row[3]))
        {
            throw new OutboxMessageConflictException(messageId);
        }

        var stored = new OutboxIntent(messageId, Guid.Parse(row.GetString(0), CultureInfo.InvariantCulture), row.GetString(1), (byte[])row[8],
            (byte[])row[2], (byte[])row[3], (byte[])row[4], Guid.Parse(row.GetString(5), CultureInfo.InvariantCulture),
            DateTimeOffset.FromUnixTimeMilliseconds(row.GetInt64(6)), DateTimeOffset.FromUnixTimeMilliseconds(row.GetInt64(7)));
        return OutboxStager.Result(sourceStoreId, stored, existing: true);
    }
}
