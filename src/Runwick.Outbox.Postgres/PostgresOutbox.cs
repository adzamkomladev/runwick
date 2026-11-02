using Npgsql;
using NpgsqlTypes;

namespace Runwick.Outbox.Postgres;

/// <summary>
/// Stages a job on the caller's PostgreSQL transaction for later delivery (14 §2.1). Use it when business data and the
/// queue do not share one database; when they do, use native staging (<c>IPostgresJobDispatcher</c>) instead.
/// </summary>
public interface IPostgresOutbox
{
    /// <summary>
    /// Serializes and inserts the immutable intent on <paramref name="transaction"/>'s connection. It never contacts the
    /// destination, never commits, rolls back or retries the transaction, and returns no queue receipt: the message is
    /// delivered only after the caller commits. A rejection leaves the transaction as it was.
    /// </summary>
    Task<StagedOutboxMessage> StageAsync<TJob>(TJob job, NpgsqlTransaction transaction, OutboxStageOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : class, IQueueJob;
}

internal sealed class PostgresOutbox(OutboxStager stager) : IPostgresOutbox
{
    const string Savepoint = "runwick_outbox_stage";

    public async Task<StagedOutboxMessage> StageAsync<TJob>(TJob job, NpgsqlTransaction transaction, OutboxStageOptions? options = null,
        CancellationToken cancellationToken = default) where TJob : class, IQueueJob
    {
        ArgumentNullException.ThrowIfNull(transaction);

        // Everything that can fail without I/O happens before the first statement (DispatchValidationException).
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
        catch (Exception ex)
        {
            // Any failure, cancellation included, leaves the caller's transaction as it was before Stage (14 §2.1).
            await OutboxStager.RollBackToSavepointAsync(transaction, Savepoint).ConfigureAwait(false);
            if (ex is PostgresException { SqlState: "42P01" or "3F000" })
            {
                throw new OutboxStagingException("outbox_not_installed", "The outbox source schema is not installed (PostgresOutboxSchema.InstallAsync).", ex);
            }

            if (ex is PostgresException { SqlState: "25006" })
            {
                throw new OutboxStagingException("transaction_invalid", "The transaction is read-only.", ex);
            }

            throw;
        }
    }

    static async Task<StagedOutboxMessage> StageCoreAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, PreparedDispatch prepared,
        Guid messageId, TimeSpan horizon, CancellationToken ct)
    {
        Guid sourceStoreId;
        DateTimeOffset now;
        using (var meta = new NpgsqlCommand("""
            SELECT source_store_id, schema_version, clock_timestamp() FROM runwick_outbox.outbox_metadata WHERE singleton_id = 1
            """, connection, transaction))
        {
            var r = await meta.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using var rScope = r.ConfigureAwait(false);
            if (!await r.ReadAsync(ct).ConfigureAwait(false))
            {
                throw new OutboxStagingException("outbox_not_installed", "The outbox source schema has no metadata row.");
            }

            if (r.GetInt32(1) != PostgresOutboxSchema.Version)
            {
                throw new OutboxStagingException("outbox_schema_incompatible", $"Outbox source schema version {r.GetInt32(1)} is not {PostgresOutboxSchema.Version}.");
            }

            sourceStoreId = r.GetGuid(0);
            now = new DateTimeOffset(DateTime.SpecifyKind(r.GetDateTime(2), DateTimeKind.Utc));
        }

        var intent = OutboxStager.Freeze(sourceStoreId, messageId, prepared, now, horizon);
        // A new row also notifies the relay's wake channel (14 Changes 2026-11-02, PF-07); PostgreSQL delivers it only
        // when the caller's transaction commits.
        using (var insert = new NpgsqlCommand("""
            WITH ins AS (
            INSERT INTO runwick_outbox.outbox_messages (
                message_id, destination_store_id, destination_application, intent, business_digest, policy_digest,
                delivery_fingerprint, proposed_job_id, staged_at, accept_until, next_attempt_at)
            VALUES (@m, @store, @app, @intent, @business, @policy, @delivery, @job, @staged, @accept, @staged)
            ON CONFLICT (message_id) DO NOTHING
            RETURNING true AS inserted),
            n AS (SELECT pg_notify('runwick_outbox', '') FROM ins)
            SELECT ins.inserted FROM ins WHERE EXISTS (SELECT FROM n)
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue("m", NpgsqlDbType.Uuid, intent.MessageId);
            insert.Parameters.AddWithValue("store", NpgsqlDbType.Uuid, intent.DestinationStoreId);
            insert.Parameters.AddWithValue("app", NpgsqlDbType.Text, intent.DestinationApplication);
            insert.Parameters.AddWithValue("intent", NpgsqlDbType.Bytea, intent.Envelope);
            insert.Parameters.AddWithValue("business", NpgsqlDbType.Bytea, intent.BusinessDigest);
            insert.Parameters.AddWithValue("policy", NpgsqlDbType.Bytea, intent.PolicyDigest);
            insert.Parameters.AddWithValue("delivery", NpgsqlDbType.Bytea, intent.DeliveryFingerprint);
            insert.Parameters.AddWithValue("job", NpgsqlDbType.Uuid, intent.ProposedJobId);
            insert.Parameters.AddWithValue("staged", NpgsqlDbType.TimestampTz, intent.StagedAt.UtcDateTime);
            insert.Parameters.AddWithValue("accept", NpgsqlDbType.TimestampTz, intent.AcceptUntil.UtcDateTime);
            if (await insert.ExecuteScalarAsync(ct).ConfigureAwait(false) is true)
            {
                return OutboxStager.Result(sourceStoreId, intent, existing: false);
            }
        }

        // The message ID exists (committed, or committed by the concurrent writer we waited on): same intent or conflict.
        using var read = new NpgsqlCommand("""
            SELECT destination_store_id, destination_application, business_digest, policy_digest, delivery_fingerprint,
                   proposed_job_id, staged_at, accept_until, intent
            FROM runwick_outbox.outbox_messages WHERE message_id = @m
            """, connection, transaction);
        read.Parameters.AddWithValue("m", NpgsqlDbType.Uuid, messageId);
        var row = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await using var rowScope = row.ConfigureAwait(false);
        if (!await row.ReadAsync(ct).ConfigureAwait(false)
            || !OutboxStager.SameIntent(intent, row.GetGuid(0), row.GetString(1), (byte[])row[2], (byte[])row[3]))
        {
            throw new OutboxMessageConflictException(messageId);
        }

        var stored = new OutboxIntent(messageId, row.GetGuid(0), row.GetString(1), (byte[])row[8], (byte[])row[2], (byte[])row[3], (byte[])row[4],
            row.GetGuid(5), Utc(row.GetDateTime(6)), Utc(row.GetDateTime(7)));
        return OutboxStager.Result(sourceStoreId, stored, existing: true);
    }

    static DateTimeOffset Utc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));
}
