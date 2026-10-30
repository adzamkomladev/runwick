using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>Harness-only operations (04 §7.14–7.16). Not reachable from the public API in M2.</summary>
internal sealed partial class PgStore
{
    internal sealed record QueueState(string Queue, bool Paused, long Revision);

    /// <summary>SetQueueState/1 (04 §7.14).</summary>
    internal Task<StoreReply<QueueState>> SetQueueStateAsync(string application, string queue, bool paused, long? expectedRevision,
        Guid commandId, DateTimeOffset notAfter, CancellationToken ct)
    {
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, application, Operations.SetQueueState, commandId, notAfter,
            CommandEnvelopes.SetQueueState(queue, paused, expectedRevision));
        return runner.ExecuteAsync(
            Op(application, commandId, Operations.SetQueueState, notAfter, fingerprint, PgLane.Background),
            async tx =>
            {
                using var q = tx.Sql("""
                    SELECT paused, revision FROM runwick.queue_controls
                    WHERE application = @application AND queue = @queue
                    FOR UPDATE
                    """).P("queue", queue);
                var rows = await tx.QueryAsync(q, r => r.GetInt64(1)).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<QueueState>.Rollback(tx.Reject<QueueState>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (rows.Count == 0)
                {
                    return new PgOutcome<QueueState>.Rollback(tx.Reject<QueueState>(StoreStatus.NotFound, "queue_not_registered"));
                }

                if (expectedRevision is { } expected && expected != rows[0])
                {
                    return new PgOutcome<QueueState>.Rollback(new StoreReply<QueueState>(StoreStatus.VersionConflict, StoreEffect.ObservedNoEffect, false,
                        null, new StoreError("version_conflict", ActualRevision: rows[0]), tx.Now));
                }

                using var u = tx.Sql("""
                    UPDATE runwick.queue_controls
                    SET paused = @paused, revision = revision + 1, updated_at = @now, updated_by = 'harness'
                    WHERE application = @application AND queue = @queue
                    """).P("paused", paused).P("now", tx.Now).P("queue", queue);
                await tx.ExecAsync(u).ConfigureAwait(false);
                var state = new QueueState(queue, paused, rows[0] + 1);
                return new PgOutcome<QueueState>.Commit(StoreReply<QueueState>.Applied(state, tx.Now),
                    new JsonObject { ["queue"] = queue, ["paused"] = paused, ["revision"] = state.Revision });
            },
            (tx, row) => Task.FromResult(StoreReply<QueueState>.Applied(new QueueState(row.Result!["queue"]!.GetValue<string>(),
                row.Result["paused"]!.GetValue<bool>(), row.Result["revision"]!.GetValue<long>()), row.DecidedAt!.Value, replayed: true)),
            ct);
    }
}
