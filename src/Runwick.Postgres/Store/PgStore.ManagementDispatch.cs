using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    // ------------------------------------------------------------------ ManagementDispatch/1 (19 §6.2, schema revision 17)

    /// <summary>
    /// Gates (creation path) → receipt → the shared dispatch core (runwick.rw_dispatch_core_v2, unchanged) → the
    /// <c>dispatched</c> event gains the actor → one <c>Dispatch</c> audit row → receipt, in one transaction. A
    /// <c>reused</c> result is audited too; a rejection rolls back with no receipt and no audit.
    /// </summary>
    public async Task<StoreReply<ManagementDispatched>> ManagementDispatchAsync(StoreCommand<ManagementDispatchStoreRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return StoreReply<ManagementDispatched>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.ManagementDispatch, command.CommandId, command.NotAfter,
            CommandEnvelopes.ManagementDispatch(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.ManagementDispatch, command.NotAfter, fp, PgLane.Producer, storeId),
            async tx =>
            {
                var outcome = await DispatchCoreAsync(tx, [r.Item], continueOnConflict: false).ConfigureAwait(false);
                if (outcome.Rejection is { } rejection)
                {
                    return new PgOutcome<ManagementDispatched>.Rollback(StoreReply<ManagementDispatched>.NoEffect(rejection.Status, rejection.Error, tx.Now));
                }

                var d = outcome.Items[0].Result!;
                using var q = tx.Sql("SELECT generation, revision, queue FROM runwick.jobs WHERE application = @application AND job_id = @job_id")
                    .P("job_id", d.JobId);
                var job = (await tx.QueryAsync(q, x => (Generation: x.GetInt32(0), Revision: x.GetInt64(1), Queue: x.GetString(2))).ConfigureAwait(false))[0];
                var value = new ManagementDispatched(d, job.Generation, job.Revision, job.Queue);
                var disposition = d.Disposition == DispatchDisposition.Created ? "created" : "reused";

                using (var e = tx.Sql("""
                    UPDATE runwick.queue_events SET actor_id = @actor
                    WHERE application = @application AND command_id = @command_id AND job_id = @job_id AND event_kind = 'dispatched'
                    """).P("actor", r.Actor.Subject).P("command_id", tx.Op.CommandId).P("job_id", d.JobId))
                {
                    await tx.ExecAsync(e).ConfigureAwait(false);
                }

                await AuditAsync(tx, "job", d.JobId.ToString("D"), r.Actor, null, job.Revision,
                    new JsonObject { ["jobName"] = r.Item.Intent.JobName, ["queue"] = job.Queue, ["disposition"] = disposition }, "Dispatch").ConfigureAwait(false);
                return new PgOutcome<ManagementDispatched>.Commit(StoreReply<ManagementDispatched>.Applied(value, tx.Now), ManagementDispatchReceipt(value));
            },
            (tx, row) => Task.FromResult(StoreReply<ManagementDispatched>.Applied(ReadManagementDispatch(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    /// <summary>The Dispatch/1 receipt result plus the job's position and queue.</summary>
    static JsonObject ManagementDispatchReceipt(ManagementDispatched v)
    {
        var o = ReceiptCodec.Dispatch(v.Dispatch);
        o["generation"] = v.Generation;
        o["revision"] = v.Revision;
        o["queue"] = v.Queue;
        return o;
    }

    static ManagementDispatched ReadManagementDispatch(JsonNode o) =>
        new(ReceiptCodec.ReadDispatch(o), o["generation"]!.GetValue<int>(), o["revision"]!.GetValue<long>(), o["queue"]!.GetValue<string>());
}
