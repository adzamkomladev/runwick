using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

internal sealed partial class SqliteStore
{
    // ------------------------------------------------------------------ ManagementDispatch/1 (19 §6.2, 20 §7)

    /// <summary>
    /// Gates (creation path) → receipt → the shared dispatch core → the <c>dispatched</c> event gains the actor → one
    /// <c>Dispatch</c> audit row → receipt, in one write transaction. A <c>reused</c> result is audited too; a rejection
    /// rolls back with no receipt and no audit.
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.ManagementDispatch, command.NotAfter, fp, SqLane.Producer, storeId),
            tx =>
            {
                var core = DispatchCore(tx, [r.Item], continueOnConflict: false, "initial");
                if (core.Reject is { } reject)
                {
                    return new SqOutcome<ManagementDispatched>.Rollback(reject.Reject<ManagementDispatched>());
                }

                var d = ReceiptCodec.ReadDispatch(JsonNode.Parse(core.Items![0]!.ToJsonString())!);
                var job = tx.Query(tx.Sql("SELECT generation, revision, queue FROM runwick_jobs WHERE application = @application AND job_id = @job_id")
                    .P("job_id", d.JobId), x => (Generation: x.GetInt32(0), Revision: x.GetInt64(1), Queue: x.GetString(2)))[0];
                var value = new ManagementDispatched(d, job.Generation, job.Revision, job.Queue);

                tx.Exec(tx.Sql("""
                    UPDATE runwick_queue_events SET actor_id = @actor
                    WHERE application = @application AND command_id = @command_id AND job_id = @job_id AND event_kind = 'dispatched'
                    """).P("actor", r.Actor.Subject).P("command_id", tx.Op.CommandId).P("job_id", d.JobId));
                Audit(tx, "job", d.JobId.ToString("D"), r.Actor, null, job.Revision, new JsonObject
                {
                    ["jobName"] = r.Item.Intent.JobName, ["queue"] = job.Queue,
                    ["disposition"] = d.Disposition == DispatchDisposition.Created ? "created" : "reused",
                }, "Dispatch");
                return new SqOutcome<ManagementDispatched>.Commit(StoreReply<ManagementDispatched>.Applied(value, tx.Now), ManagementDispatchReceipt(value));
            },
            (tx, row) => StoreReply<ManagementDispatched>.Applied(ReadManagementDispatch(row.Result!), row.DecidedAt!.Value, replayed: true),
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
