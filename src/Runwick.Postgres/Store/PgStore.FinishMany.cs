using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Worker;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IFinishManyStore
{
    // ------------------------------------------------------------------ FinishMany/1 (04 §7.19, PF-06 batched finish)

    /// <summary>
    /// Settles up to 32 Finish/1 commands in one transaction (runwick.finish_many_v2). Each item keeps its own command ID,
    /// NotAfter and fingerprint (exactly the ones Finish/1 would send for it) and gets its own durable Finish receipt, so
    /// a single Finish/1 resubmission of an item replays what the batch recorded for it. Items are validated here as
    /// Finish/1 validates its request; an item Finish/1 would refuse before the call makes the whole call InvalidRequest.
    /// </summary>
    public Task<StoreReply<FinishedMany>> FinishManyAsync(StoreCommand<FinishManyRequest> command, CancellationToken ct)
    {
        var items = command.Body.Items;
        if (items.Count is 0 or > IFinishManyStore.MaxItems
            || items.Any(i => i.Application != command.Application || i.Body.RequestedRetryDelay < TimeSpan.Zero)
            || items.Select(i => i.CommandId).Distinct().Count() != items.Count
            || items.Select(i => i.Body.Lease.JobId).Distinct().Count() != items.Count)
        {
            return Task.FromResult(StoreReply<FinishedMany>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request"), time.GetUtcNow()));
        }

        // Gates; each item's receipt (R1-R3) in command-ID order; J (UUID order), K (terminal plans), S (UUID order); one
        // fresh clock; then runs of plain succeeded items set-based (rw_finish_plain_v1) and every other item through
        // rw_finish_job_v1 and its own R4, in request order, run in runwick.finish_many_v2.
        var fingerprints = items.Select(i => CommandEnvelopes.Fingerprint(StoreId, i.Application, Operations.Finish, i.CommandId, i.NotAfter,
            CommandEnvelopes.Finish(i.Body))).ToArray();
        var entropy = new long[items.Count];
        System.Security.Cryptography.RandomNumberGenerator.Fill(System.Runtime.InteropServices.MemoryMarshal.AsBytes(entropy.AsSpan()));
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.FinishMany, command.NotAfter, null, PgLane.Settlement),
            new PgCall("finish_many_v3")
                .Arr("p_command_ids", [.. items.Select(i => i.CommandId)], NpgsqlDbType.Uuid)
                .Arr("p_not_afters", [.. items.Select(i => PgTime.Floor(i.NotAfter).UtcDateTime)], NpgsqlDbType.TimestampTz)
                .Arr("p_fingerprints", fingerprints, NpgsqlDbType.Bytea)
                .Arr("p_job_ids", [.. items.Select(i => i.Body.Lease.JobId)], NpgsqlDbType.Uuid)
                .Arr("p_attempt_ids", [.. items.Select(i => i.Body.Lease.AttemptId)], NpgsqlDbType.Uuid)
                .Arr("p_worker_ids", [.. items.Select(i => i.Body.Lease.WorkerInstanceId)], NpgsqlDbType.Uuid)
                .Arr("p_tokens", [.. items.Select(i => i.Body.Lease.OwnershipToken)], NpgsqlDbType.Uuid)
                .Arr("p_generations", [.. items.Select(i => i.Body.Lease.Generation)], NpgsqlDbType.Integer)
                .Arr("p_outcomes", [.. items.Select(i => Snake(i.Body.Outcome))], NpgsqlDbType.Text)
                .Arr("p_elapsed_ms", [.. items.Select(i => (long)Math.Max(0, i.Body.ExecutionElapsed.TotalMilliseconds))], NpgsqlDbType.Bigint)
                .Arr("p_requested_delays_ms", [.. items.Select(i => i.Body.RequestedRetryDelay is { } d ? (long?)Math.Max(1, CeilMs(d)) : null)], NpgsqlDbType.Bigint)
                .Arr("p_errors", [.. items.Select(i => ErrorJson(i.Body.Error)?.ToJsonString())], NpgsqlDbType.Jsonb)
                .Arr("p_entropies", entropy, NpgsqlDbType.Bigint)
                .Arr("p_usages", [.. items.Select(i => i.Body.Usage is { } u ? ResourceUsageJson.Write(u) : null)], NpgsqlDbType.Jsonb),
            row => row.HasValue ? new StoreReply<FinishedMany>(StoreStatus.Applied, StoreEffect.Applied, false, ReadFinishedMany(row), null, row.Decided)
                : row.Reject<FinishedMany>(),
            ct);
    }

    /// <summary>One <see cref="StoreReply{FinishedAttempt}"/> per item, exactly as <see cref="MapFinished"/> reads a Finish/1 row.</summary>
    static FinishedMany ReadFinishedMany(PgRow row)
    {
        var result = row.Result;
        if (row.FinishedManyResult is { } utf8)
        {
            if (FinishedManyReplyReader.TryRead(utf8) is { } finished)
            {
                return finished;
            }

            result = JsonNode.Parse(utf8)!.AsObject();
        }

        var outcomes = new List<StoreReply<FinishedAttempt>>();
        foreach (var node in result!["items"]!.AsArray())
        {
            var item = node!.AsObject();
            var status = Enum.Parse<StoreStatus>(item["status"]!.GetValue<string>());
            var applied = item["effect"]!.GetValue<string>() == nameof(StoreEffect.Applied);
            var decided = item["decidedAt"] is { } ms ? PgTime.FromMs(ms.GetValue<long>()) : default;
            var code = item["code"]?.GetValue<string>();
            outcomes.Add(applied
                ? new StoreReply<FinishedAttempt>(status, StoreEffect.Applied, item["replayed"]!.GetValue<bool>(),
                    ReceiptCodec.ReadFinished(item["result"]!.AsObject()), code is null ? null : new StoreError(code), decided)
                : StoreReply<FinishedAttempt>.NoEffect(status, new StoreError(code ?? "invariant_fault"), decided));
        }

        return new FinishedMany(outcomes);
    }
}
