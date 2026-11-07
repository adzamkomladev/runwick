using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : IReserveAndBeginStore
{
    // ------------------------------------------------------------------ ReserveAndBegin/1 (04 §7.18, PF-06 batched claim)

    public Task<StoreReply<ClaimedJobs>> ReserveAndBeginAsync(StoreCommand<ReserveAndBeginRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var contracts = request.SupportedContracts.Distinct().ToArray();
        if (request.LeaseDuration < MinLease || request.LeaseDuration > MaxLease || contracts.Length is < 1 or > 512
            || request.MaxJobs is < 1 or > IReserveAndBeginStore.MaxJobs)
        {
            return Task.FromResult(StoreReply<ClaimedJobs>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request"), time.GetUtcNow()));
        }

        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.ReserveAndBegin, command.CommandId, command.NotAfter,
            CommandEnvelopes.ReserveAndBegin(request));

        // Q, C (up to MaxJobs candidates, SKIP LOCKED), K/S/R across the claimed jobs in lock order, one fresh clock, then
        // runs of plain jobs set-based (rw_claim_plain_v1) and every other job through rw_reserve_job_v1 and rw_begin_job_v1, in
        // candidate order, and one R4 for the command, run in runwick.reserve_and_begin_v2.
        // One ownership token and one defer jitter per possible job are drawn here and sent with the call.
        var tokens = new Guid[request.MaxJobs];
        var jitters = new int[request.MaxJobs];
        for (var i = 0; i < tokens.Length; i++)
        {
            tokens[i] = NewToken();
            jitters[i] = Random.Shared.Next(0, MaxDeferJitterMs + 1);
        }

        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.ReserveAndBegin, command.NotAfter, fingerprint, PgLane.Admission),
            new PgCall("reserve_and_begin_v2").P("p_queue", request.Queue).P("p_priority", request.Priority)
                .P("p_after_eligible_at", request.After?.EligibleAt).P("p_after_job_id", request.After?.JobId)
                .Arr("p_names", [.. contracts.Select(x => x.Name)], NpgsqlDbType.Text)
                .Arr("p_versions", [.. contracts.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                .Arr("p_profile_ids", [.. contracts.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                .Arr("p_profile_versions", [.. contracts.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer)
                .P("p_lease", request.LeaseDuration).P("p_worker_id", request.WorkerInstanceId)
                .Arr("p_tokens", tokens, NpgsqlDbType.Uuid)
                .P("p_min_defer", MinimumDefer).Arr("p_defer_jitters_ms", jitters, NpgsqlDbType.Integer),
            row => row.HasValue ? row.Applied(ReadClaimed(row)) : row.Reject<ClaimedJobs>(),
            ct);
    }

    /// <summary>Decodes the command's receipt result (applied now or replayed) and the payload rows of the begun jobs.</summary>
    static ClaimedJobs ReadClaimed(PgRow row)
    {
        var payloads = row.More.Where(m => m.Status == "Payload").ToDictionary(m => m.ConflictingJobId!.Value);
        var outcomes = new List<ClaimOutcome>();
        foreach (var node in row.Result!["items"]!.AsArray())
        {
            var item = node!.AsObject();
            var jobId = Guid.Parse(item["jobId"]!.GetValue<string>());
            var status = Enum.Parse<StoreStatus>(item["status"]!.GetValue<string>());
            var code = item["code"]?.GetValue<string>();
            switch (status)
            {
                case StoreStatus.CapacityBlocked:
                    var cursor = item["cursor"]!.AsObject();
                    outcomes.Add(new ClaimOutcome(jobId, status, new StoreError(code ?? "capacity_blocked",
                        BlockedScopeId: Guid.Parse(item["blockedScopeId"]!.GetValue<string>()),
                        ResumeAfter: new CandidateCursor(PgTime.FromMs(cursor["eligibleAt"]!.GetValue<long>()), Guid.Parse(cursor["jobId"]!.GetValue<string>()))),
                        null, null, null, null));
                    break;
                case StoreStatus.Applied:
                    if (!payloads.TryGetValue(jobId, out var p))
                    {
                        throw new InvalidOperationException("payload row missing for a waiting job");
                    }

                    var (payload, business, diagnostic, createdAt, schedule) = Payload([(p.Payload!, p.Headers!, p.CreatedAt!.Value)]);
                    outcomes.Add(new ClaimOutcome(jobId, status, null,
                        ReceiptCodec.ReadReserve(item["reserve"]!.AsObject(), payload, business, diagnostic, createdAt) with { Schedule = schedule },
                        ReceiptCodec.ReadBegin(item["begin"]!.AsObject()), null, null));
                    break;
                case StoreStatus.RateLimited:
                    outcomes.Add(new ClaimOutcome(jobId, status, new StoreError(code ?? "rate_limited",
                        Detail: item["begin"]!["deferredUntil"]!.GetValue<long>().ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        null, null, null, ReceiptCodec.ReadBeginDeferred(item["begin"]!.AsObject())));
                    break;
                case StoreStatus.InvalidState or StoreStatus.BudgetExhausted:
                    outcomes.Add(new ClaimOutcome(jobId, status, new StoreError(code ?? item["begin"]!["errorCode"]!.GetValue<string>()),
                        null, null, ReceiptCodec.ReadBeginClosed(item["begin"]!.AsObject()).Closed, null));
                    break;
                default:
                    throw new InvalidOperationException($"ReserveAndBegin item status {status} is not an outcome.");
            }
        }

        return new ClaimedJobs(outcomes);
    }
}
