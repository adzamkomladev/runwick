using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : ILeaseBatchStore
{
    // ------------------------------------------------------------------ RenewMany/1 (04 Changes 2026-11-02, PF-06)

    /// <summary>
    /// Renews distinct leases in one transaction with per-lease outcomes. Every job row is locked in UUID order (rank 5),
    /// then every required concurrency scope of those jobs in UUID order (rank 7), then the decision clock; each lease is
    /// checked against its locked row exactly as Renew/1 checks it (ownership, fence, generation, state, unexpired) and
    /// its permit set is checked before any write. Only the leases that pass are extended, by one set-based update per
    /// table; a refused lease has no effect and never rolls back the others.
    /// </summary>
    public Task<StoreReply<RenewedLeases>> RenewManyAsync(StoreCommand<RenewManyRequest> command, CancellationToken ct)
    {
        var leases = command.Body.Leases;
        if (command.Body.LeaseDuration < MinLease || command.Body.LeaseDuration > MaxLease
            || leases.Count is 0 or > ILeaseBatchStore.MaxLeases
            || leases.Select(l => l.JobId).Distinct().Count() != leases.Count)
        {
            return Task.FromResult(StoreReply<RenewedLeases>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request"), time.GetUtcNow()));
        }

        // J (all jobs, UUID order), S (all their scopes, UUID order), the fresh clock, the per-lease checks and the two
        // set-based updates with their per-lease check run in runwick.renew_many_v1.
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.RenewMany, command.NotAfter, null, PgLane.Renewal),
            new PgCall("renew_many_v1")
                .Arr("p_job_ids", [.. leases.Select(l => l.JobId)], NpgsqlDbType.Uuid)
                .Arr("p_attempt_ids", [.. leases.Select(l => l.AttemptId)], NpgsqlDbType.Uuid)
                .Arr("p_worker_ids", [.. leases.Select(l => l.WorkerInstanceId)], NpgsqlDbType.Uuid)
                .Arr("p_tokens", [.. leases.Select(l => l.OwnershipToken)], NpgsqlDbType.Uuid)
                .Arr("p_generations", [.. leases.Select(l => l.Generation)], NpgsqlDbType.Integer)
                .P("p_lease", command.Body.LeaseDuration),
            row =>
            {
                if (!row.HasValue)
                {
                    return row.Reject<RenewedLeases>();
                }

                var items = row.Result!["items"]!.AsArray();
                var outcomes = new LeaseRenewal[leases.Count];
                for (var i = 0; i < leases.Count; i++)
                {
                    var o = items[i]!;
                    var status = Enum.Parse<StoreStatus>(o["status"]!.GetValue<string>());
                    outcomes[i] = status == StoreStatus.Applied
                        ? new LeaseRenewal(status, null, new RenewedLease(
                            new LeaseGrant(leases[i], PgTime.FromMs(o["leaseExpiresAt"]!.GetValue<long>())), o["cancelRequested"]!.GetValue<bool>()))
                        : new LeaseRenewal(status, o["code"]!.GetValue<string>(), null);
                }

                return row.Applied(new RenewedLeases(outcomes));
            },
            ct);
    }
}
