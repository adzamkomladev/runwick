using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore : ILeaseBatchStore
{
    // ------------------------------------------------------------------ RenewMany/1 (04 Changes 2026-11-02, PF-06)

    sealed record PermitRow(Guid JobId, Guid ScopeId, Guid AttemptId, Guid Token, DateTimeOffset LeaseExpiresAt);

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

        var jobIds = leases.Select(l => l.JobId).ToArray();
        return runner.ExecuteAsync<RenewedLeases>(
            Op(command.Application, command.CommandId, Operations.RenewMany, command.NotAfter, null, PgLane.Renewal),
            async tx =>
            {
                // J (all jobs, UUID order), S (all their scopes, UUID order), then reads behind those locks (requirements
                // are immutable; permits are reached through the locked jobs), then the decision clock: one round trip.
                var locks = tx.Batch();
                var jq = locks.Query(tx.Sql("""
                    SELECT job_id, state, generation, revision, current_attempt_id, worker_instance_id, ownership_token,
                           lease_expires_at, cancel_requested_at, active_unique_key, job_name, eligible_at
                    FROM runwick.jobs
                    WHERE application = @application AND job_id = ANY (@job_ids::uuid[])
                    ORDER BY job_id
                    FOR NO KEY UPDATE
                    """).Arr("job_ids", jobIds, NpgsqlDbType.Uuid), r => (Id: r.GetGuid(0), Job: MapJobAt(r, 1)));
                locks.Query(tx.Sql("""
                    SELECT s.scope_id
                    FROM runwick.concurrency_scopes AS s
                    WHERE s.application = @application
                      AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                                         WHERE r.application = @application AND r.job_id = ANY (@job_ids::uuid[]))
                    ORDER BY s.scope_id
                    FOR UPDATE OF s
                    """).Arr("job_ids", jobIds, NpgsqlDbType.Uuid), r => r.GetGuid(0));
                var rq = locks.Query(tx.Sql("""
                    SELECT job_id, scope_id FROM runwick.job_concurrency_requirements
                    WHERE application = @application AND job_id = ANY (@job_ids::uuid[])
                    """).Arr("job_ids", jobIds, NpgsqlDbType.Uuid), r => (Job: r.GetGuid(0), Scope: r.GetGuid(1)));
                var pq = locks.Query(tx.Sql("""
                    SELECT job_id, scope_id, attempt_id, ownership_token, lease_expires_at FROM runwick.concurrency_permits
                    WHERE application = @application AND job_id = ANY (@job_ids::uuid[])
                    """).Arr("job_ids", jobIds, NpgsqlDbType.Uuid), r => new PermitRow(r.GetGuid(0), r.GetGuid(1), r.GetGuid(2), r.GetGuid(3), PgTime.Read(r, 4)));
                var clock = locks.Query(PgTx.ClockSql(tx), PgTx.ReadClock);
                await tx.RunAsync(locks).ConfigureAwait(false);
                if (!await tx.DecideAtAsync(clock.Items[0]).ConfigureAwait(false))
                {
                    return new PgOutcome<RenewedLeases>.Rollback(tx.Reject<RenewedLeases>(StoreStatus.CommandExpired, "command_expired"));
                }

                var jobs = jq.Items.ToDictionary(x => x.Id, x => x.Job);
                var required = rq.Items.ToLookup(x => x.Job, x => x.Scope);
                var permits = pq.Items.ToLookup(p => p.JobId);
                var expiry = PgTime.Floor(tx.Now + command.Body.LeaseDuration);
                var outcomes = new LeaseRenewal[leases.Count];
                var renew = new List<(LeaseHandle Lease, JobRow Job, HashSet<Guid> Scopes)>();
                for (var i = 0; i < leases.Count; i++)
                {
                    var lease = leases[i];
                    var job = jobs.GetValueOrDefault(lease.JobId);
                    if (Ownership(job, lease, tx.Now, JobStates.Reserved, JobStates.Running) is { } refusal)
                    {
                        outcomes[i] = new LeaseRenewal(refusal.Status, refusal.Code, null);
                        continue;
                    }

                    // The exact permit set of this attempt, at the job's current expiry, one per required scope (03 §7.2);
                    // checked before the write so a mismatch refuses this lease alone.
                    var scopes = required[lease.JobId].ToHashSet();
                    var held = permits[lease.JobId]
                        .Where(p => p.AttemptId == lease.AttemptId && p.Token == lease.OwnershipToken && p.LeaseExpiresAt == job!.LeaseExpiresAt)
                        .Select(p => p.ScopeId).ToList();
                    if (held.Count != scopes.Count || !scopes.SetEquals(held))
                    {
                        outcomes[i] = new LeaseRenewal(StoreStatus.InvariantFault, "permit_set_mismatch", null);
                        continue;
                    }

                    outcomes[i] = new LeaseRenewal(StoreStatus.Applied, null,
                        new RenewedLease(new LeaseGrant(lease, expiry), job!.CancelRequestedAt is not null));
                    renew.Add((lease, job, scopes));
                }

                var value = new RenewedLeases(outcomes);
                if (renew.Count == 0)
                {
                    return new PgOutcome<RenewedLeases>.Rollback(StoreReply<RenewedLeases>.Applied(value, tx.Now));
                }

                // The job and permit updates for every passing lease in one round trip, each row fenced by its own
                // attempt, token, worker, generation, state and unexpired lease; both are checked per lease before COMMIT.
                var effects = tx.Batch();
                var ju = effects.Query(tx.Sql("""
                    UPDATE runwick.jobs AS j
                    SET lease_expires_at = @expiry
                    FROM unnest(@job_ids::uuid[], @attempt_ids::uuid[], @tokens::uuid[], @workers::uuid[], @generations::int[])
                         AS l(job_id, attempt_id, token, worker, generation)
                    WHERE j.application = @application AND j.job_id = l.job_id
                      AND j.current_attempt_id = l.attempt_id AND j.ownership_token = l.token AND j.worker_instance_id = l.worker
                      AND j.generation = l.generation AND j.state IN ('reserved', 'running') AND j.lease_expires_at > @now
                    RETURNING j.job_id
                    """).P("expiry", expiry).P("now", tx.Now)
                    .Arr("job_ids", renew.Select(x => x.Lease.JobId).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("attempt_ids", renew.Select(x => x.Lease.AttemptId).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("tokens", renew.Select(x => x.Lease.OwnershipToken).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("workers", renew.Select(x => x.Lease.WorkerInstanceId).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("generations", renew.Select(x => x.Lease.Generation).ToArray(), NpgsqlDbType.Integer),
                    r => r.GetGuid(0), mutation: true);
                var pu = effects.Query(tx.Sql("""
                    UPDATE runwick.concurrency_permits AS p
                    SET lease_expires_at = @expiry
                    FROM unnest(@job_ids::uuid[], @attempt_ids::uuid[], @tokens::uuid[], @previous::timestamptz[])
                         AS l(job_id, attempt_id, token, previous)
                    WHERE p.application = @application AND p.job_id = l.job_id AND p.attempt_id = l.attempt_id
                      AND p.ownership_token = l.token AND p.lease_expires_at = l.previous
                    RETURNING p.job_id, p.scope_id
                    """).P("expiry", expiry)
                    .Arr("job_ids", renew.Select(x => x.Lease.JobId).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("attempt_ids", renew.Select(x => x.Lease.AttemptId).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("tokens", renew.Select(x => x.Lease.OwnershipToken).ToArray(), NpgsqlDbType.Uuid)
                    .Arr("previous", renew.Select(x => x.Job.LeaseExpiresAt!.Value).ToArray(), NpgsqlDbType.TimestampTz),
                    r => (Job: r.GetGuid(0), Scope: r.GetGuid(1)), mutation: true);

                // Every lease was checked under its locks, so a count that differs is a broken invariant: nothing commits.
                var mismatch = new PgOutcome<RenewedLeases>.Rollback(tx.Reject<RenewedLeases>(StoreStatus.InvariantFault, "permit_set_mismatch"));
                return new PgOutcome<RenewedLeases>.Commit(StoreReply<RenewedLeases>.Applied(value, tx.Now), null)
                {
                    Effects = effects,
                    Verify = () =>
                    {
                        var updated = ju.Items.ToHashSet();
                        var extended = pu.Items.ToLookup(x => x.Job, x => x.Scope);
                        return updated.Count == renew.Count && ju.Items.Count == renew.Count
                            && renew.All(x => updated.Contains(x.Lease.JobId) && x.Scopes.SetEquals(extended[x.Lease.JobId])
                                && extended[x.Lease.JobId].Count() == x.Scopes.Count)
                            ? null
                            : mismatch;
                    },
                };
            },
            null,
            ct);
    }
}
