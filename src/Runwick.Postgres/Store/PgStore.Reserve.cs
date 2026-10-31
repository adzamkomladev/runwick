using System.Security.Cryptography;
using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    static readonly TimeSpan MinLease = TimeSpan.FromSeconds(1);
    static readonly TimeSpan MaxLease = TimeSpan.FromHours(1);

    // ------------------------------------------------------------------ Reserve/1 (04 §7.3)

    public Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var contracts = request.SupportedContracts.Distinct().ToArray();
        if (request.LeaseDuration < MinLease || request.LeaseDuration > MaxLease || contracts.Length is < 1 or > 512)
        {
            return Task.FromResult(StoreReply<ReservedJob>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request"), time.GetUtcNow()));
        }

        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Reserve, command.CommandId, command.NotAfter,
            CommandEnvelopes.Reserve(request));
        return runner.ExecuteAsync(
            Op(command.Application, command.CommandId, Operations.Reserve, command.NotAfter, fingerprint, PgLane.Admission),
            async tx =>
            {
                using (var q = tx.Sql("""
                    SELECT paused FROM runwick.queue_controls
                    WHERE application = @application AND queue = @queue
                    FOR SHARE
                    """).P("queue", request.Queue))
                {
                    var paused = await tx.QueryAsync(q, r => r.GetBoolean(0)).ConfigureAwait(false);
                    if (paused.Count == 0)
                    {
                        return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.NoWork, "queue_not_registered"));
                    }

                    if (paused[0])
                    {
                        return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.QueuePaused, "queue_paused"));
                    }
                }

                using var c = tx.Sql("""
                    SELECT j.job_id, j.eligible_at, j.generation, j.revision, j.reservation_count,
                           j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version
                    FROM runwick.jobs AS j
                    WHERE j.application = @application
                      AND j.queue = @queue
                      AND j.priority = @priority
                      AND j.state IN ('pending', 'retrying')
                      AND j.eligible_at <= statement_timestamp()
                      AND (@after_job_id::uuid IS NULL
                           OR (j.eligible_at, j.job_id) > (@after_eligible_at::timestamptz, @after_job_id::uuid))
                      AND EXISTS (
                          SELECT 1
                          FROM unnest(@names::text[], @versions::integer[], @profile_ids::text[], @profile_versions::integer[])
                               AS c(name, version, profile_id, profile_version)
                          WHERE c.name = j.job_name AND c.version = j.payload_version
                            AND c.profile_id = j.serializer_profile_id
                            AND c.profile_version = j.serializer_profile_version)
                    ORDER BY j.eligible_at, j.job_id
                    LIMIT 1
                    FOR NO KEY UPDATE OF j SKIP LOCKED
                    """).P("queue", request.Queue).P("priority", request.Priority)
                        .P("after_job_id", request.After?.JobId, NpgsqlDbType.Uuid).P("after_eligible_at", request.After?.EligibleAt)
                        .Arr("names", [.. contracts.Select(x => x.Name)], NpgsqlDbType.Text)
                        .Arr("versions", [.. contracts.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                        .Arr("profile_ids", [.. contracts.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                        .Arr("profile_versions", [.. contracts.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer);
                var candidates = await tx.QueryAsync(c, r => (
                    JobId: r.GetGuid(0), EligibleAt: PgTime.Read(r, 1), Generation: r.GetInt32(2), Revision: r.GetInt64(3),
                    ReservationCount: r.GetInt64(4),
                    Contract: new JobContract(r.GetString(5), r.GetInt32(6), r.GetString(7), r.GetInt32(8)))).ConfigureAwait(false);
                if (candidates.Count == 0)
                {
                    return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.NoWork, "no_work"));
                }

                var job = candidates[0];
                var policy = await ReadGenerationAsync(tx, job.JobId, job.Generation).ConfigureAwait(false);
                var scopes = await LockScopesAsync(tx, job.JobId).ConfigureAwait(false);

                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.CommandExpired, "command_expired"));
                }

                var now = tx.Now;
                if (scopes.Count > 0)
                {
                    using var p = tx.Sql("""
                        SELECT r.scope_id, count(p.attempt_id) AS live
                        FROM unnest(@scope_ids::uuid[]) AS r(scope_id)
                        LEFT JOIN runwick.concurrency_permits AS p
                          ON p.application = @application AND p.scope_id = r.scope_id AND p.lease_expires_at > @now
                        GROUP BY r.scope_id
                        """).Arr("scope_ids", [.. scopes.Select(s => s.ScopeId)], NpgsqlDbType.Uuid).P("now", now);
                    var live = (await tx.QueryAsync(p, r => (Scope: r.GetGuid(0), Live: r.GetInt64(1))).ConfigureAwait(false))
                        .ToDictionary(x => x.Scope, x => x.Live);
                    var blocked = scopes.FirstOrDefault(s => live[s.ScopeId] >= s.Capacity);
                    if (blocked is not null)
                    {
                        return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.CapacityBlocked, "capacity_blocked",
                            blockedScope: blocked.ScopeId, resumeAfter: new CandidateCursor(job.EligibleAt, job.JobId)));
                    }
                }

                var expiry = PgTime.Floor(now + request.LeaseDuration);
                var reservationNumber = job.ReservationCount + 1;
                var attemptId = Guid.CreateVersion7(now);
                var token = NewToken();

                var fences = new Dictionary<Guid, long>();
                if (scopes.Any(s => s.Fencing))
                {
                    using var f = tx.Sql("""
                        UPDATE runwick.concurrency_scopes
                        SET fencing_counter = fencing_counter + 1
                        WHERE application = @application AND fencing_enabled AND scope_id = ANY (@scope_ids::uuid[])
                        RETURNING scope_id, fencing_counter
                        """).Arr("scope_ids", [.. scopes.Where(s => s.Fencing).Select(s => s.ScopeId)], NpgsqlDbType.Uuid);
                    foreach (var (scope, value) in await tx.QueryAsync(f, r => (r.GetGuid(0), r.GetInt64(1)), mutation: true).ConfigureAwait(false))
                    {
                        fences[scope] = value;
                    }
                }

                using (var a = tx.Sql("""
                    INSERT INTO runwick.job_attempts (
                        application, attempt_id, job_id, generation, reservation_number, worker_instance_id, ownership_token, reserved_at)
                    VALUES (@application, @attempt_id, @job_id, @generation, @reservation_number, @worker, @token, @now)
                    """).P("attempt_id", attemptId).P("job_id", job.JobId).P("generation", job.Generation)
                        .P("reservation_number", reservationNumber).P("worker", request.WorkerInstanceId).P("token", token).P("now", now))
                {
                    await tx.ExecAsync(a).ConfigureAwait(false);
                }

                long revision;
                using (var j = tx.Sql("""
                    UPDATE runwick.jobs
                    SET state = 'reserved', reservation_count = reservation_count + 1, revision = revision + 1,
                        current_attempt_id = @attempt_id, worker_instance_id = @worker, ownership_token = @token,
                        lease_expires_at = @expiry, updated_at = @now
                    WHERE application = @application AND job_id = @job_id AND state IN ('pending', 'retrying')
                    RETURNING revision
                    """).P("attempt_id", attemptId).P("worker", request.WorkerInstanceId).P("token", token)
                        .P("expiry", expiry).P("now", now).P("job_id", job.JobId))
                {
                    var rows = await tx.QueryAsync(j, r => r.GetInt64(0), mutation: true).ConfigureAwait(false);
                    if (rows.Count != 1)
                    {
                        return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.InvariantFault, "invalid_transition"));
                    }

                    revision = rows[0];
                }

                if (scopes.Count > 0)
                {
                    using var pi = tx.Sql("""
                        INSERT INTO runwick.concurrency_permits (
                            application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at, fencing_value)
                        SELECT @application, p.scope_id, @attempt_id, @job_id, @token, @expiry, p.fence
                        FROM unnest(@scope_ids::uuid[], @fences::bigint[]) AS p(scope_id, fence)
                        ORDER BY p.scope_id
                        """).P("attempt_id", attemptId).P("job_id", job.JobId).P("token", token).P("expiry", expiry)
                            .Arr("scope_ids", [.. scopes.Select(s => s.ScopeId)], NpgsqlDbType.Uuid)
                            .Arr("fences", [.. scopes.Select(s => fences.TryGetValue(s.ScopeId, out var v) ? (long?)v : null)], NpgsqlDbType.Bigint);
                    await tx.ExecAsync(pi).ConfigureAwait(false);
                }

                await EventAsync(tx, job.JobId, attemptId, "attempt_reserved",
                    new JsonObject { ["generation"] = job.Generation, ["reservationNumber"] = reservationNumber }).ConfigureAwait(false);

                var (payload, business, diagnostic, createdAt) = await ReadPayloadAsync(tx, job.JobId).ConfigureAwait(false);
                var reserved = new ReservedJob(
                    new JobPosition(job.JobId, job.Generation, revision, JobStates.Reserved),
                    new LeaseGrant(new LeaseHandle(job.JobId, attemptId, request.WorkerInstanceId, token, job.Generation), expiry),
                    job.Contract,
                    new CandidateCursor(job.EligibleAt, job.JobId),
                    reservationNumber,
                    payload, business, diagnostic,
                    request.Queue, request.Priority,
                    policy.Policy,
                    [.. scopes.Where(s => s.Fencing).Select(s => new StoreFence(s.ScopeId, fences[s.ScopeId]))],
                    createdAt);
                return new PgOutcome<ReservedJob>.Commit(StoreReply<ReservedJob>.Applied(reserved, now), ReceiptCodec.Reserve(reserved));
            },
            async (tx, row) =>
            {
                var o = row.Result!;
                var (payload, business, diagnostic, createdAt) = await ReadPayloadAsync(tx, Guid.Parse(o["jobId"]!.GetValue<string>())).ConfigureAwait(false);
                return StoreReply<ReservedJob>.Applied(ReceiptCodec.ReadReserve(o, payload, business, diagnostic, createdAt), row.DecidedAt!.Value, replayed: true);
            },
            ct);
    }

    static Guid NewToken()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        return new Guid(bytes, bigEndian: true);
    }

    static async Task<(byte[] Payload, Dictionary<string, string> Business, Dictionary<string, string> Diagnostic, DateTimeOffset CreatedAt)> ReadPayloadAsync(PgTx tx, Guid jobId)
    {
        using var cmd = tx.Sql("""
            SELECT p.payload_utf8, p.headers::text, j.created_at
            FROM runwick.job_payloads p JOIN runwick.jobs j USING (application, job_id)
            WHERE p.application = @application AND p.job_id = @job_id
            """).P("job_id", jobId);
        var rows = await tx.QueryAsync(cmd, r => ((byte[])r[0], r.GetString(1), r.GetFieldValue<DateTimeOffset>(2))).ConfigureAwait(false);
        if (rows.Count != 1)
        {
            throw new InvalidOperationException("payload row missing for a waiting job");
        }

        var (business, diagnostic) = ReadHeaders(rows[0].Item2);
        return (rows[0].Item1, business, diagnostic, rows[0].Item3);
    }

    sealed record GenerationRow(ExecutionPolicy Policy, int StartedCount, int ConsecutiveUnstarted);

    static async Task<GenerationRow> ReadGenerationAsync(PgTx tx, Guid jobId, int generation)
    {
        using var g = tx.Sql("""
            SELECT max_executions, max_unstarted_abandonments, execution_timeout_ms,
                   retry_policy::text, unstarted_retry_policy::text, started_count, consecutive_unstarted_count
            FROM runwick.job_generations
            WHERE application = @application AND job_id = @job_id AND generation = @generation
            """).P("job_id", jobId).P("generation", generation);
        var rows = await tx.QueryAsync(g, r => new GenerationRow(
            new ExecutionPolicy(r.GetInt32(0), r.IsDBNull(2) ? null : r.GetInt64(2), ReadRetry(r.GetString(3)), r.GetInt32(1), ReadRetry(r.GetString(4))),
            r.GetInt32(5), r.GetInt32(6))).ConfigureAwait(false);
        return rows.Count == 1 ? rows[0] : throw new InvalidOperationException("generation row missing");
    }

    sealed record ScopeRow(Guid ScopeId, int Capacity, bool Fencing);

    /// <summary>S: the job's required scopes, locked in UUID order.</summary>
    static async Task<List<ScopeRow>> LockScopesAsync(PgTx tx, Guid jobId)
    {
        using var s = tx.Sql("""
            SELECT s.scope_id, s.capacity, s.fencing_enabled
            FROM runwick.concurrency_scopes AS s
            WHERE s.application = @application
              AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                                 WHERE r.application = @application AND r.job_id = @job_id)
            ORDER BY s.scope_id
            FOR UPDATE OF s
            """).P("job_id", jobId);
        return await tx.QueryAsync(s, r => new ScopeRow(r.GetGuid(0), r.GetInt32(1), r.GetBoolean(2))).ConfigureAwait(false);
    }

    static async Task EventAsync(PgTx tx, Guid jobId, Guid? attemptId, string kind, JsonObject details)
    {
        using var e = tx.Sql("""
            INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
            VALUES (@application, @job_id, @attempt_id, @command_id, @kind, @now, @details)
            """).P("job_id", jobId).P("attempt_id", attemptId, NpgsqlDbType.Uuid).P("command_id", tx.Op.CommandId)
                .P("kind", kind).P("now", tx.Now).Json("details", details);
        await tx.ExecAsync(e).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ Begin/1 (04 §7.5)

    public async Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        BeginClosed? closed = null;
        RateDeferral? deferred = null;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Begin, command.CommandId, command.NotAfter,
            CommandEnvelopes.Begin(command.Body));
        var reply = await runner.ExecuteAsync(
            Op(command.Application, command.CommandId, Operations.Begin, command.NotAfter, fingerprint, PgLane.Admission),
            async tx =>
            {
                closed = null;
                deferred = null;
                var job = await LockJobAsync(tx, lease.JobId).ConfigureAwait(false);
                GenerationRow? gen = job is null ? null : await ReadGenerationAsync(tx, lease.JobId, job.Generation).ConfigureAwait(false);
                var plan = job is null ? "none"
                    : job.CancelRequestedAt is not null ? "cancel"
                    : gen!.StartedCount >= gen.Policy.MaxExecutions ? "exhausted"
                    : "authorize";
                if (plan is "cancel" or "exhausted")
                {
                    await LockActiveKeyAsync(tx, job!).ConfigureAwait(false);
                }

                // Rate step (10 §4): concurrency scopes, then rate scopes, both in UUID order. Cancellation and an
                // exhausted budget are decided before any charge, so their plans never lock rate scopes.
                var rated = job is not null && plan == "authorize" && (await RateRequirementsAsync(tx, lease.JobId).ConfigureAwait(false)).Count > 0;
                if (job is not null && (plan != "authorize" || rated))
                {
                    await LockScopesAsync(tx, lease.JobId).ConfigureAwait(false);
                }

                List<RateScopeRow> rateScopes = rated ? await LockRateScopesAsync(tx, lease.JobId).ConfigureAwait(false) : [];

                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<BegunExecution>.Rollback(tx.Reject<BegunExecution>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (Ownership(job, lease, tx.Now, JobStates.Reserved) is { } refusal)
                {
                    return new PgOutcome<BegunExecution>.Rollback(tx.Reject<BegunExecution>(refusal.Status, refusal.Code));
                }

                if (plan is "cancel" or "exhausted")
                {
                    var cancel = plan == "cancel";
                    var outcome = cancel ? "cancelled_before_start" : "abandoned_before_start";
                    var reason = cancel ? "cancelled_before_start" : "execution_budget_exhausted";
                    var state = cancel ? JobStates.Cancelled : JobStates.Failed;
                    var revision = await CloseAsync(tx, new Close(job!, lease.JobId, lease.AttemptId, lease.OwnershipToken, outcome, state, reason,
                        cancel ? "attempt_cancelled_before_start" : "attempt_finished")).ConfigureAwait(false);
                    var status = cancel ? StoreStatus.InvalidState : StoreStatus.BudgetExhausted;
                    var code = cancel ? "cancelled_before_start" : "execution_budget_exhausted";
                    closed = new BeginClosed(new JobPosition(lease.JobId, job!.Generation, revision, state), outcome, reason);
                    return new PgOutcome<BegunExecution>.Commit(
                        new StoreReply<BegunExecution>(status, StoreEffect.Applied, false, null, new StoreError(code), tx.Now),
                        ReceiptCodec.BeginClosed(closed, code));
                }

                if (rateScopes.Count > 0)
                {
                    deferred = await ChargeRatesAsync(tx, job!, gen!, lease, rateScopes).ConfigureAwait(false);
                    if (deferred is not null)
                    {
                        return new PgOutcome<BegunExecution>.Commit(
                            new StoreReply<BegunExecution>(StoreStatus.RateLimited, StoreEffect.Applied, false, null,
                                new StoreError("rate_limited", Detail: PgTime.Ms(deferred.EligibleAt).ToString(System.Globalization.CultureInfo.InvariantCulture)),
                                tx.Now),
                            ReceiptCodec.BeginDeferred(deferred));
                    }
                }

                int executionNumber;
                using (var g = tx.Sql("""
                    UPDATE runwick.job_generations
                    SET started_count = started_count + 1, consecutive_unstarted_count = 0
                    WHERE application = @application AND job_id = @job_id AND generation = @generation AND started_count < max_executions
                    RETURNING started_count
                    """).P("job_id", lease.JobId).P("generation", lease.Generation))
                {
                    var rows = await tx.QueryAsync(g, r => r.GetInt32(0), mutation: true).ConfigureAwait(false);
                    if (rows.Count != 1)
                    {
                        return new PgOutcome<BegunExecution>.Rollback(tx.Reject<BegunExecution>(StoreStatus.InvariantFault, "invalid_transition"));
                    }

                    executionNumber = rows[0];
                }

                (long Total, long Revision, DateTimeOffset Expires) updated;
                using (var j = tx.Sql("""
                    UPDATE runwick.jobs
                    SET state = 'running', execution_count = execution_count + 1, revision = revision + 1, updated_at = @now
                    WHERE application = @application AND job_id = @job_id AND state = 'reserved'
                      AND current_attempt_id = @attempt_id AND ownership_token = @token
                    RETURNING execution_count, revision, lease_expires_at
                    """).P("now", tx.Now).P("job_id", lease.JobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken))
                {
                    var rows = await tx.QueryAsync(j, r => (r.GetInt64(0), r.GetInt64(1), PgTime.Read(r, 2)), mutation: true).ConfigureAwait(false);
                    if (rows.Count != 1)
                    {
                        return new PgOutcome<BegunExecution>.Rollback(tx.Reject<BegunExecution>(StoreStatus.InvariantFault, "invalid_transition"));
                    }

                    updated = rows[0];
                }

                using (var a = tx.Sql("""
                    UPDATE runwick.job_attempts
                    SET execution_number = @execution_number, authorized_at = @now
                    WHERE application = @application AND attempt_id = @attempt_id AND finished_at IS NULL AND execution_number IS NULL
                    """).P("execution_number", executionNumber).P("now", tx.Now).P("attempt_id", lease.AttemptId))
                {
                    if (await tx.ExecAsync(a).ConfigureAwait(false) != 1)
                    {
                        return new PgOutcome<BegunExecution>.Rollback(tx.Reject<BegunExecution>(StoreStatus.InvariantFault, "invalid_transition"));
                    }
                }

                await EventAsync(tx, lease.JobId, lease.AttemptId, "attempt_started",
                    new JsonObject { ["generation"] = lease.Generation, ["executionNumber"] = executionNumber }).ConfigureAwait(false);
                var begun = new BegunExecution(
                    new JobPosition(lease.JobId, lease.Generation, updated.Revision, JobStates.Running),
                    new LeaseGrant(lease, updated.Expires), executionNumber, updated.Total, tx.Now);
                return new PgOutcome<BegunExecution>.Commit(StoreReply<BegunExecution>.Applied(begun, tx.Now), ReceiptCodec.Begin(begun));
            },
            (tx, row) =>
            {
                var o = row.Result!;
                if (o.ContainsKey("deferredUntil"))
                {
                    closed = null;
                    deferred = ReceiptCodec.ReadBeginDeferred(o);
                    return Task.FromResult(new StoreReply<BegunExecution>(StoreStatus.RateLimited, StoreEffect.Applied, true, null,
                        new StoreError("rate_limited", Detail: PgTime.Ms(deferred.EligibleAt).ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        row.DecidedAt!.Value));
                }

                deferred = null;
                if (o.ContainsKey("attemptOutcome"))
                {
                    var (c, code) = ReceiptCodec.ReadBeginClosed(o);
                    closed = c;
                    return Task.FromResult(new StoreReply<BegunExecution>(Enum.Parse<StoreStatus>(row.Status!), StoreEffect.Applied, true, null,
                        new StoreError(code), row.DecidedAt!.Value));
                }

                closed = null;
                return Task.FromResult(StoreReply<BegunExecution>.Applied(ReceiptCodec.ReadBegin(o), row.DecidedAt!.Value, replayed: true));
            },
            ct).ConfigureAwait(false);
        return new BeginReply(reply, reply.Effect == StoreEffect.Applied ? closed : null)
        {
            Deferred = reply.Status == StoreStatus.RateLimited && reply.Effect == StoreEffect.Applied ? deferred : null,
        };
    }

    // ------------------------------------------------------------------ Renew/1 (04 §7.6)

    public Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        if (command.Body.LeaseDuration < MinLease || command.Body.LeaseDuration > MaxLease)
        {
            return Task.FromResult(StoreReply<RenewedLease>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request"), time.GetUtcNow()));
        }

        return runner.ExecuteAsync<RenewedLease>(
            Op(command.Application, command.CommandId, Operations.Renew, command.NotAfter, null, PgLane.Renewal),
            async tx =>
            {
                var job = await LockJobAsync(tx, lease.JobId).ConfigureAwait(false);
                var scopes = job is null ? [] : await LockScopesAsync(tx, lease.JobId).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<RenewedLease>.Rollback(tx.Reject<RenewedLease>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (Ownership(job, lease, tx.Now, JobStates.Reserved, JobStates.Running) is { } refusal)
                {
                    return new PgOutcome<RenewedLease>.Rollback(tx.Reject<RenewedLease>(refusal.Status, refusal.Code));
                }

                var expiry = PgTime.Floor(tx.Now + command.Body.LeaseDuration);
                using (var j = tx.Sql("""
                    UPDATE runwick.jobs
                    SET lease_expires_at = @expiry
                    WHERE application = @application AND job_id = @job_id
                      AND current_attempt_id = @attempt_id AND ownership_token = @token AND worker_instance_id = @worker
                      AND generation = @generation AND state IN ('reserved', 'running') AND lease_expires_at > @now
                    """).P("expiry", expiry).P("job_id", lease.JobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken)
                        .P("worker", lease.WorkerInstanceId).P("generation", lease.Generation).P("now", tx.Now))
                {
                    if (await tx.ExecAsync(j).ConfigureAwait(false) != 1)
                    {
                        return new PgOutcome<RenewedLease>.Rollback(tx.Reject<RenewedLease>(StoreStatus.StaleLease, "lease_expired"));
                    }
                }

                using (var p = tx.Sql("""
                    UPDATE runwick.concurrency_permits
                    SET lease_expires_at = @expiry
                    WHERE application = @application AND job_id = @job_id AND attempt_id = @attempt_id AND ownership_token = @token
                      AND lease_expires_at = @previous
                    RETURNING scope_id
                    """).P("expiry", expiry).P("job_id", lease.JobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken)
                    .P("previous", job!.LeaseExpiresAt))
                {
                    var renewed = (await tx.QueryAsync(p, r => r.GetGuid(0), mutation: true).ConfigureAwait(false)).ToHashSet();
                    if (!renewed.SetEquals(scopes.Select(s => s.ScopeId)))
                    {
                        return new PgOutcome<RenewedLease>.Rollback(tx.Reject<RenewedLease>(StoreStatus.InvariantFault, "permit_set_mismatch"));
                    }
                }

                var value = new RenewedLease(new LeaseGrant(lease, expiry), job!.CancelRequestedAt is not null);
                return new PgOutcome<RenewedLease>.Commit(StoreReply<RenewedLease>.Applied(value, tx.Now), null);
            },
            null,
            ct);
    }
}
