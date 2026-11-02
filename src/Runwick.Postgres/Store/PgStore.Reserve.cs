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
                // Q and the candidate claim in one round trip (PF-04): the claim's inputs are the request's, and the
                // server takes Q's share lock first (04 §5). A paused or unregistered queue rolls the claim back.
                var claim = tx.Batch();
                var qq = claim.Query(tx.Sql("""
                    SELECT paused FROM runwick.queue_controls
                    WHERE application = @application AND queue = @queue
                    FOR SHARE
                    """).P("queue", request.Queue), r => r.GetBoolean(0));
                var cq = claim.Query(tx.Sql("""
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
                        .Arr("profile_versions", [.. contracts.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer), r => (
                    JobId: r.GetGuid(0), EligibleAt: PgTime.Read(r, 1), Generation: r.GetInt32(2), Revision: r.GetInt64(3),
                    ReservationCount: r.GetInt64(4),
                    Contract: new JobContract(r.GetString(5), r.GetInt32(6), r.GetString(7), r.GetInt32(8))));
                await tx.RunAsync(claim).ConfigureAwait(false);
                if (qq.Items.Count == 0)
                {
                    return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.NoWork, "queue_not_registered"));
                }

                if (qq.Items[0])
                {
                    return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.QueuePaused, "queue_paused"));
                }

                var candidates = cq.Items;
                if (candidates.Count == 0)
                {
                    return new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.NoWork, "no_work"));
                }

                // G, the payload, S and the decision clock in one round trip: each needs only the claimed job, and
                // S is the last blocking lock (PF-04).
                var job = candidates[0];
                var locks = tx.Batch();
                var gq = locks.Query(GenerationSql(tx, job.JobId, job.Generation), MapGeneration);
                var pq = locks.Query(PayloadSql(tx, job.JobId), MapPayload);
                var sq = locks.Query(LockScopesSql(tx, job.JobId), MapScope);
                var clock = locks.Query(PgTx.ClockSql(tx), PgTx.ReadClock);
                await tx.RunAsync(locks).ConfigureAwait(false);
                var policy = gq.Items.Count == 1 ? gq.Items[0] : throw new InvalidOperationException("generation row missing");
                var (payload, business, diagnostic, createdAt, schedule) = Payload(pq.Items);
                var scopes = sq.Items;

                if (!await tx.DecideAtAsync(clock.Items[0]).ConfigureAwait(false))
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

                // The effects and R4 in one round trip (PF-04). The scopes are locked, so each fence is the scope's
                // counter + 1, and the job is locked, so its new revision is its revision + 1; both are checked.
                var fences = scopes.Where(s => s.Fencing).ToDictionary(s => s.ScopeId, s => s.FencingCounter + 1);
                var revision = job.Revision + 1;
                var effects = tx.Batch();
                PgBatch.Rows<(Guid, long)>? fq = null;
                if (fences.Count > 0)
                {
                    fq = effects.Query(tx.Sql("""
                        UPDATE runwick.concurrency_scopes
                        SET fencing_counter = fencing_counter + 1
                        WHERE application = @application AND fencing_enabled AND scope_id = ANY (@scope_ids::uuid[])
                        RETURNING scope_id, fencing_counter
                        """).Arr("scope_ids", [.. fences.Keys], NpgsqlDbType.Uuid), r => (r.GetGuid(0), r.GetInt64(1)), mutation: true);
                }

                effects.Exec(tx.Sql("""
                    INSERT INTO runwick.job_attempts (
                        application, attempt_id, job_id, generation, reservation_number, worker_instance_id, ownership_token, reserved_at)
                    VALUES (@application, @attempt_id, @job_id, @generation, @reservation_number, @worker, @token, @now)
                    """).P("attempt_id", attemptId).P("job_id", job.JobId).P("generation", job.Generation)
                        .P("reservation_number", reservationNumber).P("worker", request.WorkerInstanceId).P("token", token).P("now", now));

                var ju = effects.Query(tx.Sql("""
                    UPDATE runwick.jobs
                    SET state = 'reserved', reservation_count = reservation_count + 1, revision = revision + 1,
                        current_attempt_id = @attempt_id, worker_instance_id = @worker, ownership_token = @token,
                        lease_expires_at = @expiry, updated_at = @now
                    WHERE application = @application AND job_id = @job_id AND state IN ('pending', 'retrying')
                    RETURNING revision
                    """).P("attempt_id", attemptId).P("worker", request.WorkerInstanceId).P("token", token)
                        .P("expiry", expiry).P("now", now).P("job_id", job.JobId), r => r.GetInt64(0), mutation: true);

                if (scopes.Count > 0)
                {
                    effects.Exec(tx.Sql("""
                        INSERT INTO runwick.concurrency_permits (
                            application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at, fencing_value)
                        SELECT @application, p.scope_id, @attempt_id, @job_id, @token, @expiry, p.fence
                        FROM unnest(@scope_ids::uuid[], @fences::bigint[]) AS p(scope_id, fence)
                        ORDER BY p.scope_id
                        """).P("attempt_id", attemptId).P("job_id", job.JobId).P("token", token).P("expiry", expiry)
                            .Arr("scope_ids", [.. scopes.Select(s => s.ScopeId)], NpgsqlDbType.Uuid)
                            .Arr("fences", [.. scopes.Select(s => fences.TryGetValue(s.ScopeId, out var v) ? (long?)v : null)], NpgsqlDbType.Bigint));
                }

                EventInto(effects, tx, job.JobId, attemptId, "attempt_reserved",
                    new JsonObject { ["generation"] = job.Generation, ["reservationNumber"] = reservationNumber });

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
                    createdAt) { Schedule = schedule };
                var invalid = new PgOutcome<ReservedJob>.Rollback(tx.Reject<ReservedJob>(StoreStatus.InvariantFault, "invalid_transition"));
                return new PgOutcome<ReservedJob>.Commit(StoreReply<ReservedJob>.Applied(reserved, now), ReceiptCodec.Reserve(reserved))
                {
                    Effects = effects,
                    Verify = () =>
                    {
                        if (fq is not null && !(fq.Items.Count == fences.Count && fq.Items.All(f => fences[f.Item1] == f.Item2)))
                        {
                            throw new InvariantViolationException("invalid_transition");
                        }

                        return ju.Items is [var r] && r == revision ? null : invalid;
                    },
                };
            },
            async (tx, row) =>
            {
                var o = row.Result!;
                var (payload, business, diagnostic, createdAt, schedule) = await ReadPayloadAsync(tx, Guid.Parse(o["jobId"]!.GetValue<string>())).ConfigureAwait(false);
                return StoreReply<ReservedJob>.Applied(ReceiptCodec.ReadReserve(o, payload, business, diagnostic, createdAt) with { Schedule = schedule }, row.DecidedAt!.Value, replayed: true);
            },
            ct);
    }

    static Guid NewToken()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        return new Guid(bytes, bigEndian: true);
    }

    static async Task<(byte[] Payload, Dictionary<string, string> Business, Dictionary<string, string> Diagnostic, DateTimeOffset CreatedAt, JobScheduleInfo? Schedule)> ReadPayloadAsync(PgTx tx, Guid jobId)
    {
        using var cmd = PayloadSql(tx, jobId);
        return Payload(await tx.QueryAsync(cmd, MapPayload).ConfigureAwait(false));
    }

    static Npgsql.NpgsqlCommand PayloadSql(PgTx tx, Guid jobId) => tx.Sql("""
        SELECT p.payload_utf8, p.headers::text, j.created_at
        FROM runwick.job_payloads p JOIN runwick.jobs j USING (application, job_id)
        WHERE p.application = @application AND p.job_id = @job_id
        """).P("job_id", jobId);

    static (byte[], string, DateTimeOffset) MapPayload(Npgsql.NpgsqlDataReader r) => ((byte[])r[0], r.GetString(1), r.GetFieldValue<DateTimeOffset>(2));

    static (byte[] Payload, Dictionary<string, string> Business, Dictionary<string, string> Diagnostic, DateTimeOffset CreatedAt, JobScheduleInfo? Schedule) Payload(
        List<(byte[], string, DateTimeOffset)> rows)
    {
        if (rows.Count != 1)
        {
            throw new InvalidOperationException("payload row missing for a waiting job");
        }

        var (business, diagnostic) = ReadHeaders(rows[0].Item2);
        return (rows[0].Item1, business, diagnostic, rows[0].Item3, ReadScheduleHeader(rows[0].Item2));
    }

    sealed record GenerationRow(ExecutionPolicy Policy, int StartedCount, int ConsecutiveUnstarted);

    static async Task<GenerationRow> ReadGenerationAsync(PgTx tx, Guid jobId, int generation)
    {
        using var g = GenerationSql(tx, jobId, generation);
        var rows = await tx.QueryAsync(g, MapGeneration).ConfigureAwait(false);
        return rows.Count == 1 ? rows[0] : throw new InvalidOperationException("generation row missing");
    }

    static Npgsql.NpgsqlCommand GenerationSql(PgTx tx, Guid jobId, int generation) => tx.Sql("""
        SELECT max_executions, max_unstarted_abandonments, execution_timeout_ms,
               retry_policy::text, unstarted_retry_policy::text, started_count, consecutive_unstarted_count
        FROM runwick.job_generations
        WHERE application = @application AND job_id = @job_id AND generation = @generation
        """).P("job_id", jobId).P("generation", generation);

    static GenerationRow MapGeneration(Npgsql.NpgsqlDataReader r) => new(
        new ExecutionPolicy(r.GetInt32(0), r.IsDBNull(2) ? null : r.GetInt64(2), ReadRetry(r.GetString(3)), r.GetInt32(1), ReadRetry(r.GetString(4))),
        r.GetInt32(5), r.GetInt32(6));

    sealed record ScopeRow(Guid ScopeId, int Capacity, bool Fencing, long FencingCounter);

    /// <summary>S: the job's required scopes, locked in UUID order.</summary>
    static async Task<List<ScopeRow>> LockScopesAsync(PgTx tx, Guid jobId)
    {
        using var s = LockScopesSql(tx, jobId);
        return await tx.QueryAsync(s, MapScope).ConfigureAwait(false);
    }

    static Npgsql.NpgsqlCommand LockScopesSql(PgTx tx, Guid jobId) => tx.Sql("""
        SELECT s.scope_id, s.capacity, s.fencing_enabled, s.fencing_counter
        FROM runwick.concurrency_scopes AS s
        WHERE s.application = @application
          AND s.scope_id IN (SELECT r.scope_id FROM runwick.job_concurrency_requirements AS r
                             WHERE r.application = @application AND r.job_id = @job_id)
        ORDER BY s.scope_id
        FOR UPDATE OF s
        """).P("job_id", jobId);

    static ScopeRow MapScope(Npgsql.NpgsqlDataReader r) => new(r.GetGuid(0), r.GetInt32(1), r.GetBoolean(2), r.GetInt64(3));

    static async Task EventAsync(PgTx tx, Guid jobId, Guid? attemptId, string kind, JsonObject details)
    {
        using var e = EventSql(tx, jobId, attemptId, kind, details);
        await tx.ExecAsync(e).ConfigureAwait(false);
    }

    static void EventInto(PgBatch batch, PgTx tx, Guid jobId, Guid? attemptId, string kind, JsonObject details) =>
        batch.Exec(EventSql(tx, jobId, attemptId, kind, details));

    static Npgsql.NpgsqlCommand EventSql(PgTx tx, Guid jobId, Guid? attemptId, string kind, JsonObject details) => tx.Sql("""
        INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
        VALUES (@application, @job_id, @attempt_id, @command_id, @kind, @now, @details)
        """).P("job_id", jobId).P("attempt_id", attemptId, NpgsqlDbType.Uuid).P("command_id", tx.Op.CommandId)
            .P("kind", kind).P("now", tx.Now).Json("details", details);

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

                // J, then G with the rate-requirement EXISTS and the clock, in one round trip (PF-04). For an unrated
                // authorize plan J is the last blocking lock, so that clock is the decision time; any other plan locks
                // more rows and reads the clock again behind them.
                var first = tx.Batch();
                var jq = first.Query(LockJobSql(tx, lease.JobId), MapJob);
                var gq = first.Query(GenerationOfLockedJobSql(tx, lease.JobId), MapGenerationRead);
                var clock = first.Query(PgTx.ClockSql(tx), PgTx.ReadClock);
                await tx.RunAsync(first).ConfigureAwait(false);
                var job = jq.Items.Count == 1 ? jq.Items[0] : null;
                var read = job is null ? null : gq.Items.Count == 1 ? gq.Items[0] : throw new InvalidOperationException("generation row missing");
                var gen = read?.Gen;
                var plan = job is null ? "none"
                    : job.CancelRequestedAt is not null ? "cancel"
                    : gen!.StartedCount >= gen.Policy.MaxExecutions ? "exhausted"
                    : "authorize";
                // Rate step (10 §4): concurrency scopes, then rate scopes, both in UUID order. Cancellation and an
                // exhausted budget are decided before any charge, so their plans never lock rate scopes. K, S, R and
                // the fresh clock go in one batch, in lock order.
                var rated = job is not null && plan == "authorize" && read!.Rated;
                List<RateScopeRow> rateScopes = [];
                PgBatch.Rows<RateWindowRow>? windows = null;
                if (job is not null && (plan != "authorize" || rated))
                {
                    var more = tx.Batch();
                    if (plan is "cancel" or "exhausted" && job.ActiveKey is not null)
                    {
                        more.Query(ActiveKeyOfLockedJobSql(tx, lease.JobId), _ => 0);
                    }

                    more.Query(LockScopesSql(tx, lease.JobId), MapScope);
                    var rq = rated ? more.Query(LockRateScopesSql(tx, lease.JobId), MapRateScope) : null;
                    if (rated)
                    {
                        // A rated plan reads the clock in the same statement as the window counts it decides on (PF-04
                        // leftover): behind the rate-scope lock, so both are fresh after the last blocking lock.
                        windows = more.Query(RateClockAndWindowsSql(tx, lease.JobId), MapRateWindow);
                    }
                    else
                    {
                        clock = more.Query(PgTx.ClockSql(tx), PgTx.ReadClock);
                    }

                    await tx.RunAsync(more).ConfigureAwait(false);
                    rateScopes = rq?.Items ?? [];
                }

                if (!await tx.DecideAtAsync(windows is not null ? windows.Items[0].Clock : clock.Items[0]).ConfigureAwait(false))
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

                var effects = tx.Batch();
                Action? charged = null;
                if (rateScopes.Count > 0)
                {
                    (deferred, charged) = await ChargeRatesAsync(tx, job!, gen!, lease, rateScopes, windows!.Items, effects).ConfigureAwait(false);
                    if (deferred is not null)
                    {
                        return new PgOutcome<BegunExecution>.Commit(
                            new StoreReply<BegunExecution>(StoreStatus.RateLimited, StoreEffect.Applied, false, null,
                                new StoreError("rate_limited", Detail: PgTime.Ms(deferred.EligibleAt).ToString(System.Globalization.CultureInfo.InvariantCulture)),
                                tx.Now),
                            ReceiptCodec.BeginDeferred(deferred));
                    }
                }

                // The effects and R4 in one round trip (PF-04). The job row is locked, so the execution number, total and
                // new revision are known now; the returned values are checked against them before COMMIT.
                var executionNumber = gen!.StartedCount + 1;
                var total = read!.ExecutionCount + 1;
                var newRevision = job!.Revision + 1;
                var gu = effects.Query(tx.Sql("""
                    UPDATE runwick.job_generations
                    SET started_count = started_count + 1, consecutive_unstarted_count = 0
                    WHERE application = @application AND job_id = @job_id AND generation = @generation AND started_count < max_executions
                    RETURNING started_count
                    """).P("job_id", lease.JobId).P("generation", lease.Generation), r => r.GetInt32(0), mutation: true);
                var ju = effects.Query(tx.Sql("""
                    UPDATE runwick.jobs
                    SET state = 'running', execution_count = execution_count + 1, revision = revision + 1, updated_at = @now
                    WHERE application = @application AND job_id = @job_id AND state = 'reserved'
                      AND current_attempt_id = @attempt_id AND ownership_token = @token
                    RETURNING execution_count, revision, lease_expires_at
                    """).P("now", tx.Now).P("job_id", lease.JobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken),
                    r => (r.GetInt64(0), r.GetInt64(1), PgTime.Read(r, 2)), mutation: true);
                var au = effects.Exec(tx.Sql("""
                    UPDATE runwick.job_attempts
                    SET execution_number = @execution_number, authorized_at = @now
                    WHERE application = @application AND attempt_id = @attempt_id AND finished_at IS NULL AND execution_number IS NULL
                    """).P("execution_number", executionNumber).P("now", tx.Now).P("attempt_id", lease.AttemptId));
                EventInto(effects, tx, lease.JobId, lease.AttemptId, "attempt_started",
                    new JsonObject { ["generation"] = lease.Generation, ["executionNumber"] = executionNumber });
                var begun = new BegunExecution(
                    new JobPosition(lease.JobId, lease.Generation, newRevision, JobStates.Running),
                    new LeaseGrant(lease, job.LeaseExpiresAt!.Value), executionNumber, total, tx.Now);
                var invalid = new PgOutcome<BegunExecution>.Rollback(tx.Reject<BegunExecution>(StoreStatus.InvariantFault, "invalid_transition"));
                return new PgOutcome<BegunExecution>.Commit(StoreReply<BegunExecution>.Applied(begun, tx.Now), ReceiptCodec.Begin(begun))
                {
                    Effects = effects,
                    Verify = () =>
                    {
                        charged?.Invoke();
                        return gu.Items is [var n] && n == executionNumber
                            && ju.Items is [var u] && u == (total, newRevision, job.LeaseExpiresAt.Value)
                            && au.Affected == 1
                            ? null
                            : invalid;
                    },
                };
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

        // J, S, the fresh clock, the job and permit updates and their checks run in runwick.renew_v1 (04 §7.6, §7.17).
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Renew, command.NotAfter, null, PgLane.Renewal),
            new PgCall("renew_v1").P("p_job_id", lease.JobId).P("p_attempt_id", lease.AttemptId).P("p_worker_id", lease.WorkerInstanceId)
                .P("p_token", lease.OwnershipToken).P("p_generation", lease.Generation).P("p_lease", command.Body.LeaseDuration),
            row => row.HasValue
                ? row.Applied(new RenewedLease(new LeaseGrant(lease, PgTime.FromMs(row.Result!["leaseExpiresAt"]!.GetValue<long>())),
                    row.Result!["cancelRequested"]!.GetValue<bool>()))
                : row.Reject<RenewedLease>(),
            ct);
    }
}
