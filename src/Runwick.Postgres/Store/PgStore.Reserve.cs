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
        // Q, C, G, the payload, S, the fresh clock, P, the fences, the attempt, the job, the permits, the event and R4
        // run in runwick.reserve_v1 (04 §7.3); the attempt ID is UUIDv7 of the decision time, built in SQL.
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Reserve, command.NotAfter, fingerprint, PgLane.Admission),
            new PgCall("reserve_v1").P("p_queue", request.Queue).P("p_priority", request.Priority)
                .P("p_after_eligible_at", request.After?.EligibleAt).P("p_after_job_id", request.After?.JobId)
                .Arr("p_names", [.. contracts.Select(x => x.Name)], NpgsqlDbType.Text)
                .Arr("p_versions", [.. contracts.Select(x => x.PayloadVersion)], NpgsqlDbType.Integer)
                .Arr("p_profile_ids", [.. contracts.Select(x => x.SerializerProfileId)], NpgsqlDbType.Text)
                .Arr("p_profile_versions", [.. contracts.Select(x => x.SerializerProfileVersion)], NpgsqlDbType.Integer)
                .P("p_lease", request.LeaseDuration).P("p_worker_id", request.WorkerInstanceId).P("p_token", NewToken()),
            row =>
            {
                if (!row.HasValue)
                {
                    return row.Reject<ReservedJob>();
                }

                var (payload, business, diagnostic, createdAt, schedule) = Payload([(row.Payload!, row.Headers!, row.CreatedAt!.Value)]);
                return row.Applied(ReceiptCodec.ReadReserve(row.Result!, payload, business, diagnostic, createdAt) with { Schedule = schedule });
            },
            ct);
    }

    static Guid NewToken()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        return new Guid(bytes, bigEndian: true);
    }

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
