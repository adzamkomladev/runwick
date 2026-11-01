using System.Security.Cryptography;
using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Postgres.Store;

internal sealed partial class PgStore
{
    sealed record JobRow(
        string State, int Generation, long Revision, Guid? AttemptId, Guid? WorkerId, Guid? Token,
        DateTimeOffset? LeaseExpiresAt, DateTimeOffset? CancelRequestedAt, string? ActiveKey, string JobName, DateTimeOffset EligibleAt);

    /// <summary>J: the job row under FOR NO KEY UPDATE (04 §7.5).</summary>
    static async Task<JobRow?> LockJobAsync(PgTx tx, Guid jobId)
    {
        using var j = LockJobSql(tx, jobId);
        var rows = await tx.QueryAsync(j, MapJob).ConfigureAwait(false);
        return rows.Count == 1 ? rows[0] : null;
    }

    static Npgsql.NpgsqlCommand LockJobSql(PgTx tx, Guid jobId) => tx.Sql("""
        SELECT state, generation, revision, current_attempt_id, worker_instance_id, ownership_token,
               lease_expires_at, cancel_requested_at, active_unique_key, job_name, eligible_at
        FROM runwick.jobs
        WHERE application = @application AND job_id = @job_id
        FOR NO KEY UPDATE
        """).P("job_id", jobId);

    static JobRow MapJob(Npgsql.NpgsqlDataReader r) => new(
        r.GetString(0), r.GetInt32(1), r.GetInt64(2),
        r.IsDBNull(3) ? null : r.GetGuid(3), r.IsDBNull(4) ? null : r.GetGuid(4), r.IsDBNull(5) ? null : r.GetGuid(5),
        PgTime.ReadNullable(r, 6), PgTime.ReadNullable(r, 7), r.IsDBNull(8) ? null : r.GetString(8), r.GetString(9), PgTime.Read(r, 10));

    /// <summary>The locked job's current generation row, its execution count and whether it has rate requirements.</summary>
    sealed record GenerationRead(GenerationRow Gen, long ExecutionCount, bool Rated);

    /// <summary>
    /// G behind J in the same batch (PF-04): a separate statement after the job lock, so it reads the generation the
    /// locked row names with a snapshot taken after the lock was granted (a join inside the locking statement would
    /// re-check a stale generation row). The rate-requirement EXISTS is safe anywhere: requirements are immutable.
    /// </summary>
    static Npgsql.NpgsqlCommand GenerationOfLockedJobSql(PgTx tx, Guid jobId) => tx.Sql("""
        SELECT g.max_executions, g.max_unstarted_abandonments, g.execution_timeout_ms,
               g.retry_policy::text, g.unstarted_retry_policy::text, g.started_count, g.consecutive_unstarted_count,
               j.execution_count,
               EXISTS (SELECT 1 FROM runwick.job_rate_requirements AS rr WHERE rr.application = j.application AND rr.job_id = j.job_id)
        FROM runwick.jobs AS j
        JOIN runwick.job_generations AS g ON g.application = j.application AND g.job_id = j.job_id AND g.generation = j.generation
        WHERE j.application = @application AND j.job_id = @job_id
        """).P("job_id", jobId);

    static GenerationRead MapGenerationRead(Npgsql.NpgsqlDataReader r) => new(
        new GenerationRow(new ExecutionPolicy(r.GetInt32(0), r.IsDBNull(2) ? null : r.GetInt64(2), ReadRetry(r.GetString(3)), r.GetInt32(1), ReadRetry(r.GetString(4))),
            r.GetInt32(5), r.GetInt32(6)),
        r.GetInt64(7), r.GetBoolean(8));

    /// <summary>K for the locked job, found through its row (the job is locked, so its key cannot change underneath).</summary>
    static Npgsql.NpgsqlCommand ActiveKeyOfLockedJobSql(PgTx tx, Guid jobId) => tx.Sql("""
        SELECT k.bound_job_id
        FROM runwick.dispatch_keys AS k
        JOIN runwick.jobs AS j ON j.application = k.application AND j.job_name = k.job_name AND j.active_unique_key = k.key
        WHERE j.application = @application AND j.job_id = @job_id AND k.key_kind = 'active'
        FOR UPDATE OF k
        """).P("job_id", jobId);

    /// <summary>K: the active key row, locked only when the transition may release it.</summary>
    static async Task LockActiveKeyAsync(PgTx tx, JobRow job)
    {
        if (job.ActiveKey is null)
        {
            return;
        }

        using var k = tx.Sql("""
            SELECT bound_job_id FROM runwick.dispatch_keys
            WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key
            FOR UPDATE
            """).P("job_name", job.JobName).P("key", job.ActiveKey);
        await tx.QueryAsync(k, r => 0).ConfigureAwait(false);
    }

    /// <summary>The ownership check (03 §6.6).</summary>
    static (StoreStatus Status, string Code)? Ownership(JobRow? job, LeaseHandle lease, DateTimeOffset now, params string[] states)
    {
        if (job is null)
        {
            return (StoreStatus.NotFound, "job_not_found");
        }

        if (job.AttemptId != lease.AttemptId)
        {
            return JobStates.IsTerminal(job.State) ? (StoreStatus.InvalidState, "job_terminal") : (StoreStatus.StaleLease, "stale_lease");
        }

        if (job.Token != lease.OwnershipToken || job.WorkerId != lease.WorkerInstanceId || job.Generation != lease.Generation)
        {
            return (StoreStatus.StaleLease, "ownership_mismatch");
        }

        if (!(job.LeaseExpiresAt > now))
        {
            return (StoreStatus.StaleLease, "lease_expired");
        }

        return states.Contains(job.State) ? null : (StoreStatus.InvalidState, "invalid_state");
    }

    sealed record Close(
        JobRow Job, Guid JobId, Guid AttemptId, Guid Token, string Outcome, string NewState, string? TerminalReason, string EventKind)
    {
        public long? ElapsedMs { get; init; }

        public long? RetryDelayMs { get; init; }

        public DateTimeOffset? NextEligibleAt { get; init; }

        public JsonObject? Error { get; init; }

        public JsonObject? EventDetails { get; init; }
    }

    /// <summary>A → P → J' → K' → E (04 §7.10), sent as one batch. Returns the job's new revision.</summary>
    static async Task<long> CloseAsync(PgTx tx, Close c)
    {
        var batch = tx.Batch();
        var verify = CloseInto(batch, tx, c);
        await tx.RunAsync(batch).ConfigureAwait(false);
        verify();
        return c.Job.Revision + 1;
    }

    /// <summary>
    /// Adds A → P → J' → K' → E (04 §7.10) to <paramref name="batch"/> in that order (PF-04). The job's new revision is
    /// <c>Job.Revision + 1</c>: the job row is locked, so it is known before the batch is sent. The returned check runs
    /// after the batch and throws <see cref="InvariantViolationException"/> when a row count or the revision differs.
    /// </summary>
    static Action CloseInto(PgBatch batch, PgTx tx, Close c)
    {
        var now = tx.Now;
        var attempt = batch.Exec(tx.Sql("""
            UPDATE runwick.job_attempts
            SET finished_at = @now, outcome = @outcome, disposition = @disposition, applied_reason = @reason,
                execution_elapsed_ms = @elapsed_ms, retry_delay_ms = @retry_delay_ms, next_eligible_at = @next_eligible_at,
                error = @error, closed_by_command_id = @command_id
            WHERE application = @application AND attempt_id = @attempt_id AND job_id = @job_id AND finished_at IS NULL
            """).P("now", now).P("outcome", c.Outcome).P("disposition", c.NewState).P("reason", c.TerminalReason)
                .P("elapsed_ms", c.ElapsedMs, NpgsqlDbType.Bigint).P("retry_delay_ms", c.RetryDelayMs, NpgsqlDbType.Bigint)
                .P("next_eligible_at", c.NextEligibleAt).Json("error", c.Error).P("command_id", tx.Op.CommandId)
                .P("attempt_id", c.AttemptId).P("job_id", c.JobId));

        batch.Exec(tx.Sql("""
            DELETE FROM runwick.concurrency_permits
            WHERE application = @application AND job_id = @job_id AND attempt_id = @attempt_id AND ownership_token = @token
            """).P("job_id", c.JobId).P("attempt_id", c.AttemptId).P("token", c.Token));

        var job = batch.Query(tx.Sql("""
            UPDATE runwick.jobs
            SET state = @new_state,
                eligible_at = COALESCE(@new_eligible_at, eligible_at),
                current_attempt_id = NULL, worker_instance_id = NULL, ownership_token = NULL, lease_expires_at = NULL,
                terminal_at = CASE WHEN @new_state IN ('succeeded', 'failed', 'cancelled') THEN @now END,
                terminal_reason = @reason,
                revision = revision + 1,
                updated_at = @now
            WHERE application = @application AND job_id = @job_id AND current_attempt_id = @attempt_id AND ownership_token = @token
            RETURNING revision
            """).P("new_state", c.NewState).P("new_eligible_at", c.NextEligibleAt).P("now", now).P("reason", c.TerminalReason)
                .P("job_id", c.JobId).P("attempt_id", c.AttemptId).P("token", c.Token), r => r.GetInt64(0), mutation: true);

        if (JobStates.IsTerminal(c.NewState) && c.Job.ActiveKey is not null)
        {
            batch.Exec(tx.Sql("""
                UPDATE runwick.dispatch_keys
                SET bound_job_id = NULL, updated_at = @now
                WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key AND bound_job_id = @job_id
                """).P("now", now).P("job_name", c.Job.JobName).P("key", c.Job.ActiveKey).P("job_id", c.JobId));
        }

        var details = c.EventDetails ?? [];
        details["generation"] = c.Job.Generation;
        details["outcome"] = c.Outcome;
        details["disposition"] = c.NewState;
        if (c.TerminalReason is not null)
        {
            details["terminalReason"] = c.TerminalReason;
        }

        if (c.NextEligibleAt is { } eligible)
        {
            details["eligibleAt"] = PgTime.Ms(eligible);
        }

        EventInto(batch, tx, c.JobId, c.AttemptId, c.EventKind, details);
        var expected = c.Job.Revision + 1;
        return () =>
        {
            Expect(attempt.Affected, 1);
            Expect(job.Items.Count, 1);
            Expect(job.Items[0] == expected ? 1 : 0, 1);
        };
    }

    static void Expect(int actual, int expected)
    {
        if (actual != expected)
        {
            throw new InvariantViolationException("invalid_transition");
        }
    }

    /// <summary>Retry/unstarted delay (03 §6.2) through the shared evaluator; entropy is drawn once per deciding transaction.</summary>
    internal static long SampleDelayMs(RetryPolicy policy, int n) =>
        (long)Execution.RetryEvaluator.Delay(n, TimeSpan.FromMilliseconds(policy.InitialDelayMs), TimeSpan.FromMilliseconds(policy.MaximumDelayMs),
            BitConverter.ToUInt64(RandomNumberGenerator.GetBytes(8))).TotalMilliseconds;

    static string Snake(AttemptOutcome outcome) => outcome switch
    {
        AttemptOutcome.Succeeded => "succeeded",
        AttemptOutcome.RetryableFailure => "retryable_failure",
        AttemptOutcome.PermanentFailure => "permanent_failure",
        AttemptOutcome.UserCancelled => "user_cancelled",
        AttemptOutcome.Interrupted => "interrupted",
        AttemptOutcome.TimedOut => "timed_out",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome)),
    };

    static JsonObject? ErrorJson(AttemptError? e) => e is null ? null : new JsonObject
    {
        ["code"] = e.Code, ["exceptionType"] = e.ExceptionType, ["message"] = e.Message, ["stackTrace"] = e.StackTrace,
    };

    // ------------------------------------------------------------------ Finish/1 (04 §7.7)

    public Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct) =>
        FinishCoreAsync(command, null, ct);

    /// <summary>R6 (08 §8.1): Finish/1 inside a savepoint on the caller's transaction; the caller commits.</summary>
    public Task<StoreReply<FinishedAttempt>> StageFinishAsync(Npgsql.NpgsqlTransaction transaction, StoreCommand<FinishRequest> command, CancellationToken ct) =>
        FinishCoreAsync(command, transaction, ct);

    Task<StoreReply<T>> Run<T>(Npgsql.NpgsqlTransaction? transaction, PgOp op, Func<PgTx, Task<PgOutcome<T>>> body,
        Func<PgTx, ReceiptRow, Task<StoreReply<T>>>? replay, CancellationToken ct) where T : class =>
        transaction is null ? runner.ExecuteAsync(op, body, replay, ct) : runner.StageAsync(transaction, op, body, replay, ct);

    Task<StoreReply<FinishedAttempt>> FinishCoreAsync(StoreCommand<FinishRequest> command, Npgsql.NpgsqlTransaction? transaction, CancellationToken ct)
    {
        var request = command.Body;
        var lease = request.Lease;
        if (request.RequestedRetryDelay is { } requested && requested < TimeSpan.Zero)
        {
            return Task.FromResult(StoreReply<FinishedAttempt>.NoEffect(StoreStatus.InvalidRequest, new StoreError("requested_delay_invalid"), time.GetUtcNow()));
        }

        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Finish, command.CommandId, command.NotAfter,
            CommandEnvelopes.Finish(request));
        return Run(transaction,
            Op(command.Application, command.CommandId, Operations.Finish, command.NotAfter, fingerprint, PgLane.Settlement),
            async tx =>
            {
                // J, G, K, S and the decision clock in one round trip when the outcome is terminal whatever the job
                // says (success, user cancel, permanent failure): K precedes S as in 04 §5, and K is found through the
                // locked job row. Otherwise J and G go first, and K (if terminal), S and the clock follow (PF-04).
                var terminalAnyway = request.Outcome is AttemptOutcome.Succeeded or AttemptOutcome.UserCancelled or AttemptOutcome.PermanentFailure;
                var locks = tx.Batch();
                var jq = locks.Query(LockJobSql(tx, lease.JobId), MapJob);
                var gq = locks.Query(GenerationOfLockedJobSql(tx, lease.JobId), MapGenerationRead);
                PgBatch.Rows<DateTimeOffset>? clock = null;
                if (terminalAnyway)
                {
                    locks.Query(ActiveKeyOfLockedJobSql(tx, lease.JobId), _ => 0);
                    locks.Query(LockScopesSql(tx, lease.JobId), MapScope);
                    clock = locks.Query(PgTx.ClockSql(tx), PgTx.ReadClock);
                }

                await tx.RunAsync(locks).ConfigureAwait(false);
                var job = jq.Items.Count == 1 ? jq.Items[0] : null;
                var gen = job is null ? null : gq.Items.Count == 1 ? gq.Items[0].Gen : throw new InvalidOperationException("generation row missing");
                var cancel = job?.CancelRequestedAt is not null;
                var budget = gen is not null && gen.StartedCount < gen.Policy.MaxExecutions;
                var (state, reason) = request.Outcome switch
                {
                    AttemptOutcome.Succeeded => (JobStates.Succeeded, (string?)"succeeded"),
                    AttemptOutcome.UserCancelled => (JobStates.Cancelled, "user_cancelled"),
                    _ when cancel => (JobStates.Cancelled, "cancellation_requested"),
                    AttemptOutcome.PermanentFailure => (JobStates.Failed, "permanent_failure"),
                    _ when budget => (JobStates.Retrying, null),
                    _ => (JobStates.Failed, "execution_budget_exhausted"),
                };
                if (clock is null)
                {
                    var rest = tx.Batch();
                    if (job is not null && JobStates.IsTerminal(state) && job.ActiveKey is not null)
                    {
                        rest.Query(ActiveKeyOfLockedJobSql(tx, lease.JobId), _ => 0);
                    }

                    if (job is not null)
                    {
                        rest.Query(LockScopesSql(tx, lease.JobId), MapScope);
                    }

                    clock = rest.Query(PgTx.ClockSql(tx), PgTx.ReadClock);
                    await tx.RunAsync(rest).ConfigureAwait(false);
                }

                if (!await tx.DecideAtAsync(clock.Items[0]).ConfigureAwait(false))
                {
                    return new PgOutcome<FinishedAttempt>.Rollback(tx.Reject<FinishedAttempt>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (Ownership(job, lease, tx.Now, JobStates.Running) is { } refusal)
                {
                    return new PgOutcome<FinishedAttempt>.Rollback(tx.Reject<FinishedAttempt>(refusal.Status, refusal.Code));
                }

                long? delay = null;
                DateTimeOffset? eligible = null;
                if (state == JobStates.Retrying)
                {
                    var retry = gen!.Policy.Retry;
                    delay = request.Outcome == AttemptOutcome.RetryableFailure && request.RequestedRetryDelay is { } r
                        ? (long)Execution.RetryEvaluator.Requested(r, TimeSpan.FromMilliseconds(retry.MaximumDelayMs)).TotalMilliseconds
                        : SampleDelayMs(retry, gen.StartedCount);
                    eligible = PgTime.Ceil(tx.Now + TimeSpan.FromMilliseconds(delay.Value));
                }

                // The close statements and R4 in one round trip; the counts are checked before COMMIT (PF-04).
                var outcome = Snake(request.Outcome);
                var effects = tx.Batch();
                var verify = CloseInto(effects, tx, new Close(job!, lease.JobId, lease.AttemptId, lease.OwnershipToken, outcome, state, reason, "attempt_finished")
                {
                    ElapsedMs = (long)Math.Max(0, request.ExecutionElapsed.TotalMilliseconds),
                    RetryDelayMs = delay,
                    NextEligibleAt = eligible,
                    Error = ErrorJson(request.Error),
                });
                var finished = new FinishedAttempt(new JobPosition(lease.JobId, job!.Generation, job.Revision + 1, state), outcome, state, reason, eligible);
                return new PgOutcome<FinishedAttempt>.Commit(StoreReply<FinishedAttempt>.Applied(finished, tx.Now),
                    ReceiptCodec.Finished(finished, lease.AttemptId, delay))
                {
                    Effects = effects,
                    Verify = () =>
                    {
                        verify();
                        return null;
                    },
                };
            },
            (tx, row) => Task.FromResult(StoreReply<FinishedAttempt>.Applied(ReceiptCodec.ReadFinished(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct);
    }

    // ------------------------------------------------------------------ Recover/1 (04 §7.8)

    public Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Recover, command.CommandId, command.NotAfter,
            CommandEnvelopes.Recover(request));
        return runner.ExecuteAsync(
            Op(command.Application, command.CommandId, Operations.Recover, command.NotAfter, fingerprint, PgLane.Background),
            async tx =>
            {
                var job = await LockJobAsync(tx, request.JobId).ConfigureAwait(false);
                if (job is null)
                {
                    return new PgOutcome<FinishedAttempt>.Rollback(tx.Reject<FinishedAttempt>(StoreStatus.NotFound, "job_not_found"));
                }

                if (job.AttemptId != request.ExpectedAttemptId || job.Token != request.ExpectedOwnershipToken
                    || job.State is not (JobStates.Reserved or JobStates.Running))
                {
                    return new PgOutcome<FinishedAttempt>.Rollback(tx.Reject<FinishedAttempt>(StoreStatus.StaleLease, "stale_lease"));
                }

                var gen = await ReadGenerationAsync(tx, request.JobId, job.Generation).ConfigureAwait(false);
                var cancel = job.CancelRequestedAt is not null;
                var reserved = job.State == JobStates.Reserved;
                var k = gen.ConsecutiveUnstarted + 1;
                var (state, reason) = (reserved, cancel) switch
                {
                    (true, true) => (JobStates.Cancelled, (string?)"cancelled_before_start"),
                    (true, false) when k >= gen.Policy.MaxConsecutiveUnstartedAbandonments => (JobStates.Failed, "reservation_abandonment_exhausted"),
                    (true, false) => (gen.StartedCount == 0 ? JobStates.Pending : JobStates.Retrying, null),
                    (false, true) => (JobStates.Cancelled, "cancellation_requested"),
                    (false, false) when gen.StartedCount < gen.Policy.MaxExecutions => (JobStates.Retrying, null),
                    _ => (JobStates.Failed, "execution_budget_exhausted"),
                };
                if (JobStates.IsTerminal(state))
                {
                    await LockActiveKeyAsync(tx, job).ConfigureAwait(false);
                }

                await LockScopesAsync(tx, request.JobId).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<FinishedAttempt>.Rollback(tx.Reject<FinishedAttempt>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (job.LeaseExpiresAt > tx.Now)
                {
                    return new PgOutcome<FinishedAttempt>.Rollback(tx.Reject<FinishedAttempt>(StoreStatus.StaleLease, "lease_renewed"));
                }

                if (reserved && !cancel)
                {
                    using var g = tx.Sql("""
                        UPDATE runwick.job_generations
                        SET consecutive_unstarted_count = consecutive_unstarted_count + 1
                        WHERE application = @application AND job_id = @job_id AND generation = @generation
                        """).P("job_id", request.JobId).P("generation", job.Generation);
                    Expect(await tx.ExecAsync(g).ConfigureAwait(false), 1);
                }

                long? delay = null;
                DateTimeOffset? eligible = null;
                if (!JobStates.IsTerminal(state))
                {
                    delay = reserved ? SampleDelayMs(gen.Policy.UnstartedRetry, k) : SampleDelayMs(gen.Policy.Retry, gen.StartedCount);
                    eligible = PgTime.Ceil(tx.Now + TimeSpan.FromMilliseconds(delay.Value));
                }

                var outcome = reserved ? "abandoned_before_start" : "abandoned_running";
                var revision = await CloseAsync(tx, new Close(job, request.JobId, request.ExpectedAttemptId, request.ExpectedOwnershipToken,
                    outcome, state, reason, "attempt_recovered") { RetryDelayMs = delay, NextEligibleAt = eligible }).ConfigureAwait(false);
                var finished = new FinishedAttempt(new JobPosition(request.JobId, job.Generation, revision, state), outcome, state, reason, eligible);
                return new PgOutcome<FinishedAttempt>.Commit(StoreReply<FinishedAttempt>.Applied(finished, tx.Now),
                    ReceiptCodec.Finished(finished, request.ExpectedAttemptId, delay));
            },
            (tx, row) => Task.FromResult(StoreReply<FinishedAttempt>.Applied(ReceiptCodec.ReadFinished(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct);
    }

    // ------------------------------------------------------------------ ReleaseReservation/1 (04 §7.9)

    public Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.ReleaseReservation, command.CommandId, command.NotAfter,
            CommandEnvelopes.Release(command.Body));
        return runner.ExecuteAsync(
            Op(command.Application, command.CommandId, Operations.ReleaseReservation, command.NotAfter, fingerprint, PgLane.Settlement),
            async tx =>
            {
                var job = await LockJobAsync(tx, lease.JobId).ConfigureAwait(false);
                var gen = job is null ? null : await ReadGenerationAsync(tx, lease.JobId, job.Generation).ConfigureAwait(false);
                var cancel = job?.CancelRequestedAt is not null;
                if (job is not null)
                {
                    if (cancel)
                    {
                        await LockActiveKeyAsync(tx, job).ConfigureAwait(false);
                    }

                    await LockScopesAsync(tx, lease.JobId).ConfigureAwait(false);
                }

                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return new PgOutcome<ReleasedReservation>.Rollback(tx.Reject<ReleasedReservation>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (Ownership(job, lease, tx.Now, JobStates.Reserved) is { } refusal)
                {
                    return new PgOutcome<ReleasedReservation>.Rollback(tx.Reject<ReleasedReservation>(refusal.Status, refusal.Code));
                }

                var state = cancel ? JobStates.Cancelled : gen!.StartedCount == 0 ? JobStates.Pending : JobStates.Retrying;
                var revision = await CloseAsync(tx, new Close(job!, lease.JobId, lease.AttemptId, lease.OwnershipToken, "released", state,
                    cancel ? "cancelled_before_start" : null, "attempt_released")).ConfigureAwait(false);
                var released = new ReleasedReservation(new JobPosition(lease.JobId, job!.Generation, revision, state), state,
                    cancel ? null : job.EligibleAt);
                return new PgOutcome<ReleasedReservation>.Commit(StoreReply<ReleasedReservation>.Applied(released, tx.Now),
                    ReceiptCodec.Released(released, lease.AttemptId));
            },
            (tx, row) => Task.FromResult(StoreReply<ReleasedReservation>.Applied(ReceiptCodec.ReadReleased(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct);
    }
}

/// <summary>A statement affected an unexpected number of rows; the runner maps it to InvariantFault and rolls back.</summary>
internal sealed class InvariantViolationException(string code) : Exception(code)
{
    public string Code { get; } = code;
}
