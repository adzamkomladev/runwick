using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;
using Runwick.Serialization;
using Runwick.Worker;

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

    static JobRow MapJob(Npgsql.NpgsqlDataReader r) => MapJobAt(r, 0);

    /// <summary>J's columns starting at ordinal <paramref name="o"/> (RenewMany reads the job ID first).</summary>
    static JobRow MapJobAt(Npgsql.NpgsqlDataReader r, int o) => new(
        r.GetString(o), r.GetInt32(o + 1), r.GetInt64(o + 2),
        r.IsDBNull(o + 3) ? null : r.GetGuid(o + 3), r.IsDBNull(o + 4) ? null : r.GetGuid(o + 4), r.IsDBNull(o + 5) ? null : r.GetGuid(o + 5),
        PgTime.ReadNullable(r, o + 6), PgTime.ReadNullable(r, o + 7), r.IsDBNull(o + 8) ? null : r.GetString(o + 8), r.GetString(o + 9), PgTime.Read(r, o + 10));

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

    static void Expect(int actual, int expected)
    {
        if (actual != expected)
        {
            throw new InvariantViolationException("invalid_transition");
        }
    }

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
        // J, G, K (a terminal close), S, the fresh clock, the ownership check, the retry delay and A -> P -> J' -> K' -> E
        // with R4 run in runwick.finish_v2 (behind finish_v3, which then stores the attempt's usage) through rw_finish_job_v1 (04 §7.7); staged, the same call runs inside the caller's savepoint (08 §8.1).
        var op = Op(command.Application, command.CommandId, Operations.Finish, command.NotAfter, fingerprint, PgLane.Settlement);
        var call = new PgCall("finish_v3").P("p_job_id", lease.JobId).P("p_attempt_id", lease.AttemptId).P("p_worker_id", lease.WorkerInstanceId)
            .P("p_token", lease.OwnershipToken).P("p_generation", lease.Generation).P("p_outcome", Snake(request.Outcome))
            .P("p_elapsed_ms", (long)Math.Max(0, request.ExecutionElapsed.TotalMilliseconds))
            .P("p_requested_delay_ms", request.RequestedRetryDelay is { } d ? (long?)Math.Max(1, CeilMs(d)) : null)
            .Json("p_error", ErrorJson(request.Error)).Entropy("p_entropy")
            .P("p_usage", request.Usage is { } u ? ResourceUsageJson.Write(u) : null, NpgsqlDbType.Jsonb);
        return transaction is null
            ? runner.CallAsync(op, call, MapFinished, ct)
            : runner.StageCallAsync(transaction, op, call, MapFinished, ct);
    }

    static long CeilMs(TimeSpan value) => (value.Ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;

    static StoreReply<FinishedAttempt> MapFinished(PgRow row) =>
        row.HasValue ? row.Applied(ReceiptCodec.ReadFinished(row.Result!)) : row.Reject<FinishedAttempt>();

    // ------------------------------------------------------------------ Recover/1 (04 §7.8)

    public Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Recover, command.CommandId, command.NotAfter,
            CommandEnvelopes.Recover(request));
        // J, the expected-attempt check, G, K (a terminal close), S, the fresh clock, the lease check, the unstarted count,
        // the retry delay and A -> P -> J' -> K' -> E with R4 run in runwick.recover_v1 (04 §7.8).
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Recover, command.NotAfter, fingerprint, PgLane.Background),
            new PgCall("recover_v1").P("p_job_id", request.JobId).P("p_attempt_id", request.ExpectedAttemptId)
                .P("p_token", request.ExpectedOwnershipToken).Entropy("p_entropy"),
            MapFinished,
            ct);
    }

    // ------------------------------------------------------------------ ReleaseReservation/1 (04 §7.9)

    public Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.ReleaseReservation, command.CommandId, command.NotAfter,
            CommandEnvelopes.Release(command.Body));
        // J, G, K (a cancelled release), S, the fresh clock, the ownership check and A -> P -> J' -> K' -> E with R4 run in
        // runwick.release_reservation_v1 (04 §7.9).
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.ReleaseReservation, command.NotAfter, fingerprint, PgLane.Settlement),
            new PgCall("release_reservation_v1").P("p_job_id", lease.JobId).P("p_attempt_id", lease.AttemptId)
                .P("p_worker_id", lease.WorkerInstanceId).P("p_token", lease.OwnershipToken).P("p_generation", lease.Generation),
            row => row.HasValue ? row.Applied(ReceiptCodec.ReadReleased(row.Result!)) : row.Reject<ReleasedReservation>(),
            ct);
    }
}

/// <summary>A statement affected an unexpected number of rows; the runner maps it to InvariantFault and rolls back.</summary>
internal sealed class InvariantViolationException(string code) : Exception(code)
{
    public string Code { get; } = code;
}
