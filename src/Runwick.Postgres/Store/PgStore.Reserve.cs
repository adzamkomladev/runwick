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
        // J, G, then K/S/R as the plan needs, the fresh clock, the ownership check, the close or the rate step, the
        // effects and R4 run in runwick.begin_v1 (04 §7.5, 10 §4). The defer jitter is drawn here and sent with the call.
        var reply = await runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Begin, command.NotAfter, fingerprint, PgLane.Admission),
            new PgCall("begin_v1").P("p_job_id", lease.JobId).P("p_attempt_id", lease.AttemptId).P("p_worker_id", lease.WorkerInstanceId)
                .P("p_token", lease.OwnershipToken).P("p_generation", lease.Generation)
                .P("p_min_defer", MinimumDefer).P("p_defer_jitter_ms", Random.Shared.Next(0, MaxDeferJitterMs + 1)),
            row =>
            {
                closed = null;
                deferred = null;
                if (!row.HasValue)
                {
                    return row.Reject<BegunExecution>();
                }

                var o = row.Result!;
                if (o.ContainsKey("deferredUntil"))
                {
                    deferred = ReceiptCodec.ReadBeginDeferred(o);
                    return row.Applied<BegunExecution>(null);
                }

                if (o.ContainsKey("attemptOutcome"))
                {
                    closed = ReceiptCodec.ReadBeginClosed(o).Closed;
                    return row.Applied<BegunExecution>(null);
                }

                return row.Applied(ReceiptCodec.ReadBegin(o));
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
