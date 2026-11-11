using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Runwick.Internal;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Sqlite.Store;

internal sealed partial class SqliteStore : ILeaseBatchStore, IReserveAndBeginStore, IFinishManyStore
{
    // Reserve/1, Begin/1, Renew/1, Finish/1, Recover/1, ReleaseReservation/1 and the batches (04 §7.3–§7.10, §7.18–§7.19):
    // the per-job bodies of rw_reserve_job_v1, rw_begin_job_v1, rw_finish_job_v1 and rw_close_v1 in C#, on the write lock.
    // Lock steps (J, K, S, R) have no SQLite counterpart: BEGIN IMMEDIATE already excludes every other writer (20 §7).

    static readonly TimeSpan MinLease = TimeSpan.FromSeconds(1);
    static readonly TimeSpan MaxLease = TimeSpan.FromHours(1);

    /// <summary>As PgStore (10 §4): the least deferral and the jitter added to it.</summary>
    const long MinimumDeferMs = 250;
    const int MaxDeferJitterMs = 100;

    static readonly string[] Terminal = ["succeeded", "failed", "cancelled"];

    sealed record JobRow(
        string State, int Generation, long Revision, Guid? AttemptId, Guid? WorkerId, Guid? Token, DateTimeOffset? LeaseExpiresAt,
        DateTimeOffset? CancelRequestedAt, string? ActiveKey, string JobName, DateTimeOffset EligibleAt, long Executions);

    static JobRow? ReadJob(SqTx tx, Guid jobId) => tx.One(tx.Sql("""
        SELECT state, generation, revision, current_attempt_id, worker_instance_id, ownership_token, lease_expires_at,
               cancel_requested_at, active_unique_key, job_name, eligible_at, execution_count
        FROM runwick_jobs WHERE application = @application AND job_id = @job_id
        """).P("job_id", jobId), r => new JobRow(r.GetString(0), r.GetInt32(1), r.GetInt64(2), r.UuidN(3), r.UuidN(4), r.UuidN(5), r.TimeN(6),
            r.TimeN(7), r.StrN(8), r.GetString(9), r.Time(10), r.GetInt64(11)));

    sealed record GenerationRow(int MaxExecutions, int MaxUnstarted, long? TimeoutMs, string Retry, string UnstartedRetry, int Started, int Unstarted);

    static GenerationRow ReadGeneration(SqTx tx, Guid jobId, int generation) => tx.One(tx.Sql("""
        SELECT max_executions, max_unstarted_abandonments, execution_timeout_ms, retry_policy, unstarted_retry_policy, started_count,
               consecutive_unstarted_count
        FROM runwick_job_generations WHERE application = @application AND job_id = @job_id AND generation = @generation
        """).P("job_id", jobId).P("generation", generation),
        r => new GenerationRow(r.GetInt32(0), r.GetInt32(1), r.LongN(2), r.GetString(3), r.GetString(4), r.GetInt32(5), r.GetInt32(6)))
        ?? throw new InvalidOperationException("generation row missing");

    /// <summary>rw_ownership_v1: null when the lease owns the job in one of <paramref name="states"/>.</summary>
    static SqRow? Ownership(JobRow? job, LeaseHandle lease, DateTimeOffset now, params string[] states)
    {
        if (job is null)
        {
            return SqRow.NoEffect("NotFound", "job_not_found", now);
        }

        if (job.AttemptId != lease.AttemptId)
        {
            return Terminal.Contains(job.State) ? SqRow.NoEffect("InvalidState", "job_terminal", now) : SqRow.NoEffect("StaleLease", "stale_lease", now);
        }

        if (job.Token != lease.OwnershipToken || job.WorkerId != lease.WorkerInstanceId || job.Generation != lease.Generation)
        {
            return SqRow.NoEffect("StaleLease", "ownership_mismatch", now);
        }

        if (!(job.LeaseExpiresAt > now))
        {
            return SqRow.NoEffect("StaleLease", "lease_expired", now);
        }

        return states.Contains(job.State) ? null : SqRow.NoEffect("InvalidState", "invalid_state", now);
    }

    static void Event(SqTx tx, Guid commandId, Guid jobId, Guid? attemptId, string kind, JsonObject details) => tx.Exec(tx.Sql("""
        INSERT INTO runwick_queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
        VALUES (@application, @job_id, @attempt_id, @command_id, @kind, @now, @details)
        """).P("job_id", jobId).P("attempt_id", attemptId).P("command_id", commandId).P("kind", kind).P("now", tx.Now).P("details", details));

    static void Expect(bool ok)
    {
        if (!ok)
        {
            throw new SqInvariantException("invalid_transition");
        }
    }

    static long? Ms(DateTimeOffset? t) => t is { } v ? SqTime.Ms(v) : null;

    /// <summary>A → P → J' → K' → E (rw_close_v1, 04 §7.10) with each row-count check; returns the job's new revision.</summary>
    static long Close(SqTx tx, Guid commandId, Guid jobId, Guid attemptId, Guid token, int generation, long revision, string jobName,
        string? activeKey, string outcome, string newState, string? reason, string eventKind, long? elapsedMs, long? retryDelayMs,
        DateTimeOffset? nextEligible, JsonObject? error, JsonObject? eventDetails, AttemptResourceUsage? usage = null)
    {
        var now = tx.Now;
        Expect(tx.Exec(tx.Sql("""
            UPDATE runwick_job_attempts
            SET finished_at = @now, outcome = @outcome, disposition = @state, applied_reason = @reason, execution_elapsed_ms = @elapsed,
                retry_delay_ms = @delay, next_eligible_at = @eligible, error = @error, closed_by_command_id = @command_id,
                usage = COALESCE(@usage, usage)
            WHERE application = @application AND attempt_id = @attempt_id AND job_id = @job_id AND finished_at IS NULL
            """).P("now", now).P("outcome", outcome).P("state", newState).P("reason", reason).P("elapsed", elapsedMs).P("delay", retryDelayMs)
            .P("eligible", nextEligible).P("error", error).P("command_id", commandId).P("attempt_id", attemptId).P("job_id", jobId)
            .P("usage", usage is null ? null : Worker.ResourceUsageJson.Write(usage))) == 1);

        tx.Exec(tx.Sql("""
            DELETE FROM runwick_concurrency_permits
            WHERE application = @application AND job_id = @job_id AND attempt_id = @attempt_id AND ownership_token = @token
            """).P("job_id", jobId).P("attempt_id", attemptId).P("token", token));

        var terminal = Terminal.Contains(newState);
        var newRevision = tx.Query(tx.Sql("""
            UPDATE runwick_jobs
            SET state = @state, eligible_at = COALESCE(@eligible, eligible_at),
                current_attempt_id = NULL, worker_instance_id = NULL, ownership_token = NULL, lease_expires_at = NULL,
                terminal_at = @terminal_at, terminal_reason = @reason, revision = revision + 1, updated_at = @now
            WHERE application = @application AND job_id = @job_id AND current_attempt_id = @attempt_id AND ownership_token = @token
            RETURNING revision
            """).P("state", newState).P("eligible", nextEligible).P("terminal_at", terminal ? now : null).P("reason", reason).P("now", now)
            .P("job_id", jobId).P("attempt_id", attemptId).P("token", token), r => r.GetInt64(0), mutation: true);
        Expect(newRevision.Count == 1 && newRevision[0] == revision + 1);

        if (terminal && activeKey is not null)
        {
            tx.Exec(tx.Sql("""
                UPDATE runwick_dispatch_keys SET bound_job_id = NULL, updated_at = @now
                WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key AND bound_job_id = @job_id
                """).P("now", now).P("job_name", jobName).P("key", activeKey).P("job_id", jobId));
        }

        var details = eventDetails ?? [];
        details["generation"] = generation;
        details["outcome"] = outcome;
        details["disposition"] = newState;
        if (reason is not null)
        {
            details["terminalReason"] = reason;
        }

        if (nextEligible is { } e)
        {
            details["eligibleAt"] = SqTime.Ms(e);
        }

        Event(tx, commandId, jobId, attemptId, eventKind, details);
        return newRevision[0];
    }

    /// <summary>rw_retry_delay_v1: exponential-v1 with equal jitter from the client's 64-bit draw.</summary>
    internal static long RetryDelay(int n, long initialMs, long maxMs, ulong entropy)
    {
        if (n < 1)
        {
            throw new InvalidOperationException("rw:retry_n");
        }

        var b = n - 1 >= 62 || initialMs > (long.MaxValue >> (n - 1)) ? maxMs : Math.Min(maxMs, initialMs << (n - 1));
        var low = (b + 1) / 2;
        return low + (long)(entropy % (ulong)(b - low + 1));
    }

    static ulong Entropy() => BitConverter.ToUInt64(RandomNumberGenerator.GetBytes(8));

    static Guid NewToken() => new(RandomNumberGenerator.GetBytes(16), bigEndian: true);

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

    static long CeilMs(TimeSpan value) => (value.Ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;

    static StoreReply<T> Invalid<T>(string code, TimeProvider time) where T : class =>
        StoreReply<T>.NoEffect(StoreStatus.InvalidRequest, new StoreError(code), time.GetUtcNow());

    // ------------------------------------------------------------------ Reserve/1 (04 §7.3)

    public Task<StoreReply<ReservedJob>> TryReserveAsync(StoreCommand<ReserveRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var contracts = request.SupportedContracts.Distinct().ToArray();
        if (request.LeaseDuration < MinLease || request.LeaseDuration > MaxLease || contracts.Length is < 1 or > 512)
        {
            return Task.FromResult(Invalid<ReservedJob>("invalid_request", time));
        }

        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Reserve, command.CommandId, command.NotAfter,
            CommandEnvelopes.Reserve(request));
        var token = NewToken();
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Reserve, command.NotAfter, fingerprint, SqLane.Admission),
            tx =>
            {
                if (QueueGate(tx, request.Queue) is { } gate)
                {
                    return gate;
                }

                var ids = Candidates(tx, request.Queue, request.Priority, request.After, contracts, 1);
                if (ids.Count == 0)
                {
                    return SqRow.NoEffect("NoWork", "no_work", null);
                }

                var row = ReserveJob(tx, request.LeaseDuration, request.WorkerInstanceId, token, request.Queue, request.Priority, ids[0].JobId);
                return row.HasValue ? WithPayload(tx, row, ids[0].JobId) : row;
            },
            row =>
            {
                if (!row.HasValue)
                {
                    return row.Reject<ReservedJob>();
                }

                var (business, diagnostic) = ReadHeaders(row.Headers!);
                return row.Applied(ReceiptCodec.ReadReserve(row.Result!, row.Payload!, business, diagnostic, row.CreatedAt!.Value)
                    with { Schedule = ReadScheduleHeader(row.Headers!) });
            },
            ct,
            (tx, replay) => WithPayload(tx, replay, Guid.Parse(replay.Result!["jobId"]!.GetValue<string>())));
    }

    /// <summary>Q (04 §7.3): a paused or unregistered queue claims nothing.</summary>
    static SqRow? QueueGate(SqTx tx, string queue)
    {
        var paused = tx.One(tx.Sql("SELECT paused FROM runwick_queue_controls WHERE application = @application AND queue = @queue").P("queue", queue),
            r => (bool?)r.Bool(0));
        return paused switch
        {
            null => SqRow.NoEffect("NoWork", "queue_not_registered", null),
            true => SqRow.NoEffect("QueuePaused", "queue_paused", null),
            _ => null,
        };
    }

    /// <summary>C: due candidates of the lane with a supported tuple, in (eligible_at, job_id) order (20 §7: json_each tuples).</summary>
    static List<(Guid JobId, DateTimeOffset EligibleAt)> Candidates(SqTx tx, string queue, string priority, CandidateCursor? after,
        IReadOnlyList<JobContract> contracts, int limit)
    {
        var tuples = new JsonArray([.. contracts.Select(c => (JsonNode)new JsonArray(c.Name, c.PayloadVersion, c.SerializerProfileId, c.SerializerProfileVersion))]);
        return tx.Query(tx.Sql("""
            SELECT j.job_id, j.eligible_at FROM runwick_jobs AS j
            WHERE j.application = @application AND j.queue = @queue AND j.priority = @priority AND j.state IN ('pending', 'retrying')
              AND j.eligible_at <= @now
              AND (@after_job IS NULL OR (j.eligible_at, j.job_id) > (@after_at, @after_job))
              AND EXISTS (SELECT 1 FROM json_each(@tuples) AS c
                          WHERE c.value ->> 0 = j.job_name AND c.value ->> 1 = j.payload_version
                            AND c.value ->> 2 = j.serializer_profile_id AND c.value ->> 3 = j.serializer_profile_version)
            ORDER BY j.eligible_at, j.job_id
            LIMIT @limit
            """).P("queue", queue).P("priority", priority).P("now", tx.Now).P("after_job", after?.JobId).P("after_at", after?.EligibleAt)
            .P("tuples", tuples).P("limit", limit), r => (r.Uuid(0), r.Time(1)));
    }

    /// <summary>The reply's payload, headers and creation time (immutable rows; the receipt names the job).</summary>
    static SqRow WithPayload(SqTx tx, SqRow row, Guid jobId)
    {
        var p = tx.One(tx.Sql("""
            SELECT p.payload_utf8, p.headers, j.created_at FROM runwick_job_payloads AS p
            JOIN runwick_jobs AS j ON j.application = p.application AND j.job_id = p.job_id
            WHERE p.application = @application AND p.job_id = @job_id
            """).P("job_id", jobId), r => (r.Blob(0), r.GetString(1), r.Time(2)));
        if (p == default)
        {
            throw new InvalidOperationException("payload row missing for a waiting job");
        }

        (row.Payload, row.Headers, row.CreatedAt) = p;
        return row;
    }

    /// <summary>rw_reserve_job_v1: capacity check, fences, attempt, job, permits and event at the decision time.</summary>
    static SqRow ReserveJob(SqTx tx, TimeSpan leaseDuration, Guid workerId, Guid token, string queue, string priority, Guid jobId)
    {
        var now = tx.Now;
        var job = tx.One(tx.Sql("""
            SELECT state, eligible_at, generation, revision, reservation_count, job_name, payload_version, serializer_profile_id,
                   serializer_profile_version
            FROM runwick_jobs WHERE application = @application AND job_id = @job_id
            """).P("job_id", jobId), r => new
        {
            State = r.GetString(0), Eligible = r.Time(1), Generation = r.GetInt32(2), Revision = r.GetInt64(3), Reservations = r.GetInt64(4),
            Name = r.GetString(5), Version = r.GetInt32(6), ProfileId = r.GetString(7), ProfileVersion = r.GetInt32(8),
        });
        if (job is null || job.State is not ("pending" or "retrying"))
        {
            return SqRow.NoEffect("InvariantFault", "invalid_transition", now);
        }

        var g = ReadGeneration(tx, jobId, job.Generation);
        var scopes = tx.Query(tx.Sql("""
            SELECT s.scope_id, s.capacity, s.fencing_enabled, s.fencing_counter,
                   (SELECT count(*) FROM runwick_concurrency_permits AS p
                    WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.lease_expires_at > @now)
            FROM runwick_concurrency_scopes AS s
            WHERE s.application = @application
              AND s.scope_id IN (SELECT r.scope_id FROM runwick_job_concurrency_requirements AS r WHERE r.application = @application AND r.job_id = @job_id)
            ORDER BY s.scope_id
            """).P("now", now).P("job_id", jobId), r => (Id: r.Uuid(0), Capacity: r.GetInt32(1), Fencing: r.Bool(2), Counter: r.GetInt64(3), Live: r.GetInt64(4)));
        foreach (var s in scopes)
        {
            if (s.Live >= s.Capacity)
            {
                return SqRow.NoEffect("CapacityBlocked", "capacity_blocked", now, blockedScopeId: s.Id, resumeEligibleAt: job.Eligible, resumeJobId: jobId);
            }
        }

        var expiry = SqTime.Floor(now + leaseDuration);
        var attempt = Guid.CreateVersion7(now);
        var fences = new List<(Guid Id, long Value)>();
        foreach (var s in scopes.Where(s => s.Fencing))
        {
            var v = tx.Query(tx.Sql("""
                UPDATE runwick_concurrency_scopes SET fencing_counter = fencing_counter + 1
                WHERE application = @application AND scope_id = @id AND fencing_enabled = 1 RETURNING fencing_counter
                """).P("id", s.Id), r => r.GetInt64(0), mutation: true);
            Expect(v.Count == 1 && v[0] == s.Counter + 1);
            fences.Add((s.Id, v[0]));
        }

        tx.Exec(tx.Sql("""
            INSERT INTO runwick_job_attempts (application, attempt_id, job_id, generation, reservation_number, worker_instance_id, ownership_token, reserved_at)
            VALUES (@application, @attempt_id, @job_id, @generation, @number, @worker, @token, @now)
            """).P("attempt_id", attempt).P("job_id", jobId).P("generation", job.Generation).P("number", job.Reservations + 1)
            .P("worker", workerId).P("token", token).P("now", now));

        var revision = tx.Query(tx.Sql("""
            UPDATE runwick_jobs
            SET state = 'reserved', reservation_count = reservation_count + 1, revision = revision + 1, current_attempt_id = @attempt_id,
                worker_instance_id = @worker, ownership_token = @token, lease_expires_at = @expiry, updated_at = @now
            WHERE application = @application AND job_id = @job_id AND state IN ('pending', 'retrying')
            RETURNING revision
            """).P("attempt_id", attempt).P("worker", workerId).P("token", token).P("expiry", expiry).P("now", now).P("job_id", jobId),
            r => r.GetInt64(0), mutation: true);
        if (revision.Count != 1 || revision[0] != job.Revision + 1)
        {
            return SqRow.NoEffect("InvariantFault", "invalid_transition", now);
        }

        foreach (var s in scopes)
        {
            tx.Exec(tx.Sql("""
                INSERT INTO runwick_concurrency_permits (application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at, fencing_value)
                VALUES (@application, @scope_id, @attempt_id, @job_id, @token, @expiry, @fence)
                """).P("scope_id", s.Id).P("attempt_id", attempt).P("job_id", jobId).P("token", token).P("expiry", expiry)
                .P("fence", fences.Where(f => f.Id == s.Id).Select(f => (long?)f.Value).FirstOrDefault()));
        }

        Event(tx, tx.Op.CommandId, jobId, attempt, "attempt_reserved", new JsonObject { ["generation"] = job.Generation, ["reservationNumber"] = job.Reservations + 1 });

        return SqRow.Applied("Applied", null, now, new JsonObject
        {
            ["jobId"] = jobId, ["generation"] = job.Generation, ["revision"] = revision[0], ["state"] = "reserved",
            ["attemptId"] = attempt, ["workerInstanceId"] = workerId, ["ownershipToken"] = token,
            ["leaseExpiresAt"] = SqTime.Ms(expiry), ["reservationNumber"] = job.Reservations + 1,
            ["contract"] = new JsonObject
            {
                ["name"] = job.Name, ["payloadVersion"] = job.Version, ["serializerProfileId"] = job.ProfileId,
                ["serializerProfileVersion"] = job.ProfileVersion,
            },
            ["queue"] = queue, ["priority"] = priority,
            ["cursor"] = new JsonObject { ["eligibleAt"] = SqTime.Ms(job.Eligible), ["jobId"] = jobId },
            ["policy"] = new JsonObject
            {
                ["executionTimeout"] = g.TimeoutMs, ["fingerprintVersion"] = 1, ["maxConsecutiveUnstartedAbandonments"] = g.MaxUnstarted,
                ["maxExecutions"] = g.MaxExecutions, ["retry"] = JsonNode.Parse(g.Retry), ["unstartedRetry"] = JsonNode.Parse(g.UnstartedRetry),
            },
            ["fences"] = new JsonArray([.. fences.Select(f => (JsonNode)new JsonObject { ["scopeId"] = f.Id, ["value"] = f.Value })]),
        });
    }

    // ------------------------------------------------------------------ Begin/1 (04 §7.5, 10 §4)

    public async Task<BeginReply> BeginExecutionAsync(StoreCommand<BeginRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Begin, command.CommandId, command.NotAfter,
            CommandEnvelopes.Begin(command.Body));
        var jitter = Random.Shared.Next(0, MaxDeferJitterMs + 1);
        BeginClosed? closed = null;
        RateDeferral? deferred = null;
        var reply = await runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Begin, command.NotAfter, fingerprint, SqLane.Admission),
            tx => BeginJob(tx, lease, jitter),
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
            ct,
            (_, replay) => replay.Result!.ContainsKey("deferredUntil")
                ? Recoded(replay, "rate_limited", replay.Result!["deferredUntil"]!.ToJsonString())
                : replay.Result!.ContainsKey("attemptOutcome") ? Recoded(replay, replay.Result!["errorCode"]!.GetValue<string>(), null) : replay).ConfigureAwait(false);
        return new BeginReply(reply, reply.Effect == StoreEffect.Applied ? closed : null)
        {
            Deferred = reply.Status == StoreStatus.RateLimited && reply.Effect == StoreEffect.Applied ? deferred : null,
        };
    }

    /// <summary>A replayed closed or deferred Begin carries its code (begin_v2's replay branch).</summary>
    static SqRow Recoded(SqRow replay, string code, string? detail)
    {
        var row = SqRow.Applied(replay.Status, code, replay.Decided, replay.Result, detail);
        row.Replayed = true;
        return row;
    }

    /// <summary>rw_begin_job_v1: ownership, the cancel/exhausted close, the rate step and its T20 deferral, then authorize.</summary>
    static SqRow BeginJob(SqTx tx, LeaseHandle lease, int jitterMs)
    {
        var now = tx.Now;
        var jobId = lease.JobId;
        var job = ReadJob(tx, jobId);
        GenerationRow? g = null;
        var plan = "authorize";
        var rated = false;
        if (job is not null)
        {
            g = ReadGeneration(tx, jobId, job.Generation);
            plan = job.CancelRequestedAt is not null ? "cancel" : g.Started >= g.MaxExecutions ? "exhausted" : "authorize";
            rated = plan == "authorize" && tx.Count(tx.Sql("SELECT count(*) FROM runwick_job_rate_requirements WHERE application = @application AND job_id = @job_id")
                .P("job_id", jobId)) > 0;
        }

        if (Ownership(job, lease, now, "reserved") is { } refused)
        {
            return refused;
        }

        if (plan != "authorize")
        {
            var cancel = plan == "cancel";
            var outcome = cancel ? "cancelled_before_start" : "abandoned_before_start";
            var reason = cancel ? "cancelled_before_start" : "execution_budget_exhausted";
            var state = cancel ? "cancelled" : "failed";
            var revision = Close(tx, tx.Op.CommandId, jobId, lease.AttemptId, lease.OwnershipToken, job!.Generation, job.Revision, job.JobName,
                job.ActiveKey, outcome, state, reason, cancel ? "attempt_cancelled_before_start" : "attempt_finished", null, null, null, null, null);
            return SqRow.Applied(cancel ? "InvalidState" : "BudgetExhausted", reason, now, new JsonObject
            {
                ["jobId"] = jobId, ["generation"] = job.Generation, ["revision"] = revision, ["state"] = state,
                ["attemptOutcome"] = outcome, ["terminalReason"] = reason, ["errorCode"] = reason,
            });
        }

        if (rated && RateStep(tx, job!, g!, lease, jitterMs) is { } deferredRow)
        {
            return deferredRow;
        }

        var executionNumber = g!.Started + 1;
        var total = job!.Executions + 1;
        var started = tx.Query(tx.Sql("""
            UPDATE runwick_job_generations SET started_count = started_count + 1, consecutive_unstarted_count = 0
            WHERE application = @application AND job_id = @job_id AND generation = @generation AND started_count < max_executions
            RETURNING started_count
            """).P("job_id", jobId).P("generation", lease.Generation), r => r.GetInt32(0), mutation: true);
        if (started.Count != 1 || started[0] != executionNumber)
        {
            return SqRow.NoEffect("InvariantFault", "invalid_transition", now);
        }

        var j = tx.Query(tx.Sql("""
            UPDATE runwick_jobs SET state = 'running', execution_count = execution_count + 1, revision = revision + 1, updated_at = @now
            WHERE application = @application AND job_id = @job_id AND state = 'reserved' AND current_attempt_id = @attempt_id AND ownership_token = @token
            RETURNING execution_count, revision
            """).P("now", now).P("job_id", jobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken),
            r => (Executions: r.GetInt64(0), Revision: r.GetInt64(1)), mutation: true);
        if (j.Count != 1 || j[0].Executions != total || j[0].Revision != job.Revision + 1)
        {
            return SqRow.NoEffect("InvariantFault", "invalid_transition", now);
        }

        if (tx.Exec(tx.Sql("""
            UPDATE runwick_job_attempts SET execution_number = @number, authorized_at = @now
            WHERE application = @application AND attempt_id = @attempt_id AND finished_at IS NULL AND execution_number IS NULL
            """).P("number", executionNumber).P("now", now).P("attempt_id", lease.AttemptId)) != 1)
        {
            return SqRow.NoEffect("InvariantFault", "invalid_transition", now);
        }

        Event(tx, tx.Op.CommandId, jobId, lease.AttemptId, "attempt_started",
            new JsonObject { ["generation"] = lease.Generation, ["executionNumber"] = executionNumber });
        return SqRow.Applied("Applied", null, now, new JsonObject
        {
            ["jobId"] = jobId, ["generation"] = lease.Generation, ["revision"] = j[0].Revision, ["state"] = "running",
            ["attemptId"] = lease.AttemptId, ["workerInstanceId"] = lease.WorkerInstanceId, ["ownershipToken"] = lease.OwnershipToken,
            ["leaseExpiresAt"] = Ms(job.LeaseExpiresAt), ["executionNumber"] = executionNumber, ["totalExecutionNumber"] = total,
            ["authorizedAt"] = SqTime.Ms(now),
        });
    }

    /// <summary>The rate step (10 §4): grants all-or-none at logical time t, or the committed T20 deferral (returned).</summary>
    static SqRow? RateStep(SqTx tx, JobRow job, GenerationRow g, LeaseHandle lease, int jitterMs)
    {
        var now = tx.Now;
        var nowMs = SqTime.Ms(now);
        var scopes = tx.Query(tx.Sql("""
            SELECT s.rate_scope_id, s.limit_count, s.window_ms, s.policy_revision, s.last_decision_at, s.not_before
            FROM runwick_rate_scopes AS s
            WHERE s.application = @application
              AND s.rate_scope_id IN (SELECT r.rate_scope_id FROM runwick_job_rate_requirements AS r WHERE r.application = @application AND r.job_id = @job_id)
            ORDER BY s.rate_scope_id
            """).P("job_id", lease.JobId), r => (Id: r.Uuid(0), Limit: r.GetInt32(1), Window: r.GetInt64(2), Revision: r.GetInt64(3),
                Last: r.LongN(4), NotBefore: r.LongN(5)));
        if (scopes.Count == 0)
        {
            return null;
        }

        var t = Math.Max(nowMs, scopes.Max(s => s.Last ?? long.MinValue));
        var counts = scopes.Select(s => tx.Count(tx.Sql("""
            SELECT count(*) FROM runwick_rate_grants WHERE application = @application AND rate_scope_id = @id AND authorized_at > @since
            """).P("id", s.Id).P("since", t - s.Window))).ToArray();
        var blocked = scopes.Where((s, i) => counts[i] >= s.Limit || (s.NotBefore is { } nb && t < nb)).Select(s => s.Id).ToList();

        // The scopes' logical time never goes backward, whichever way the decision goes.
        foreach (var s in scopes)
        {
            tx.Exec(tx.Sql("""
                UPDATE runwick_rate_scopes SET last_decision_at = @t
                WHERE application = @application AND rate_scope_id = @id AND (last_decision_at IS NULL OR last_decision_at < @t)
                """).P("t", t).P("id", s.Id));
        }

        if (blocked.Count == 0)
        {
            foreach (var s in scopes)
            {
                tx.Exec(tx.Sql("""
                    INSERT INTO runwick_rate_grants (application, rate_scope_id, attempt_id, job_id, authorized_at, policy_revision)
                    VALUES (@application, @id, @attempt_id, @job_id, @t, @revision)
                    """).P("id", s.Id).P("attempt_id", lease.AttemptId).P("job_id", lease.JobId).P("t", t).P("revision", s.Revision));
            }

            return null;
        }

        // Earliest logical instant at which every blocked scope could admit (capacity-control §4); not a reservation.
        var hint = t;
        for (var i = 0; i < scopes.Count; i++)
        {
            var s = scopes[i];
            if (!blocked.Contains(s.Id))
            {
                continue;
            }

            if (counts[i] >= s.Limit)
            {
                var oldest = tx.One(tx.Sql("""
                    SELECT authorized_at FROM runwick_rate_grants WHERE application = @application AND rate_scope_id = @id AND authorized_at > @since
                    ORDER BY authorized_at LIMIT 1 OFFSET @offset
                    """).P("id", s.Id).P("since", t - s.Window).P("offset", counts[i] - s.Limit), r => (long?)r.GetInt64(0));
                if (oldest is { } o && o + s.Window > hint)
                {
                    hint = o + s.Window;
                }
            }

            if (s.NotBefore is { } nb && nb > hint)
            {
                hint = nb;
            }
        }

        var logical = Math.Max(hint, t + MinimumDeferMs);
        var eligible = SqTime.FromMs(nowMs + (logical - t) + jitterMs);
        var state = g.Started == 0 ? "pending" : "retrying";
        var blocking = new JsonArray([.. blocked.Select(b => (JsonNode)JsonValue.Create(b))]);
        var revision = Close(tx, tx.Op.CommandId, lease.JobId, lease.AttemptId, lease.OwnershipToken, job.Generation, job.Revision, job.JobName,
            job.ActiveKey, "admission_deferred", state, null, "attempt_admission_deferred", null, Math.Max(1, SqTime.Ms(eligible) - nowMs), eligible, null,
            new JsonObject { ["blockingScopes"] = blocking.DeepClone(), ["rateTime"] = t });
        var deferredUntil = SqTime.Ms(eligible);
        return SqRow.Applied("RateLimited", "rate_limited", now, new JsonObject
        {
            ["jobId"] = lease.JobId, ["generation"] = job.Generation, ["revision"] = revision, ["state"] = state,
            ["deferredUntil"] = deferredUntil, ["blockingScopes"] = blocking,
        }, deferredUntil.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    // ------------------------------------------------------------------ Renew/1 (04 §7.6) and RenewMany/1

    public Task<StoreReply<RenewedLease>> RenewAsync(StoreCommand<RenewRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        if (command.Body.LeaseDuration < MinLease || command.Body.LeaseDuration > MaxLease)
        {
            return Task.FromResult(Invalid<RenewedLease>("invalid_request", time));
        }

        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Renew, command.NotAfter, null, SqLane.Renewal),
            tx =>
            {
                var job = ReadJob(tx, lease.JobId);
                if (Ownership(job, lease, tx.Now, "reserved", "running") is { } refused)
                {
                    return refused;
                }

                var expiry = SqTime.Floor(tx.Now + command.Body.LeaseDuration);
                return RenewOne(tx, lease, job!, expiry) ?? SqRow.Applied("Applied", null, tx.Now,
                    new JsonObject { ["leaseExpiresAt"] = SqTime.Ms(expiry), ["cancelRequested"] = job!.CancelRequestedAt is not null });
            },
            row => row.HasValue
                ? row.Applied(new RenewedLease(new LeaseGrant(lease, SqTime.FromMs(row.Result!["leaseExpiresAt"]!.GetValue<long>())),
                    row.Result!["cancelRequested"]!.GetValue<bool>()))
                : row.Reject<RenewedLease>(),
            ct);
    }

    /// <summary>Extends one owned lease and its permits; a refusal row when the job or the permit set does not match.</summary>
    static SqRow? RenewOne(SqTx tx, LeaseHandle lease, JobRow job, DateTimeOffset expiry)
    {
        var n = tx.Exec(tx.Sql("""
            UPDATE runwick_jobs SET lease_expires_at = @expiry
            WHERE application = @application AND job_id = @job_id AND current_attempt_id = @attempt_id AND ownership_token = @token
              AND worker_instance_id = @worker AND generation = @generation AND state IN ('reserved', 'running') AND lease_expires_at > @now
            """).P("expiry", expiry).P("job_id", lease.JobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken)
            .P("worker", lease.WorkerInstanceId).P("generation", lease.Generation).P("now", tx.Now));
        var renewed = tx.Query(tx.Sql("""
            UPDATE runwick_concurrency_permits SET lease_expires_at = @expiry
            WHERE application = @application AND job_id = @job_id AND attempt_id = @attempt_id AND ownership_token = @token AND lease_expires_at = @previous
            RETURNING scope_id
            """).P("expiry", expiry).P("job_id", lease.JobId).P("attempt_id", lease.AttemptId).P("token", lease.OwnershipToken)
            .P("previous", job.LeaseExpiresAt), r => r.Uuid(0), mutation: true);
        if (n != 1)
        {
            return SqRow.NoEffect("StaleLease", "lease_expired", tx.Now);
        }

        var required = tx.Query(tx.Sql("SELECT scope_id FROM runwick_job_concurrency_requirements WHERE application = @application AND job_id = @job_id")
            .P("job_id", lease.JobId), r => r.Uuid(0));
        return renewed.Order(UuidOrder.Instance).SequenceEqual(required.Order(UuidOrder.Instance))
            ? null
            : SqRow.NoEffect("InvariantFault", "permit_set_mismatch", tx.Now);
    }

    public Task<StoreReply<RenewedLeases>> RenewManyAsync(StoreCommand<RenewManyRequest> command, CancellationToken ct)
    {
        var leases = command.Body.Leases;
        if (command.Body.LeaseDuration < MinLease || command.Body.LeaseDuration > MaxLease
            || leases.Count is 0 or > ILeaseBatchStore.MaxLeases
            || leases.Select(l => l.JobId).Distinct().Count() != leases.Count)
        {
            return Task.FromResult(Invalid<RenewedLeases>("invalid_request", time));
        }

        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.RenewMany, command.NotAfter, null, SqLane.Renewal),
            tx =>
            {
                // 20 §7.2: each member in its own savepoint; a refused member writes nothing and never rolls back another.
                var expiry = SqTime.Floor(tx.Now + command.Body.LeaseDuration);
                var items = new JsonArray();
                var renewed = 0;
                foreach (var lease in leases)
                {
                    var job = ReadJob(tx, lease.JobId);
                    var refused = Ownership(job, lease, tx.Now, "reserved", "running");
                    if (refused is null)
                    {
                        tx.Raw("SAVEPOINT rw_m");
                        refused = RenewOne(tx, lease, job!, expiry);
                        tx.Raw(refused is null ? "RELEASE rw_m" : "ROLLBACK TO rw_m; RELEASE rw_m");
                    }

                    if (refused is not null)
                    {
                        items.Add(new JsonObject { ["status"] = refused.Status, ["code"] = refused.Code });
                        continue;
                    }

                    renewed++;
                    items.Add(new JsonObject
                    {
                        ["status"] = "Applied", ["leaseExpiresAt"] = SqTime.Ms(expiry), ["cancelRequested"] = job!.CancelRequestedAt is not null,
                    });
                }

                var row = SqRow.Applied("Applied", null, tx.Now, new JsonObject { ["items"] = items });
                row.NoCommit = renewed == 0;
                return row;
            },
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
                            new LeaseGrant(leases[i], SqTime.FromMs(o["leaseExpiresAt"]!.GetValue<long>())), o["cancelRequested"]!.GetValue<bool>()))
                        : new LeaseRenewal(status, o["code"]!.GetValue<string>(), null);
                }

                return row.Applied(new RenewedLeases(outcomes));
            },
            ct);
    }

    // ------------------------------------------------------------------ ReserveAndBegin/1 (04 §7.18)

    public Task<StoreReply<ClaimedJobs>> ReserveAndBeginAsync(StoreCommand<ReserveAndBeginRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var contracts = request.SupportedContracts.Distinct().ToArray();
        if (request.LeaseDuration < MinLease || request.LeaseDuration > MaxLease || contracts.Length is < 1 or > 512
            || request.MaxJobs is < 1 or > IReserveAndBeginStore.MaxJobs)
        {
            return Task.FromResult(Invalid<ClaimedJobs>("invalid_request", time));
        }

        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.ReserveAndBegin, command.CommandId, command.NotAfter,
            CommandEnvelopes.ReserveAndBegin(request));
        var tokens = Enumerable.Range(0, request.MaxJobs).Select(_ => NewToken()).ToArray();
        var jitters = Enumerable.Range(0, request.MaxJobs).Select(_ => Random.Shared.Next(0, MaxDeferJitterMs + 1)).ToArray();
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.ReserveAndBegin, command.NotAfter, fingerprint, SqLane.Admission),
            tx => ReserveAndBeginBody(tx, request, contracts, tokens, jitters),
            row => row.HasValue ? row.Applied(ReadClaimed(row)) : row.Reject<ClaimedJobs>(),
            ct,
            (tx, replay) => WithPayloads(tx, replay, replay.Result!["items"]!.AsArray()
                .Where(i => i!["status"]!.GetValue<string>() == "Applied").Select(i => Guid.Parse(i!["jobId"]!.GetValue<string>()))));
    }

    /// <summary>reserve_and_begin_v1: candidates of Reserve/1 (up to MaxJobs), then per job, in order, Reserve then Begin at one decision time.</summary>
    static SqRow ReserveAndBeginBody(SqTx tx, ReserveAndBeginRequest request, JobContract[] contracts, Guid[] tokens, int[] jitters)
    {
        if (QueueGate(tx, request.Queue) is { } gate)
        {
            return gate;
        }

        var candidates = Candidates(tx, request.Queue, request.Priority, request.After, contracts, request.MaxJobs);
        if (candidates.Count == 0)
        {
            return SqRow.NoEffect("NoWork", "no_work", null);
        }

        var items = new JsonArray();
        var begun = new List<Guid>();
        Guid? firstBlocked = null;
        (Guid JobId, DateTimeOffset EligibleAt) last = default;
        var reserved = false;
        for (var i = 0; i < candidates.Count; i++)
        {
            var (jobId, eligible) = candidates[i];
            var r = ReserveJob(tx, request.LeaseDuration, request.WorkerInstanceId, tokens[i], request.Queue, request.Priority, jobId);
            if (r.Status == "CapacityBlocked")
            {
                firstBlocked ??= r.BlockedScopeId;
                last = (jobId, eligible);
                items.Add(new JsonObject
                {
                    ["ordinal"] = i, ["jobId"] = jobId, ["status"] = "CapacityBlocked", ["code"] = "capacity_blocked", ["blockedScopeId"] = r.BlockedScopeId,
                    ["cursor"] = new JsonObject { ["eligibleAt"] = SqTime.Ms(eligible), ["jobId"] = jobId },
                });
                continue;
            }

            if (!r.HasValue)
            {
                return r;
            }

            reserved = true;
            var lease = new LeaseHandle(jobId, Guid.Parse(r.Result!["attemptId"]!.GetValue<string>()), request.WorkerInstanceId, tokens[i],
                r.Result!["generation"]!.GetValue<int>());
            var b = BeginJob(tx, lease, jitters[i]);
            if (!b.HasValue)
            {
                return b;
            }

            if (b.Status == "Applied")
            {
                begun.Add(jobId);
            }

            items.Add(new JsonObject { ["ordinal"] = i, ["jobId"] = jobId, ["status"] = b.Status, ["code"] = b.Code, ["reserve"] = r.Result, ["begin"] = b.Result });
        }

        if (!reserved)
        {
            return SqRow.NoEffect("CapacityBlocked", "capacity_blocked", tx.Now, blockedScopeId: firstBlocked, resumeEligibleAt: last.EligibleAt, resumeJobId: last.JobId);
        }

        return WithPayloads(tx, SqRow.Applied("Applied", null, tx.Now, new JsonObject { ["items"] = items }), begun);
    }

    static SqRow WithPayloads(SqTx tx, SqRow row, IEnumerable<Guid> jobIds)
    {
        foreach (var id in jobIds)
        {
            var p = WithPayload(tx, SqRow.NoEffect("Payload", "payload", null), id);
            row.More.Add(new SqRow
            {
                Status = "Payload", Effect = "Applied", ConflictingJobId = id, Payload = p.Payload, Headers = p.Headers, CreatedAt = p.CreatedAt,
            });
        }

        return row;
    }

    static ClaimedJobs ReadClaimed(SqRow row)
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
                        ResumeAfter: new CandidateCursor(SqTime.FromMs(cursor["eligibleAt"]!.GetValue<long>()), Guid.Parse(cursor["jobId"]!.GetValue<string>()))),
                        null, null, null, null));
                    break;
                case StoreStatus.Applied:
                    if (!payloads.TryGetValue(jobId, out var p))
                    {
                        throw new InvalidOperationException("payload row missing for a waiting job");
                    }

                    var (business, diagnostic) = ReadHeaders(p.Headers!);
                    outcomes.Add(new ClaimOutcome(jobId, status, null,
                        ReceiptCodec.ReadReserve(item["reserve"]!.AsObject(), p.Payload!, business, diagnostic, p.CreatedAt!.Value)
                            with { Schedule = ReadScheduleHeader(p.Headers!) },
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

    // ------------------------------------------------------------------ Finish/1 (04 §7.7) and FinishMany/1

    public Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> command, CancellationToken ct)
    {
        if (command.Body.RequestedRetryDelay is { } requested && requested < TimeSpan.Zero)
        {
            return Task.FromResult(Invalid<FinishedAttempt>("requested_delay_invalid", time));
        }

        var entropy = Entropy();
        return runner.CallAsync(FinishOp(command), tx => FinishJob(tx, tx.Op.CommandId, command.Body, entropy), MapFinished, ct);
    }

    /// <summary>Finish/1 on a caller-owned transaction (20 §8).</summary>
    public StoreReply<FinishedAttempt> StageFinish(Microsoft.Data.Sqlite.SqliteTransaction transaction, StoreCommand<FinishRequest> command, CancellationToken ct)
    {
        if (command.Body.RequestedRetryDelay is { } requested && requested < TimeSpan.Zero)
        {
            return Invalid<FinishedAttempt>("requested_delay_invalid", time);
        }

        var entropy = Entropy();
        return runner.Stage(transaction, FinishOp(command), tx => FinishJob(tx, tx.Op.CommandId, command.Body, entropy), MapFinished, null, ct);
    }

    SqOp FinishOp(StoreCommand<FinishRequest> command) =>
        Op(command.Application, command.CommandId, Operations.Finish, command.NotAfter, FinishFingerprint(command), SqLane.Settlement);

    byte[] FinishFingerprint(StoreCommand<FinishRequest> command) =>
        CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Finish, command.CommandId, command.NotAfter, CommandEnvelopes.Finish(command.Body));

    static StoreReply<FinishedAttempt> MapFinished(SqRow row) =>
        row.HasValue ? row.Applied(ReceiptCodec.ReadFinished(row.Result!)) : row.Reject<FinishedAttempt>();

    /// <summary>rw_finish_plan_v1 (03 §6.4): new state and terminal reason.</summary>
    static (string State, string? Reason) FinishPlan(string outcome, bool cancel, int started, int max) => outcome switch
    {
        "succeeded" => ("succeeded", "succeeded"),
        "user_cancelled" => ("cancelled", "user_cancelled"),
        _ when cancel => ("cancelled", "cancellation_requested"),
        "permanent_failure" => ("failed", "permanent_failure"),
        _ when started < max => ("retrying", null),
        _ => ("failed", "execution_budget_exhausted"),
    };

    /// <summary>rw_finish_job_v1: ownership, the retry delay and the close. A refusal is decided before any mutation.</summary>
    static SqRow FinishJob(SqTx tx, Guid commandId, FinishRequest request, ulong entropy)
    {
        var now = tx.Now;
        var lease = request.Lease;
        var job = ReadJob(tx, lease.JobId);
        if (Ownership(job, lease, now, "running") is { } refused)
        {
            return refused;
        }

        var g = ReadGeneration(tx, lease.JobId, job!.Generation);
        var outcome = Snake(request.Outcome);
        var (state, reason) = FinishPlan(outcome, job.CancelRequestedAt is not null, g.Started, g.MaxExecutions);
        long? delay = null;
        DateTimeOffset? eligible = null;
        if (state == "retrying")
        {
            var retry = JsonNode.Parse(g.Retry)!;
            var max = retry["maximumDelay"]!.GetValue<long>();
            delay = outcome == "retryable_failure" && request.RequestedRetryDelay is { } d
                ? Math.Min(Math.Max(1, CeilMs(d)), max)
                : RetryDelay(g.Started, retry["initialDelay"]!.GetValue<long>(), max, entropy);
            eligible = now.AddMilliseconds(delay.Value);
        }

        var revision = Close(tx, commandId, lease.JobId, lease.AttemptId, lease.OwnershipToken, job.Generation, job.Revision, job.JobName, job.ActiveKey,
            outcome, state, reason, "attempt_finished", (long)Math.Max(0, request.ExecutionElapsed.TotalMilliseconds), delay, eligible,
            ErrorJson(request.Error), null, request.Usage);

        return SqRow.Applied("Applied", null, now, new JsonObject
        {
            ["jobId"] = lease.JobId, ["generation"] = job.Generation, ["revision"] = revision, ["state"] = state, ["attemptId"] = lease.AttemptId,
            ["attemptOutcome"] = outcome, ["disposition"] = state, ["terminalReason"] = reason, ["eligibleAt"] = Ms(eligible), ["retryDelay"] = delay,
        });
    }

    public Task<StoreReply<FinishedMany>> FinishManyAsync(StoreCommand<FinishManyRequest> command, CancellationToken ct)
    {
        var items = command.Body.Items;
        if (items.Count is 0 or > IFinishManyStore.MaxItems
            || items.Any(i => i.Application != command.Application || i.Body.RequestedRetryDelay < TimeSpan.Zero)
            || items.Select(i => i.CommandId).Distinct().Count() != items.Count
            || items.Select(i => i.Body.Lease.JobId).Distinct().Count() != items.Count)
        {
            return Task.FromResult(Invalid<FinishedMany>("invalid_request", time));
        }

        var ops = items.Select(FinishOp).ToArray();
        var entropy = items.Select(_ => Entropy()).ToArray();
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.FinishMany, command.NotAfter, null, SqLane.Settlement),
            tx => FinishManyBody(tx, items, ops, entropy),
            row => row.HasValue ? row.Applied(ReadFinishedMany(row)) : row.Reject<FinishedMany>(),
            ct);
    }

    /// <summary>
    /// finish_many_v1 (20 §7.2): each item's receipt in command-ID order (replay or refusal is that item's outcome), then each
    /// remaining item as its own Finish/1 in request order with its own R4. Commits when at least one item applied now.
    /// </summary>
    SqRow FinishManyBody(SqTx tx, IReadOnlyList<StoreCommand<FinishRequest>> items, SqOp[] ops, ulong[] entropy)
    {
        var done = new JsonObject?[items.Count];
        // The writer transaction excludes competing receipt inserts. Command IDs in this batch are distinct.
        foreach (var k in Enumerable.Range(0, items.Count).OrderBy(k => items[k].CommandId, UuidOrder.Instance))
        {
            var itemTx = new SqTx(tx.Connection, tx.Transaction, ops[k], runner.Faults, tx.Ct) { Now = tx.Now, Clock = tx.Clock };
            if (runner.Receipt(itemTx, insertPlaceholder: false) is { } r)
            {
                done[k] = ItemJson(items[k].CommandId, r);
            }
        }

        var receipts = new List<object?[]>(items.Count);
        var retentionClass = Operations.RetentionClass(Operations.Finish);
        var retentionFloor = Operations.RetentionFloor(Operations.Finish);
        for (var k = 0; k < items.Count; k++)
        {
            if (done[k] is not null)
            {
                continue;
            }

            // An item's refusal never mutates job state. An exception still rolls back the enclosing transaction.
            var op = ops[k];
            var item = FinishJob(tx, op.CommandId, items[k].Body, entropy[k]);

            if (item.HasValue)
            {
                var notAfter = SqTime.Floor(op.NotAfter);
                receipts.Add([tx.Application, retentionClass, op.CommandId, Operations.Finish, 1, 1, op.Fingerprint, 1,
                    notAfter, 1, notAfter + retentionFloor, item.Status, item.Result!.ToJsonString(), tx.Now]);
            }

            done[k] = ItemJson(op.CommandId, item);
        }

        Expect(InsertRows(tx, "runwick_storage_commands",
            ["application", "retention_class", "command_id", "operation", "operation_version", "protocol_epoch", "fingerprint",
                "fingerprint_version", "not_after", "receipt_codec_version", "retain_until", "status", "result", "decided_at"], receipts) == receipts.Count);

        var row = SqRow.Applied("Applied", null, tx.Now, new JsonObject { ["items"] = new JsonArray([.. done]) });
        row.NoCommit = receipts.Count == 0;
        return row;
    }

    static JsonObject ItemJson(Guid commandId, SqRow r) => new()
    {
        ["commandId"] = commandId, ["status"] = r.Status, ["effect"] = r.Effect, ["replayed"] = r.Replayed, ["code"] = r.Code,
        ["decidedAt"] = Ms(r.DecidedAt), ["result"] = r.Result?.DeepClone(),
    };

    static FinishedMany ReadFinishedMany(SqRow row)
    {
        var outcomes = new List<StoreReply<FinishedAttempt>>();
        foreach (var node in row.Result!["items"]!.AsArray())
        {
            var item = node!.AsObject();
            var status = Enum.Parse<StoreStatus>(item["status"]!.GetValue<string>());
            var applied = item["effect"]!.GetValue<string>() == nameof(StoreEffect.Applied);
            var decided = item["decidedAt"] is { } ms ? SqTime.FromMs(ms.GetValue<long>()) : default;
            var code = item["code"]?.GetValue<string>();
            outcomes.Add(applied
                ? new StoreReply<FinishedAttempt>(status, StoreEffect.Applied, item["replayed"]!.GetValue<bool>(),
                    ReceiptCodec.ReadFinished(item["result"]!.AsObject()), code is null ? null : new StoreError(code), decided)
                : StoreReply<FinishedAttempt>.NoEffect(status, new StoreError(code ?? "invariant_fault"), decided));
        }

        return new FinishedMany(outcomes);
    }

    // ------------------------------------------------------------------ Recover/1 (04 §7.8)

    public Task<StoreReply<FinishedAttempt>> RecoverAsync(StoreCommand<RecoverRequest> command, CancellationToken ct)
    {
        var request = command.Body;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.Recover, command.CommandId, command.NotAfter,
            CommandEnvelopes.Recover(request));
        var entropy = Entropy();
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.Recover, command.NotAfter, fingerprint, SqLane.Background),
            tx => RecoverBody(tx, request, entropy),
            MapFinished,
            ct);
    }

    static SqRow RecoverBody(SqTx tx, RecoverRequest request, ulong entropy)
    {
        var now = tx.Now;
        var job = ReadJob(tx, request.JobId);
        if (job is null)
        {
            return SqRow.NoEffect("NotFound", "job_not_found", null);
        }

        if (job.AttemptId != request.ExpectedAttemptId || job.Token != request.ExpectedOwnershipToken || job.State is not ("reserved" or "running"))
        {
            return SqRow.NoEffect("StaleLease", "stale_lease", null);
        }

        var g = ReadGeneration(tx, request.JobId, job.Generation);
        var reservedState = job.State == "reserved";
        var cancel = job.CancelRequestedAt is not null;
        var k = g.Unstarted + 1;
        (string State, string? Reason) plan = (reservedState, cancel) switch
        {
            (true, true) => ("cancelled", "cancelled_before_start"),
            (true, _) when k >= g.MaxUnstarted => ("failed", "reservation_abandonment_exhausted"),
            (true, _) => (g.Started == 0 ? "pending" : "retrying", null),
            (_, true) => ("cancelled", "cancellation_requested"),
            _ when g.Started < g.MaxExecutions => ("retrying", null),
            _ => ("failed", "execution_budget_exhausted"),
        };
        var (state, reason) = plan;

        if (job.LeaseExpiresAt > now)
        {
            return SqRow.NoEffect("StaleLease", "lease_renewed", now);
        }

        if (reservedState && !cancel)
        {
            Expect(tx.Exec(tx.Sql("""
                UPDATE runwick_job_generations SET consecutive_unstarted_count = consecutive_unstarted_count + 1
                WHERE application = @application AND job_id = @job_id AND generation = @generation
                """).P("job_id", request.JobId).P("generation", job.Generation)) == 1);
        }

        long? delay = null;
        DateTimeOffset? eligible = null;
        if (!Terminal.Contains(state))
        {
            var policy = JsonNode.Parse(reservedState ? g.UnstartedRetry : g.Retry)!;
            delay = RetryDelay(reservedState ? k : g.Started, policy["initialDelay"]!.GetValue<long>(), policy["maximumDelay"]!.GetValue<long>(), entropy);
            eligible = now.AddMilliseconds(delay.Value);
        }

        var outcome = reservedState ? "abandoned_before_start" : "abandoned_running";
        var revision = Close(tx, tx.Op.CommandId, request.JobId, request.ExpectedAttemptId, request.ExpectedOwnershipToken, job.Generation, job.Revision,
            job.JobName, job.ActiveKey, outcome, state, reason, "attempt_recovered", null, delay, eligible, null, null);
        return SqRow.Applied("Applied", null, now, new JsonObject
        {
            ["jobId"] = request.JobId, ["generation"] = job.Generation, ["revision"] = revision, ["state"] = state,
            ["attemptId"] = request.ExpectedAttemptId, ["attemptOutcome"] = outcome, ["disposition"] = state, ["terminalReason"] = reason,
            ["eligibleAt"] = Ms(eligible), ["retryDelay"] = delay,
        });
    }

    // ------------------------------------------------------------------ ReleaseReservation/1 (04 §7.9)

    public Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(StoreCommand<ReleaseRequest> command, CancellationToken ct)
    {
        var lease = command.Body.Lease;
        var fingerprint = CommandEnvelopes.Fingerprint(StoreId, command.Application, Operations.ReleaseReservation, command.CommandId, command.NotAfter,
            CommandEnvelopes.Release(command.Body));
        return runner.CallAsync(
            Op(command.Application, command.CommandId, Operations.ReleaseReservation, command.NotAfter, fingerprint, SqLane.Settlement),
            tx =>
            {
                var job = ReadJob(tx, lease.JobId);
                var started = job is null ? 0 : ReadGeneration(tx, lease.JobId, job.Generation).Started;
                if (Ownership(job, lease, tx.Now, "reserved") is { } refused)
                {
                    return refused;
                }

                var cancel = job!.CancelRequestedAt is not null;
                var state = cancel ? "cancelled" : started == 0 ? "pending" : "retrying";
                var revision = Close(tx, tx.Op.CommandId, lease.JobId, lease.AttemptId, lease.OwnershipToken, job.Generation, job.Revision, job.JobName,
                    job.ActiveKey, "released", state, cancel ? "cancelled_before_start" : null, "attempt_released", null, null, null, null, null);
                return SqRow.Applied("Applied", null, tx.Now, new JsonObject
                {
                    ["jobId"] = lease.JobId, ["generation"] = job.Generation, ["revision"] = revision, ["state"] = state,
                    ["attemptId"] = lease.AttemptId, ["disposition"] = state, ["eligibleAt"] = cancel ? null : SqTime.Ms(job.EligibleAt),
                });
            },
            row => row.HasValue ? row.Applied(ReceiptCodec.ReadReleased(row.Result!)) : row.Reject<ReleasedReservation>(),
            ct);
    }
}
