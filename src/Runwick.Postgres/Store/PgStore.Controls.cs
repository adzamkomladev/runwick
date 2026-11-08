using System.Text;
using System.Text.Json.Nodes;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Revisioned controls (08 §3): Cancel/1, ManualRetry/1, RerunSucceeded/1, SetQueueState/1 (public path),
/// EditScopePolicy/1 and EventsSince/1. Every success commits its effect, audit row(s), event and receipt together;
/// a replay writes nothing; a rejection rolls back with no receipt and no audit.
/// </summary>
internal sealed partial class PgStore : IJobControlStore
{
    const int MaxScopeCapacity = 10_000;

    // ------------------------------------------------------------------ Cancel/1 (08 §3.2)

    public async Task<StoreReply<ControlledJob>> CancelAsync(StoreCommand<CancelRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return StoreReply<ControlledJob>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.Cancel, command.CommandId, command.NotAfter, CommandEnvelopes.Cancel(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.Cancel, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                var job = await LockJobAsync(tx, r.JobId).ConfigureAwait(false);
                var waiting = job is { State: JobStates.Pending or JobStates.Retrying };
                if (waiting)
                {
                    await LockActiveKeyAsync(tx, job!).ConfigureAwait(false);
                }

                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (job is null)
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.NotFound, "job_not_found");
                }

                if (job.Generation != r.ExpectedGeneration || job.Revision != r.ExpectedRevision)
                {
                    return JobConflict<ControlledJob>(tx, job);
                }

                if (JobStates.IsTerminal(job.State))
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.InvalidState, "job_terminal");
                }

                var now = tx.Now;
                ControlledJob value;
                if (waiting)
                {
                    // T17: close the waiting job; release the active binding only if it still names this job (03 §5.6).
                    using (var u = tx.Sql("""
                        UPDATE runwick.jobs
                        SET state = 'cancelled', terminal_at = @now, terminal_reason = 'cancelled_while_waiting',
                            cancel_requested_at = @now, cancel_reason = @reason, cancel_requested_by = @subject,
                            revision = revision + 1, updated_at = @now
                        WHERE application = @application AND job_id = @job_id AND revision = @revision
                        """).P("now", now).P("reason", r.Actor.Reason).P("subject", r.Actor.Subject).P("job_id", r.JobId).P("revision", job.Revision))
                    {
                        Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                    }

                    if (job.ActiveKey is not null)
                    {
                        using var k = tx.Sql("""
                            UPDATE runwick.dispatch_keys
                            SET bound_job_id = NULL, updated_at = @now
                            WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key AND bound_job_id = @job_id
                            """).P("now", now).P("job_name", job.JobName).P("key", job.ActiveKey).P("job_id", r.JobId);
                        await tx.ExecAsync(k).ConfigureAwait(false);
                    }

                    value = new ControlledJob(new JobPosition(r.JobId, job.Generation, job.Revision + 1, JobStates.Cancelled),
                        "cancelled", "cancelled_while_waiting", now);
                }
                else
                {
                    // T22: record the request only. Lease, permits and attempt stay; precedence decides later (03 §6.4–6.5).
                    if (job.CancelRequestedAt is not null)
                    {
                        return Rollback<ControlledJob>(tx, StoreStatus.InvalidState, "cancel_already_requested");
                    }

                    using (var u = tx.Sql("""
                        UPDATE runwick.jobs
                        SET cancel_requested_at = @now, cancel_reason = @reason, cancel_requested_by = @subject,
                            revision = revision + 1, updated_at = @now
                        WHERE application = @application AND job_id = @job_id AND revision = @revision
                        """).P("now", now).P("reason", r.Actor.Reason).P("subject", r.Actor.Subject).P("job_id", r.JobId).P("revision", job.Revision))
                    {
                        Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                    }

                    value = new ControlledJob(new JobPosition(r.JobId, job.Generation, job.Revision + 1, job.State), "cancel_requested", null, now);
                }

                await ControlEventAsync(tx, r.JobId, job.AttemptId, waiting ? "job_cancelled" : "job_cancel_requested", r.Actor,
                    new JsonObject { ["generation"] = job.Generation, ["revision"] = value.Position.Revision, ["previousState"] = job.State }).ConfigureAwait(false);
                await AuditAsync(tx, "job", r.JobId.ToString("D"), r.Actor, job.Revision, value.Position.Revision,
                    new JsonObject { ["effect"] = value.Effect, ["previousState"] = job.State }).ConfigureAwait(false);
                return new PgOutcome<ControlledJob>.Commit(StoreReply<ControlledJob>.Applied(value, now), ReceiptCodec.Controlled(value));
            },
            (tx, row) => Task.FromResult(StoreReply<ControlledJob>.Applied(ReceiptCodec.ReadControlled(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ ManualRetry/1, RerunSucceeded/1 (08 §3.3)

    public Task<StoreReply<ControlledJob>> ManualRetryAsync(StoreCommand<RetryRequest> command, CancellationToken ct) =>
        RegenerateAsync(command, Operations.ManualRetry, ct);

    public Task<StoreReply<ControlledJob>> RerunSucceededAsync(StoreCommand<RetryRequest> command, CancellationToken ct) =>
        RegenerateAsync(command, Operations.RerunSucceeded, ct);

    async Task<StoreReply<ControlledJob>> RegenerateAsync(StoreCommand<RetryRequest> command, string operation, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return StoreReply<ControlledJob>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var rerun = operation == Operations.RerunSucceeded;
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, operation, command.CommandId, command.NotAfter, CommandEnvelopes.Retry(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, operation, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                var job = await LockJobAsync(tx, r.JobId).ConfigureAwait(false);
                Guid? keyHolder = null;
                var keyRow = false;
                // Retention may delete an unbound key row between the seed and the lock (17 §3.3): repeat at most 3 times.
                for (var seeds = 0; job?.ActiveKey is { } activeKey && !keyRow; seeds++)
                {
                    if (seeds > 3)
                    {
                        throw new InvariantViolationException("key_row_missing");
                    }

                    using (var insert = tx.Sql("""
                        INSERT INTO runwick.dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
                        VALUES (@application, @job_name, 'active', @key, runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp()))
                        ON CONFLICT DO NOTHING
                        """).P("job_name", job.JobName).P("key", activeKey))
                    {
                        await tx.ExecAsync(insert, mutation: false).ConfigureAwait(false);
                    }

                    using var k = tx.Sql("""
                        SELECT bound_job_id FROM runwick.dispatch_keys
                        WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key
                        FOR UPDATE
                        """).P("job_name", job.JobName).P("key", activeKey);
                    var bound = await tx.QueryAsync(k, x => x.IsDBNull(0) ? (Guid?)null : x.GetGuid(0)).ConfigureAwait(false);
                    keyRow = bound.Count == 1;
                    keyHolder = keyRow ? bound[0] : null;
                }

                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (job is null)
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.NotFound, "job_not_found");
                }

                if (job.Generation != r.ExpectedGeneration || job.Revision != r.ExpectedRevision)
                {
                    return JobConflict<ControlledJob>(tx, job);
                }

                if (rerun ? job.State != JobStates.Succeeded : job.State is not (JobStates.Failed or JobStates.Cancelled))
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.InvalidState, "invalid_state");
                }

                using (var p = tx.Sql("""
                    SELECT j.payload_retained AND EXISTS (
                               SELECT 1 FROM runwick.job_payloads AS p WHERE p.application = j.application AND p.job_id = j.job_id),
                           EXISTS (SELECT 1 FROM runwick.queue_controls AS q WHERE q.application = j.application AND q.queue = j.queue)
                    FROM runwick.jobs AS j
                    WHERE j.application = @application AND j.job_id = @job_id
                    """).P("job_id", r.JobId))
                {
                    var (usable, queued) = (await tx.QueryAsync(p, x => (x.GetBoolean(0), x.GetBoolean(1))).ConfigureAwait(false))[0];
                    if (!usable)
                    {
                        return Rollback<ControlledJob>(tx, StoreStatus.HistoryUnavailable, "payload_unavailable");
                    }

                    if (!queued)
                    {
                        return Rollback<ControlledJob>(tx, StoreStatus.InvalidRequest, "queue_not_registered");
                    }
                }

                if (job.ActiveKey is not null && keyHolder is { } holder && holder != r.JobId)
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.UniquenessConflict, "uniqueness_conflict", holder);
                }

                var now = tx.Now;
                var generation = job.Generation + 1;
                var reason = rerun ? "rerun_succeeded" : "manual_retry";
                using (var g = tx.Sql("""
                    INSERT INTO runwick.job_generations (
                        application, job_id, generation, created_at, created_by, created_reason, policy_snapshot_version,
                        max_executions, max_unstarted_abandonments, execution_timeout_ms,
                        retry_policy, retry_policy_version, unstarted_retry_policy, unstarted_retry_policy_version)
                    SELECT application, job_id, @generation, @now, @subject, @reason, policy_snapshot_version,
                           max_executions, max_unstarted_abandonments, execution_timeout_ms,
                           retry_policy, retry_policy_version, unstarted_retry_policy, unstarted_retry_policy_version
                    FROM runwick.job_generations
                    WHERE application = @application AND job_id = @job_id AND generation = @previous
                    """).P("generation", generation).P("now", now).P("subject", r.Actor.Subject).P("reason", reason)
                        .P("job_id", r.JobId).P("previous", job.Generation))
                {
                    Expect(await tx.ExecAsync(g).ConfigureAwait(false), 1);
                }

                using (var u = tx.Sql("""
                    UPDATE runwick.jobs
                    SET state = 'pending', generation = @generation, revision = revision + 1, eligible_at = @now,
                        terminal_at = NULL, terminal_reason = NULL,
                        cancel_requested_at = NULL, cancel_reason = NULL, cancel_requested_by = NULL,
                        updated_at = @now
                    WHERE application = @application AND job_id = @job_id AND revision = @revision
                    """).P("generation", generation).P("now", now).P("job_id", r.JobId).P("revision", job.Revision))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                if (job.ActiveKey is not null && keyRow && keyHolder is null)
                {
                    using var k = tx.Sql("""
                        UPDATE runwick.dispatch_keys
                        SET bound_job_id = @job_id, updated_at = @now
                        WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key AND bound_job_id IS NULL
                        """).P("job_id", r.JobId).P("now", now).P("job_name", job.JobName).P("key", job.ActiveKey);
                    Expect(await tx.ExecAsync(k).ConfigureAwait(false), 1);
                }

                var value = new ControlledJob(new JobPosition(r.JobId, generation, job.Revision + 1, JobStates.Pending), rerun ? "rerun" : "retried", null, null);
                await ControlEventAsync(tx, r.JobId, null, rerun ? "job_rerun" : "job_retried", r.Actor,
                    new JsonObject { ["generation"] = generation, ["previousState"] = job.State, ["reason"] = reason }).ConfigureAwait(false);
                await AuditAsync(tx, "job", r.JobId.ToString("D"), r.Actor, job.Revision, value.Position.Revision,
                    new JsonObject { ["effect"] = value.Effect, ["previousState"] = job.State, ["generation"] = generation }).ConfigureAwait(false);
                return new PgOutcome<ControlledJob>.Commit(StoreReply<ControlledJob>.Applied(value, now), ReceiptCodec.Controlled(value));
            },
            (tx, row) => Task.FromResult(StoreReply<ControlledJob>.Applied(ReceiptCodec.ReadControlled(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ SetQueueState/1 public path (08 §3.4)

    public async Task<StoreReply<QueueState>> SetQueueStateAsync(StoreCommand<QueueStateRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return StoreReply<QueueState>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.SetQueueState, command.CommandId, command.NotAfter,
            CommandEnvelopes.QueueState(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.SetQueueState, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                using var q = tx.Sql("""
                    SELECT paused, revision FROM runwick.queue_controls
                    WHERE application = @application AND queue = @queue
                    FOR UPDATE
                    """).P("queue", r.Queue);
                var rows = await tx.QueryAsync(q, x => (Paused: x.GetBoolean(0), Revision: x.GetInt64(1))).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<QueueState>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (rows.Count == 0)
                {
                    return Rollback<QueueState>(tx, StoreStatus.NotFound, "queue_not_registered");
                }

                var current = rows[0];
                if (current.Revision != r.ExpectedRevision)
                {
                    return new PgOutcome<QueueState>.Rollback(StoreReply<QueueState>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: current.Revision, Detail: $"actual={current.Revision}"), tx.Now));
                }

                if (current.Paused == r.Paused)
                {
                    return Rollback<QueueState>(tx, StoreStatus.InvalidState, "no_change");
                }

                using (var u = tx.Sql("""
                    UPDATE runwick.queue_controls
                    SET paused = @paused, revision = revision + 1, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND queue = @queue
                    """).P("paused", r.Paused).P("now", tx.Now).P("subject", r.Actor.Subject).P("queue", r.Queue))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var state = new QueueState(r.Queue, r.Paused, current.Revision + 1);
                await ControlEventAsync(tx, null, null, r.Paused ? "queue_paused" : "queue_resumed", r.Actor,
                    new JsonObject { ["queue"] = r.Queue, ["revision"] = state.Revision }).ConfigureAwait(false);
                await AuditAsync(tx, "queue", r.Queue, r.Actor, current.Revision, state.Revision,
                    new JsonObject { ["paused"] = r.Paused }).ConfigureAwait(false);
                return new PgOutcome<QueueState>.Commit(StoreReply<QueueState>.Applied(state, tx.Now), ReceiptCodec.Queue(state));
            },
            (tx, row) => Task.FromResult(StoreReply<QueueState>.Applied(ReceiptCodec.ReadQueue(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ EditScopePolicy/1 (08 §3.5)

    public async Task<StoreReply<ScopePolicy>> EditScopePolicyAsync(StoreCommand<ScopePolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor)
            ?? (r.Capacity is < 0 or > MaxScopeCapacity ? "capacity" : null)
            ?? (r.ScopeKind is not (Serialization.ScopeRef.QueueKind or Serialization.ScopeRef.JobTypeKind or Serialization.ScopeRef.ResourceKind) ? "scopeKind" : null)
            ?? (string.IsNullOrEmpty(r.ScopeKey) || Encoding.UTF8.GetByteCount(r.ScopeKey) > 512 ? "scopeKey" : null);
        if (invalid is not null)
        {
            return StoreReply<ScopePolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.EditScopePolicy, command.CommandId, command.NotAfter,
            CommandEnvelopes.ScopePolicy(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.EditScopePolicy, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                using var s = tx.Sql("""
                    SELECT scope_id, capacity, policy_revision FROM runwick.concurrency_scopes
                    WHERE application = @application AND scope_kind = @kind AND scope_key = @key
                    FOR UPDATE
                    """).P("kind", r.ScopeKind).P("key", r.ScopeKey);
                var rows = await tx.QueryAsync(s, x => (Id: x.GetGuid(0), Capacity: x.GetInt32(1), Revision: x.GetInt64(2))).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ScopePolicy>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (rows.Count == 0)
                {
                    return Rollback<ScopePolicy>(tx, StoreStatus.NotFound, "scope_not_registered");
                }

                var scope = rows[0];
                if (scope.Revision != r.ExpectedPolicyRevision)
                {
                    return new PgOutcome<ScopePolicy>.Rollback(StoreReply<ScopePolicy>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: scope.Revision, Detail: $"actual={scope.Revision}"), tx.Now));
                }

                if (scope.Capacity == r.Capacity)
                {
                    return Rollback<ScopePolicy>(tx, StoreStatus.InvalidState, "no_change");
                }

                using (var u = tx.Sql("""
                    UPDATE runwick.concurrency_scopes
                    SET capacity = @capacity, policy_revision = policy_revision + 1, updated_at = @now
                    WHERE application = @application AND scope_id = @scope_id
                    """).P("capacity", r.Capacity).P("now", tx.Now).P("scope_id", scope.Id))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                // A decrease never revokes a live permit (CTL-01): report the live count observed under the scope lock.
                int live;
                using (var c = tx.Sql("""
                    SELECT count(*)::int FROM runwick.concurrency_permits
                    WHERE application = @application AND scope_id = @scope_id AND lease_expires_at > @now
                    """).P("scope_id", scope.Id).P("now", tx.Now))
                {
                    live = (await tx.QueryAsync(c, x => x.GetInt32(0)).ConfigureAwait(false))[0];
                }

                var value = new ScopePolicy(scope.Id, r.ScopeKind, r.ScopeKey, r.Capacity, scope.Revision + 1, live);
                var target = r.ScopeKind + "/" + r.ScopeKey;
                await ControlEventAsync(tx, null, null, "scope_policy_edited", r.Actor, new JsonObject
                {
                    ["scope"] = scope.Id.ToString("D"), ["kind"] = r.ScopeKind, ["key"] = r.ScopeKey,
                    ["capacity"] = r.Capacity, ["previous"] = scope.Capacity, ["policyRevision"] = value.PolicyRevision,
                }).ConfigureAwait(false);
                await AuditAsync(tx, "scope", target, r.Actor, scope.Revision, value.PolicyRevision,
                    new JsonObject { ["capacity"] = r.Capacity, ["previous"] = scope.Capacity, ["livePermits"] = live }).ConfigureAwait(false);
                return new PgOutcome<ScopePolicy>.Commit(StoreReply<ScopePolicy>.Applied(value, tx.Now), ReceiptCodec.Scope(value));
            },
            (tx, row) => Task.FromResult(StoreReply<ScopePolicy>.Applied(ReceiptCodec.ReadScope(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ EventsSince/1 (08 §7.2)

    public async Task<EventPage> EventsSinceAsync(string application, EventCursor? after, int limit, CancellationToken ct)
    {
        if (limit is < 1 or > 1000)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), limit, "The page size must be 1-1000.");
        }

        var op = new PgOp(application, Guid.CreateVersion7(), Operations.EventsSince, DateTimeOffset.MaxValue, null, PgLane.Background, null);
        var reply = await runner.ExecuteAsync<EventPage>(op,
            async tx =>
            {
                var tx0 = after?.TxId ?? 0UL;
                var id0 = after?.EventId ?? 0L;
                using var q = tx.Sql("""
                    WITH horizon AS (SELECT pg_snapshot_xmin(pg_current_snapshot()) AS xmin)
                    SELECT e.event_id, e.tx_id::text, e.schema_version, e.event_kind, e.job_id, e.attempt_id, e.command_id,
                           e.occurred_at, e.actor_id, e.details::text,
                           e.tx_id < h.xmin AS visible
                    FROM runwick.queue_events AS e, horizon AS h
                    WHERE e.application = @application
                      AND (e.tx_id, e.event_id) > (@tx::text::xid8, @id)
                    ORDER BY e.tx_id, e.event_id
                    LIMIT @limit
                    """).P("tx", tx0.ToString(System.Globalization.CultureInfo.InvariantCulture)).P("id", id0).P("limit", limit + 1);
                var rows = await tx.QueryAsync(q, x => (Event: new JobEvent(
                    x.GetInt64(0), ulong.Parse(x.GetString(1), System.Globalization.CultureInfo.InvariantCulture), x.GetInt16(2), x.GetString(3),
                    x.IsDBNull(4) ? null : x.GetGuid(4), x.IsDBNull(5) ? null : x.GetGuid(5), x.IsDBNull(6) ? null : x.GetGuid(6),
                    PgTime.Read(x, 7), x.IsDBNull(8) ? null : x.GetString(8), JsonNode.Parse(x.GetString(9))!.AsObject()),
                    Visible: x.GetBoolean(10))).ConfigureAwait(false);

                // Only rows below the snapshot horizon are returned; anything at or beyond it is "partial" (08 §7.2).
                var visible = rows.TakeWhile(x => x.Visible).Take(limit).Select(x => x.Event).ToList();
                var partial = rows.Count > visible.Count && rows.Skip(visible.Count).Any(x => !x.Visible);
                var next = visible.Count > 0 ? visible[^1].Cursor : after;
                var page = new EventPage(visible, next, partial);
                return new PgOutcome<EventPage>.Rollback(StoreReply<EventPage>.Applied(page, PgTime.Floor(time.GetUtcNow())));
            },
            replay: null, ct).ConfigureAwait(false);
        return reply.Value ?? throw new StoreNotAppliedException(reply.Error?.Code ?? "store_unavailable");
    }

    // ------------------------------------------------------------------ helpers

    static PgOutcome<T> Rollback<T>(PgTx tx, StoreStatus status, string code, Guid? conflicting = null) where T : class =>
        new PgOutcome<T>.Rollback(tx.Reject<T>(status, code, conflicting));

    static PgOutcome<T> JobConflict<T>(PgTx tx, JobRow job) where T : class =>
        new PgOutcome<T>.Rollback(StoreReply<T>.NoEffect(StoreStatus.VersionConflict,
            new StoreError("revision_conflict", ActualRevision: job.Revision, Detail: $"actual={job.Generation}/{job.Revision}"), tx.Now));

    /// <summary>08 §2 bounds; returns the offending field or null.</summary>
    internal static string? ValidateActor(ControlActor actor)
    {
        static bool Bytes(string? s, int min, int max) => s is null ? min == 0 : Encoding.UTF8.GetByteCount(s) is var n && n >= min && n <= max;

        if (actor is null || !Bytes(actor.Subject, 1, 256) || actor.Subject.Any(char.IsControl))
        {
            return "actor.subject";
        }

        if (!Bytes(actor.Display, 0, 256))
        {
            return "actor.display";
        }

        if (string.IsNullOrEmpty(actor.Scheme) || actor.Scheme.Length > 64 || !char.IsAsciiLetterOrDigit(actor.Scheme[0])
            || actor.Scheme.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-')))
        {
            return "actor.scheme";
        }

        return Bytes(actor.Reason, 0, 512) ? null : "reason";
    }

    static async Task ControlEventAsync(PgTx tx, Guid? jobId, Guid? attemptId, string kind, ControlActor actor, JsonObject details)
    {
        using var e = tx.Sql("""
            INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
            VALUES (@application, @job_id, @attempt_id, @command_id, @kind, @now, @actor, @details)
            """).P("job_id", jobId, NpgsqlDbType.Uuid).P("attempt_id", attemptId, NpgsqlDbType.Uuid).P("command_id", tx.Op.CommandId)
                .P("kind", kind).P("now", tx.Now).P("actor", actor.Subject).Json("details", details);
        await tx.ExecAsync(e).ConfigureAwait(false);
    }

    /// <summary>The management audit row (08 §5), written only on success, in the effect's transaction.</summary>
    static async Task AuditAsync(PgTx tx, string targetKind, string targetId, ControlActor actor, long? expectedRevision, long? resultingRevision, JsonObject details,
        string? operation = null)
    {
        using var a = tx.Sql("""
            INSERT INTO runwick.management_audit (
                application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
                expected_revision, resulting_revision, details, occurred_at)
            VALUES (@application, @command_id, @operation, @target_kind, @target_id, @subject, @display, @scheme, @reason,
                    @expected, @resulting, @details, @now)
            """).P("command_id", tx.Op.CommandId).P("operation", operation ?? tx.Op.Operation).P("target_kind", targetKind).P("target_id", targetId)
                .P("subject", actor.Subject).P("display", actor.Display).P("scheme", actor.Scheme).P("reason", actor.Reason)
                .P("expected", expectedRevision, NpgsqlDbType.Bigint).P("resulting", resultingRevision, NpgsqlDbType.Bigint)
                .Json("details", details).P("now", tx.Now);
        Expect(await tx.ExecAsync(a).ConfigureAwait(false), 1);
    }
}
