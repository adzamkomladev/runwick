using System.Text;
using System.Text.Json.Nodes;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Revisioned controls (08 §3, 20 §7): Cancel/1, ManualRetry/1, RerunSucceeded/1, SetQueueState/1, EditScopePolicy/1 and
/// EventsSince/1 — PgStore.Controls's steps on the write lock. Every success commits its effect, audit row, event and
/// receipt together; a replay writes nothing; a rejection rolls back with no receipt and no audit.
/// </summary>
internal sealed partial class SqliteStore : IJobControlStore
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.Cancel, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var job = ReadJob(tx, r.JobId);
                if (!tx.Decide())
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
                var waiting = job.State is JobStates.Pending or JobStates.Retrying;
                ControlledJob value;
                if (waiting)
                {
                    // T17: close the waiting job; release the active binding only if it still names this job (03 §5.6).
                    Expect(tx.Exec(tx.Sql("""
                        UPDATE runwick_jobs
                        SET state = 'cancelled', terminal_at = @now, terminal_reason = 'cancelled_while_waiting',
                            cancel_requested_at = @now, cancel_reason = @reason, cancel_requested_by = @subject,
                            revision = revision + 1, updated_at = @now
                        WHERE application = @application AND job_id = @job_id AND revision = @revision
                        """).P("now", now).P("reason", r.Actor.Reason).P("subject", r.Actor.Subject).P("job_id", r.JobId).P("revision", job.Revision)) == 1);

                    if (job.ActiveKey is not null)
                    {
                        tx.Exec(tx.Sql("""
                            UPDATE runwick_dispatch_keys
                            SET bound_job_id = NULL, updated_at = @now
                            WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key AND bound_job_id = @job_id
                            """).P("now", now).P("job_name", job.JobName).P("key", job.ActiveKey).P("job_id", r.JobId));
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

                    Expect(tx.Exec(tx.Sql("""
                        UPDATE runwick_jobs
                        SET cancel_requested_at = @now, cancel_reason = @reason, cancel_requested_by = @subject,
                            revision = revision + 1, updated_at = @now
                        WHERE application = @application AND job_id = @job_id AND revision = @revision
                        """).P("now", now).P("reason", r.Actor.Reason).P("subject", r.Actor.Subject).P("job_id", r.JobId).P("revision", job.Revision)) == 1);

                    value = new ControlledJob(new JobPosition(r.JobId, job.Generation, job.Revision + 1, job.State), "cancel_requested", null, now);
                }

                ControlEvent(tx, r.JobId, job.AttemptId, waiting ? "job_cancelled" : "job_cancel_requested", r.Actor,
                    new JsonObject { ["generation"] = job.Generation, ["revision"] = value.Position.Revision, ["previousState"] = job.State });
                Audit(tx, "job", r.JobId.ToString("D"), r.Actor, job.Revision, value.Position.Revision,
                    new JsonObject { ["effect"] = value.Effect, ["previousState"] = job.State });
                return new SqOutcome<ControlledJob>.Commit(StoreReply<ControlledJob>.Applied(value, now), ReceiptCodec.Controlled(value));
            },
            (tx, row) => StoreReply<ControlledJob>.Applied(ReceiptCodec.ReadControlled(row.Result!), row.DecidedAt!.Value, replayed: true),
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, operation, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var job = ReadJob(tx, r.JobId);
                Guid? keyHolder = null;
                if (job?.ActiveKey is { } activeKey)
                {
                    // Seed the key row if absent (the write lock already makes it exclusive), then read its binding.
                    tx.Exec(tx.Sql("""
                        INSERT INTO runwick_dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
                        VALUES (@application, @job_name, 'active', @key, @now, @now) ON CONFLICT DO NOTHING
                        """).P("job_name", job.JobName).P("key", activeKey).P("now", tx.Now), mutation: false);
                    keyHolder = tx.One(tx.Sql("""
                        SELECT bound_job_id FROM runwick_dispatch_keys
                        WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key
                        """).P("job_name", job.JobName).P("key", activeKey), x => x.UuidN(0));
                }

                if (!tx.Decide())
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

                var (usable, queued) = tx.Query(tx.Sql("""
                    SELECT j.payload_retained AND EXISTS (
                               SELECT 1 FROM runwick_job_payloads AS p WHERE p.application = j.application AND p.job_id = j.job_id),
                           EXISTS (SELECT 1 FROM runwick_queue_controls AS q WHERE q.application = j.application AND q.queue = j.queue)
                    FROM runwick_jobs AS j
                    WHERE j.application = @application AND j.job_id = @job_id
                    """).P("job_id", r.JobId), x => (x.Bool(0), x.Bool(1)))[0];
                if (!usable)
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.HistoryUnavailable, "payload_unavailable");
                }

                if (!queued)
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.InvalidRequest, "queue_not_registered");
                }

                if (job.ActiveKey is not null && keyHolder is { } holder && holder != r.JobId)
                {
                    return Rollback<ControlledJob>(tx, StoreStatus.UniquenessConflict, "uniqueness_conflict", holder);
                }

                var now = tx.Now;
                var generation = job.Generation + 1;
                var reason = rerun ? "rerun_succeeded" : "manual_retry";
                Expect(tx.Exec(tx.Sql("""
                    INSERT INTO runwick_job_generations (
                        application, job_id, generation, created_at, created_by, created_reason, policy_snapshot_version,
                        max_executions, max_unstarted_abandonments, execution_timeout_ms,
                        retry_policy, retry_policy_version, unstarted_retry_policy, unstarted_retry_policy_version)
                    SELECT application, job_id, @generation, @now, @subject, @reason, policy_snapshot_version,
                           max_executions, max_unstarted_abandonments, execution_timeout_ms,
                           retry_policy, retry_policy_version, unstarted_retry_policy, unstarted_retry_policy_version
                    FROM runwick_job_generations
                    WHERE application = @application AND job_id = @job_id AND generation = @previous
                    """).P("generation", generation).P("now", now).P("subject", r.Actor.Subject).P("reason", reason)
                    .P("job_id", r.JobId).P("previous", job.Generation)) == 1);

                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_jobs
                    SET state = 'pending', generation = @generation, revision = revision + 1, eligible_at = @now,
                        terminal_at = NULL, terminal_reason = NULL,
                        cancel_requested_at = NULL, cancel_reason = NULL, cancel_requested_by = NULL,
                        updated_at = @now
                    WHERE application = @application AND job_id = @job_id AND revision = @revision
                    """).P("generation", generation).P("now", now).P("job_id", r.JobId).P("revision", job.Revision)) == 1);

                if (job.ActiveKey is not null && keyHolder is null)
                {
                    Expect(tx.Exec(tx.Sql("""
                        UPDATE runwick_dispatch_keys
                        SET bound_job_id = @job_id, updated_at = @now
                        WHERE application = @application AND job_name = @job_name AND key_kind = 'active' AND key = @key AND bound_job_id IS NULL
                        """).P("job_id", r.JobId).P("now", now).P("job_name", job.JobName).P("key", job.ActiveKey)) == 1);
                }

                var value = new ControlledJob(new JobPosition(r.JobId, generation, job.Revision + 1, JobStates.Pending), rerun ? "rerun" : "retried", null, null);
                ControlEvent(tx, r.JobId, null, rerun ? "job_rerun" : "job_retried", r.Actor,
                    new JsonObject { ["generation"] = generation, ["previousState"] = job.State, ["reason"] = reason });
                Audit(tx, "job", r.JobId.ToString("D"), r.Actor, job.Revision, value.Position.Revision,
                    new JsonObject { ["effect"] = value.Effect, ["previousState"] = job.State, ["generation"] = generation });
                return new SqOutcome<ControlledJob>.Commit(StoreReply<ControlledJob>.Applied(value, now), ReceiptCodec.Controlled(value));
            },
            (tx, row) => StoreReply<ControlledJob>.Applied(ReceiptCodec.ReadControlled(row.Result!), row.DecidedAt!.Value, replayed: true),
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.SetQueueState, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var rows = tx.Query(tx.Sql("SELECT paused, revision FROM runwick_queue_controls WHERE application = @application AND queue = @queue")
                    .P("queue", r.Queue), x => (Paused: x.Bool(0), Revision: x.GetInt64(1)));
                if (!tx.Decide())
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
                    return new SqOutcome<QueueState>.Rollback(StoreReply<QueueState>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: current.Revision, Detail: $"actual={current.Revision}"), tx.Now));
                }

                if (current.Paused == r.Paused)
                {
                    return Rollback<QueueState>(tx, StoreStatus.InvalidState, "no_change");
                }

                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_queue_controls
                    SET paused = @paused, revision = revision + 1, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND queue = @queue
                    """).P("paused", r.Paused).P("now", tx.Now).P("subject", r.Actor.Subject).P("queue", r.Queue)) == 1);

                var state = new QueueState(r.Queue, r.Paused, current.Revision + 1);
                ControlEvent(tx, null, null, r.Paused ? "queue_paused" : "queue_resumed", r.Actor,
                    new JsonObject { ["queue"] = r.Queue, ["revision"] = state.Revision });
                Audit(tx, "queue", r.Queue, r.Actor, current.Revision, state.Revision, new JsonObject { ["paused"] = r.Paused });
                return new SqOutcome<QueueState>.Commit(StoreReply<QueueState>.Applied(state, tx.Now), ReceiptCodec.Queue(state));
            },
            (tx, row) => StoreReply<QueueState>.Applied(ReceiptCodec.ReadQueue(row.Result!), row.DecidedAt!.Value, replayed: true),
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
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.EditScopePolicy, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var rows = tx.Query(tx.Sql("""
                    SELECT scope_id, capacity, policy_revision FROM runwick_concurrency_scopes
                    WHERE application = @application AND scope_kind = @kind AND scope_key = @key
                    """).P("kind", r.ScopeKind).P("key", r.ScopeKey), x => (Id: x.Uuid(0), Capacity: x.GetInt32(1), Revision: x.GetInt64(2)));
                if (!tx.Decide())
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
                    return new SqOutcome<ScopePolicy>.Rollback(StoreReply<ScopePolicy>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: scope.Revision, Detail: $"actual={scope.Revision}"), tx.Now));
                }

                if (scope.Capacity == r.Capacity)
                {
                    return Rollback<ScopePolicy>(tx, StoreStatus.InvalidState, "no_change");
                }

                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_concurrency_scopes
                    SET capacity = @capacity, policy_revision = policy_revision + 1, updated_at = @now
                    WHERE application = @application AND scope_id = @scope_id
                    """).P("capacity", r.Capacity).P("now", tx.Now).P("scope_id", scope.Id)) == 1);

                // A decrease never revokes a live permit (CTL-01): report the live count observed under the write lock.
                var live = (int)tx.Count(tx.Sql("""
                    SELECT count(*) FROM runwick_concurrency_permits
                    WHERE application = @application AND scope_id = @scope_id AND lease_expires_at > @now
                    """).P("scope_id", scope.Id).P("now", tx.Now));

                var value = new ScopePolicy(scope.Id, r.ScopeKind, r.ScopeKey, r.Capacity, scope.Revision + 1, live);
                ControlEvent(tx, null, null, "scope_policy_edited", r.Actor, new JsonObject
                {
                    ["scope"] = scope.Id.ToString("D"), ["kind"] = r.ScopeKind, ["key"] = r.ScopeKey,
                    ["capacity"] = r.Capacity, ["previous"] = scope.Capacity, ["policyRevision"] = value.PolicyRevision,
                });
                Audit(tx, "scope", r.ScopeKind + "/" + r.ScopeKey, r.Actor, scope.Revision, value.PolicyRevision,
                    new JsonObject { ["capacity"] = r.Capacity, ["previous"] = scope.Capacity, ["livePermits"] = live });
                return new SqOutcome<ScopePolicy>.Commit(StoreReply<ScopePolicy>.Applied(value, tx.Now), ReceiptCodec.Scope(value));
            },
            (tx, row) => StoreReply<ScopePolicy>.Applied(ReceiptCodec.ReadScope(row.Result!), row.DecidedAt!.Value, replayed: true),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ EventsSince/1 (08 §7.2, 20 §7.1)

    /// <summary>
    /// Writers are serialized and IDs come from AUTOINCREMENT inside the writer's transaction, so IDs are in commit order:
    /// a plain <c>event_id &gt; after</c> never skips a row. TxId is always 0 and Partial always false (20 §7.1).
    /// </summary>
    public Task<EventPage> EventsSinceAsync(string application, EventCursor? after, int limit, CancellationToken ct)
    {
        if (limit is < 1 or > 1000)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), limit, "The page size must be 1-1000.");
        }

        return runner.ReadAsync(conn =>
        {
            ReadGate(conn, application);
            var tx = ReadTx(conn, application, Operations.EventsSince, ct);
            var events = tx.Query(tx.Sql("""
                SELECT event_id, schema_version, event_kind, job_id, attempt_id, command_id, occurred_at, actor_id, details
                FROM runwick_queue_events
                WHERE application = @application AND event_id > @after
                ORDER BY event_id LIMIT @limit
                """).P("after", after?.EventId ?? 0L).P("limit", limit),
                x => new JobEvent(x.GetInt64(0), 0, x.GetInt32(1), x.GetString(2), x.UuidN(3), x.UuidN(4), x.UuidN(5), x.Time(6), x.StrN(7),
                    x.JsonN(8)!));
            return new EventPage(events, events.Count > 0 ? events[^1].Cursor : after, false);
        }, ct);
    }

    // ------------------------------------------------------------------ helpers

    static SqOutcome<T> Rollback<T>(SqTx tx, StoreStatus status, string code, Guid? conflicting = null) where T : class =>
        new SqOutcome<T>.Rollback(tx.Reject<T>(status, code, conflicting));

    static SqOutcome<T> JobConflict<T>(SqTx tx, JobRow job) where T : class =>
        new SqOutcome<T>.Rollback(StoreReply<T>.NoEffect(StoreStatus.VersionConflict,
            new StoreError("revision_conflict", ActualRevision: job.Revision, Detail: $"actual={job.Generation}/{job.Revision}"), tx.Now));

    /// <summary>A transaction-shaped handle over a reader connection, so reads use the same command helpers.</summary>
    SqTx ReadTx(Microsoft.Data.Sqlite.SqliteConnection conn, string application, string operation, CancellationToken ct) =>
        new(conn, null, new SqOp(application, Guid.CreateVersion7(), operation, DateTimeOffset.MaxValue, null, SqLane.Background), runner.Faults, ct)
        {
            Now = Now(),
        };

    /// <summary>The read-only gates (03 §10.1) with PostgreSQL's codes; any refusal is not applied.</summary>
    void ReadGate(Microsoft.Data.Sqlite.SqliteConnection conn, string application)
    {
        bool open;
        try
        {
            open = CheckGate(conn);
        }
        catch (StoreIncompatibleException ex)
        {
            throw new StoreNotAppliedException(ex.Message, ex);
        }

        if (!open)
        {
            throw new StoreNotAppliedException("store_maintenance");
        }

        using var cmd = Cmd(conn, application, "SELECT admission_mode FROM runwick_application_controls WHERE application = @application");
        var mode = cmd.ExecuteScalar() as string;
        if (mode is null or "maintenance")
        {
            throw new StoreNotAppliedException(mode is null ? "application_not_registered" : "application_maintenance");
        }
    }

    /// <summary>08 §2 bounds; returns the offending field or null (as PgStore.ValidateActor).</summary>
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

    static void ControlEvent(SqTx tx, Guid? jobId, Guid? attemptId, string kind, ControlActor actor, JsonObject details) => tx.Exec(tx.Sql("""
        INSERT INTO runwick_queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
        VALUES (@application, @job_id, @attempt_id, @command_id, @kind, @now, @actor, @details)
        """).P("job_id", jobId).P("attempt_id", attemptId).P("command_id", tx.Op.CommandId).P("kind", kind).P("now", tx.Now)
        .P("actor", actor.Subject).P("details", details));

    /// <summary>The management audit row (08 §5), written only on success, in the effect's transaction.</summary>
    static void Audit(SqTx tx, string targetKind, string targetId, ControlActor actor, long? expectedRevision, long? resultingRevision, JsonObject details,
        string? operation = null) => Expect(tx.Exec(tx.Sql("""
        INSERT INTO runwick_management_audit (
            application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
            expected_revision, resulting_revision, details, occurred_at)
        VALUES (@application, @command_id, @operation, @target_kind, @target_id, @subject, @display, @scheme, @reason,
                @expected, @resulting, @details, @now)
        """).P("command_id", tx.Op.CommandId).P("operation", operation ?? tx.Op.Operation).P("target_kind", targetKind).P("target_id", targetId)
        .P("subject", actor.Subject).P("display", actor.Display).P("scheme", actor.Scheme).P("reason", actor.Reason)
        .P("expected", expectedRevision).P("resulting", resultingRevision).P("details", details).P("now", tx.Now)) == 1);
}
