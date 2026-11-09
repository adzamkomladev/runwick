using System.Text;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Management reads and RecordDisclosure/1 (16 §2, §3, §6). Reads are keyset pages: the caller's allowed queues are a
/// <c>queue = ANY(...)</c> predicate before the LIMIT, and each read runs under a 1 s budget (its statement timeout).
/// </summary>
internal sealed partial class PgStore : IManagementQueryStore
{
    internal const string ManagementRead = "ManagementRead";
    internal const string RecordDisclosure = "RecordDisclosure";
    static readonly TimeSpan ReadBudget = TimeSpan.FromSeconds(1);

    /// <summary>One read-only transaction (rolled back) under the read budget; the value carries the store time.</summary>
    async Task<StoreRead<T>> ReadAsync<T>(string application, Func<PgTx, Task<T>> body, CancellationToken ct)
    {
        var op = new PgOp(application, Guid.CreateVersion7(), ManagementRead, PgTime.Floor(time.GetUtcNow() + ReadBudget), null, PgLane.Background, null);
        var reply = await runner.ExecuteAsync<StoreRead<T>>(op,
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<StoreRead<T>>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                var value = await body(tx).ConfigureAwait(false);
                return new PgOutcome<StoreRead<T>>.Rollback(StoreReply<StoreRead<T>>.Applied(new StoreRead<T>(value, tx.Now), tx.Now));
            },
            replay: null, ct).ConfigureAwait(false);
        return reply.Value ?? throw new StoreNotAppliedException(reply.Error?.Code ?? "store_unavailable");
    }

    static NpgsqlCommand Queues(NpgsqlCommand cmd, IReadOnlyList<string>? queues) =>
        queues is null ? cmd : cmd.Arr("queues", queues.ToArray(), NpgsqlDbType.Text);

    static string QueuePredicate(IReadOnlyList<string>? queues, string column = "queue") =>
        queues is null ? "" : $" AND {column} = ANY(@queues)";

    const string JobColumns = """
        job_id, job_name, payload_version, queue, priority, state, generation, revision, created_at, updated_at, eligible_at,
        terminal_at, terminal_reason, cancel_requested_at IS NOT NULL, execution_count, detail_expired_at
        """;

    static JobSummary ReadJob(NpgsqlDataReader r) => new(
        r.GetGuid(0), r.GetString(1), r.GetInt32(2), r.GetString(3), r.GetString(4), r.GetString(5), r.GetInt32(6), r.GetInt64(7),
        PgTime.Read(r, 8), PgTime.Read(r, 9), PgTime.Read(r, 10), PgTime.ReadNullable(r, 11), r.IsDBNull(12) ? null : r.GetString(12),
        r.GetBoolean(13), r.GetInt64(14), PgTime.ReadNullable(r, 15));

    public Task<StoreRead<IReadOnlyList<JobSummary>>> ListJobsAsync(string application, IReadOnlyList<string>? queues, JobListFilter filter,
        (DateTimeOffset CreatedAt, Guid JobId)? after, int limit, CancellationToken ct) =>
        ReadAsync<IReadOnlyList<JobSummary>>(application, async tx =>
        {
            // Predicates are appended, not "@x IS NULL OR ...", so each filter shape plans onto its index (16 §6).
            var sql = new StringBuilder($"SELECT {JobColumns} FROM runwick.jobs WHERE application = @application")
                .Append(QueuePredicate(queues))
                .Append(filter.Queue is null ? "" : " AND queue = @queue")
                .Append(filter.State is null ? "" : " AND state = @state")
                .Append(filter.JobName is null ? "" : " AND job_name = @job_name")
                .Append(" AND created_at >= @from AND created_at <= @to")
                .Append(after is null ? "" : " AND (created_at, job_id) < (@after_at, @after_id)")
                .Append(" ORDER BY created_at DESC, job_id DESC LIMIT @limit");
            using var q = Queues(tx.Sql(sql.ToString()), queues).P("queue", filter.Queue).P("state", filter.State).P("job_name", filter.JobName)
                .P("from", filter.From).P("to", filter.To).P("after_at", after?.CreatedAt).P("after_id", after?.JobId, NpgsqlDbType.Uuid).P("limit", limit);
            return await tx.QueryAsync(q, ReadJob).ConfigureAwait(false);
        }, ct);

    public async Task<StoreRead<JobDetail?>> GetJobAsync(string application, Guid jobId, CancellationToken ct)
    {
        var read = await ReadAsync<JobDetail[]>(application, async tx =>
        {
            using var j = tx.Sql($"SELECT {JobColumns} FROM runwick.jobs WHERE application = @application AND job_id = @job_id").P("job_id", jobId);
            var jobs = await tx.QueryAsync(j, ReadJob).ConfigureAwait(false);
            if (jobs.Count == 0)
            {
                return [];
            }

            using var g = tx.Sql("""
                SELECT generation, created_reason, created_by, created_at, started_count, max_executions
                FROM runwick.job_generations WHERE application = @application AND job_id = @job_id ORDER BY generation
                """).P("job_id", jobId);
            var generations = await tx.QueryAsync(g, r => new JobGenerationInfo(r.GetInt32(0), r.GetString(1), Str(r, 2), PgTime.Read(r, 3),
                r.GetInt32(4), r.GetInt32(5))).ConfigureAwait(false);
            using var a = tx.Sql("""
                SELECT attempt_id, generation, reservation_number, execution_number, worker_instance_id, reserved_at, authorized_at, finished_at,
                       outcome, disposition, applied_reason, error ->> 'code', usage::text
                FROM runwick.job_attempts WHERE application = @application AND job_id = @job_id ORDER BY reservation_number
                """).P("job_id", jobId);
            var attempts = await tx.QueryAsync(a, r => new JobAttemptInfo(r.GetGuid(0), r.GetInt32(1), r.GetInt64(2), r.IsDBNull(3) ? null : r.GetInt32(3),
                r.GetGuid(4), PgTime.Read(r, 5), PgTime.ReadNullable(r, 6), PgTime.ReadNullable(r, 7), Str(r, 8), Str(r, 9), Str(r, 10), Str(r, 11),
                Runwick.Worker.ResourceUsageJson.Read(Str(r, 12))))
                .ConfigureAwait(false);
            return [new JobDetail(jobs[0], generations, attempts)];
        }, ct).ConfigureAwait(false);
        return new StoreRead<JobDetail?>(read.Value.FirstOrDefault(), read.ObservedAt);
    }

    public async Task<string?> QueueOfAsync(string application, Guid? jobId, Guid? attemptId, CancellationToken ct)
    {
        var read = await ReadAsync<string[]>(application, async tx =>
        {
            using var q = tx.Sql(jobId is not null
                ? "SELECT queue FROM runwick.jobs WHERE application = @application AND job_id = @id"
                : """
                  SELECT j.queue FROM runwick.job_attempts AS a
                  JOIN runwick.jobs AS j ON j.application = a.application AND j.job_id = a.job_id
                  WHERE a.application = @application AND a.attempt_id = @id
                  """).P("id", (jobId ?? attemptId)!.Value);
            return [.. await tx.QueryAsync(q, r => r.GetString(0)).ConfigureAwait(false)];
        }, ct).ConfigureAwait(false);
        return read.Value.FirstOrDefault();
    }

    public async Task<DateTimeOffset?> CommandNotAfterAsync(string application, Guid commandId, CancellationToken ct)
    {
        var read = await ReadAsync<DateTimeOffset[]>(application, async tx =>
        {
            using var q = tx.Sql("SELECT not_after FROM runwick.storage_commands WHERE application = @application AND command_id = @id LIMIT 1")
                .P("id", commandId);
            return [.. await tx.QueryAsync(q, r => PgTime.Read(r, 0)).ConfigureAwait(false)];
        }, ct).ConfigureAwait(false);
        return read.Value.Length == 0 ? null : read.Value[0];
    }

    public Task<StoreRead<IReadOnlyList<QueueSummary>>> ListQueuesAsync(string application, IReadOnlyList<string>? queues, string? after, int limit,
        CancellationToken ct) =>
        ReadAsync<IReadOnlyList<QueueSummary>>(application, async tx =>
        {
            using var q = Queues(tx.Sql($"""
                SELECT queue, paused, revision FROM runwick.queue_controls
                WHERE application = @application{QueuePredicate(queues)}{(after is null ? "" : " AND queue > @after")}
                ORDER BY queue LIMIT @limit
                """), queues).P("after", after).P("limit", limit);
            return await tx.QueryAsync(q, r => new QueueSummary(r.GetString(0), r.GetBoolean(1), r.GetInt64(2))).ConfigureAwait(false);
        }, ct);

    public Task<StoreRead<IReadOnlyList<WorkerSummary>>> ListWorkersAsync(string application, bool includeExpired,
        (DateTimeOffset HeartbeatAt, Guid InstanceId)? after, int limit, CancellationToken ct) =>
        ReadAsync<IReadOnlyList<WorkerSummary>>(application, async tx =>
        {
            using var q = tx.Sql($"""
                SELECT instance_id, host_name, process_id, build_id, status, subscriptions::text, pool_targets::text, started_at, heartbeat_at, expires_at
                FROM runwick.worker_instances
                WHERE application = @application{(includeExpired ? "" : " AND expires_at > @now")}
                  {(after is null ? "" : "AND (heartbeat_at, instance_id) < (@after_at, @after_id)")}
                ORDER BY heartbeat_at DESC, instance_id DESC LIMIT @limit
                """).P("now", tx.Now).P("after_at", after?.HeartbeatAt).P("after_id", after?.InstanceId, NpgsqlDbType.Uuid).P("limit", limit);
            return await tx.QueryAsync(q, r => new WorkerSummary(r.GetGuid(0), r.GetString(1), r.GetInt32(2), r.GetString(3), r.GetString(4),
                JsonNode.Parse(r.GetString(5)), JsonNode.Parse(r.GetString(6)), PgTime.Read(r, 7), PgTime.Read(r, 8), PgTime.Read(r, 9))).ConfigureAwait(false);
        }, ct);

    public Task<StoreRead<IReadOnlyList<ScopeSummary>>> ListScopesAsync(string application, string? kind, (string Kind, string Key)? after, int limit,
        CancellationToken ct) =>
        ReadAsync<IReadOnlyList<ScopeSummary>>(application, async tx =>
        {
            using var q = tx.Sql($"""
                SELECT s.scope_kind, s.scope_key, s.capacity, s.policy_revision,
                       (SELECT count(*)::int FROM runwick.concurrency_permits AS p
                        WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.lease_expires_at > @now)
                FROM runwick.concurrency_scopes AS s
                WHERE s.application = @application{(kind is null ? "" : " AND s.scope_kind = @kind")}
                  {(after is null ? "" : "AND (s.scope_kind, s.scope_key) > (@after_kind, @after_key)")}
                ORDER BY s.scope_kind, s.scope_key LIMIT @limit
                """).P("now", tx.Now).P("kind", kind).P("after_kind", after?.Kind).P("after_key", after?.Key).P("limit", limit);
            return await tx.QueryAsync(q, r => new ScopeSummary(r.GetString(0), r.GetString(1), r.GetInt32(2), r.GetInt64(3), r.GetInt32(4))).ConfigureAwait(false);
        }, ct);

    public Task<StoreRead<IReadOnlyList<ScheduleSummary>>> ListSchedulesAsync(string application, string? after, int limit, CancellationToken ct) =>
        ReadAsync<IReadOnlyList<ScheduleSummary>>(application, async tx =>
        {
            using var q = tx.Sql($"""
                SELECT s.schedule_key, s.paused, s.needs_review, s.definition_revision, s.control_revision, s.next_due_at,
                       (SELECT max(o.materialized_at) FROM runwick.schedule_occurrences AS o
                        WHERE o.application = s.application AND o.schedule_id = s.schedule_id)
                FROM runwick.schedules AS s
                WHERE s.application = @application{(after is null ? "" : " AND s.schedule_key > @after")}
                ORDER BY s.schedule_key LIMIT @limit
                """).P("after", after).P("limit", limit);
            return await tx.QueryAsync(q, r => new ScheduleSummary(r.GetString(0), r.GetBoolean(1), r.GetBoolean(2), r.GetInt64(3), r.GetInt64(4),
                PgTime.ReadNullable(r, 5), PgTime.ReadNullable(r, 6))).ConfigureAwait(false);
        }, ct);

    public Task<StoreRead<IReadOnlyList<AuditEntry>>> ListAuditAsync(string application, AuditFilter filter, long? after, int limit, CancellationToken ct) =>
        ReadAsync<IReadOnlyList<AuditEntry>>(application, async tx =>
        {
            var sql = new StringBuilder("""
                SELECT audit_id, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
                       expected_revision, resulting_revision, details::text, occurred_at
                FROM runwick.management_audit WHERE application = @application
                """)
                .Append(filter.TargetKind is null ? "" : " AND target_kind = @kind")
                .Append(filter.TargetId is null ? "" : " AND target_id = @target")
                .Append(filter.From is null ? "" : " AND occurred_at >= @from")
                .Append(filter.To is null ? "" : " AND occurred_at <= @to")
                .Append(after is null ? "" : " AND audit_id < @after")
                .Append(" ORDER BY audit_id DESC LIMIT @limit");
            using var q = tx.Sql(sql.ToString()).P("kind", filter.TargetKind).P("target", filter.TargetId).P("from", filter.From).P("to", filter.To)
                .P("after", after, NpgsqlDbType.Bigint).P("limit", limit);
            return await tx.QueryAsync(q, r => new AuditEntry(r.GetInt64(0), r.GetGuid(1), r.GetString(2), r.GetString(3), r.GetString(4), r.GetString(5),
                Str(r, 6), r.GetString(7), Str(r, 8), r.IsDBNull(9) ? null : r.GetInt64(9), r.IsDBNull(10) ? null : r.GetInt64(10),
                JsonNode.Parse(r.GetString(11))!.AsObject(), PgTime.Read(r, 12))).ConfigureAwait(false);
        }, ct);

    /// <summary>Served by <c>scaling_events_retention (application, created_at, event_id)</c> read backwards (21 §6).</summary>
    public Task<StoreRead<IReadOnlyList<ScalingEventSummary>>> ListScalingEventsAsync(string application, ScalingEventFilter filter,
        (DateTimeOffset CreatedAt, Guid EventId)? after, int limit, CancellationToken ct) =>
        ReadAsync<IReadOnlyList<ScalingEventSummary>>(application, async tx =>
        {
            var sql = new StringBuilder("""
                SELECT event_id, instance_id, pool_name, old_target, new_target, desired, candidate, reason, snapshot_at, created_at
                FROM runwick.scaling_events WHERE application = @application AND created_at >= @from AND created_at <= @to
                """)
                .Append(filter.InstanceId is null ? "" : " AND instance_id = @instance")
                .Append(filter.Pool is null ? "" : " AND pool_name = @pool")
                .Append(after is null ? "" : " AND (created_at, event_id) < (@after_at, @after_id)")
                .Append(" ORDER BY created_at DESC, event_id DESC LIMIT @limit");
            using var q = tx.Sql(sql.ToString()).P("from", filter.From).P("to", filter.To).P("instance", filter.InstanceId, NpgsqlDbType.Uuid)
                .P("pool", filter.Pool).P("after_at", after?.CreatedAt).P("after_id", after?.EventId, NpgsqlDbType.Uuid).P("limit", limit);
            return await tx.QueryAsync(q, r => new ScalingEventSummary(r.GetGuid(0), r.GetGuid(1), r.GetString(2), r.GetInt32(3), r.GetInt32(4),
                r.IsDBNull(5) ? null : r.GetInt32(5), r.IsDBNull(6) ? null : r.GetInt32(6), r.GetString(7), PgTime.ReadNullable(r, 8),
                PgTime.Read(r, 9))).ConfigureAwait(false);
        }, ct);

    // ------------------------------------------------------------------ RecordDisclosure/1 (16 §3)

    public async Task<StoreReply<Disclosure>> RecordDisclosureAsync(StoreCommand<DisclosureRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return StoreReply<Disclosure>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalid), time.GetUtcNow());
        }

        var payload = r.Kind == "RevealPayload";
        var op = new PgOp(command.Application, command.CommandId, RecordDisclosure, command.NotAfter, null, PgLane.Background, null);
        return await runner.ExecuteAsync<Disclosure>(op,
            async tx =>
            {
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<Disclosure>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                // The target and its retention state, inside the caller's queues; nothing outside them is confirmed.
                using var t = Queues(tx.Sql(payload
                    ? $"""
                      SELECT j.job_id, NOT j.payload_retained OR j.detail_expired_at IS NOT NULL
                      FROM runwick.jobs AS j WHERE j.application = @application AND j.job_id = @id{QueuePredicate(r.Queues, "j.queue")}
                      """
                    : $"""
                      SELECT j.job_id, j.detail_expired_at IS NOT NULL
                      FROM runwick.job_attempts AS a JOIN runwick.jobs AS j ON j.application = a.application AND j.job_id = a.job_id
                      WHERE a.application = @application AND a.attempt_id = @id{QueuePredicate(r.Queues, "j.queue")}
                      """), r.Queues).P("id", r.TargetId);
                var target = await tx.QueryAsync(t, x => (JobId: x.GetGuid(0), Expired: x.GetBoolean(1))).ConfigureAwait(false);
                if (target.Count == 0)
                {
                    return Done(new Disclosure("not_found", null, null, null, null));
                }

                if (target[0].Expired)
                {
                    return Done(new Disclosure("detail_expired", target[0].JobId, null, null, null));
                }

                // The audit row first: if it fails, nothing is read and nothing is returned (MGMT-09).
                using (var a = tx.Sql("""
                    INSERT INTO runwick.management_audit (
                        application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason, details, occurred_at)
                    VALUES (@application, @command_id, @operation, @target_kind, @target_id, @subject, @display, @scheme, @reason, @details, @now)
                    """).P("command_id", tx.Op.CommandId).P("operation", r.Kind).P("target_kind", payload ? "job" : "attempt")
                        .P("target_id", r.TargetId.ToString("D")).P("subject", r.Actor.Subject).P("display", r.Actor.Display).P("scheme", r.Actor.Scheme)
                        .P("reason", r.Actor.Reason).Json("details", new JsonObject()).P("now", tx.Now))
                {
                    Expect(await tx.ExecAsync(a).ConfigureAwait(false), 1);
                }

                using var d = tx.Sql(payload
                    ? "SELECT convert_from(payload_utf8, 'UTF8'), headers::text, NULL FROM runwick.job_payloads WHERE application = @application AND job_id = @id"
                    : "SELECT NULL, NULL, error::text FROM runwick.job_attempts WHERE application = @application AND attempt_id = @id").P("id", r.TargetId);
                var rows = await tx.QueryAsync(d, x => (Payload: Str(x, 0), Headers: Str(x, 1), Error: Str(x, 2))).ConfigureAwait(false);
                if (rows.Count == 0)
                {
                    return Rollback<Disclosure>(tx, StoreStatus.HistoryUnavailable, "payload_unavailable");
                }

                var value = new Disclosure("disclosed", target[0].JobId, Parse(rows[0].Payload), Parse(rows[0].Headers), Parse(rows[0].Error));
                return new PgOutcome<Disclosure>.Commit(StoreReply<Disclosure>.Applied(value, tx.Now), null);

                PgOutcome<Disclosure> Done(Disclosure v) => new PgOutcome<Disclosure>.Rollback(StoreReply<Disclosure>.Applied(v, tx.Now));
            },
            replay: null, ct).ConfigureAwait(false);
    }

    static JsonNode? Parse(string? json) => json is null ? null : JsonNode.Parse(json);
}
