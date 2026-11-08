using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Management reads and RecordDisclosure/1 (16 §2, §3, §6; 20 §7): PgStore.Management's statements. Reads run on the read
/// path under the 1 s budget of 20 §5.3 (an interrupted read is <c>statement_timeout</c>); disclosure runs on the write path.
/// The caller's allowed queues are a <c>queue IN json_each(...)</c> predicate before the LIMIT.
/// </summary>
internal sealed partial class SqliteStore : IManagementQueryStore
{
    static readonly TimeSpan ReadBudget = TimeSpan.FromSeconds(1);

    /// <summary>One read snapshot under the read budget; the value carries the store time.</summary>
    Task<StoreRead<T>> ManagementReadAsync<T>(string application, Func<SqTx, T> body, CancellationToken ct) =>
        runner.ReadAsync(conn =>
        {
            ReadGate(conn, application);
            var tx = ReadTx(conn, application, ManagementRead, ct);
            return new StoreRead<T>(body(tx), tx.Now);
        }, ReadBudget, ct);

    static SqliteCommand Queues(SqliteCommand cmd, IReadOnlyList<string>? queues) => queues is null ? cmd : cmd.Arr("queues", queues);

    static string QueuePredicate(IReadOnlyList<string>? queues, string column = "queue") =>
        queues is null ? "" : $" AND {column} IN (SELECT value FROM json_each(@queues))";

    const string JobColumns = """
        job_id, job_name, payload_version, queue, priority, state, generation, revision, created_at, updated_at, eligible_at,
        terminal_at, terminal_reason, cancel_requested_at IS NOT NULL, execution_count, detail_expired_at
        """;

    static JobSummary ReadSummary(SqliteDataReader r) => new(
        r.Uuid(0), r.GetString(1), r.GetInt32(2), r.GetString(3), r.GetString(4), r.GetString(5), r.GetInt32(6), r.GetInt64(7),
        r.Time(8), r.Time(9), r.Time(10), r.TimeN(11), r.StrN(12), r.Bool(13), r.GetInt64(14), r.TimeN(15));

    public Task<StoreRead<IReadOnlyList<JobSummary>>> ListJobsAsync(string application, IReadOnlyList<string>? queues, JobListFilter filter,
        (DateTimeOffset CreatedAt, Guid JobId)? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<JobSummary>>(application, tx =>
        {
            var sql = new StringBuilder($"SELECT {JobColumns} FROM runwick_jobs WHERE application = @application")
                .Append(QueuePredicate(queues))
                .Append(filter.Queue is null ? "" : " AND queue = @queue")
                .Append(filter.State is null ? "" : " AND state = @state")
                .Append(filter.JobName is null ? "" : " AND job_name = @job_name")
                .Append(" AND created_at >= @from AND created_at <= @to")
                .Append(after is null ? "" : " AND (created_at, job_id) < (@after_at, @after_id)")
                .Append(" ORDER BY created_at DESC, job_id DESC LIMIT @limit");
            return tx.Query(Queues(tx.Sql(sql.ToString()), queues).P("queue", filter.Queue).P("state", filter.State).P("job_name", filter.JobName)
                .P("from", filter.From).P("to", filter.To).P("after_at", after?.CreatedAt).P("after_id", after?.JobId).P("limit", limit), ReadSummary);
        }, ct);

    public async Task<StoreRead<JobDetail?>> GetJobAsync(string application, Guid jobId, CancellationToken ct)
    {
        var read = await ManagementReadAsync<JobDetail?>(application, tx =>
        {
            var job = tx.One(tx.Sql($"SELECT {JobColumns} FROM runwick_jobs WHERE application = @application AND job_id = @job_id").P("job_id", jobId),
                ReadSummary);
            if (job is null)
            {
                return null;
            }

            var generations = tx.Query(tx.Sql("""
                SELECT generation, created_reason, created_by, created_at, started_count, max_executions
                FROM runwick_job_generations WHERE application = @application AND job_id = @job_id ORDER BY generation
                """).P("job_id", jobId), r => new JobGenerationInfo(r.GetInt32(0), r.GetString(1), r.StrN(2), r.Time(3), r.GetInt32(4), r.GetInt32(5)));
            var attempts = tx.Query(tx.Sql("""
                SELECT attempt_id, generation, reservation_number, execution_number, worker_instance_id, reserved_at, authorized_at, finished_at,
                       outcome, disposition, applied_reason, json_extract(error, '$.code')
                FROM runwick_job_attempts WHERE application = @application AND job_id = @job_id ORDER BY reservation_number
                """).P("job_id", jobId), r => new JobAttemptInfo(r.Uuid(0), r.GetInt32(1), r.GetInt64(2), r.IntN(3), r.Uuid(4), r.Time(5), r.TimeN(6),
                    r.TimeN(7), r.StrN(8), r.StrN(9), r.StrN(10), r.IsDBNull(11) ? null : Convert.ToString(r.GetValue(11), System.Globalization.CultureInfo.InvariantCulture)));
            return new JobDetail(job, generations, attempts);
        }, ct).ConfigureAwait(false);
        return read;
    }

    public async Task<string?> QueueOfAsync(string application, Guid? jobId, Guid? attemptId, CancellationToken ct) =>
        (await ManagementReadAsync(application, tx => tx.One(tx.Sql(jobId is not null
            ? "SELECT queue FROM runwick_jobs WHERE application = @application AND job_id = @id"
            : """
              SELECT j.queue FROM runwick_job_attempts AS a
              JOIN runwick_jobs AS j ON j.application = a.application AND j.job_id = a.job_id
              WHERE a.application = @application AND a.attempt_id = @id
              """).P("id", (jobId ?? attemptId)!.Value), r => r.GetString(0)), ct).ConfigureAwait(false)).Value;

    public async Task<DateTimeOffset?> CommandNotAfterAsync(string application, Guid commandId, CancellationToken ct) =>
        (await ManagementReadAsync(application, tx => tx.One(tx.Sql(
            "SELECT not_after FROM runwick_storage_commands WHERE application = @application AND command_id = @id LIMIT 1").P("id", commandId),
            r => (DateTimeOffset?)r.Time(0)), ct).ConfigureAwait(false)).Value;

    public Task<StoreRead<IReadOnlyList<QueueSummary>>> ListQueuesAsync(string application, IReadOnlyList<string>? queues, string? after, int limit,
        CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<QueueSummary>>(application, tx => tx.Query(Queues(tx.Sql($"""
            SELECT queue, paused, revision FROM runwick_queue_controls
            WHERE application = @application{QueuePredicate(queues)}{(after is null ? "" : " AND queue > @after")}
            ORDER BY queue LIMIT @limit
            """), queues).P("after", after).P("limit", limit), r => new QueueSummary(r.GetString(0), r.Bool(1), r.GetInt64(2))), ct);

    public Task<StoreRead<IReadOnlyList<WorkerSummary>>> ListWorkersAsync(string application, bool includeExpired,
        (DateTimeOffset HeartbeatAt, Guid InstanceId)? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<WorkerSummary>>(application, tx => tx.Query(tx.Sql($"""
            SELECT instance_id, host_name, process_id, build_id, status, subscriptions, pool_targets, started_at, heartbeat_at, expires_at
            FROM runwick_worker_instances
            WHERE application = @application{(includeExpired ? "" : " AND expires_at > @now")}
              {(after is null ? "" : "AND (heartbeat_at, instance_id) < (@after_at, @after_id)")}
            ORDER BY heartbeat_at DESC, instance_id DESC LIMIT @limit
            """).P("now", tx.Now).P("after_at", after?.HeartbeatAt).P("after_id", after?.InstanceId).P("limit", limit),
            r => new WorkerSummary(r.Uuid(0), r.GetString(1), r.GetInt32(2), r.GetString(3), r.GetString(4),
                JsonNode.Parse(r.GetString(5)), JsonNode.Parse(r.GetString(6)), r.Time(7), r.Time(8), r.Time(9))), ct);

    public Task<StoreRead<IReadOnlyList<ScopeSummary>>> ListScopesAsync(string application, string? kind, (string Kind, string Key)? after, int limit,
        CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScopeSummary>>(application, tx => tx.Query(tx.Sql($"""
            SELECT s.scope_kind, s.scope_key, s.capacity, s.policy_revision,
                   (SELECT count(*) FROM runwick_concurrency_permits AS p
                    WHERE p.application = s.application AND p.scope_id = s.scope_id AND p.lease_expires_at > @now)
            FROM runwick_concurrency_scopes AS s
            WHERE s.application = @application{(kind is null ? "" : " AND s.scope_kind = @kind")}
              {(after is null ? "" : "AND (s.scope_kind, s.scope_key) > (@after_kind, @after_key)")}
            ORDER BY s.scope_kind, s.scope_key LIMIT @limit
            """).P("now", tx.Now).P("kind", kind).P("after_kind", after?.Kind).P("after_key", after?.Key).P("limit", limit),
            r => new ScopeSummary(r.GetString(0), r.GetString(1), r.GetInt32(2), r.GetInt64(3), r.GetInt32(4))), ct);

    public Task<StoreRead<IReadOnlyList<ScheduleSummary>>> ListSchedulesAsync(string application, string? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScheduleSummary>>(application, tx => tx.Query(tx.Sql($"""
            SELECT s.schedule_key, s.paused, s.needs_review, s.definition_revision, s.control_revision, s.next_due_at,
                   (SELECT max(o.materialized_at) FROM runwick_schedule_occurrences AS o
                    WHERE o.application = s.application AND o.schedule_id = s.schedule_id)
            FROM runwick_schedules AS s
            WHERE s.application = @application{(after is null ? "" : " AND s.schedule_key > @after")}
            ORDER BY s.schedule_key LIMIT @limit
            """).P("after", after).P("limit", limit),
            r => new ScheduleSummary(r.GetString(0), r.Bool(1), r.Bool(2), r.GetInt64(3), r.GetInt64(4), r.TimeN(5), r.TimeN(6))), ct);

    public Task<StoreRead<IReadOnlyList<AuditEntry>>> ListAuditAsync(string application, AuditFilter filter, long? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<AuditEntry>>(application, tx =>
        {
            var sql = new StringBuilder("""
                SELECT audit_id, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
                       expected_revision, resulting_revision, details, occurred_at
                FROM runwick_management_audit WHERE application = @application
                """)
                .Append(filter.TargetKind is null ? "" : " AND target_kind = @kind")
                .Append(filter.TargetId is null ? "" : " AND target_id = @target")
                .Append(filter.From is null ? "" : " AND occurred_at >= @from")
                .Append(filter.To is null ? "" : " AND occurred_at <= @to")
                .Append(after is null ? "" : " AND audit_id < @after")
                .Append(" ORDER BY audit_id DESC LIMIT @limit");
            return tx.Query(tx.Sql(sql.ToString()).P("kind", filter.TargetKind).P("target", filter.TargetId).P("from", filter.From).P("to", filter.To)
                .P("after", after).P("limit", limit), r => new AuditEntry(r.GetInt64(0), r.Uuid(1), r.GetString(2), r.GetString(3), r.GetString(4),
                    r.GetString(5), r.StrN(6), r.GetString(7), r.StrN(8), r.LongN(9), r.LongN(10), r.JsonN(11)!, r.Time(12)));
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
        var op = new SqOp(command.Application, command.CommandId, RecordDisclosure, SqTime.Floor(command.NotAfter), null, SqLane.Background);
        return await runner.ExecuteAsync<Disclosure>(op,
            tx =>
            {
                if (!tx.Decide())
                {
                    return Rollback<Disclosure>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                // The target and its retention state, inside the caller's queues; nothing outside them is confirmed.
                var target = tx.One(Queues(tx.Sql(payload
                    ? $"""
                      SELECT j.job_id, NOT j.payload_retained OR j.detail_expired_at IS NOT NULL
                      FROM runwick_jobs AS j WHERE j.application = @application AND j.job_id = @id{QueuePredicate(r.Queues, "j.queue")}
                      """
                    : $"""
                      SELECT j.job_id, j.detail_expired_at IS NOT NULL
                      FROM runwick_job_attempts AS a JOIN runwick_jobs AS j ON j.application = a.application AND j.job_id = a.job_id
                      WHERE a.application = @application AND a.attempt_id = @id{QueuePredicate(r.Queues, "j.queue")}
                      """), r.Queues).P("id", r.TargetId), x => (JobId: x.Uuid(0), Expired: x.Bool(1)));
                if (target == default)
                {
                    return Done(new Disclosure("not_found", null, null, null, null));
                }

                if (target.Expired)
                {
                    return Done(new Disclosure("detail_expired", target.JobId, null, null, null));
                }

                // The audit row first: if it fails, nothing is read and nothing is returned (MGMT-09).
                Expect(tx.Exec(tx.Sql("""
                    INSERT INTO runwick_management_audit (
                        application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason, details, occurred_at)
                    VALUES (@application, @command_id, @operation, @target_kind, @target_id, @subject, @display, @scheme, @reason, '{}', @now)
                    """).P("command_id", tx.Op.CommandId).P("operation", r.Kind).P("target_kind", payload ? "job" : "attempt")
                    .P("target_id", r.TargetId.ToString("D")).P("subject", r.Actor.Subject).P("display", r.Actor.Display).P("scheme", r.Actor.Scheme)
                    .P("reason", r.Actor.Reason).P("now", tx.Now)) == 1);

                var rows = tx.Query(tx.Sql(payload
                    ? "SELECT payload_utf8, headers, NULL FROM runwick_job_payloads WHERE application = @application AND job_id = @id"
                    : "SELECT NULL, NULL, error FROM runwick_job_attempts WHERE application = @application AND attempt_id = @id").P("id", r.TargetId),
                    x => (Payload: x.IsDBNull(0) ? null : Encoding.UTF8.GetString(x.Blob(0)), Headers: x.StrN(1), Error: x.StrN(2)));
                if (rows.Count == 0)
                {
                    return Rollback<Disclosure>(tx, StoreStatus.HistoryUnavailable, "payload_unavailable");
                }

                var value = new Disclosure("disclosed", target.JobId, Parse(rows[0].Payload), Parse(rows[0].Headers), Parse(rows[0].Error));
                return new SqOutcome<Disclosure>.Commit(StoreReply<Disclosure>.Applied(value, tx.Now), null);

                SqOutcome<Disclosure> Done(Disclosure v) => new SqOutcome<Disclosure>.Rollback(StoreReply<Disclosure>.Applied(v, tx.Now));
            },
            replay: null, ct).ConfigureAwait(false);
    }

    static JsonNode? Parse(string? json) => json is null ? null : JsonNode.Parse(json);
}
