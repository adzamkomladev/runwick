using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Retention (17, 20 §7): CleanupBatch/1, EditRetentionPolicy/1 and the retention read, as PgStore.Retention. The write lock
/// excludes all live work for the batch's length, so there is nothing to skip: every row the predicate selects is
/// unlocked, the batch never reports <c>skipped_locked</c>, and Dispatch's key-row repeat (17 §3.3) can never be needed.
/// </summary>
internal sealed partial class SqliteStore : IRetentionStore
{
    static readonly string[] CompletedStates = ["succeeded", "cancelled"], FailedStates = ["failed"];

    /// <summary>One batch's deletions: rows deleted, whether the pass is complete, and the cursor to store (null at the end).</summary>
    readonly record struct Swept(int Deleted, bool Complete, DateTimeOffset? CursorAt = null, string? CursorKey = null);

    public async Task<CleanupBatchResult> RunCleanupBatchAsync(string application, string recordClass, CleanupLimits limits, CancellationToken ct)
    {
        if (!RecordClasses.All.Contains(recordClass, StringComparer.Ordinal))
        {
            throw new ArgumentOutOfRangeException(nameof(recordClass), recordClass, "Unknown record class.");
        }

        string code;
        try
        {
            // shortcut: no statement timeout on the write path (SQLite has no per-statement timer here); the batch is bounded by its row limit.
            var op = new SqOp(application, Guid.CreateVersion7(), Operations.CleanupBatch,
                SqTime.Floor(time.GetUtcNow() + limits.StatementTimeout + TimeSpan.FromSeconds(5)), null, SqLane.Background);
            var reply = await runner.ExecuteAsync<CleanupBatchResult>(op, tx => Batch(tx, recordClass, limits), replay: null, ct).ConfigureAwait(false);
            if (reply.Value is { } done)
            {
                return done;
            }

            code = reply.Error?.Code ?? reply.Status.ToString();
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && ex is StoreNotAppliedException or StoreOutcomeUnknownException or SqliteException)
        {
            code = ex switch
            {
                StoreNotAppliedException n => n.Code,
                StoreOutcomeUnknownException => "outcome_unknown",
                SqliteException s => "sqlite_" + s.SqliteErrorCode.ToString(CultureInfo.InvariantCulture),
                _ => "store_unavailable",
            };
        }

        await RecordCleanupFailureAsync(application, recordClass, code, ct).ConfigureAwait(false);
        return new CleanupBatchResult("failed", 0, false, false, code);
    }

    /// <summary>A failed batch rolled back; its error is recorded in a separate small transaction, best effort.</summary>
    async Task RecordCleanupFailureAsync(string application, string recordClass, string code, CancellationToken ct)
    {
        try
        {
            var op = new SqOp(application, Guid.CreateVersion7(), Operations.CleanupBatch, SqTime.Floor(time.GetUtcNow() + TimeSpan.FromSeconds(5)), null,
                SqLane.Background);
            await runner.ExecuteAsync<CleanupBatchResult>(op, tx =>
            {
                tx.Exec(tx.Sql("""
                    INSERT INTO runwick_cleanup_progress (application, record_class, last_error_code, last_error_at, day_failures)
                    VALUES (@application, @class, substr(@code, 1, 64), @now, 1)
                    ON CONFLICT (application, record_class) DO UPDATE
                    SET last_error_code = excluded.last_error_code, last_error_at = excluded.last_error_at, day_failures = day_failures + 1
                    """).P("class", recordClass).P("code", code).P("now", tx.Now), mutation: false);
                return new SqOutcome<CleanupBatchResult>.Commit(StoreReply<CleanupBatchResult>.Applied(new CleanupBatchResult("failed", 0, false, false, code), tx.Now), null);
            }, replay: null, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is StoreNotAppliedException or StoreOutcomeUnknownException or SqliteException or IOException)
        {
            // Progress is observational; the next batch runs regardless.
        }
    }

    static SqOutcome<CleanupBatchResult> Batch(SqTx tx, string recordClass, CleanupLimits limits)
    {
        // The progress row is created on first use (17 §4.2); the write lock makes the seed and read one step.
        tx.Exec(tx.Sql("INSERT INTO runwick_cleanup_progress (application, record_class) VALUES (@application, @class) ON CONFLICT DO NOTHING")
            .P("class", recordClass), mutation: false);
        var row = tx.Query(tx.Sql("""
            SELECT cursor_at, cursor_key, summary_day, day_rows, day_batches, day_failures, pass_rows
            FROM runwick_cleanup_progress WHERE application = @application AND record_class = @class
            """).P("class", recordClass), r => (CursorAt: r.TimeN(0), CursorKey: r.StrN(1),
            Day: r.IsDBNull(2) ? (DateOnly?)null : DateOnly.ParseExact(r.GetString(2), "yyyy-MM-dd", CultureInfo.InvariantCulture),
            Rows: r.GetInt64(3), Batches: r.GetInt32(4), Failures: r.GetInt32(5), PassRows: r.GetInt64(6)))[0];

        if (!tx.Decide())
        {
            return new SqOutcome<CleanupBatchResult>.Rollback(tx.Reject<CleanupBatchResult>(StoreStatus.CommandExpired, "command_expired"));
        }

        var now = tx.Now;
        var today = DateOnly.FromDateTime(now.UtcDateTime);
        if (row.Day is { } day && day < today)
        {
            Summary(tx, recordClass, day, row.Rows, row.Batches, row.Failures);
        }

        var policy = ReadPolicy(tx).Policy;
        TimeSpan? horizon = recordClass switch
        {
            RecordClasses.JobDetailCompleted => policy.CompletedDetail,
            RecordClasses.JobDetailFailed => policy.FailedDetail,
            RecordClasses.JobHistory or RecordClasses.JobEvents => policy.JobHistory,
            RecordClasses.ManagementAudit or RecordClasses.ScheduleAudit => policy.Audit,
            RecordClasses.ScalingEvents => policy.ScalingEvents,
            RecordClasses.ScheduleOccurrences => policy.ScheduleOccurrences,
            RecordClasses.WorkerPresence => policy.WorkerPresence,
            _ => TimeSpan.Zero, // fixed classes: their own predicate against now
        };
        var cutoff = now - (horizon ?? TimeSpan.Zero);
        var cursor = row.CursorAt is { } at && row.CursorKey is { } key ? (at, Guid.Parse(key)) : ((DateTimeOffset, Guid)?)null;
        var rows = limits.Rows;
        var swept = horizon is null ? new Swept(0, true) : recordClass switch
        {
            RecordClasses.JobDetailCompleted => ExpireDetail(tx, CompletedStates, cutoff, cursor, limits),
            RecordClasses.JobDetailFailed => ExpireDetail(tx, FailedStates, cutoff, cursor, limits),
            RecordClasses.JobHistory => PurgeJobs(tx, cutoff, cursor, rows),
            RecordClasses.JobEvents => Ended(Delete(tx, "queue_events", "event_id", "occurred_at < @cutoff", "occurred_at, event_id", rows, cutoff), rows),
            RecordClasses.ManagementAudit => PurgeAudit(tx, "management_audit", "occurred_at", cutoff, rows),
            RecordClasses.ScheduleAudit => PurgeAudit(tx, "schedule_audit_ranges", "created_at", cutoff, rows),
            RecordClasses.ScalingEvents => Ended(Delete(tx, "scaling_events", "event_id", "created_at < @cutoff", "created_at, event_id", rows, cutoff), rows),
            RecordClasses.ScheduleOccurrences => Ended(Delete(tx, "schedule_occurrences", "schedule_id, occurrence_kind, occurrence_key",
                "materialized_at < @cutoff", "materialized_at, schedule_id, occurrence_kind, occurrence_key", rows, cutoff), rows),
            RecordClasses.WorkerPresence => Ended(Delete(tx, "worker_instances", "instance_id", "expires_at < @cutoff", "expires_at, instance_id", rows, cutoff), rows),
            RecordClasses.ReceiptsOwnership or RecordClasses.ReceiptsDurable => Ended(Delete(tx, "storage_commands", "retention_class, command_id",
                $"retention_class = '{(recordClass == RecordClasses.ReceiptsOwnership ? Operations.OwnershipClass : Operations.DurableClass)}' AND retain_until < @cutoff",
                "retain_until, command_id", rows, cutoff), rows),
            RecordClasses.DispatchKeys => PurgeKeys(tx, now, rows),
            RecordClasses.RateGrants => PruneGrants(tx, row.CursorKey, rows),
            _ => Ended(Delete(tx, "outbox_deliveries", "source_store_id, message_id", "retain_until < @cutoff", "retain_until, source_store_id, message_id",
                rows, now), rows),
        };

        var todayText = today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var n = tx.Exec(tx.Sql("""
            UPDATE runwick_cleanup_progress
            SET cursor_at = @cursor_at, cursor_key = @cursor_key,
                pass_started_at = CASE WHEN @complete THEN NULL ELSE COALESCE(pass_started_at, @now) END,
                caught_up_at = CASE WHEN @complete AND pass_rows + @deleted = 0 THEN @now ELSE caught_up_at END,
                pass_rows = CASE WHEN @complete THEN 0 ELSE pass_rows + @deleted END,
                last_batch_at = @now, last_rows = @deleted, total_rows = total_rows + @deleted,
                day_rows = CASE WHEN summary_day = @today THEN day_rows ELSE 0 END + @deleted,
                day_batches = CASE WHEN summary_day = @today THEN day_batches ELSE 0 END + 1,
                day_failures = CASE WHEN summary_day = @today THEN day_failures ELSE 0 END,
                summary_day = @today
            WHERE application = @application AND record_class = @class
            """).P("class", recordClass).P("cursor_at", swept.CursorAt).P("cursor_key", swept.CursorKey).P("complete", swept.Complete)
            .P("deleted", swept.Deleted).P("now", now).P("today", todayText), mutation: false);
        if (n != 1)
        {
            throw new SqInvariantException("invalid_transition");
        }

        var result = new CleanupBatchResult("done", swept.Deleted, swept.Complete, swept.Complete && row.PassRows + swept.Deleted == 0, null);
        return new SqOutcome<CleanupBatchResult>.Commit(StoreReply<CleanupBatchResult>.Applied(result, now), null);
    }

    static Swept Ended(int deleted, int rows) => new(deleted, deleted < rows);

    /// <summary>One RetentionSummary audit row for a closed UTC day (17 §4.2); its command ID is derived, so it is the same on every process.</summary>
    static void Summary(SqTx tx, string recordClass, DateOnly day, long rows, int batches, int failures)
    {
        var dayText = day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var commandId = new Guid(SHA256.HashData(Encoding.UTF8.GetBytes(tx.Application + "\0" + recordClass + "\0" + dayText)).AsSpan(0, 16), bigEndian: true);
        var n = tx.Exec(tx.Sql("""
            INSERT INTO runwick_management_audit (
                application, command_id, operation, target_kind, target_id, actor_subject, actor_scheme, details, occurred_at)
            VALUES (@application, @command_id, 'RetentionSummary', 'application', @class, 'runwick:retention', 'system', @details, @now)
            """).P("command_id", commandId).P("class", recordClass).P("now", tx.Now)
            .P("details", new JsonObject { ["batches"] = batches, ["day"] = dayText, ["failures"] = failures, ["rows"] = rows }));
        if (n != 1)
        {
            throw new SqInvariantException("invalid_transition");
        }
    }

    /// <summary>Detail expiry (17 §3.1): payload and error text go; identity, state, history and error codes stay.</summary>
    static Swept ExpireDetail(SqTx tx, string[] states, DateTimeOffset cutoff, (DateTimeOffset At, Guid Id)? cursor, CleanupLimits limits)
    {
        var c = tx.Sql("");
        c.CommandText = $"""
            SELECT j.job_id, j.terminal_at,
                   COALESCE(length(p.payload_utf8), 0)
                     + COALESCE((SELECT sum(length(CAST(a.error AS BLOB))) FROM runwick_job_attempts AS a
                                 WHERE a.application = j.application AND a.job_id = j.job_id AND a.error IS NOT NULL), 0)
            FROM runwick_jobs AS j
            LEFT JOIN runwick_job_payloads AS p ON p.application = j.application AND p.job_id = j.job_id
            WHERE j.application = @application AND j.state IN {c.In("s", states)} AND j.detail_expired_at IS NULL AND j.terminal_at < @cutoff
              {(cursor is null ? "" : "AND (j.terminal_at, j.job_id) > (@cursor_at, @cursor_id)")}
            ORDER BY j.terminal_at, j.job_id
            LIMIT @rows
            """;
        c.P("cutoff", cutoff).P("cursor_at", cursor?.At).P("cursor_id", cursor?.Id).P("rows", limits.Rows);
        var candidates = tx.Query(c, r => (Id: r.Uuid(0), At: r.Time(1), Bytes: r.GetInt64(2)));

        // The prefix within the detail budget, always at least one row.
        var kept = 0;
        for (long total = 0; kept < candidates.Count && (kept == 0 || total + candidates[kept].Bytes <= limits.DetailBytes); kept++)
        {
            total += candidates[kept].Bytes;
        }

        if (kept == 0)
        {
            return new Swept(0, true);
        }

        var ids = candidates.Take(kept).Select(x => x.Id).ToList();
        string[] statements =
        [
            "DELETE FROM runwick_job_payloads WHERE application = @application AND job_id IN {ids}",
            """
            UPDATE runwick_job_attempts SET error = json_object('code', error -> '$.code', 'expired', json('true'))
            WHERE application = @application AND job_id IN {ids} AND error IS NOT NULL
            """,
            "UPDATE runwick_jobs SET payload_retained = 0, detail_expired_at = @now WHERE application = @application AND job_id IN {ids}",
        ];
        foreach (var sql in statements)
        {
            var s = tx.Sql("");
            s.CommandText = sql.Replace("{ids}", s.In("id", ids), StringComparison.Ordinal);
            tx.Exec(s.P("now", tx.Now));
        }

        var last = candidates[kept - 1];
        return kept == candidates.Count && candidates.Count < limits.Rows ? new Swept(kept, true) : new Swept(kept, false, last.At, last.Id.ToString("D"));
    }

    /// <summary>Job history (17 §3.2): the job row and its cascade, only after detail expiry and with no live binding or permit.</summary>
    static Swept PurgeJobs(SqTx tx, DateTimeOffset cutoff, (DateTimeOffset At, Guid Id)? cursor, int rows)
    {
        var candidates = tx.Query(tx.Sql($"""
            SELECT j.job_id, j.terminal_at
            FROM runwick_jobs AS j
            WHERE j.application = @application
              AND j.state IN ('succeeded', 'failed', 'cancelled')
              AND j.terminal_at < @cutoff
              AND j.detail_expired_at IS NOT NULL
              {(cursor is null ? "" : "AND (j.terminal_at, j.job_id) > (@cursor_at, @cursor_id)")}
              AND NOT EXISTS (SELECT 1 FROM runwick_dispatch_keys AS d
                              WHERE d.application = j.application AND d.bound_job_id = j.job_id
                                AND (d.key_kind = 'active' OR d.expires_at > @now))
              AND NOT EXISTS (SELECT 1 FROM runwick_concurrency_permits AS pm
                              WHERE pm.application = j.application AND pm.job_id = j.job_id)
            ORDER BY j.terminal_at, j.job_id
            LIMIT @rows
            """).P("cutoff", cutoff).P("now", tx.Now).P("cursor_at", cursor?.At).P("cursor_id", cursor?.Id).P("rows", rows),
            r => (Id: r.Uuid(0), At: r.Time(1)));
        if (candidates.Count == 0)
        {
            return new Swept(0, true);
        }

        var d = tx.Sql("");
        d.CommandText = $"DELETE FROM runwick_jobs WHERE application = @application AND job_id IN {d.In("id", candidates.Select(x => x.Id).ToList())}";
        if (tx.Exec(d) != candidates.Count)
        {
            throw new SqInvariantException("invalid_transition");
        }

        var last = candidates[^1];
        return candidates.Count < rows ? new Swept(candidates.Count, true) : new Swept(candidates.Count, false, last.At, last.Id.ToString("D"));
    }

    /// <summary>Age-ordered delete of at most <paramref name="rows"/> rows (17 §3.6, §3.7); the scan restarts at the oldest row.</summary>
    static int Delete(SqTx tx, string table, string keys, string predicate, string order, int rows, DateTimeOffset cutoff) =>
        tx.Exec(tx.Sql($"""
            DELETE FROM runwick_{table}
            WHERE application = @application AND ({keys}) IN (
                SELECT {keys} FROM runwick_{table} WHERE application = @application AND {predicate} ORDER BY {order} LIMIT @rows)
            """).P("cutoff", cutoff).P("rows", rows));

    /// <summary>Audit (17 §3.6): identity order, deleting while older than the horizon and stopping at the first row that is not.</summary>
    static Swept PurgeAudit(SqTx tx, string table, string column, DateTimeOffset cutoff, int rows)
    {
        var scanned = tx.Query(tx.Sql($"SELECT audit_id, {column} < @cutoff FROM runwick_{table} WHERE application = @application ORDER BY audit_id LIMIT @rows")
            .P("cutoff", cutoff).P("rows", rows), r => (Id: r.GetInt64(0), Old: r.Bool(1)));
        var ids = scanned.TakeWhile(x => x.Old).Select(x => x.Id).ToList();
        if (ids.Count > 0)
        {
            var d = tx.Sql("");
            d.CommandText = $"DELETE FROM runwick_{table} WHERE application = @application AND audit_id IN {d.In("id", ids)}";
            tx.Exec(d);
        }

        return Ended(ids.Count, rows);
    }

    /// <summary>Dispatch keys (17 §3.3): expired window keys, then keys unbound for over an hour.</summary>
    static Swept PurgeKeys(SqTx tx, DateTimeOffset now, int rows)
    {
        const string Keys = "job_name, key_kind, key";
        var expired = Delete(tx, "dispatch_keys", Keys, "key_kind = 'window' AND bound_job_id IS NOT NULL AND expires_at < @cutoff", "expires_at", rows, now);
        var unbound = expired < rows
            ? Delete(tx, "dispatch_keys", Keys, "bound_job_id IS NULL AND updated_at < @cutoff", "updated_at", rows - expired, now.AddHours(-1))
            : 0;
        return Ended(expired + unbound, rows);
    }

    /// <summary>Rate grants (17 §3.4): one scope per batch, keyset by scope ID.</summary>
    static Swept PruneGrants(SqTx tx, string? cursorKey, int rows)
    {
        var after = cursorKey is null ? (Guid?)null : Guid.Parse(cursorKey);
        var scope = tx.Query(tx.Sql($"""
            SELECT rate_scope_id, window_ms, last_decision_at FROM runwick_rate_scopes
            WHERE application = @application{(after is null ? "" : " AND rate_scope_id > @after")}
            ORDER BY rate_scope_id LIMIT 1
            """).P("after", after), r => (Id: r.Uuid(0), WindowMs: r.GetInt64(1), Last: r.TimeN(2)));
        if (scope.Count == 0)
        {
            return new Swept(0, true);
        }

        var deleted = 0;
        if (scope[0].Last is { } last)
        {
            // Logical time never goes backward, so no later window (t − W, t] with t ≥ last contains a pruned grant.
            deleted = tx.Exec(tx.Sql("""
                DELETE FROM runwick_rate_grants
                WHERE application = @application AND (rate_scope_id, attempt_id) IN (
                    SELECT rate_scope_id, attempt_id FROM runwick_rate_grants
                    WHERE application = @application AND rate_scope_id = @scope AND authorized_at <= @cutoff
                    ORDER BY authorized_at LIMIT @rows)
                """).P("scope", scope[0].Id).P("cutoff", last - TimeSpan.FromMilliseconds(scope[0].WindowMs)).P("rows", rows));
        }

        return new Swept(deleted, false, null, scope[0].Id.ToString("D"));
    }

    // ------------------------------------------------------------------ policy (17 §2)

    const string PolicyColumns = """
        completed_detail_hours, failed_detail_hours, job_history_hours, audit_hours, scaling_events_hours, occurrence_hours, presence_hours
        """;

    static StoredRetentionPolicy ReadPolicy(SqTx tx) =>
        tx.One(tx.Sql($"SELECT revision, {PolicyColumns} FROM runwick_retention_policies WHERE application = @application"),
            r => new StoredRetentionPolicy(RetentionPolicy.FromHours([.. Enumerable.Range(1, 7).Select(i => r.IntN(i))]), r.GetInt64(0)))
        ?? new StoredRetentionPolicy(RetentionPolicy.Default, 0);

    static readonly string[] HoursKeys =
    [
        "completedDetailHours", "failedDetailHours", "jobHistoryHours", "auditHours", "scalingEventsHours", "scheduleOccurrencesHours", "workerPresenceHours",
    ];

    static JsonObject HoursJson(RetentionPolicy p) => new([.. p.Hours().Select((h, i) => KeyValuePair.Create(HoursKeys[i], (JsonNode?)h))]);

    static RetentionPolicy ReadHoursJson(JsonObject o) => RetentionPolicy.FromHours([.. HoursKeys.Select(k => o[k]?.GetValue<int>())]);

    public Task<StoreRead<RetentionState>> GetRetentionAsync(string application, CancellationToken ct) => runner.ReadAsync(conn =>
    {
        var tx = new SqTx(conn, null, new SqOp(application, Guid.CreateVersion7(), ManagementRead, DateTimeOffset.MaxValue, null, SqLane.Background),
            runner.Faults, ct);
        var now = Now();
        var policy = ReadPolicy(tx);
        var today = DateOnly.FromDateTime(now.UtcDateTime).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var rows = tx.Query(tx.Sql("""
            SELECT record_class, last_batch_at, caught_up_at, total_rows, summary_day, day_rows, day_batches, day_failures, last_error_code, last_error_at
            FROM runwick_cleanup_progress WHERE application = @application
            """), r =>
        {
            var current = r.StrN(4) == today;
            var caughtUp = r.TimeN(2);
            return new RetentionClassProgress(r.GetString(0), RecordClasses.IsPolicy(r.GetString(0)) ? "policy" : "fixed", r.TimeN(1),
                caughtUp, r.GetInt64(3), current ? r.GetInt64(5) : 0, current ? r.GetInt32(6) : 0, current ? r.GetInt32(7) : 0, r.StrN(8),
                r.TimeN(9), caughtUp is not { } c || c < now - TimeSpan.FromHours(24));
        }).ToDictionary(p => p.RecordClass, StringComparer.Ordinal);
        IReadOnlyList<RetentionClassProgress> classes = [.. RecordClasses.All.Select(c => rows.TryGetValue(c, out var p) ? p
            : new RetentionClassProgress(c, RecordClasses.IsPolicy(c) ? "policy" : "fixed", null, null, 0, 0, 0, 0, null, null, true))];
        return new StoreRead<RetentionState>(new RetentionState(policy, classes), now);
    }, ct);

    public async Task<StoreReply<StoredRetentionPolicy>> EditRetentionPolicyAsync(StoreCommand<RetentionPolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (RetentionActorProblem(r.Actor) is { } invalidActor)
        {
            return StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalidActor), Now());
        }

        if (r.Policy.Problem() is { } problem)
        {
            return StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_retention_policy", Detail: problem), Now());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.EditRetentionPolicy, command.CommandId, command.NotAfter,
            CommandEnvelopes.RetentionPolicy(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.EditRetentionPolicy, command.NotAfter, fp, SqLane.Background),
            tx =>
            {
                var current = ReadPolicy(tx);
                if (!tx.Decide())
                {
                    return new SqOutcome<StoredRetentionPolicy>.Rollback(tx.Reject<StoredRetentionPolicy>(StoreStatus.CommandExpired, "command_expired"));
                }

                if (current.Revision != r.ExpectedRevision)
                {
                    return new SqOutcome<StoredRetentionPolicy>.Rollback(StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.VersionConflict,
                        new StoreError("revision_conflict", ActualRevision: current.Revision, Detail: $"actual={current.Revision}"), tx.Now));
                }

                if (current.Policy.Hours().SequenceEqual(r.Policy.Hours()))
                {
                    return new SqOutcome<StoredRetentionPolicy>.Rollback(tx.Reject<StoredRetentionPolicy>(StoreStatus.InvalidState, "no_change"));
                }

                var h = r.Policy.Hours();
                var w = tx.Sql(current.Revision == 0
                    ? $"""
                      INSERT INTO runwick_retention_policies (application, revision, {PolicyColumns}, updated_at, updated_by)
                      VALUES (@application, 1, @h0, @h1, @h2, @h3, @h4, @h5, @h6, @now, @by)
                      """
                    : """
                      UPDATE runwick_retention_policies
                      SET revision = revision + 1, completed_detail_hours = @h0, failed_detail_hours = @h1, job_history_hours = @h2, audit_hours = @h3,
                          scaling_events_hours = @h4, occurrence_hours = @h5, presence_hours = @h6, updated_at = @now, updated_by = @by
                      WHERE application = @application AND revision = @expected
                      """);
                for (var i = 0; i < 7; i++)
                {
                    w.P("h" + i, h[i]);
                }

                if (tx.Exec(w.P("now", tx.Now).P("by", r.Actor.Subject).P("expected", r.ExpectedRevision)) != 1)
                {
                    throw new SqInvariantException("invalid_transition");
                }

                var value = new StoredRetentionPolicy(r.Policy, r.ExpectedRevision + 1);
                var details = new JsonObject { ["after"] = HoursJson(r.Policy), ["before"] = HoursJson(current.Policy) };
                if (tx.Exec(tx.Sql("""
                    INSERT INTO runwick_management_audit (
                        application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
                        expected_revision, resulting_revision, details, occurred_at)
                    VALUES (@application, @command_id, @operation, 'application', @application, @subject, @display, @scheme, @reason,
                            @expected, @resulting, @details, @now)
                    """).P("command_id", tx.Op.CommandId).P("operation", Operations.EditRetentionPolicy).P("subject", r.Actor.Subject)
                    .P("display", r.Actor.Display).P("scheme", r.Actor.Scheme).P("reason", r.Actor.Reason).P("expected", r.ExpectedRevision)
                    .P("resulting", value.Revision).P("details", details).P("now", tx.Now)) != 1)
                {
                    throw new SqInvariantException("invalid_transition");
                }

                return new SqOutcome<StoredRetentionPolicy>.Commit(StoreReply<StoredRetentionPolicy>.Applied(value, tx.Now),
                    new JsonObject { ["revision"] = value.Revision, ["policy"] = HoursJson(value.Policy) });
            },
            (tx, row) => StoreReply<StoredRetentionPolicy>.Applied(
                new StoredRetentionPolicy(ReadHoursJson(row.Result!["policy"]!.AsObject()), row.Result!["revision"]!.GetValue<long>()), row.DecidedAt!.Value,
                replayed: true),
            ct).ConfigureAwait(false);
    }

    /// <summary>08 §2 actor bounds (as PgStore.ValidateActor); returns the offending field or null.</summary>
    static string? RetentionActorProblem(ControlActor actor)
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
}
