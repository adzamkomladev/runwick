using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Retention (17): CleanupBatch/1, EditRetentionPolicy/1 and the retention read. A batch takes the class's progress row
/// <c>FOR UPDATE SKIP LOCKED</c> (rank 2a), then only locks that already exist, each with SKIP LOCKED, so it never waits on
/// live work; every deletion re-checks its predicate under its own lock, so a batch needs no receipt.
/// </summary>
internal sealed partial class PgStore : IRetentionStore
{
    static readonly string[] CompletedStates = ["succeeded", "cancelled"], FailedStates = ["failed"];
    readonly ConcurrentDictionary<(string, string), bool> _progressSeeded = new();

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
            await SeedProgressAsync(application, recordClass, ct).ConfigureAwait(false);
            var op = new PgOp(application, Guid.CreateVersion7(), Operations.CleanupBatch,
                PgTime.Floor(time.GetUtcNow() + limits.StatementTimeout + TimeSpan.FromSeconds(5)), null, PgLane.Background, null);
            var reply = await runner.ExecuteAsync<CleanupBatchResult>(op, tx => BatchAsync(tx, recordClass, limits), replay: null, ct)
                .ConfigureAwait(false);
            if (reply.Value is { } done)
            {
                return done;
            }

            code = reply.Error?.Code ?? reply.Status.ToString();
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && ex is StoreNotAppliedException or StoreOutcomeUnknownException or NpgsqlException)
        {
            code = (ex as PostgresException ?? ex.InnerException as PostgresException)?.SqlState switch
            {
                "55P03" => "lock_timeout",
                "57014" => "statement_timeout",
                { } state => "sqlstate_" + state[..2],
                null => ex is StoreNotAppliedException n ? n.Code : ex is StoreOutcomeUnknownException ? "outcome_unknown" : "store_unavailable",
            };
        }

        await RecordFailureAsync(application, recordClass, code, ct).ConfigureAwait(false);
        return new CleanupBatchResult("failed", 0, false, false, code);
    }

    /// <summary>The progress row is created on first use, in its own transaction (17 §4.2).</summary>
    async Task SeedProgressAsync(string application, string recordClass, CancellationToken ct)
    {
        if (_progressSeeded.ContainsKey((application, recordClass)))
        {
            return;
        }

        var conn = await sources.For(PgLane.Background).OpenConnectionAsync(ct).ConfigureAwait(false);
        await using (conn.ConfigureAwait(false))
        {
            using var cmd = new NpgsqlCommand(
                "INSERT INTO runwick.cleanup_progress (application, record_class) VALUES (@application, @class) ON CONFLICT DO NOTHING", conn)
                .P("application", application).P("class", recordClass);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        _progressSeeded[(application, recordClass)] = true;
    }

    /// <summary>A failed batch rolled back; its error is recorded in a separate small transaction, best effort.</summary>
    async Task RecordFailureAsync(string application, string recordClass, string code, CancellationToken ct)
    {
        try
        {
            var conn = await sources.For(PgLane.Background).OpenConnectionAsync(ct).ConfigureAwait(false);
            await using (conn.ConfigureAwait(false))
            {
                using var cmd = new NpgsqlCommand("""
                    UPDATE runwick.cleanup_progress
                    SET last_error_code = left(@code, 64), last_error_at = runwick.ms_floor(clock_timestamp()), day_failures = day_failures + 1
                    WHERE (application, record_class) IN (
                        SELECT application, record_class FROM runwick.cleanup_progress
                        WHERE application = @application AND record_class = @class FOR UPDATE SKIP LOCKED)
                    """, conn).P("application", application).P("class", recordClass).P("code", code);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is NpgsqlException or TimeoutException or IOException)
        {
            // Progress is observational; the next batch runs regardless.
        }
    }

    async Task<PgOutcome<CleanupBatchResult>> BatchAsync(PgTx tx, string recordClass, CleanupLimits limits)
    {
        using (var timeouts = tx.Sql("SELECT set_config('lock_timeout', '100ms', true), set_config('statement_timeout', @stmt, true)")
            .P("stmt", ((long)limits.StatementTimeout.TotalMilliseconds).ToString(System.Globalization.CultureInfo.InvariantCulture) + "ms"))
        {
            await tx.ExecAsync(timeouts, mutation: false).ConfigureAwait(false);
        }

        using var lockRow = tx.Sql("""
            SELECT cursor_at, cursor_key, summary_day, day_rows, day_batches, day_failures, pass_rows
            FROM runwick.cleanup_progress WHERE application = @application AND record_class = @class
            FOR UPDATE SKIP LOCKED
            """).P("class", recordClass);
        var progress = await tx.QueryAsync(lockRow, r => (CursorAt: PgTime.ReadNullable(r, 0), CursorKey: Str(r, 1),
            Day: r.IsDBNull(2) ? (DateOnly?)null : DateOnly.FromDateTime(r.GetDateTime(2)), Rows: r.GetInt64(3), Batches: r.GetInt32(4),
            Failures: r.GetInt32(5), PassRows: r.GetInt64(6))).ConfigureAwait(false);
        if (progress.Count == 0)
        {
            return new PgOutcome<CleanupBatchResult>.Rollback(StoreReply<CleanupBatchResult>.Applied(
                new CleanupBatchResult("skipped_locked", 0, false, false, null), PgTime.Floor(time.GetUtcNow())));
        }

        if (!await tx.DecideAsync().ConfigureAwait(false))
        {
            return Rollback<CleanupBatchResult>(tx, StoreStatus.CommandExpired, "command_expired");
        }

        var row = progress[0];
        var now = tx.Now;
        var today = DateOnly.FromDateTime(now.UtcDateTime);
        if (row.Day is { } day && day < today)
        {
            await SummaryAsync(tx, recordClass, day, row.Rows, row.Batches, row.Failures).ConfigureAwait(false);
        }

        var policy = (await ReadPolicyAsync(tx).ConfigureAwait(false)).Policy;
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
            RecordClasses.JobDetailCompleted => await ExpireDetailAsync(tx, CompletedStates, cutoff, cursor, limits).ConfigureAwait(false),
            RecordClasses.JobDetailFailed => await ExpireDetailAsync(tx, FailedStates, cutoff, cursor, limits).ConfigureAwait(false),
            RecordClasses.JobHistory => await PurgeJobsAsync(tx, cutoff, cursor, rows).ConfigureAwait(false),
            RecordClasses.JobEvents => Ended(await DeleteAsync(tx, "queue_events", "event_id", "occurred_at < @cutoff", "occurred_at, event_id", rows, cutoff)
                .ConfigureAwait(false), rows),
            RecordClasses.ManagementAudit => await PurgeAuditAsync(tx, "management_audit", "occurred_at", cutoff, rows).ConfigureAwait(false),
            RecordClasses.ScheduleAudit => await PurgeAuditAsync(tx, "schedule_audit_ranges", "created_at", cutoff, rows).ConfigureAwait(false),
            RecordClasses.ScalingEvents => Ended(await DeleteAsync(tx, "scaling_events", "event_id", "created_at < @cutoff", "created_at, event_id", rows, cutoff)
                .ConfigureAwait(false), rows),
            RecordClasses.ScheduleOccurrences => Ended(await DeleteAsync(tx, "schedule_occurrences", "schedule_id, occurrence_kind, occurrence_key",
                "materialized_at < @cutoff", "materialized_at, schedule_id, occurrence_kind, occurrence_key", rows, cutoff).ConfigureAwait(false), rows),
            RecordClasses.WorkerPresence => Ended(await DeleteAsync(tx, "worker_instances", "instance_id", "expires_at < @cutoff", "expires_at, instance_id",
                rows, cutoff).ConfigureAwait(false), rows),
            RecordClasses.ReceiptsOwnership or RecordClasses.ReceiptsDurable => Ended(await DeleteAsync(tx, "storage_commands", "retention_class, command_id",
                $"retention_class = '{(recordClass == RecordClasses.ReceiptsOwnership ? Operations.OwnershipClass : Operations.DurableClass)}' AND retain_until < @cutoff",
                "retain_until, command_id", rows, cutoff).ConfigureAwait(false), rows),
            RecordClasses.DispatchKeys => await PurgeKeysAsync(tx, now, rows).ConfigureAwait(false),
            RecordClasses.RateGrants => await PruneGrantsAsync(tx, row.CursorKey, rows).ConfigureAwait(false),
            _ => await PurgeMappingsAsync(tx, now, rows).ConfigureAwait(false),
        };

        using (var u = tx.Sql("""
            UPDATE runwick.cleanup_progress
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
                .P("deleted", swept.Deleted).P("now", now).P("today", today.ToDateTime(TimeOnly.MinValue), NpgsqlDbType.Date))
        {
            Expect(await tx.ExecAsync(u, mutation: false).ConfigureAwait(false), 1);
        }

        var result = new CleanupBatchResult("done", swept.Deleted, swept.Complete, swept.Complete && row.PassRows + swept.Deleted == 0, null);
        return new PgOutcome<CleanupBatchResult>.Commit(StoreReply<CleanupBatchResult>.Applied(result, now), null);
    }

    static Swept Ended(int deleted, int rows) => new(deleted, deleted < rows);

    /// <summary>One RetentionSummary audit row for a closed UTC day (17 §4.2); its command ID is derived, so it is the same on every process.</summary>
    static async Task SummaryAsync(PgTx tx, string recordClass, DateOnly day, long rows, int batches, int failures)
    {
        var dayText = day.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        var commandId = new Guid(SHA256.HashData(Encoding.UTF8.GetBytes(tx.Application + "\0" + recordClass + "\0" + dayText)).AsSpan(0, 16), bigEndian: true);
        using var a = tx.Sql("""
            INSERT INTO runwick.management_audit (
                application, command_id, operation, target_kind, target_id, actor_subject, actor_scheme, details, occurred_at)
            VALUES (@application, @command_id, 'RetentionSummary', 'application', @class, 'runwick:retention', 'system', @details, @now)
            """).P("command_id", commandId).P("class", recordClass).P("now", tx.Now)
            .Json("details", new JsonObject { ["day"] = dayText, ["rows"] = rows, ["batches"] = batches, ["failures"] = failures });
        Expect(await tx.ExecAsync(a).ConfigureAwait(false), 1);
    }

    /// <summary>Detail expiry (17 §3.1): payload and error text go; identity, state, history and error codes stay.</summary>
    static async Task<Swept> ExpireDetailAsync(PgTx tx, string[] states, DateTimeOffset cutoff, (DateTimeOffset At, Guid Id)? cursor, CleanupLimits limits)
    {
        using var c = tx.Sql($"""
            SELECT j.job_id, j.terminal_at,
                   COALESCE(octet_length(p.payload_utf8), 0)
                     + COALESCE((SELECT sum(octet_length(a.error::text)) FROM runwick.job_attempts AS a
                                 WHERE a.application = j.application AND a.job_id = j.job_id AND a.error IS NOT NULL), 0)
            FROM runwick.jobs AS j
            LEFT JOIN runwick.job_payloads AS p ON p.application = j.application AND p.job_id = j.job_id
            WHERE j.application = @application AND j.state = ANY(@states) AND j.detail_expired_at IS NULL AND j.terminal_at < @cutoff
              {(cursor is null ? "" : "AND (j.terminal_at, j.job_id) > (@cursor_at, @cursor_id)")}
            ORDER BY j.terminal_at, j.job_id
            LIMIT @rows
            FOR NO KEY UPDATE OF j SKIP LOCKED
            """).Arr("states", states, NpgsqlDbType.Text).P("cutoff", cutoff).P("cursor_at", cursor?.At).P("cursor_id", cursor?.Id, NpgsqlDbType.Uuid)
            .P("rows", limits.Rows);
        var candidates = await tx.QueryAsync(c, r => (Id: r.GetGuid(0), At: PgTime.Read(r, 1), Bytes: r.GetInt64(2))).ConfigureAwait(false);

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

        var ids = candidates.Take(kept).Select(x => x.Id).ToArray();
        string[] statements =
        [
            "DELETE FROM runwick.job_payloads WHERE application = @application AND job_id = ANY(@ids)",
            """
            UPDATE runwick.job_attempts SET error = jsonb_build_object('code', error -> 'code', 'expired', true)
            WHERE application = @application AND job_id = ANY(@ids) AND error IS NOT NULL
            """,
            "UPDATE runwick.jobs SET payload_retained = false, detail_expired_at = runwick.ms_floor(@now) WHERE application = @application AND job_id = ANY(@ids)",
        ];
        foreach (var sql in statements)
        {
            using var s = tx.Sql(sql).Arr("ids", ids, NpgsqlDbType.Uuid).P("now", tx.Now);
            await tx.ExecAsync(s).ConfigureAwait(false);
        }

        var last = candidates[kept - 1];
        return kept == candidates.Count && candidates.Count < limits.Rows ? new Swept(kept, true) : new Swept(kept, false, last.At, last.Id.ToString("D"));
    }

    /// <summary>Job history (17 §3.2): the job row and its cascade, only after detail expiry and with no live binding or permit.</summary>
    static async Task<Swept> PurgeJobsAsync(PgTx tx, DateTimeOffset cutoff, (DateTimeOffset At, Guid Id)? cursor, int rows)
    {
        using var c = tx.Sql($"""
            SELECT j.job_id, j.terminal_at
            FROM runwick.jobs AS j
            WHERE j.application = @application
              AND j.state IN ('succeeded', 'failed', 'cancelled')
              AND j.terminal_at < @cutoff
              AND j.detail_expired_at IS NOT NULL
              {(cursor is null ? "" : "AND (j.terminal_at, j.job_id) > (@cursor_at, @cursor_id)")}
              AND NOT EXISTS (SELECT 1 FROM runwick.dispatch_keys AS d
                              WHERE d.application = j.application AND d.bound_job_id = j.job_id
                                AND (d.key_kind = 'active' OR d.expires_at > @now))
              AND NOT EXISTS (SELECT 1 FROM runwick.concurrency_permits AS pm
                              WHERE pm.application = j.application AND pm.job_id = j.job_id)
            ORDER BY j.terminal_at, j.job_id
            LIMIT @rows
            FOR UPDATE OF j SKIP LOCKED
            """).P("cutoff", cutoff).P("now", tx.Now).P("cursor_at", cursor?.At).P("cursor_id", cursor?.Id, NpgsqlDbType.Uuid).P("rows", rows);
        var candidates = await tx.QueryAsync(c, r => (Id: r.GetGuid(0), At: PgTime.Read(r, 1))).ConfigureAwait(false);
        if (candidates.Count == 0)
        {
            return new Swept(0, true);
        }

        using (var d = tx.Sql("DELETE FROM runwick.jobs WHERE application = @application AND job_id = ANY(@ids)")
            .Arr("ids", [.. candidates.Select(x => x.Id)], NpgsqlDbType.Uuid))
        {
            Expect(await tx.ExecAsync(d).ConfigureAwait(false), candidates.Count);
        }

        var last = candidates[^1];
        return candidates.Count < rows ? new Swept(candidates.Count, true) : new Swept(candidates.Count, false, last.At, last.Id.ToString("D"));
    }

    /// <summary>
    /// Age-ordered delete of at most <paramref name="rows"/> unlocked rows (17 §3.6, §3.7). Every row the scan returns is
    /// deleted, so the scan restarts at the oldest row and a row locked by live work is met again on a later batch.
    /// </summary>
    static async Task<int> DeleteAsync(PgTx tx, string table, string keys, string predicate, string order, int rows, DateTimeOffset cutoff)
    {
        var join = string.Join(" AND ", keys.Split(", ").Select(k => $"t.{k} = c.{k}"));
        using var d = tx.Sql($"""
            DELETE FROM runwick.{table} AS t
            USING (SELECT {keys} FROM runwick.{table}
                   WHERE application = @application AND {predicate}
                   ORDER BY {order} LIMIT @rows FOR UPDATE SKIP LOCKED) AS c
            WHERE t.application = @application AND {join}
            """).P("cutoff", cutoff).P("rows", rows);
        return await tx.ExecAsync(d).ConfigureAwait(false);
    }

    /// <summary>Audit (17 §3.6): identity order, deleting while older than the horizon and stopping at the first row that is not.</summary>
    static async Task<Swept> PurgeAuditAsync(PgTx tx, string table, string column, DateTimeOffset cutoff, int rows)
    {
        using var c = tx.Sql($"SELECT audit_id, {column} < @cutoff FROM runwick.{table} WHERE application = @application ORDER BY audit_id LIMIT @rows")
            .P("cutoff", cutoff).P("rows", rows);
        var scanned = await tx.QueryAsync(c, r => (Id: r.GetInt64(0), Old: r.GetBoolean(1))).ConfigureAwait(false);
        var ids = scanned.TakeWhile(x => x.Old).Select(x => x.Id).ToArray();
        if (ids.Length > 0)
        {
            using var d = tx.Sql($"DELETE FROM runwick.{table} WHERE application = @application AND audit_id = ANY(@ids)").Arr("ids", ids, NpgsqlDbType.Bigint);
            await tx.ExecAsync(d).ConfigureAwait(false);
        }

        return Ended(ids.Length, rows);
    }

    /// <summary>Dispatch keys (17 §3.3): expired window keys, then keys unbound for over an hour; both locked SKIP LOCKED.</summary>
    static async Task<Swept> PurgeKeysAsync(PgTx tx, DateTimeOffset now, int rows)
    {
        const string Keys = "job_name, key_kind, key";
        var expired = await DeleteAsync(tx, "dispatch_keys", Keys, "key_kind = 'window' AND bound_job_id IS NOT NULL AND expires_at < @cutoff",
            "expires_at", rows, now).ConfigureAwait(false);
        var unbound = expired < rows
            ? await DeleteAsync(tx, "dispatch_keys", Keys, "bound_job_id IS NULL AND updated_at < @cutoff", "updated_at", rows - expired, now.AddHours(-1))
                .ConfigureAwait(false)
            : 0;
        return Ended(expired + unbound, rows);
    }

    /// <summary>
    /// Rate grants (17 §3.4): one scope per batch under its lock, keyset by scope ID. A scope another transaction holds is
    /// skipped (SKIP LOCKED) and met again on the next pass.
    /// </summary>
    static async Task<Swept> PruneGrantsAsync(PgTx tx, string? cursorKey, int rows)
    {
        var after = cursorKey is null ? (Guid?)null : Guid.Parse(cursorKey);
        using var s = tx.Sql($"""
            SELECT rate_scope_id, window_ms, last_decision_at FROM runwick.rate_scopes
            WHERE application = @application{(after is null ? "" : " AND rate_scope_id > @after")}
            ORDER BY rate_scope_id LIMIT 1
            FOR UPDATE SKIP LOCKED
            """).P("after", after, NpgsqlDbType.Uuid);
        var scope = await tx.QueryAsync(s, r => (Id: r.GetGuid(0), WindowMs: r.GetInt64(1), Last: PgTime.ReadNullable(r, 2))).ConfigureAwait(false);
        if (scope.Count == 0)
        {
            return new Swept(0, true);
        }

        var deleted = 0;
        if (scope[0].Last is { } last)
        {
            // Logical time never goes backward, so no later window (t − W, t] with t ≥ last contains a pruned grant.
            using var d = tx.Sql("""
                DELETE FROM runwick.rate_grants
                WHERE (application, rate_scope_id, attempt_id) IN (
                    SELECT application, rate_scope_id, attempt_id FROM runwick.rate_grants
                    WHERE application = @application AND rate_scope_id = @scope AND authorized_at <= @cutoff
                    ORDER BY authorized_at LIMIT @rows)
                """).P("scope", scope[0].Id).P("cutoff", last - TimeSpan.FromMilliseconds(scope[0].WindowMs)).P("rows", rows);
            deleted = await tx.ExecAsync(d).ConfigureAwait(false);
        }

        return new Swept(deleted, false, null, scope[0].Id.ToString("D"));
    }

    /// <summary>
    /// Outbox mappings (17 §3.5): past retain_until, each under its delivery-identity lock (rank 3a). The lock is tried, not
    /// waited for: a mapping a transport command holds is skipped and met again later.
    /// </summary>
    static async Task<Swept> PurgeMappingsAsync(PgTx tx, DateTimeOffset now, int rows)
    {
        using var d = tx.Sql("""
            DELETE FROM runwick.outbox_deliveries AS t
            USING (SELECT source_store_id, message_id FROM runwick.outbox_deliveries
                   WHERE application = @application AND retain_until < @now
                   ORDER BY retain_until, source_store_id, message_id LIMIT @rows) AS c
            WHERE t.application = @application AND t.source_store_id = c.source_store_id AND t.message_id = c.message_id
              AND t.retain_until < @now
              AND pg_try_advisory_xact_lock(hashtextextended(
                  'runwick.outbox/' || @application || '/' || c.source_store_id::text || '/' || c.message_id::text, 0))
            """).P("now", now).P("rows", rows);
        return Ended(await tx.ExecAsync(d).ConfigureAwait(false), rows);
    }

    // ------------------------------------------------------------------ policy (17 §2)

    const string PolicyColumns = """
        completed_detail_hours, failed_detail_hours, job_history_hours, audit_hours, scaling_events_hours, occurrence_hours, presence_hours
        """;

    static int?[] ReadHours(NpgsqlDataReader r, int from) =>
        [.. Enumerable.Range(from, 7).Select(i => r.IsDBNull(i) ? (int?)null : r.GetInt32(i))];

    static async Task<StoredRetentionPolicy> ReadPolicyAsync(PgTx tx, bool forUpdate = false)
    {
        using var q = tx.Sql($"SELECT revision, {PolicyColumns} FROM runwick.retention_policies WHERE application = @application{(forUpdate ? " FOR UPDATE" : "")}");
        var rows = await tx.QueryAsync(q, r => new StoredRetentionPolicy(RetentionPolicy.FromHours(ReadHours(r, 1)), r.GetInt64(0))).ConfigureAwait(false);
        return rows.Count == 0 ? new StoredRetentionPolicy(RetentionPolicy.Default, 0) : rows[0];
    }


    static readonly string[] HoursKeys =
    [
        "completedDetailHours", "failedDetailHours", "jobHistoryHours", "auditHours", "scalingEventsHours", "scheduleOccurrencesHours", "workerPresenceHours",
    ];

    static JsonObject HoursJson(RetentionPolicy p) => new([.. p.Hours().Select((h, i) => KeyValuePair.Create(HoursKeys[i], (JsonNode?)h))]);

    static RetentionPolicy ReadHoursJson(JsonObject o) => RetentionPolicy.FromHours([.. HoursKeys.Select(k => o[k]?.GetValue<int>())]);

    public async Task<StoreRead<RetentionState>> GetRetentionAsync(string application, CancellationToken ct) =>
        await ReadAsync(application, async tx =>
        {
            var policy = await ReadPolicyAsync(tx).ConfigureAwait(false);
            var today = DateOnly.FromDateTime(tx.Now.UtcDateTime);
            using var q = tx.Sql("""
                SELECT record_class, last_batch_at, caught_up_at, total_rows, summary_day, day_rows, day_batches, day_failures, last_error_code, last_error_at
                FROM runwick.cleanup_progress WHERE application = @application
                """);
            var rows = (await tx.QueryAsync(q, r =>
            {
                var current = !r.IsDBNull(4) && DateOnly.FromDateTime(r.GetDateTime(4)) == today;
                var caughtUp = PgTime.ReadNullable(r, 2);
                return new RetentionClassProgress(r.GetString(0), RecordClasses.IsPolicy(r.GetString(0)) ? "policy" : "fixed", PgTime.ReadNullable(r, 1),
                    caughtUp, r.GetInt64(3), current ? r.GetInt64(5) : 0, current ? r.GetInt32(6) : 0, current ? r.GetInt32(7) : 0, Str(r, 8),
                    PgTime.ReadNullable(r, 9), caughtUp is not { } c || c < tx.Now - TimeSpan.FromHours(24));
            }).ConfigureAwait(false)).ToDictionary(p => p.RecordClass, StringComparer.Ordinal);
            IReadOnlyList<RetentionClassProgress> classes = [.. RecordClasses.All.Select(c => rows.TryGetValue(c, out var p) ? p
                : new RetentionClassProgress(c, RecordClasses.IsPolicy(c) ? "policy" : "fixed", null, null, 0, 0, 0, 0, null, null, true))];
            return new RetentionState(policy, classes);
        }, ct).ConfigureAwait(false);

    public async Task<StoreReply<StoredRetentionPolicy>> EditRetentionPolicyAsync(StoreCommand<RetentionPolicyRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalidActor)
        {
            return StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_request", Detail: invalidActor), time.GetUtcNow());
        }

        if (r.Policy.Problem() is { } problem)
        {
            return StoreReply<StoredRetentionPolicy>.NoEffect(StoreStatus.InvalidRequest, new StoreError("invalid_retention_policy", Detail: problem), time.GetUtcNow());
        }

        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, Operations.EditRetentionPolicy, command.CommandId, command.NotAfter,
            CommandEnvelopes.RetentionPolicy(r));
        return await runner.ExecuteAsync(Op(command.Application, command.CommandId, Operations.EditRetentionPolicy, command.NotAfter, fp, PgLane.Background),
            async tx =>
            {
                var current = await ReadPolicyAsync(tx, forUpdate: true).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<StoredRetentionPolicy>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (current.Revision != r.ExpectedRevision)
                {
                    return Conflict(current.Revision);
                }

                if (current.Policy.Hours().SequenceEqual(r.Policy.Hours()))
                {
                    return Rollback<StoredRetentionPolicy>(tx, StoreStatus.InvalidState, "no_change");
                }

                var h = r.Policy.Hours();
                using (var w = tx.Sql(current.Revision == 0
                    ? $"""
                      INSERT INTO runwick.retention_policies (application, revision, {PolicyColumns}, updated_at, updated_by)
                      VALUES (@application, 1, @h0, @h1, @h2, @h3, @h4, @h5, @h6, @now, @by)
                      ON CONFLICT DO NOTHING
                      """
                    : """
                      UPDATE runwick.retention_policies
                      SET revision = revision + 1, completed_detail_hours = @h0, failed_detail_hours = @h1, job_history_hours = @h2, audit_hours = @h3,
                          scaling_events_hours = @h4, occurrence_hours = @h5, presence_hours = @h6, updated_at = @now, updated_by = @by
                      WHERE application = @application AND revision = @expected
                      """))
                {
                    for (var i = 0; i < 7; i++)
                    {
                        w.P("h" + i, h[i], NpgsqlDbType.Integer);
                    }

                    w.P("now", tx.Now).P("by", r.Actor.Subject).P("expected", r.ExpectedRevision);
                    if (await tx.ExecAsync(w).ConfigureAwait(false) != 1)
                    {
                        // A concurrent first edit inserted the row after our read: report its revision.
                        return Conflict((await ReadPolicyAsync(tx).ConfigureAwait(false)).Revision);
                    }
                }

                var value = new StoredRetentionPolicy(r.Policy, r.ExpectedRevision + 1);
                await AuditAsync(tx, "application", tx.Application, r.Actor, r.ExpectedRevision, value.Revision,
                    new JsonObject { ["before"] = HoursJson(current.Policy), ["after"] = HoursJson(r.Policy) }).ConfigureAwait(false);
                return new PgOutcome<StoredRetentionPolicy>.Commit(StoreReply<StoredRetentionPolicy>.Applied(value, tx.Now),
                    new JsonObject { ["revision"] = value.Revision, ["policy"] = HoursJson(value.Policy) });

                PgOutcome<StoredRetentionPolicy> Conflict(long actual) => new PgOutcome<StoredRetentionPolicy>.Rollback(StoreReply<StoredRetentionPolicy>.NoEffect(
                    StoreStatus.VersionConflict, new StoreError("revision_conflict", ActualRevision: actual, Detail: $"actual={actual}"), tx.Now));
            },
            (tx, row) => Task.FromResult(StoreReply<StoredRetentionPolicy>.Applied(
                new StoredRetentionPolicy(ReadHoursJson(row.Result!["policy"]!.AsObject()), row.Result!["revision"]!.GetValue<long>()), row.DecidedAt!.Value,
                replayed: true)),
            ct).ConfigureAwait(false);
    }
}
