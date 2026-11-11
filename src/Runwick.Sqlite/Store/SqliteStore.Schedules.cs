using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Runwick.Protocol;

namespace Runwick.Sqlite.Store;

/// <summary>
/// Schedules (13 §6, 20 §7): PgStore.Schedules step for step on the write lock. The store never runs the evaluator; every
/// cursor move is the caller's plan, checked against the row's revisions, the high-water mark and the decision time.
/// </summary>
internal sealed partial class SqliteStore : IScheduleStore
{
    /// <summary>The actor of system schedule commands (13 C-6).</summary>
    static readonly ControlActor SchedulerActor = new("svc:scheduler", "Runwick scheduler", "runwick", null);

    const string ScheduleColumns = """
        s.schedule_id, s.schedule_key, s.definition_revision, s.control_revision, s.cursor_revision, s.enabled, s.paused,
        s.needs_review, s.review_reason, s.next_due_at, s.scan_after_at, s.evaluated_through, s.episode_id, s.episode_cutoff,
        s.episode_emitted, s.episode_limit, s.resume_policy, s.resume_cutoff,
        d.kind, d.due_at, d.cron_text, d.cron_dialect, d.zone_id, d.evaluator_profile, d.tzdb_version, d.tzdb_sha256,
        d.gap_policy, d.fold_policy, d.misfire_policy, d.misfire_grace_ms, d.max_batch, d.max_episode_jobs,
        d.max_catchup_age_ms, d.overlap_policy, d.allow_reuse, d.job_name, d.payload_version, d.serializer_profile_id,
        d.serializer_profile_version, d.payload_template, d.template, d.definition_fingerprint
        """;

    const string ScheduleJoin = """
        runwick_schedules AS s
        JOIN runwick_schedule_definitions AS d
          ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
        """;

    static ScheduleSnapshot ReadSnapshot(SqliteDataReader r)
    {
        ScheduleEpisode? episode = r.IsDBNull(12) ? null : new ScheduleEpisode(r.Uuid(12), r.Time(13), r.GetInt32(14), r.GetInt32(15));
        var definition = new ScheduleDefinitionRecord(
            r.GetString(18), r.TimeN(19), r.StrN(20), r.StrN(21), r.StrN(22), r.StrN(23), r.StrN(24), r.IsDBNull(25) ? null : r.Blob(25),
            r.GetString(26), r.GetString(27), r.GetString(28), r.GetInt64(29), r.GetInt32(30), r.GetInt32(31), r.GetInt64(32), r.GetString(33),
            r.Bool(34),
            ReadTemplate(r.GetString(35), r.GetInt32(36), r.GetString(37), r.GetInt32(38), r.Blob(39), JsonNode.Parse(r.GetString(40))!.AsObject()),
            r.Blob(41));
        return new ScheduleSnapshot(
            r.Uuid(0), r.GetString(1), r.GetInt64(2), r.GetInt64(3), r.GetInt64(4), r.Bool(5), r.Bool(6), r.Bool(7), r.StrN(8), r.TimeN(9),
            r.Time(10), r.TimeN(11), episode, r.StrN(16), r.TimeN(17), definition);
    }

    // ------------------------------------------------------------------ template (13 §3.2)

    static JsonObject TemplateJson(ScheduleTemplate t)
    {
        var metadata = new JsonObject();
        foreach (var key in t.BusinessMetadata.Keys.Order(Internal.Utf8Order.Instance))
        {
            metadata[key] = t.BusinessMetadata[key];
        }

        return new JsonObject
        {
            ["activeKey"] = t.ActiveKey,
            ["businessMetadata"] = metadata,
            ["policy"] = ReceiptCodec.Policy(t.Policy),
            ["priority"] = t.Priority,
            ["queue"] = t.Queue,
            ["rates"] = new JsonArray([.. t.Rates.Select(r => (JsonNode)new JsonObject
            {
                ["kind"] = r.Kind, ["key"] = r.Key, ["limit"] = r.Limit, ["windowMs"] = r.WindowMs,
            })]),
            ["scopes"] = new JsonArray([.. t.Scopes.Select(s => (JsonNode)new JsonObject
            {
                ["kind"] = s.Kind, ["key"] = s.Key, ["capacity"] = s.Capacity,
            })]),
        };
    }

    static ScheduleTemplate ReadTemplate(string jobName, int payloadVersion, string profileId, int profileVersion, byte[] payload, JsonObject o) =>
        new(jobName, payloadVersion, profileId, profileVersion, payload,
            o["queue"]!.GetValue<string>(), o["priority"]!.GetValue<string>(), ReceiptCodec.ReadPolicy(o["policy"]!.AsObject()),
            [.. o["scopes"]!.AsArray().Select(s => new ScopeRequirement(s!["kind"]!.GetValue<string>(), s["key"]!.GetValue<string>(), s["capacity"]!.GetValue<int>()))],
            [.. o["rates"]!.AsArray().Select(r => new RateRequirement(r!["kind"]!.GetValue<string>(), r["key"]!.GetValue<string>(),
                r["limit"]!.GetValue<int>(), r["windowMs"]!.GetValue<long>()))],
            o["activeKey"]?.GetValue<string>(),
            o["businessMetadata"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal));

    /// <summary>Store-side definition bounds (13 §3, §10); returns the offending field or null.</summary>
    static string? ValidateScheduleDefinition(ScheduleDefinitionRecord d)
    {
        var cron = d.Kind == ScheduleOperations.Cron;
        if (!cron && d.Kind != ScheduleOperations.OneTime)
        {
            return "kind";
        }

        if (cron ? d.CronText is null || d.CronDialect is null || d.EvaluatorProfile is null || d.TzdbVersion is null || d.TzdbSha256 is not { Length: 32 } || d.DueAt is not null
                 : d.DueAt is null || d.CronText is not null || d.TzdbSha256 is not null)
        {
            return "recurrence";
        }

        return d.GapPolicy is not ("skip" or "shift") ? "gapPolicy"
            : d.FoldPolicy is not ("earlier" or "later" or "both") ? "foldPolicy"
            : d.MisfirePolicy is not (ScheduleOperations.MisfireSkip or ScheduleOperations.MisfireRunOnce or ScheduleOperations.MisfireCatchUp) ? "misfirePolicy"
            : d.MisfireGraceMs is < 0 or > 86_400_000 ? "misfireGraceMs"
            : d.MaxBatch is < 1 or > ScheduleBounds.MaxBatch ? "maxBatch"
            : d.MaxEpisodeJobs is < 1 or > ScheduleBounds.MaxEpisodeJobs ? "maxEpisodeJobs"
            : d.MaxCatchUpAgeMs is < 60_000 or > 2_678_400_000 ? "maxCatchUpAgeMs"
            : d.OverlapPolicy is not (ScheduleOperations.OverlapAllow or ScheduleOperations.OverlapSerialize) ? "overlapPolicy"
            : d.Fingerprint is not { Length: 32 } ? "fingerprint"
            : d.Template.ActiveKey is not null && !d.AllowReuse ? "allowReuse"
            : d.Template.PayloadUtf8.Length > ScheduleBounds.MaxPlanPayloadBytes ? "template"
            : null;
    }

    /// <summary>08 §2 actor bounds (PgStore.ValidateActor); returns the offending field or null.</summary>
    static string? ScheduleActorInvalid(ControlActor actor)
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

    /// <summary>A refusal decided at the client's clock (PgStore.Invalid).</summary>
    StoreReply<T> ScheduleInvalid<T>(StoreStatus status, string code, string? detail = null) where T : class =>
        StoreReply<T>.NoEffect(status, new StoreError(code, Detail: detail), Now());

    static SqOutcome<T> ScheduleRollback<T>(SqTx tx, StoreStatus status, string code) where T : class =>
        new SqOutcome<T>.Rollback(tx.Reject<T>(status, code));

    static SqOutcome<T> ScheduleConflict<T>(SqTx tx, long actual) where T : class =>
        new SqOutcome<T>.Rollback(StoreReply<T>.NoEffect(StoreStatus.VersionConflict,
            new StoreError("revision_conflict", Detail: "actual=" + actual.ToString(CultureInfo.InvariantCulture)), tx.Now));

    // ------------------------------------------------------------------ reads (13 §6.9)

    public Task<DueSchedules> ReadDueSchedulesAsync(string application, int limit, CancellationToken ct, DueSchedulePageCursor? after = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, ScheduleBounds.DueReadLimit);
        return runner.ReadAsync(conn =>
        {
            var observed = Now();
            if (!CheckGate(conn))
            {
                return new DueSchedules(observed, []);
            }

            var continuation = after is null ? "" : "AND (s.scan_after_at, s.schedule_id) > (@scan, @id)";
            var through = after is { } cursor && cursor.ScanThrough < observed ? cursor.ScanThrough : observed;
            var cmd = Cmd(conn, application, $"""
                SELECT {ScheduleColumns}
                FROM {ScheduleJoin}
                WHERE s.application = @application
                  AND s.enabled = 1 AND s.paused = 0 AND s.needs_review = 0 AND s.next_due_at IS NOT NULL
                  AND s.scan_after_at <= @through
                  {continuation}
                ORDER BY s.scan_after_at, s.schedule_id
                LIMIT @limit
                """).P("through", through).P("limit", limit);
            if (after is { } position)
            {
                cmd.P("scan", position.ScanAfterAt).P("id", position.ScheduleId);
            }

            var schedules = Rows(cmd, ReadSnapshot);
            if (after is not null || schedules.Count != 0)
            {
                return new DueSchedules(observed, schedules);
            }

            using var nextScan = Cmd(conn, application, """
                SELECT s.scan_after_at
                FROM runwick_schedules AS s
                WHERE s.application = @application
                  AND s.enabled = 1 AND s.paused = 0 AND s.needs_review = 0 AND s.next_due_at IS NOT NULL
                  AND s.scan_after_at > @observed
                ORDER BY s.scan_after_at, s.schedule_id
                LIMIT 1
                """).P("observed", observed);
            var nextScanAfterAt = nextScan.ExecuteScalar() is long next ? SqTime.FromMs(next) : (DateTimeOffset?)null;
            return new DueSchedules(observed, schedules, nextScanAfterAt);
        }, ct);
    }

    public Task<ScheduleSnapshot?> ReadScheduleAsync(string application, string key, CancellationToken ct) =>
        runner.ReadAsync(conn => Rows(Cmd(conn, application, $"""
            SELECT {ScheduleColumns} FROM {ScheduleJoin} WHERE s.application = @application AND s.schedule_key = @key
            """).P("key", key), ReadSnapshot).FirstOrDefault(), ct);

    /// <summary>Store time is the host clock (20 §4.1 step 3), ms floor.</summary>
    public Task<DateTimeOffset> StoreNowAsync(CancellationToken ct) => Task.FromResult(Now());

    // ------------------------------------------------------------------ helpers under the transaction

    static bool SchedulerActive(SqTx tx) =>
        tx.One(tx.Sql("""
            SELECT json_type(required_capabilities, '$.scheduler') IS NOT NULL FROM runwick_application_controls WHERE application = @application
            """), r => r.Bool(0));

    /// <summary>The schedule row with its current definition; the write lock already makes it exclusive.</summary>
    static ScheduleSnapshot? LockSchedule(SqTx tx, Guid? scheduleId, string? key)
    {
        var q = tx.Sql($"""
            SELECT {ScheduleColumns} FROM {ScheduleJoin}
            WHERE s.application = @application AND {(scheduleId is null ? "s.schedule_key = @key" : "s.schedule_id = @id")}
            """);
        q = scheduleId is { } id ? q.P("id", id) : q.P("key", key);
        return tx.One(q, ReadSnapshot);
    }

    static void InsertDefinition(SqTx tx, Guid scheduleId, long revision, ScheduleDefinitionRecord d, DateTimeOffset cutoff, string? createdBy)
    {
        var t = d.Template;
        Expect(tx.Exec(tx.Sql("""
            INSERT INTO runwick_schedule_definitions (
                application, schedule_id, revision, kind, due_at, cron_text, cron_dialect, zone_id, evaluator_profile, tzdb_version,
                tzdb_sha256, gap_policy, fold_policy, misfire_policy, misfire_grace_ms, max_batch, max_episode_jobs, max_catchup_age_ms,
                overlap_policy, allow_reuse, job_name, payload_version, serializer_profile_id, serializer_profile_version,
                payload_template, template, definition_fingerprint, cutoff_at, created_at, created_by)
            VALUES (@application, @id, @revision, @kind, @due_at, @cron_text, @cron_dialect, @zone_id, @profile, @tzdb_version,
                @tzdb_sha256, @gap, @fold, @misfire, @grace, @max_batch, @max_episode, @max_age, @overlap, @reuse, @job_name,
                @payload_version, @profile_id, @profile_version, @payload, @template, @fingerprint, @cutoff, @now, @created_by)
            """).P("id", scheduleId).P("revision", revision).P("kind", d.Kind).P("due_at", d.DueAt).P("cron_text", d.CronText)
            .P("cron_dialect", d.CronDialect).P("zone_id", d.ZoneId).P("profile", d.EvaluatorProfile).P("tzdb_version", d.TzdbVersion)
            .P("tzdb_sha256", d.TzdbSha256).P("gap", d.GapPolicy).P("fold", d.FoldPolicy).P("misfire", d.MisfirePolicy)
            .P("grace", d.MisfireGraceMs).P("max_batch", d.MaxBatch).P("max_episode", d.MaxEpisodeJobs).P("max_age", d.MaxCatchUpAgeMs)
            .P("overlap", d.OverlapPolicy).P("reuse", d.AllowReuse).P("job_name", t.JobName).P("payload_version", t.PayloadVersion)
            .P("profile_id", t.SerializerProfileId).P("profile_version", t.SerializerProfileVersion).P("payload", t.PayloadUtf8)
            .P("template", TemplateJson(t)).P("fingerprint", d.Fingerprint).P("cutoff", cutoff).P("now", tx.Now).P("created_by", createdBy)), 1);
    }

    static void Expect(int actual, int expected)
    {
        if (actual != expected)
        {
            throw new SqInvariantException("invalid_transition");
        }
    }

    static void AuditRanges(SqTx tx, Guid scheduleId, long definitionRevision, IReadOnlyList<ScheduleAuditRange> ranges)
    {
        foreach (var r in ranges)
        {
            Expect(tx.Exec(tx.Sql("""
                INSERT INTO runwick_schedule_audit_ranges (
                    application, schedule_id, definition_revision, command_id, from_exclusive, through_inclusive, reason, created_at)
                VALUES (@application, @id, @revision, @command_id, @from, @through, @reason, @now)
                """).P("id", scheduleId).P("revision", definitionRevision).P("command_id", tx.Op.CommandId).P("from", r.FromExclusive)
                .P("through", r.ThroughInclusive).P("reason", r.Reason).P("now", tx.Now)), 1);
        }
    }

    /// <summary>Inserts occurrence rows in key order; false when any already exists (an invariant failure).</summary>
    static bool InsertOccurrences(SqTx tx, Guid scheduleId, long definitionRevision,
        IEnumerable<(string Kind, string Key, DateTimeOffset? ScheduledAt, Guid JobId, string Disposition, bool Coalesced, DateTimeOffset? From, DateTimeOffset? Through)> rows)
    {
        var all = true;
        foreach (var o in rows.OrderBy(r => r.Kind, StringComparer.Ordinal).ThenBy(r => r.Key, StringComparer.Ordinal))
        {
            all &= tx.Exec(tx.Sql("""
                INSERT INTO runwick_schedule_occurrences (
                    application, schedule_id, occurrence_kind, occurrence_key, scheduled_at, definition_revision, job_id, disposition,
                    coalesced, covered_from, covered_through, materialized_at, command_id)
                VALUES (@application, @id, @kind, @key, @at, @revision, @job, @disposition, @coalesced, @from, @through, @now, @command_id)
                ON CONFLICT DO NOTHING
                """).P("id", scheduleId).P("kind", o.Kind).P("key", o.Key).P("at", o.ScheduledAt).P("revision", definitionRevision)
                .P("job", o.JobId).P("disposition", o.Disposition).P("coalesced", o.Coalesced).P("from", o.From).P("through", o.Through)
                .P("now", tx.Now).P("command_id", tx.Op.CommandId)) == 1;
        }

        return all;
    }

    /// <summary>Creates the jobs of one command through the shared dispatch core (created reason <c>scheduled</c>, 13 C-10).</summary>
    (List<(Guid JobId, string Disposition)> Jobs, StoreReply<T>? Rejection) DispatchScheduled<T>(
        SqTx tx, ScheduleSnapshot s, IReadOnlyList<(Guid JobId, JobScheduleInfo Occurrence)> proposed) where T : class
    {
        if (proposed.Count == 0)
        {
            return tx.Decide() ? ([], null) : ([], tx.Reject<T>(StoreStatus.CommandExpired, "command_expired"));
        }

        var notAfterMs = SqTime.Ms(tx.Op.NotAfter);
        var items = proposed
            .Select(p => s.Definition.Template.ToItem(StoreId, tx.Application, tx.Op.CommandId, notAfterMs, s.ScheduleId, s.Definition.Serialize, p.JobId,
                p.Occurrence))
            .ToList();
        var core = DispatchCore(tx, items, continueOnConflict: false, createdReason: "scheduled");
        if (core.Reject is { } reject)
        {
            return ([], StoreReply<T>.NoEffect(reject.StoreStatus,
                new StoreError(reject.Code ?? "invariant_fault", ConflictingJobId: reject.ConflictingJobId, Detail: reject.Detail), tx.Now));
        }

        return ([.. core.Items!.Select(i => (i!["jobId"]!.GetValue<Guid>(),
            i["disposition"]!.GetValue<string>() == "Created" ? "created" : "linked_existing"))], null);
    }

    /// <summary>What <c>IJobContext.Schedule</c> reports for a job of this occurrence (PgStore.Occurrence).</summary>
    static JobScheduleInfo Occurrence(ScheduleSnapshot s, DateTimeOffset at, DateTimeOffset? from, DateTimeOffset? through) =>
        new(s.Key, at, from ?? at, through ?? at);

    static void ScheduleEvent(SqTx tx, string kind, Guid scheduleId, string key, string actor, JsonObject details)
    {
        details["scheduleId"] = scheduleId.ToString("D");
        details["scheduleKey"] = key;
        tx.Exec(tx.Sql("""
            INSERT INTO runwick_queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, actor_id, details)
            VALUES (@application, NULL, NULL, @command_id, @kind, @now, @actor, @details)
            """).P("command_id", tx.Op.CommandId).P("kind", kind).P("now", tx.Now).P("actor", actor).P("details", details));
    }

    /// <summary>The management audit row (08 §5), written only on success, in the effect's transaction.</summary>
    static void ScheduleAudit(SqTx tx, string key, ControlActor actor, long? expected, long? resulting, JsonObject details) =>
        Expect(tx.Exec(tx.Sql("""
            INSERT INTO runwick_management_audit (
                application, command_id, operation, target_kind, target_id, actor_subject, actor_display, actor_scheme, reason,
                expected_revision, resulting_revision, details, occurred_at)
            VALUES (@application, @command_id, @operation, 'schedule', @target, @subject, @display, @scheme, @reason,
                    @expected, @resulting, @details, @now)
            """).P("command_id", tx.Op.CommandId).P("operation", tx.Op.Operation).P("target", key).P("subject", actor.Subject)
            .P("display", actor.Display).P("scheme", actor.Scheme).P("reason", actor.Reason).P("expected", expected).P("resulting", resulting)
            .P("details", details).P("now", tx.Now)), 1);

    async Task<SqOp> ScheduleOpAsync<T>(StoreCommand<T> command, string operation, Serialization.CanonicalValue body, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, command.Application, operation, command.CommandId, command.NotAfter, body);
        return Op(command.Application, command.CommandId, operation, command.NotAfter, fp, SqLane.Background);
    }

    static StoreReply<ScheduleRegistration> RegistrationReplay(SqTx tx, SqReceiptRow row) =>
        StoreReply<ScheduleRegistration>.Applied(ScheduleReceiptCodec.ReadRegistration(row.Result!), row.DecidedAt!.Value, replayed: true);

    static StoreReply<ScheduleMaterialization> MaterializationReplay(SqTx tx, SqReceiptRow row) =>
        StoreReply<ScheduleMaterialization>.Applied(ScheduleReceiptCodec.ReadMaterialization(row.Result!), row.DecidedAt!.Value, replayed: true);

    static StoreReply<ScheduleState> StateReplay(SqTx tx, SqReceiptRow row) =>
        StoreReply<ScheduleState>.Applied(ScheduleReceiptCodec.ReadState(row.Result!), row.DecidedAt!.Value, replayed: true);

    static ScheduleState StateOf(ScheduleSnapshot s, bool? paused = null, bool? review = null, long? control = null) =>
        new(s.Key, paused ?? s.Paused, review ?? s.NeedsReview, s.DefinitionRevision, control ?? s.ControlRevision, s.CursorRevision);

    static SqOutcome<T> Applied<T>(SqTx tx, T value, JsonObject receipt) where T : class =>
        new SqOutcome<T>.Commit(StoreReply<T>.Applied(value, tx.Now), receipt);

    // ------------------------------------------------------------------ RegisterSchedule/1 (13 §6.1)

    public async Task<StoreReply<ScheduleRegistration>> RegisterScheduleAsync(StoreCommand<RegisterScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = (string.IsNullOrEmpty(r.Key) || Encoding.UTF8.GetByteCount(r.Key) > 512 || r.Key.Any(char.IsControl) ? "key" : null)
            ?? ValidateScheduleDefinition(r.Definition)
            ?? (r.FirstDueAt is { } due && due <= r.Cutoff && r.Definition.Kind == ScheduleOperations.Cron ? "firstDueAt" : null);
        if (invalid is not null)
        {
            return ScheduleInvalid<ScheduleRegistration>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.RegisterSchedule, ScheduleEnvelopes.Register(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            if (!SchedulerActive(tx))
            {
                return new SqOutcome<ScheduleRegistration>.Rollback(ScheduleInvalid<ScheduleRegistration>(StoreStatus.UnsupportedVersion, "scheduler_not_active"));
            }

            // Seed-if-absent; cron schedules start strictly after the cutoff, a one-time schedule has no high-water mark.
            var cron = r.Definition.Kind == ScheduleOperations.Cron;
            tx.Exec(tx.Sql("""
                INSERT INTO runwick_schedules (application, schedule_id, schedule_key, next_due_at, scan_after_at, evaluated_through, created_at, updated_at)
                VALUES (@application, @id, @key, @next, COALESCE(@next, @cutoff), @through, @now, @now)
                ON CONFLICT (application, schedule_key) DO NOTHING
                """).P("id", Guid.CreateVersion7()).P("key", r.Key).P("next", r.FirstDueAt).P("cutoff", r.Cutoff)
                .P("through", cron ? r.Cutoff : null).P("now", tx.Now));
            var row = tx.Query(tx.Sql("""
                SELECT s.schedule_id, s.definition_revision, d.definition_fingerprint
                FROM runwick_schedules AS s
                LEFT JOIN runwick_schedule_definitions AS d
                  ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
                WHERE s.application = @application AND s.schedule_key = @key
                """).P("key", r.Key), x => (Id: x.Uuid(0), Revision: x.GetInt64(1), Fingerprint: x.IsDBNull(2) ? null : x.Blob(2)))[0];
            if (!tx.Decide())
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.CommandExpired, "command_expired");
            }

            if (tx.Now < r.Cutoff)
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "cutoff_in_future");
            }

            ScheduleRegistration result;
            if (row.Fingerprint is null)
            {
                InsertDefinition(tx, row.Id, 1, r.Definition, r.Cutoff, null);
                result = new ScheduleRegistration(row.Id, "created", 1, r.Definition.Fingerprint);
                ScheduleEvent(tx, "schedule_registered", row.Id, r.Key, SchedulerActor.Subject, new JsonObject
                {
                    ["firstDueAt"] = r.FirstDueAt is { } f ? SqTime.Ms(f) : null,
                    ["definitionFingerprint"] = Convert.ToHexStringLower(r.Definition.Fingerprint),
                });
            }
            else
            {
                // Startup never overwrites: a different fingerprint is a drift diagnostic (13 §6.1).
                result = new ScheduleRegistration(row.Id,
                    row.Fingerprint.AsSpan().SequenceEqual(r.Definition.Fingerprint) ? "same" : "drift", row.Revision, row.Fingerprint);
            }

            return Applied(tx, result, ScheduleReceiptCodec.Registration(result));
        }, RegistrationReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ MaterializeOccurrences/1 (13 §4.2, materialize_occurrences_v3)

    public async Task<StoreReply<ScheduleMaterialization>> MaterializeOccurrencesAsync(StoreCommand<MaterializeRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var ascending = r.Occurrences.Zip(r.Occurrences.Skip(1)).All(p => p.First.ScheduledAt < p.Second.ScheduledAt);
        var invalid = (r.Occurrences.Count > ScheduleBounds.MaxBatch ? "occurrences" : null)
            ?? (!ascending ? "order" : null)
            ?? (r.Occurrences.Select(o => o.ProposedJobId).Distinct().Count() != r.Occurrences.Count ? "proposedJobId" : null)
            ?? (r.Occurrences.Any(o => o.ScheduledAt > r.Cutoff) ? "cutoff" : null)
            ?? (r.AuditRanges.Any(a => a.Reason is not ("misfire_skip" or "run_once_covered" or "skipped_paused" or "catch_up_skipped")) ? "auditRanges" : null);
        if (invalid is not null)
        {
            return ScheduleInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.MaterializeOccurrences, ScheduleEnvelopes.Materialize(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            if (!SchedulerActive(tx))
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(ScheduleInvalid<ScheduleMaterialization>(StoreStatus.UnsupportedVersion, "scheduler_not_active"));
            }

            // Plan checks precede the dispatch core, so a stale plan is always reported as stale (13 §4.2).
            var s = LockSchedule(tx, r.ScheduleId, null);
            var through = s?.EvaluatedThrough;
            var refusal = s is null ? (StoreStatus.NotFound, "schedule_not_found", (string?)null)
                : !s.Enabled ? (StoreStatus.InvalidState, "schedule_disabled", null)
                : s.Paused ? (StoreStatus.InvalidState, "schedule_paused", null)
                : s.NeedsReview ? (StoreStatus.InvalidState, "schedule_needs_review", null)
                : s.DefinitionRevision != r.ExpectedDefinitionRevision || s.ControlRevision != r.ExpectedControlRevision
                    || s.CursorRevision != r.ExpectedCursorRevision || s.Episode?.Id != r.ExpectedEpisodeId
                    ? (StoreStatus.VersionConflict, "revision_conflict",
                        string.Create(CultureInfo.InvariantCulture, $"actual={s.DefinitionRevision}/{s.ControlRevision}/{s.CursorRevision}"))
                : (long)r.Occurrences.Count * s.Definition.Template.PayloadUtf8.Length > ScheduleBounds.MaxPlanPayloadBytes
                    ? (StoreStatus.InvalidRequest, "plan_too_large", null)
                : through is { } t && r.Occurrences.Any(o => o.ScheduledAt <= t) ? (StoreStatus.InvariantFault, "occurrence_below_high_water", null)
                : through is { } t2 && (r.After.EvaluatedThrough is null || r.After.EvaluatedThrough < t2) ? (StoreStatus.InvariantFault, "cursor_rewind", null)
                : default((StoreStatus, string, string?)?);
            if (refusal is { } x)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(ScheduleInvalid<ScheduleMaterialization>(x.Item1, x.Item2, x.Item3));
            }

            var (jobs, refused) = DispatchScheduled<ScheduleMaterialization>(tx, s!, [.. r.Occurrences.Select(o => (o.ProposedJobId,
                new JobScheduleInfo(s!.Key, o.ScheduledAt, o.CoveredFrom ?? o.ScheduledAt, o.CoveredThrough ?? o.ScheduledAt)))]);
            if (refused is not null)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(refused);
            }

            // The clock went backward after planning: reject and replan (13 §4.2).
            if (tx.Now < r.Cutoff)
            {
                return ScheduleRollback<ScheduleMaterialization>(tx, StoreStatus.InvalidState, "cutoff_in_future");
            }

            var keys = r.Occurrences.Select(o => ScheduleOperations.OccurrenceKey(o.ScheduledAt)).ToList();
            if (!InsertOccurrences(tx, s!.ScheduleId, s.DefinitionRevision, r.Occurrences.Select((o, i) =>
                (ScheduleOperations.ScheduledKind, keys[i], (DateTimeOffset?)o.ScheduledAt, jobs[i].JobId, jobs[i].Disposition, o.Coalesced, o.CoveredFrom, o.CoveredThrough))))
            {
                return ScheduleRollback<ScheduleMaterialization>(tx, StoreStatus.InvariantFault, "occurrence_exists");
            }

            var a = r.After;
            Expect(tx.Exec(tx.Sql("""
                UPDATE runwick_schedules
                SET evaluated_through = @through, next_due_at = @next, scan_after_at = @scan, episode_id = @episode, episode_cutoff = @episode_cutoff,
                    episode_emitted = @emitted, episode_limit = @limit, needs_review = @review IS NOT NULL, review_reason = @review,
                    resume_policy = CASE WHEN @clear THEN NULL ELSE resume_policy END,
                    resume_cutoff = CASE WHEN @clear THEN NULL ELSE resume_cutoff END,
                    cursor_revision = cursor_revision + 1, updated_at = @now
                WHERE application = @application AND schedule_id = @id
                """).P("through", a.EvaluatedThrough).P("next", a.NextDueAt).P("scan", a.ScanAfterAt).P("episode", a.Episode?.Id)
                .P("episode_cutoff", a.Episode?.Cutoff).P("emitted", a.Episode?.Emitted ?? 0).P("limit", a.Episode?.Limit)
                .P("review", a.ReviewReason).P("clear", r.ClearsPendingPolicy).P("now", tx.Now).P("id", s.ScheduleId)), 1);
            AuditRanges(tx, s.ScheduleId, s.DefinitionRevision, r.AuditRanges);
            ScheduleEvent(tx, "schedule_materialized", s.ScheduleId, s.Key, SchedulerActor.Subject, new JsonObject
            {
                ["cursorRevision"] = s.CursorRevision + 1, ["cutoff"] = SqTime.Ms(r.Cutoff), ["jobs"] = r.Occurrences.Count,
                ["skippedRanges"] = r.AuditRanges.Count, ["episodeId"] = a.Episode?.Id.ToString("D"), ["needsReview"] = a.ReviewReason,
            });
            if (a.ReviewReason is not null)
            {
                ScheduleEvent(tx, "schedule_needs_review", s.ScheduleId, s.Key, SchedulerActor.Subject, new JsonObject { ["reason"] = a.ReviewReason });
            }

            var result = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision + 1,
                [.. keys.Select((k, i) => new MaterializedOccurrence(ScheduleOperations.ScheduledKind, k, jobs[i].JobId, jobs[i].Disposition))]);
            return Applied(tx, result, ScheduleReceiptCodec.Materialization(result));
        }, MaterializationReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ SetScheduleState/1 (13 §6.3)

    public async Task<StoreReply<ScheduleState>> SetScheduleStateAsync(StoreCommand<ScheduleStateRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ScheduleActorInvalid(r.Actor)
            ?? (r.Paused ? (r.ResumePolicy is not null ? "resumePolicy" : null)
                : r.ResumePolicy is not (ScheduleOperations.ResumeSkipPaused or ScheduleOperations.MisfireRunOnce or ScheduleOperations.MisfireCatchUp) ? "resumePolicy" : null);
        if (invalid is not null)
        {
            return ScheduleInvalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.SetScheduleState, ScheduleEnvelopes.State(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            var s = LockSchedule(tx, null, r.Key);
            if (!tx.Decide())
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.CommandExpired, "command_expired");
            }

            if (s is null)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.NotFound, "schedule_not_found");
            }

            if (s.ControlRevision != r.ExpectedControlRevision)
            {
                return ScheduleConflict<ScheduleState>(tx, s.ControlRevision);
            }

            if (s.Paused == r.Paused)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.InvalidState, "no_change");
            }

            if (!r.Paused && s.Episode is not null)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.InvalidState, "episode_unresolved");
            }

            // Pause cancels nothing. Resume stores its policy and cutoff; the next plan applies it (13 §5.4).
            Expect(tx.Exec(tx.Sql("""
                UPDATE runwick_schedules
                SET paused = @paused, control_revision = control_revision + 1, updated_at = @now, updated_by = @subject,
                    resume_policy = CASE WHEN @paused THEN resume_policy ELSE @policy END,
                    resume_cutoff = CASE WHEN @paused THEN resume_cutoff ELSE @now END,
                    scan_after_at = CASE WHEN @paused THEN scan_after_at ELSE @now END
                WHERE application = @application AND schedule_id = @id
                """).P("paused", r.Paused).P("now", tx.Now).P("subject", r.Actor.Subject).P("policy", r.ResumePolicy).P("id", s.ScheduleId)), 1);
            var state = StateOf(s, paused: r.Paused, control: s.ControlRevision + 1);
            ScheduleEvent(tx, r.Paused ? "schedule_paused" : "schedule_resumed", s.ScheduleId, s.Key, r.Actor.Subject, new JsonObject
            {
                ["controlRevision"] = state.ControlRevision, ["resumePolicy"] = r.ResumePolicy,
            });
            ScheduleAudit(tx, s.Key, r.Actor, s.ControlRevision, state.ControlRevision, new JsonObject
            {
                ["paused"] = r.Paused, ["resumePolicy"] = r.ResumePolicy,
            });
            return Applied(tx, state, ScheduleReceiptCodec.State(state));
        }, StateReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ UpdateSchedule/1 (13 §6.4)

    public async Task<StoreReply<ScheduleRegistration>> UpdateScheduleAsync(StoreCommand<UpdateScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ScheduleActorInvalid(r.Actor) ?? ValidateScheduleDefinition(r.Definition)
            ?? (r.Definition.Kind == ScheduleOperations.OneTime && r.Definition.DueAt <= r.Cutoff ? "dueAt" : null)
            ?? (r.FirstDueAt is { } due && due <= r.Cutoff ? "firstDueAt" : null);
        if (invalid is not null)
        {
            return ScheduleInvalid<ScheduleRegistration>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.UpdateSchedule, ScheduleEnvelopes.Update(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            var s = LockSchedule(tx, null, r.Key);
            if (!tx.Decide())
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.CommandExpired, "command_expired");
            }

            if (s is null)
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.NotFound, "schedule_not_found");
            }

            if (s.DefinitionRevision != r.ExpectedDefinitionRevision)
            {
                return ScheduleConflict<ScheduleRegistration>(tx, s.DefinitionRevision);
            }

            if (s.Episode is not null)
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "episode_unresolved");
            }

            if (s.Definition.Fingerprint.AsSpan().SequenceEqual(r.Definition.Fingerprint))
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "no_change");
            }

            if (tx.Now < r.Cutoff)
            {
                return ScheduleRollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "cutoff_in_future");
            }

            // Forward cutover (13 §6.4): unmaterialized old occurrences through the cutoff are skipped by edit;
            // the high-water mark never moves back; materialized jobs keep their payload.
            var revision = s.DefinitionRevision + 1;
            InsertDefinition(tx, s.ScheduleId, revision, r.Definition, r.Cutoff, r.Actor.Subject);
            var through = s.EvaluatedThrough is { } e && e > r.Cutoff ? e : r.Cutoff;
            if (s.NextDueAt is { } pending && pending <= r.Cutoff)
            {
                AuditRanges(tx, s.ScheduleId, s.DefinitionRevision, [new ScheduleAuditRange(s.EvaluatedThrough, r.Cutoff, "skipped_by_edit")]);
            }

            Expect(tx.Exec(tx.Sql("""
                UPDATE runwick_schedules
                SET definition_revision = @revision, evaluated_through = @through, next_due_at = @next,
                    scan_after_at = COALESCE(@next, @now), needs_review = 0, review_reason = NULL,
                    resume_policy = NULL, resume_cutoff = NULL, cursor_revision = cursor_revision + 1,
                    updated_at = @now, updated_by = @subject
                WHERE application = @application AND schedule_id = @id
                """).P("revision", revision).P("through", through).P("next", r.FirstDueAt).P("now", tx.Now)
                .P("subject", r.Actor.Subject).P("id", s.ScheduleId)), 1);
            var result = new ScheduleRegistration(s.ScheduleId, "updated", revision, r.Definition.Fingerprint);
            ScheduleEvent(tx, "schedule_updated", s.ScheduleId, s.Key, r.Actor.Subject, new JsonObject
            {
                ["definitionRevision"] = revision, ["cutoff"] = SqTime.Ms(r.Cutoff),
                ["definitionFingerprint"] = Convert.ToHexStringLower(r.Definition.Fingerprint),
            });
            ScheduleAudit(tx, s.Key, r.Actor, s.DefinitionRevision, revision, new JsonObject
            {
                ["previousFingerprint"] = Convert.ToHexStringLower(s.Definition.Fingerprint),
                ["definitionFingerprint"] = Convert.ToHexStringLower(r.Definition.Fingerprint),
            });
            return Applied(tx, result, ScheduleReceiptCodec.Registration(result));
        }, RegistrationReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ TriggerSchedule/1 (13 §6.5)

    public async Task<StoreReply<ScheduleMaterialization>> TriggerScheduleAsync(StoreCommand<TriggerScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ScheduleActorInvalid(r.Actor) is { } invalid)
        {
            return ScheduleInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.TriggerSchedule, ScheduleEnvelopes.Trigger(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            if (!SchedulerActive(tx))
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(ScheduleInvalid<ScheduleMaterialization>(StoreStatus.UnsupportedVersion, "scheduler_not_active"));
            }

            var s = LockSchedule(tx, null, r.Key);
            var rejection = s is null ? (StoreStatus.NotFound, "schedule_not_found", (string?)null)
                : !s.Enabled ? (StoreStatus.InvalidState, "schedule_disabled", null)
                : s.DefinitionRevision != r.ExpectedDefinitionRevision
                    ? (StoreStatus.VersionConflict, "revision_conflict", "actual=" + s.DefinitionRevision.ToString(CultureInfo.InvariantCulture))
                : default((StoreStatus, string, string?)?);
            if (rejection is { } x)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(ScheduleInvalid<ScheduleMaterialization>(x.Item1, x.Item2, x.Item3));
            }

            // The same trigger ID returns the existing occurrence, never new work (13 §6.5).
            var key = r.TriggerId.ToString("D");
            var existing = tx.Query(tx.Sql("""
                SELECT job_id, disposition FROM runwick_schedule_occurrences
                WHERE application = @application AND schedule_id = @id AND occurrence_kind = 'manual' AND occurrence_key = @key
                """).P("id", s!.ScheduleId).P("key", key), o => (JobId: o.UuidN(0), Disposition: o.GetString(1)));
            if (existing.Count == 1)
            {
                if (!tx.Decide())
                {
                    return ScheduleRollback<ScheduleMaterialization>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                var prior = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision,
                    [new MaterializedOccurrence(ScheduleOperations.ManualKind, key, existing[0].JobId, existing[0].Disposition)]);
                return Applied(tx, prior, ScheduleReceiptCodec.Materialization(prior));
            }

            var (jobs, refused) = DispatchScheduled<ScheduleMaterialization>(tx, s, [(r.ProposedJobId, Occurrence(s, default, null, null))]);
            if (refused is not null)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(refused);
            }

            if (!InsertOccurrences(tx, s.ScheduleId, s.DefinitionRevision,
                [(ScheduleOperations.ManualKind, key, null, jobs[0].JobId, jobs[0].Disposition, false, null, null)]))
            {
                return ScheduleRollback<ScheduleMaterialization>(tx, StoreStatus.InvariantFault, "occurrence_exists");
            }

            var result = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision,
                [new MaterializedOccurrence(ScheduleOperations.ManualKind, key, jobs[0].JobId, jobs[0].Disposition)]);
            ScheduleEvent(tx, "schedule_triggered", s.ScheduleId, s.Key, r.Actor.Subject, new JsonObject
            {
                ["triggerId"] = key, ["jobId"] = jobs[0].JobId.ToString("D"), ["disposition"] = jobs[0].Disposition, ["paused"] = s.Paused,
            });
            ScheduleAudit(tx, s.Key, r.Actor, s.DefinitionRevision, s.DefinitionRevision, new JsonObject
            {
                ["triggerId"] = key, ["jobId"] = jobs[0].JobId.ToString("D"),
            });
            return Applied(tx, result, ScheduleReceiptCodec.Materialization(result));
        }, MaterializationReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ ResolveCatchUp/1 (13 §6.6)

    public async Task<StoreReply<ScheduleMaterialization>> ResolveCatchUpAsync(StoreCommand<ResolveCatchUpRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ScheduleActorInvalid(r.Actor)
            ?? (r.Resolution is not ("skip" or "extend" or "run_once") ? "resolution" : null)
            ?? (r.Resolution == "extend" && r.AdditionalJobs is < 1 or > ScheduleBounds.MaxEpisodeJobs ? "additionalJobs" : null);
        if (invalid is not null)
        {
            return ScheduleInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.ResolveCatchUp, ScheduleEnvelopes.Resolve(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            var s = LockSchedule(tx, null, r.Key);
            if (s is null || s.Episode is null || s.Episode.Id != r.EpisodeId)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(s is null
                    ? ScheduleInvalid<ScheduleMaterialization>(StoreStatus.NotFound, "schedule_not_found")
                    : ScheduleInvalid<ScheduleMaterialization>(StoreStatus.VersionConflict, "revision_conflict", "episode=" + s.Episode?.Id.ToString("D")));
            }

            var episode = s.Episode;
            if (r.Resolution == "extend" && episode.Limit + r.AdditionalJobs > ScheduleBounds.MaxEpisodeJobs)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(ScheduleInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", "additionalJobs"));
            }

            if (r.NextDueAfterCutoff is { } nd && nd <= episode.Cutoff)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(ScheduleInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", "nextDueAfterCutoff"));
            }

            var runOnce = r.Resolution == "run_once" && s.NextDueAt is not null;
            var (jobs, refused) = DispatchScheduled<ScheduleMaterialization>(tx, s,
                runOnce ? [(r.ProposedJobId, Occurrence(s, s.NextDueAt!.Value, s.NextDueAt, episode.Cutoff))] : []);
            if (refused is not null)
            {
                return new SqOutcome<ScheduleMaterialization>.Rollback(refused);
            }

            var occurrences = new List<MaterializedOccurrence>();
            if (runOnce)
            {
                var key = ScheduleOperations.OccurrenceKey(s.NextDueAt!.Value);
                if (!InsertOccurrences(tx, s.ScheduleId, s.DefinitionRevision,
                    [(ScheduleOperations.ScheduledKind, key, s.NextDueAt, jobs[0].JobId, jobs[0].Disposition, false, s.NextDueAt, episode.Cutoff)]))
                {
                    return ScheduleRollback<ScheduleMaterialization>(tx, StoreStatus.InvariantFault, "occurrence_exists");
                }

                occurrences.Add(new MaterializedOccurrence(ScheduleOperations.ScheduledKind, key, jobs[0].JobId, jobs[0].Disposition));
            }

            if (r.Resolution == "extend")
            {
                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_schedules
                    SET episode_limit = episode_limit + @add, needs_review = 0, review_reason = NULL, scan_after_at = @now,
                        cursor_revision = cursor_revision + 1, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND schedule_id = @id
                    """).P("add", r.AdditionalJobs).P("now", tx.Now).P("subject", r.Actor.Subject).P("id", s.ScheduleId)), 1);
            }
            else
            {
                if (s.NextDueAt is not null && s.NextDueAt <= episode.Cutoff)
                {
                    AuditRanges(tx, s.ScheduleId, s.DefinitionRevision,
                        [new ScheduleAuditRange(s.EvaluatedThrough, episode.Cutoff, runOnce ? "run_once_covered" : "catch_up_skipped")]);
                }

                // GREATEST(evaluated_through, cutoff) with PostgreSQL's NULL handling.
                var through = s.EvaluatedThrough is { } e && e > episode.Cutoff ? e : episode.Cutoff;
                Expect(tx.Exec(tx.Sql("""
                    UPDATE runwick_schedules
                    SET evaluated_through = @through, next_due_at = @next,
                        scan_after_at = COALESCE(@next, @now), episode_id = NULL, episode_cutoff = NULL, episode_emitted = 0,
                        episode_limit = NULL, needs_review = 0, review_reason = NULL,
                        cursor_revision = cursor_revision + 1, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND schedule_id = @id
                    """).P("through", through).P("next", r.NextDueAfterCutoff).P("now", tx.Now).P("subject", r.Actor.Subject)
                    .P("id", s.ScheduleId)), 1);
            }

            var result = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision + 1, occurrences);
            ScheduleEvent(tx, "catch_up_resolved", s.ScheduleId, s.Key, r.Actor.Subject, new JsonObject
            {
                ["episodeId"] = episode.Id.ToString("D"), ["resolution"] = r.Resolution, ["additionalJobs"] = r.AdditionalJobs,
                ["jobs"] = occurrences.Count,
            });
            ScheduleAudit(tx, s.Key, r.Actor, s.CursorRevision, result.CursorRevision, new JsonObject
            {
                ["episodeId"] = episode.Id.ToString("D"), ["resolution"] = r.Resolution,
            });
            return Applied(tx, result, ScheduleReceiptCodec.Materialization(result));
        }, MaterializationReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ RecordScheduleEvaluationFailure/1 (13 §6.7)

    public async Task<StoreReply<ScheduleState>> RecordEvaluationFailureAsync(StoreCommand<EvaluationFailureRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ScheduleActorInvalid(r.Actor)
            ?? (string.IsNullOrEmpty(r.Code) || r.Code.Length > 64 || r.Code.Any(c => !(char.IsAsciiLetterOrDigit(c) || c == '_')) ? "code" : null)
            ?? (r.Detail is { } d && Encoding.UTF8.GetByteCount(d) > 1024 ? "detail" : null);
        if (invalid is not null)
        {
            return ScheduleInvalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.RecordScheduleEvaluationFailure, ScheduleEnvelopes.Failure(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            var s = LockSchedule(tx, r.ScheduleId, null);
            if (!tx.Decide())
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.CommandExpired, "command_expired");
            }

            if (s is null)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.NotFound, "schedule_not_found");
            }

            if (s.DefinitionRevision != r.ExpectedDefinitionRevision)
            {
                return ScheduleConflict<ScheduleState>(tx, s.DefinitionRevision);
            }

            if (s.NeedsReview)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.InvalidState, "no_change");
            }

            // The cursor never moves on a failure (13 §6.7).
            Expect(tx.Exec(tx.Sql("""
                UPDATE runwick_schedules SET needs_review = 1, review_reason = @code, updated_at = @now
                WHERE application = @application AND schedule_id = @id
                """).P("code", r.Code).P("now", tx.Now).P("id", s.ScheduleId)), 1);
            var state = StateOf(s, review: true);
            ScheduleEvent(tx, "schedule_needs_review", s.ScheduleId, s.Key, r.Actor.Subject, new JsonObject { ["reason"] = r.Code, ["detail"] = r.Detail });
            ScheduleAudit(tx, s.Key, r.Actor, s.DefinitionRevision, s.DefinitionRevision, new JsonObject { ["code"] = r.Code, ["detail"] = r.Detail });
            return Applied(tx, state, ScheduleReceiptCodec.State(state));
        }, StateReplay, ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ RevalidateSchedule/1 (13 §6.8)

    public async Task<StoreReply<ScheduleState>> RevalidateScheduleAsync(StoreCommand<RevalidateScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ScheduleActorInvalid(r.Actor) is { } invalid)
        {
            return ScheduleInvalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var op = await ScheduleOpAsync(command, ScheduleOperations.RevalidateSchedule, ScheduleEnvelopes.Revalidate(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(op, tx =>
        {
            var s = LockSchedule(tx, null, r.Key);
            if (!tx.Decide())
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.CommandExpired, "command_expired");
            }

            if (s is null)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.NotFound, "schedule_not_found");
            }

            if (s.DefinitionRevision != r.ExpectedDefinitionRevision)
            {
                return ScheduleConflict<ScheduleState>(tx, s.DefinitionRevision);
            }

            if (s.Episode is not null)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.InvalidState, "episode_unresolved");
            }

            if (!s.NeedsReview)
            {
                return ScheduleRollback<ScheduleState>(tx, StoreStatus.InvalidState, "no_change");
            }

            Expect(tx.Exec(tx.Sql("""
                UPDATE runwick_schedules
                SET needs_review = 0, review_reason = NULL, scan_after_at = @now, updated_at = @now, updated_by = @subject
                WHERE application = @application AND schedule_id = @id
                """).P("now", tx.Now).P("subject", r.Actor.Subject).P("id", s.ScheduleId)), 1);
            var state = StateOf(s, review: false);
            ScheduleEvent(tx, "schedule_revalidated", s.ScheduleId, s.Key, r.Actor.Subject, new JsonObject { ["previousReason"] = s.ReviewReason });
            ScheduleAudit(tx, s.Key, r.Actor, s.DefinitionRevision, s.DefinitionRevision, new JsonObject { ["previousReason"] = s.ReviewReason });
            return Applied(tx, state, ScheduleReceiptCodec.State(state));
        }, StateReplay, ct).ConfigureAwait(false);
    }
}
