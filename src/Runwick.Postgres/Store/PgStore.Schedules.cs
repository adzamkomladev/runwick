using System.Text;
using System.Text.Json.Nodes;
using Npgsql;
using NpgsqlTypes;
using Runwick.Protocol;

namespace Runwick.Postgres.Store;

/// <summary>
/// Schedules (13): registration seeds, MaterializeOccurrences/1 against the locked cursor, controls, manual triggers,
/// catch-up resolution, evaluation failures and revalidation. The store never runs the evaluator; every cursor move
/// is the caller's plan, checked against the locked row's revisions, the high-water mark and fresh time (13 [choice] 1).
/// </summary>
internal sealed partial class PgStore : IScheduleStore
{
    /// <summary>The actor of system schedule commands (13 C-6).</summary>
    internal static readonly ControlActor SchedulerActor = new("svc:scheduler", "Runwick scheduler", "runwick", null);

    const string ScheduleColumns = """
        s.schedule_id, s.schedule_key, s.definition_revision, s.control_revision, s.cursor_revision, s.enabled, s.paused,
        s.needs_review, s.review_reason, s.next_due_at, s.scan_after_at, s.evaluated_through, s.episode_id, s.episode_cutoff,
        s.episode_emitted, s.episode_limit, s.resume_policy, s.resume_cutoff,
        d.kind, d.due_at, d.cron_text, d.cron_dialect, d.zone_id, d.evaluator_profile, d.tzdb_version, d.tzdb_sha256,
        d.gap_policy, d.fold_policy, d.misfire_policy, d.misfire_grace_ms, d.max_batch, d.max_episode_jobs,
        d.max_catchup_age_ms, d.overlap_policy, d.allow_reuse, d.job_name, d.payload_version, d.serializer_profile_id,
        d.serializer_profile_version, d.payload_template, d.template::text AS template, d.definition_fingerprint
        """;

    const string ScheduleJoin = """
        runwick.schedules AS s
        JOIN runwick.schedule_definitions AS d
          ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
        """;

    static ScheduleSnapshot ReadSnapshot(NpgsqlDataReader r, int o)
    {
        ScheduleEpisode? episode = r.IsDBNull(o + 12) ? null
            : new ScheduleEpisode(r.GetGuid(o + 12), PgTime.Read(r, o + 13), r.GetInt32(o + 14), r.GetInt32(o + 15));
        var definition = new ScheduleDefinitionRecord(
            r.GetString(o + 18), PgTime.ReadNullable(r, o + 19), Str(r, o + 20), Str(r, o + 21), Str(r, o + 22), Str(r, o + 23),
            Str(r, o + 24), r.IsDBNull(o + 25) ? null : (byte[])r[o + 25], r.GetString(o + 26), r.GetString(o + 27), r.GetString(o + 28),
            r.GetInt64(o + 29), r.GetInt32(o + 30), r.GetInt32(o + 31), r.GetInt64(o + 32), r.GetString(o + 33), r.GetBoolean(o + 34),
            ReadTemplate(r.GetString(o + 35), r.GetInt32(o + 36), r.GetString(o + 37), r.GetInt32(o + 38), (byte[])r[o + 39],
                JsonNode.Parse(r.GetString(o + 40))!.AsObject()),
            (byte[])r[o + 41]);
        return new ScheduleSnapshot(
            r.GetGuid(o), r.GetString(o + 1), r.GetInt64(o + 2), r.GetInt64(o + 3), r.GetInt64(o + 4), r.GetBoolean(o + 5),
            r.GetBoolean(o + 6), r.GetBoolean(o + 7), Str(r, o + 8), PgTime.ReadNullable(r, o + 9), PgTime.Read(r, o + 10),
            PgTime.ReadNullable(r, o + 11), episode, Str(r, o + 16), PgTime.ReadNullable(r, o + 17), definition);
    }

    static string? Str(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

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
    internal static string? ValidateDefinition(ScheduleDefinitionRecord d)
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

    StoreReply<T> Invalid<T>(StoreStatus status, string code, string? detail = null) where T : class =>
        StoreReply<T>.NoEffect(status, new StoreError(code, Detail: detail), PgTime.Floor(time.GetUtcNow()));

    static PgOutcome<T> Conflict<T>(PgTx tx, string actual) where T : class =>
        new PgOutcome<T>.Rollback(StoreReply<T>.NoEffect(StoreStatus.VersionConflict,
            new StoreError("revision_conflict", Detail: "actual=" + actual), tx.Now));

    // ------------------------------------------------------------------ reads (13 §6.9)

    public Task<DueSchedules> ReadDueSchedulesAsync(string application, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, ScheduleBounds.DueReadLimit);
        return ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand($"""
                WITH gate AS (
                    SELECT m.store_id, m.schema_revision, m.protocol_epoch, m.migration_mode, clock_timestamp() AS observed_at
                    FROM runwick.store_metadata AS m
                    WHERE m.singleton_id = 1
                    FOR SHARE
                )
                SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode, g.observed_at, x.*
                FROM gate AS g
                LEFT JOIN LATERAL (
                    SELECT {ScheduleColumns}
                    FROM {ScheduleJoin}
                    WHERE s.application = @application
                      AND s.enabled AND NOT s.paused AND NOT s.needs_review AND s.next_due_at IS NOT NULL
                      AND s.scan_after_at <= g.observed_at
                    ORDER BY s.scan_after_at, s.schedule_id
                    LIMIT @limit
                ) AS x ON true
                """, conn);
            cmd.P("application", application).P("limit", limit);
            var list = new List<ScheduleSnapshot>();
            DateTimeOffset observed = default;
            var open = true;
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    open = CheckGate(reader.GetGuid(0), reader.GetInt64(1), reader.GetInt32(2), reader.GetString(3));
                    observed = PgTime.Floor(PgTime.Read(reader, 4));
                    if (!await reader.IsDBNullAsync(5, ct).ConfigureAwait(false))
                    {
                        var snapshot = ReadSnapshot(reader, 5);
                        RememberDefinition(application, snapshot);
                        list.Add(snapshot);
                    }
                }
            }

            return new DueSchedules(observed, open ? list : []);
        }, ct);
    }

    public Task<ScheduleSnapshot?> ReadScheduleAsync(string application, string key, CancellationToken ct) =>
        ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand($"""
                SELECT {ScheduleColumns}
                FROM {ScheduleJoin}
                WHERE s.application = @application AND s.schedule_key = @key
                """, conn);
            cmd.P("application", application).P("key", key);
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    return null;
                }

                var snapshot = ReadSnapshot(reader, 0);
                RememberDefinition(application, snapshot);
                return snapshot;
            }
        }, ct);

    public Task<DateTimeOffset> StoreNowAsync(CancellationToken ct) =>
        ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand("SELECT clock_timestamp()", conn);
            var value = (DateTime)(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!;
            return PgTime.Floor(new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc)));
        }, ct);

    // ------------------------------------------------------------------ helpers under the transaction

    static async Task<bool> SchedulerActiveAsync(PgTx tx)
    {
        using var q = tx.Sql("""
            SELECT required_capabilities ? 'scheduler' FROM runwick.application_controls WHERE application = @application
            """);
        var rows = await tx.QueryAsync(q, r => r.GetBoolean(0)).ConfigureAwait(false);
        return rows.Count == 1 && rows[0];
    }

    /// <summary>The schedule row, locked (rank: after the receipt, before queue controls; 13 §8), with its current definition.</summary>
    static async Task<ScheduleSnapshot?> LockScheduleAsync(PgTx tx, Guid? scheduleId, string? key)
    {
        using var q = tx.Sql($"""
            SELECT {ScheduleColumns}
            FROM {ScheduleJoin}
            WHERE s.application = @application AND {(scheduleId is null ? "s.schedule_key = @key" : "s.schedule_id = @id")}
            FOR UPDATE OF s
            """);
        if (scheduleId is { } id)
        {
            q.P("id", id);
        }
        else
        {
            q.P("key", key);
        }

        var rows = await tx.QueryAsync(q, r => ReadSnapshot(r, 0)).ConfigureAwait(false);
        return rows.Count == 1 ? rows[0] : null;
    }

    static async Task InsertDefinitionAsync(PgTx tx, Guid scheduleId, long revision, ScheduleDefinitionRecord d, DateTimeOffset cutoff, string? createdBy)
    {
        var t = d.Template;
        using var i = tx.Sql("""
            INSERT INTO runwick.schedule_definitions (
                application, schedule_id, revision, kind, due_at, cron_text, cron_dialect, zone_id, evaluator_profile, tzdb_version,
                tzdb_sha256, gap_policy, fold_policy, misfire_policy, misfire_grace_ms, max_batch, max_episode_jobs, max_catchup_age_ms,
                overlap_policy, allow_reuse, job_name, payload_version, serializer_profile_id, serializer_profile_version,
                payload_template, template, definition_fingerprint, cutoff_at, created_at, created_by)
            VALUES (@application, @id, @revision, @kind, @due_at, @cron_text, @cron_dialect, @zone_id, @profile, @tzdb_version,
                @tzdb_sha256, @gap, @fold, @misfire, @grace, @max_batch, @max_episode, @max_age, @overlap, @reuse, @job_name,
                @payload_version, @profile_id, @profile_version, @payload, @template, @fingerprint, @cutoff, @now, @created_by)
            """).P("id", scheduleId).P("revision", revision).P("kind", d.Kind).P("due_at", d.DueAt).P("cron_text", d.CronText)
                .P("cron_dialect", d.CronDialect).P("zone_id", d.ZoneId).P("profile", d.EvaluatorProfile).P("tzdb_version", d.TzdbVersion)
                .P("tzdb_sha256", d.TzdbSha256, NpgsqlDbType.Bytea).P("gap", d.GapPolicy).P("fold", d.FoldPolicy).P("misfire", d.MisfirePolicy)
                .P("grace", d.MisfireGraceMs).P("max_batch", d.MaxBatch).P("max_episode", d.MaxEpisodeJobs).P("max_age", d.MaxCatchUpAgeMs)
                .P("overlap", d.OverlapPolicy).P("reuse", d.AllowReuse).P("job_name", t.JobName).P("payload_version", t.PayloadVersion)
                .P("profile_id", t.SerializerProfileId).P("profile_version", t.SerializerProfileVersion)
                .P("payload", t.PayloadUtf8, NpgsqlDbType.Bytea).Json("template", TemplateJson(t))
                .P("fingerprint", d.Fingerprint, NpgsqlDbType.Bytea).P("cutoff", cutoff).P("now", tx.Now).P("created_by", createdBy);
        Expect(await tx.ExecAsync(i).ConfigureAwait(false), 1);
    }

    static async Task AuditRangesAsync(PgTx tx, Guid scheduleId, long definitionRevision, IReadOnlyList<ScheduleAuditRange> ranges)
    {
        if (ranges.Count == 0)
        {
            return;
        }

        using var i = tx.Sql("""
            INSERT INTO runwick.schedule_audit_ranges (
                application, schedule_id, definition_revision, command_id, from_exclusive, through_inclusive, reason, created_at)
            SELECT @application, @id, @revision, @command_id, r.from_exclusive, r.through_inclusive, r.reason, @now
            FROM unnest(@froms::timestamptz[], @throughs::timestamptz[], @reasons::text[]) AS r(from_exclusive, through_inclusive, reason)
            """).P("id", scheduleId).P("revision", definitionRevision).P("command_id", tx.Op.CommandId).P("now", tx.Now)
                .Arr("froms", ranges.Select(r => r.FromExclusive is { } f ? (DateTime?)f.UtcDateTime : null).ToArray(), NpgsqlDbType.TimestampTz)
                .Arr("throughs", ranges.Select(r => r.ThroughInclusive.UtcDateTime).ToArray(), NpgsqlDbType.TimestampTz)
                .Arr("reasons", ranges.Select(r => r.Reason).ToArray(), NpgsqlDbType.Text);
        Expect(await tx.ExecAsync(i).ConfigureAwait(false), ranges.Count);
    }

    /// <summary>Inserts occurrence rows; false when any already exists (an invariant failure, never a reason to advance).</summary>
    static async Task<bool> InsertOccurrencesAsync(PgTx tx, Guid scheduleId, long definitionRevision,
        IReadOnlyList<(string Kind, string Key, DateTimeOffset? ScheduledAt, Guid JobId, string Disposition, bool Coalesced, DateTimeOffset? From, DateTimeOffset? Through)> rows)
    {
        if (rows.Count == 0)
        {
            return true;
        }

        using var i = tx.Sql("""
            INSERT INTO runwick.schedule_occurrences (
                application, schedule_id, occurrence_kind, occurrence_key, scheduled_at, definition_revision, job_id, disposition,
                coalesced, covered_from, covered_through, materialized_at, command_id)
            SELECT @application, @id, o.kind, o.key, o.scheduled_at, @revision, o.job_id, o.disposition, o.coalesced,
                   o.covered_from, o.covered_through, @now, @command_id
            FROM unnest(@kinds::text[], @keys::text[], @scheduled::timestamptz[], @job_ids::uuid[], @dispositions::text[],
                        @coalesced::boolean[], @froms::timestamptz[], @throughs::timestamptz[])
                 AS o(kind, key, scheduled_at, job_id, disposition, coalesced, covered_from, covered_through)
            ORDER BY o.kind, o.key
            ON CONFLICT DO NOTHING
            """).P("id", scheduleId).P("revision", definitionRevision).P("now", tx.Now).P("command_id", tx.Op.CommandId)
                .Arr("kinds", rows.Select(r => r.Kind).ToArray(), NpgsqlDbType.Text)
                .Arr("keys", rows.Select(r => r.Key).ToArray(), NpgsqlDbType.Text)
                .Arr("scheduled", rows.Select(r => r.ScheduledAt is { } s ? (DateTime?)s.UtcDateTime : null).ToArray(), NpgsqlDbType.TimestampTz)
                .Arr("job_ids", rows.Select(r => r.JobId).ToArray(), NpgsqlDbType.Uuid)
                .Arr("dispositions", rows.Select(r => r.Disposition).ToArray(), NpgsqlDbType.Text)
                .Arr("coalesced", rows.Select(r => r.Coalesced).ToArray(), NpgsqlDbType.Boolean)
                .Arr("froms", rows.Select(r => r.From is { } f ? (DateTime?)f.UtcDateTime : null).ToArray(), NpgsqlDbType.TimestampTz)
                .Arr("throughs", rows.Select(r => r.Through is { } t ? (DateTime?)t.UtcDateTime : null).ToArray(), NpgsqlDbType.TimestampTz);
        return await tx.ExecAsync(i).ConfigureAwait(false) == rows.Count;
    }

    /// <summary>Creates the jobs of one command through the shared dispatch core (created reason <c>scheduled</c>, 13 C-10).</summary>
    async Task<(IReadOnlyList<(Guid JobId, string Disposition)> Jobs, StoreReply<T>? Rejection)> DispatchScheduledAsync<T>(
        PgTx tx, ScheduleSnapshot s, IReadOnlyList<(Guid JobId, JobScheduleInfo Occurrence)> proposedJobIds) where T : class
    {
        IReadOnlyList<(Guid JobId, string Disposition)> none = [];
        if (proposedJobIds.Count == 0)
        {
            return await tx.DecideAsync().ConfigureAwait(false) ? (none, null) : (none, tx.Reject<T>(StoreStatus.CommandExpired, "command_expired"));
        }

        var storeId = runner.StoreId ?? throw new InvalidOperationException("Describe has not run yet.");
        var notAfterMs = PgTime.Ms(tx.Op.NotAfter);
        var items = proposedJobIds
            .Select(p => s.Definition.Template.ToItem(storeId, tx.Application, tx.Op.CommandId, notAfterMs, s.ScheduleId, s.Definition.Serialize, p.JobId,
                p.Occurrence))
            .ToList();
        var outcome = await DispatchCoreAsync(tx, items, continueOnConflict: false, createdReason: "scheduled").ConfigureAwait(false);
        if (outcome.Rejection is { } rejection)
        {
            return (none, StoreReply<T>.NoEffect(rejection.Status, rejection.Error, tx.Now));
        }

        IReadOnlyList<(Guid JobId, string Disposition)> jobs = [.. outcome.Items.Select(i => (i.Result!.JobId,
            i.Result!.Disposition == DispatchDisposition.Created ? "created" : "linked_existing"))];
        return (jobs, null);
    }

    /// <summary>
    /// What <c>IJobContext.Schedule</c> reports for a job built from this occurrence (01 §9): the schedule key, the scheduled
    /// instant and the covered range, which is the instant itself unless a run-once covers more. A manual trigger passes
    /// <c>default</c>: the dispatch core substitutes its decision time, which is only known after the scope locks.
    /// </summary>
    static JobScheduleInfo Occurrence(ScheduleSnapshot s, DateTimeOffset at, DateTimeOffset? from, DateTimeOffset? through) =>
        new(s.Key, at, from ?? at, through ?? at);

    async Task ScheduleEventAsync(PgTx tx, string kind, ScheduleSnapshot s, ControlActor actor, JsonObject details)
    {
        details["scheduleId"] = s.ScheduleId.ToString("D");
        details["scheduleKey"] = s.Key;
        await ControlEventAsync(tx, null, null, kind, actor, details).ConfigureAwait(false);
    }

    async Task<byte[]> FingerprintAsync<T>(StoreCommand<T> command, string operation, Serialization.CanonicalValue body, CancellationToken ct)
    {
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        return CommandEnvelopes.Fingerprint(storeId, command.Application, operation, command.CommandId, command.NotAfter, body);
    }

    PgOp OpFor<T>(StoreCommand<T> command, string operation, byte[] fingerprint) =>
        Op(command.Application, command.CommandId, operation, command.NotAfter, fingerprint, PgLane.Background);

    // ------------------------------------------------------------------ RegisterSchedule/1 (13 §6.1)

    public async Task<StoreReply<ScheduleRegistration>> RegisterScheduleAsync(StoreCommand<RegisterScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = (string.IsNullOrEmpty(r.Key) || Encoding.UTF8.GetByteCount(r.Key) > 512 || r.Key.Any(char.IsControl) ? "key" : null)
            ?? ValidateDefinition(r.Definition)
            ?? (r.FirstDueAt is { } due && due <= r.Cutoff && r.Definition.Kind == ScheduleOperations.Cron ? "firstDueAt" : null);
        if (invalid is not null)
        {
            return Invalid<ScheduleRegistration>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.RegisterSchedule, ScheduleEnvelopes.Register(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.RegisterSchedule, fp),
            async tx =>
            {
                if (!await SchedulerActiveAsync(tx).ConfigureAwait(false))
                {
                    return new PgOutcome<ScheduleRegistration>.Rollback(Invalid<ScheduleRegistration>(StoreStatus.UnsupportedVersion, "scheduler_not_active"));
                }

                // Seed-if-absent: the insert waits on a concurrent registrant and then does nothing; the lock below
                // reads whichever row won. Cron schedules start strictly after the cutoff (no backfill); a one-time
                // schedule has no high-water mark until its disposition.
                var cron = r.Definition.Kind == ScheduleOperations.Cron;
                // A new row also notifies the schedule channel (13 Changes 2026-11-02, PF-07), delivered only on commit.
                using (var insert = tx.Sql("""
                    WITH ins AS (
                        INSERT INTO runwick.schedules (
                            application, schedule_id, schedule_key, next_due_at, scan_after_at, evaluated_through, created_at, updated_at)
                        VALUES (@application, @id, @key, @next, COALESCE(@next, @cutoff), @through,
                                runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp()))
                        ON CONFLICT (application, schedule_key) DO NOTHING
                        RETURNING schedule_key)
                    SELECT pg_notify('runwick_schedule', @application || '/' || ins.schedule_key) FROM ins
                    """).P("id", Guid.CreateVersion7()).P("key", r.Key).P("next", r.FirstDueAt).P("cutoff", r.Cutoff)
                        .P("through", cron ? r.Cutoff : (DateTimeOffset?)null))
                {
                    await tx.ExecAsync(insert).ConfigureAwait(false);
                }

                using var q = tx.Sql("""
                    SELECT s.schedule_id, s.definition_revision, d.definition_fingerprint
                    FROM runwick.schedules AS s
                    LEFT JOIN runwick.schedule_definitions AS d
                      ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
                    WHERE s.application = @application AND s.schedule_key = @key
                    FOR UPDATE OF s
                    """).P("key", r.Key);
                var row = (await tx.QueryAsync(q, x => (Id: x.GetGuid(0), Revision: x.GetInt64(1), Fingerprint: x.IsDBNull(2) ? null : (byte[])x[2]))
                    .ConfigureAwait(false))[0];
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (tx.Now < r.Cutoff)
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "cutoff_in_future");
                }

                ScheduleRegistration result;
                if (row.Fingerprint is null)
                {
                    await InsertDefinitionAsync(tx, row.Id, 1, r.Definition, r.Cutoff, null).ConfigureAwait(false);
                    result = new ScheduleRegistration(row.Id, "created", 1, r.Definition.Fingerprint);
                    await ControlEventAsync(tx, null, null, "schedule_registered", SchedulerActor, new JsonObject
                    {
                        ["scheduleId"] = row.Id.ToString("D"), ["scheduleKey"] = r.Key,
                        ["firstDueAt"] = r.FirstDueAt is { } f ? PgTime.Ms(f) : null,
                        ["definitionFingerprint"] = Convert.ToHexStringLower(r.Definition.Fingerprint),
                    }).ConfigureAwait(false);
                }
                else
                {
                    // Startup never overwrites: a different fingerprint is a drift diagnostic (13 §6.1).
                    result = new ScheduleRegistration(row.Id,
                        row.Fingerprint.AsSpan().SequenceEqual(r.Definition.Fingerprint) ? "same" : "drift", row.Revision, row.Fingerprint);
                }

                return new PgOutcome<ScheduleRegistration>.Commit(StoreReply<ScheduleRegistration>.Applied(result, tx.Now),
                    ScheduleReceiptCodec.Registration(result));
            },
            (tx, row) => Task.FromResult(StoreReply<ScheduleRegistration>.Applied(
                ScheduleReceiptCodec.ReadRegistration(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ MaterializeOccurrences/1 (13 §4.2)

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
            return Invalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.MaterializeOccurrences, ScheduleEnvelopes.Materialize(r), ct).ConfigureAwait(false);
        var op = OpFor(command, ScheduleOperations.MaterializeOccurrences, fp);

        // The jobs are built here from the definition at the expected revision (immutable per revision); the function
        // checks the plan against the locked row before it uses them, so items from a stale revision are never used.
        var items = new List<DispatchItem>();
        if (r.Occurrences.Count > 0 && await DefinitionAtAsync(command.Application, r.ScheduleId, r.ExpectedDefinitionRevision, ct).ConfigureAwait(false)
                is { } d)
        {
            var storeId = runner.StoreId ?? throw new InvalidOperationException("Describe has not run yet.");
            var notAfterMs = PgTime.Ms(op.NotAfter);
            items.AddRange(r.Occurrences.Select(o => d.Definition.Template.ToItem(storeId, command.Application, command.CommandId, notAfterMs,
                r.ScheduleId, d.Definition.Serialize, o.ProposedJobId, new JobScheduleInfo(d.Key, o.ScheduledAt, o.CoveredFrom ?? o.ScheduledAt,
                    o.CoveredThrough ?? o.ScheduledAt))));
        }

        static DateTime? T(DateTimeOffset? t) => t?.UtcDateTime;
        const NpgsqlDbType A = NpgsqlDbType.Array;
        var a = r.After;
        var call = new PgCall("materialize_occurrences_v1")
            .P("p_schedule_id", r.ScheduleId).P("p_expected_definition", r.ExpectedDefinitionRevision)
            .P("p_expected_control", r.ExpectedControlRevision).P("p_expected_cursor", r.ExpectedCursorRevision)
            .P("p_expected_episode", r.ExpectedEpisodeId).P("p_cutoff", r.Cutoff).P("p_max_plan_bytes", (long)ScheduleBounds.MaxPlanPayloadBytes)
            .P("p_occ_scheduled", r.Occurrences.Select(o => o.ScheduledAt.UtcDateTime).ToArray(), A | NpgsqlDbType.TimestampTz)
            .P("p_occ_keys", r.Occurrences.Select(o => ScheduleOperations.OccurrenceKey(o.ScheduledAt)).ToArray(), A | NpgsqlDbType.Text)
            .P("p_occ_from", r.Occurrences.Select(o => T(o.CoveredFrom)).ToArray(), A | NpgsqlDbType.TimestampTz)
            .P("p_occ_through", r.Occurrences.Select(o => T(o.CoveredThrough)).ToArray(), A | NpgsqlDbType.TimestampTz)
            .P("p_occ_coalesced", r.Occurrences.Select(o => o.Coalesced).ToArray(), A | NpgsqlDbType.Boolean)
            .P("p_after_through", a.EvaluatedThrough).P("p_after_next", a.NextDueAt).P("p_after_scan", a.ScanAfterAt)
            .P("p_after_episode", a.Episode?.Id).P("p_after_episode_cutoff", a.Episode?.Cutoff)
            .P("p_after_episode_emitted", a.Episode?.Emitted ?? 0).P("p_after_episode_limit", a.Episode?.Limit)
            .P("p_review_reason", a.ReviewReason).P("p_clear", r.ClearsPendingPolicy)
            .P("p_audit_from", r.AuditRanges.Select(x => T(x.FromExclusive)).ToArray(), A | NpgsqlDbType.TimestampTz)
            .P("p_audit_through", r.AuditRanges.Select(x => x.ThroughInclusive.UtcDateTime).ToArray(), A | NpgsqlDbType.TimestampTz)
            .P("p_audit_reasons", r.AuditRanges.Select(x => x.Reason).ToArray(), A | NpgsqlDbType.Text);
        foreach (var (name, value, type) in DispatchArgs(items))
        {
            call.P(name, value, type);
        }

        return await runner.CallAsync(op, call, MapMaterialization, ct).ConfigureAwait(false);
    }

    /// <summary>The refusals the statement path reported at the client's clock (no decision time from the store).</summary>
    static readonly HashSet<string> PlanRefusals = new(StringComparer.Ordinal)
    {
        "scheduler_not_active", "schedule_not_found", "schedule_disabled", "schedule_paused", "schedule_needs_review", "revision_conflict",
        "plan_too_large", "occurrence_below_high_water", "cursor_rewind",
    };

    StoreReply<ScheduleMaterialization> MapMaterialization(PgRow row) =>
        row.HasValue ? row.Applied(ScheduleReceiptCodec.ReadMaterialization(row.Result!))
        : row.DecidedAt is null && row.Code is { } code && PlanRefusals.Contains(code)
            ? Invalid<ScheduleMaterialization>(row.StoreStatus, code, row.Detail)
            : row.Reject<ScheduleMaterialization>();

    /// <summary>
    /// Schedule definitions by (application, schedule, revision). A revision is immutable, so an entry never goes stale; it is
    /// filled by the schedule reads that precede a plan and, on a miss, by one read of the current row.
    /// </summary>
    readonly System.Collections.Concurrent.ConcurrentDictionary<(string Application, Guid ScheduleId, long Revision), (string Key, ScheduleDefinitionRecord Definition)>
        _definitions = new();

    void RememberDefinition(string application, ScheduleSnapshot s)
    {
        if (_definitions.Count >= 4096)
        {
            _definitions.Clear();
        }

        _definitions.TryAdd((application, s.ScheduleId, s.DefinitionRevision), (s.Key, s.Definition));
    }

    async Task<(string Key, ScheduleDefinitionRecord Definition)?> DefinitionAtAsync(string application, Guid scheduleId, long revision, CancellationToken ct)
    {
        if (_definitions.TryGetValue((application, scheduleId, revision), out var hit))
        {
            return hit;
        }

        var s = await ReadOnlyAsync(PgLane.Background, async conn =>
        {
            using var cmd = new NpgsqlCommand($"""
                SELECT {ScheduleColumns}
                FROM {ScheduleJoin}
                WHERE s.application = @application AND s.schedule_id = @id
                """, conn);
            cmd.P("application", application).P("id", scheduleId);
            var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                return await reader.ReadAsync(ct).ConfigureAwait(false) ? ReadSnapshot(reader, 0) : null;
            }
        }, ct).ConfigureAwait(false);
        if (s is null)
        {
            return null;
        }

        RememberDefinition(application, s);
        return s.DefinitionRevision == revision ? (s.Key, s.Definition) : null;
    }

    static Task<StoreReply<ScheduleMaterialization>> MaterializationReplay(PgTx tx, ReceiptRow row) =>
        Task.FromResult(StoreReply<ScheduleMaterialization>.Applied(ScheduleReceiptCodec.ReadMaterialization(row.Result!), row.DecidedAt!.Value, replayed: true));

    static Task<StoreReply<ScheduleState>> StateReplay(PgTx tx, ReceiptRow row) =>
        Task.FromResult(StoreReply<ScheduleState>.Applied(ScheduleReceiptCodec.ReadState(row.Result!), row.DecidedAt!.Value, replayed: true));

    static ScheduleState StateOf(ScheduleSnapshot s, bool? paused = null, bool? review = null, long? definition = null, long? control = null, long? cursor = null) =>
        new(s.Key, paused ?? s.Paused, review ?? s.NeedsReview, definition ?? s.DefinitionRevision, control ?? s.ControlRevision, cursor ?? s.CursorRevision);

    // ------------------------------------------------------------------ SetScheduleState/1 (13 §6.3)

    public async Task<StoreReply<ScheduleState>> SetScheduleStateAsync(StoreCommand<ScheduleStateRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor)
            ?? (r.Paused ? (r.ResumePolicy is not null ? "resumePolicy" : null)
                : r.ResumePolicy is not (ScheduleOperations.ResumeSkipPaused or ScheduleOperations.MisfireRunOnce or ScheduleOperations.MisfireCatchUp) ? "resumePolicy" : null);
        if (invalid is not null)
        {
            return Invalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.SetScheduleState, ScheduleEnvelopes.State(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.SetScheduleState, fp),
            async tx =>
            {
                var s = await LockScheduleAsync(tx, null, r.Key).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (s is null)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.NotFound, "schedule_not_found");
                }

                if (s.ControlRevision != r.ExpectedControlRevision)
                {
                    return Conflict<ScheduleState>(tx, s.ControlRevision.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }

                if (s.Paused == r.Paused)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.InvalidState, "no_change");
                }

                if (!r.Paused && s.Episode is not null)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.InvalidState, "episode_unresolved");
                }

                // Pause cancels nothing. Resume stores its policy and cutoff; the next plan applies it (13 §5.4).
                using (var u = tx.Sql("""
                    WITH n AS (SELECT pg_notify('runwick_schedule', @application || '/' || @key))
                    UPDATE runwick.schedules
                    SET paused = @paused, control_revision = control_revision + 1, updated_at = @now, updated_by = @subject,
                        resume_policy = CASE WHEN @paused THEN resume_policy ELSE @policy END,
                        resume_cutoff = CASE WHEN @paused THEN resume_cutoff ELSE @now END,
                        scan_after_at = CASE WHEN @paused THEN scan_after_at ELSE @now END
                    WHERE application = @application AND schedule_id = @id AND EXISTS (SELECT FROM n)
                    """).P("paused", r.Paused).P("now", tx.Now).P("subject", r.Actor.Subject).P("policy", r.ResumePolicy).P("id", s.ScheduleId)
                        .P("key", s.Key))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var state = StateOf(s, paused: r.Paused, control: s.ControlRevision + 1);
                await ScheduleEventAsync(tx, r.Paused ? "schedule_paused" : "schedule_resumed", s, r.Actor, new JsonObject
                {
                    ["controlRevision"] = state.ControlRevision, ["resumePolicy"] = r.ResumePolicy,
                }).ConfigureAwait(false);
                await AuditAsync(tx, "schedule", s.Key, r.Actor, s.ControlRevision, state.ControlRevision, new JsonObject
                {
                    ["paused"] = r.Paused, ["resumePolicy"] = r.ResumePolicy,
                }).ConfigureAwait(false);
                return new PgOutcome<ScheduleState>.Commit(StoreReply<ScheduleState>.Applied(state, tx.Now), ScheduleReceiptCodec.State(state));
            },
            StateReplay,
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ UpdateSchedule/1 (13 §6.4)

    public async Task<StoreReply<ScheduleRegistration>> UpdateScheduleAsync(StoreCommand<UpdateScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor) ?? ValidateDefinition(r.Definition)
            ?? (r.Definition.Kind == ScheduleOperations.OneTime && r.Definition.DueAt <= r.Cutoff ? "dueAt" : null)
            ?? (r.FirstDueAt is { } due && due <= r.Cutoff ? "firstDueAt" : null);
        if (invalid is not null)
        {
            return Invalid<ScheduleRegistration>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.UpdateSchedule, ScheduleEnvelopes.Update(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.UpdateSchedule, fp),
            async tx =>
            {
                var s = await LockScheduleAsync(tx, null, r.Key).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (s is null)
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.NotFound, "schedule_not_found");
                }

                if (s.DefinitionRevision != r.ExpectedDefinitionRevision)
                {
                    return Conflict<ScheduleRegistration>(tx, s.DefinitionRevision.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }

                if (s.Episode is not null)
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "episode_unresolved");
                }

                if (s.Definition.Fingerprint.AsSpan().SequenceEqual(r.Definition.Fingerprint))
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "no_change");
                }

                if (tx.Now < r.Cutoff)
                {
                    return Rollback<ScheduleRegistration>(tx, StoreStatus.InvalidState, "cutoff_in_future");
                }

                // Forward cutover (13 §6.4): unmaterialized old occurrences through the cutoff are skipped by edit;
                // the high-water mark never moves back; materialized jobs keep their payload.
                var revision = s.DefinitionRevision + 1;
                await InsertDefinitionAsync(tx, s.ScheduleId, revision, r.Definition, r.Cutoff, r.Actor.Subject).ConfigureAwait(false);
                var through = s.EvaluatedThrough is { } e && e > r.Cutoff ? e : r.Cutoff;
                if (s.NextDueAt is { } pending && pending <= r.Cutoff)
                {
                    await AuditRangesAsync(tx, s.ScheduleId, s.DefinitionRevision,
                        [new ScheduleAuditRange(s.EvaluatedThrough, r.Cutoff, "skipped_by_edit")]).ConfigureAwait(false);
                }

                using (var u = tx.Sql("""
                    WITH n AS (SELECT pg_notify('runwick_schedule', @application || '/' || @key))
                    UPDATE runwick.schedules
                    SET definition_revision = @revision, evaluated_through = @through, next_due_at = @next,
                        scan_after_at = COALESCE(@next, @now), needs_review = false, review_reason = NULL,
                        resume_policy = NULL, resume_cutoff = NULL, cursor_revision = cursor_revision + 1,
                        updated_at = @now, updated_by = @subject
                    WHERE application = @application AND schedule_id = @id AND EXISTS (SELECT FROM n)
                    """).P("revision", revision).P("through", through).P("next", r.FirstDueAt).P("now", tx.Now)
                        .P("subject", r.Actor.Subject).P("id", s.ScheduleId).P("key", s.Key))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var result = new ScheduleRegistration(s.ScheduleId, "updated", revision, r.Definition.Fingerprint);
                await ScheduleEventAsync(tx, "schedule_updated", s, r.Actor, new JsonObject
                {
                    ["definitionRevision"] = revision, ["cutoff"] = PgTime.Ms(r.Cutoff),
                    ["definitionFingerprint"] = Convert.ToHexStringLower(r.Definition.Fingerprint),
                }).ConfigureAwait(false);
                await AuditAsync(tx, "schedule", s.Key, r.Actor, s.DefinitionRevision, revision, new JsonObject
                {
                    ["previousFingerprint"] = Convert.ToHexStringLower(s.Definition.Fingerprint),
                    ["definitionFingerprint"] = Convert.ToHexStringLower(r.Definition.Fingerprint),
                }).ConfigureAwait(false);
                return new PgOutcome<ScheduleRegistration>.Commit(StoreReply<ScheduleRegistration>.Applied(result, tx.Now),
                    ScheduleReceiptCodec.Registration(result));
            },
            (tx, row) => Task.FromResult(StoreReply<ScheduleRegistration>.Applied(
                ScheduleReceiptCodec.ReadRegistration(row.Result!), row.DecidedAt!.Value, replayed: true)),
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ TriggerSchedule/1 (13 §6.5)

    public async Task<StoreReply<ScheduleMaterialization>> TriggerScheduleAsync(StoreCommand<TriggerScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return Invalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.TriggerSchedule, ScheduleEnvelopes.Trigger(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.TriggerSchedule, fp),
            async tx =>
            {
                if (!await SchedulerActiveAsync(tx).ConfigureAwait(false))
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(Invalid<ScheduleMaterialization>(StoreStatus.UnsupportedVersion, "scheduler_not_active"));
                }

                var s = await LockScheduleAsync(tx, null, r.Key).ConfigureAwait(false);
                var rejection = s is null ? (StoreStatus.NotFound, "schedule_not_found", (string?)null)
                    : !s.Enabled ? (StoreStatus.InvalidState, "schedule_disabled", null)
                    : s.DefinitionRevision != r.ExpectedDefinitionRevision
                        ? (StoreStatus.VersionConflict, "revision_conflict", "actual=" + s.DefinitionRevision.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    : default((StoreStatus, string, string?)?);
                if (rejection is { } x)
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(Invalid<ScheduleMaterialization>(x.Item1, x.Item2, x.Item3));
                }

                // The same trigger ID returns the existing occurrence, never new work (13 §6.5).
                var key = r.TriggerId.ToString("D");
                using (var q = tx.Sql("""
                    SELECT job_id, disposition FROM runwick.schedule_occurrences
                    WHERE application = @application AND schedule_id = @id AND occurrence_kind = 'manual' AND occurrence_key = @key
                    """).P("id", s!.ScheduleId).P("key", key))
                {
                    var existing = await tx.QueryAsync(q, o => (JobId: o.IsDBNull(0) ? (Guid?)null : o.GetGuid(0), Disposition: o.GetString(1))).ConfigureAwait(false);
                    if (existing.Count == 1)
                    {
                        if (!await tx.DecideAsync().ConfigureAwait(false))
                        {
                            return Rollback<ScheduleMaterialization>(tx, StoreStatus.CommandExpired, "command_expired");
                        }

                        var prior = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision,
                            [new MaterializedOccurrence(ScheduleOperations.ManualKind, key, existing[0].JobId, existing[0].Disposition)]);
                        return new PgOutcome<ScheduleMaterialization>.Commit(StoreReply<ScheduleMaterialization>.Applied(prior, tx.Now),
                            ScheduleReceiptCodec.Materialization(prior));
                    }
                }

                var (jobs, refused) = await DispatchScheduledAsync<ScheduleMaterialization>(tx, s, [(r.ProposedJobId, Occurrence(s, default, null, null))])
                    .ConfigureAwait(false);
                if (refused is not null)
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(refused);
                }

                if (!await InsertOccurrencesAsync(tx, s.ScheduleId, s.DefinitionRevision,
                    [(ScheduleOperations.ManualKind, key, null, jobs[0].JobId, jobs[0].Disposition, false, null, null)]).ConfigureAwait(false))
                {
                    return Rollback<ScheduleMaterialization>(tx, StoreStatus.InvariantFault, "occurrence_exists");
                }

                var result = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision,
                    [new MaterializedOccurrence(ScheduleOperations.ManualKind, key, jobs[0].JobId, jobs[0].Disposition)]);
                await ScheduleEventAsync(tx, "schedule_triggered", s, r.Actor, new JsonObject
                {
                    ["triggerId"] = key, ["jobId"] = jobs[0].JobId.ToString("D"), ["disposition"] = jobs[0].Disposition, ["paused"] = s.Paused,
                }).ConfigureAwait(false);
                await AuditAsync(tx, "schedule", s.Key, r.Actor, s.DefinitionRevision, s.DefinitionRevision, new JsonObject
                {
                    ["triggerId"] = key, ["jobId"] = jobs[0].JobId.ToString("D"),
                }).ConfigureAwait(false);
                return new PgOutcome<ScheduleMaterialization>.Commit(StoreReply<ScheduleMaterialization>.Applied(result, tx.Now),
                    ScheduleReceiptCodec.Materialization(result));
            },
            MaterializationReplay,
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ ResolveCatchUp/1 (13 §6.6)

    public async Task<StoreReply<ScheduleMaterialization>> ResolveCatchUpAsync(StoreCommand<ResolveCatchUpRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor)
            ?? (r.Resolution is not ("skip" or "extend" or "run_once") ? "resolution" : null)
            ?? (r.Resolution == "extend" && r.AdditionalJobs is < 1 or > ScheduleBounds.MaxEpisodeJobs ? "additionalJobs" : null);
        if (invalid is not null)
        {
            return Invalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.ResolveCatchUp, ScheduleEnvelopes.Resolve(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.ResolveCatchUp, fp),
            async tx =>
            {
                var s = await LockScheduleAsync(tx, null, r.Key).ConfigureAwait(false);
                if (s is null || s.Episode is null || s.Episode.Id != r.EpisodeId)
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(s is null
                        ? Invalid<ScheduleMaterialization>(StoreStatus.NotFound, "schedule_not_found")
                        : Invalid<ScheduleMaterialization>(StoreStatus.VersionConflict, "revision_conflict", "episode=" + s.Episode?.Id.ToString("D")));
                }

                var episode = s.Episode;
                if (r.Resolution == "extend" && episode.Limit + r.AdditionalJobs > ScheduleBounds.MaxEpisodeJobs)
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(Invalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", "additionalJobs"));
                }

                if (r.NextDueAfterCutoff is { } nd && nd <= episode.Cutoff)
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(Invalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", "nextDueAfterCutoff"));
                }

                var runOnce = r.Resolution == "run_once" && s.NextDueAt is not null;
                var (jobs, refused) = await DispatchScheduledAsync<ScheduleMaterialization>(tx, s,
                    runOnce ? [(r.ProposedJobId, Occurrence(s, s.NextDueAt!.Value, s.NextDueAt, episode.Cutoff))] : []).ConfigureAwait(false);
                if (refused is not null)
                {
                    return new PgOutcome<ScheduleMaterialization>.Rollback(refused);
                }

                var occurrences = new List<MaterializedOccurrence>();
                if (runOnce)
                {
                    var key = ScheduleOperations.OccurrenceKey(s.NextDueAt!.Value);
                    if (!await InsertOccurrencesAsync(tx, s.ScheduleId, s.DefinitionRevision,
                        [(ScheduleOperations.ScheduledKind, key, s.NextDueAt, jobs[0].JobId, jobs[0].Disposition, false, s.NextDueAt, episode.Cutoff)])
                        .ConfigureAwait(false))
                    {
                        return Rollback<ScheduleMaterialization>(tx, StoreStatus.InvariantFault, "occurrence_exists");
                    }

                    occurrences.Add(new MaterializedOccurrence(ScheduleOperations.ScheduledKind, key, jobs[0].JobId, jobs[0].Disposition));
                }

                if (r.Resolution == "extend")
                {
                    using var u = tx.Sql("""
                        UPDATE runwick.schedules
                        SET episode_limit = episode_limit + @add, needs_review = false, review_reason = NULL, scan_after_at = @now,
                            cursor_revision = cursor_revision + 1, updated_at = @now, updated_by = @subject
                        WHERE application = @application AND schedule_id = @id
                        """).P("add", r.AdditionalJobs).P("now", tx.Now).P("subject", r.Actor.Subject).P("id", s.ScheduleId);
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }
                else
                {
                    if (s.NextDueAt is not null && s.NextDueAt <= episode.Cutoff)
                    {
                        await AuditRangesAsync(tx, s.ScheduleId, s.DefinitionRevision,
                            [new ScheduleAuditRange(s.EvaluatedThrough, episode.Cutoff, runOnce ? "run_once_covered" : "catch_up_skipped")]).ConfigureAwait(false);
                    }

                    using var u = tx.Sql("""
                        UPDATE runwick.schedules
                        SET evaluated_through = GREATEST(evaluated_through, @cutoff), next_due_at = @next,
                            scan_after_at = COALESCE(@next, @now), episode_id = NULL, episode_cutoff = NULL, episode_emitted = 0,
                            episode_limit = NULL, needs_review = false, review_reason = NULL,
                            cursor_revision = cursor_revision + 1, updated_at = @now, updated_by = @subject
                        WHERE application = @application AND schedule_id = @id
                        """).P("cutoff", episode.Cutoff).P("next", r.NextDueAfterCutoff).P("now", tx.Now).P("subject", r.Actor.Subject)
                            .P("id", s.ScheduleId);
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var result = new ScheduleMaterialization(s.ScheduleId, s.CursorRevision + 1, occurrences);
                await ScheduleEventAsync(tx, "catch_up_resolved", s, r.Actor, new JsonObject
                {
                    ["episodeId"] = episode.Id.ToString("D"), ["resolution"] = r.Resolution, ["additionalJobs"] = r.AdditionalJobs,
                    ["jobs"] = occurrences.Count,
                }).ConfigureAwait(false);
                await AuditAsync(tx, "schedule", s.Key, r.Actor, s.CursorRevision, result.CursorRevision, new JsonObject
                {
                    ["episodeId"] = episode.Id.ToString("D"), ["resolution"] = r.Resolution,
                }).ConfigureAwait(false);
                return new PgOutcome<ScheduleMaterialization>.Commit(StoreReply<ScheduleMaterialization>.Applied(result, tx.Now),
                    ScheduleReceiptCodec.Materialization(result));
            },
            MaterializationReplay,
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ RecordScheduleEvaluationFailure/1 (13 §6.7)

    public async Task<StoreReply<ScheduleState>> RecordEvaluationFailureAsync(StoreCommand<EvaluationFailureRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ValidateActor(r.Actor)
            ?? (string.IsNullOrEmpty(r.Code) || r.Code.Length > 64 || r.Code.Any(c => !(char.IsAsciiLetterOrDigit(c) || c == '_')) ? "code" : null)
            ?? (r.Detail is { } d && Encoding.UTF8.GetByteCount(d) > 1024 ? "detail" : null);
        if (invalid is not null)
        {
            return Invalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.RecordScheduleEvaluationFailure, ScheduleEnvelopes.Failure(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.RecordScheduleEvaluationFailure, fp),
            async tx =>
            {
                var s = await LockScheduleAsync(tx, r.ScheduleId, null).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (s is null)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.NotFound, "schedule_not_found");
                }

                if (s.DefinitionRevision != r.ExpectedDefinitionRevision)
                {
                    return Conflict<ScheduleState>(tx, s.DefinitionRevision.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }

                if (s.NeedsReview)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.InvalidState, "no_change");
                }

                // The cursor never moves on a failure (13 §6.7).
                using (var u = tx.Sql("""
                    UPDATE runwick.schedules SET needs_review = true, review_reason = @code, updated_at = @now
                    WHERE application = @application AND schedule_id = @id
                    """).P("code", r.Code).P("now", tx.Now).P("id", s.ScheduleId))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var state = StateOf(s, review: true);
                await ScheduleEventAsync(tx, "schedule_needs_review", s, r.Actor, new JsonObject { ["reason"] = r.Code, ["detail"] = r.Detail })
                    .ConfigureAwait(false);
                await AuditAsync(tx, "schedule", s.Key, r.Actor, s.DefinitionRevision, s.DefinitionRevision,
                    new JsonObject { ["code"] = r.Code, ["detail"] = r.Detail }).ConfigureAwait(false);
                return new PgOutcome<ScheduleState>.Commit(StoreReply<ScheduleState>.Applied(state, tx.Now), ScheduleReceiptCodec.State(state));
            },
            StateReplay,
            ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ RevalidateSchedule/1 (13 §6.8)

    public async Task<StoreReply<ScheduleState>> RevalidateScheduleAsync(StoreCommand<RevalidateScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ValidateActor(r.Actor) is { } invalid)
        {
            return Invalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var fp = await FingerprintAsync(command, ScheduleOperations.RevalidateSchedule, ScheduleEnvelopes.Revalidate(r), ct).ConfigureAwait(false);
        return await runner.ExecuteAsync(OpFor(command, ScheduleOperations.RevalidateSchedule, fp),
            async tx =>
            {
                var s = await LockScheduleAsync(tx, null, r.Key).ConfigureAwait(false);
                if (!await tx.DecideAsync().ConfigureAwait(false))
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.CommandExpired, "command_expired");
                }

                if (s is null)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.NotFound, "schedule_not_found");
                }

                if (s.DefinitionRevision != r.ExpectedDefinitionRevision)
                {
                    return Conflict<ScheduleState>(tx, s.DefinitionRevision.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }

                if (s.Episode is not null)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.InvalidState, "episode_unresolved");
                }

                if (!s.NeedsReview)
                {
                    return Rollback<ScheduleState>(tx, StoreStatus.InvalidState, "no_change");
                }

                using (var u = tx.Sql("""
                    UPDATE runwick.schedules
                    SET needs_review = false, review_reason = NULL, scan_after_at = @now, updated_at = @now, updated_by = @subject
                    WHERE application = @application AND schedule_id = @id
                    """).P("now", tx.Now).P("subject", r.Actor.Subject).P("id", s.ScheduleId))
                {
                    Expect(await tx.ExecAsync(u).ConfigureAwait(false), 1);
                }

                var state = StateOf(s, review: false);
                await ScheduleEventAsync(tx, "schedule_revalidated", s, r.Actor, new JsonObject { ["previousReason"] = s.ReviewReason })
                    .ConfigureAwait(false);
                await AuditAsync(tx, "schedule", s.Key, r.Actor, s.DefinitionRevision, s.DefinitionRevision,
                    new JsonObject { ["previousReason"] = s.ReviewReason }).ConfigureAwait(false);
                return new PgOutcome<ScheduleState>.Commit(StoreReply<ScheduleState>.Applied(state, tx.Now), ScheduleReceiptCodec.State(state));
            },
            StateReplay,
            ct).ConfigureAwait(false);
    }
}
