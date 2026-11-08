using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Runwick.Protocol;
using Runwick.Serialization;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>
/// Schedules (13 §6, 22 §7): the C# side builds the arguments (the template items are built from the schedule read just before,
/// because their digests depend on the command), the sched_* scripts decide with the server clock. Materialization creates its jobs
/// through the shared dispatch core of the same script call.
/// </summary>
internal sealed partial class RedisStore : IScheduleStore
{
    static readonly ControlActor SchActor = new("svc:scheduler", "Runwick scheduler", "runwick", null);

    // ------------------------------------------------------------------ definition and snapshot JSON

    static JsonObject SchTemplateJson(ScheduleTemplate t)
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
            ["jobName"] = t.JobName,
            ["payload"] = Convert.ToBase64String(t.PayloadUtf8),
            ["payloadVersion"] = t.PayloadVersion,
            ["policy"] = ReceiptCodec.Policy(t.Policy),
            ["priority"] = t.Priority,
            ["profileId"] = t.SerializerProfileId,
            ["profileVersion"] = t.SerializerProfileVersion,
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

    static ScheduleTemplate SchReadTemplate(JsonObject o) =>
        new(o["jobName"]!.GetValue<string>(), o["payloadVersion"]!.GetValue<int>(), o["profileId"]!.GetValue<string>(), o["profileVersion"]!.GetValue<int>(),
            Convert.FromBase64String(o["payload"]!.GetValue<string>()), o["queue"]!.GetValue<string>(), o["priority"]!.GetValue<string>(),
            ReceiptCodec.ReadPolicy(o["policy"]!.AsObject()),
            [.. o["scopes"]!.AsArray().Select(s => new ScopeRequirement(s!["kind"]!.GetValue<string>(), s["key"]!.GetValue<string>(), s["capacity"]!.GetValue<int>()))],
            [.. o["rates"]!.AsArray().Select(r => new RateRequirement(r!["kind"]!.GetValue<string>(), r["key"]!.GetValue<string>(),
                r["limit"]!.GetValue<int>(), r["windowMs"]!.GetValue<long>()))],
            o["activeKey"]?.GetValue<string>(),
            o["businessMetadata"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>(), StringComparer.Ordinal));

    static JsonObject SchDefinitionJson(ScheduleDefinitionRecord d) => new()
    {
        ["allowReuse"] = d.AllowReuse,
        ["cronDialect"] = d.CronDialect,
        ["cronText"] = d.CronText,
        ["dueAt"] = d.DueAt?.ToUnixTimeMilliseconds(),
        ["evaluatorProfile"] = d.EvaluatorProfile,
        ["fingerprint"] = Convert.ToHexStringLower(d.Fingerprint),
        ["foldPolicy"] = d.FoldPolicy,
        ["gapPolicy"] = d.GapPolicy,
        ["kind"] = d.Kind,
        ["maxBatch"] = d.MaxBatch,
        ["maxCatchUpAgeMs"] = d.MaxCatchUpAgeMs,
        ["maxEpisodeJobs"] = d.MaxEpisodeJobs,
        ["misfireGraceMs"] = d.MisfireGraceMs,
        ["misfirePolicy"] = d.MisfirePolicy,
        ["overlapPolicy"] = d.OverlapPolicy,
        ["template"] = SchTemplateJson(d.Template),
        ["tzdbSha256"] = d.TzdbSha256 is null ? null : Convert.ToHexStringLower(d.TzdbSha256),
        ["tzdbVersion"] = d.TzdbVersion,
        ["zoneId"] = d.ZoneId,
    };

    static ScheduleDefinitionRecord SchReadDefinition(JsonObject o) =>
        new(o["kind"]!.GetValue<string>(), SchTn(o["dueAt"]), o["cronText"]?.GetValue<string>(), o["cronDialect"]?.GetValue<string>(),
            o["zoneId"]?.GetValue<string>(), o["evaluatorProfile"]?.GetValue<string>(), o["tzdbVersion"]?.GetValue<string>(),
            o["tzdbSha256"] is { } sha ? Convert.FromHexString(sha.GetValue<string>()) : null, o["gapPolicy"]!.GetValue<string>(),
            o["foldPolicy"]!.GetValue<string>(), o["misfirePolicy"]!.GetValue<string>(), o["misfireGraceMs"]!.GetValue<long>(),
            o["maxBatch"]!.GetValue<int>(), o["maxEpisodeJobs"]!.GetValue<int>(), o["maxCatchUpAgeMs"]!.GetValue<long>(),
            o["overlapPolicy"]!.GetValue<string>(), o["allowReuse"]!.GetValue<bool>(), SchReadTemplate(o["template"]!.AsObject()),
            Convert.FromHexString(o["fingerprint"]!.GetValue<string>()));

    static DateTimeOffset? SchTn(JsonNode? n) => n is null ? null : DateTimeOffset.FromUnixTimeMilliseconds(n.GetValue<long>());

    static DateTimeOffset SchT(JsonNode? n) => DateTimeOffset.FromUnixTimeMilliseconds(n!.GetValue<long>());

    static ScheduleSnapshot SchSnapshot(string rowJson, string definitionJson)
    {
        var r = JsonNode.Parse(rowJson)!.AsObject();
        var e = r["ep"] as JsonObject;
        return new ScheduleSnapshot(
            Guid.Parse(r["id"]!.GetValue<string>()), r["key"]!.GetValue<string>(), r["drev"]!.GetValue<long>(), r["crev"]!.GetValue<long>(),
            r["curev"]!.GetValue<long>(), r["enabled"]!.GetValue<bool>(), r["paused"]!.GetValue<bool>(), r["review"]!.GetValue<bool>(),
            r["reason"]?.GetValue<string>(), SchTn(r["next"]), SchT(r["scan"]), SchTn(r["through"]),
            e is null ? null : new ScheduleEpisode(Guid.Parse(e["id"]!.GetValue<string>()), SchT(e["cutoff"]), e["emitted"]!.GetValue<int>(), e["limit"]!.GetValue<int>()),
            r["rpol"]?.GetValue<string>(), SchTn(r["rcut"]), SchReadDefinition(JsonNode.Parse(definitionJson)!.AsObject()));
    }

    static JsonObject SchActorJson(ControlActor a) => new()
    {
        ["subject"] = a.Subject, ["display"] = a.Display, ["scheme"] = a.Scheme, ["reason"] = a.Reason,
    };

    // ------------------------------------------------------------------ validation (13 §3, §10; as the other stores)

    static string? SchValidateDefinition(ScheduleDefinitionRecord d)
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

    /// <summary>08 §2 actor bounds; returns the offending field or null.</summary>
    internal static string? ActorProblem(ControlActor actor)
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

    StoreReply<T> SchInvalid<T>(StoreStatus status, string code, string? detail = null) where T : class =>
        StoreReply<T>.NoEffect(status, new StoreError(code, Detail: detail), time.GetUtcNow());

    // ------------------------------------------------------------------ reads (13 §6.9)

    public async Task<DueSchedules> ReadDueSchedulesAsync(string application, int limit, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, ScheduleBounds.DueReadLimit);
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("sched_due"), RxLane.Background, [limit], ct).ConfigureAwait(false);
        if ((string)a[0]! != "OK" || a.Length < 3)
        {
            return new DueSchedules(time.GetUtcNow(), []);
        }

        Seen(application, (string)a[1]!);
        var list = new List<ScheduleSnapshot>();
        for (var i = 3; i + 1 < a.Length; i += 2)
        {
            list.Add(SchSnapshot((string)a[i]!, (string)a[i + 1]!));
        }

        return new DueSchedules(At(a[2]), list);
    }

    public Task<ScheduleSnapshot?> ReadScheduleAsync(string application, string key, CancellationToken ct) => SchReadAsync(application, key, false, ct);

    async Task<ScheduleSnapshot?> SchReadAsync(string application, string keyOrId, bool byId, CancellationToken ct)
    {
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read("sched_read"), RxLane.Background, [keyOrId, byId ? "1" : "0"], ct).ConfigureAwait(false);
        if ((string)a[0]! != "OK" || a.Length < 4)
        {
            return null;
        }

        Seen(application, (string)a[1]!);
        return SchSnapshot((string)a[2]!, (string)a[3]!);
    }

    /// <summary>Store time is the Redis clock (22 §4.1 step 2), ms floor.</summary>
    public async Task<DateTimeOffset> StoreNowAsync(CancellationToken ct)
    {
        var db = await runner.Client.DbAsync(RxLane.Background).ConfigureAwait(false);
        try
        {
            var t = (RedisResult[])(await db.ExecuteAsync("TIME").ConfigureAwait(false))!;
            return DateTimeOffset.FromUnixTimeMilliseconds(((long)t[0]) * 1000 + ((long)t[1]) / 1000);
        }
        catch (RedisException ex)
        {
            throw new StoreNotAppliedException("store_unavailable", ex);
        }
    }

    // ------------------------------------------------------------------ commands

    async Task<StoreReply<T>> SchWriteAsync<T>(string application, Guid commandId, string operation, DateTimeOffset notAfter, CanonicalValue body, string script,
        Func<RxReply, T> read, CancellationToken ct, params RedisValue[] args) where T : class
    {
        var storeId = await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var fp = CommandEnvelopes.Fingerprint(storeId, application, operation, commandId, notAfter, body);
        var r = await Write(Op(application, commandId, operation, notAfter, fp, RxLane.Background, GateOf(operation)), script, ct, args).ConfigureAwait(false);
        return r.HasValue ? r.Applied(read(r)) : r.Reject<T>();
    }

    static ScheduleRegistration SchReadRegistration(RxReply r) =>
        new(Guid.Parse(r.Result!["scheduleId"]!.GetValue<string>()), r.Result["outcome"]!.GetValue<string>(), r.Result["definitionRevision"]!.GetValue<long>(),
            Convert.FromHexString(r.Result["storedFingerprint"]!.GetValue<string>()));

    static ScheduleMaterialization SchReadMaterialization(RxReply r) =>
        new(Guid.Parse(r.Result!["scheduleId"]!.GetValue<string>()), r.Result["cursorRevision"]!.GetValue<long>(),
            [.. r.Result["occurrences"]!.AsArray().Select(x => new MaterializedOccurrence(x!["kind"]!.GetValue<string>(), x["key"]!.GetValue<string>(),
                x["jobId"] is { } j ? Guid.Parse(j.GetValue<string>()) : null, x["disposition"]!.GetValue<string>()))]);

    static ScheduleState SchReadState(RxReply r) =>
        new(r.Result!["key"]!.GetValue<string>(), r.Result["paused"]!.GetValue<bool>(), r.Result["needsReview"]!.GetValue<bool>(),
            r.Result["definitionRevision"]!.GetValue<long>(), r.Result["controlRevision"]!.GetValue<long>(), r.Result["cursorRevision"]!.GetValue<long>());

    static string? SchMs(DateTimeOffset? t) => t is { } v ? Ms(v) : null;

    public Task<StoreReply<ScheduleRegistration>> RegisterScheduleAsync(StoreCommand<RegisterScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = (string.IsNullOrEmpty(r.Key) || Encoding.UTF8.GetByteCount(r.Key) > 512 || r.Key.Any(char.IsControl) ? "key" : null)
            ?? SchValidateDefinition(r.Definition)
            ?? (r.FirstDueAt is { } due && due <= r.Cutoff && r.Definition.Kind == ScheduleOperations.Cron ? "firstDueAt" : null);
        if (invalid is not null)
        {
            return Task.FromResult(SchInvalid<ScheduleRegistration>(StoreStatus.InvalidRequest, "invalid_request", invalid));
        }

        return SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.RegisterSchedule, command.NotAfter, ScheduleEnvelopes.Register(r),
            "sched_register", SchReadRegistration, ct, r.Key, SchMs(r.FirstDueAt) ?? "", Ms(r.Cutoff), r.Definition.Kind == ScheduleOperations.Cron ? "1" : "0",
            Convert.ToHexStringLower(r.Definition.Fingerprint), Json(SchDefinitionJson(r.Definition)), Hex(Guid.CreateVersion7()),
            r.Definition.Template.PayloadUtf8.Length);
    }

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
            return SchInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        // The items' digests depend on this command, so they are built from the definition read now; the script rejects the plan
        // if the revisions it holds are not the expected ones (revisions only grow, so a match means the same definition).
        var s = r.Occurrences.Count == 0 ? null : await SchReadAsync(command.Application, r.ScheduleId.ToString("N"), true, ct).ConfigureAwait(false);
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var items = new List<RedisValue>();
        var count = 0;
        if (s is not null && s.DefinitionRevision == r.ExpectedDefinitionRevision
            && (long)r.Occurrences.Count * s.Definition.Template.PayloadUtf8.Length <= ScheduleBounds.MaxPlanPayloadBytes)
        {
            foreach (var o in r.Occurrences)
            {
                var item = s.Definition.Template.ToItem(storeId, command.Application, command.CommandId, command.NotAfter.ToUnixTimeMilliseconds(), s.ScheduleId,
                    s.Definition.Serialize, o.ProposedJobId, new JobScheduleInfo(s.Key, o.ScheduledAt, o.CoveredFrom ?? o.ScheduledAt, o.CoveredThrough ?? o.ScheduledAt));
                items.Add(Json(ItemJson(item)));
                items.Add(item.Intent.PayloadUtf8);
                count++;
            }
        }

        var a = r.After;
        var plan = new JsonObject
        {
            ["id"] = Hex(r.ScheduleId), ["drev"] = r.ExpectedDefinitionRevision, ["crev"] = r.ExpectedControlRevision, ["curev"] = r.ExpectedCursorRevision,
            ["ep"] = r.ExpectedEpisodeId is { } ee ? Hex(ee) : null, ["cutoff"] = r.Cutoff.ToUnixTimeMilliseconds(),
            ["occ"] = new JsonArray([.. r.Occurrences.Select(o => (JsonNode)new JsonObject
            {
                ["at"] = o.ScheduledAt.ToUnixTimeMilliseconds(), ["from"] = o.CoveredFrom?.ToUnixTimeMilliseconds(),
                ["through"] = o.CoveredThrough?.ToUnixTimeMilliseconds(), ["coal"] = o.Coalesced,
            })]),
            ["after"] = new JsonObject
            {
                ["through"] = a.EvaluatedThrough?.ToUnixTimeMilliseconds(), ["next"] = a.NextDueAt?.ToUnixTimeMilliseconds(), ["scan"] = a.ScanAfterAt.ToUnixTimeMilliseconds(),
                ["ep"] = a.Episode is { } e ? new JsonObject
                {
                    ["id"] = Hex(e.Id), ["cutoff"] = e.Cutoff.ToUnixTimeMilliseconds(), ["emitted"] = e.Emitted, ["limit"] = e.Limit,
                } : null,
                ["review"] = a.ReviewReason,
            },
            ["ranges"] = new JsonArray([.. r.AuditRanges.Select(g => (JsonNode)new JsonObject
            {
                ["from"] = g.FromExclusive?.ToUnixTimeMilliseconds(), ["through"] = g.ThroughInclusive.ToUnixTimeMilliseconds(), ["reason"] = g.Reason,
            })]),
            ["clear"] = r.ClearsPendingPolicy,
        };
        return await SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.MaterializeOccurrences, command.NotAfter,
            ScheduleEnvelopes.Materialize(r), "sched_materialize", SchReadMaterialization, ct, [Json(plan), count, .. items]).ConfigureAwait(false);
    }

    public Task<StoreReply<ScheduleState>> SetScheduleStateAsync(StoreCommand<ScheduleStateRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ActorProblem(r.Actor)
            ?? (r.Paused ? (r.ResumePolicy is not null ? "resumePolicy" : null)
                : r.ResumePolicy is not (ScheduleOperations.ResumeSkipPaused or ScheduleOperations.MisfireRunOnce or ScheduleOperations.MisfireCatchUp) ? "resumePolicy" : null);
        if (invalid is not null)
        {
            return Task.FromResult(SchInvalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid));
        }

        return SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.SetScheduleState, command.NotAfter, ScheduleEnvelopes.State(r),
            "sched_state", SchReadState, ct, r.Key, r.Paused ? "1" : "0", r.ResumePolicy ?? "", r.ExpectedControlRevision, Json(SchActorJson(r.Actor)));
    }

    public Task<StoreReply<ScheduleRegistration>> UpdateScheduleAsync(StoreCommand<UpdateScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ActorProblem(r.Actor) ?? SchValidateDefinition(r.Definition)
            ?? (r.Definition.Kind == ScheduleOperations.OneTime && r.Definition.DueAt <= r.Cutoff ? "dueAt" : null)
            ?? (r.FirstDueAt is { } due && due <= r.Cutoff ? "firstDueAt" : null);
        if (invalid is not null)
        {
            return Task.FromResult(SchInvalid<ScheduleRegistration>(StoreStatus.InvalidRequest, "invalid_request", invalid));
        }

        return SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.UpdateSchedule, command.NotAfter, ScheduleEnvelopes.Update(r),
            "sched_update", SchReadRegistration, ct, r.Key, r.ExpectedDefinitionRevision, Ms(r.Cutoff), SchMs(r.FirstDueAt) ?? "",
            Convert.ToHexStringLower(r.Definition.Fingerprint), Json(SchDefinitionJson(r.Definition)), r.Definition.Template.PayloadUtf8.Length,
            Json(SchActorJson(r.Actor)));
    }

    public async Task<StoreReply<ScheduleMaterialization>> TriggerScheduleAsync(StoreCommand<TriggerScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ActorProblem(r.Actor) is { } invalid)
        {
            return SchInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var s = await SchReadAsync(command.Application, r.Key, false, ct).ConfigureAwait(false);
        var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var items = new List<RedisValue>();
        if (s is not null && s.DefinitionRevision == r.ExpectedDefinitionRevision)
        {
            // Default times mark a manual trigger; the script stamps its header time with the server clock (13 §6.5).
            var item = s.Definition.Template.ToItem(storeId, command.Application, command.CommandId, command.NotAfter.ToUnixTimeMilliseconds(), s.ScheduleId,
                s.Definition.Serialize, r.ProposedJobId, new JobScheduleInfo(s.Key, default, default, default));
            items.Add(Json(ItemJson(item)));
            items.Add(item.Intent.PayloadUtf8);
        }

        return await SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.TriggerSchedule, command.NotAfter, ScheduleEnvelopes.Trigger(r),
            "sched_trigger", SchReadMaterialization, ct,
            [r.Key, r.ExpectedDefinitionRevision, r.TriggerId.ToString("D"), Json(SchActorJson(r.Actor)), items.Count / 2, .. items]).ConfigureAwait(false);
    }

    public async Task<StoreReply<ScheduleMaterialization>> ResolveCatchUpAsync(StoreCommand<ResolveCatchUpRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ActorProblem(r.Actor)
            ?? (r.Resolution is not ("skip" or "extend" or "run_once") ? "resolution" : null)
            ?? (r.Resolution == "extend" && r.AdditionalJobs is < 1 or > ScheduleBounds.MaxEpisodeJobs ? "additionalJobs" : null);
        if (invalid is not null)
        {
            return SchInvalid<ScheduleMaterialization>(StoreStatus.InvalidRequest, "invalid_request", invalid);
        }

        var items = new List<RedisValue>();
        var nextDue = "";
        if (r.Resolution == "run_once")
        {
            var s = await SchReadAsync(command.Application, r.Key, false, ct).ConfigureAwait(false);
            if (s is { NextDueAt: { } next, Episode: { } episode } && episode.Id == r.EpisodeId)
            {
                var storeId = await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
                var item = s.Definition.Template.ToItem(storeId, command.Application, command.CommandId, command.NotAfter.ToUnixTimeMilliseconds(), s.ScheduleId,
                    s.Definition.Serialize, r.ProposedJobId, new JobScheduleInfo(s.Key, next, next, episode.Cutoff));
                items.Add(Json(ItemJson(item)));
                items.Add(item.Intent.PayloadUtf8);
                nextDue = Ms(next);
            }
        }

        return await SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.ResolveCatchUp, command.NotAfter, ScheduleEnvelopes.Resolve(r),
            "sched_resolve", SchReadMaterialization, ct,
            [r.Key, Hex(r.EpisodeId), r.Resolution, r.AdditionalJobs, SchMs(r.NextDueAfterCutoff) ?? "", Json(SchActorJson(r.Actor)), nextDue, items.Count / 2, .. items])
            .ConfigureAwait(false);
    }

    public Task<StoreReply<ScheduleState>> RecordEvaluationFailureAsync(StoreCommand<EvaluationFailureRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        var invalid = ActorProblem(r.Actor)
            ?? (string.IsNullOrEmpty(r.Code) || r.Code.Length > 64 || r.Code.Any(c => !(char.IsAsciiLetterOrDigit(c) || c == '_')) ? "code" : null)
            ?? (r.Detail is { } d && Encoding.UTF8.GetByteCount(d) > 1024 ? "detail" : null);
        if (invalid is not null)
        {
            return Task.FromResult(SchInvalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid));
        }

        var body = new JsonObject { ["id"] = Hex(r.ScheduleId), ["drev"] = r.ExpectedDefinitionRevision, ["code"] = r.Code, ["detail"] = r.Detail };
        return SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.RecordScheduleEvaluationFailure, command.NotAfter,
            ScheduleEnvelopes.Failure(r), "sched_failure", SchReadState, ct, Json(body), Json(SchActorJson(r.Actor)));
    }

    public Task<StoreReply<ScheduleState>> RevalidateScheduleAsync(StoreCommand<RevalidateScheduleRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ActorProblem(r.Actor) is { } invalid)
        {
            return Task.FromResult(SchInvalid<ScheduleState>(StoreStatus.InvalidRequest, "invalid_request", invalid));
        }

        return SchWriteAsync(command.Application, command.CommandId, ScheduleOperations.RevalidateSchedule, command.NotAfter, ScheduleEnvelopes.Revalidate(r),
            "sched_revalidate", SchReadState, ct, r.Key, r.ExpectedDefinitionRevision, Json(SchActorJson(r.Actor)));
    }
}
