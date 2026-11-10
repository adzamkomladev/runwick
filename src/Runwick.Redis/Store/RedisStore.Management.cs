using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json.Nodes;
using Runwick.Management;
using Runwick.Protocol;
using StackExchange.Redis;

namespace Runwick.Redis.Store;

/// <summary>
/// Management reads and RecordDisclosure/1 (16 §2, §3, §6; 22 §7.2). Reads are keyset pages from the <c>idx:created*</c> indexes
/// and the aggregate hashes in one read script; a page that would examine more than 5,000 entries is a statement timeout,
/// which the management session reports as <c>query_budget_exceeded</c>.
/// </summary>
internal sealed partial class RedisStore : IManagementQueryStore
{
    /// <summary>One management read script call: OK with the rows after the first three elements, or the matching exception.</summary>
    async Task<RedisResult[]> ManagementReplyAsync(string application, string sub, RedisValue[] args, CancellationToken ct)
    {
        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        var a = await runner.ReadAsync(application, RxScript.Read(sub == "events" ? "events" : "mread"), RxLane.Background,
            sub == "events" ? args : [sub, .. args], ct).ConfigureAwait(false);
        switch ((string)a[0]!)
        {
            case "OK":
                Seen(application, (string)a[1]!);
                return a;
            case "BUDGET":
                throw new StoreNotAppliedException("statement_timeout");
            case "CLOSED":
                throw new StoreNotAppliedException("store_maintenance");
            default:
                throw new StoreNotAppliedException((string)a[1]!);
        }
    }

    async Task<StoreRead<T>> ManagementReadAsync<T>(string application, string sub, RedisValue[] args, Func<RedisResult[], T> parse, CancellationToken ct)
    {
        var a = await ManagementReplyAsync(application, sub, args, ct).ConfigureAwait(false);
        return new StoreRead<T>(parse(a), At(a[2]));
    }

    static IEnumerable<JsonObject> Rows(RedisResult[] a) => a.Skip(3).Select(r => JsonNode.Parse((string)r!)!.AsObject());

    static string QueuesArg(IReadOnlyList<string>? queues) => queues is null ? "" : Json(new JsonArray([.. queues.Select(q => (JsonNode?)q)]));

    static DateTimeOffset MsTime(JsonNode? n) => DateTimeOffset.FromUnixTimeMilliseconds(n!.GetValue<long>());

    static DateTimeOffset? MsTimeN(JsonNode? n) => n is null ? null : MsTime(n);

    static string FloorMs(DateTimeOffset t) => t.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    static string CeilMsOf(DateTimeOffset t) =>
        ((t.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond).ToString(CultureInfo.InvariantCulture);

    static JobSummary ReadSummary(JsonNode o) => new(
        Guid.Parse(o["id"]!.GetValue<string>()), o["name"]!.GetValue<string>(), o["pver"]!.GetValue<int>(), o["queue"]!.GetValue<string>(),
        o["priority"]!.GetValue<string>(), o["state"]!.GetValue<string>(), o["gen"]!.GetValue<int>(), o["rev"]!.GetValue<long>(),
        MsTime(o["created"]), MsTime(o["updated"]), MsTime(o["eligible"]), MsTimeN(o["terminalAt"]), o["reason"]?.GetValue<string>(),
        o["cancel"]!.GetValue<bool>(), o["xcount"]?.GetValue<long>() ?? 0, MsTimeN(o["detailExpiredAt"]));

    public Task<StoreRead<IReadOnlyList<JobSummary>>> ListJobsAsync(string application, IReadOnlyList<string>? queues, JobListFilter filter,
        (DateTimeOffset CreatedAt, Guid JobId)? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<JobSummary>>(application, "jobs",
            [QueuesArg(queues), filter.Queue ?? "", filter.State ?? "", filter.JobName ?? "", CeilMsOf(filter.From), FloorMs(filter.To),
                after is { } a ? FloorMs(a.CreatedAt) : "", after is { } b ? Hex(b.JobId) : "", limit],
            r => [.. Rows(r).Select(ReadSummary)], ct);

    public async Task<StoreRead<JobDetail?>> GetJobAsync(string application, Guid jobId, CancellationToken ct) =>
        await ManagementReadAsync<JobDetail?>(application, "job", [Hex(jobId)], r =>
        {
            if (r.Length <= 3)
            {
                return null;
            }

            var o = JsonNode.Parse((string)r[3]!)!;
            var generations = o["generations"]!.AsArray().Select(g => new JobGenerationInfo(g!["generation"]!.GetValue<int>(), g["reason"]!.GetValue<string>(),
                g["by"]?.GetValue<string>(), MsTime(g["created"]), g["started"]?.GetValue<int>() ?? 0, g["max"]!.GetValue<int>())).ToList();
            var attempts = o["attempts"]!.AsArray().Select(x => new JobAttemptInfo(Guid.Parse(x!["id"]!.GetValue<string>()), x["generation"]!.GetValue<int>(),
                x["reservation"]!.GetValue<long>(), x["execution"]?.GetValue<int>(), Guid.Parse(x["worker"]!.GetValue<string>()), MsTime(x["reserved"]),
                MsTimeN(x["authorized"]), MsTimeN(x["finished"]), x["outcome"]?.GetValue<string>(), x["disposition"]?.GetValue<string>(),
                x["reason"]?.GetValue<string>(), x["errorCode"]?.GetValue<string>(), Runwick.Worker.ResourceUsageJson.Read(x["usage"]?.GetValue<string>()))).ToList();
            return new JobDetail(ReadSummary(o["job"]!), generations, attempts);
        }, ct).ConfigureAwait(false);

    public async Task<string?> QueueOfAsync(string application, Guid? jobId, Guid? attemptId, CancellationToken ct) =>
        (await ManagementReadAsync(application, "queueof", [jobId is { } j ? Hex(j) : "", attemptId is { } a ? Hex(a) : ""],
            r => r.Length > 3 ? (string?)r[3] : null, ct).ConfigureAwait(false)).Value;

    public async Task<DateTimeOffset?> CommandNotAfterAsync(string application, Guid commandId, CancellationToken ct) =>
        (await ManagementReadAsync(application, "notafter", [Hex(commandId)], r => r.Length > 3 ? (DateTimeOffset?)At(r[3]) : null, ct).ConfigureAwait(false)).Value;

    public Task<StoreRead<IReadOnlyList<QueueSummary>>> ListQueuesAsync(string application, IReadOnlyList<string>? queues, string? after, int limit,
        CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<QueueSummary>>(application, "queues", [QueuesArg(queues), after ?? "", limit],
            r => [.. Rows(r).Select(o => new QueueSummary(o["name"]!.GetValue<string>(), o["row"]!["paused"]!.GetValue<bool>(), o["row"]!["revision"]!.GetValue<long>()))], ct);

    public Task<StoreRead<IReadOnlyList<WorkerSummary>>> ListWorkersAsync(string application, bool includeExpired,
        (DateTimeOffset HeartbeatAt, Guid InstanceId)? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<WorkerSummary>>(application, "workers",
            [includeExpired ? "1" : "0", after is { } a ? FloorMs(a.HeartbeatAt) : "", after is { } b ? Hex(b.InstanceId) : "", limit],
            r => [.. Rows(r).Select(o =>
            {
                var w = o["row"]!;
                return new WorkerSummary(Guid.Parse(o["id"]!.GetValue<string>()), w["host"]!.GetValue<string>(), w["pid"]!.GetValue<int>(),
                    w["build"]!.GetValue<string>(), w["status"]!.GetValue<string>(), w["subscriptions"]!.DeepClone(), w["pools"]!.DeepClone(),
                    MsTime(w["started"]), MsTime(w["heartbeat"]), MsTime(w["expires"]),
                    w["detail"]?.GetValue<string>() is { } detail ? JsonNode.Parse(detail) : null);
            })], ct);

    static readonly TimeSpan StatsBudget = TimeSpan.FromSeconds(5);
    readonly ConcurrentDictionary<string, bool> _statsIndexed = new(StringComparer.Ordinal);

    /// <summary>
    /// Runs the StatsIndex walk to its end once per application and process: stores written by an older binary lack
    /// <c>idx:terminal</c> and the <c>jobtypes</c> counts (applications registered by this binary start complete).
    /// </summary>
    async Task EnsureStatsIndexAsync(string application, CancellationToken ct)
    {
        if (_statsIndexed.ContainsKey(application))
        {
            return;
        }

        await EnsureStoreIdAsync(application, ct).ConfigureAwait(false);
        while (true)
        {
            var r = await Write(Op(application, Guid.CreateVersion7(), "StatsIndex", time.GetUtcNow() + TimeSpan.FromSeconds(10), null, RxLane.Background, 'r'),
                "stats_index", ct, 500).ConfigureAwait(false);
            if (!r.HasValue)
            {
                throw new StoreNotAppliedException(r.Code ?? "store_unavailable");
            }

            if (r.Result!["done"]!.GetValue<bool>())
            {
                _statsIndexed[application] = true;
                return;
            }
        }
    }

    /// <summary>
    /// Exact by scan, in pages of at most 5,000 index entries: jobs created in range (<c>idx:created</c> or the queue's), jobs
    /// ended since From (<c>idx:terminal</c>; those before To are the ended set, all of them carry error codes) and live jobs
    /// (<c>idx:created:s:</c> of the four live states). Each page is atomic; the whole read is bounded by <see cref="StatsBudget"/>
    /// and answers <c>statement_timeout</c> past it, never a partial figure.
    /// </summary>
    public async Task<StoreRead<JobStatsCells>> GetJobStatsAsync(string application, JobStatsFilter f, CancellationToken ct)
    {
        await EnsureStatsIndexAsync(application, ct).ConfigureAwait(false);
        var deadline = time.GetUtcNow() + StatsBudget;
        var observed = time.GetUtcNow();
        var worker = f.Worker is { } w ? Hex(w) : "";
        string fromMs = FloorMs(f.From), toMs = FloorMs(f.To);

        async Task<List<JsonObject>> Scan(string key, string min, string max, bool errors)
        {
            var rows = new List<JsonObject>();
            string afterScore = "", afterId = "";
            while (true)
            {
                if (time.GetUtcNow() > deadline)
                {
                    throw new StoreNotAppliedException("statement_timeout");
                }

                var read = await ManagementReadAsync(application, "jscan", [key, min, max, afterScore, afterId, worker, errors ? fromMs : "", errors ? toMs : ""],
                    r => Rows(r).ToList(), ct).ConfigureAwait(false);
                observed = read.ObservedAt;
                if (read.Value.Count > 0 && read.Value[^1]["more"] is not null)
                {
                    var cursor = read.Value[^1];
                    (afterScore, afterId) = (cursor["sc"]!.ToJsonString(), cursor["id"]!.GetValue<string>());
                    rows.AddRange(read.Value.SkipLast(1));
                }
                else
                {
                    rows.AddRange(read.Value);
                    return rows;
                }
            }
        }

        bool Keep(JsonObject j, bool byWorker = true) =>
            f.Allows(j["q"]!.GetValue<string>()) && (f.JobName is null || j["n"]!.GetValue<string>() == f.JobName) && (!byWorker || f.Worker is null || j["w"]!.GetValue<bool>());

        var created = (await Scan(f.Queue is { } q ? "idx:created:q:" + q : "idx:created", fromMs, "(" + toMs, false).ConfigureAwait(false)).Where(j => Keep(j));
        var ended = await Scan("idx:terminal", fromMs, "+inf", true).ConfigureAwait(false);
        var live = new List<JsonObject>();
        foreach (var state in (string[])["pending", "retrying", "reserved", "running"])
        {
            live.AddRange(await Scan("idx:created:s:" + state, "-inf", "+inf", true).ConfigureAwait(false));
        }

        var now = observed;
        var cells = JobStatsCells.From(f,
            created.Select(j => (j["q"]!.GetValue<string>(), j["n"]!.GetValue<string>(), j["s"]!.GetValue<string>(), MsTime(j["c"]), j["x"]?.GetValue<long>() ?? 0,
                Guid.Parse(j["id"]!.GetValue<string>()))),
            ended.Where(j => Keep(j) && j["t"] is not null && f.Contains(MsTime(j["t"])) && j["s"]!.GetValue<string>() is "succeeded" or "failed" or "cancelled")
                .Select(j => (j["q"]!.GetValue<string>(), j["n"]!.GetValue<string>(), j["s"]!.GetValue<string>(), MsTime(j["t"]),
                    j["st"] is { } st ? (double?)(j["t"]!.GetValue<long>() - st.GetValue<long>()) : null)),
            live.Where(j => Keep(j)).Select(j =>
            {
                var state = j["s"]!.GetValue<string>();
                var eligible = MsTime(j["e"]);
                return (j["q"]!.GetValue<string>(), j["n"]!.GetValue<string>(), state is "reserved" or "running" ? state : eligible <= now ? "pending" : "scheduled",
                    eligible);
            }),
            ended.Concat(live).Where(j => Keep(j, byWorker: false)).SelectMany(j => j["err"]!.AsArray().Select(e => e!.GetValue<string>())));
        return new StoreRead<JobStatsCells>(cells, observed);
    }

    public async Task<StoreRead<JobNamesRead>> ListJobNamesAsync(string application, CancellationToken ct)
    {
        await EnsureStatsIndexAsync(application, ct).ConfigureAwait(false);
        return await ManagementReadAsync(application, "jobnames", [], r =>
        {
            var o = Rows(r).Single();
            return new JobNamesRead(
                [.. o["names"]!.AsArray().Select(x => (x![1]!.GetValue<string>(), x[0]!.GetValue<string>()))],
                [.. o["schedules"]!.AsArray().Select(x => (x![0]!.GetValue<string>(), x[1]!.GetValue<string>(), x[2]!.GetValue<string>()))]);
        }, ct).ConfigureAwait(false);
    }

    public Task<StoreRead<ScheduleStats>> GetScheduleStatsAsync(string application, DateTimeOffset from, DateTimeOffset to, CancellationToken ct) =>
        ManagementReadAsync(application, "sstats", [FloorMs(from), FloorMs(to)], r =>
        {
            var o = Rows(r).Single();
            return new ScheduleStats(from, to, o["total"]!.GetValue<long>(), o["paused"]!.GetValue<long>(), o["review"]!.GetValue<long>(), MsTimeN(o["next"]),
                o["occ"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<long>(), StringComparer.Ordinal), o["misfires"]!.GetValue<long>());
        }, ct);

    /// <summary>Pages of at most 5,000 audit rows, newest first, merged; bounded by <see cref="StatsBudget"/> like the job stats.</summary>
    public async Task<StoreRead<AuditStats>> GetAuditStatsAsync(string application, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var deadline = time.GetUtcNow() + StatsBudget;
        var ops = new Dictionary<string, long>(StringComparer.Ordinal);
        var actors = new Dictionary<string, long>(StringComparer.Ordinal);
        var before = "";
        while (true)
        {
            if (time.GetUtcNow() > deadline)
            {
                throw new StoreNotAppliedException("statement_timeout");
            }

            var read = await ManagementReadAsync(application, "astats", [FloorMs(from), FloorMs(to), before], r => Rows(r).Single(), ct).ConfigureAwait(false);
            foreach (var (map, field) in ((Dictionary<string, long>, string)[])[(ops, "ops"), (actors, "actors")])
            {
                foreach (var p in read.Value[field]!.AsObject())
                {
                    map[p.Key] = map.GetValueOrDefault(p.Key) + p.Value!.GetValue<long>();
                }
            }

            if (read.Value["next"] is not { } next)
            {
                return new StoreRead<AuditStats>(new AuditStats(from, to, ops.Values.Sum(), [.. ops.Select(p => new AuditOperationCount(p.Key, p.Value))],
                    [.. actors.Select(p => new AuditActorCount(p.Key, p.Value))], null), read.ObservedAt);
            }

            // The next page starts at the row this one stopped before (exclusive bound is that row's successor).
            before = (long.Parse(next.GetValue<string>(), CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);
        }
    }

    public Task<StoreRead<ScheduleDetail?>> GetScheduleAsync(string application, string key, int limit, CancellationToken ct) =>
        ManagementReadAsync(application, "schedule", [key, limit], r =>
        {
            if (r.Length <= 4)
            {
                return null;
            }

            var s = JsonNode.Parse((string)r[3]!)!;
            var d = SchReadDefinition(JsonNode.Parse((string)r[4]!)!.AsObject());
            var revision = s["drev"]!.GetValue<long>();
            return (ScheduleDetail?)new ScheduleDetail(
                new ScheduleSummary(s["key"]!.GetValue<string>(), s["paused"]!.GetValue<bool>(), s["review"]!.GetValue<bool>(), revision,
                    s["crev"]!.GetValue<long>(), MsTimeN(s["next"]), MsTimeN(s["mat"])),
                new ScheduleDefinitionInfo(revision, d.Kind, d.CronText, d.CronDialect, d.ZoneId, d.DueAt, d.Template.JobName, d.Template.Queue,
                    d.MisfirePolicy, d.MisfireGraceMs, d.OverlapPolicy, d.GapPolicy, d.FoldPolicy, d.MaxBatch, d.MaxEpisodeJobs, d.MaxCatchUpAgeMs),
                [.. r.Skip(5).Select(x => JsonNode.Parse((string)x!)!).Select(o => new ScheduleOccurrenceInfo(o["kind"]!.GetValue<string>(),
                    o["key"]!.GetValue<string>(), MsTimeN(o["at"]), MsTime(o["mat"]), o["disp"]!.GetValue<string>(), o["coal"]!.GetValue<bool>(),
                    o["job"] is { } job ? Guid.Parse(job.GetValue<string>()) : null, o["state"]?.GetValue<string>()))]);
        }, ct);

    public Task<StoreRead<IReadOnlyList<ScopeSummary>>> ListScopesAsync(string application, string? kind, (string Kind, string Key)? after, int limit,
        CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScopeSummary>>(application, "scopes", [kind ?? "", after?.Kind ?? "", after?.Key ?? "", limit],
            r => [.. Rows(r).Select(o =>
            {
                var s = o["row"]!;
                return new ScopeSummary(s["kind"]!.GetValue<string>(), s["key"]!.GetValue<string>(), s["capacity"]!.GetValue<int>(),
                    s["prev"]!.GetValue<long>(), o["live"]!.GetValue<int>());
            })], ct);

    // The schedule row is the one the schedule operations store in 'sched' (13); the script returns its summary fields.
    public Task<StoreRead<IReadOnlyList<ScheduleSummary>>> ListSchedulesAsync(string application, string? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScheduleSummary>>(application, "schedules", [after ?? "", limit],
            r => [.. Rows(r).Select(o => new ScheduleSummary(o["key"]!.GetValue<string>(), o["paused"]!.GetValue<bool>(), o["review"]!.GetValue<bool>(),
                o["drev"]!.GetValue<long>(), o["crev"]!.GetValue<long>(), MsTimeN(o["next"]), MsTimeN(o["mat"])))], ct);

    public Task<StoreRead<IReadOnlyList<AuditEntry>>> ListAuditAsync(string application, AuditFilter filter, long? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<AuditEntry>>(application, "audit",
            [filter.TargetKind ?? "", filter.TargetId ?? "", filter.From is { } f ? CeilMsOf(f) : "", filter.To is { } t ? FloorMs(t) : "",
                after?.ToString(CultureInfo.InvariantCulture) ?? "", limit],
            r => [.. Rows(r).Select(o => new AuditEntry(o["auditId"]!.GetValue<long>(), Guid.Parse(o["commandId"]!.GetValue<string>()), o["action"]!.GetValue<string>(),
                o["targetKind"]!.GetValue<string>(), o["targetId"]!.GetValue<string>(), o["subject"]!.GetValue<string>(), o["display"]?.GetValue<string>(),
                o["scheme"]!.GetValue<string>(), o["reason"]?.GetValue<string>(), o["expectedRevision"]?.GetValue<long>(), o["resultingRevision"]?.GetValue<long>(),
                o["details"]!.AsObject(), MsTime(o["at"])))], ct);

    public Task<StoreRead<IReadOnlyList<ScalingEventSummary>>> ListScalingEventsAsync(string application, ScalingEventFilter filter,
        (DateTimeOffset CreatedAt, Guid EventId)? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScalingEventSummary>>(application, "scaling",
            [filter.InstanceId is { } i ? Hex(i) : "", filter.Pool ?? "", CeilMsOf(filter.From), FloorMs(filter.To),
                after is { } a ? FloorMs(a.CreatedAt) : "", after is { } b ? Hex(b.EventId) : "", limit],
            r => [.. Rows(r).Select(o => new ScalingEventSummary(Guid.Parse(o["eventId"]!.GetValue<string>()), Guid.Parse(o["instanceId"]!.GetValue<string>()),
                o["pool"]!.GetValue<string>(), o["oldTarget"]!.GetValue<int>(), o["newTarget"]!.GetValue<int>(), o["desired"]?.GetValue<int>(),
                o["candidate"]?.GetValue<int>(), o["reason"]!.GetValue<string>(), MsTimeN(o["snapshotAt"]), MsTime(o["createdAt"])))], ct);

    // ------------------------------------------------------------------ RecordDisclosure/1 (16 §3)

    public async Task<StoreReply<Disclosure>> RecordDisclosureAsync(StoreCommand<DisclosureRequest> command, CancellationToken ct)
    {
        var r = command.Body;
        if (ControlValidateActor(r.Actor) is { } invalid)
        {
            return ControlInvalid<Disclosure>(invalid);
        }

        await EnsureStoreIdAsync(command.Application, ct).ConfigureAwait(false);
        var reply = await Write(Op(command.Application, command.CommandId, "RecordDisclosure", command.NotAfter, null, RxLane.Producer, 'd'), "disclosure", ct,
            r.Kind, Hex(r.TargetId), QueuesArg(r.Queues), ControlActorJson(r.Actor)).ConfigureAwait(false);
        if (!reply.HasValue)
        {
            return ControlReject<Disclosure>(reply);
        }

        var o = reply.Result!;
        var status = o["status"]!.GetValue<string>();
        static JsonNode? Parse(JsonNode? text) => text is null ? null : JsonNode.Parse(text.GetValue<string>());
        return reply.Applied(new Disclosure(status, o["jobId"] is { } id ? Guid.Parse(id.GetValue<string>()) : null,
            Parse(o["payload"]), Parse(o["headers"]), Parse(o["error"])));
    }
}
