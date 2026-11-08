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
                x["reason"]?.GetValue<string>(), x["errorCode"]?.GetValue<string>())).ToList();
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
                    MsTime(w["started"]), MsTime(w["heartbeat"]), MsTime(w["expires"]));
            })], ct);

    public Task<StoreRead<IReadOnlyList<ScopeSummary>>> ListScopesAsync(string application, string? kind, (string Kind, string Key)? after, int limit,
        CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScopeSummary>>(application, "scopes", [kind ?? "", after?.Kind ?? "", after?.Key ?? "", limit],
            r => [.. Rows(r).Select(o =>
            {
                var s = o["row"]!;
                return new ScopeSummary(s["kind"]!.GetValue<string>(), s["key"]!.GetValue<string>(), s["capacity"]!.GetValue<int>(),
                    s["prev"]!.GetValue<long>(), o["live"]!.GetValue<int>());
            })], ct);

    // The schedule row is the one the schedule operations store in 'sched' (13); absent fields read as their defaults.
    public Task<StoreRead<IReadOnlyList<ScheduleSummary>>> ListSchedulesAsync(string application, string? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<ScheduleSummary>>(application, "schedules", [after ?? "", limit],
            r => [.. Rows(r).Select(o =>
            {
                var s = o["row"]!;
                return new ScheduleSummary(o["key"]!.GetValue<string>(), s["paused"]?.GetValue<bool>() ?? false, s["needsReview"]?.GetValue<bool>() ?? false,
                    s["definitionRevision"]?.GetValue<long>() ?? 0, s["controlRevision"]?.GetValue<long>() ?? 0, MsTimeN(s["nextDueAt"]),
                    MsTimeN(s["lastMaterializedAt"]));
            })], ct);

    public Task<StoreRead<IReadOnlyList<AuditEntry>>> ListAuditAsync(string application, AuditFilter filter, long? after, int limit, CancellationToken ct) =>
        ManagementReadAsync<IReadOnlyList<AuditEntry>>(application, "audit",
            [filter.TargetKind ?? "", filter.TargetId ?? "", filter.From is { } f ? CeilMsOf(f) : "", filter.To is { } t ? FloorMs(t) : "",
                after?.ToString(CultureInfo.InvariantCulture) ?? "", limit],
            r => [.. Rows(r).Select(o => new AuditEntry(o["auditId"]!.GetValue<long>(), Guid.Parse(o["commandId"]!.GetValue<string>()), o["action"]!.GetValue<string>(),
                o["targetKind"]!.GetValue<string>(), o["targetId"]!.GetValue<string>(), o["subject"]!.GetValue<string>(), o["display"]?.GetValue<string>(),
                o["scheme"]!.GetValue<string>(), o["reason"]?.GetValue<string>(), o["expectedRevision"]?.GetValue<long>(), o["resultingRevision"]?.GetValue<long>(),
                o["details"]!.AsObject(), MsTime(o["at"])))], ct);

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
