using System.Security.Cryptography;
using Runwick.Serialization;

namespace Runwick.Protocol;

// Schedule storage contract (spec 13 §3–§6). Internal; a separate interface so drivers without schedules need no stubs.
// The caller plans (it owns the evaluator), the store checks revisions, bounds and the cutoff (13 [choice] 1).

/// <summary>Schedule operation names, lifetimes and wire names (13 §6, §9, C-9).</summary>
internal static class ScheduleOperations
{
    public const string RegisterSchedule = "RegisterSchedule";
    public const string MaterializeOccurrences = "MaterializeOccurrences";
    public const string SetScheduleState = "SetScheduleState";
    public const string UpdateSchedule = "UpdateSchedule";
    public const string TriggerSchedule = "TriggerSchedule";
    public const string ResolveCatchUp = "ResolveCatchUp";
    public const string RecordScheduleEvaluationFailure = "RecordScheduleEvaluationFailure";
    public const string RevalidateSchedule = "RevalidateSchedule";

    public static readonly string[] All =
    [
        RegisterSchedule, MaterializeOccurrences, SetScheduleState, UpdateSchedule, TriggerSchedule, ResolveCatchUp,
        RecordScheduleEvaluationFailure, RevalidateSchedule,
    ];

    /// <summary>Scheduler-family commands other than registration: 30 s default, 60 s cap, durable (13 §9, C-1).</summary>
    public static bool IsScheduler(string operation) =>
        operation is MaterializeOccurrences or SetScheduleState or UpdateSchedule or TriggerSchedule or ResolveCatchUp
            or RecordScheduleEvaluationFailure or RevalidateSchedule;

    /// <summary>Operations that create jobs or schedules and therefore pass the creation gate and the capability check.</summary>
    public static bool IsCreation(string operation) => operation is RegisterSchedule or MaterializeOccurrences or TriggerSchedule;

    public static readonly TimeSpan SchedulerLifetime = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan SchedulerCap = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan RegisterLifetime = TimeSpan.FromMinutes(5);

    public const string Capability = "scheduler";

    // Wire names (13 C-9).
    public const string Cron = "cron";
    public const string OneTime = "one_time";
    public const string MisfireSkip = "skip";
    public const string MisfireRunOnce = "run_once";
    public const string MisfireCatchUp = "catch_up";
    public const string ResumeSkipPaused = "skip_paused";
    public const string PendingEdit = "skipped_by_edit";
    public const string OverlapAllow = "allow";
    public const string OverlapSerialize = "serialize";
    public const string ScheduledKind = "scheduled";
    public const string ManualKind = "manual";
    public const string ScopeKind = "schedule";

    public static string OccurrenceKey(DateTimeOffset scheduledAt) =>
        scheduledAt.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
}

/// <summary>Bounds of 13 §10.</summary>
internal static class ScheduleBounds
{
    public const int MaxBatch = 32;
    public const long MaxPlanPayloadBytes = 2 * 1024 * 1024;
    public const int DefaultEpisodeJobs = 1_000;
    public const int MaxEpisodeJobs = 10_000;
    public static readonly TimeSpan DefaultGrace = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan MaxGrace = TimeSpan.FromHours(24);
    public static readonly TimeSpan DefaultCatchUpAge = TimeSpan.FromDays(7);
    public static readonly TimeSpan MinCatchUpAge = TimeSpan.FromMinutes(1);
    public static readonly TimeSpan MaxCatchUpAge = TimeSpan.FromDays(31);
    public static readonly TimeSpan CatchUpPacing = TimeSpan.FromMilliseconds(250);
    public const int DueReadLimit = 32;
}

/// <summary>The frozen job template of a definition (13 §3.2). Rates are aligned with the normalized rate scope order.</summary>
internal sealed record ScheduleTemplate(
    string JobName,
    int PayloadVersion,
    string SerializerProfileId,
    int SerializerProfileVersion,
    byte[] PayloadUtf8,
    string Queue,
    string Priority,
    ExecutionPolicy Policy,
    IReadOnlyList<ScopeRequirement> Scopes,
    IReadOnlyList<RateRequirement> Rates,
    string? ActiveKey,
    IReadOnlyDictionary<string, string> BusinessMetadata)
{
    public JobContract Contract => new(JobName, PayloadVersion, SerializerProfileId, SerializerProfileVersion);

    public string PayloadSha256Hex => Convert.ToHexStringLower(SHA256.HashData(PayloadUtf8));

    /// <summary>
    /// The dispatch item for one occurrence: the template plus the proposed job ID, and the exclusive
    /// <c>schedule</c> scope when the definition serializes (13 §7.1). Eligible at once (no NotBefore).
    /// </summary>
    public DispatchItem ToItem(Guid storeId, string application, Guid commandId, long notAfterMs, Guid scheduleId, bool serialize, Guid proposedJobId)
    {
        IReadOnlyList<ScopeRequirement> scopes = serialize
            ? [.. Scopes, new ScopeRequirement(ScheduleOperations.ScopeKind, scheduleId.ToString("D"), 1)]
            : Scopes;
        var intent = new DispatchIntent(
            JobName, PayloadVersion, SerializerProfileId, SerializerProfileVersion, PayloadUtf8, Queue, Priority, null,
            ScopeRef.Normalize(scopes.Select(s => new ScopeRef(s.Kind, s.Key))),
            ScopeRef.Normalize(Rates.Select(r => new ScopeRef(r.Kind, r.Key))),
            ActiveKey, null, null, BusinessMetadata, new Dictionary<string, string>(StringComparer.Ordinal), Policy, proposedJobId);
        var digests = Fingerprints.ForDispatch(storeId, application, commandId, notAfterMs, intent);
        var rates = intent.RateScopes.Select(s => Rates.First(r => r.Kind == s.Kind && r.Key == s.Key)).ToList();
        return new DispatchItem(intent, scopes, digests) { Rates = rates };
    }
}

/// <summary>One immutable definition revision (13 §3.1). Policies use the persisted wire names.</summary>
internal sealed record ScheduleDefinitionRecord(
    string Kind,
    DateTimeOffset? DueAt,
    string? CronText,
    string? CronDialect,
    string? ZoneId,
    string? EvaluatorProfile,
    string? TzdbVersion,
    byte[]? TzdbSha256,
    string GapPolicy,
    string FoldPolicy,
    string MisfirePolicy,
    long MisfireGraceMs,
    int MaxBatch,
    int MaxEpisodeJobs,
    long MaxCatchUpAgeMs,
    string OverlapPolicy,
    bool AllowReuse,
    ScheduleTemplate Template,
    byte[] Fingerprint)
{
    public bool Serialize => OverlapPolicy == ScheduleOperations.OverlapSerialize;

    public TimeSpan Grace => TimeSpan.FromMilliseconds(MisfireGraceMs);

    public TimeSpan MaxCatchUpAge => TimeSpan.FromMilliseconds(MaxCatchUpAgeMs);
}

internal sealed record ScheduleEpisode(Guid Id, DateTimeOffset Cutoff, int Emitted, int Limit);

/// <summary>The cursor and control state of one schedule plus its current definition.</summary>
internal sealed record ScheduleSnapshot(
    Guid ScheduleId,
    string Key,
    long DefinitionRevision,
    long ControlRevision,
    long CursorRevision,
    bool Enabled,
    bool Paused,
    bool NeedsReview,
    string? ReviewReason,
    DateTimeOffset? NextDueAt,
    DateTimeOffset ScanAfterAt,
    DateTimeOffset? EvaluatedThrough,
    ScheduleEpisode? Episode,
    string? ResumePolicy,
    DateTimeOffset? ResumeCutoff,
    ScheduleDefinitionRecord Definition);

internal sealed record DueSchedules(DateTimeOffset ObservedAt, IReadOnlyList<ScheduleSnapshot> Schedules);

// ---- 6.1 RegisterSchedule / 6.4 UpdateSchedule

internal sealed record RegisterScheduleRequest(string Key, ScheduleDefinitionRecord Definition, DateTimeOffset Cutoff, DateTimeOffset? FirstDueAt);

/// <summary>Outcome <c>created</c>, <c>same</c> or <c>drift</c> (registration), or <c>updated</c> (edit).</summary>
internal sealed record ScheduleRegistration(Guid ScheduleId, string Outcome, long DefinitionRevision, byte[] StoredFingerprint);

internal sealed record UpdateScheduleRequest(
    string Key, ScheduleDefinitionRecord Definition, long ExpectedDefinitionRevision, DateTimeOffset Cutoff, DateTimeOffset? FirstDueAt,
    ControlActor Actor);

// ---- 6.2 MaterializeOccurrences

/// <summary>One automatic occurrence of a plan; the store builds its job from the template.</summary>
internal sealed record PlannedOccurrence(
    DateTimeOffset ScheduledAt, Guid ProposedJobId, DateTimeOffset? CoveredFrom = null, DateTimeOffset? CoveredThrough = null, bool Coalesced = false);

/// <summary>A skipped interval recorded instead of per-occurrence rows (13 §3.1).</summary>
internal sealed record ScheduleAuditRange(DateTimeOffset? FromExclusive, DateTimeOffset ThroughInclusive, string Reason);

/// <summary>The cursor after a plan. A non-null <see cref="ReviewReason"/> sets <c>needs_review</c>.</summary>
internal sealed record ScheduleCursor(
    DateTimeOffset? EvaluatedThrough,
    DateTimeOffset? NextDueAt,
    DateTimeOffset ScanAfterAt,
    ScheduleEpisode? Episode,
    string? ReviewReason = null);

internal sealed record MaterializeRequest(
    Guid ScheduleId,
    long ExpectedDefinitionRevision,
    long ExpectedControlRevision,
    long ExpectedCursorRevision,
    Guid? ExpectedEpisodeId,
    DateTimeOffset Cutoff,
    IReadOnlyList<PlannedOccurrence> Occurrences,
    ScheduleCursor After,
    IReadOnlyList<ScheduleAuditRange> AuditRanges,
    bool ClearsPendingPolicy);

internal sealed record MaterializedOccurrence(string Kind, string Key, Guid? JobId, string Disposition);

internal sealed record ScheduleMaterialization(Guid ScheduleId, long CursorRevision, IReadOnlyList<MaterializedOccurrence> Occurrences);

// ---- 6.3 SetScheduleState / 6.5 TriggerSchedule / 6.6 ResolveCatchUp / 6.7 failure / 6.8 Revalidate

internal sealed record ScheduleStateRequest(string Key, bool Paused, string? ResumePolicy, long ExpectedControlRevision, ControlActor Actor);

internal sealed record ScheduleState(string Key, bool Paused, bool NeedsReview, long DefinitionRevision, long ControlRevision, long CursorRevision);

internal sealed record TriggerScheduleRequest(string Key, long ExpectedDefinitionRevision, Guid TriggerId, Guid ProposedJobId, ControlActor Actor);

internal sealed record ResolveCatchUpRequest(
    string Key, Guid EpisodeId, string Resolution, int AdditionalJobs, DateTimeOffset? NextDueAfterCutoff, Guid ProposedJobId,
    ControlActor Actor);

internal sealed record EvaluationFailureRequest(Guid ScheduleId, long ExpectedDefinitionRevision, string Code, string? Detail, ControlActor Actor);

internal sealed record RevalidateScheduleRequest(string Key, long ExpectedDefinitionRevision, ControlActor Actor);

internal interface IScheduleStore
{
    /// <summary>Due read (13 §6.9): no receipt, no locks, at most <paramref name="limit"/> schedules.</summary>
    Task<DueSchedules> ReadDueSchedulesAsync(string application, int limit, CancellationToken ct);

    Task<ScheduleSnapshot?> ReadScheduleAsync(string application, string key, CancellationToken ct);

    /// <summary>Fresh store time, used as the frozen cutoff of registration and edits (13 §6.1, §6.4).</summary>
    Task<DateTimeOffset> StoreNowAsync(CancellationToken ct);

    Task<StoreReply<ScheduleRegistration>> RegisterScheduleAsync(StoreCommand<RegisterScheduleRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleMaterialization>> MaterializeOccurrencesAsync(StoreCommand<MaterializeRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleState>> SetScheduleStateAsync(StoreCommand<ScheduleStateRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleRegistration>> UpdateScheduleAsync(StoreCommand<UpdateScheduleRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleMaterialization>> TriggerScheduleAsync(StoreCommand<TriggerScheduleRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleMaterialization>> ResolveCatchUpAsync(StoreCommand<ResolveCatchUpRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleState>> RecordEvaluationFailureAsync(StoreCommand<EvaluationFailureRequest> command, CancellationToken ct);

    Task<StoreReply<ScheduleState>> RevalidateScheduleAsync(StoreCommand<RevalidateScheduleRequest> command, CancellationToken ct);
}

/// <summary>Canonical command bodies for the schedule operations (fingerprints, 02 §8; members in name order).</summary>
internal static class ScheduleEnvelopes
{
    static CanonicalValue Ms(DateTimeOffset? t) => t is { } v ? CanonicalValue.Int(v.ToUnixTimeMilliseconds()) : CanonicalValue.Null;

    static CanonicalValue Id(Guid? g) => g is { } v ? Fingerprints.Uuid(v) : CanonicalValue.Null;

    public static CanonicalValue Register(RegisterScheduleRequest r) => CanonicalValue.Object()
        .Add("cutoff", Ms(r.Cutoff))
        .Add("definitionFingerprint", Convert.ToHexStringLower(r.Definition.Fingerprint))
        .Add("firstDueAt", Ms(r.FirstDueAt))
        .Add("key", r.Key)
        .Build();

    public static CanonicalValue Update(UpdateScheduleRequest r) => CanonicalValue.Object()
        .Add("actor", CommandEnvelopes.Actor(r.Actor))
        .Add("cutoff", Ms(r.Cutoff))
        .Add("definitionFingerprint", Convert.ToHexStringLower(r.Definition.Fingerprint))
        .Add("expectedDefinitionRevision", r.ExpectedDefinitionRevision)
        .Add("firstDueAt", Ms(r.FirstDueAt))
        .Add("key", r.Key)
        .Build();

    static CanonicalValue Episode(ScheduleEpisode? e) => e is null
        ? CanonicalValue.Null
        : CanonicalValue.Object().Add("cutoff", Ms(e.Cutoff)).Add("emitted", e.Emitted).Add("id", Fingerprints.Uuid(e.Id)).Add("limit", e.Limit).Build();

    public static CanonicalValue Materialize(MaterializeRequest r) => CanonicalValue.Object()
        .Add("after", CanonicalValue.Object()
            .Add("episode", Episode(r.After.Episode))
            .Add("evaluatedThrough", Ms(r.After.EvaluatedThrough))
            .Add("nextDueAt", Ms(r.After.NextDueAt))
            .Add("reviewReason", r.After.ReviewReason)
            .Add("scanAfterAt", Ms(r.After.ScanAfterAt))
            .Build())
        .Add("auditRanges", CanonicalValue.Array(r.AuditRanges.Select(a => CanonicalValue.Object()
            .Add("fromExclusive", Ms(a.FromExclusive))
            .Add("reason", a.Reason)
            .Add("throughInclusive", Ms(a.ThroughInclusive))
            .Build())))
        .Add("clearsPendingPolicy", r.ClearsPendingPolicy)
        .Add("cutoff", Ms(r.Cutoff))
        .Add("expectedControlRevision", r.ExpectedControlRevision)
        .Add("expectedCursorRevision", r.ExpectedCursorRevision)
        .Add("expectedDefinitionRevision", r.ExpectedDefinitionRevision)
        .Add("expectedEpisodeId", Id(r.ExpectedEpisodeId))
        .Add("occurrences", CanonicalValue.Array(r.Occurrences.Select(o => CanonicalValue.Object()
            .Add("coalesced", o.Coalesced)
            .Add("coveredFrom", Ms(o.CoveredFrom))
            .Add("coveredThrough", Ms(o.CoveredThrough))
            .Add("proposedJobId", Fingerprints.Uuid(o.ProposedJobId))
            .Add("scheduledAt", Ms(o.ScheduledAt))
            .Build())))
        .Add("scheduleId", Fingerprints.Uuid(r.ScheduleId))
        .Build();

    public static CanonicalValue State(ScheduleStateRequest r) => CanonicalValue.Object()
        .Add("actor", CommandEnvelopes.Actor(r.Actor))
        .Add("expectedControlRevision", r.ExpectedControlRevision)
        .Add("key", r.Key)
        .Add("paused", r.Paused)
        .Add("resumePolicy", r.ResumePolicy)
        .Build();

    public static CanonicalValue Trigger(TriggerScheduleRequest r) => CanonicalValue.Object()
        .Add("actor", CommandEnvelopes.Actor(r.Actor))
        .Add("expectedDefinitionRevision", r.ExpectedDefinitionRevision)
        .Add("key", r.Key)
        .Add("proposedJobId", Fingerprints.Uuid(r.ProposedJobId))
        .Add("triggerId", Fingerprints.Uuid(r.TriggerId))
        .Build();

    public static CanonicalValue Resolve(ResolveCatchUpRequest r) => CanonicalValue.Object()
        .Add("actor", CommandEnvelopes.Actor(r.Actor))
        .Add("additionalJobs", r.AdditionalJobs)
        .Add("episodeId", Fingerprints.Uuid(r.EpisodeId))
        .Add("key", r.Key)
        .Add("nextDueAfterCutoff", Ms(r.NextDueAfterCutoff))
        .Add("proposedJobId", Fingerprints.Uuid(r.ProposedJobId))
        .Add("resolution", r.Resolution)
        .Build();

    public static CanonicalValue Failure(EvaluationFailureRequest r) => CanonicalValue.Object()
        .Add("actor", CommandEnvelopes.Actor(r.Actor))
        .Add("code", r.Code)
        .Add("detail", r.Detail)
        .Add("expectedDefinitionRevision", r.ExpectedDefinitionRevision)
        .Add("scheduleId", Fingerprints.Uuid(r.ScheduleId))
        .Build();

    public static CanonicalValue Revalidate(RevalidateScheduleRequest r) => CanonicalValue.Object()
        .Add("actor", CommandEnvelopes.Actor(r.Actor))
        .Add("expectedDefinitionRevision", r.ExpectedDefinitionRevision)
        .Add("key", r.Key)
        .Build();
}
