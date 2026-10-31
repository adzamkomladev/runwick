using Runwick.Serialization;

namespace Runwick.Scheduling.Evaluation;

/// <summary>Local times inside a forward transition (spec 12 §7.2). Default <see cref="Skip"/>.</summary>
internal enum GapPolicy
{
    Skip,
    Shift,
}

/// <summary>Local times inside a backward transition, by UTC order (spec 12 §7.3). Default <see cref="Earlier"/>.</summary>
internal enum FoldPolicy
{
    Earlier,
    Later,
    Both,
}

/// <summary>A compiled cron definition: normalized fields, resolved zone, DST policies and the Cronos streams.</summary>
internal sealed class CronSchedule
{
    internal CronSchedule(NormalizedCron cron, ResolvedZone zone, GapPolicy gap, FoldPolicy fold)
    {
        Cron = cron;
        Zone = zone;
        Gap = gap;
        Fold = fold;
        Adapter = CronosAdapter.Compile(cron);
    }

    public NormalizedCron Cron { get; }

    public ResolvedZone Zone { get; }

    public GapPolicy Gap { get; }

    public FoldPolicy Fold { get; }

    public string Profile => ScheduleDefinition.CronProfile;

    internal CronosAdapter Adapter { get; }
}

/// <summary>
/// Definition identity (spec 12 §5): the evaluator profile, the canonical recurrence value and the definition
/// fingerprint over the canonical encoder of spec 02 §7.
/// </summary>
internal static class ScheduleDefinition
{
    public const string Domain = "runwick.schedule-definition.v1";
    public const int FingerprintVersion = 1;

    /// <summary>The evaluator profile every cron definition (UTC included) persists and hashes.</summary>
    public const string CronProfile = "runwick.cron.v1/cronos-0.13.0/nodatime-3.3.7";

    /// <summary>
    /// Parses, normalizes and resolves a cron definition against <paramref name="artifact"/>. A null zone is explicit UTC.
    /// </summary>
    public static CronSchedule Compile(
        string? text,
        string dialect,
        string? zoneId,
        TzdbArtifact artifact,
        GapPolicy gap = GapPolicy.Skip,
        FoldPolicy fold = FoldPolicy.Earlier) =>
        new(CronGrammar.Parse(text, dialect), ZoneMapper.Resolve(artifact, zoneId), gap, fold);

    /// <summary>
    /// Fails with <see cref="EvaluationErrorCode.MissingProfile"/> / <see cref="EvaluationErrorCode.MissingArtifact"/>
    /// when a stored definition names a profile or artifact this process does not have (spec 12 §6.3). The caller treats
    /// this as local incompatibility, never as a defect of the definition.
    /// </summary>
    public static void RequireSupported(string profile, string tzdbVersion, ReadOnlySpan<byte> tzdbSha256, TzdbArtifact artifact)
    {
        if (!string.Equals(profile, CronProfile, StringComparison.Ordinal))
        {
            throw new ScheduleEvaluationException(EvaluationErrorCode.MissingProfile, $"Evaluator profile '{profile}' is not supported here.");
        }

        if (!string.Equals(tzdbVersion, artifact.Version, StringComparison.Ordinal) || !tzdbSha256.SequenceEqual(artifact.Sha256))
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.MissingArtifact, $"TZDB artifact '{tzdbVersion}' with the stored hash is not available here.");
        }
    }

    public static string WireName(GapPolicy gap) => gap switch
    {
        GapPolicy.Skip => "skip",
        GapPolicy.Shift => "shift",
        _ => throw new ArgumentOutOfRangeException(nameof(gap)),
    };

    public static string WireName(FoldPolicy fold) => fold switch
    {
        FoldPolicy.Earlier => "earlier",
        FoldPolicy.Later => "later",
        FoldPolicy.Both => "both",
        _ => throw new ArgumentOutOfRangeException(nameof(fold)),
    };

    /// <summary>The canonical recurrence of a cron definition (spec 12 §5.2). Raw text and the entered zone ID are not part of it.</summary>
    public static CanonicalValue Recurrence(CronSchedule schedule)
    {
        var cron = schedule.Cron;
        return CanonicalValue.Object()
            .Add("dayOfMonth", Set(cron.DaysOfMonth))
            .Add("dayOfWeek", Set(cron.DaysOfWeek))
            .Add("dialect", cron.Dialect)
            .Add("evaluatorProfile", schedule.Profile)
            .Add("foldPolicy", WireName(schedule.Fold))
            .Add("gapPolicy", WireName(schedule.Gap))
            .Add("hour", Set(cron.Hours))
            .Add("minute", Set(cron.Minutes))
            .Add("month", Set(cron.Months))
            .Add("second", Set(cron.Seconds))
            .Add("zone", CanonicalValue.Object()
                .Add("canonicalId", schedule.Zone.CanonicalId)
                .Add("tzdbSha256", schedule.Zone.Artifact.Sha256Hex)
                .Add("tzdbVersion", schedule.Zone.Artifact.Version)
                .Build())
            .Build();
    }

    /// <summary>The canonical recurrence of a one-time definition (spec 12 §5.3).</summary>
    public static CanonicalValue InstantRecurrence(long dueAtUtcEpochMilliseconds) =>
        CanonicalValue.Object()
            .Add("dialect", CronDialect.Instant)
            .Add("dueAt", dueAtUtcEpochMilliseconds)
            .Build();

    /// <summary>
    /// The definition envelope (spec 12 §5.1). <paramref name="settings"/> carries the members owned by spec 13 (job
    /// template profile, misfire, overlap, bounds); until then it is the empty object.
    /// </summary>
    public static CanonicalValue Envelope(CanonicalValue recurrence, CanonicalValue? settings = null) =>
        CanonicalValue.Object()
            .Add("fingerprintVersion", FingerprintVersion)
            .Add("recurrence", recurrence)
            .Add("settings", settings ?? CanonicalValue.Object().Build())
            .Build();

    public static byte[] Fingerprint(CanonicalValue recurrence, CanonicalValue? settings = null) =>
        CanonicalEncoder.Digest(Domain, Envelope(recurrence, settings));

    public static byte[] Fingerprint(CronSchedule schedule, CanonicalValue? settings = null) =>
        Fingerprint(Recurrence(schedule), settings);

    static CanonicalValue Set(ulong set) => CanonicalValue.Array(NormalizedCron.Values(set).Select(v => CanonicalValue.Int(v)));
}
