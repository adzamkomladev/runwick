using NodaTime;

namespace Runwick.Scheduling.Evaluation;

/// <summary>A zone resolved against one artifact: the ID as entered (display), its canonical ID (hashed) and the rules.</summary>
internal sealed record ResolvedZone(string EnteredId, string CanonicalId, DateTimeZone Zone, TzdbArtifact Artifact);

/// <summary>Zone ID acceptance and normalization (spec 12 §6.2).</summary>
internal static class ZoneMapper
{
    /// <summary>The zone used when a definition names none: explicit UTC, never the machine's local zone.</summary>
    public const string DefaultZoneId = "UTC";

    const int MaxIdLength = 64;

    /// <summary>
    /// Accepts case-sensitive IANA IDs and aliases present in <paramref name="artifact"/> and normalizes aliases through
    /// its canonical map. Windows IDs, machine-local shorthand and anything else outside the artifact are
    /// <see cref="EvaluationErrorCode.InvalidZone"/>.
    /// </summary>
    public static ResolvedZone Resolve(TzdbArtifact artifact, string? zoneId)
    {
        var id = zoneId ?? DefaultZoneId;
        if (id.Length is 0 or > MaxIdLength || id.Any(c => c is <= ' ' or > '~'))
        {
            throw new ScheduleEvaluationException(EvaluationErrorCode.InvalidZone, "A zone ID is 1–64 printable ASCII characters.");
        }

        if (!artifact.CanonicalIds.TryGetValue(id, out var canonical))
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.InvalidZone,
                $"'{id}' is not an IANA zone ID in TZDB {artifact.Version}; IDs are case-sensitive, and Windows IDs and local shorthand are not accepted.");
        }

        var zone = artifact.Zone(canonical)
            ?? throw new ScheduleEvaluationException(
                EvaluationErrorCode.MissingArtifact, $"TZDB {artifact.Version} maps '{id}' to '{canonical}' but has no rules for it.");
        return new ResolvedZone(id, canonical, zone, artifact);
    }
}
