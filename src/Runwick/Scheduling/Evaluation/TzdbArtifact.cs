using System.Security.Cryptography;
using NodaTime;
using NodaTime.TimeZones;

namespace Runwick.Scheduling.Evaluation;

/// <summary>
/// An approved, immutable TZDB artifact (spec 12 §6): the exact NZD bytes, their SHA-256, the release label and an
/// application-owned provider. The process-global <c>DateTimeZoneProviders.Tzdb</c> and machine zoneinfo are never used,
/// and nothing is downloaded at run time.
/// </summary>
internal sealed class TzdbArtifact
{
    public const string PinnedVersion = "2026f";
    public const string PinnedSha256Hex = "b17c31650e9163761b50645b4ab3806649deb24be55c790857fdd004b5611be3";
    public const string PinnedResourceName = "Runwick.Scheduling.Evaluation.Tzdb.tzdb2026f.nzd";

    static readonly Lazy<TzdbArtifact> Embedded = new(LoadEmbedded, LazyThreadSafetyMode.ExecutionAndPublication);

    readonly IDateTimeZoneProvider _provider;

    TzdbArtifact(string version, byte[] sha256, IDateTimeZoneProvider provider, Dictionary<string, string> canonicalIds)
    {
        Version = version;
        Sha256 = sha256;
        _provider = provider;
        CanonicalIds = canonicalIds;
    }

    /// <summary>The release label reported by the artifact itself, e.g. <c>2026f</c>.</summary>
    public string Version { get; }

    /// <summary>SHA-256 of the exact NZD bytes (32 bytes).</summary>
    public byte[] Sha256 { get; }

    public string Sha256Hex => Convert.ToHexStringLower(Sha256);

    /// <summary>Every ID in the artifact, canonical or alias, mapped to its canonical ID. Ordinal: IDs are case-sensitive.</summary>
    public IReadOnlyDictionary<string, string> CanonicalIds { get; }

    /// <summary>The artifact compiled into this assembly; verified on first use.</summary>
    public static TzdbArtifact Pinned => Embedded.Value;

    /// <summary>The zone for a canonical ID, or null when the artifact has none.</summary>
    public DateTimeZone? Zone(string canonicalId) => _provider.GetZoneOrNull(canonicalId);

    /// <summary>
    /// Verifies the hash of <paramref name="nzd"/> before NodaTime parses it, then the release label it reports.
    /// Any mismatch is <see cref="EvaluationErrorCode.MissingArtifact"/>: this process cannot evaluate definitions
    /// that name the expected artifact.
    /// </summary>
    public static TzdbArtifact Load(ReadOnlySpan<byte> nzd, string expectedVersion, string expectedSha256Hex)
    {
        var sha = SHA256.HashData(nzd);
        if (!string.Equals(Convert.ToHexStringLower(sha), expectedSha256Hex, StringComparison.Ordinal))
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.MissingArtifact,
                $"The TZDB artifact hash {Convert.ToHexStringLower(sha)} is not the approved {expectedSha256Hex}.");
        }

        TzdbDateTimeZoneSource source;
        try
        {
            using var stream = new MemoryStream(nzd.ToArray(), writable: false);
            source = TzdbDateTimeZoneSource.FromStream(stream);
            source.Validate();
        }
        catch (Exception ex) when (ex is NodaTime.Utility.InvalidNodaDataException or IOException or InvalidOperationException)
        {
            throw new ScheduleEvaluationException(EvaluationErrorCode.MissingArtifact, "The TZDB artifact could not be loaded.", ex);
        }

        if (!string.Equals(source.TzdbVersion, expectedVersion, StringComparison.Ordinal))
        {
            throw new ScheduleEvaluationException(
                EvaluationErrorCode.MissingArtifact,
                $"The TZDB artifact reports release {source.TzdbVersion}, not {expectedVersion}.");
        }

        var ids = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (id, canonical) in source.CanonicalIdMap)
        {
            ids[id] = canonical;
        }

        return new TzdbArtifact(source.TzdbVersion, sha, new DateTimeZoneCache(source), ids);
    }

    static TzdbArtifact LoadEmbedded()
    {
        using var resource = typeof(TzdbArtifact).Assembly.GetManifestResourceStream(PinnedResourceName)
            ?? throw new ScheduleEvaluationException(
                EvaluationErrorCode.MissingArtifact, $"The embedded TZDB artifact {PinnedResourceName} is missing.");
        using var buffer = new MemoryStream();
        resource.CopyTo(buffer);
        return Load(buffer.GetBuffer().AsSpan(0, (int)buffer.Length), PinnedVersion, PinnedSha256Hex);
    }
}
