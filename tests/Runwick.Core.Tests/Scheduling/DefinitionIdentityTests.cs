using Runwick.Scheduling.Evaluation;
using Runwick.Serialization;
using static Runwick.Core.Tests.Scheduling.ScheduleFixtures;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// EVAL-01 … EVAL-03 (spec 12 §5–§6, §12.2): zone ID acceptance and alias normalization, the pinned TZDB artifact's
/// identity, and golden definition fingerprints over the canonical encoder.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-015")]
public sealed class DefinitionIdentityTests
{
    [Fact]
    [Trait("Case", "EVAL-01")]
    public void No_zone_means_explicit_utc()
    {
        var utc = ZoneMapper.Resolve(TzdbArtifact.Pinned, null);

        Assert.Equal(ZoneMapper.DefaultZoneId, utc.EnteredId);
        Assert.Equal(ZoneMapper.Resolve(TzdbArtifact.Pinned, "Etc/UTC").CanonicalId, utc.CanonicalId);
        Assert.Equal(ScheduleDefinition.Fingerprint(Cron("0 9 * * *")), ScheduleDefinition.Fingerprint(Cron("0 9 * * *", "Etc/UTC")));
    }

    [Theory]
    [Trait("Case", "EVAL-01")]
    [InlineData("Asia/Calcutta", "Asia/Kolkata")]
    [InlineData("US/Pacific", "America/Los_Angeles")]
    [InlineData("Europe/Kiev", "Europe/Kyiv")]
    public void Aliases_normalize_through_the_artifact_and_keep_the_entered_id(string alias, string canonical)
    {
        var zone = ZoneMapper.Resolve(TzdbArtifact.Pinned, alias);

        Assert.Equal(alias, zone.EnteredId);
        Assert.Equal(canonical, zone.CanonicalId);
        Assert.Equal(ScheduleDefinition.Fingerprint(Cron("0 9 * * *", alias)), ScheduleDefinition.Fingerprint(Cron("0 9 * * *", canonical)));
    }

    [Theory]
    [Trait("Case", "EVAL-01")]
    [InlineData("europe/berlin")]
    [InlineData("EUROPE/BERLIN")]
    [InlineData("W. Europe Standard Time")]
    [InlineData("Central European Standard Time")]
    [InlineData("Local")]
    [InlineData("local")]
    [InlineData("")]
    [InlineData(" Europe/Berlin")]
    [InlineData("Europe/Berlin\n")]
    [InlineData("Mars/Olympus_Mons")]
    public void Windows_ids_local_shorthand_and_case_variants_are_rejected(string id)
    {
        var error = Fails(() => ZoneMapper.Resolve(TzdbArtifact.Pinned, id));

        Assert.Equal(EvaluationErrorCode.InvalidZone, error.Code);
        Assert.Equal(EvaluationErrorClass.Invalid, error.Class);
    }

    [Fact]
    [Trait("Case", "EVAL-02")]
    public void The_pinned_artifact_is_tzdb_2026f_with_the_recorded_hash()
    {
        var artifact = TzdbArtifact.Pinned;

        Assert.Equal("2026f", artifact.Version);
        Assert.Equal("b17c31650e9163761b50645b4ab3806649deb24be55c790857fdd004b5611be3", artifact.Sha256Hex);
        Assert.Equal(32, artifact.Sha256.Length);
        Assert.Equal(132_773, Embedded().Length);
    }

    [Fact]
    [Trait("Case", "EVAL-02")]
    public void Altered_bytes_or_a_different_label_are_a_missing_artifact()
    {
        var bytes = Embedded();
        var tampered = (byte[])bytes.Clone();
        tampered[^1] ^= 0x01;

        Assert.Equal(
            EvaluationErrorCode.MissingArtifact,
            Fails(() => TzdbArtifact.Load(tampered, TzdbArtifact.PinnedVersion, TzdbArtifact.PinnedSha256Hex)).Code);
        Assert.Equal(
            EvaluationErrorCode.MissingArtifact,
            Fails(() => TzdbArtifact.Load(bytes, "2026e", TzdbArtifact.PinnedSha256Hex)).Code);
        Assert.Equal("2026f", TzdbArtifact.Load(bytes, TzdbArtifact.PinnedVersion, TzdbArtifact.PinnedSha256Hex).Version);
    }

    [Fact]
    [Trait("Case", "EVAL-02")]
    public void A_stored_definition_naming_another_profile_or_artifact_is_local_incompatibility()
    {
        var artifact = TzdbArtifact.Pinned;

        ScheduleDefinition.RequireSupported(ScheduleDefinition.CronProfile, "2026f", artifact.Sha256, artifact);

        var profile = Fails(() => ScheduleDefinition.RequireSupported("runwick.cron.v1/cronos-0.14.0/nodatime-3.3.7", "2026f", artifact.Sha256, artifact));
        var version = Fails(() => ScheduleDefinition.RequireSupported(ScheduleDefinition.CronProfile, "2026g", artifact.Sha256, artifact));
        var hash = Fails(() => ScheduleDefinition.RequireSupported(ScheduleDefinition.CronProfile, "2026f", new byte[32], artifact));

        Assert.Equal(EvaluationErrorCode.MissingProfile, profile.Code);
        Assert.Equal(EvaluationErrorCode.MissingArtifact, version.Code);
        Assert.Equal(EvaluationErrorCode.MissingArtifact, hash.Code);
        Assert.All(new[] { profile, version, hash }, e => Assert.Equal(EvaluationErrorClass.LocalIncompatibility, e.Class));
    }

    [Fact]
    [Trait("Case", "EVAL-03")]
    public void Cron_definition_fingerprint_golden_vector()
    {
        // Independently derived from spec 02 §7 encoding of the §5 envelope.
        Assert.Equal(
            "76883d95cd2413ed1e4f9438a8d786fff8bdbc4a0f48e22391998ee0945a9fc1",
            Convert.ToHexStringLower(ScheduleDefinition.Fingerprint(Cron("0 9 * * MON-FRI", "Europe/Berlin"))));
        Assert.Equal(
            "63ee9ef060168a8c58d656396dcf5db4c35c8ea1f8b133bb2c30502ec4d7ec10",
            Convert.ToHexStringLower(ScheduleDefinition.Fingerprint(
                Cron("0 9 * * MON-FRI", "Europe/Berlin", GapPolicy.Shift, FoldPolicy.Both))));
    }

    [Fact]
    [Trait("Case", "EVAL-03")]
    public void One_time_definition_fingerprint_golden_vector()
    {
        var recurrence = ScheduleDefinition.InstantRecurrence(Ms("2026-07-01T09:00:00Z"));

        Assert.Equal(
            "0600000003030000001266696e6765727072696e7456657273696f6e040000000131030000000a726563757272656e6365060000000203000000076469616c656374030000001272756e7769636b2e696e7374616e742e763103000000056475654174040000000d31373832383936343030303030030000000873657474696e67730600000000",
            Convert.ToHexStringLower(CanonicalEncoder.Encode(ScheduleDefinition.Envelope(recurrence))));
        Assert.Equal(
            "c66d3c054d3d4a4713d4b45143931c8f1b80a774c51b6d9415d2d6e0181fffaf",
            Convert.ToHexStringLower(ScheduleDefinition.Fingerprint(recurrence)));
    }

    [Fact]
    [Trait("Case", "EVAL-03")]
    public void Every_hashed_member_changes_the_fingerprint()
    {
        var baseline = Hex(Cron("0 9 * * *", "Europe/Berlin"));
        string[] variants =
        [
            Hex(Cron("0 9 * * *", "Europe/Paris")),
            Hex(Cron("0 9 * * *", "Europe/Berlin", GapPolicy.Shift)),
            Hex(Cron("0 9 * * *", "Europe/Berlin", fold: FoldPolicy.Later)),
            Hex(Cron("0 9 * * *", "Europe/Berlin", fold: FoldPolicy.Both)),
            Hex(Cron("0 0 9 * * *", "Europe/Berlin", dialect: CronDialect.Cron6)),
            Hex(Cron("1 9 * * *", "Europe/Berlin")),
            Hex(Cron("0 9 * * *", "Europe/Berlin"), CanonicalValue.Object().Add("misfire", "skip").Build()),
        ];

        Assert.DoesNotContain(baseline, variants);
        Assert.Equal(variants.Length, variants.Distinct().Count());
    }

    [Fact]
    [Trait("Case", "EVAL-03")]
    public void Raw_text_and_entered_zone_id_are_display_only()
    {
        var a = Cron("0 9 * * MON-FRI", "Asia/Calcutta");
        var b = Cron("0  09 * *  1-5", "Asia/Kolkata");

        Assert.NotEqual(a.Cron.RawText, b.Cron.RawText);
        Assert.NotEqual(a.Zone.EnteredId, b.Zone.EnteredId);
        Assert.Equal(Hex(a), Hex(b));
    }

    static string Hex(CronSchedule schedule, CanonicalValue? settings = null) =>
        Convert.ToHexStringLower(ScheduleDefinition.Fingerprint(schedule, settings));

    static byte[] Embedded()
    {
        using var stream = typeof(TzdbArtifact).Assembly.GetManifestResourceStream(TzdbArtifact.PinnedResourceName)!;
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }
}
