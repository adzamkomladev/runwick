using System.Text.Json;
using Runwick.Core.Tests.Support;
using Runwick.Scheduling.Evaluation;
using Runwick.Serialization;
using static Runwick.Core.Tests.Scheduling.ScheduleFixtures;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// SER-11 for the definition-fingerprint domain <c>runwick.schedule-definition.v1</c> (12 §5.4, 15 §3): the frozen
/// vectors in tests/vectors/wire/schedule-definition.json, produced by the independent Node encoder from the
/// normalized sets, are reproduced byte for byte by compiling the entered text through the C# grammar and zone mapper.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-015")]
[Trait("Case", "SER-11")]
public sealed class DefinitionVectorTests
{
    const string File = "schedule-definition.json";

    public static TheoryData<string> Ids()
    {
        using var doc = Repo.LoadWireVector(File);
        var data = new TheoryData<string>();
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            data.Add(c.GetProperty("id").GetString()!);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Ids))]
    public void Definition_vector_bytes_and_fingerprint_match(string id)
    {
        using var doc = Repo.LoadWireVector(File);
        var c = doc.RootElement.GetProperty("cases").EnumerateArray().Single(x => x.GetProperty("id").GetString() == id);
        var r = c.GetProperty("recurrence");
        Assert.Empty(c.GetProperty("settings").EnumerateObject());

        CanonicalValue recurrence;
        if (r.GetProperty("dialect").GetString() == CronDialect.Instant)
        {
            recurrence = ScheduleDefinition.InstantRecurrence(r.GetProperty("dueAt").GetInt64());
        }
        else
        {
            var zone = c.GetProperty("zoneEntered");
            var schedule = ScheduleDefinition.Compile(
                c.GetProperty("text").GetString(), r.GetProperty("dialect").GetString()!,
                zone.ValueKind == JsonValueKind.Null ? null : zone.GetString(), TzdbArtifact.Pinned,
                GapOf(r.GetProperty("gapPolicy").GetString()!), FoldOf(r.GetProperty("foldPolicy").GetString()!));
            Assert.Equal(r.GetProperty("zone").GetProperty("canonicalId").GetString(), schedule.Zone.CanonicalId);
            recurrence = ScheduleDefinition.Recurrence(schedule);
        }

        Assert.Equal(c.GetProperty("canonicalHex").GetString(), Convert.ToHexStringLower(CanonicalEncoder.Encode(ScheduleDefinition.Envelope(recurrence))));
        Assert.Equal(c.GetProperty("fingerprint").GetString(), Convert.ToHexStringLower(ScheduleDefinition.Fingerprint(recurrence)));
        foreach (var expected in c.GetProperty("expect").EnumerateObject())
        {
            Assert.Equal(expected.Value.GetString(), c.GetProperty(expected.Name).GetString());
        }
    }

    [Fact]
    public void The_vectors_carry_the_spec_12_golden_fingerprints()
    {
        using var doc = Repo.LoadWireVector(File);
        var fingerprints = doc.RootElement.GetProperty("cases").EnumerateArray()
            .ToDictionary(c => c.GetProperty("id").GetString()!, c => c.GetProperty("fingerprint").GetString());

        Assert.Equal("76883d95cd2413ed1e4f9438a8d786fff8bdbc4a0f48e22391998ee0945a9fc1", fingerprints["SD-1"]);
        Assert.Equal("63ee9ef060168a8c58d656396dcf5db4c35c8ea1f8b133bb2c30502ec4d7ec10", fingerprints["SD-2"]);
        Assert.Equal("c66d3c054d3d4a4713d4b45143931c8f1b80a774c51b6d9415d2d6e0181fffaf", fingerprints["SD-3"]);
        Assert.Equal(fingerprints["SD-1"], fingerprints["SD-4"]);
        Assert.Equal(8, fingerprints.Values.Distinct().Count() + 1);
    }
}
