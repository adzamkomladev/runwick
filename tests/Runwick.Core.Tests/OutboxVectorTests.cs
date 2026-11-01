using System.Text.Json;
using Runwick.Core.Tests.Support;
using Runwick.Protocol;
using Runwick.Serialization;

namespace Runwick.Core.Tests;

/// <summary>
/// SER-11 for the outbox hash domains <c>runwick.outbox.delivery/1</c> and <c>runwick.outbox.command/1</c> (14 §7,
/// §10 item 2): the frozen vectors in tests/vectors/wire/outbox.json, produced by the independent Node encoder, are
/// reproduced by <see cref="OutboxFingerprints"/>. The OB-0 intent is GV-B0, so its business and policy digests are the
/// SP-004 goldens. Both source adapters store what this function computes (Runwick.Outbox.Tests, SER-11).
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-017")]
[Trait("Case", "SER-11")]
public sealed class OutboxVectorTests
{
    const string File = "outbox.json";

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
    public void Delivery_and_command_vectors_match(string id)
    {
        using var doc = Repo.LoadWireVector(File);
        var c = doc.RootElement.GetProperty("cases").EnumerateArray().Single(x => x.GetProperty("id").GetString() == id);
        var d = c.GetProperty("delivery");
        var t = c.GetProperty("transport");
        var (intent, _, _, _, _) = WireVectorTests.Base();
        intent = intent with
        {
            DiagnosticMetadata = d.GetProperty("diagnosticMetadata").EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString()!, StringComparer.Ordinal),
            ProposedJobId = Guid.Parse(d.GetProperty("proposedJobId").GetString()!),
        };
        var digests = new DispatchDigests(Convert.FromHexString(d.GetProperty("businessDigest").GetString()!),
            Convert.FromHexString(d.GetProperty("policyDigest").GetString()!), new byte[32]);
        var store = Guid.Parse(d.GetProperty("destinationStoreId").GetString()!);
        var application = d.GetProperty("application").GetString()!;

        var delivery = OutboxFingerprints.Delivery(store, application,
            new OutboxSourceIdentity(Guid.Parse(d.GetProperty("sourceStoreId").GetString()!), Guid.Parse(d.GetProperty("messageId").GetString()!)),
            d.GetProperty("acceptUntil").GetInt64(), intent, digests);
        var command = OutboxFingerprints.Command(store, application, Guid.Parse(t.GetProperty("commandId").GetString()!),
            DateTimeOffset.FromUnixTimeMilliseconds(t.GetProperty("notAfter").GetInt64()), delivery);

        Assert.Equal(c.GetProperty("deliveryDigest").GetString(), Convert.ToHexStringLower(delivery));
        Assert.Equal(c.GetProperty("commandDigest").GetString(), Convert.ToHexStringLower(command));
    }

    [Fact]
    public void Ob0_freezes_the_gv_b0_intent_and_the_transport_is_outside_the_delivery_identity()
    {
        using var doc = Repo.LoadWireVector(File);
        var cases = doc.RootElement.GetProperty("cases").EnumerateArray().ToDictionary(c => c.GetProperty("id").GetString()!, c => c.Clone());
        var b0 = WireVectorTests.GvC0();

        Assert.Equal(Convert.ToHexStringLower(b0.Digests.Business), cases["OB-0"].GetProperty("delivery").GetProperty("businessDigest").GetString());
        Assert.Equal(Convert.ToHexStringLower(b0.Digests.Policy), cases["OB-0"].GetProperty("delivery").GetProperty("policyDigest").GetString());
        string Delivery(string id) => cases[id].GetProperty("deliveryDigest").GetString()!;
        string Command(string id) => cases[id].GetProperty("commandDigest").GetString()!;

        Assert.Equal(7, new[] { "OB-0", "OB-1", "OB-2", "OB-3", "OB-4", "OB-5", "OB-6" }.Select(Delivery).Distinct().Count());
        Assert.Equal(Delivery("OB-0"), Delivery("OB-7"));
        Assert.Equal(Delivery("OB-0"), Delivery("OB-8"));
        Assert.Equal(3, new[] { "OB-0", "OB-7", "OB-8" }.Select(Command).Distinct().Count());

        // The command canonical bytes are runwick.command.v1's envelope (02 §8.3) with the delivery digest as its body.
        var c0 = cases["OB-0"];
        var envelope = Fingerprints.Command(Guid.Parse(c0.GetProperty("delivery").GetProperty("destinationStoreId").GetString()!), "billing",
            "ReceiveOutbox", 1, Guid.Parse(c0.GetProperty("transport").GetProperty("commandId").GetString()!),
            c0.GetProperty("transport").GetProperty("notAfter").GetInt64(),
            CanonicalValue.Object().Add("deliveryDigest", Delivery("OB-0")).Build());
        Assert.Equal(c0.GetProperty("commandCanonicalHex").GetString(), Convert.ToHexStringLower(CanonicalEncoder.Encode(envelope)));
    }
}
