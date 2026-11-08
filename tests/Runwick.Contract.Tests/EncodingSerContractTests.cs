using System.Text;
using System.Text.Json;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Serialization;

namespace Runwick.Contract.Tests;

public sealed record JsonPayloadJob(string Id, string Text, JsonElement Data) : IQueueJob;

/// <summary>SER-06 and SER-07 (02 §SER, 07 §3) on every store, ported from Runwick.Postgres.Tests.EncodingCaseTests.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-028")]
public sealed class EncodingSerContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static readonly TimeSpan Wait = TimeSpan.FromSeconds(120);

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "SER-06")]
    public async Task Reserved_payload_bytes_and_profile_are_identical_to_the_dispatched_ones(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("ser06_" + Guid.NewGuid().ToString("N")[..8], kind);
        var c = await f.OpenClient(configure: r => r.AddJob<JsonPayloadJob>().Named("test.json").ToQueue("default"));
        string[] data = ["1", "1.0", "1e0", "-0", "123456789012345678901234567890", "\"\\u00e9\\\"\\\\\"", "\"é\\n\"", """{"b":1.0,"a":[1e0,-0]}"""];
        string[] texts = ["plain", "é \" \\ / \u0001 \n"];
        var sent = new Dictionary<Guid, byte[]>();
        foreach (var d in data)
        {
            foreach (var text in texts)
            {
                using var doc = JsonDocument.Parse(d);
                var prepared = c.Prepared.Prepare(new JsonPayloadJob(d, text, doc.RootElement.Clone()));
                var receipt = await c.Prepared.DispatchAsync(prepared);
                sent[receipt.JobId.Value] = prepared.Intent.PayloadUtf8.ToArray();
            }
        }

        var reserved = new List<ReservedJob>();
        while (await c.ReserveAsync() is { Status: StoreStatus.Applied, Value: { } r })
        {
            reserved.Add(r);
        }

        Assert.Equal(sent.Count, reserved.Count);
        foreach (var r in reserved)
        {
            Assert.Equal(sent[r.Position.JobId], r.PayloadUtf8);
            Assert.Equal((RunwickJsonProfile.ProfileId, RunwickJsonProfile.ProfileVersion), (r.Contract.SerializerProfileId, r.Contract.SerializerProfileVersion));
        }

        foreach (var (job, bytes) in sent)
        {
            Assert.Equal(bytes, await StoredRows.PayloadAsync(f, job));
            Assert.True(await StoredRows.ProfileIsAsync(f, job, RunwickJsonProfile.ProfileId, RunwickJsonProfile.ProfileVersion));
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "SER-07")]
    public async Task Unsupported_profile_is_never_reserved_and_malformed_data_fails_permanently_once(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("ser07_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 2);
        var unsupported = (await w.DispatchAsync("unsupported")).JobId.Value;
        var malformed = (await w.DispatchAsync("malformed")).JobId.Value;

        await StoredRows.SetProfileAsync(f, unsupported, "runwick.json-unsupported");
        await StoredRows.SetPayloadAsync(f, malformed, Encoding.UTF8.GetBytes("""{"id":"""));

        await w.StartAsync();
        await w.DispatchAsync("ok");
        try
        {
            await WorkerHost.WaitUntil(() => w.Script.InvocationsOf("ok") == 1, TimeSpan.FromSeconds(20), "the supported job");
        }
        catch (TimeoutException)
        {
            var s = (await f.ReadCommittedSnapshot()).Model;
            Assert.Fail("states: " + string.Join(", ", s.Jobs.Select(j => j.JobId + "=" + j.State + "/" + j.TerminalReason)) + "; events "
                + string.Join(",", w.Events.Select(e => e.Name).Distinct()) + "; attempts " + string.Join(",", s.Attempts.Select(a => a.Outcome)));
        }

        await WorkerHost.WaitUntil(() => f.ReadCommittedSnapshot().GetAwaiter().GetResult().Model.Job(malformed).State == "failed", Wait, "the malformed job fails");
        await Task.Delay(TimeSpan.FromSeconds(3));
        await w.StopAsync();

        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Equal(0, w.Script.InvocationsOf("malformed"));
        Assert.Equal(1, model.Job(malformed).LifetimeExecutions);
        var attempt = Assert.Single(model.AttemptsOf(malformed));
        Assert.Equal("permanent_failure", attempt.Outcome);
        Assert.Equal("pending", model.Job(unsupported).State);
        Assert.Empty(model.AttemptsOf(unsupported));
        Assert.Equal(0, w.Script.InvocationsOf("unsupported"));
        Assert.Empty(SnapshotInvariants.Violations(model));
    }
}
