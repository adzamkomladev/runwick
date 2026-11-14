using System.Text;
using System.Text.Json.Nodes;
using Runwick.Core.Tests.Support;
using Runwick.Serialization;

namespace Runwick.Core.Tests;

/// <summary>
/// SER-03: every invalid form and every §4.1 bound (limit accepted, limit + 1 rejected) yields the
/// same stable §11 code at Prepare, at import and at worker decode.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-004")]
[Trait("Case", "SER-03")]
public sealed class BoundaryTests
{
    static string Repeat(char c, int n) => new(c, n);

    static string Members(int count) => "{" + string.Join(",", Enumerable.Range(0, count).Select(i => "\"m" + i + "\":0")) + "}";

    static byte[] OneMiB() => RunwickJsonProfile.V1.Serialize(Payloads.OfSize(1_048_576), typeof(BoundsJob)).Utf8;

    /// <summary>(bound, payload at the limit, payload at limit + 1, code).</summary>
    public static TheoryData<string> Bounds() => ["size", "depth", "string", "name", "properties", "array", "number"];

    static (byte[] Limit, byte[] Over, string Code) Raw(string bound) => bound switch
    {
        "size" => (OneMiB(), [(byte)' ', .. OneMiB()], RunwickErrorCodes.PayloadTooLarge),
        "depth" => (Bytes("{\"a\":" + Payloads.Nested(31) + "}"), Bytes("{\"a\":" + Payloads.Nested(32) + "}"), RunwickErrorCodes.PayloadDepthExceeded),
        "string" => (Bytes("{\"a\":\"" + Repeat('x', 262_144) + "\"}"), Bytes("{\"a\":\"" + Repeat('x', 262_145) + "\"}"), RunwickErrorCodes.PayloadStringTooLong),
        "name" => (Bytes("{\"" + Repeat('n', 256) + "\":1}"), Bytes("{\"" + Repeat('n', 257) + "\":1}"), RunwickErrorCodes.PayloadPropertyNameTooLong),
        "properties" => (Bytes(Members(256)), Bytes(Members(257)), RunwickErrorCodes.PayloadTooManyProperties),
        "array" => (Bytes("{\"a\":[" + string.Join(",", Enumerable.Repeat("0", 16_384)) + "]}"),
            Bytes("{\"a\":[" + string.Join(",", Enumerable.Repeat("0", 16_385)) + "]}"), RunwickErrorCodes.PayloadArrayTooLong),
        "number" => (Bytes("{\"a\":" + Repeat('1', 128) + "}"), Bytes("{\"a\":" + Repeat('1', 129) + "}"), RunwickErrorCodes.PayloadNumberTooLong),
        _ => throw new ArgumentOutOfRangeException(nameof(bound)),
    };

    static (IQueueJob Limit, IQueueJob Over) Jobs(string bound) => bound switch
    {
        "size" => (Payloads.OfSize(1_048_576), Payloads.OfSize(1_048_577)),
        "depth" => (Payloads.Raw(Payloads.Nested(31)), Payloads.Raw(Payloads.Nested(32))),
        "string" => (BoundsJob.WithParts(Repeat('x', 262_144)), BoundsJob.WithParts(Repeat('x', 262_145))),
        "name" => (Map(1, Repeat('n', 256)), Map(1, Repeat('n', 257))),
        "properties" => (Map(256), Map(257)),
        "array" => (new BoundsJob(null, null, new int[16_384], null), new BoundsJob(null, null, new int[16_385], null)),
        "number" => (Payloads.Raw(Repeat('1', 128)), Payloads.Raw(Repeat('1', 129))),
        _ => throw new ArgumentOutOfRangeException(nameof(bound)),
    };

    static BoundsJob Map(int count, string? name = null) =>
        new(null, Enumerable.Range(0, count).ToDictionary(i => name ?? "k" + i, i => i, StringComparer.Ordinal), null, null);

    static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    static DispatchValidationException ImportPayload(byte[] payload) =>
        TestHost.ImportFails(TestHost.Utf8(JsonNode.Parse(WireVectorTests.GvC0Export)!.AsObject().Also(o => o["payloadBase64"] = Convert.ToBase64String(payload)).ToJsonString()));

    [Theory]
    [MemberData(nameof(Bounds))]
    public void Bound_is_identical_at_prepare_import_and_decode(string bound)
    {
        var (limit, over, code) = Raw(bound);
        var (limitJob, overJob) = Jobs(bound);
        var preparer = TestHost.Preparer(b => b.AddJob<BoundsJob>());

        // Prepare: the limit is accepted and round-trips through export/import; limit + 1 is rejected.
        var prepared = preparer.Prepare(limitJob);
        TestHost.Import(prepared.Export().Span);
        Assert.Equal(code, preparer.PrepareFails(overJob).Code);

        // Import: the scanner passes at the limit (then the digest check fails), rejects limit + 1.
        JsonPayloadScanner.ScanPayload(limit);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeDigestMismatch, ImportPayload(limit).Code);
        Assert.Equal(code, ImportPayload(over).Code);

        // Worker decode.
        JsonPayloadScanner.ValidatePayload(limit);
        Assert.Equal(code, Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(over, typeof(BoundsJob))).Code);
    }

    [Fact]
    public void Scanner_rejections_are_identical_at_import_and_decode()
    {
        using var doc = Repo.LoadWireVector("payload-scanner.json");
        foreach (var c in doc.RootElement.GetProperty("rejected").EnumerateArray())
        {
            var bytes = Convert.FromBase64String(c.GetProperty("inputBase64").GetString()!);
            var code = c.GetProperty("code").GetString();

            Assert.Equal(code, ImportPayload(bytes).Code);
            Assert.Equal(code, Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(bytes, typeof(PlainJob))).Code);
        }
    }

    [Fact]
    public void Invalid_payload_values_at_prepare()
    {
        var preparer = TestHost.Preparer(b =>
        {
            b.AddJob<BoundsJob>();
            b.AddJob<TypedJob>();
        });

        Assert.Equal(RunwickErrorCodes.PayloadDuplicateProperty, preparer.PrepareFails(Payloads.Raw("""{"a":1,"a":2}""")).Code);
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, preparer.PrepareFails(new TypedJob(1, 1, "n", null, double.NaN)).Code);
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, preparer.PrepareFails(new TypedJob(1, 1, "n", null, double.PositiveInfinity)).Code);
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, preparer.PrepareFails(new TypedJob(1, 1, "n", DateTime.Now)).Code);
        Assert.Equal(RunwickErrorCodes.PayloadInvalidUtf8, preparer.PrepareFails(new TypedJob(1, 1, "a" + (char)0xD800 + "b")).Code);
    }

    [Fact]
    public void Binding_failures_at_decode()
    {
        static string Decode(string json) =>
            Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(Bytes(json), typeof(TypedJob))).Code;

        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, Decode("""{"count":"1","big":1,"name":"n","at":null,"ratio":0}"""));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, Decode("""{"count":1,"big":12345678901234567890123,"name":"n","at":null,"ratio":0}"""));
        Assert.Equal(RunwickErrorCodes.PayloadUnmappedMember, Decode("""{"count":1,"big":1,"name":"n","at":null,"ratio":0,"extra":1}"""));
        Assert.Equal(RunwickErrorCodes.PayloadUnmappedMember, Decode("""{"Count":1,"big":1,"name":"n","at":null,"ratio":0}"""));
        Assert.Equal(RunwickErrorCodes.PayloadMissingRequired, Decode("""{"big":1,"name":"n","at":null,"ratio":0}"""));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, Decode("""{"count":1,"big":1,"name":null,"at":null,"ratio":0}"""));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidJson, Decode("""{"count":1,"big":1,"name":"n","at":null,"ratio":NaN}"""));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidJson, Decode("""{"count":1,/*c*/"big":1,"name":"n","at":null,"ratio":0}"""));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidJson, Decode("""{"count":1,"big":1,"name":"n","at":null,"ratio":0,}"""));
        Assert.Equal(RunwickErrorCodes.PayloadRootNotObject, Decode("null"));
        Assert.Equal(RunwickErrorCodes.PayloadMissingRequired,
            Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(Bytes("{}"), typeof(RequiredJob))).Code);
    }

    [Fact]
    public void Metadata_bounds_at_prepare()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<PlainJob>());
        var job = new PlainJob("x");
        static DispatchOptions With(Dictionary<string, string> m) => new() { Metadata = m };

        preparer.Prepare(job, With(new() { ["k"] = Repeat('v', 16_383) }));
        Assert.Equal(RunwickErrorCodes.MetadataTooLarge, preparer.PrepareFails(job, With(new() { ["k"] = Repeat('v', 16_384) })).Code);
        preparer.Prepare(job, With(Enumerable.Range(0, 32).ToDictionary(i => "k" + i, _ => "v")));
        Assert.Equal(RunwickErrorCodes.MetadataTooLarge, preparer.PrepareFails(job, With(Enumerable.Range(0, 33).ToDictionary(i => "k" + i, _ => "v"))).Code);
        preparer.Prepare(job, With(new() { [Repeat('k', 128)] = "v" }));
        Assert.Equal(RunwickErrorCodes.InvalidMetadata, preparer.PrepareFails(job, With(new() { [Repeat('k', 129)] = "v" })).Code);
        Assert.Equal(RunwickErrorCodes.InvalidMetadata, preparer.PrepareFails(job, With(new() { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" })).Code);
        Assert.Equal(RunwickErrorCodes.InvalidMetadata, preparer.PrepareFails(job, With(new() { ["k"] = "a\nb" })).Code);
        Assert.Equal(RunwickErrorCodes.InvalidMetadata, preparer.PrepareFails(job, With(new() { ["k"] = ((char)0xDC00).ToString() })).Code);

        // Metadata at the limit survives export/import (the envelope repeats the same rules).
        var full = preparer.Prepare(job, With(new() { ["k"] = Repeat('v', 16_383) }));
        TestHost.Import(full.Export().Span);
    }

    [Fact]
    public void Uniqueness_and_resource_key_bounds_at_prepare()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<MutableJob>().PerResource(j => j.Key, 1));
        static DispatchOptions Keys(string? active, string? window, TimeSpan? w) => new() { Uniqueness = new DispatchUniqueness(active, window, w) };
        var job = new MutableJob { Key = Repeat('r', 512) };

        preparer.Prepare(job, Keys(Repeat('a', 512), Repeat('w', 512), TimeSpan.FromMilliseconds(1)));
        preparer.Prepare(job, Keys(null, "w", TimeSpan.FromDays(3650)));
        Assert.Equal(RunwickErrorCodes.InvalidKey, preparer.PrepareFails(job, Keys(Repeat('a', 513), null, null)).Code);
        Assert.Equal(RunwickErrorCodes.InvalidKey, preparer.PrepareFails(job, Keys(Repeat('é', 257), null, null)).Code);
        Assert.Equal(RunwickErrorCodes.InvalidKey, preparer.PrepareFails(job, Keys("", null, null)).Code);
        Assert.Equal(RunwickErrorCodes.InvalidKey, preparer.PrepareFails(job, Keys("a" + (char)0x85 + "b", null, null)).Code);
        Assert.Equal(RunwickErrorCodes.InvalidKey, preparer.PrepareFails(new MutableJob { Key = Repeat('r', 513) }).Code);
        Assert.Equal(RunwickErrorCodes.InvalidUniqueness, preparer.PrepareFails(job, Keys(null, "w", null)).Code);
        Assert.Equal(RunwickErrorCodes.InvalidUniqueness, preparer.PrepareFails(job, Keys(null, null, TimeSpan.FromSeconds(1))).Code);
        Assert.Equal(RunwickErrorCodes.InvalidUniqueness, preparer.PrepareFails(job, Keys(null, "w", TimeSpan.FromTicks(15_000))).Code);
        Assert.Equal(RunwickErrorCodes.InvalidUniqueness, preparer.PrepareFails(job, Keys(null, "w", TimeSpan.FromDays(3650) + TimeSpan.FromMilliseconds(1))).Code);
        Assert.Equal(RunwickErrorCodes.InvalidUniqueness, preparer.PrepareFails(job, Keys(null, "w", TimeSpan.Zero)).Code);
    }

    [Fact]
    public void Batch_item_cap_follows_the_store_and_defaults_to_100()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<PlainJob>());
        List<BatchDispatchItem> Items(int n) => [.. Enumerable.Range(0, n).Select(i => new BatchDispatchItem(new PlainJob("p" + i)))];
        const int cap = PayloadBounds.PostgresMaxBatchItems;

        var batch = preparer.PrepareMany(TestHost.StoreId, Items(cap), null, cap);
        Assert.Equal(RunwickErrorCodes.BatchTooLarge,
            Assert.Throws<DispatchValidationException>(() => preparer.PrepareMany(TestHost.StoreId, Items(cap + 1), null, cap)).Code);
        Assert.Equal(RunwickErrorCodes.BatchTooLarge,
            Assert.Throws<DispatchValidationException>(() => preparer.PrepareMany(TestHost.StoreId, Items(cap), null)).Code);

        PreparedEnvelopeCodec.ReadBatch(batch.Export().Span, TestHost.StoreId, "billing", cap);
        Assert.Throws<DispatchValidationException>(() => PreparedEnvelopeCodec.ReadBatch(batch.Export().Span, TestHost.StoreId, "billing"));
    }

    [Fact]
    public void Batch_count_and_envelope_bounds()
    {
        var preparer = TestHost.Preparer(b =>
        {
            b.AddJob<BoundsJob>();
            b.AddJob<PlainJob>();
        });
        BatchDispatchItem Plain(int i) => new(new PlainJob("p" + i));

        preparer.PrepareMany(TestHost.StoreId, [.. Enumerable.Range(0, 100).Select(Plain)], null);
        Assert.Equal(RunwickErrorCodes.BatchTooLarge,
            Assert.Throws<DispatchValidationException>(() => preparer.PrepareMany(TestHost.StoreId, [.. Enumerable.Range(0, 101).Select(Plain)], null)).Code);
        Assert.Equal(RunwickErrorCodes.BatchEmpty,
            Assert.Throws<DispatchValidationException>(() => preparer.PrepareMany(TestHost.StoreId, [], null)).Code);

        // Two ~760 KiB payloads; a metadata pad steers the batch export to exactly 2 MiB.
        var big = Payloads.OfSize(780_000);
        PreparedBatch Batch(int pad) => preparer.PrepareMany(TestHost.StoreId,
            [new BatchDispatchItem(big, new DispatchOptions { Metadata = new Dictionary<string, string> { ["pad"] = Repeat('p', pad) } }), new BatchDispatchItem(big)], null);
        var probe = Batch(1_000).Export().Length;
        var pad = 1_000 + PayloadBounds.MaxEnvelopeBytes - probe;
        Assert.InRange(pad, 1, 16_000);

        var exact = Batch(pad);
        Assert.Equal(PayloadBounds.MaxEnvelopeBytes, exact.Export().Length);
        PreparedEnvelopeCodec.ReadBatch(exact.Export().Span, TestHost.StoreId, "billing");
        Assert.Equal(RunwickErrorCodes.BatchEnvelopeTooLarge, Assert.Throws<DispatchValidationException>(() => Batch(pad + 1)).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeTooLarge,
            Assert.Throws<DispatchValidationException>(() => PreparedEnvelopeCodec.ReadBatch([.. exact.Export().Span, (byte)' '], TestHost.StoreId, "billing")).Code);
    }

    [Fact]
    public void Invalid_batch_items_are_all_reported()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<PlainJob>());
        var ex = Assert.Throws<DispatchValidationException>(() => preparer.PrepareMany(TestHost.StoreId,
        [
            new BatchDispatchItem(new PlainJob("ok")),
            new BatchDispatchItem(new PlainJob("q"), new DispatchOptions { Queue = "missing" }),
            new BatchDispatchItem(new OtherJob(1)),
        ], null));

        Assert.Equal([(1, RunwickErrorCodes.UnknownQueue), (2, RunwickErrorCodes.JobNotRegistered)], ex.ItemFailures.Select(f => (f.Ordinal, f.Code)));
    }

    [Fact]
    public void Validation_messages_never_echo_payload_content()
    {
        foreach (var json in new[] { """{"secret-name":1,"secret-name":2}""", """{"a":01234567}""", """{"a":-secret}""" })
        {
            var ex = Assert.Throws<PayloadValidationException>(() => JsonPayloadScanner.ScanPayload(Bytes(json)));
            Assert.DoesNotContain("secret", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("1234567", ex.Message, StringComparison.Ordinal);
        }
    }
}

internal static class NodeExtensions
{
    public static T Also<T>(this T value, Action<T> action)
    {
        action(value);
        return value;
    }
}
