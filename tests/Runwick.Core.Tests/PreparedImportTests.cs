using System.Text;
using System.Text.Json.Nodes;
using Runwick.Core.Tests.Support;
using Runwick.Serialization;

namespace Runwick.Core.Tests;

/// <summary>SER-10 Pure part: export/import of prepared envelopes (spec 02 §9.3, WV-08, WV-11).</summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-004")]
public sealed class PreparedImportTests
{
    static string GvC0Text => WireVectorTests.GvC0Export;

    static byte[] Tampered(string from, string to)
    {
        Assert.Contains(from, GvC0Text, StringComparison.Ordinal);
        return TestHost.Utf8(GvC0Text.Replace(from, to, StringComparison.Ordinal));
    }

    static byte[] Mutate(Action<JsonObject> change, string? source = null)
    {
        var node = JsonNode.Parse(source ?? GvC0Text)!.AsObject();
        change(node);
        return TestHost.Utf8(node.ToJsonString());
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Valid_import_reproduces_operation_id_deadline_and_all_three_digests()
    {
        var original = WireVectorTests.GvC0();

        var imported = TestHost.Import(TestHost.Utf8(GvC0Text));

        Assert.Equal(original.OperationId, imported.OperationId);
        Assert.Equal(original.NotAfter, imported.NotAfter);
        Assert.Equal(original.Digests.Business, imported.Digests.Business);
        Assert.Equal(original.Digests.Policy, imported.Digests.Policy);
        Assert.Equal(original.Digests.Command, imported.Digests.Command);
        Assert.Equal(original.Intent.PayloadUtf8, imported.Intent.PayloadUtf8);
        Assert.Equal(original.Intent.ProposedJobId, imported.Intent.ProposedJobId);
        Assert.Equal(GvC0Text, TestHost.Utf8(imported.Export()));
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Member_order_and_whitespace_are_not_required_on_read()
    {
        var reordered = new JsonObject();
        var source = JsonNode.Parse(GvC0Text)!.AsObject();
        foreach (var (name, value) in source.Reverse())
        {
            reordered[name] = value?.DeepClone();
        }

        var text = reordered.ToJsonString(new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
        var imported = TestHost.Import(TestHost.Utf8(text));

        Assert.Equal(GvC0Text, TestHost.Utf8(imported.Export()));
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Export_then_import_round_trips_a_fresh_prepare()
    {
        var preparer = TestHost.Preparer(b => b.Queue("q", q => q.MaxConcurrent(2)).AddJob<MutableJob>()
            .ToQueue("q").UniqueWhileActive(j => j.Key).PerResource(j => "tenant:" + j.Key, 1).MaxConcurrent(3));
        var prepared = preparer.Prepare(new MutableJob { Text = "a\"b\\c\n\u0001/\u2028é😀", Key = "k" }, new DispatchOptions
        {
            NotBefore = TestHost.Now.AddMinutes(1),
            Metadata = new Dictionary<string, string> { ["zeta"] = "1", ["alpha"] = "x/y", ["Beta"] = "é" },
        });

        var export = prepared.Export();
        var imported = TestHost.Import(export.Span);

        Assert.Equal(prepared.OperationId, imported.OperationId);
        Assert.Equal(TestHost.Hex(prepared.Digests.Command), TestHost.Hex(imported.Digests.Command));
        Assert.Equal(export.ToArray(), imported.Export().ToArray());
        Assert.Equal(3, imported.Intent.ConcurrencyScopes.Count);
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Export_escapes_strings_like_JSON_stringify()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<PlainJob>());
        var prepared = preparer.Prepare(new PlainJob("x"), new DispatchOptions
        {
            Metadata = new Dictionary<string, string> { ["k"] = "q\"b\\s/" + "\u2028" + "é" },
        });

        var text = TestHost.Utf8(prepared.Export());

        Assert.Contains("\"businessMetadata\":{\"k\":\"q\\\"b\\\\s/\u2028é\"}", text, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------ WV-11

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_application_mismatch() =>
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeApplicationMismatch,
            TestHost.ImportFails(Tampered("\"application\":\"billing\"", "\"application\":\"shipping\"")).Code);

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_store_mismatch()
    {
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeStoreMismatch,
            TestHost.ImportFails(Tampered("5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e5f", "5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e50")).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeStoreMismatch,
            TestHost.ImportFails(TestHost.Utf8(GvC0Text), storeId: Guid.CreateVersion7()).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeApplicationMismatch,
            TestHost.ImportFails(TestHost.Utf8(GvC0Text), application: "Billing").Code);
    }

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_unsupported_versions()
    {
        foreach (var (from, to) in new[]
        {
            ("\"formatVersion\":1", "\"formatVersion\":2"),
            ("\"fingerprintVersion\":1", "\"fingerprintVersion\":2"),
            ("\"operationVersion\":1", "\"operationVersion\":2"),
            ("\"payloadEncodingVersion\":1", "\"payloadEncodingVersion\":2"),
            ("\"operation\":\"Dispatch\"", "\"operation\":\"DispatchMany\""),
            ("\"format\":\"runwick.prepared-dispatch\"", "\"format\":\"runwick.prepared-dispatch2\""),
        })
        {
            Assert.Equal(RunwickErrorCodes.PreparedEnvelopeUnsupportedVersion, TestHost.ImportFails(Tampered(from, to)).Code);
        }
    }

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_profile_payload_and_digest_changes_are_digest_mismatches()
    {
        var payload = Convert.ToBase64String(TestHost.Utf8("""{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":"em"}"""));
        foreach (var bytes in new[]
        {
            Tampered("\"serializerProfileVersion\":1", "\"serializerProfileVersion\":2"),
            Tampered("eyJ1c2VySWQiOiIwMTkyZjNhMS03YzRlLTdiMmEtOWQxMC0zZTVmNmE3YjhjOWQiLCJsb2NhbGUiOiJlbiJ9", payload),
            Tampered("\"businessDigest\":\"e7", "\"businessDigest\":\"e8"),
            Tampered("\"policyDigest\":\"b6", "\"policyDigest\":\"b7"),
            Tampered("\"commandDigest\":\"61", "\"commandDigest\":\"62"),
            Tampered("\"notAfter\":1793354700000", "\"notAfter\":1793354700001"),
            Tampered("000000000002", "000000000003"),
            Tampered("\"maxExecutions\":3", "\"maxExecutions\":4"),
            Tampered("\"tenant\":\"acme\"", "\"tenant\":\"acmf\""),
            Tampered("-01\"}", "-00\"}"),
        })
        {
            Assert.Equal(RunwickErrorCodes.PreparedEnvelopeDigestMismatch, TestHost.ImportFails(bytes).Code);
        }
    }

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_unknown_or_missing_member_is_invalid()
    {
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o["extra"] = 1)).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o.Remove("queue"))).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o["policy"]!.AsObject()["extra"] = 1)).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o["eligibility"] = new JsonObject())).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o["uniqueness"]!.AsObject().Remove("window"))).Code);
    }

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_non_canonical_base64_is_invalid()
    {
        // QR== decodes to "A" like QQ== but has nonzero unused bits.
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o["payloadBase64"] = "QR==")).Code);

        var bytes = TestHost.Utf8("""{"a":1}""");
        var canonical = Convert.ToBase64String(bytes);
        Assert.EndsWith("fQ==", canonical, StringComparison.Ordinal);
        foreach (var variant in new[] { canonical[..^3] + "R==", canonical.TrimEnd('='), canonical + "\n", " " + canonical, canonical.Replace("e", "-", StringComparison.Ordinal) })
        {
            Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(Mutate(o => o["payloadBase64"] = variant)).Code);
        }
    }

    [Fact]
    [Trait("Case", "WV-11")]
    public void WV11_envelope_size_boundary()
    {
        var padded = GvC0Text + new string(' ', PayloadBounds.MaxEnvelopeBytes - GvC0Text.Length);
        Assert.Equal(2_097_152, Encoding.UTF8.GetByteCount(padded));
        TestHost.Import(TestHost.Utf8(padded));

        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeTooLarge, TestHost.ImportFails(TestHost.Utf8(padded + " ")).Code);
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Strict_parse_rejects_malformed_envelopes()
    {
        var text = GvC0Text;
        foreach (var bad in new[]
        {
            TestHost.Utf8(text[..^1] + ",}"),
            TestHost.Utf8(text + "//"),
            TestHost.Utf8(text.Replace("{\"format\"", "{\"format\":\"x\",\"format\"", StringComparison.Ordinal)),
            TestHost.Utf8(text.Replace("{\"format\"", "{\"\\" + "u0066ormat\":\"x\",\"format\"", StringComparison.Ordinal)),
            [0xEF, 0xBB, 0xBF, .. TestHost.Utf8(text)],
            TestHost.Utf8("[" + text + "]"),
            TestHost.Utf8(text.Replace("\"tenant\":\"acme\"", "\"tenant\":\"\\" + "ud800\"", StringComparison.Ordinal)),
            [.. TestHost.Utf8(text.Replace("\"tenant\":\"acme\"", "\"tenant\":\"", StringComparison.Ordinal)).Take(text.IndexOf("\"tenant\":\"", StringComparison.Ordinal) + 10), 0xC3, 0x28, .. TestHost.Utf8("\"},\"diagnosticMetadata\":{}}")],
        })
        {
            Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, TestHost.ImportFails(bad).Code);
        }
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Schema_types_ranges_and_rules_are_enforced()
    {
        var index = 0;
        foreach (var mutate in new Action<JsonObject>[]
        {
            o => o["payloadVersion"] = "2",
            o => o["payloadVersion"] = JsonNode.Parse("2.0"),
            o => o["payloadVersion"] = 3_000_000_000L,
            o => o["payloadVersion"] = 0,
            o => o["notAfter"] = JsonNode.Parse("1e3"),
            o => o["storeId"] = "5D0C6F8E-2B7A-4C1D-9E3F-0A1B2C3D4E5F",
            o => o["jobName"] = "bad name",
            o => o["queue"] = "",
            o => o["priority"] = "urgent",
            o => o["eligibility"] = JsonNode.Parse("""{"mode":"later"}"""),
            o => o["concurrencyScopes"] = JsonNode.Parse("""[{"kind":"resource","key":"b"},{"kind":"queue","key":"emails"}]"""),
            o => o["concurrencyScopes"] = JsonNode.Parse("""[{"kind":"pool","key":"emails"}]"""),
            o => o["rateScopes"] = JsonNode.Parse("""[{"kind":"queue","key":"emails"}]"""),
            o => o["uniqueness"] = JsonNode.Parse("""{"activeKey":null,"windowKey":"w","window":null}"""),
            o => o["uniqueness"] = JsonNode.Parse("""{"activeKey":"","windowKey":null,"window":null}"""),
            o => o["diagnosticMetadata"] = JsonNode.Parse("""{"baggage":"x"}"""),
            o => o["policy"]!["maxExecutions"] = 0,
            o => o["policy"]!["maxExecutions"] = 101,
            o => o["policy"]!["maxConsecutiveUnstartedAbandonments"] = 101,
            o => o["policy"]!["executionTimeout"] = 2_592_000_001L,
            o => o["policy"]!["executionTimeout"] = 0,
            o => o["policy"]!["retry"]!["initialDelay"] = 0,
            o => o["policy"]!["retry"]!["maximumDelay"] = 86_400_001,
            o => o["policy"]!["unstartedRetry"]!["initialDelay"] = 40_000,
            o => o["policy"]!["retry"]!["backoff"] = "linear",
            o => o["policy"]!["retry"]!["evaluatorVersion"] = 2,
            o => o["policy"]!["retry"]!["jitterKind"] = "full",
            o => o["businessDigest"] = "E74516838955FECC9EBB1A6D278844BF61698119E303D4D8E5BFFE4C8EDE8277",
        })
        {
            var bytes = Mutate(mutate);
            var ex = Record.Exception(() => TestHost.Import(bytes));
            Assert.True(ex is DispatchValidationException { Code: RunwickErrorCodes.PreparedEnvelopeInvalid }, $"mutation #{index}: {ex}");
            index++;
        }
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Metadata_rules_use_the_prepare_codes_at_import()
    {
        Assert.Equal(RunwickErrorCodes.InvalidMetadata,
            TestHost.ImportFails(Mutate(o => o["businessMetadata"] = JsonNode.Parse("""{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}"""))).Code);
        Assert.Equal(RunwickErrorCodes.InvalidMetadata,
            TestHost.ImportFails(Mutate(o => o["diagnosticMetadata"] = JsonNode.Parse("""{"traceparent":"00-00000000000000000000000000000000-00f067aa0ba902b7-01"}"""))).Code);
        Assert.Equal(RunwickErrorCodes.InvalidMetadata,
            TestHost.ImportFails(Mutate(o => o["businessMetadata"] = new JsonObject { ["bad key"] = "v" })).Code);
        Assert.Equal(RunwickErrorCodes.MetadataTooLarge,
            TestHost.ImportFails(Mutate(o => o["businessMetadata"] = new JsonObject { ["tenant"] = new string('x', 16_384) })).Code);
        var many = new JsonObject();
        for (var i = 0; i < 32; i++)
        {
            many["k" + i] = "v";
        }

        Assert.Equal(RunwickErrorCodes.MetadataTooLarge, TestHost.ImportFails(Mutate(o => o["businessMetadata"] = many)).Code);
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Import_ignores_current_defaults_and_never_re_resolves_them()
    {
        // WV-08: export under the original defaults, "redeploy" with changed retry defaults, import.
        var original = TestHost.Preparer(b => b.AddJob<PlainJob>()).Prepare(new PlainJob("x"));
        var export = original.Export().ToArray();
        var redeployed = TestHost.Preparer(b =>
        {
            b.JobDefaults(d => d.WithRetryDelay(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(2)));
            b.AddJob<PlainJob>();
        });

        var imported = TestHost.Import(export);
        var fresh = redeployed.Prepare(new PlainJob("x"));

        Assert.Equal(original.OperationId, imported.OperationId);
        Assert.Equal(original.NotAfter, imported.NotAfter);
        Assert.Equal(original.Intent.Policy, imported.Intent.Policy);
        Assert.Equal(TestHost.Hex(original.Digests.Command), TestHost.Hex(imported.Digests.Command));
        Assert.Equal(TestHost.Hex(original.Digests.Business), TestHost.Hex(fresh.Digests.Business));
        Assert.NotEqual(TestHost.Hex(original.Digests.Policy), TestHost.Hex(fresh.Digests.Policy));
    }

    [Fact]
    [Trait("Case", "SER-10")]
    public void Expired_envelope_still_imports_command_expiry_is_decided_by_the_store()
    {
        var imported = TestHost.Import(TestHost.Utf8(GvC0Text));

        Assert.True(imported.NotAfter < DateTimeOffset.UtcNow || imported.NotAfter == DateTimeOffset.FromUnixTimeMilliseconds(1793354700000));
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1793354700000), imported.NotAfter);
    }

    // ------------------------------------------------------------------ batch

    [Fact]
    [Trait("Case", "SER-10")]
    public void Batch_round_trip_and_batch_rules()
    {
        var preparer = TestHost.Preparer(b =>
        {
            b.AddJob<PlainJob>();
            b.AddJob<OtherJob>();
        });
        var batch = preparer.PrepareMany(TestHost.StoreId, [new BatchDispatchItem(new PlainJob("a")), new BatchDispatchItem(new OtherJob(2)), new BatchDispatchItem(new PlainJob("c"))],
            new BatchDispatchOptions { ContinueOnConflict = true });
        var export = TestHost.Utf8(batch.Export());

        var imported = PreparedEnvelopeCodec.ReadBatch(TestHost.Utf8(export), TestHost.StoreId, "billing");
        Assert.Equal(batch.OperationId, imported.OperationId);
        Assert.Equal(TestHost.Hex(batch.CommandDigest), TestHost.Hex(imported.CommandDigest));
        Assert.True(imported.ContinueOnConflict);
        Assert.Equal(export, TestHost.Utf8(imported.Export()));

        DispatchValidationException Fails(string json) =>
            Assert.Throws<DispatchValidationException>(() => PreparedEnvelopeCodec.ReadBatch(TestHost.Utf8(json), TestHost.StoreId, "billing"));

        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, Fails(export.Replace("\"ordinal\":1", "\"ordinal\":2", StringComparison.Ordinal)).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeDigestMismatch, Fails(export.Replace("\"continueOnConflict\":true", "\"continueOnConflict\":false", StringComparison.Ordinal)).Code);
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeUnsupportedVersion, Assert.Throws<DispatchValidationException>(() => TestHost.Import(TestHost.Utf8(export))).Code);
        var node = JsonNode.Parse(export)!.AsObject();
        var items = node["items"]!.AsArray();
        var template = items[0]!;
        items.Clear();
        for (var i = 0; i < 101; i++)
        {
            var item = template.DeepClone();
            item["ordinal"] = i;
            items.Add(item);
        }

        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, Fails(node.ToJsonString()).Code);
        node["items"] = new JsonArray();
        Assert.Equal(RunwickErrorCodes.PreparedEnvelopeInvalid, Fails(node.ToJsonString()).Code);
    }
}
