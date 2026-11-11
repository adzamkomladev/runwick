using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Runwick.Core.Tests.Support;
using Runwick.Serialization;

namespace Runwick.Core.Tests;

/// <summary>
/// The published wire vectors (tests/vectors/wire, produced by the independent Node encoder
/// tests/vectors/tools/canonical.mjs) reproduced byte-for-byte by the C# encoder (spec 02 §12, §13).
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-004")]
public sealed class WireVectorTests
{
    const string GvB0Business = "e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277";
    const string GvP0Policy = "b6af7a208ee69df3802b637b02e52582dd2525eaf10b99720be012e0c69b91b3";
    const string GvC0Command = "6103715514c292481704a7901e98ccccaa7ead786c69b7c742aee6746806a362";

    /// <summary>Export rendering of GV-C0 as printed in spec 02 §12.2 (1,510 bytes).</summary>
    internal const string GvC0Export = """{"format":"runwick.prepared-dispatch","formatVersion":1,"fingerprintVersion":1,"storeId":"5d0c6f8e-2b7a-4c1d-9e3f-0a1b2c3d4e5f","application":"billing","operation":"Dispatch","operationVersion":1,"operationId":"0192f3a1-8000-7000-8000-000000000001","notAfter":1793354700000,"proposedJobId":"0192f3a1-8000-7000-8000-000000000002","jobName":"send-welcome-email","payloadVersion":2,"serializerProfileId":"runwick.json","serializerProfileVersion":1,"payloadEncodingVersion":1,"payloadBase64":"eyJ1c2VySWQiOiIwMTkyZjNhMS03YzRlLTdiMmEtOWQxMC0zZTVmNmE3YjhjOWQiLCJsb2NhbGUiOiJlbiJ9","queue":"emails","priority":"high","eligibility":{"mode":"immediate"},"concurrencyScopes":[{"kind":"queue","key":"emails"}],"rateScopes":[],"uniqueness":{"activeKey":"user:42","windowKey":"req-7f3a","window":86400000},"businessMetadata":{"tenant":"acme"},"diagnosticMetadata":{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},"policy":{"maxExecutions":3,"executionTimeout":null,"retry":{"backoff":"exponential","evaluatorVersion":1,"jitterKind":"equal","initialDelay":5000,"maximumDelay":60000},"maxConsecutiveUnstartedAbandonments":5,"unstartedRetry":{"backoff":"exponential","evaluatorVersion":1,"jitterKind":"equal","initialDelay":1000,"maximumDelay":30000}},"businessDigest":"e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277","policyDigest":"b6af7a208ee69df3802b637b02e52582dd2525eaf10b99720be012e0c69b91b3","commandDigest":"6103715514c292481704a7901e98ccccaa7ead786c69b7c742aee6746806a362"}""";

    // ------------------------------------------------------------------ fixtures

    public static TheoryData<int> PrimitiveIndexes() => Indexes("primitives.json", "cases");

    public static TheoryData<string> RejectedIds() => Ids("payload-scanner.json", "rejected");

    public static TheoryData<string> AcceptedIds() => Ids("payload-scanner.json", "accepted");

    public static TheoryData<string> DispatchIds() => Ids("dispatch.json", "cases");

    static TheoryData<int> Indexes(string file, string array)
    {
        using var doc = Repo.LoadWireVector(file);
        var data = new TheoryData<int>();
        for (var i = 0; i < doc.RootElement.GetProperty(array).GetArrayLength(); i++)
        {
            data.Add(i);
        }

        return data;
    }

    static TheoryData<string> Ids(string file, string array)
    {
        using var doc = Repo.LoadWireVector(file);
        var data = new TheoryData<string>();
        foreach (var c in doc.RootElement.GetProperty(array).EnumerateArray())
        {
            data.Add(c.GetProperty("id").GetString()!);
        }

        return data;
    }

    static JsonElement Case(JsonDocument doc, string array, string id) =>
        doc.RootElement.GetProperty(array).EnumerateArray().Single(c => c.GetProperty("id").GetString() == id);

    /// <summary>Rebuilds the dispatch intent described by a dispatch.json <c>input</c> object.</summary>
    internal static (DispatchIntent Intent, Guid StoreId, string Application, Guid OperationId, long NotAfter) Input(JsonElement input)
    {
        static RetryPolicy Retry(JsonElement r) => new(
            r.GetProperty("backoff").GetString()!, r.GetProperty("evaluatorVersion").GetInt32(), r.GetProperty("jitterKind").GetString()!,
            r.GetProperty("initialDelay").GetInt64(), r.GetProperty("maximumDelay").GetInt64());
        static List<ScopeRef> Scopes(JsonElement a) => [.. a.EnumerateArray().Select(s => new ScopeRef(s.GetProperty("kind").GetString()!, s.GetProperty("key").GetString()!))];
        static Dictionary<string, string> Map(JsonElement o) => o.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString()!, StringComparer.Ordinal);
        static long? NullableLong(JsonElement e) => e.ValueKind == JsonValueKind.Null ? null : e.GetInt64();
        static string? NullableString(JsonElement e) => e.ValueKind == JsonValueKind.Null ? null : e.GetString();

        var p = input.GetProperty("policy");
        var policy = new ExecutionPolicy(
            p.GetProperty("maxExecutions").GetInt32(), NullableLong(p.GetProperty("executionTimeout")), Retry(p.GetProperty("retry")),
            p.GetProperty("maxConsecutiveUnstartedAbandonments").GetInt32(), Retry(p.GetProperty("unstartedRetry")));
        var intent = new DispatchIntent(
            input.GetProperty("jobName").GetString()!,
            input.GetProperty("payloadVersion").GetInt32(),
            input.GetProperty("serializerProfileId").GetString()!,
            input.GetProperty("serializerProfileVersion").GetInt32(),
            TestHost.Utf8(input.GetProperty("payload").GetString()!),
            input.GetProperty("queue").GetString()!,
            input.GetProperty("priority").GetString()!,
            NullableLong(input.GetProperty("notBeforeMs")),
            ScopeRef.Normalize(Scopes(input.GetProperty("concurrencyScopes"))),
            ScopeRef.Normalize(Scopes(input.GetProperty("rateScopes"))),
            NullableString(input.GetProperty("activeKey")),
            NullableString(input.GetProperty("windowKey")),
            NullableLong(input.GetProperty("windowMs")),
            Map(input.GetProperty("businessMetadata")),
            Map(input.GetProperty("diagnosticMetadata")),
            policy,
            Guid.Parse(input.GetProperty("proposedJobId").GetString()!));
        return (intent, Guid.Parse(input.GetProperty("storeId").GetString()!), input.GetProperty("application").GetString()!,
            Guid.Parse(input.GetProperty("operationId").GetString()!), input.GetProperty("notAfter").GetInt64());
    }

    internal static (DispatchIntent Intent, Guid StoreId, string Application, Guid OperationId, long NotAfter) Base()
    {
        using var doc = Repo.LoadWireVector("dispatch.json");
        return Input(Case(doc, "cases", "GV-B0").GetProperty("input"));
    }

    /// <summary>The GV-C0 prepared dispatch built from the base vector inputs.</summary>
    internal static PreparedDispatch GvC0()
    {
        var (intent, store, app, op, notAfter) = Base();
        return new PreparedDispatch(store, app, op, notAfter, intent, Fingerprints.ForDispatch(store, app, op, notAfter, intent));
    }

    // ------------------------------------------------------------------ primitives / scanner

    [Theory]
    [Trait("Case", "SER-11")]
    [MemberData(nameof(PrimitiveIndexes))]
    public void Primitive_encoding_matches_vector(int index)
    {
        using var doc = Repo.LoadWireVector("primitives.json");
        var c = doc.RootElement.GetProperty("cases")[index];
        var input = Convert.FromBase64String(c.GetProperty("inputBase64").GetString()!);

        var canonical = CanonicalEncoder.Encode(JsonPayloadScanner.Scan(input));

        Assert.Equal(c.GetProperty("canonicalHex").GetString(), TestHost.Hex(canonical));
    }

    [Theory]
    [Trait("Case", "SER-11")]
    [MemberData(nameof(RejectedIds))]
    public void Scanner_rejects_vector_with_its_code(string id)
    {
        using var doc = Repo.LoadWireVector("payload-scanner.json");
        var c = Case(doc, "rejected", id);
        var input = Convert.FromBase64String(c.GetProperty("inputBase64").GetString()!);

        var ex = Assert.Throws<PayloadValidationException>(() => JsonPayloadScanner.ScanPayload(input));
        var validation = Assert.Throws<PayloadValidationException>(() => JsonPayloadScanner.ValidatePayload(input));

        Assert.Equal(c.GetProperty("code").GetString(), ex.Code);
        Assert.Equal((ex.Code, ex.Message), (validation.Code, validation.Message));
    }

    [Theory]
    [Trait("Case", "SER-11")]
    [MemberData(nameof(AcceptedIds))]
    public void Scanner_accepts_vector_with_its_canonical_bytes(string id)
    {
        using var doc = Repo.LoadWireVector("payload-scanner.json");
        var c = Case(doc, "accepted", id);
        var input = Convert.FromBase64String(c.GetProperty("inputBase64").GetString()!);

        var canonical = CanonicalEncoder.Encode(JsonPayloadScanner.ScanPayload(input));
        JsonPayloadScanner.ValidatePayload(input);

        Assert.Equal(c.GetProperty("canonicalHex").GetString(), TestHost.Hex(canonical));
    }

    // ------------------------------------------------------------------ dispatch envelopes

    [Theory]
    [Trait("Case", "SER-11")]
    [MemberData(nameof(DispatchIds))]
    public void Dispatch_vector_bytes_and_digests_match(string id)
    {
        using var doc = Repo.LoadWireVector("dispatch.json");
        var c = Case(doc, "cases", id);
        var (intent, store, app, op, notAfter) = Input(c.GetProperty("input"));

        var business = CanonicalEncoder.Encode(Fingerprints.Business(app, intent));
        var policy = CanonicalEncoder.Encode(Fingerprints.Policy(intent.Policy));
        var digests = Fingerprints.ForDispatch(store, app, op, notAfter, intent);

        Assert.Equal(c.GetProperty("businessCanonicalHex").GetString(), TestHost.Hex(business));
        Assert.Equal(c.GetProperty("policyCanonicalHex").GetString(), TestHost.Hex(policy));
        Assert.Equal(c.GetProperty("businessDigest").GetString(), TestHost.Hex(digests.Business));
        Assert.Equal(c.GetProperty("policyDigest").GetString(), TestHost.Hex(digests.Policy));
        Assert.Equal(c.GetProperty("commandDigest").GetString(), TestHost.Hex(digests.Command));
        foreach (var expected in c.GetProperty("expect").EnumerateObject())
        {
            Assert.Equal(expected.Value.GetString(), c.GetProperty(expected.Name).GetString());
        }
    }

    [Fact]
    [Trait("Case", "SER-11")]
    public void Golden_GV_B0_P0_C0_lengths_and_digests()
    {
        var (intent, store, app, op, notAfter) = Base();

        var business = CanonicalEncoder.Encode(Fingerprints.Business(app, intent));
        var policy = CanonicalEncoder.Encode(Fingerprints.Policy(intent.Policy));
        var digests = Fingerprints.ForDispatch(store, app, op, notAfter, intent);

        Assert.Equal(605, business.Length);
        Assert.Equal(431, policy.Length);
        Assert.Equal(63, intent.PayloadUtf8.Length);
        Assert.Equal(GvB0Business, TestHost.Hex(digests.Business));
        Assert.Equal(GvP0Policy, TestHost.Hex(digests.Policy));
        Assert.Equal(GvC0Command, TestHost.Hex(digests.Command));
        Assert.Equal(GvP0Policy, TestHost.Hex(Fingerprints.Digest(Fingerprints.PolicyDomain, Fingerprints.Policy(ExecutionPolicy.Default))));
    }

    [Fact]
    [Trait("Case", "SER-11")]
    public void Golden_GV_C0_export_rendering_is_byte_exact()
    {
        var export = GvC0().Export();

        Assert.Equal(1510, export.Length);
        Assert.Equal(GvC0Export, TestHost.Utf8(export));
    }

    [Fact]
    [Trait("Case", "SER-11")]
    public void Independent_node_encoder_verifies_published_vectors()
    {
        var script = Path.Combine(Repo.Root, "tests", "vectors", "tools", "canonical.mjs");
        ProcessStartInfo start = new("node", ["--no-warnings", script, "--verify"])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = Repo.Root,
        };
        Process? process;
        try
        {
            process = Process.Start(start);
        }
        catch (System.ComponentModel.Win32Exception)
        {
            Assert.Skip("node is not installed; CI runs canonical.mjs --verify separately.");
            return;
        }

        using (process)
        {
            Assert.NotNull(process);
            var output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
            process.WaitForExit();
            Assert.True(process.ExitCode == 0, output);
            Assert.Contains("vectors OK", output, StringComparison.Ordinal);
        }
    }

    // ------------------------------------------------------------------ WV rows

    [Fact]
    [Trait("Case", "WV-01")]
    public void WV01_object_order_and_whitespace_do_not_matter_array_order_does()
    {
        var a = Business("""{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":"en"}""");
        var b = Business("{ \"locale\" : \"en\",\n  \"userId\" : \"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d\" }");
        var c = Business("""{"list":[1,2]}""");
        var d = Business("""{"list":[2,1]}""");

        Assert.Equal(GvB0Business, a);
        Assert.Equal(a, b);
        Assert.NotEqual(c, d);
    }

    [Fact]
    [Trait("Case", "WV-02")]
    public void WV02_escapes_equal_and_composed_differs_from_decomposed()
    {
        Assert.Equal(Business("""{"a":"a"}"""), Business("""{"a":"\u0061"}"""));
        Assert.NotEqual(Business("{\"a\":\"\u00e9\"}"), Business("{\"a\":\"e\u0301\"}"));
        Assert.Equal(Business("""{"a":"\/"}"""), Business("""{"a":"/"}"""));
    }

    [Fact]
    [Trait("Case", "WV-03")]
    public void WV03_number_lexemes_are_distinct_and_preserved()
    {
        string[] spellings = ["1", "1.0", "1e0", "1E0", "0", "-0"];
        var digests = spellings.Select(s => Business("{\"n\":" + s + "}")).ToList();
        Assert.Equal(spellings.Length, digests.Distinct().Count());

        foreach (var lexeme in new[] { "12345678901234567890123", "0.1000000000000000055511151231257827", new string('9', 128) })
        {
            var canonical = CanonicalEncoder.Encode(JsonPayloadScanner.ScanPayload(TestHost.Utf8("{\"n\":" + lexeme + "}")));
            Assert.Contains(Convert.ToHexStringLower(TestHost.Utf8(lexeme)), TestHost.Hex(canonical), StringComparison.Ordinal);
        }

        Assert.Equal("payload_number_too_long",
            Assert.Throws<PayloadValidationException>(() => JsonPayloadScanner.ScanPayload(TestHost.Utf8("{\"n\":" + new string('9', 129) + "}"))).Code);
    }

    [Fact]
    [Trait("Case", "WV-04")]
    public void WV04_null_absent_defaults_and_eligibility()
    {
        var (intent, _, app, _, _) = Base();
        var withNull = intent with { PayloadUtf8 = TestHost.Utf8("""{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":null}""") };
        var absent = intent with { PayloadUtf8 = TestHost.Utf8("""{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d"}""") };
        Assert.NotEqual(BusinessDigest(app, withNull), BusinessDigest(app, absent));

        var explicitDefaults = new ExecutionPolicy(3, null, new RetryPolicy("exponential", 1, "equal", 5000, 60000), 5,
            new RetryPolicy("exponential", 1, "equal", 1000, 30000));
        Assert.Equal(GvP0Policy, PolicyDigest(explicitDefaults));
        var timeout = intent.Policy with { ExecutionTimeoutMs = 30_000 };
        Assert.NotEqual(GvP0Policy, PolicyDigest(timeout));
        Assert.Equal(GvB0Business, BusinessDigest(app, intent with { Policy = timeout }));

        var immediate = BusinessDigest(app, intent);
        var past = BusinessDigest(app, intent with { NotBeforeMs = 1_000 });
        var future = BusinessDigest(app, intent with { NotBeforeMs = 4_000_000_000_000 });
        Assert.Equal(3, new[] { immediate, past, future }.Distinct().Count());
    }

    [Fact]
    [Trait("Case", "WV-05")]
    public void WV05_instants_normalize_to_ms_with_ceil_and_floor()
    {
        var z = DateTimeOffset.Parse("2026-10-30T10:00:00Z", CultureInfo.InvariantCulture);
        var plus2 = DateTimeOffset.Parse("2026-10-30T12:00:00+02:00", CultureInfo.InvariantCulture);
        var remainder = DateTimeOffset.Parse("2026-10-30T10:00:00.0001Z", CultureInfo.InvariantCulture);
        var notAfter = DateTimeOffset.Parse("2026-10-30T10:00:00.9999Z", CultureInfo.InvariantCulture);

        Assert.Equal(1793354400000, Fingerprints.CeilEpochMs(z));
        Assert.Equal(1793354400000, Fingerprints.CeilEpochMs(plus2));
        Assert.Equal(1793354400001, Fingerprints.CeilEpochMs(remainder));
        Assert.Equal(1793354400999, Fingerprints.FloorEpochMs(notAfter));
        Assert.Equal(-1, Fingerprints.FloorEpochMs(DateTimeOffset.UnixEpoch.AddTicks(-1)));
        Assert.Equal(0, Fingerprints.CeilEpochMs(DateTimeOffset.UnixEpoch.AddTicks(-1)));

        var (intent, _, app, _, _) = Base();
        Assert.Equal("b78eef3cb5297df3040045bc877ff479612b2571fcdd425e24f4e3b211a74c67",
            BusinessDigest(app, intent with { NotBeforeMs = Fingerprints.CeilEpochMs(plus2) }));
        Assert.Equal("e74d140895560330d2f42041db9198244bb451f1579ad88a4b8c024c4f59210a",
            BusinessDigest(app, intent with { NotBeforeMs = Fingerprints.CeilEpochMs(remainder) }));
    }

    [Fact]
    [Trait("Case", "WV-06")]
    public void WV06_trace_changes_only_the_command_digest_tenant_changes_business()
    {
        var (intent, store, app, op, notAfter) = Base();
        var trace = intent with { DiagnosticMetadata = new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01" } };
        var tenant = intent with { BusinessMetadata = new Dictionary<string, string> { ["tenant"] = "globex" } };

        var d0 = Fingerprints.ForDispatch(store, app, op, notAfter, intent);
        var d1 = Fingerprints.ForDispatch(store, app, op, notAfter, trace);
        var d2 = Fingerprints.ForDispatch(store, app, op, notAfter, tenant);

        Assert.Equal(d0.Business, d1.Business);
        Assert.Equal(d0.Policy, d1.Policy);
        Assert.Equal("87f00f288c36013da7e3b20b83cdeba13acf13a9f6506795bff7fbc018cd141d", TestHost.Hex(d1.Command));
        Assert.Equal("227b7a85093aecaca215a40ad29cbe60c9112794fa4f37298a2f934d7279a407", TestHost.Hex(d2.Business));
    }

    [Fact]
    [Trait("Case", "WV-07")]
    public void WV07_route_and_scope_change_business_budget_and_timeout_change_policy_only()
    {
        var (intent, _, app, _, _) = Base();
        Assert.Equal("20559a776a6a5c0b9677c0262b4bd09596699bf5e5cc1fdba7fd01b8f7d19f56", BusinessDigest(app, intent with { Queue = "bulk" }));
        Assert.NotEqual(GvB0Business, BusinessDigest(app, intent with { Priority = "low" }));
        Assert.NotEqual(GvB0Business, BusinessDigest(app, intent with
        {
            ConcurrencyScopes = ScopeRef.Normalize([new ScopeRef("resource", "tenant:acme"), new ScopeRef("queue", "emails")]),
        }));

        foreach (var policy in new[] { intent.Policy with { MaxExecutions = 5 }, intent.Policy with { ExecutionTimeoutMs = 30_000 } })
        {
            Assert.Equal(GvB0Business, BusinessDigest(app, intent with { Policy = policy }));
            Assert.NotEqual(GvP0Policy, PolicyDigest(policy));
        }
    }

    [Fact]
    [Trait("Case", "WV-07")]
    public void Scope_membership_is_a_set_sorted_by_utf8_bytes()
    {
        var (intent, _, app, _, _) = Base();
        var a = intent with { ConcurrencyScopes = [new ScopeRef("resource", "r"), new ScopeRef("queue", "emails"), new ScopeRef("queue", "emails")] };
        var b = intent with { ConcurrencyScopes = [new ScopeRef("queue", "emails"), new ScopeRef("resource", "r")] };

        Assert.Equal(BusinessDigest(app, a), BusinessDigest(app, b));
        Assert.Equal(
            ["\uE000", "\U0001F600"],
            ScopeRef.Normalize([new ScopeRef("resource", "\U0001F600"), new ScopeRef("resource", "\uE000")]).Select(s => s.Key));
    }

    static string Business(string payload)
    {
        var (intent, _, app, _, _) = Base();
        return BusinessDigest(app, intent with { PayloadUtf8 = TestHost.Utf8(payload) });
    }

    static string BusinessDigest(string app, DispatchIntent intent) =>
        TestHost.Hex(Fingerprints.Digest(Fingerprints.BusinessDomain, Fingerprints.Business(app, intent)));

    static string PolicyDigest(ExecutionPolicy policy) =>
        TestHost.Hex(Fingerprints.Digest(Fingerprints.PolicyDomain, Fingerprints.Policy(policy)));
}
