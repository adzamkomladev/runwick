using System.Diagnostics;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Core.Tests.Support;
using Runwick.Serialization;

namespace Runwick.Core.Tests;

/// <summary>SER-01 … SER-05, SER-08, SER-09, SER-11 (Prepare end-to-end) and SER-12 Pure parts (spec 02 §14.1, spec 07 §SER).</summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-004")]
public sealed class SerializationCaseTests
{
    static readonly Guid UserId = Guid.Parse("0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d");

    static void WelcomeRegistration(RunwickBuilder b) =>
        b.Queue("emails", q => q.MaxConcurrent(4))
            .AddJob<WelcomeEmail>()
            .UniqueWhileActive(_ => "user:42")
            .UniqueWithinWindow(_ => "req-7f3a", TimeSpan.FromHours(24));

    static readonly DispatchOptions TenantAcme = new() { Metadata = new Dictionary<string, string> { ["tenant"] = "acme" } };

    // ------------------------------------------------------------------ SER-11 end-to-end

    [Fact]
    [Trait("Case", "SER-11")]
    public void Prepare_reproduces_GV_B0_GV_P0_and_GV_C0_end_to_end()
    {
        var preparer = TestHost.Preparer(WelcomeRegistration);
        using var activity = new Activity("gv").SetIdFormat(ActivityIdFormat.W3C).Start();

        var prepared = preparer.Prepare(new WelcomeEmail(UserId, "en"), TenantAcme);

        Assert.Equal("""{"userId":"0192f3a1-7c4e-7b2a-9d10-3e5f6a7b8c9d","locale":"en"}""", TestHost.Utf8(prepared.Intent.PayloadUtf8));
        Assert.Equal("e74516838955fecc9ebb1a6d278844bf61698119e303d4d8e5bffe4c8ede8277", TestHost.Hex(prepared.Digests.Business));
        Assert.Equal("b6af7a208ee69df3802b637b02e52582dd2525eaf10b99720be012e0c69b91b3", TestHost.Hex(prepared.Digests.Policy));
        Assert.Equal(activity.Id, prepared.Intent.DiagnosticMetadata["traceparent"]);
        Assert.Equal(TestHost.Now.AddMinutes(5), prepared.NotAfter);

        // Fix the generated identities to the vector's and the command digest is GV-C0.
        var intent = prepared.Intent with
        {
            ProposedJobId = Guid.Parse("0192f3a1-8000-7000-8000-000000000002"),
            DiagnosticMetadata = new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" },
        };
        var digests = Fingerprints.ForDispatch(prepared.StoreId, prepared.Application, Guid.Parse("0192f3a1-8000-7000-8000-000000000001"), 1793354700000, intent);
        Assert.Equal("6103715514c292481704a7901e98ccccaa7ead786c69b7c742aee6746806a362", TestHost.Hex(digests.Command));
    }

    [Fact]
    [Trait("Case", "SER-11")]
    public void Prepare_rounds_NotBefore_up_and_freezes_NotAfter_down()
    {
        var preparer = TestHost.Preparer(WelcomeRegistration);
        var notBefore = DateTimeOffset.Parse("2026-10-30T12:00:00.0001+02:00", System.Globalization.CultureInfo.InvariantCulture);

        var prepared = preparer.Prepare(new WelcomeEmail(UserId, "en"), TenantAcme with { NotBefore = notBefore });

        Assert.Equal(1793354400001, prepared.Intent.NotBeforeMs);
        Assert.Equal("e74d140895560330d2f42041db9198244bb451f1579ad88a4b8c024c4f59210a", TestHost.Hex(prepared.Digests.Business));
    }

    // ------------------------------------------------------------------ SER-01

    [Fact]
    [Trait("Case", "SER-01")]
    public void Missing_or_invalid_identity_fails_startup()
    {
        Assert.Contains(RunwickErrorCodes.MissingJobName, TestHost.Codes(TestHost.FailApp(b => b.AddJob<UnnamedJob>())));
        foreach (var bad in new[] { "", " plain", "plain ", "-plain", "pla in", "plä", "a/b", new string('j', 201) })
        {
            Assert.Contains(RunwickErrorCodes.InvalidJobName, TestHost.Codes(TestHost.FailApp(b => b.AddJob<UnnamedJob>().Named(bad))));
        }

        TestHost.CompileApp(b => b.AddJob<UnnamedJob>().Named(new string('j', 200)));
        foreach (var bad in new[] { "", " billing", new string('a', 129), "bill:ing" })
        {
            Assert.Contains(RunwickErrorCodes.InvalidApplicationName, TestHost.Codes(TestHost.Fail(b => b.Application(bad).UsePostgres("Host=x"))));
        }

        TestHost.Compile(b => b.Application(new string('a', 128)).UsePostgres("Host=x"));
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void Same_layer_identity_conflicts_fail_startup()
    {
        var name = TestHost.FailApp(b =>
        {
            b.AddJob<UnnamedJob>().Named("x");
            b.ConfigureJob<UnnamedJob>().Named("y");
        });
        var version = TestHost.FailApp(b => b.AddJob<PlainJob>().AtVersion(1).AtVersion(2));
        var attrVsFluent = TestHost.FailApp(b => b.AddJob<PlainJob>().Named("other-name"));

        Assert.Equal([RunwickErrorCodes.IdentityConflict], TestHost.Codes(name));
        Assert.Equal([RunwickErrorCodes.IdentityConflict], TestHost.Codes(version));
        Assert.Equal([RunwickErrorCodes.IdentityConflict], TestHost.Codes(attrVsFluent));

        // Repeating an identical declaration is idempotent.
        TestHost.CompileApp(b => b.AddJob<PlainJob>().Named("plain").AtVersion(1).AtVersion(1));
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void Renamed_clr_type_keeps_name_digests_and_capability_tuple()
    {
        var a = TestHost.CompileApp(WelcomeRegistration);
        var b = TestHost.CompileApp(r => r.Queue("emails", q => q.MaxConcurrent(4))
            .AddJob<WelcomeEmailRenamed>()
            .UniqueWhileActive(_ => "user:42")
            .UniqueWithinWindow(_ => "req-7f3a", TimeSpan.FromHours(24)));

        var pa = TestHost.Preparer(a).Prepare(new WelcomeEmail(UserId, "en"), TenantAcme);
        var pb = TestHost.Preparer(b).Prepare(new WelcomeEmailRenamed(UserId, "en"), TenantAcme);

        Assert.Equal(pa.JobName, pb.JobName);
        Assert.Equal(pa.Digests.Business, pb.Digests.Business);
        Assert.Equal(pa.Digests.Policy, pb.Digests.Policy);
        Assert.Equal(pa.Intent.PayloadUtf8, pb.Intent.PayloadUtf8);
        var ta = a.ByName("send-welcome-email")!;
        var tb = b.ByName("send-welcome-email")!;
        Assert.Equal((ta.Name, ta.CurrentVersion), (tb.Name, tb.CurrentVersion));
    }

    // ------------------------------------------------------------------ SER-02

    [Fact]
    [Trait("Case", "SER-02")]
    public void Effective_policy_follows_precedence_with_a_source_map()
    {
        var registry = TestHost.CompileApp(b =>
        {
            b.JobDefaults(d => d.ToQueue("app-q").WithPriority("high").WithMaxExecutions(4).WithRetryDelay(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(20)));
            b.AddJob<PlainJob>();
            b.AddJob<AttributedJob>().WithMaxExecutions(9).WithExecutionTimeout(TimeSpan.FromSeconds(30));
            b.AddJob<OtherJob>().WithoutExecutionTimeout().WithMaxUnstartedAbandonments(7);
        });

        var plain = registry.Find("plain")!;
        Assert.Equal(new EffectiveValue<string>("app-q", ConfigurationSource.ApplicationDefault), plain.Queue);
        Assert.Equal(new EffectiveValue<string>("high", ConfigurationSource.ApplicationDefault), plain.Priority);
        Assert.Equal(new EffectiveValue<int>(4, ConfigurationSource.ApplicationDefault), plain.MaxExecutions);
        Assert.Equal(new EffectiveValue<TimeSpan?>(null, ConfigurationSource.LibraryDefault), plain.ExecutionTimeout);
        Assert.Equal(ConfigurationSource.ApplicationDefault, plain.RetryDelay.Source);
        Assert.Equal(TimeSpan.FromSeconds(2), plain.RetryDelay.Value.InitialDelay);
        Assert.Equal(new EffectiveValue<int>(5, ConfigurationSource.LibraryDefault), plain.MaxUnstartedAbandonments);
        Assert.Equal(ConfigurationSource.LibraryDefault, plain.UnstartedRecoveryDelay.Source);
        Assert.Equal(new EffectiveValue<int>(1, ConfigurationSource.LibraryDefault), plain.PayloadVersion);

        var attributed = registry.Find("attr.max")!;
        Assert.Equal(new EffectiveValue<string>("attr-queue", ConfigurationSource.Attribute), attributed.Queue);
        Assert.Equal(new EffectiveValue<string>("low", ConfigurationSource.Attribute), attributed.Priority);
        Assert.Equal(new EffectiveValue<int>(9, ConfigurationSource.Fluent), attributed.MaxExecutions);
        Assert.Equal(new EffectiveValue<TimeSpan?>(TimeSpan.FromSeconds(30), ConfigurationSource.Fluent), attributed.ExecutionTimeout);

        var other = registry.Find("other")!;
        Assert.Equal(new EffectiveValue<TimeSpan?>(null, ConfigurationSource.Fluent), other.ExecutionTimeout);
        Assert.Equal(new EffectiveValue<int>(7, ConfigurationSource.Fluent), other.MaxUnstartedAbandonments);

        var compiled = registry.ByName("attr.max")!;
        Assert.Equal(new ExecutionPolicy(9, 30_000, new RetryPolicy("exponential", 1, "equal", 2_000, 20_000), 5, RetryPolicy.DefaultUnstarted), compiled.Policy);
        Assert.Equal(["app-q", "attr-queue"], registry.Queues);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Library_defaults_match_06_defaults()
    {
        var plain = TestHost.CompileApp(b => b.AddJob<PlainJob>());
        var job = plain.ByName("plain")!;

        Assert.Equal(ExecutionPolicy.Default, job.Policy);
        Assert.Equal("default", job.Queue);
        Assert.Equal("medium", job.Priority);
        Assert.Equal(new RetryDelayPolicy("exponential-v1", "equal", TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(60)), job.Descriptor.RetryDelay.Value);
        Assert.Equal(new RetryDelayPolicy("exponential-v1", "equal", TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)), job.Descriptor.UnstartedRecoveryDelay.Value);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Middleware_attribute_ties_resolve_by_ordinal_alias()
    {
        var registry = TestHost.CompileApp(b =>
        {
            b.UseMiddleware<MetricsMiddleware>();
            b.AddMiddleware<AuditMiddleware>("alpha").AddMiddleware<TenantMiddleware>("beta").AddMiddleware<PassMiddleware>("zeta");
            b.AddJob<MiddlewareJob>();
        });

        // Global, then attribute aliases by Order then ordinal alias: beta(1), zeta(1), alpha(2).
        Assert.Equal([typeof(MetricsMiddleware), typeof(TenantMiddleware), typeof(PassMiddleware), typeof(AuditMiddleware)],
            registry.ByName("mw.attrs")!.MiddlewareChain);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Dispatch_options_cannot_override_policy_or_disable_required_uniqueness()
    {
        // The dispatch surface carries no execution-policy member at all.
        Assert.Equal(["Metadata", "NotBefore", "Priority", "Queue", "Uniqueness"],
            typeof(DispatchOptions).GetProperties().Select(p => p.Name).Where(n => n != "EqualityContract").Order(StringComparer.Ordinal));

        var preparer = TestHost.Preparer(WelcomeRegistration);
        var job = new WelcomeEmail(UserId, "en");

        // Omitting or nulling the keys never disables a registration-derived dimension.
        var nulled = preparer.Prepare(job, TenantAcme with { Uniqueness = new DispatchUniqueness(null, null, null) });
        Assert.Equal(("user:42", "req-7f3a", 86_400_000L), (nulled.Intent.ActiveKey, nulled.Intent.WindowKey, nulled.Intent.WindowMs!.Value));

        Assert.Equal(RunwickErrorCodes.UniquenessKeyMismatch,
            preparer.PrepareFails(job, new DispatchOptions { Uniqueness = new DispatchUniqueness("user:43", null, null) }).Code);
        Assert.Equal(RunwickErrorCodes.UniquenessKeyMismatch,
            preparer.PrepareFails(job, new DispatchOptions { Uniqueness = new DispatchUniqueness(null, "req-7f3a", TimeSpan.FromHours(1)) }).Code);
        Assert.Equal(RunwickErrorCodes.UnknownQueue, preparer.PrepareFails(job, new DispatchOptions { Queue = "nowhere" }).Code);
        Assert.Equal(RunwickErrorCodes.InvalidPriority, preparer.PrepareFails(job, new DispatchOptions { Priority = "urgent" }).Code);

        var nullSelector = TestHost.Preparer(b => b.AddJob<PlainJob>().UniqueWhileActive(_ => null!));
        Assert.Equal(RunwickErrorCodes.UniquenessRequired, nullSelector.PrepareFails(new PlainJob("x")).Code);
        var throwing = TestHost.Preparer(b => b.AddJob<PlainJob>().PerResource(_ => throw new InvalidOperationException("boom"), 1));
        Assert.Equal(RunwickErrorCodes.KeySelectorFailed, throwing.PrepareFails(new PlainJob("x")).Code);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Repeated_runs_produce_byte_identical_policy_envelopes()
    {
        static byte[] Policy() =>
            CanonicalEncoder.Encode(Fingerprints.Policy(TestHost.Preparer(b => b.AddJob<AttributedJob>().WithExecutionTimeout(TimeSpan.FromSeconds(3)))
                .Prepare(new AttributedJob("x")).Intent.Policy));

        Assert.Equal(Policy(), Policy());
    }

    // ------------------------------------------------------------------ SER-04

    [Fact]
    [Trait("Case", "SER-04")]
    public void Profile_options_are_complete_explicit_and_read_only()
    {
        var o = RunwickJsonProfile.V1.Options;

        Assert.True(o.IsReadOnly);
        Assert.False(o.AllowDuplicateProperties);
        Assert.False(o.AllowOutOfOrderMetadataProperties);
        Assert.False(o.AllowTrailingCommas);
        Assert.Equal(JsonCommentHandling.Disallow, o.ReadCommentHandling);
        Assert.Equal(JsonNumberHandling.Strict, o.NumberHandling);
        Assert.Same(JsonNamingPolicy.CamelCase, o.PropertyNamingPolicy);
        Assert.Null(o.DictionaryKeyPolicy);
        Assert.False(o.PropertyNameCaseInsensitive);
        Assert.Equal(JsonUnmappedMemberHandling.Disallow, o.UnmappedMemberHandling);
        Assert.True(o.RespectNullableAnnotations);
        Assert.True(o.RespectRequiredConstructorParameters);
        Assert.Equal(JsonIgnoreCondition.Never, o.DefaultIgnoreCondition);
        Assert.False(o.IgnoreReadOnlyProperties);
        Assert.True(o.IgnoreReadOnlyFields);
        Assert.False(o.IncludeFields);
        Assert.Null(o.ReferenceHandler);
        Assert.Equal(32, o.MaxDepth);
        Assert.False(o.WriteIndented);
        Assert.Equal((' ', 2, "\n"), (o.IndentCharacter, o.IndentSize, o.NewLine));
        Assert.Same(JavaScriptEncoder.Default, o.Encoder);
        Assert.Equal(JsonUnknownTypeHandling.JsonElement, o.UnknownTypeHandling);
        Assert.Equal(JsonObjectCreationHandling.Replace, o.PreferredObjectCreationHandling);
        Assert.Equal(16_384, o.DefaultBufferSize);
        Assert.Equal(2, o.Converters.Count);
        Assert.IsType<JsonStringEnumConverter>(o.Converters[0]);

        Assert.Throws<InvalidOperationException>(() => o.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower);
        Assert.Throws<InvalidOperationException>(() => o.NumberHandling = JsonNumberHandling.AllowReadingFromString);
        Assert.Throws<InvalidOperationException>(() => o.Converters.Add(new JsonStringEnumConverter()));
    }

    [Fact]
    [Trait("Case", "SER-04")]
    public void Host_serializer_settings_never_change_stored_bytes_or_digests()
    {
        const string expected = "{\"count\":1,\"big\":2,\"name\":\"" + @"\u003Cb\u003E" + "\",\"at\":\"2026-10-30T10:00:00Z\",\"ratio\":0.5}";
        var job = new TypedJob(1, 2, "<b>", new DateTime(2026, 10, 30, 10, 0, 0, DateTimeKind.Utc), 0.5);
        var before = TestHost.Preparer(b => b.AddJob<TypedJob>()).Prepare(job);

        var hostOptions = new JsonSerializerOptions(JsonSerializerOptions.Web)
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseUpper,
            NumberHandling = JsonNumberHandling.WriteAsString,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };
        var services = new ServiceCollection();
        services.AddSingleton(hostOptions);
        services.Configure<JsonSerializerOptions>(o => o.PropertyNamingPolicy = JsonNamingPolicy.KebabCaseLower);
        services.AddRunwick(TestHost.App(b => b.AddJob<TypedJob>()));
        using var provider = services.BuildServiceProvider();
        var after = TestHost.Preparer(provider.GetRequiredService<Runwick.Registration.JobRegistry>()).Prepare(job);

        Assert.Equal(expected, TestHost.Utf8(before.Intent.PayloadUtf8));
        Assert.Equal(before.Intent.PayloadUtf8, after.Intent.PayloadUtf8);
        Assert.Equal(before.Digests.Business, after.Digests.Business);
        Assert.Equal(before.Digests.Policy, after.Digests.Policy);
    }

    [Fact]
    [Trait("Case", "SER-04")]
    public void Enums_are_member_names_and_integers_are_rejected_on_read()
    {
        var bytes = RunwickJsonProfile.V1.Serialize(new EnumJob(Mood.Angry), typeof(EnumJob)).Utf8;
        Assert.Equal("""{"mood":"Angry"}""", TestHost.Utf8(bytes));

        var ex = Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(TestHost.Utf8("""{"mood":1}"""), typeof(EnumJob)));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, ex.Code);
    }

    [Fact]
    [Trait("Case", "SER-04")]
    public void Utc_datetime_converter_is_strict()
    {
        var local = new TypedJob(1, 1, "n", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Local));
        var unspecified = new TypedJob(1, 1, "n", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified));
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Serialize(local, typeof(TypedJob))).Code);
        Assert.Equal(RunwickErrorCodes.PayloadInvalidValue, Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Serialize(unspecified, typeof(TypedJob))).Code);

        var fraction = new TypedJob(1, 1, "n", new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1_234_500));
        Assert.Contains("\"at\":\"2026-01-01T00:00:00.12345Z\"", TestHost.Utf8(RunwickJsonProfile.V1.Serialize(fraction, typeof(TypedJob)).Utf8), StringComparison.Ordinal);

        var decoded = (TypedJob)RunwickJsonProfile.V1.Deserialize(TestHost.Utf8("""{"count":1,"big":1,"name":"n","at":"2026-01-01T00:00:00.12345Z","ratio":0}"""), typeof(TypedJob));
        Assert.Equal(DateTimeKind.Utc, decoded.At!.Value.Kind);
        Assert.Equal(fraction.At, decoded.At);
        foreach (var bad in new[] { "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00", "2026-01-01 00:00:00Z", "2026-01-01T00:00:00.12345678Z" })
        {
            var json = "{\"count\":1,\"big\":1,\"name\":\"n\",\"at\":\"" + bad + "\",\"ratio\":0}";
            Assert.Equal(RunwickErrorCodes.PayloadInvalidValue,
                Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(TestHost.Utf8(json), typeof(TypedJob))).Code);
        }
    }

    // ------------------------------------------------------------------ SER-05

    [Fact]
    [Trait("Case", "SER-05")]
    public void Mutating_the_source_after_prepare_changes_nothing()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<MutableJob>().UniqueWithinWindow(j => j.Key, TimeSpan.FromMinutes(1)).PerResource(j => "r:" + j.Key, 1));
        var job = new MutableJob { Text = "before", Values = [1, 2], Key = "k1" };
        var prepared = preparer.Prepare(job);
        var bytes = prepared.Intent.PayloadUtf8.ToArray();
        var digests = Hexes(prepared.Digests);
        var export = prepared.Export().ToArray();

        job.Text = "after";
        job.Values.Add(3);
        job.Key = "k2";

        Assert.Equal(bytes, prepared.Intent.PayloadUtf8);
        Assert.Equal("k1", prepared.Intent.WindowKey);
        Assert.Equal([new ScopeRef("resource", "r:k1")], prepared.Intent.ConcurrencyScopes);
        Assert.Equal(digests, Hexes(prepared.Digests));
        Assert.Equal(export, prepared.Export().ToArray());
        Assert.DoesNotContain("before", prepared.ToString(), StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------ SER-08

    static void ReportChain(RunwickBuilder b, bool withV1 = true)
    {
        var job = b.AddJob<ReportV3>();
        if (withV1)
        {
            job.Upgrade<ReportV1, ReportV2>(1, 2, v1 => new ReportV2(v1.Id, 0));
        }

        job.Upgrade<ReportV2, ReportV3>(2, 3, v2 => new ReportV3(v2.Id, v2.Rows, "pdf"));
        b.AddHandler<ReportV3, ReportHandler>();
        b.AddWorker(w => w.Pool("p", p => p.Queues("default")));
    }

    [Fact]
    [Trait("Case", "SER-08")]
    public void Upgrade_chain_decodes_v1_through_v3_without_touching_stored_identity()
    {
        var registry = TestHost.CompileApp(b => ReportChain(b));
        var job = registry.ByName("report")!;
        var stored = TestHost.Utf8("""{"id":"r-1"}""");
        var intent = WireVectorTests.Base().Intent with { JobName = "report", PayloadVersion = 1, PayloadUtf8 = stored };
        var before = Fingerprints.ForDispatch(TestHost.StoreId, "billing", Guid.Empty, 1, intent);

        Assert.Equal([1, 2, 3], job.Descriptor.DecodablePayloadVersions);
        var route = job.Routes[1];
        Assert.Equal(typeof(ReportV1), route.DtoType);
        var dto = RunwickJsonProfile.V1.Deserialize(stored, route.DtoType);
        var current = Assert.IsType<ReportV3>(route.Upgrade(dto));

        Assert.Equal(new ReportV3("r-1", 0, "pdf"), current);
        Assert.Equal(new ReportV3("r-2", 5, "pdf"), job.Routes[2].Upgrade(RunwickJsonProfile.V1.Deserialize(TestHost.Utf8("""{"id":"r-2","rows":5}"""), job.Routes[2].DtoType)));
        Assert.Same(typeof(ReportV3), job.Routes[3].DtoType);
        Assert.Empty(job.Routes[3].Steps);
        Assert.Equal("""{"id":"r-1"}""", TestHost.Utf8(stored));
        var after = Fingerprints.ForDispatch(TestHost.StoreId, "billing", Guid.Empty, 1, intent);
        Assert.Equal(Hexes(before), Hexes(after));
        Assert.Equal([1, 2, 3], registry.Capabilities.Where(c => c.Name == "report").Select(c => c.PayloadVersion));
    }

    [Fact]
    [Trait("Case", "SER-08")]
    public void Removing_the_v1_decoder_withdraws_the_v1_capability_tuple()
    {
        var registry = TestHost.CompileApp(b => ReportChain(b, withV1: false));

        Assert.Equal([2, 3], registry.Capabilities.Where(c => c.Name == "report").Select(c => c.PayloadVersion));
        Assert.False(registry.ByName("report")!.Routes.ContainsKey(1));
        Assert.All(registry.Capabilities, c => Assert.Equal(("runwick.json", 1), (c.SerializerProfileId, c.SerializerProfileVersion)));
    }

    // ------------------------------------------------------------------ SER-09

    [Fact]
    [Trait("Case", "SER-09")]
    public void Unknown_discriminator_or_type_name_fails_decode_without_loading_types()
    {
        var circle = RunwickJsonProfile.V1.Serialize(new DrawJob(new Circle(2)), typeof(DrawJob)).Utf8;
        Assert.Equal("""{"shape":{"$type":"circle","radius":2}}""", TestHost.Utf8(circle));
        Assert.Equal(new DrawJob(new Circle(2)), RunwickJsonProfile.V1.Deserialize(circle, typeof(DrawJob)));

        Assert.ThrowsAny<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(TestHost.Utf8("""{"shape":{"$type":"x"}}"""), typeof(DrawJob)));
        foreach (var discriminator in new[] { "triangle", "Runwick.Core.Tests.Support.Square, Runwick.Core.Tests", "Evil.Payload, Evil.Assembly, Version=1.0.0.0" })
        {
            var json = "{\"shape\":{\"$type\":\"" + discriminator + "\",\"side\":1}}";
            var ex = Assert.Throws<PayloadValidationException>(() => RunwickJsonProfile.V1.Deserialize(TestHost.Utf8(json), typeof(DrawJob)));
            Assert.StartsWith("payload_", ex.Code, StringComparison.Ordinal);
        }

        Assert.DoesNotContain(AppDomain.CurrentDomain.GetAssemblies(), a => a.GetName().Name == "Evil.Assembly");
    }

    [Fact]
    [Trait("Case", "SER-09")]
    public void Registration_rejects_metadata_that_relaxes_the_profile()
    {
        var ex = TestHost.FailApp(b =>
        {
            b.AddJob<RelaxedNumbersJob>();
            b.AddJob<RelaxedMemberJob>();
            b.AddJob<ExtensionDataJob>();
            b.AddJob<SkipUnmappedJob>();
            b.AddJob<NestedRelaxedJob>();
            b.AddJob<ObjectMemberJob>();
            b.AddJob<ObjectElementJob>();
            b.AddJob<FlagsJob>();
        });

        var subjects = ex.Errors.Select(e => e.Subject).Distinct().Order(StringComparer.Ordinal).ToList();
        Assert.Equal(["flags", "object.element", "object.member", "relaxed.extension", "relaxed.member", "relaxed.nested", "relaxed.numbers", "relaxed.unmapped"], subjects);
        Assert.All(ex.Errors, e => Assert.Equal(RunwickErrorCodes.InvalidPolicy, e.Code));

        TestHost.CompileApp(b =>
        {
            b.AddJob<DrawJob>();
            b.AddJob<EnumJob>();
            b.AddJob<TypedJob>();
            b.AddJob<BoundsJob>();
        });
    }

    // ------------------------------------------------------------------ SER-12

    [Fact]
    [Trait("Case", "SER-12")]
    public void Prepare_allocation_is_linear_in_payload_size()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<BoundsJob>());
        var half = Payloads.OfSize(524_288);
        var full = Payloads.OfSize(1_048_576);
        Assert.Equal(1_048_576, Payloads.SerializedLength(full));
        preparer.Prepare(Payloads.OfSize(4_096));
        // Warm both sizes: a cold first call allocates one-time pool and cache state that the warmed one does not (ratio 3.0 cold, 2.19 steady).
        preparer.Prepare(half);
        preparer.Prepare(full);

        var halfBytes = Allocated(() => preparer.Prepare(half));
        var fullBytes = Allocated(() => preparer.Prepare(full));

        var ratio = fullBytes / (double)halfBytes;
        Assert.True(ratio <= 2.2, $"allocation ratio {ratio:F3} ({fullBytes:N0} / {halfBytes:N0})");
        Assert.True(fullBytes < 32L * 1_048_576, $"allocated {fullBytes:N0} bytes for a 1 MiB payload");
    }

    [Fact]
    [Trait("Case", "SER-12")]
    public void Import_allocation_is_linear_in_envelope_size()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<BoundsJob>());
        var half = preparer.Prepare(Payloads.OfSize(524_288)).Export().ToArray();
        var full = preparer.Prepare(Payloads.OfSize(1_048_576)).Export().ToArray();
        TestHost.Import(half);

        var halfBytes = Allocated(() => TestHost.Import(half));
        var fullBytes = Allocated(() => TestHost.Import(full));

        var ratio = fullBytes / (double)halfBytes;
        Assert.True(ratio <= 2.2, $"allocation ratio {ratio:F3} ({fullBytes:N0} / {halfBytes:N0})");
    }

    [Fact]
    [Trait("Case", "SER-12")]
    public void Over_limit_payload_is_rejected_with_bounded_buffering()
    {
        var preparer = TestHost.Preparer(b => b.AddJob<BoundsJob>());
        var huge = BoundsJob.WithParts([.. Enumerable.Range(0, 64).Select(i => new string('h', 262_144))]);
        preparer.Prepare(Payloads.OfSize(4_096));

        var ex = preparer.PrepareFails(huge);

        Assert.Equal(RunwickErrorCodes.PayloadTooLarge, ex.Code);
        var during = Allocated(() => preparer.PrepareFails(huge));
        Assert.True(during < 8L * 1_048_576, $"rejecting a 16 MiB payload allocated {during:N0} bytes");
    }

    [Fact]
    [Trait("Case", "SER-12")]
    public void Bounded_buffer_rejects_before_limit_plus_64_KiB_is_written()
    {
        using var buffer = new BoundedBufferWriter(PayloadBounds.MaxPayloadBytes);
        var written = 0;
        Assert.Throws<BoundedBufferWriter.LimitExceededException>((Action)(() =>
        {
            while (true)
            {
                var span = buffer.GetSpan(4_096);
                span[..4_096].Fill((byte)'x');
                buffer.Advance(4_096);
                written += 4_096;
            }
        }));

        Assert.True(written <= PayloadBounds.MaxPayloadBytes + 65_536, $"{written:N0} bytes were buffered");
        Assert.True(written >= PayloadBounds.MaxPayloadBytes, $"{written:N0} bytes were buffered");
    }

    static string Hexes(DispatchDigests d) => string.Join('/', TestHost.Hex(d.Business), TestHost.Hex(d.Policy), TestHost.Hex(d.Command));

    static long Allocated(Action action)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        action();
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }
}
