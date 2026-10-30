using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Core.Tests.Support;
using Runwick.Registration;
using Runwick.Serialization;

namespace Runwick.Core.Tests;

/// <summary>Registry compilation and startup validation (spec 01 §4–§8, §5.8).</summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-004")]
public sealed class RegistryValidationTests
{
    static void Worker(RunwickBuilder b) => b.AddWorker(w => w.Pool("p", p => p.Queues("default")));

    static IReadOnlyList<string> Codes(Action<RunwickBuilder> configure) => TestHost.Codes(TestHost.FailApp(configure));

    // ------------------------------------------------------------------ application / store

    [Fact]
    [Trait("Case", "SER-01")]
    public void Application_is_required_and_single_valued()
    {
        Assert.Equal([RunwickErrorCodes.MissingApplication], TestHost.Codes(TestHost.Fail(b => b.UsePostgres("Host=x"))));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], TestHost.Codes(TestHost.Fail(b => b.Application("a").Application("b").UsePostgres("Host=x"))));
        TestHost.Compile(b => b.Application("a").Application("a").UsePostgres("Host=x"));
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void Exactly_one_store_is_required()
    {
        Assert.Equal([RunwickErrorCodes.MissingStore], TestHost.Codes(TestHost.Fail(b => b.Application("a"))));
        Assert.Equal([RunwickErrorCodes.DuplicateStore],
            TestHost.Codes(TestHost.Fail(b => b.Application("a").UsePostgres("Host=x").UsePostgres("Host=y"))));
        Assert.Contains(RunwickErrorCodes.MissingStore, TestHost.Codes(TestHost.Fail(b => b.Application("a").UsePostgres(""))));
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void Every_error_is_collected_into_one_exception()
    {
        var ex = TestHost.Fail(b =>
        {
            b.Application("bad name");
            b.AddJob<UnnamedJob>();
            b.AddJob<PlainJob>().WithMaxExecutions(0);
            b.ConfigureJob<OtherJob>();
            b.Queue("bad queue");
        });

        Assert.Equal(
            [RunwickErrorCodes.InvalidApplicationName, RunwickErrorCodes.InvalidPolicy, RunwickErrorCodes.InvalidQueueName,
                RunwickErrorCodes.JobNotRegistered, RunwickErrorCodes.MissingJobName, RunwickErrorCodes.MissingStore],
            ex.Errors.Select(e => e.Code).Order(StringComparer.Ordinal));
        Assert.Equal(RunwickErrorCodes.ConfigurationInvalid, ex.Code);
        Assert.Equal(ErrorDisposition.KnownNotApplied, ex.Disposition);
        Assert.All(ex.Errors, e => Assert.Contains(e.Code, ex.Message, StringComparison.Ordinal));
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void AddRunwick_twice_and_AddWorker_twice_fail()
    {
        var services = new ServiceCollection();
        services.AddRunwick(TestHost.App(_ => { }));
        var twice = Assert.Throws<RunwickConfigurationException>(() => services.AddRunwick(TestHost.App(_ => { })));
        Assert.Equal([RunwickErrorCodes.RunwickAlreadyAdded], TestHost.Codes(twice));

        Assert.Contains(RunwickErrorCodes.DuplicateWorker, Codes(b =>
        {
            Worker(b);
            Worker(b);
        }));
    }

    // ------------------------------------------------------------------ identity

    [Fact]
    [Trait("Case", "SER-01")]
    public void Job_identity_rules()
    {
        Assert.Equal([RunwickErrorCodes.MissingJobName], Codes(b => b.AddJob<UnnamedJob>()));
        Assert.Equal([RunwickErrorCodes.DuplicateJobName], Codes(b =>
        {
            b.AddJob<PlainJob>();
            b.AddJob<UnnamedJob>().Named("plain");
        }));
        Assert.Equal([RunwickErrorCodes.IdentityConflict], Codes(b => b.AddJob<WelcomeEmail>().AtVersion(3)));
        Assert.Equal([RunwickErrorCodes.InvalidPayloadVersion], Codes(b => b.AddJob<UnnamedJob>().Named("v0").AtVersion(0)));
        Assert.Equal([RunwickErrorCodes.JobNotRegistered], Codes(b => b.ConfigureJob<PlainJob>().WithMaxExecutions(2)));

        // ConfigureJob before or after registration contributes to the same fluent layer.
        var registry = TestHost.CompileApp(b =>
        {
            b.ConfigureJob<PlainJob>().WithPriority("low");
            b.AddJob<PlainJob>();
        });
        Assert.Equal(new EffectiveValue<string>("low", ConfigurationSource.Fluent), registry.Find("plain")!.Priority);
        Assert.Equal(typeof(PlainJob), registry.Get<PlainJob>().PayloadType);
        Assert.Null(registry.Find("Plain"));
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void Assembly_scanning_registers_only_named_contracts_and_handlers_register_their_jobs()
    {
        var registry = TestHost.CompileApp(b =>
        {
            b.AddJobsFromAssemblyContaining<Runwick.Samples.Contracts.SendWelcomeEmail>();
            b.AddHandler<PlainJob, PlainHandler>();
        });

        Assert.Equal(["email.welcome", "harness.work", "plain"], registry.Jobs.Select(j => j.Name));
        Assert.Equal(typeof(PlainHandler), registry.Find("plain")!.HandlerType);
        Assert.Null(registry.Find("email.welcome")!.HandlerType);
        Assert.Equal(["default", "emails"], registry.Queues);
    }

    [Fact]
    [Trait("Case", "SER-01")]
    public void Duplicate_handler_fails()
    {
        Assert.Equal([RunwickErrorCodes.DuplicateHandler], Codes(b => b.AddHandler<PlainJob, PlainHandler>().AddHandler<PlainJob, PlainHandler2>()));
        TestHost.CompileApp(b => b.AddHandler<PlainJob, PlainHandler>().AddHandler<PlainJob, PlainHandler>());
    }

    // ------------------------------------------------------------------ upgrades

    [Fact]
    [Trait("Case", "SER-08")]
    public void Upgrade_graph_validation()
    {
        Assert.Equal([RunwickErrorCodes.UpgradeGap], Codes(b => b.AddJob<ReportV3>().Upgrade<ReportV1, ReportV2>(1, 2, v => new ReportV2(v.Id, 0))));
        Assert.Equal([RunwickErrorCodes.UpgradeNotAdjacent], Codes(b => b.AddJob<ReportV3>().Upgrade<ReportV1, ReportV3>(1, 3, v => new ReportV3(v.Id, 0, "x"))));
        Assert.Equal([RunwickErrorCodes.UpgradeCycle], Codes(b => b.AddJob<ReportV3>().Upgrade<ReportV3, ReportV3>(3, 4, v => v)));
        Assert.Contains(RunwickErrorCodes.UpgradeDuplicateRoute, Codes(b => b.AddJob<ReportV3>()
            .Upgrade<ReportV2, ReportV3>(2, 3, v => new ReportV3(v.Id, v.Rows, "a"))
            .Upgrade<ReportV2, ReportV3>(2, 3, v => new ReportV3(v.Id, v.Rows, "b"))));
        Assert.Equal([RunwickErrorCodes.UpgradeTypeConflict], Codes(b => b.AddJob<ReportV3>().Upgrade<ReportV1, ReportV2>(2, 3, v => new ReportV2(v.Id, 0))));
        Assert.Equal([RunwickErrorCodes.UpgradeTypeConflict], Codes(b => b.AddJob<ReportV3>()
            .Upgrade<ReportV1, ReportV1>(1, 2, v => v)
            .Upgrade<ReportV2, ReportV3>(2, 3, v => new ReportV3(v.Id, v.Rows, "a"))));
        Assert.Equal([RunwickErrorCodes.UpgradeTypeConflict], Codes(b => b.AddJob<ReportV3>()
            .Upgrade<ReportV3, ReportV2>(1, 2, v => new ReportV2(v.Id, v.Rows))
            .Upgrade<ReportV2, ReportV3>(2, 3, v => new ReportV3(v.Id, v.Rows, "a"))));

        var ok = TestHost.CompileApp(b => b.AddJob<ReportV3>()
            .Upgrade<ReportV2, ReportV3>(2, 3, v => new ReportV3(v.Id, v.Rows, "a"))
            .Upgrade<ReportV1, ReportV2>(1, 2, v => new ReportV2(v.Id, 0)));
        Assert.Equal([1, 2, 3], ok.Find("report")!.DecodablePayloadVersions);
    }

    // ------------------------------------------------------------------ middleware

    [Fact]
    [Trait("Case", "SER-02")]
    public void Unknown_alias_fails_only_on_worker_hosts()
    {
        var producer = TestHost.CompileApp(b => b.AddJob<UnknownAliasJob>());
        Assert.Empty(producer.ByName("mw.unknown")!.MiddlewareChain);

        var ex = TestHost.FailApp(b =>
        {
            b.AddJob<UnknownAliasJob>();
            Worker(b);
        });
        var error = Assert.Single(ex.Errors);
        Assert.Equal(RunwickErrorCodes.UnknownMiddlewareAlias, error.Code);
        Assert.Contains("mw.unknown", error.Message, StringComparison.Ordinal);
        Assert.Contains("AddMiddleware<T>(\"missing\")", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Middleware_conflicts_and_duplicates_fail()
    {
        Assert.Contains(RunwickErrorCodes.MiddlewareAliasConflict, Codes(b => b.AddMiddleware<AuditMiddleware>("a").AddMiddleware<TenantMiddleware>("a")));
        Assert.Contains(RunwickErrorCodes.InvalidMiddlewareAlias, Codes(b => b.AddMiddleware<AuditMiddleware>("bad alias")));
        Assert.Equal([RunwickErrorCodes.DuplicateMiddleware], Codes(b =>
        {
            b.AddMiddleware<AuditMiddleware>("alpha").AddMiddleware<TenantMiddleware>("beta").AddMiddleware<PassMiddleware>("zeta");
            b.AddJob<MiddlewareJob>().UseMiddleware("beta");
        }));
        Assert.Equal([RunwickErrorCodes.DuplicateMiddleware], Codes(b =>
        {
            b.UseMiddleware<AuditMiddleware>();
            b.AddJob<PlainJob>().UseMiddleware<AuditMiddleware>();
        }));

        // The same alias→type binding repeated is idempotent; fluent middleware runs after attributes.
        var registry = TestHost.CompileApp(b =>
        {
            b.AddMiddleware<AuditMiddleware>("alpha").AddMiddleware<AuditMiddleware>("alpha").AddMiddleware<TenantMiddleware>("beta").AddMiddleware<PassMiddleware>("zeta");
            b.AddJob<MiddlewareJob>().UseMiddleware<MetricsMiddleware>();
            Worker(b);
        });
        Assert.Equal(["TenantMiddleware", "PassMiddleware", "AuditMiddleware", "MetricsMiddleware"], registry.Find("mw.attrs")!.Middleware);
    }

    // ------------------------------------------------------------------ scopes and uniqueness

    [Fact]
    [Trait("Case", "SER-02")]
    public void Scope_count_is_bounded_at_eight()
    {
        static void Scopes(RunwickBuilder b, int resources)
        {
            b.Queue("default", q => q.MaxConcurrent(2));
            var job = b.AddJob<PlainJob>().MaxConcurrent(3);
            for (var i = 0; i < resources; i++)
            {
                var n = i;
                job.PerResource(j => "r" + n + j.Text, 1);
            }
        }

        var registry = TestHost.CompileApp(b => Scopes(b, 6));
        Assert.Equal(8, registry.Find("plain")!.Scopes.Count);
        Assert.Equal([RunwickErrorCodes.TooManyScopes], Codes(b => Scopes(b, 7)));
        Assert.Equal([RunwickErrorCodes.InvalidScope], Codes(b => b.AddJob<PlainJob>().MaxConcurrent(0)));
        Assert.Equal([RunwickErrorCodes.InvalidScope], Codes(b => b.AddJob<PlainJob>().PerResource(j => j.Text, 0)));
        Assert.Equal([RunwickErrorCodes.InvalidScope], Codes(b => b.Queue("q", q => q.MaxConcurrent(0))));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b => b.Queue("q", q => q.MaxConcurrent(1)).Queue("q", q => q.MaxConcurrent(2))));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b => b.AddJob<PlainJob>().MaxConcurrent(1).MaxConcurrent(2)));

        var declared = registry.Find("plain")!.Scopes;
        Assert.Equal(new ScopeDeclaration("queue", "default", 2), declared[0]);
        Assert.Equal(new ScopeDeclaration("job-type", "plain", 3), declared[1]);
        Assert.Equal(new ScopeDeclaration("resource", null, 1), declared[2]);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Uniqueness_window_is_validated()
    {
        foreach (var window in new[] { TimeSpan.Zero, TimeSpan.FromMilliseconds(-1), TimeSpan.FromTicks(15_000), TimeSpan.FromDays(3650) + TimeSpan.FromMilliseconds(1) })
        {
            Assert.Equal([RunwickErrorCodes.InvalidUniqueness], Codes(b => b.AddJob<PlainJob>().UniqueWithinWindow(j => j.Text, window)));
        }

        TestHost.CompileApp(b => b.AddJob<PlainJob>().UniqueWithinWindow(j => j.Text, TimeSpan.FromDays(3650)));
        TestHost.CompileApp(b => b.AddJob<PlainJob>().UniqueWithinWindow(j => j.Text, TimeSpan.FromMilliseconds(1)));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b => b.AddJob<PlainJob>().UniqueWhileActive(j => j.Text).UniqueWhileActive(j => j.Text + "x")));
    }

    // ------------------------------------------------------------------ policy layers

    [Fact]
    [Trait("Case", "SER-02")]
    public void Same_layer_conflicts_fail_and_identical_repeats_are_idempotent()
    {
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b =>
        {
            b.AddJob<PlainJob>().ToQueue("a");
            b.ConfigureJob<PlainJob>().ToQueue("b");
        }));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b => b.AddJob<PlainJob>().WithExecutionTimeout(TimeSpan.FromSeconds(1)).WithoutExecutionTimeout()));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b => b.AddJob<PlainJob>().WithRetryDelay(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).WithRetryDelay(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))));

        // A JobDefaults conflict is reported once, whether or not a job inherits it.
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b => b.JobDefaults(d => d.WithPriority("low").WithPriority("high"))));
        Assert.Equal([RunwickErrorCodes.SameLayerConflict], Codes(b =>
        {
            b.JobDefaults(d => d.WithMaxExecutions(2)).JobDefaults(d => d.WithMaxExecutions(4));
            b.AddJob<PlainJob>();
            b.AddJob<OtherJob>();
        }));

        // Different layers never conflict: the later layer wins.
        var registry = TestHost.CompileApp(b =>
        {
            b.JobDefaults(d => d.WithExecutionTimeout(TimeSpan.FromSeconds(9)).WithMaxExecutions(2));
            b.AddJob<AttributedJob>().WithoutExecutionTimeout().WithPriority("medium").WithPriority("medium");
        });
        var job = registry.Find("attr.max")!;
        Assert.Equal(new EffectiveValue<TimeSpan?>(null, ConfigurationSource.Fluent), job.ExecutionTimeout);
        Assert.Equal(new EffectiveValue<int>(7, ConfigurationSource.Attribute), job.MaxExecutions);
        Assert.Equal(new EffectiveValue<string>("medium", ConfigurationSource.Fluent), job.Priority);
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Invalid_policy_values_fail()
    {
        Assert.Equal([RunwickErrorCodes.InvalidPriority], Codes(b => b.AddJob<PlainJob>().WithPriority("High")));
        Assert.Equal([RunwickErrorCodes.InvalidQueueName], Codes(b => b.AddJob<PlainJob>().ToQueue("bad queue")));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithExecutionTimeout(TimeSpan.FromTicks(15_000))));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithExecutionTimeout(TimeSpan.Zero)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithRetryDelay(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1))));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithMaxUnstartedAbandonments(0)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithUnstartedRecoveryDelay(TimeSpan.Zero, TimeSpan.FromSeconds(1))));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.JobDefaults(d => d.WithMaxExecutions(0)).AddJob<PlainJob>()));
    }

    [Fact]
    [Trait("Case", "SER-02")]
    public void Policy_bounds_match_the_storage_contract()
    {
        // Spec 03 I-6 / spec 04 job_generations CHECKs / spec 01 I-23.
        var ok = TestHost.CompileApp(b => b.AddJob<PlainJob>()
            .WithMaxExecutions(100)
            .WithMaxUnstartedAbandonments(100)
            .WithExecutionTimeout(TimeSpan.FromDays(30))
            .WithRetryDelay(TimeSpan.FromMilliseconds(1), TimeSpan.FromHours(24))
            .WithUnstartedRecoveryDelay(TimeSpan.FromHours(24), TimeSpan.FromHours(24)));
        Assert.Equal(new ExecutionPolicy(100, 2_592_000_000, new RetryPolicy("exponential", 1, "equal", 1, 86_400_000), 100,
            new RetryPolicy("exponential", 1, "equal", 86_400_000, 86_400_000)), ok.ByName("plain")!.Policy);
        TestHost.CompileApp(b => b.AddJob<PlainJob>().WithMaxExecutions(1).WithMaxUnstartedAbandonments(1));

        var oneMs = TimeSpan.FromMilliseconds(1);
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithMaxExecutions(101)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<AttributedJob>().WithMaxExecutions(101)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithMaxUnstartedAbandonments(101)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithExecutionTimeout(TimeSpan.FromDays(30) + oneMs)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithRetryDelay(oneMs, TimeSpan.FromHours(24) + oneMs)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<PlainJob>().WithUnstartedRecoveryDelay(oneMs, TimeSpan.FromHours(24) + oneMs)));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.JobDefaults(d => d.WithMaxExecutions(101)).AddJob<PlainJob>()));
        Assert.Null(PolicyBounds.Violation(ok.ByName("plain")!.Policy));
    }

    // ------------------------------------------------------------------ capability bound

    static void Generated(RunwickBuilder b, int count)
    {
        var markers = typeof(M00).Assembly.GetTypes()
            .Where(t => t.Namespace == typeof(M00).Namespace && t.Name.Length == 3 && t.Name[0] == 'M' && char.IsAsciiDigit(t.Name[1]))
            .OrderBy(t => t.Name, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(23, markers.Length);
        var addJob = typeof(RunwickBuilder).GetMethod(nameof(RunwickBuilder.AddJob))!;
        var addHandler = typeof(RunwickBuilder).GetMethod(nameof(RunwickBuilder.AddHandler))!;
        var n = 0;
        foreach (var x in markers)
        {
            foreach (var y in markers)
            {
                if (n == count)
                {
                    return;
                }

                var job = typeof(GenJob<,>).MakeGenericType(x, y);
                var builder = addJob.MakeGenericMethod(job).Invoke(b, null)!;
                builder.GetType().GetMethod("Named")!.Invoke(builder, ["gen." + n.ToString("d3", System.Globalization.CultureInfo.InvariantCulture)]);
                addHandler.MakeGenericMethod(job, typeof(GenHandler<,>).MakeGenericType(x, y)).Invoke(b, null);
                n++;
            }
        }
    }

    [Fact]
    [Trait("Case", "CFG-01")]
    public void Worker_capability_set_is_bounded_at_512_tuples()
    {
        var at = TestHost.CompileApp(b =>
        {
            Generated(b, 512);
            Worker(b);
        });
        Assert.Equal(512, at.Capabilities.Count);

        var over = TestHost.FailApp(b =>
        {
            Generated(b, 513);
            Worker(b);
        });
        Assert.Equal([RunwickErrorCodes.CapabilitySetTooLarge], TestHost.Codes(over));

        // A producer-only host advertises nothing and is not bounded.
        Assert.Equal(513, TestHost.CompileApp(b => Generated(b, 513)).Jobs.Count);

        // Upgrade routes count: one job with three decodable versions is three tuples.
        static void Chain(RunwickBuilder b, int generated)
        {
            Generated(b, generated);
            b.AddJob<ReportV3>().Upgrade<ReportV1, ReportV2>(1, 2, v => new ReportV2(v.Id, 0)).Upgrade<ReportV2, ReportV3>(2, 3, v => new ReportV3(v.Id, v.Rows, "a"));
            b.AddHandler<ReportV3, ReportHandler>();
            Worker(b);
        }

        Assert.Equal(512, TestHost.CompileApp(b => Chain(b, 509)).Capabilities.Count);
        Assert.Equal([RunwickErrorCodes.CapabilitySetTooLarge], TestHost.Codes(TestHost.FailApp(b => Chain(b, 510))));
    }

    [Fact]
    [Trait("Case", "SER-09")]
    public void Converters_and_polymorphic_subtypes_are_inspected()
    {
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<ConverterJob>()));
        Assert.Equal([RunwickErrorCodes.InvalidPolicy], Codes(b => b.AddJob<RelaxedShapeJob>()));
    }
}

[JobName("converter")]
public sealed record ConverterJob([property: JsonConverter(typeof(JsonStringEnumConverter))] Mood Mood) : IQueueJob;

[JsonPolymorphic]
[JsonDerivedType(typeof(RelaxedCircle), "circle")]
public abstract record RelaxedShape;

public sealed record RelaxedCircle([property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] int Radius) : RelaxedShape;

[JobName("relaxed.shape")]
public sealed record RelaxedShapeJob(RelaxedShape Shape) : IQueueJob;
