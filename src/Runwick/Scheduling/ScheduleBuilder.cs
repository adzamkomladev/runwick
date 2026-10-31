using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Runwick.Dispatching;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using Runwick.Serialization;

namespace Runwick;

/// <summary>Cron field layout of a schedule (12 §2): five fields, or six with seconds.</summary>
public enum ScheduleCronFormat
{
    FiveField,
    SixField,
}

/// <summary>Local times inside a forward DST transition (12 §7.2).</summary>
public enum ScheduleGap
{
    Skip,
    Shift,
}

/// <summary>Local times inside a backward DST transition (12 §7.3).</summary>
public enum ScheduleFold
{
    Earlier,
    Later,
    Both,
}

/// <summary>What happens to occurrences missed by more than the grace period (13 §5.2).</summary>
public enum MisfirePolicy
{
    RunOnce,
    Skip,
    CatchUp,
}

/// <summary>Whether occurrences of one schedule may execute concurrently (13 §7.1).</summary>
public enum ScheduleOverlap
{
    Allow,
    Serialize,
}

/// <summary>How a resumed schedule treats the occurrences it missed while paused (13 §6.3).</summary>
public enum ResumePolicy
{
    SkipPaused,
    RunOnce,
    CatchUp,
}

/// <summary>Scheduler role options (13 §10). Preview in M3.</summary>
public sealed class SchedulerOptions
{
    /// <summary>Poll interval while due work exists; idle back-off doubles to <see cref="IdlePollInterval"/>.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan IdlePollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Local job allowance per scheduler/store group (13 §10).</summary>
    public int JobsPerSecond { get; set; } = 100;

    public int Burst { get; set; } = 32;
}

/// <summary>Registration of the scheduler role and of schedules (13 §2). Preview in M3.</summary>
public static class RunwickSchedulerExtensions
{
    /// <summary>Runs the scheduler role in this host: it seeds declared schedules and materializes due occurrences.</summary>
    public static RunwickBuilder AddScheduler(this RunwickBuilder builder, Action<SchedulerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var declarations = Declarations(builder);
        if (declarations.SchedulerAdded)
        {
            builder.EarlyErrors.Add(new ConfigurationError("duplicate_scheduler", "scheduler", "AddScheduler may be called once."));
            return builder;
        }

        declarations.SchedulerAdded = true;
        configure?.Invoke(declarations.Options);
        builder.Services.AddSingleton(sp => ActivatorUtilities.CreateInstance<SchedulerService>(sp, declarations));
        builder.Services.AddSingleton<Hosting.IRunwickComponent>(sp => sp.GetRequiredService<SchedulerService>());
        return builder;
    }

    /// <summary>Declares one schedule of <typeparamref name="TJob"/>; the payload is frozen when the scheduler seeds it.</summary>
    public static ScheduleBuilder<TJob> AddSchedule<TJob>(this RunwickBuilder builder, string key, TJob job) where TJob : class, IQueueJob
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(job);
        var declarations = Declarations(builder);
        var declaration = new ScheduleDeclaration(key, job);
        if (declarations.Schedules.Any(s => s.Key == key))
        {
            builder.EarlyErrors.Add(new ConfigurationError("duplicate_schedule", key, "A schedule key may be declared once."));
        }

        declarations.Schedules.Add(declaration);
        return new ScheduleBuilder<TJob>(declaration);
    }

    static ScheduleDeclarations Declarations(RunwickBuilder builder)
    {
        var existing = builder.Services.FirstOrDefault(d => d.ServiceType == typeof(ScheduleDeclarations))?.ImplementationInstance;
        if (existing is ScheduleDeclarations found)
        {
            return found;
        }

        var created = new ScheduleDeclarations();
        builder.Services.TryAddSingleton(created);
        return created;
    }
}

/// <summary>Fluent definition of one schedule (13 §2).</summary>
public sealed class ScheduleBuilder<TJob> where TJob : class, IQueueJob
{
    readonly ScheduleDeclaration _d;

    internal ScheduleBuilder(ScheduleDeclaration declaration) => _d = declaration;

    public ScheduleBuilder<TJob> Cron(string expression, ScheduleCronFormat format = ScheduleCronFormat.FiveField, string? timeZone = null)
    {
        _d.Cron = expression;
        _d.Dialect = format == ScheduleCronFormat.SixField ? CronDialect.Cron6 : CronDialect.Cron5;
        _d.Zone = timeZone;
        return this;
    }

    public ScheduleBuilder<TJob> Once(DateTimeOffset dueAt)
    {
        _d.DueAt = dueAt;
        return this;
    }

    public ScheduleBuilder<TJob> OnGap(ScheduleGap gap)
    {
        _d.Gap = gap;
        return this;
    }

    public ScheduleBuilder<TJob> OnFold(ScheduleFold fold)
    {
        _d.Fold = fold;
        return this;
    }

    public ScheduleBuilder<TJob> OnMisfire(MisfirePolicy policy, TimeSpan? grace = null)
    {
        _d.Misfire = policy;
        _d.Grace = grace ?? ScheduleBounds.DefaultGrace;
        return this;
    }

    public ScheduleBuilder<TJob> CatchUpLimits(int maxJobsPerEpisode = ScheduleBounds.DefaultEpisodeJobs, TimeSpan? maxAge = null)
    {
        _d.MaxEpisodeJobs = maxJobsPerEpisode;
        _d.MaxCatchUpAge = maxAge ?? ScheduleBounds.DefaultCatchUpAge;
        return this;
    }

    public ScheduleBuilder<TJob> Overlap(ScheduleOverlap overlap)
    {
        _d.Overlap = overlap;
        return this;
    }

    public ScheduleBuilder<TJob> AllowOccurrenceReuse()
    {
        _d.AllowReuse = true;
        return this;
    }

    /// <summary>Queue, priority, uniqueness and metadata of the template job. <c>NotBefore</c> and window uniqueness are refused.</summary>
    public ScheduleBuilder<TJob> WithOptions(DispatchOptions options)
    {
        _d.Options = options;
        return this;
    }
}

internal sealed class ScheduleDeclarations
{
    public bool SchedulerAdded { get; set; }

    public SchedulerOptions Options { get; } = new();

    public List<ScheduleDeclaration> Schedules { get; } = [];
}

internal sealed class ScheduleDeclaration(string key, IQueueJob job)
{
    public string Key { get; } = key;

    public IQueueJob Job { get; } = job;

    public string? Cron { get; set; }

    public string Dialect { get; set; } = CronDialect.Cron5;

    public string? Zone { get; set; }

    public DateTimeOffset? DueAt { get; set; }

    public ScheduleGap Gap { get; set; }

    public ScheduleFold Fold { get; set; }

    public MisfirePolicy Misfire { get; set; }

    public TimeSpan Grace { get; set; } = ScheduleBounds.DefaultGrace;

    public int MaxEpisodeJobs { get; set; } = ScheduleBounds.DefaultEpisodeJobs;

    public TimeSpan MaxCatchUpAge { get; set; } = ScheduleBounds.DefaultCatchUpAge;

    public ScheduleOverlap Overlap { get; set; }

    public bool AllowReuse { get; set; }

    public DispatchOptions? Options { get; set; }
}

/// <summary>Compiles a declaration into the stored definition (13 §3) with its fingerprint (12 §5, 13 §3.3).</summary>
internal static class ScheduleCompiler
{
    public static ScheduleDefinitionRecord Compile(ScheduleDeclaration d, DispatchPreparer preparer, TzdbArtifact artifact)
    {
        static RunwickConfigurationException Fail(string code, string key, string message) => new([new ConfigurationError(code, key, message)]);

        if (!ValidKey(d.Key))
        {
            throw Fail(RunwickErrorCodes.InvalidKey, d.Key, "Schedule keys are 1–512 UTF-8 bytes without control characters.");
        }

        if ((d.Cron is null) == (d.DueAt is null))
        {
            throw Fail("schedule_definition_missing", d.Key, "Declare exactly one of Cron(...) or Once(...).");
        }

        if (d.Grace < TimeSpan.Zero || d.Grace > ScheduleBounds.MaxGrace || d.Grace.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            throw Fail("invalid_schedule_policy", d.Key, "Misfire grace must be whole milliseconds within 0–24 h.");
        }

        if (d.MaxEpisodeJobs is < 1 or > ScheduleBounds.MaxEpisodeJobs || d.MaxCatchUpAge < ScheduleBounds.MinCatchUpAge
            || d.MaxCatchUpAge > ScheduleBounds.MaxCatchUpAge || d.MaxCatchUpAge.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            throw Fail("invalid_schedule_policy", d.Key, "Catch-up limits: 1–10,000 jobs per episode, maximum age 1 min–31 d.");
        }

        if (d.Options is { NotBefore: not null })
        {
            throw Fail("schedule_delay_unsupported", d.Key, "A schedule template cannot carry NotBefore.");
        }

        var (intent, scopes) = preparer.PrepareItem(d.Job, d.Options);
        if (intent.WindowKey is not null)
        {
            throw Fail("schedule_window_unsupported", d.Key, "Window uniqueness on scheduled jobs is not supported in M3.");
        }

        if (intent.ActiveKey is not null && !d.AllowReuse)
        {
            throw Fail("schedule_occurrence_reuse_required", d.Key,
                "An active uniqueness key is shared by every occurrence; call AllowOccurrenceReuse() to link occurrences to the active job.");
        }

        var rates = preparer.RatesFor(intent);
        var template = new ScheduleTemplate(intent.JobName, intent.PayloadVersion, intent.SerializerProfileId, intent.SerializerProfileVersion,
            intent.PayloadUtf8, intent.Queue, intent.Priority, intent.Policy, scopes, rates, intent.ActiveKey, intent.BusinessMetadata);

        CanonicalValue recurrence;
        string kind;
        byte[]? sha = null;
        string? profile = null, version = null;
        if (d.Cron is not null)
        {
            CronSchedule compiled;
            try
            {
                compiled = ScheduleDefinition.Compile(d.Cron, d.Dialect, d.Zone, artifact, Gap(d.Gap), Fold(d.Fold));
            }
            catch (ScheduleEvaluationException ex)
            {
                throw Fail(ex.WireName, d.Key, ex.Message);
            }

            recurrence = ScheduleDefinition.Recurrence(compiled);
            kind = ScheduleOperations.Cron;
            sha = artifact.Sha256.ToArray();
            profile = compiled.Profile;
            version = artifact.Version;
        }
        else
        {
            recurrence = ScheduleDefinition.InstantRecurrence(d.DueAt!.Value.ToUnixTimeMilliseconds());
            kind = ScheduleOperations.OneTime;
        }

        var misfire = d.Misfire switch
        {
            MisfirePolicy.Skip => ScheduleOperations.MisfireSkip,
            MisfirePolicy.CatchUp => ScheduleOperations.MisfireCatchUp,
            _ => ScheduleOperations.MisfireRunOnce,
        };
        var overlap = d.Overlap == ScheduleOverlap.Serialize ? ScheduleOperations.OverlapSerialize : ScheduleOperations.OverlapAllow;
        var settings = Settings(template, misfire, (long)d.Grace.TotalMilliseconds, overlap, ScheduleBounds.MaxBatch, d.MaxEpisodeJobs,
            (long)d.MaxCatchUpAge.TotalMilliseconds, d.AllowReuse);
        return new ScheduleDefinitionRecord(
            kind, d.DueAt is { } due ? DateTimeOffset.FromUnixTimeMilliseconds(due.ToUnixTimeMilliseconds()) : null,
            d.Cron, d.Cron is null ? CronDialect.Instant : d.Dialect, d.Zone, profile, version, sha,
            ScheduleDefinition.WireName(Gap(d.Gap)), ScheduleDefinition.WireName(Fold(d.Fold)), misfire, (long)d.Grace.TotalMilliseconds,
            ScheduleBounds.MaxBatch, d.MaxEpisodeJobs, (long)d.MaxCatchUpAge.TotalMilliseconds, overlap, d.AllowReuse, template,
            ScheduleDefinition.Fingerprint(recurrence, settings));
    }

    /// <summary>The fingerprint's <c>settings</c> member (13 §3.3).</summary>
    public static CanonicalValue Settings(ScheduleTemplate t, string misfire, long graceMs, string overlap, int maxBatch, int maxEpisodeJobs,
        long maxCatchUpAgeMs, bool allowReuse) =>
        CanonicalValue.Object()
            .Add("allowReuse", allowReuse)
            .Add("bounds", CanonicalValue.Object()
                .Add("maxBatch", maxBatch)
                .Add("maxCatchupAgeMs", maxCatchUpAgeMs)
                .Add("maxEpisodeJobs", maxEpisodeJobs)
                .Build())
            .Add("dispatch", CanonicalValue.Object()
                .Add("activeKey", t.ActiveKey)
                .Add("policy", Fingerprints.Policy(t.Policy))
                .Add("priority", t.Priority)
                .Add("queue", t.Queue)
                .Build())
            .Add("job", CanonicalValue.Object()
                .Add("name", t.JobName)
                .Add("payloadVersion", t.PayloadVersion)
                .Add("profileId", t.SerializerProfileId)
                .Add("profileVersion", t.SerializerProfileVersion)
                .Add("templateSha256", t.PayloadSha256Hex)
                .Build())
            .Add("misfire", CanonicalValue.Object().Add("graceMs", graceMs).Add("policy", misfire).Build())
            .Add("overlap", overlap)
            .Build();

    static GapPolicy Gap(ScheduleGap g) => g == ScheduleGap.Shift ? GapPolicy.Shift : GapPolicy.Skip;

    static FoldPolicy Fold(ScheduleFold f) => f switch
    {
        ScheduleFold.Later => FoldPolicy.Later,
        ScheduleFold.Both => FoldPolicy.Both,
        _ => FoldPolicy.Earlier,
    };

    static bool ValidKey(string key) =>
        !string.IsNullOrEmpty(key) && System.Text.Encoding.UTF8.GetByteCount(key) <= 512 && !key.Any(char.IsControl);
}
