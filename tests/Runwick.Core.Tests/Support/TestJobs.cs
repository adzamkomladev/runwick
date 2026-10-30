using System.Text.Json;
using System.Text.Json.Serialization;

namespace Runwick.Core.Tests.Support;

/// <summary>The spec 02 §12.2 worked-example contract (GV-B0).</summary>
[JobName("send-welcome-email")]
[PayloadVersion(2)]
[Queue("emails")]
[Priority("high")]
public sealed record WelcomeEmail(Guid UserId, string Locale) : IQueueJob;

/// <summary>Same durable name as <see cref="WelcomeEmail"/>: a "renamed" CLR type (SER-01).</summary>
[JobName("send-welcome-email")]
[PayloadVersion(2)]
[Queue("emails")]
[Priority("high")]
public sealed record WelcomeEmailRenamed(Guid UserId, string Locale) : IQueueJob;

[JobName("plain")]
public sealed record PlainJob(string Text) : IQueueJob;

[JobName("other")]
public sealed record OtherJob(int Number) : IQueueJob;

/// <summary>No [JobName]: needs a fluent Named(...).</summary>
public sealed record UnnamedJob(string Text) : IQueueJob;

public sealed record UnnamedJob2(string Text) : IQueueJob;

[JobName("attr.max")]
[MaxExecutions(7)]
[Queue("attr-queue")]
[Priority("low")]
public sealed record AttributedJob(string Text) : IQueueJob;

/// <summary>Bounds probes: parts (strings), numbers, nested JSON.</summary>
[JobName("bounds")]
public sealed record BoundsJob(IReadOnlyList<string>? Parts, IReadOnlyDictionary<string, int>? Map, int[]? Numbers, JsonElement? Raw) : IQueueJob
{
    public static BoundsJob WithParts(params string[] parts) => new(parts, null, null, null);
}

[JobName("mutable")]
public sealed class MutableJob : IQueueJob
{
    public string Text { get; set; } = "";

    public List<int> Values { get; set; } = [];

    public string Key { get; set; } = "";
}

[JobName("typed")]
public sealed record TypedJob(int Count, long Big, string Name, DateTime? At = null, double Ratio = 0) : IQueueJob;

[JobName("required")]
public sealed class RequiredJob : IQueueJob
{
    public required string Name { get; init; }
}

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(Circle), "circle")]
[JsonDerivedType(typeof(Square), "square")]
public abstract record Shape;

public sealed record Circle(int Radius) : Shape;

public sealed record Square(int Side) : Shape;

[JobName("draw")]
public sealed record DrawJob(Shape Shape) : IQueueJob;

[JobName("relaxed.numbers")]
[JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
public sealed record RelaxedNumbersJob(int Count) : IQueueJob;

[JobName("relaxed.member")]
public sealed record RelaxedMemberJob([property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] int Count) : IQueueJob;

[JobName("relaxed.extension")]
public sealed class ExtensionDataJob : IQueueJob
{
    public string Name { get; set; } = "";

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? Extra { get; set; }
}

[JobName("relaxed.unmapped")]
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Skip)]
public sealed record SkipUnmappedJob(string Name) : IQueueJob;

public sealed record NestedRelaxed([property: JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)] int Value);

[JobName("relaxed.nested")]
public sealed record NestedRelaxedJob(IReadOnlyList<NestedRelaxed> Items) : IQueueJob;

[JobName("object.member")]
public sealed record ObjectMemberJob(object Anything) : IQueueJob;

[JobName("object.element")]
public sealed record ObjectElementJob(List<object> Anything) : IQueueJob;

[Flags]
public enum Colors
{
    None = 0,
    Red = 1,
    Blue = 2,
}

[JobName("flags")]
public sealed record FlagsJob(Colors Colors) : IQueueJob;

public enum Mood
{
    Calm,
    Angry,
}

[JobName("enum")]
public sealed record EnumJob(Mood Mood) : IQueueJob;

// Upgrade chain v1 → v2 → v3 (SER-08).
public sealed record ReportV1(string Id);

public sealed record ReportV2(string Id, int Rows);

[JobName("report")]
[PayloadVersion(3)]
public sealed record ReportV3(string Id, int Rows, string Format) : IQueueJob;

public sealed class PlainHandler : IJobHandler<PlainJob>
{
    public Task HandleAsync(PlainJob job, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class PlainHandler2 : IJobHandler<PlainJob>
{
    public Task HandleAsync(PlainJob job, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class ReportHandler : IJobHandler<ReportV3>
{
    public Task HandleAsync(ReportV3 job, CancellationToken cancellationToken) => Task.CompletedTask;
}

public class PassMiddleware : IJobMiddleware
{
    public Task<JobExecutionResult> InvokeAsync(IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken) => next();
}

public sealed class AuditMiddleware : PassMiddleware;

public sealed class TenantMiddleware : PassMiddleware;

public sealed class MetricsMiddleware : PassMiddleware;

[JobName("mw.attrs")]
[UseJobMiddleware("zeta", Order = 1)]
[UseJobMiddleware("beta", Order = 1)]
[UseJobMiddleware("alpha", Order = 2)]
public sealed record MiddlewareJob(string Text) : IQueueJob;

[JobName("mw.unknown")]
[UseJobMiddleware("missing")]
public sealed record UnknownAliasJob(string Text) : IQueueJob;

// Capability bound (> 512 tuples): 23 × 23 closed generic job types.
#pragma warning disable CA1812
public sealed class GenJob<TA, TB> : IQueueJob
{
    public int Value { get; set; }
}

public sealed class GenHandler<TA, TB> : IJobHandler<GenJob<TA, TB>>
{
    public Task HandleAsync(GenJob<TA, TB> job, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class M00; public sealed class M01; public sealed class M02; public sealed class M03; public sealed class M04;
public sealed class M05; public sealed class M06; public sealed class M07; public sealed class M08; public sealed class M09;
public sealed class M10; public sealed class M11; public sealed class M12; public sealed class M13; public sealed class M14;
public sealed class M15; public sealed class M16; public sealed class M17; public sealed class M18; public sealed class M19;
public sealed class M20; public sealed class M21; public sealed class M22;
#pragma warning restore CA1812
