namespace Runwick;

/// <summary>Names of Runwick's telemetry (18 §1): one <c>ActivitySource</c> and one <c>Meter</c>, both <see cref="SourceName"/>.</summary>
public static class RunwickTelemetry
{
    public const string SourceName = "Runwick";

    /// <summary>Version of the frozen span, metric and attribute tables of spec 18.</summary>
    public const int SchemaVersion = 1;

    /// <summary>The OpenTelemetry semantic conventions the <c>messaging.*</c> attributes follow (18 §2.3).</summary>
    public const string SemanticConventionsVersion = "1.44.0";

    public const string TelemetrySchemaUrl = "https://opentelemetry.io/schemas/" + SemanticConventionsVersion;
}

/// <summary>Telemetry options (18 §1): <c>services.AddRunwick(r =&gt; r.Telemetry(t =&gt; …))</c>.</summary>
public sealed class RunwickTelemetryOptions
{
    /// <summary>Puts a sanitized, bounded exception message on spans and log 1011 (18 §6). Default false.</summary>
    public bool RecordExceptionDetails { get; set; }

    /// <summary>Attribute combinations per instrument before the overflow series (100 – 10,000).</summary>
    public int MaxSeriesPerInstrument { get; set; } = 1_000;

    /// <summary>Makes this process the application-wide collector (18 §4). Default false.</summary>
    public bool CollectApplicationMetrics { get; set; }

    /// <summary>Collector sample period (5 s – 5 min).</summary>
    public TimeSpan ApplicationSamplePeriod { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>The <c>runwick.collector</c> attribute of global series.</summary>
    public string CollectorName { get; set; } = "default";

    internal void Validate()
    {
        if (MaxSeriesPerInstrument is < 100 or > 10_000)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxSeriesPerInstrument), "MaxSeriesPerInstrument must be 100 – 10,000.");
        }

        if (ApplicationSamplePeriod < TimeSpan.FromSeconds(5) || ApplicationSamplePeriod > TimeSpan.FromMinutes(5))
        {
            throw new ArgumentOutOfRangeException(nameof(ApplicationSamplePeriod), "ApplicationSamplePeriod must be 5 s – 5 min.");
        }

        if (string.IsNullOrWhiteSpace(CollectorName) || CollectorName.Length > 64)
        {
            throw new ArgumentException("CollectorName must be 1 – 64 characters.", nameof(CollectorName));
        }
    }
}
