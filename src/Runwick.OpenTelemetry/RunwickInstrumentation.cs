using System.Diagnostics.Metrics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Runwick;
using Runwick.Telemetry;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Options of <see cref="RunwickInstrumentationExtensions.AddRunwickInstrumentation(MeterProviderBuilder, Action{RunwickMetricsOptions}?)"/>.</summary>
public sealed class RunwickMetricsOptions
{
    /// <summary>Must match <c>RunwickTelemetryOptions.MaxSeriesPerInstrument</c>; the SDK limit is this plus the overflow series.</summary>
    public int MaxSeriesPerInstrument { get; set; } = 1_000;
}

/// <summary>Subscribes OpenTelemetry to Runwick's source and meter (18 §8). Adds no exporter.</summary>
public static class RunwickInstrumentationExtensions
{
    public static TracerProviderBuilder AddRunwickInstrumentation(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSource(RunwickTelemetry.SourceName);
    }

    /// <summary>
    /// Adds the <c>Runwick</c> meter and one view per instrument that keeps only its frozen attributes and sets the
    /// cardinality limit to Runwick's own bound plus its overflow series, so <c>otel.metric.overflow</c> appears only
    /// if Runwick's bound failed.
    /// </summary>
    public static MeterProviderBuilder AddRunwickInstrumentation(this MeterProviderBuilder builder, Action<RunwickMetricsOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new RunwickMetricsOptions();
        configure?.Invoke(options);
        if (options.MaxSeriesPerInstrument is < 100 or > 10_000)
        {
            throw new ArgumentOutOfRangeException(nameof(configure), "MaxSeriesPerInstrument must be 100 – 10,000.");
        }

        var limit = options.MaxSeriesPerInstrument + 1;
        builder.AddMeter(RunwickTelemetry.SourceName);
        foreach (var (name, keys, buckets) in Telemetry.Instruments())
        {
            string[] kept = [.. keys, "runwick.overflow"];
            MetricStreamConfiguration view = buckets is null
                ? new MetricStreamConfiguration { TagKeys = kept, CardinalityLimit = limit }
                : new ExplicitBucketHistogramConfiguration { TagKeys = kept, CardinalityLimit = limit, Boundaries = [.. buckets] };
            builder.AddView(instrument => instrument.Meter.Name == RunwickTelemetry.SourceName && instrument.Name == name ? view : null);
        }

        return builder;
    }
}
