using System.Diagnostics.Metrics;

namespace Runwick.Workloads;

/// <summary>Feeds the engine's <c>runwick.store.phase</c> histogram into <see cref="Metrics"/> as <c>phase:&lt;operation&gt;:&lt;phase&gt;</c>.</summary>
internal static class PhaseListener
{
    public static MeterListener Attach(Metrics metrics)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Runwick.Diagnostics")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            if (instrument.Name == "runwick.sqlite.dispatch.commit_size")
            {
                metrics.Count($"sqlite:dispatch:commitSize:{(int)value}");
                return;
            }

            string? op = null, phase = null;
            foreach (var t in tags)
            {
                if (t.Key == "operation")
                {
                    op = t.Value as string;
                }
                else if (t.Key == "phase")
                {
                    phase = t.Value as string;
                }
            }

            metrics.Observe($"phase:{op}:{phase}", value);
        });
        listener.Start();
        return listener;
    }
}
