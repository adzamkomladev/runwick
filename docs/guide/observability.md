# Observability

Runwick uses the standard .NET tools and exports nothing unless you ask.

## What it emits

- **Traces:** one `ActivitySource` named `Runwick`.
- **Metrics:** one `Meter` named `Runwick`, with fixed metric names, units and attributes. Job IDs, keys, error messages and URLs are never metric attributes.
- **Logs:** `ILogger` with fixed event IDs.
- Payloads, credentials and exception text stay out by default.

## Turning it on with OpenTelemetry

```csharp
services.AddOpenTelemetry()
    .WithTracing(t => t.AddRunwickInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddRunwickInstrumentation().AddOtlpExporter());
```

`Runwick.OpenTelemetry` has no exporter; add the one you use. It sets cardinality limits and keeps only Runwick's attributes.

## Reading the numbers honestly

- A handler that succeeded and a completion that was not confirmed are different results; a replayed command is not counted twice.
- Application-wide figures (queue depth, oldest waiting job) come from one collector, not from every worker, so do not add them up across workers. Stale or missing data is shown as missing, not zero.
- "Due" means waiting and eligible; it does not mean runnable right now.
- Counts can be lost if a process crashes. They are for operations, not billing or audit.

## Health checks

With `Runwick.AspNetCore`: `services.AddHealthChecks().AddRunwick()` registers a liveness check and readiness checks per role (producer, worker, scheduler, management), tagged `live` and `ready`.

## Shipped dashboards and alerts

`Runwick.OpenTelemetry` packs a Grafana dashboard (`content/grafana/runwick-overview.json`) and Prometheus alert rules (`content/prometheus/runwick-alerts.yml`). The alert thresholds are starting points, not tuned for your workload; adjust them. The incompatible-store alert reads a gauge your host must export itself.

The telemetry does not protect against a hostile listener in your own process, and it does not claim compatibility with OpenTelemetry semantic conventions other than the pinned version.
