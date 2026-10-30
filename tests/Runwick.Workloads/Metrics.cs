using System.Collections.Concurrent;
using System.Text.Json.Nodes;

namespace Runwick.Workloads;

/// <summary>Named latency reservoirs and counters for one process (07 §14.4); cumulative, sampled by the caller.</summary>
public sealed class Metrics
{
    readonly ConcurrentDictionary<string, LatencyReservoir> _latencies = new(StringComparer.Ordinal);
    readonly ConcurrentDictionary<string, long> _counters = new(StringComparer.Ordinal);
    readonly ConcurrentDictionary<string, long> _maxima = new(StringComparer.Ordinal);

    public void Observe(string name, double milliseconds) =>
        _latencies.GetOrAdd(name, static _ => new LatencyReservoir()).Add(milliseconds);

    public void Count(string name, long by = 1) => _counters.AddOrUpdate(name, by, (_, v) => v + by);

    public void Max(string name, long value) => _maxima.AddOrUpdate(name, value, (_, v) => Math.Max(v, value));

    public long Counter(string name) => _counters.TryGetValue(name, out var v) ? v : 0;

    public JsonObject CountersJson() => new(_counters.OrderBy(kv => kv.Key, StringComparer.Ordinal)
        .Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value)));

    public JsonObject MaximaJson() => new(_maxima.OrderBy(kv => kv.Key, StringComparer.Ordinal)
        .Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value)));

    public JsonObject LatenciesJson()
    {
        var o = new JsonObject();
        foreach (var (name, reservoir) in _latencies.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            var s = reservoir.Summarize();
            o[name] = new JsonObject
            {
                ["count"] = s.Count, ["meanMs"] = Math.Round(reservoir.Mean, 3), ["minMs"] = Math.Round(s.MinMs, 3),
                ["p01Ms"] = Math.Round(reservoir.Quantile(0.01), 3), ["p50Ms"] = Math.Round(s.P50Ms, 3),
                ["p95Ms"] = Math.Round(s.P95Ms, 3), ["p99Ms"] = Math.Round(s.P99Ms, 3), ["maxMs"] = Math.Round(s.MaxMs, 3),
            };
        }

        return o;
    }

    /// <summary>Drops every latency sample (end of warm-up); counters stay cumulative.</summary>
    public void ResetLatencies() => _latencies.Clear();
}
