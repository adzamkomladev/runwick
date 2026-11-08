using System.Text.Json;
using System.Text.RegularExpressions;
using Runwick.Core.Tests.Support;

namespace Runwick.Core.Tests;

/// <summary>
/// OPS-01 (18 §9): the shipped Grafana dashboard and Prometheus alert rules use only metric names of the frozen
/// instrument table (18 §3.1, OpenTelemetry → Prometheus mapping), and never sum a global (collector) series.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-022")]
[Trait("Case", "OPS-01")]
public sealed partial class OpsAssetTests
{
    /// <summary>The one name the host exports itself (from the readiness reason); Runwick emits no readiness metric.</summary>
    const string HostProvided = "runwick_host_readiness_incompatible";

    static readonly string[] Alerts =
    [
        "RunwickLeaseMarginLow", "RunwickAuthorityLost", "RunwickSettlementOld", "RunwickDueAgeRising", "RunwickFailuresRepeated",
        "RunwickScheduleLag", "RunwickRetentionFailing", "RunwickCollectorStale", "RunwickStoreIncompatible",
    ];

    static readonly string[] Global =
        ["runwick_queue_", "runwick_schedule_needs_review", "runwick_schedule_oldest_overdue_age_seconds", "runwick_collector_snapshot_age_seconds"];

    [GeneratedRegex(@"^\| `(runwick\.[a-z_.]+)` \| (Counter|Histogram|ObservableGauge) (\S+)")]
    private static partial Regex InstrumentRow();

    [GeneratedRegex(@"\{[^}]*\}")]
    private static partial Regex LabelMatchers();

    [GeneratedRegex(@"\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)")]
    private static partial Regex LabelLists();

    [GeneratedRegex(@"\brunwick_[a-z0-9_]+")]
    private static partial Regex MetricToken();

    /// <summary>Prometheus names the OpenTelemetry exporter produces for the instruments of 18 §3.1.</summary>
    static HashSet<string> LegalNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal) { HostProvided };
        foreach (var line in File.ReadLines(Path.Combine(Repo.Root, "docs", "spec", "18-telemetry.md")))
        {
            var m = InstrumentRow().Match(line);
            if (!m.Success)
            {
                continue;
            }

            var name = m.Groups[1].Value.Replace('.', '_');
            var seconds = m.Groups[3].Value == "s" ? "_seconds" : "";
            switch (m.Groups[2].Value)
            {
                case "Counter":
                    names.Add(name + "_total");
                    break;
                case "Histogram":
                    names.UnionWith([name + seconds + "_bucket", name + seconds + "_sum", name + seconds + "_count"]);
                    break;
                default:
                    names.Add(name + seconds);
                    break;
            }
        }

        Assert.True(names.Count >= 50,$"only {names.Count} names parsed from 18 §3.1");
        return names;
    }

    /// <summary>Metric names in a PromQL expression: label matchers and label lists are removed first.</summary>
    static IEnumerable<string> Metrics(string expr) =>
        MetricToken().Matches(LabelLists().Replace(LabelMatchers().Replace(expr, "{}"), "")).Select(m => m.Value);

    static List<string> RuleExpressions()
    {
        var lines = File.ReadAllLines(Path.Combine(Repo.Root, "ops", "prometheus", "runwick-alerts.yml"));
        var exprs = new List<string>();
        for (var i = 0; i < lines.Length; i++)
        {
            var at = lines[i].IndexOf("expr:", StringComparison.Ordinal);
            if (at < 0)
            {
                continue;
            }

            var value = lines[i][(at + 5)..].Trim();
            if (value is ">-" or ">" or "|" or "|-")
            {
                // Block scalar: the following lines indented deeper than "expr:".
                var parts = new List<string>();
                while (i + 1 < lines.Length && (lines[i + 1].Length - lines[i + 1].TrimStart().Length) > at && lines[i + 1].Trim().Length > 0)
                {
                    parts.Add(lines[++i].Trim());
                }

                value = string.Join(' ', parts);
            }

            exprs.Add(value);
        }

        return exprs;
    }

    static List<string> DashboardExpressions()
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Repo.Root, "ops", "grafana", "runwick-overview.json")));
        return [.. doc.RootElement.GetProperty("panels").EnumerateArray()
            .Where(p => p.TryGetProperty("targets", out _))
            .SelectMany(p => p.GetProperty("targets").EnumerateArray())
            .Select(t => t.GetProperty("expr").GetString()!)];
    }

    [Fact]
    public void Extraction_ignores_label_names()
    {
        Assert.Equal(["runwick_a_total", "runwick_b_seconds"],
            Metrics("sum by (runwick_queue) (rate(runwick_a_total{runwick_result=\"x\"}[5m])) / on(runwick_pool) runwick_b_seconds"));
    }

    [Fact]
    public void Alert_rules_name_the_nine_alerts_and_only_frozen_metrics()
    {
        var text = File.ReadAllText(Path.Combine(Repo.Root, "ops", "prometheus", "runwick-alerts.yml"));
        foreach (var alert in Alerts)
        {
            Assert.Contains("- alert: " + alert + "\n", text.ReplaceLineEndings("\n"), StringComparison.Ordinal);
        }

        var exprs = RuleExpressions();
        Assert.Equal(Alerts.Length, exprs.Count);
        AssertLegal(exprs);
    }

    [Fact]
    public void Dashboard_parses_and_uses_only_frozen_metrics()
    {
        var exprs = DashboardExpressions();
        Assert.NotEmpty(exprs);
        AssertLegal(exprs);

        // Every global panel shows the collector snapshot age next to its value.
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(Repo.Root, "ops", "grafana", "runwick-overview.json")));
        foreach (var panel in doc.RootElement.GetProperty("panels").EnumerateArray().Where(p => p.TryGetProperty("targets", out _)))
        {
            var panelExprs = panel.GetProperty("targets").EnumerateArray().Select(t => t.GetProperty("expr").GetString()!).ToList();
            if (panelExprs.SelectMany(Metrics).Any(n => Global.Any(g => n.StartsWith(g, StringComparison.Ordinal))))
            {
                Assert.Contains(panelExprs, e => e.Contains("runwick_collector_snapshot_age_seconds", StringComparison.Ordinal));
            }
        }
    }

    static void AssertLegal(IEnumerable<string> exprs)
    {
        var legal = LegalNames();
        foreach (var expr in exprs)
        {
            foreach (var name in Metrics(expr))
            {
                Assert.True(legal.Contains(name), $"unknown metric {name} in: {expr}");
            }

            // Two collectors report the same queue: global series are combined with max, never sum (OBS-06).
            if (Metrics(expr).Any(n => Global.Any(g => n.StartsWith(g, StringComparison.Ordinal))))
            {
                Assert.DoesNotContain("sum", expr, StringComparison.Ordinal);
            }
        }
    }
}
