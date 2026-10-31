using System.Text;
using System.Text.Json.Nodes;

namespace Runwick.Harness.Reports;

/// <summary>
/// G-2 report builder (15 §12.1 item 3): one row per §3 case and item, and per deferred sub-part, from xunit v3 XML
/// results and the workload result files. A row matches only tests that carry both its <c>Case</c> and its M3
/// <c>Item</c>, so a G-1 test of the same case never stands in for the M3 sub-part.
/// </summary>
public static class G2CaseTable
{
    static readonly (string Prefix, string Document)[] Documents =
    [
        ("END", "driver-contract-suite"), ("MGMT", "management-and-dashboard"), ("CTL", "15 (I-1)"), ("EVT", "15 (I-1)"),
        ("TXC", "15 (I-1)"), ("MODE", "15 (I-1)"), ("UNI", "driver-contract-suite"), ("PGX", "postgres-schema-and-operations"),
        ("ADM", "driver-contract-suite"), ("CMD", "driver-contract-suite"), ("ISO", "driver-contract-suite"),
        ("CFG", "driver-contract-suite"), ("OPS", "driver-contract-suite"), ("CAP", "capacity-defaults-and-budgets"),
        ("RDY", "15 (I-1)"), ("OBS", "driver-contract-suite"), ("WRK", "worker-engine-internals"), ("RATE", "capacity-control"),
        ("TIME", "execution-defaults-and-timing"), ("UPG", "postgres-upgrades"), ("SCL", "15 (I-1)"),
        ("SCHED", "scheduler-evaluator"), ("DEP", "provider-dependency-plan"), ("SER", "identity-and-serialization"),
        ("SCH", "scheduling-internals"), ("OUTBOX", "transactional-outbox"), ("OUT", "driver-contract-suite"),
        ("TX", "driver-contract-suite"), ("P-", "performance-acceptance-plan"), ("G1-M", "07 §14.3 (inferred)"),
    ];

    /// <summary>The 15 §3 inventory as (case, item) pairs: one row per sub-part an M3 item owns.</summary>
    public static IReadOnlyList<(string Case, string Sp)> Inventory { get; } =
    [
        // SP-011 (15 §4)
        .. For("SP-011", "END-02", "END-03", "MGMT-04", "UNI-04", "PGX-04", "PGX-06", "ADM-04", "ISO-01", "CFG-01", "OPS-03"),
        .. For("SP-011", [.. Range("CTL", 1, 5), .. Range("EVT", 1, 2), .. Range("TXC", 1, 6), .. Range("MODE", 1, 5), .. Range("CMD", 1, 5)]),

        // SP-012 (15 §5): MODE again with presence and peer-process workers
        .. For("SP-012", "CAP-01", "CAP-02", "CAP-03", "CAP-04", "OBS-01", "WRK-15", "WRK-19", "PGX-07"),
        .. For("SP-012", [.. Range("RDY", 1, 3), .. Range("MODE", 1, 5)]),

        // SP-013 (15 §6)
        .. For("SP-013", "CMD-06", "CAP-05", "PGX-01", "PGX-03", "TIME-06", "UPG-02", "UPG-06"),
        .. For("SP-013", [.. Range("RATE", 1, 4)]),

        // SP-014 (15 §7)
        .. For("SP-014", "SCL-01", "CAP-01", "CAP-06", "CAP-07", "CAP-08", "CAP-10", "CAP-11", "CAP-12", "WRK-02"),

        // SP-015 (15 §8)
        .. For("SP-015", [.. Range("SCHED", 1, 8), "SCHED-14", "DEP-01"]),

        // SP-016 (15 §9)
        .. For("SP-016", [.. Range("SCHED", 9, 13), "SCHED-15", .. Range("SCH", 1, 5), "PGX-02", "PGX-04", "ISO-01", .. Range("CMD", 1, 5)]),

        // SP-017 (15 §10)
        .. For("SP-017", [.. Range("OUTBOX", 1, 7), "OUT-01", "OUT-02", .. Range("TX", 1, 4), .. Range("CMD", 1, 5), "ISO-01"]),
    ];

    /// <summary>Sub-parts 15 §3 and the item specs defer (§12.2 rule 2): <c>not-applicable</c> with the later SP.</summary>
    public static IReadOnlyList<(string Case, string Part, string Sp)> Deferred { get; } =
    [
        ("MGMT-04", "HTTP half, authorization", "SP-020"),
        ("UNI-04", "window survival across retention cleanup", "SP-021"),
        ("TXC-01", "SQLite twin of transactional completion (TXC-01 … TXC-06)", "SP-019"),
        ("RATE-04", "enlargement/cleanup race (capacity-control §6)", "SP-021"),
        ("CAP-11", "dashboard/management lanes", "SP-020, SP-024"),
        ("SCH-05", "schedule delete and tombstones", "SP-020, SP-021"),
    ];

    /// <summary>15 §11 workloads (name, owning item).</summary>
    public static IReadOnlyList<(string Name, string Sp)> Measurements { get; } =
    [
        ("G1-M1", "SP-012"), ("G1-M3", "SP-012"), ("P-04", "SP-012"), ("P-05", "SP-012"), ("P-06r", "SP-013"), ("P-06s", "SP-016"), ("P-09", "SP-017"),
    ];

    static IEnumerable<(string, string)> For(string sp, params string[] cases) => cases.Select(c => (c, sp));

    static IEnumerable<string> Range(string prefix, int from, int to) =>
        Enumerable.Range(from, to - from + 1).Select(i => $"{prefix}-{i:00}");

    static string DocumentOf(string id) =>
        Documents.Where(d => id.StartsWith(d.Prefix, StringComparison.Ordinal)
                && (d.Prefix.EndsWith('-') || id.Length > d.Prefix.Length && (id[d.Prefix.Length] == '-' || char.IsDigit(id[d.Prefix.Length]))))
            .OrderByDescending(d => d.Prefix.Length).Select(d => d.Document).FirstOrDefault() ?? "?";

    public static IReadOnlyList<G1CaseTable.Row> Build(IEnumerable<string> xunitXmlFiles, string? workloadRoot, IReadOnlyDictionary<string, string>? reasons = null)
    {
        var tests = xunitXmlFiles.SelectMany(G1CaseTable.Parse).ToList();
        var rows = new List<G1CaseTable.Row>();
        foreach (var (id, sp) in Inventory)
        {
            var matching = tests.Where(t => Has(t, "Case", id) && Has(t, "Item", sp)).ToList();
            var result = matching.Count == 0 ? "unimplemented"
                : matching.Any(t => t.Result == "Fail") ? "failed"
                : matching.Any(t => t.Result != "Pass") ? "not-run"
                : "passed";
            var key = id + " " + sp;
            var reason = reasons?.GetValueOrDefault(key) ?? reasons?.GetValueOrDefault(id)
                ?? (matching.Count == 0 ? "no test carries this Case and Item" : $"{matching.Count(t => t.Result == "Pass")}/{matching.Count} tests passed");
            rows.Add(new G1CaseTable.Row(DocumentOf(id), id, string.Join("; ", matching.Select(t => t.Method).Distinct()), sp,
                string.Join(", ", matching.Select(t => t.Assembly).Distinct()),
                string.Join(", ", matching.SelectMany(t => t.Traits.GetValueOrDefault("Category") ?? []).Distinct().Order(StringComparer.Ordinal)),
                result, reason));
        }

        foreach (var (id, part, sp) in Deferred)
        {
            rows.Add(new G1CaseTable.Row(DocumentOf(id), id + " (deferred part)", part, sp, "—", "—", "not-applicable", $"deferred by 15 to {sp}"));
        }

        foreach (var (id, sp) in Measurements)
        {
            rows.Add(Measurement(id, sp, workloadRoot, reasons));
        }

        return rows;
    }

    static bool Has(G1CaseTable.TestResult t, string trait, string value) => t.Traits.TryGetValue(trait, out var values) && values.Contains(value);

    static G1CaseTable.Row Measurement(string id, string sp, string? workloadRoot, IReadOnlyDictionary<string, string>? reasons)
    {
        var file = workloadRoot is null || !Directory.Exists(workloadRoot) ? null
            : Directory.EnumerateFiles(workloadRoot, "result.json", SearchOption.AllDirectories)
                .Where(f => Path.GetFileName(Path.GetDirectoryName(f)!).EndsWith("-" + id, StringComparison.Ordinal)).Order(StringComparer.Ordinal).LastOrDefault();
        string result, reason;
        if (file is null)
        {
            (result, reason) = ("not-run", "no workload result file");
        }
        else
        {
            var json = JsonNode.Parse(File.ReadAllText(file))!;
            var violations = json["correctnessViolations"]?.GetValue<int>() ?? -1;
            var runs = json["runs"]!.AsArray();
            var failures = runs.Count(r => r?["failure"] is not null);
            result = violations == 0 && failures == 0 ? "passed" : violations > 0 ? "failed" : "not-run";
            reason = $"{runs.Count} run(s), {violations} correctness violation(s), {failures} run failure(s); {Path.GetRelativePath(workloadRoot!, file)}";
        }

        return new G1CaseTable.Row(DocumentOf(id), id, "workload (15 §11)", sp, "Runwick.Workloads", "Perf", result, reasons?.GetValueOrDefault(id) ?? reason);
    }

    /// <summary>The 15 §12.2 rule 6 checklist: each architecture-acceptance G-2 box with the case families it links to.</summary>
    public static string Checklist(IReadOnlyList<G1CaseTable.Row> rows)
    {
        (string Box, string[] Prefixes)[] boxes =
        [
            ("Five deployments", ["MODE-"]),
            ("Controls and audit races", ["END-02", "END-03", "MGMT-04", "CTL-"]),
            ("Bounded readiness and diagnostics", ["CAP-02", "CAP-03", "CAP-04", "RDY-"]),
            ("Rates across replicas with old-writer rejection", ["RATE-", "UPG-"]),
            ("Scaler traces", ["SCL-01", "CAP-06", "CAP-07", "CAP-08"]),
            ("Cron/DST, materialization, misfire, edit, retention", ["SCHED-", "SCH-"]),
            ("Outbox contract", ["OUTBOX-", "OUT-"]),
        ];
        var sb = new StringBuilder();
        sb.AppendLine("| G-2 box | Rows | Passed | Not applicable | Other | Satisfied |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (var (box, prefixes) in boxes)
        {
            var linked = rows.Where(r => prefixes.Any(p => r.Case.StartsWith(p, StringComparison.Ordinal))).ToList();
            var passed = linked.Count(r => r.Result == "passed");
            var na = linked.Count(r => r.Result == "not-applicable");
            var other = linked.Count - passed - na;
            sb.AppendLine($"| {box} | {linked.Count} | {passed} | {na} | {other} | {(linked.Count > 0 && other == 0 ? "yes" : "**no**")} |");
        }

        return sb.ToString();
    }
}
