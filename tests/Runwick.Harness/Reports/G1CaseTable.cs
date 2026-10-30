using System.Text;
using System.Text.Json.Nodes;
using System.Xml.Linq;

namespace Runwick.Harness.Reports;

/// <summary>
/// G-1 report builder (07 §15.1 item 3): one row per §3 inventory case and per deferred sub-part, generated from xunit v3
/// XML result files (<c>Case</c>/<c>Item</c>/<c>Category</c> traits) and the workload result files. Humans add only
/// reasons and evidence links.
/// </summary>
public static class G1CaseTable
{
    public sealed record Row(string Document, string Case, string Title, string Sp, string Host, string Category, string Result, string Reason);

    sealed record TestResult(string Assembly, string Method, string Result, IReadOnlyDictionary<string, List<string>> Traits);

    static readonly (string Prefix, string Document)[] Documents =
    [
        ("CMD", "driver-contract-suite"), ("ADM", "driver-contract-suite"), ("RUN", "driver-contract-suite"), ("OWN", "driver-contract-suite"),
        ("END", "driver-contract-suite"), ("UNI", "driver-contract-suite"), ("TX", "driver-contract-suite"), ("ISO", "driver-contract-suite"),
        ("ENC", "driver-contract-suite"), ("CFG", "driver-contract-suite"), ("TIME", "execution-defaults-and-timing"),
        ("WRK", "worker-engine-internals"), ("SER", "identity-and-serialization"), ("PGX", "postgres-schema-and-operations"),
        ("DEP", "provider-dependency-plan"), ("CAP", "capacity-defaults-and-budgets"), ("INIT", "07 (I-2)"), ("BATCH", "07 (I-2)"),
        ("P-", "performance-acceptance-plan"), ("G1-M", "07 §14.3 (inferred)"),
    ];

    /// <summary>The 07 §3 G-1 inventory.</summary>
    public static IReadOnlyList<string> Inventory { get; } =
    [
        .. Range("CMD", 1, 6), .. Range("ADM", 1, 5), .. Range("RUN", 1, 2), .. Range("OWN", 1, 5), "END-01", "END-04",
        .. Range("UNI", 1, 4), .. Range("TX", 1, 4), "ISO-01", "ENC-01", "CFG-01", .. Range("TIME", 1, 13),
        "WRK-01", .. Range("WRK", 3, 14), .. Range("WRK", 16, 18), .. Range("SER", 1, 12), "PGX-04", "PGX-05", "PGX-09",
        "DEP-02", "DEP-04", "CAP-01", "CAP-03", "CAP-10", "CAP-11", "INIT-01", .. Range("BATCH", 1, 3),
    ];

    /// <summary>Sub-parts 07 defers explicitly (§15.2 rule 2): reported <c>not-applicable</c> with the later SP.</summary>
    public static IReadOnlyList<(string Case, string Part, string Sp)> Deferred { get; } =
    [
        ("WRK-02", "target reduction with in-flight claims", "SP-014"),
        ("WRK-15", "queue ceiling with 10k waiting jobs", "SP-012"),
        ("WRK-19", "presence expires while job lease healthy", "SP-012"),
        ("CMD-06", "committed RateLimited Begin deferral replay", "SP-013"),
        ("UNI-04", "manual-retry reacquisition races; window survival across retention cleanup", "SP-011, SP-021"),
        ("TIME-06", "rate deferral charges neither budget", "SP-013"),
        ("PGX-04", "manual retry; materialization/scope edits", "SP-011, SP-016"),
        ("PGX-09", "cross-driver comparison", "SP-019, SP-025"),
        ("SER-06", "cross-driver identity", "SP-019, SP-025"),
        ("CAP-01", "automatic-mode runtime behaviour and target reductions", "SP-014"),
        ("CAP-10", "autoscaler growth-freeze interaction", "SP-014"),
        ("CAP-11", "dashboard/management lanes", "SP-020, SP-024"),
    ];

    public static IReadOnlyList<string> Measurements { get; } = ["P-01", "P-02", "P-08a", "P-08b", "P-08c", "G1-M1", "G1-M2", "G1-M3"];

    static IEnumerable<string> Range(string prefix, int from, int to) =>
        Enumerable.Range(from, to - from + 1).Select(i => $"{prefix}-{i:00}");

    static string DocumentOf(string id) => Documents.First(d => id.StartsWith(d.Prefix, StringComparison.Ordinal)).Document;

    public static IReadOnlyList<Row> Build(IEnumerable<string> xunitXmlFiles, string? workloadRoot, IReadOnlyDictionary<string, string>? reasons = null)
    {
        var tests = xunitXmlFiles.SelectMany(Parse).ToList();
        var rows = new List<Row>();
        foreach (var id in Inventory)
        {
            var matching = tests.Where(t => t.Traits.TryGetValue("Case", out var cases) && cases.Contains(id)).ToList();
            var result = matching.Count == 0 ? "unimplemented"
                : matching.Any(t => t.Result == "Fail") ? "failed"
                : matching.Any(t => t.Result != "Pass") ? "not-run"
                : "passed";
            string Join(string trait) => string.Join(", ", matching.SelectMany(t => t.Traits.GetValueOrDefault(trait) ?? []).Distinct().Order(StringComparer.Ordinal));
            var reason = reasons?.GetValueOrDefault(id)
                ?? (matching.Count == 0 ? "no test carries this Case trait" : $"{matching.Count(t => t.Result == "Pass")}/{matching.Count} tests passed");
            rows.Add(new Row(DocumentOf(id), id, string.Join("; ", matching.Select(t => t.Method).Distinct()), Join("Item"),
                string.Join(", ", matching.Select(t => t.Assembly).Distinct()), Join("Category"), result, reason));
            foreach (var (_, part, sp) in Deferred.Where(d => d.Case == id))
            {
                rows.Add(new Row(DocumentOf(id), id + " (deferred part)", part, sp, "—", "—", "not-applicable", $"deferred by 07 to {sp}"));
            }
        }

        foreach (var (id, part, sp) in Deferred.Where(d => !Inventory.Contains(d.Case)))
        {
            rows.Add(new Row(DocumentOf(id), id, part, sp, "—", "—", "not-applicable", $"deferred by 07 to {sp}"));
        }

        foreach (var id in Measurements)
        {
            var file = workloadRoot is null ? null : Directory.EnumerateFiles(workloadRoot, "result.json", SearchOption.AllDirectories)
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

            rows.Add(new Row(DocumentOf(id), id, "workload (07 §14.3)", "SP-010", "Runwick.Workloads", "Perf", result,
                reasons?.GetValueOrDefault(id) ?? reason));
        }

        return rows;
    }

    static IEnumerable<TestResult> Parse(string path)
    {
        var doc = XDocument.Load(path);
        foreach (var assembly in doc.Descendants("assembly"))
        {
            var name = Path.GetFileNameWithoutExtension((string?)assembly.Attribute("name") ?? "?");
            foreach (var test in assembly.Descendants("test"))
            {
                var traits = new Dictionary<string, List<string>>(StringComparer.Ordinal);
                foreach (var trait in test.Descendants("trait"))
                {
                    var key = (string)trait.Attribute("name")!;
                    if (!traits.TryGetValue(key, out var list))
                    {
                        traits[key] = list = [];
                    }

                    list.Add((string)trait.Attribute("value")!);
                }

                var method = (string?)test.Attribute("method") ?? (string?)test.Attribute("name") ?? "?";
                yield return new TestResult(name, method, (string?)test.Attribute("result") ?? "?", traits);
            }
        }
    }

    public static string ToMarkdown(IReadOnlyList<Row> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine("| Document | Case | Title | SP | Host project | Category | Result | Reason / evidence |");
        sb.AppendLine("|---|---|---|---|---|---|---|---|");
        foreach (var r in rows)
        {
            sb.AppendLine($"| {r.Document} | {r.Case} | {Escape(r.Title)} | {r.Sp} | {r.Host} | {r.Category} | {r.Result} | {Escape(r.Reason)} |");
        }

        return sb.ToString();
    }

    static string Escape(string s) => s.Replace("|", "\\|", StringComparison.Ordinal);
}
