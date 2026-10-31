using Runwick.Harness.Reports;

namespace Runwick.Workloads;

/// <summary>
/// <c>report --results dir --workloads dir --out file [--reasons file] [--gate g1|g2]</c>: writes the G-1 case table
/// (07 §15.1 item 3) or, with <c>--gate g2</c>, the G-2 case table and acceptance checklist (15 §12.1, §12.2 rule 6).
/// <c>--reasons</c> is an optional <c>CASE&lt;TAB&gt;reason</c> file with the human-added reasons and evidence.
/// Prints the row counts by result; exits 0 only when §15.2 rules 1–3 hold for the table.
/// </summary>
internal static class ReportMode
{
    public static async Task<int> RunAsync(Args a)
    {
        var xml = Directory.EnumerateFiles(a.Str("results"), "*.xml", SearchOption.AllDirectories).Order(StringComparer.Ordinal).ToList();
        Dictionary<string, string>? reasons = null;
        if (a.OptStr("reasons") is { } path)
        {
            reasons = (await File.ReadAllLinesAsync(path).ConfigureAwait(false)).Where(l => l.Contains('\t', StringComparison.Ordinal))
                .Select(l => l.Split('\t', 2)).ToDictionary(p => p[0], p => p[1], StringComparer.Ordinal);
        }

        var g2 = a.OptStr("gate") == "g2";
        var rows = g2 ? G2CaseTable.Build(xml, a.OptStr("workloads"), reasons) : G1CaseTable.Build(xml, a.OptStr("workloads"), reasons);
        var markdown = G1CaseTable.ToMarkdown(rows);
        if (g2)
        {
            markdown += Environment.NewLine + "## Acceptance checklist (15 §12.2 rule 6)" + Environment.NewLine + Environment.NewLine + G2CaseTable.Checklist(rows);
        }

        await File.WriteAllTextAsync(a.Str("out"), markdown).ConfigureAwait(false);
        foreach (var g in rows.GroupBy(r => r.Result).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            Console.WriteLine($"{g.Key}: {g.Count()}  {string.Join(' ', g.Where(r => r.Result is not "passed" and not "not-applicable").Select(r => r.Case))}");
        }

        return rows.All(r => r.Result is "passed" or "not-applicable") ? 0 : 1;
    }
}
