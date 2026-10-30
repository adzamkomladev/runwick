using Runwick.Harness.Reports;

namespace Runwick.Workloads;

/// <summary>
/// <c>report --results dir --workloads dir --out file [--reasons file]</c>: writes the G-1 case table (07 §15.1 item 3).
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

        var rows = G1CaseTable.Build(xml, a.OptStr("workloads"), reasons);
        await File.WriteAllTextAsync(a.Str("out"), G1CaseTable.ToMarkdown(rows)).ConfigureAwait(false);
        foreach (var g in rows.GroupBy(r => r.Result).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            Console.WriteLine($"{g.Key}: {g.Count()}  {string.Join(' ', g.Where(r => r.Result is not "passed" and not "not-applicable").Select(r => r.Case))}");
        }

        return rows.All(r => r.Result is "passed" or "not-applicable") ? 0 : 1;
    }
}
