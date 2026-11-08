// Runwick.Scenarios: executable scenarios for docs/scenarios.md (SP-027).
//   list                  one line per scenario: "SCN-nn<TAB>title"
//   run SCN-nn            runs one scenario; prints "PASS|FAIL SCN-nn title (detail) seconds"; exit 0 on PASS
//   node --store ... ...  one deployment process (used by the scenarios; see NodeOptions)
// Needs RUNWICK_TEST_PG (default 127.0.0.1:55499 postgres/pg) and RUNWICK_TEST_REDIS (default 127.0.0.1:56392). samples/run-scenarios.sh sets both.
using System.Diagnostics;

switch (args.FirstOrDefault())
{
    case "node":
        return await NodeHost.RunNodeAsync(args);
    case "list":
        foreach (var s in Catalogue.All)
        {
            Console.WriteLine($"{s.Id}\t{s.Title}");
        }

        return 0;
    case "run" when args.Length == 2 && Catalogue.All.FirstOrDefault(s => s.Id == args[1]) is { } scenario:
        var clock = Stopwatch.StartNew();
        try
        {
            var detail = await scenario.Run();
            Console.WriteLine($"PASS {scenario.Id} {scenario.Title} ({detail}) {clock.Elapsed.TotalSeconds:F1}s");
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"FAIL {scenario.Id} {scenario.Title} ({ex.GetType().Name}: {ex.Message}) {clock.Elapsed.TotalSeconds:F1}s");
            return 1;
        }

    default:
        Console.Error.WriteLine("usage: list | run SCN-nn | node ...");
        return 2;
}
