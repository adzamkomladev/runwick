using System.Globalization;
using System.Text.Json.Nodes;

namespace Runwick.Workloads;

/// <summary>
/// <c>bench --lease-crash --store sqlite|postgres|redis</c> (G5 crash-detection latency). A first worker process takes
/// <c>--workers</c> long jobs (one per slot) and is killed (SIGKILL, no graceful stop) as soon as all of them have started. A
/// second worker process starts on the same store at once. For each job the first worker was running, the result gives the time
/// from the kill to the moment the second worker started it: about the lease length, since nothing else frees a dead worker's
/// jobs. The lease flags (<c>--fewer-lease-renewals</c>, <c>--lease-seconds</c>, <c>--renew-seconds</c>) and the store flags pass
/// through to both workers.
/// </summary>
internal static class LeaseCrashRun
{
    // Flags the parent sets per child; the copies the user gave are not forwarded.
    static readonly HashSet<string> ParentOnly =
        ["lease-crash", "out", "mode", "seconds", "rate", "job-ms", "n", "ledger", "sqlite-reuse", "db-reuse", "redis-reuse", "detect-seconds", "calls"];

    public static async Task<int> RunAsync(Args a)
    {
        var store = a.OptStr("store") ?? "postgres";
        var jobs = a.OptInt("slots") ?? a.Int("workers", 16);
        var lease = a.Has("fewer-lease-renewals") ? 300 : a.Double("lease-seconds", 60);
        var detectSeconds = a.Double("detect-seconds", 2 * lease + 60);
        var dir = Directory.CreateDirectory(Path.Combine(a.OptStr("sqlite-dir") ?? Path.GetTempPath(), "bench-lease-crash-" + Guid.NewGuid().ToString("N"))).FullName;
        var forwarded = new List<string>();
        for (var i = 0; i < a.Raw.Count; i++)
        {
            var name = a.Raw[i].StartsWith("--", StringComparison.Ordinal) ? a.Raw[i][2..] : null;
            var hasValue = name is not null && i + 1 < a.Raw.Count && !a.Raw[i + 1].StartsWith("--", StringComparison.Ordinal);
            if (name is not null && ParentOnly.Contains(name))
            {
                i += hasValue ? 1 : 0;
                continue;
            }

            forwarded.Add(a.Raw[i]);
        }

        List<string> reuse = store switch
        {
            "sqlite" => ["--sqlite-reuse", Path.Combine(dir, "runwick.db")],
            "redis" => ["--redis-reuse"],
            _ => ["--db-reuse"],
        };
        var ledger1 = Path.Combine(dir, "ledger-1.txt");
        var ledger2 = Path.Combine(dir, "ledger-2.txt");

        // First worker: a long job for every slot (and a few extra that wait), offered in 1.5 s; it never finishes them.
        var first = ChildProcess.Start("worker-1", Path.Combine(dir, "worker-1.log"),
            ["bench", .. forwarded, .. store == "sqlite" ? reuse : [], "--mode", "steady", "--rate", Inv(jobs), "--seconds", "1.5",
                "--job-ms", "3600000", "--ledger", ledger1, "--out", Path.Combine(dir, "worker-1.json")]);
        var startDeadline = DateTime.UtcNow.AddSeconds(120);
        // Kill once every slot runs a job and the 1.5 s of offers is over, so the ledger names every job the store accepted.
        while (Read(ledger1) is var seen && (seen.Count(l => l.Kind == 'i') < jobs
            || DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < seen.Where(l => l.Kind == 'a').Select(l => l.Ms).DefaultIfEmpty(long.MaxValue / 2).Min() + 2500))
        {
            if (first.HasExited || DateTime.UtcNow > startDeadline)
            {
                Console.Error.WriteLine($"the first worker did not start {jobs} jobs; see {dir}/worker-1.log");
                await first.DisposeAsync().ConfigureAwait(false);
                return 1;
            }

            await Task.Delay(100).ConfigureAwait(false);
        }

        var killedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await first.DisposeAsync().ConfigureAwait(false); // SIGKILL of the whole process tree

        var firstRun = Read(ledger1);
        var accepted = firstRun.Where(l => l.Kind == 'a').Select(l => l.Id).ToHashSet();
        var finishedFirst = firstRun.Where(l => l.Kind == 'f').Select(l => l.Id).ToHashSet();
        var orphans = firstRun.Where(l => l.Kind == 'i' && !finishedFirst.Contains(l.Id)).Select(l => l.Id).ToHashSet();

        // Second worker: no new jobs, no delay in the handler; it stops once every accepted job has finished.
        await using var second = ChildProcess.Start("worker-2", Path.Combine(dir, "worker-2.log"),
            ["bench", .. forwarded, .. reuse, "--mode", "work", "--n", Inv(accepted.Count), "--seconds", Inv(detectSeconds),
                "--job-ms", "0", "--ledger", ledger2, "--out", Path.Combine(dir, "worker-2.json")]);
        var secondExit = await second.StopAsync(TimeSpan.FromSeconds(detectSeconds + 60)).ConfigureAwait(false);

        var pickup = new Dictionary<Guid, long>();
        foreach (var line in Read(ledger2).Where(l => l.Kind == 'i' && orphans.Contains(l.Id)))
        {
            pickup.TryAdd(line.Id, line.Ms - killedAtMs);
        }

        var detection = pickup.Values.Select(v => (double)v).ToList();
        var secondOut = Path.Combine(dir, "worker-2.json");
        var result = new JsonObject
        {
            ["system"] = store == "postgres" ? "runwick" : "runwick-" + store,
            ["store"] = store,
            ["mode"] = "lease-crash",
            ["fewerLeaseRenewals"] = a.Has("fewer-lease-renewals"),
            ["leaseSeconds"] = lease,
            ["jobsAccepted"] = accepted.Count,
            ["orphans"] = orphans.Count,
            ["orphansPickedUp"] = pickup.Count,
            ["orphansNotPickedUp"] = orphans.Count - pickup.Count,
            ["detectionMinMs"] = detection.Count == 0 ? null : detection.Min(),
            ["detectionMs"] = SteadyCrashRun.Percentiles(detection),
            ["secondWorkerExit"] = secondExit,
            ["secondWorker"] = File.Exists(secondOut) ? JsonNode.Parse(await File.ReadAllTextAsync(secondOut).ConfigureAwait(false)) : null,
            ["directory"] = dir,
        };
        var json = result.ToJsonString(Json.Indented);
        Console.WriteLine(json);
        if (a.OptStr("out") is { } outFile)
        {
            await File.WriteAllTextAsync(outFile, json).ConfigureAwait(false);
        }

        return 0;
    }

    static string Inv(double value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>A ledger's lines (kind, job, wall-clock ms); a line cut off by the kill is skipped.</summary>
    static List<(char Kind, Guid Id, long Ms)> Read(string path)
    {
        var lines = new List<(char, Guid, long)>();
        if (!File.Exists(path))
        {
            return lines;
        }

        using var reader = new StreamReader(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
        while (reader.ReadLine() is { } line)
        {
            var parts = line.Split(' ');
            if (parts.Length == 3 && Guid.TryParseExact(parts[1], "N", out var id) && long.TryParse(parts[2], CultureInfo.InvariantCulture, out var ms))
            {
                lines.Add((parts[0][0], id, ms));
            }
        }

        return lines;
    }
}
