using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json.Nodes;
using Runwick.Registration;

namespace Runwick.Worker;

/// <summary>
/// What a worker reports about itself with each presence publication (05 §14, 16 §2 <c>WorkerSummary.Detail</c>): host
/// figures, pools, settings, the attempts it runs now and the last ones it finished (kept in memory only). Built fresh on
/// each publication; a part that cannot be read is left out, and building never throws.
/// </summary>
internal sealed class WorkerDetail(WorkerEngine engine)
{
    const int MaxRunning = 20, MaxRecent = 10;

    readonly Lock _lock = new();
    readonly Queue<JsonObject> _recent = new();
    TimeSpan _lastCpu;
    long _lastStamp;

    /// <summary>Remembers an attempt that ran on this worker once it retires.</summary>
    public void Finished(AttemptEntry entry)
    {
        if (entry.ExecutionStartedAt is not { } started)
        {
            return;
        }

        var outcome = entry.Loss is not null ? "ownership_lost"
            : entry.FrozenFinish is { } f ? WorkerEngine.Snake(f.Outcome.ToString())
            : entry.Staged is not null ? "succeeded"
            : "unknown";
        var item = Attempt(entry, started);
        item["outcome"] = outcome;
        item["finishedAt"] = engine.Clock.WallNow;
        item["cpuMicroseconds"] = entry.Meter?.Stop().ProcessCpuMicroseconds;
        lock (_lock)
        {
            _recent.Enqueue(item);
            if (_recent.Count > MaxRecent)
            {
                _recent.Dequeue();
            }
        }
    }

    public JsonObject Build()
    {
        var o = new JsonObject { ["v"] = 2 };
        Part(o, "host", Host);
        Part(o, "handlers", () => new JsonArray([.. engine.Pipelines.Keys.Order(StringComparer.Ordinal).Select(n => (JsonNode?)n)]));
        Part(o, "pools", () => new JsonArray([.. engine.Pools.Select(p => (JsonNode?)Pool(p))]));
        Part(o, "settings", Settings);
        Part(o, "running", () =>
        {
            var running = engine.Entries.Where(e => e.ExecutionStartedAt is not null && e.Phase != AttemptPhase.Retired).ToList();
            o["runningCount"] = running.Count;
            return new JsonArray([.. running.OrderBy(e => e.ExecutionStartedAt).Take(MaxRunning)
                .Select(e => (JsonNode?)Attempt(e, e.ExecutionStartedAt!.Value))]);
        });
        Part(o, "recent", () =>
        {
            lock (_lock)
            {
                return new JsonArray([.. _recent.Reverse().Select(r => r.DeepClone())]);
            }
        });
        return o;
    }

    static void Part(JsonObject o, string name, Func<JsonNode> read)
    {
        try
        {
            o[name] = read();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Left out: the detail is observability only and must never fail the presence publication.
        }
    }

    JsonObject Attempt(AttemptEntry e, TimeSpan started) => new()
    {
        ["jobId"] = e.Lease.JobId, ["attemptId"] = e.AttemptId, ["jobName"] = e.Reserved.Contract.Name, ["queue"] = e.Reserved.Queue,
        ["pool"] = e.Pool.Settings.Name, ["startedAt"] = engine.Clock.WallNow - (engine.Clock.Now - started),
    };

    static JsonObject Pool(PoolCoordinator p) => new()
    {
        ["name"] = p.Settings.Name,
        ["queues"] = new JsonArray([.. p.Settings.Queues.Select(q => (JsonNode?)new JsonObject { ["queue"] = q.Queue, ["weight"] = q.Weight })]),
        ["mode"] = p.Settings.Auto is null ? "fixed" : "auto",
        ["target"] = p.Gate.Target, ["min"] = p.Settings.Auto?.Min, ["max"] = p.Settings.Max, ["inFlight"] = p.Gate.Outstanding,
        ["laneSelection"] = p.Settings.Mode.ToString(),
        ["laneWeights"] = new JsonObject { ["high"] = p.Settings.LaneWeights.High, ["medium"] = p.Settings.LaneWeights.Medium, ["low"] = p.Settings.LaneWeights.Low },
    };

    /// <summary>Every TimeSpan option as milliseconds (<c>leaseDurationMs</c>) and every bool option as is.</summary>
    JsonObject Settings()
    {
        var o = new JsonObject();
        foreach (var p in typeof(WorkerOptions).GetProperties())
        {
            var name = char.ToLowerInvariant(p.Name[0]) + p.Name[1..];
            switch (p.GetValue(engine.Options))
            {
                case TimeSpan t:
                    o[name + "Ms"] = (long)t.TotalMilliseconds;
                    break;
                case bool b:
                    o[name] = b;
                    break;
            }
        }

        return o;
    }

    JsonObject Host()
    {
        var cpu = Environment.CpuUsage.TotalTime;
        var stamp = Stopwatch.GetTimestamp();
        double percent = 0;
        lock (_lock)
        {
            // CPU time over wall time since the previous publication, across all cores; 0 on the first.
            if (_lastStamp != 0 && Stopwatch.GetElapsedTime(_lastStamp, stamp) is { Ticks: > 0 } wall)
            {
                percent = (cpu - _lastCpu) / wall / Environment.ProcessorCount * 100;
            }

            (_lastCpu, _lastStamp) = (cpu, stamp);
        }

        var o = new JsonObject
        {
            ["os"] = RuntimeInformation.OSDescription, ["runtime"] = RuntimeInformation.FrameworkDescription,
            ["arch"] = RuntimeInformation.ProcessArchitecture.ToString(), ["cpus"] = Environment.ProcessorCount,
        };
        var (total, available) = Memory();
        o["memoryTotalBytes"] = total;
        o["memoryAvailableBytes"] = available;
        o["processCpuPercent"] = Math.Round(Math.Max(0, percent), 1);
        o["workingSetBytes"] = Environment.WorkingSet;
        o["gcHeapBytes"] = GC.GetTotalMemory(false);
        return o;
    }

    /// <summary>Linux: /proc/meminfo (MemTotal, MemAvailable). Elsewhere, or when it cannot be read: what the GC sees.</summary>
    static (long? Total, long? Available) Memory()
    {
        if (OperatingSystem.IsLinux())
        {
            try
            {
                long? total = null, available = null;
                foreach (var line in File.ReadLines("/proc/meminfo"))
                {
                    if (line.StartsWith("MemTotal:", StringComparison.Ordinal))
                    {
                        total = Kib(line);
                    }
                    else if (line.StartsWith("MemAvailable:", StringComparison.Ordinal))
                    {
                        available = Kib(line);
                    }
                }

                if (total is not null)
                {
                    return (total, available);
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
            }
        }

        var gc = GC.GetGCMemoryInfo();
        return (gc.TotalAvailableMemoryBytes, gc.TotalAvailableMemoryBytes - gc.MemoryLoadBytes);
    }

    static long? Kib(string line) =>
        line.Split(' ', StringSplitOptions.RemoveEmptyEntries) is [_, var n, ..] && long.TryParse(n, NumberStyles.None, CultureInfo.InvariantCulture, out var kib)
            ? kib * 1024
            : null;
}
