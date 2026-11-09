using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Runwick.Management;

namespace Runwick.Worker;

/// <summary>
/// Measures one attempt's window (the runner's start to the handler's end) with process-wide counters: .NET has no per-job
/// CPU or allocation counter that survives async hops between threads, so every figure is the worker process's during the
/// window, and <see cref="AttemptResourceUsage.OverlappingAttempts"/> says how many other attempts shared it (0 = the
/// figures are this attempt's alone, plus the worker's own background work). CPU time and allocated bytes are read exactly
/// at both ends. Working set is costly to read (about 0.1 ms on Linux), so one shared timer samples it every
/// <see cref="SampleInterval"/> while any attempt runs; start, end and peak are those samples (a fresh read only when an
/// attempt starts in an idle process).
/// </summary>
internal sealed class ResourceMeter
{
    internal static readonly TimeSpan SampleInterval = TimeSpan.FromMilliseconds(250);

    static readonly ConcurrentDictionary<ResourceMeter, byte> Active = new();
    static readonly Lazy<Timer> Sampler = new(() => new Timer(static _ => Sample(), null, SampleInterval, SampleInterval));
    static long _workingSet;
    static long _starts;
    static int _active;

    readonly TimeSpan _cpuStart;
    readonly long _allocStart;
    readonly long _wsStart;
    readonly long _startsBefore;
    readonly int _othersAtStart;
    long _wsPeak;
    AttemptResourceUsage? _result;

    ResourceMeter(int othersAtStart, long startsBefore)
    {
        _othersAtStart = othersAtStart;
        _startsBefore = startsBefore;
        if (othersAtStart == 0)
        {
            Volatile.Write(ref _workingSet, Environment.WorkingSet); // idle process: the last sample may be old
        }

        _wsStart = _wsPeak = Volatile.Read(ref _workingSet);
        _cpuStart = Environment.CpuUsage.TotalTime;
        _allocStart = GC.GetTotalAllocatedBytes(false);
    }

    public static ResourceMeter Start()
    {
        _ = Sampler.Value;
        var others = Interlocked.Increment(ref _active) - 1;
        var meter = new ResourceMeter(others, Interlocked.Increment(ref _starts));
        Active[meter] = 0;
        return meter;
    }

    /// <summary>Ends the window once; later calls return the same figures (a Finish is frozen and may be resubmitted).</summary>
    public AttemptResourceUsage Stop()
    {
        if (Volatile.Read(ref _result) is { } done)
        {
            return done;
        }

        var cpu = Environment.CpuUsage.TotalTime - _cpuStart;
        var alloc = GC.GetTotalAllocatedBytes(false) - _allocStart;
        var wsEnd = Volatile.Read(ref _workingSet);
        var startedSince = Volatile.Read(ref _starts) - _startsBefore;
        var usage = new AttemptResourceUsage(Math.Max(0, cpu.Ticks / 10), Math.Max(0, alloc), _wsStart, wsEnd,
            Math.Max(Interlocked.Read(ref _wsPeak), wsEnd), (int)Math.Min(int.MaxValue, _othersAtStart + startedSince));
        if (Interlocked.CompareExchange(ref _result, usage, null) is { } raced)
        {
            return raced;
        }

        Active.TryRemove(this, out _);
        Interlocked.Decrement(ref _active);
        return usage;
    }

    static void Sample()
    {
        if (Volatile.Read(ref _active) == 0)
        {
            return;
        }

        var ws = Environment.WorkingSet;
        Volatile.Write(ref _workingSet, ws);
        foreach (var m in Active.Keys)
        {
            long seen;
            while ((seen = Interlocked.Read(ref m._wsPeak)) < ws && Interlocked.CompareExchange(ref m._wsPeak, ws, seen) != seen)
            {
            }
        }
    }
}

/// <summary>The one JSON shape every store keeps for an attempt's usage (a nullable column or field; null = not recorded).</summary>
internal static class ResourceUsageJson
{
    public static string Write(AttemptResourceUsage u) => new JsonObject
    {
        ["v"] = 1, ["scope"] = "process", ["cpuUs"] = u.ProcessCpuMicroseconds, ["allocBytes"] = u.ProcessAllocatedBytes,
        ["wsStart"] = u.WorkingSetStartBytes, ["wsEnd"] = u.WorkingSetEndBytes, ["wsPeak"] = u.WorkingSetPeakBytes,
        ["overlap"] = u.OverlappingAttempts,
    }.ToJsonString();

    /// <summary>Null for a missing, unknown-version or damaged value: the attempt shows as not recorded rather than failing the read.</summary>
    public static AttemptResourceUsage? Read(string? json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return null;
        }

        try
        {
            var o = JsonNode.Parse(json) as JsonObject;
            if (o is null || o["v"]?.GetValue<int>() != 1)
            {
                return null;
            }

            return new AttemptResourceUsage(o["cpuUs"]!.GetValue<long>(), o["allocBytes"]!.GetValue<long>(), o["wsStart"]!.GetValue<long>(),
                o["wsEnd"]!.GetValue<long>(), o["wsPeak"]!.GetValue<long>(), o["overlap"]!.GetValue<int>());
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException or NullReferenceException)
        {
            return null;
        }
    }
}
