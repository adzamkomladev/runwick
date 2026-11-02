using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Store;
using Runwick.Protocol;

namespace Runwick.Harness.Peer;

/// <summary>Serialized writer for stdout (one JSON object per line).</summary>
internal sealed class PeerOutput(TextWriter writer)
{
    readonly object _gate = new();

    public void Write(JsonObject message)
    {
        lock (_gate)
        {
            writer.WriteLine(message.ToJsonString());
            writer.Flush();
        }
    }
}

/// <summary>Peer-side fault injector: like the in-process one, plus <c>KillProcess</c> (block and report; the fixture SIGKILLs).</summary>
internal sealed class PeerFaultInjector(PeerOutput output) : IFaultInjector, IFaultPlanner
{
    /// <summary>Whether a fault is armed at a point for this operation at any step (server points, PF-05).</summary>
    public bool IsArmed(FaultPoint point, string operation, Guid commandId, string application)
    {
        lock (_gate)
        {
            return _armed.Exists(a => a.Remaining > 0 && a.Point == point && (a.Operation is null || a.Operation == operation));
        }
    }

    sealed record Armed(FaultPoint Point, string Action, string? Operation, int? Step, string Kind, int DelayMs)
    {
        public int Remaining;
    }

    readonly List<Armed> _armed = [];
    readonly ConcurrentDictionary<FaultPoint, TaskCompletionSource> _barriers = new();
    readonly object _gate = new();

    public void Arm(JsonObject m)
    {
        var armed = new Armed(
            Enum.Parse<FaultPoint>(m["point"]!.GetValue<string>()),
            m["action"]!.GetValue<string>(),
            m["operation"]?.GetValue<string>(),
            m["step"]?.GetValue<int>(),
            m["kind"]?.GetValue<string>() ?? "Transient",
            m["delayMs"]?.GetValue<int>() ?? 0)
        {
            Remaining = m["times"]?.GetValue<int>() ?? 1,
        };
        lock (_gate)
        {
            _armed.Add(armed);
        }
    }

    public void Release(FaultPoint point)
    {
        if (_barriers.TryRemove(point, out var tcs))
        {
            tcs.TrySetResult();
        }
    }

    public async ValueTask OnAsync(FaultContext context, CancellationToken cancellationToken)
    {
        Armed? hit;
        lock (_gate)
        {
            hit = _armed.FirstOrDefault(a => a.Remaining > 0 && a.Point == context.Point
                && (a.Operation is null || a.Operation == context.Operation) && (a.Step is null || a.Step == context.Step));
            if (hit is not null)
            {
                hit.Remaining--;
            }
        }

        if (hit is null)
        {
            return;
        }

        var report = new JsonObject
        {
            ["type"] = "atBarrier",
            ["point"] = context.Point.ToString(),
            ["operation"] = context.Operation,
            ["commandId"] = context.CommandId.ToString("D"),
            ["step"] = context.Step,
            ["action"] = hit.Action,
        };
        switch (hit.Action)
        {
            case "Throw":
                throw new InjectedFaultException(context.Point, context.Operation, hit.Kind);
            case "Delay":
                await Task.Delay(hit.DelayMs, cancellationToken).ConfigureAwait(false);
                break;
            case "Barrier":
                var tcs = _barriers.GetOrAdd(context.Point, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                output.Write(report);
                await tcs.Task.ConfigureAwait(false);
                break;
            case "KillProcess":
                output.Write(report);
                await Task.Delay(Timeout.Infinite, CancellationToken.None).ConfigureAwait(false);
                break;
            case "DropReply":
                throw new NpgsqlException("Injected reply loss after commit.", new IOException("connection reset (injected)"));
        }
    }
}
