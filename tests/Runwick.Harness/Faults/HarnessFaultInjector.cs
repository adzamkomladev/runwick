using System.Collections.Concurrent;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Protocol;

namespace Runwick.Harness.Faults;

/// <summary>Which store commands a fault applies to (07 §2.3).</summary>
internal sealed record FaultTarget
{
    public Guid? CommandId { get; init; }

    public string? Operation { get; init; }

    public string? Application { get; init; }

    /// <summary>AfterMutation only: the n-th mutation (1-based); null matches every step.</summary>
    public int? Step { get; init; }

    /// <summary>Fire on the n-th matching hit (1-based); null fires from the first.</summary>
    public int? Occurrence { get; init; }

    public static FaultTarget Any { get; } = new();

    public static FaultTarget Command(Guid id) => new() { CommandId = id };

    public static FaultTarget Op(string operation) => new() { Operation = operation };

    internal bool Matches(FaultContext c) =>
        (CommandId is null || CommandId == c.CommandId)
        && (Operation is null || Operation == c.Operation)
        && (Application is null || Application == c.Application)
        && (Step is null || Step == c.Step);

    public override string ToString() => $"{Operation ?? "*"}/{CommandId?.ToString("N")[..8] ?? "*"}{(Step is { } s ? "#" + s : "")}";
}

/// <summary>A harness fault action with its parameter.</summary>
internal sealed record FaultSpec(FaultAction Action, string ExceptionKind = "Transient", TimeSpan Delay = default)
{
    public static FaultSpec Throw(string kind = "Transient") => new(FaultAction.Throw, kind);

    public static FaultSpec Sleep(TimeSpan delay) => new(FaultAction.Delay, Delay: delay);

    public static FaultSpec Barrier { get; } = new(FaultAction.Barrier);

    public static FaultSpec DropReply { get; } = new(FaultAction.DropReply);
}

/// <summary>Handle to an armed fault: how often it fired and its barrier, if any.</summary>
internal sealed class FaultHandle(FaultPoint point, FaultSpec spec, FaultTarget target, int times)
{
    int _remaining = times;
    int _hits;
    int _fired;

    public FaultPoint Point => point;

    public FaultSpec Spec => spec;

    public FaultTarget Target => target;

    public int Fired => Volatile.Read(ref _fired);

    /// <summary>Whether the handle can still fire (an exhausted handle stays listed but plans nothing).</summary>
    public bool CanFire => Volatile.Read(ref _remaining) > 0;

    public TaskBarrier Barrier { get; } = new($"{point}:{target}");

    internal readonly ConcurrentQueue<FaultContext> Contexts = new();

    internal bool TryClaim(FaultContext context)
    {
        if (context.Point != point || !target.Matches(context))
        {
            return false;
        }

        var hit = Interlocked.Increment(ref _hits);
        if (target.Occurrence is { } n && hit < n)
        {
            return false;
        }

        if (Interlocked.Decrement(ref _remaining) < 0)
        {
            return false;
        }

        Interlocked.Increment(ref _fired);
        Contexts.Enqueue(context);
        return true;
    }
}

/// <summary>The in-process <see cref="IFaultInjector"/> used by DriverFixture clients.</summary>
internal sealed class HarnessFaultInjector : IFaultInjector, IFaultPlanner
{
    /// <summary>Whether a fault is armed at a point for this command at any step (server points, PF-05).</summary>
    public bool IsArmed(FaultPoint point, string operation, Guid commandId, string application)
    {
        lock (_gate)
        {
            return _armed.Exists(h => h.CanFire && h.Point == point
                && (h.Target with { Step = null }).Matches(new FaultContext(point, operation, commandId, application)));
        }
    }

    readonly List<FaultHandle> _armed = [];
    readonly object _gate = new();

    public ConcurrentQueue<string> Log { get; } = new();

    public FaultHandle Arm(FaultPoint point, FaultSpec spec, FaultTarget target, int times = 1)
    {
        if (spec.Action == FaultAction.DropReply && point != FaultPoint.AfterCommitBeforeReply)
        {
            throw new ArgumentException("DropReply is valid only at AfterCommitBeforeReply.");
        }

        if (spec.Action == FaultAction.Throw && point == FaultPoint.AfterCommitBeforeReply)
        {
            throw new ArgumentException("Throw is not valid at AfterCommitBeforeReply.");
        }

        if (spec.Action == FaultAction.KillProcess)
        {
            throw new ArgumentException("KillProcess is valid only for peer clients.");
        }

        var handle = new FaultHandle(point, spec, target, times);
        lock (_gate)
        {
            _armed.Add(handle);
        }

        return handle;
    }

    public void DisarmAll()
    {
        lock (_gate)
        {
            foreach (var h in _armed)
            {
                h.Barrier.Release();
            }

            _armed.Clear();
        }
    }

    public async ValueTask OnAsync(FaultContext context, CancellationToken cancellationToken)
    {
        FaultHandle? handle;
        lock (_gate)
        {
            handle = _armed.FirstOrDefault(h => h.TryClaim(context));
        }

        if (handle is null)
        {
            return;
        }

        Log.Enqueue($"{context.Point} {context.Operation} {context.CommandId:N} step={context.Step} -> {handle.Spec.Action}");
        switch (handle.Spec.Action)
        {
            case FaultAction.Throw:
                throw new InjectedFaultException(context.Point, context.Operation, handle.Spec.ExceptionKind);
            case FaultAction.Delay:
                await Task.Delay(handle.Spec.Delay, cancellationToken).ConfigureAwait(false);
                break;
            case FaultAction.Barrier:
                await handle.Barrier.ArriveAndWaitAsync(CancellationToken.None).ConfigureAwait(false);
                break;
            case FaultAction.DropReply:
                throw new NpgsqlException("Injected reply loss after commit.", new IOException("connection reset (injected)"));
        }
    }
}
