namespace Runwick.Protocol;

/// <summary>Named points inside a store command where tests may inject faults.</summary>
internal enum FaultPoint
{
    BeforeGateLock,
    AfterLocksBeforeFreshTime,
    BeforeFirstMutation,
    AfterMutation,
    BeforeCommit,
    AfterCommitBeforeReply,
    DuringReceiptLookup,
}

internal enum FaultAction
{
    Throw,
    Delay,
    Barrier,
    DropReply,
    KillProcess,
}

/// <summary>
/// Test seam for store commands. Production uses <see cref="NullFaultInjector"/>; there is no
/// public way to register another implementation.
/// </summary>
internal interface IFaultInjector
{
    /// <summary>Called at <see cref="FaultContext.Point"/> of a store command.</summary>
    ValueTask OnAsync(FaultContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Implemented by harness injectors (PF-05): whether any fault is armed at a point for this command, at any
/// step. A PostgreSQL server function stops at a point the harness planned (its <c>runwick.rw_fault</c> hook, present only
/// in fault-hooks stores) and the injector handles it there, exactly as at a client point.
/// </summary>
internal interface IFaultPlanner
{
    bool IsArmed(FaultPoint point, string operation, Guid commandId, string application);
}

/// <summary>Where a store command is: point, operation, command, and the 1-based mutation step (AfterMutation only).</summary>
internal readonly record struct FaultContext(FaultPoint Point, string Operation, Guid CommandId, string Application, int Step = 0);

internal sealed class NullFaultInjector : IFaultInjector
{
    public static readonly NullFaultInjector Instance = new();

    public ValueTask OnAsync(FaultContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

/// <summary>Thrown by a fault injector to simulate a reply lost after a successful commit.</summary>
internal sealed class InjectedReplyLossException(string operation)
    : IOException($"Injected reply loss after commit of {operation}.");

/// <summary>Thrown by a fault injector at a point before commit.</summary>
internal sealed class InjectedFaultException(FaultPoint point, string operation, string kind = "Transient")
    : Exception($"Injected {kind} fault at {point} of {operation}.")
{
    public FaultPoint Point { get; } = point;

    /// <summary><c>Transient</c>, <c>Io</c>, <c>Deadlock40P01</c> or <c>Serialization40001</c>.</summary>
    public string Kind { get; } = kind;
}
