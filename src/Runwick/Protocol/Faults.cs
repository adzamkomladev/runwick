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
    /// <summary>Called at <paramref name="point"/> of <paramref name="operation"/>.</summary>
    ValueTask OnAsync(FaultPoint point, string operation, Guid commandId, CancellationToken cancellationToken);
}

internal sealed class NullFaultInjector : IFaultInjector
{
    public static readonly NullFaultInjector Instance = new();

    public ValueTask OnAsync(FaultPoint point, string operation, Guid commandId, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
}

/// <summary>Thrown by a fault injector to simulate a reply lost after a successful commit.</summary>
internal sealed class InjectedReplyLossException(string operation)
    : IOException($"Injected reply loss after commit of {operation}.");

/// <summary>Thrown by a fault injector at a point before commit.</summary>
internal sealed class InjectedFaultException(FaultPoint point, string operation)
    : Exception($"Injected fault at {point} of {operation}.")
{
    public FaultPoint Point { get; } = point;
}
