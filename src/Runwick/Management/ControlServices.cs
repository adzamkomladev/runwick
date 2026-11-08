using Runwick.Hosting;
using Runwick.Protocol;

namespace Runwick.Management;

// Preview control surface (08 §10). Transport-free, no authorization: the host supplies a trusted actor.
// HTTP and authorization arrive with SP-020; IScheduleControl with SP-016.

/// <summary>The host-authenticated principal behind a control (08 §2). Preview.</summary>
public sealed record ManagementActor(string Subject, string? Display, string Scheme);

/// <summary>A job addressed at the position the caller last observed. Preview.</summary>
public readonly record struct JobTarget(JobId JobId, int ExpectedGeneration, long ExpectedRevision);

/// <summary>The outcome of one job control. A rejection is a result, not an exception. Preview.</summary>
public sealed record JobControlResult(
    JobId JobId, bool Succeeded, string? Code, string? Effect, int? Generation, long? Revision, string? State, bool Replayed)
{
    /// <summary>The job's generation when a position conflict rejected the control (stale generation vs stale revision).</summary>
    internal int? ActualGeneration { get; init; }
}

/// <summary>Preview.</summary>
public sealed record QueueControlResult(string Queue, bool Succeeded, string? Code, bool? Paused, long? Revision, bool Replayed);

/// <summary>Preview. <see cref="LivePermits"/> is the live count when the edit committed; a decrease revokes nothing.</summary>
public sealed record CapacityControlResult(
    string ScopeKind, string ScopeKey, bool Succeeded, string? Code, int? Capacity, long? PolicyRevision, int? LivePermits, bool Replayed);

/// <summary>Cancel, retry and rerun individual jobs (08 §3.2–3.3). Preview.</summary>
public interface IJobControl
{
    Task<JobControlResult> CancelAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default);

    Task<JobControlResult> RetryAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default);

    Task<JobControlResult> RerunAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>One command per target, each with its own receipt and audit row; results are in target order (CTL-05).</summary>
    Task<IReadOnlyList<JobControlResult>> CancelManyAsync(IReadOnlyList<JobTarget> targets, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);
}

/// <summary>Pause and resume queues (08 §3.4). Pause blocks Reserve only. Preview.</summary>
public interface IQueueControl
{
    Task<QueueControlResult> PauseAsync(string queue, long expectedRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<QueueControlResult> ResumeAsync(string queue, long expectedRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);
}

/// <summary>Edit a registered scope's capacity (08 §3.5). Membership is never editable. Preview.</summary>
public interface ICapacityControl
{
    Task<CapacityControlResult> SetScopeCapacityAsync(string scopeKind, string scopeKey, int capacity, long expectedPolicyRevision,
        ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// The one implementation of the three preview services over <see cref="IJobControlStore"/>. A command whose outcome
/// is unknown is resent with the same ID and body until it resolves or its NotAfter passes (03 §8.2).
/// </summary>
internal sealed class ControlService(RunwickRuntime runtime, IJobControlStore? store, TimeProvider time) : IJobControl, IQueueControl, ICapacityControl
{
    static readonly TimeSpan[] Backoff = [TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1)];

    IJobControlStore Store => store ?? throw new InvalidOperationException("The configured store does not support revisioned controls.");

    public Task<JobControlResult> CancelAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default) =>
        JobAsync(Operations.Cancel, target, actor, reason, null, cancellationToken);

    public Task<JobControlResult> RetryAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default) =>
        JobAsync(Operations.ManualRetry, target, actor, reason, null, cancellationToken);

    public Task<JobControlResult> RerunAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default) =>
        JobAsync(Operations.RerunSucceeded, target, actor, reason, null, cancellationToken);

    public async Task<IReadOnlyList<JobControlResult>> CancelManyAsync(IReadOnlyList<JobTarget> targets, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targets);
        var results = new List<JobControlResult>(targets.Count);
        foreach (var target in targets)
        {
            results.Add(await CancelAsync(target, actor, reason, cancellationToken).ConfigureAwait(false));
        }

        return results;
    }

    public Task<QueueControlResult> PauseAsync(string queue, long expectedRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default) => QueueAsync(queue, true, expectedRevision, actor, reason, null, cancellationToken);

    public Task<QueueControlResult> ResumeAsync(string queue, long expectedRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default) => QueueAsync(queue, false, expectedRevision, actor, reason, null, cancellationToken);

    internal async Task<QueueControlResult> QueueAsync(string queue, bool paused, long expectedRevision, ManagementActor actor, string? reason, CommandSeed? seed,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        var reply = await SendAsync(Actor(actor, reason), a => new QueueStateRequest(queue, paused, expectedRevision, a), Store.SetQueueStateAsync, seed, ct)
            .ConfigureAwait(false);
        return reply.Value is { } v
            ? new QueueControlResult(queue, true, null, v.Paused, v.Revision, reply.Replayed)
            : new QueueControlResult(queue, false, reply.Error?.Code, null, reply.Error?.ActualRevision, false);
    }

    public Task<CapacityControlResult> SetScopeCapacityAsync(string scopeKind, string scopeKey, int capacity, long expectedPolicyRevision,
        ManagementActor actor, string? reason = null, CancellationToken cancellationToken = default) =>
        ScopeAsync(scopeKind, scopeKey, capacity, expectedPolicyRevision, actor, reason, null, cancellationToken);

    internal async Task<CapacityControlResult> ScopeAsync(string scopeKind, string scopeKey, int capacity, long expectedPolicyRevision,
        ManagementActor actor, string? reason, CommandSeed? seed, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKind);
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        var reply = await SendAsync(Actor(actor, reason), a => new ScopePolicyRequest(scopeKind, scopeKey, capacity, expectedPolicyRevision, a),
            Store.EditScopePolicyAsync, seed, cancellationToken).ConfigureAwait(false);
        return reply.Value is { } v
            ? new CapacityControlResult(scopeKind, scopeKey, true, null, v.Capacity, v.PolicyRevision, v.LivePermits, reply.Replayed)
            : new CapacityControlResult(scopeKind, scopeKey, false, reply.Error?.Code, null, reply.Error?.ActualRevision, null, false);
    }

    /// <summary>One job control; <paramref name="seed"/> is the caller's command ID and NotAfter (SP-020), else a new command.</summary>
    internal async Task<JobControlResult> JobAsync(string operation, JobTarget target, ManagementActor actor, string? reason, CommandSeed? seed,
        CancellationToken ct)
    {
        var controlActor = Actor(actor, reason);
        await runtime.EnsureReadyAsync(ct).ConfigureAwait(false);
        var command = NewCommand(controlActor, seed);
        var retry = new RetryRequest(target.JobId.Value, target.ExpectedGeneration, target.ExpectedRevision, controlActor);
        Func<CancellationToken, Task<StoreReply<ControlledJob>>> call = operation switch
        {
            Operations.Cancel => t => Store.CancelAsync(
                command.With(new CancelRequest(target.JobId.Value, target.ExpectedGeneration, target.ExpectedRevision, controlActor)), t),
            Operations.ManualRetry => t => Store.ManualRetryAsync(command.With(retry), t),
            _ => t => Store.RerunSucceededAsync(command.With(retry), t),
        };
        var reply = await ResendAsync(command.NotAfter, call, ct).ConfigureAwait(false);
        return reply.Value is { } v
            ? new JobControlResult(target.JobId, true, null, v.Effect, v.Position.Generation, v.Position.Revision, v.Position.State, reply.Replayed)
            : new JobControlResult(target.JobId, false, reply.Error?.Code, null, null, reply.Error?.ActualRevision, null, false)
            {
                ActualGeneration = ActualGeneration(reply.Error?.Detail),
            };
    }

    /// <summary>A position conflict's detail is <c>actual=generation/revision</c> (08 §3.1).</summary>
    static int? ActualGeneration(string? detail) =>
        detail is not null && detail.StartsWith("actual=", StringComparison.Ordinal) && detail[7..].Split('/') is [var g, _]
            && int.TryParse(g, System.Globalization.CultureInfo.InvariantCulture, out var gen) ? gen : null;

    internal async Task<StoreReply<TResult>> SendAsync<TRequest, TResult>(ControlActor actor, Func<ControlActor, TRequest> body,
        Func<StoreCommand<TRequest>, CancellationToken, Task<StoreReply<TResult>>> send, CommandSeed? seed, CancellationToken ct) where TResult : class
    {
        await runtime.EnsureReadyAsync(ct).ConfigureAwait(false);
        var command = NewCommand(actor, seed).With(body(actor));
        return await ResendAsync(command.NotAfter, t => send(command, t), ct).ConfigureAwait(false);
    }

    /// <summary>Same command until it resolves: a lost reply never becomes a second, differently identified command.</summary>
    async Task<StoreReply<T>> ResendAsync<T>(DateTimeOffset notAfter, Func<CancellationToken, Task<StoreReply<T>>> send, CancellationToken ct) where T : class
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await send(ct).ConfigureAwait(false);
            }
            catch (StoreOutcomeUnknownException) when (time.GetUtcNow() < notAfter)
            {
                // Resend the same command: the receipt returns the committed reply if the first one applied.
            }
            catch (StoreOutcomeUnknownException ex)
            {
                throw new ControlOutcomeUnknownException(ex.CommandId, ex);
            }
            catch (StoreNotAppliedException) when (time.GetUtcNow() < notAfter && attempt < Backoff.Length)
            {
                // Known not applied: safe to resend the same command.
            }
            catch (StoreNotAppliedException ex)
            {
                throw new StoreUnavailableException($"The control was not applied ({ex.Code}).", ex);
            }

            await Task.Delay(Backoff[Math.Min(attempt, Backoff.Length - 1)], time, ct).ConfigureAwait(false);
        }
    }

    StoreCommand<ControlActor> NewCommand(ControlActor actor, CommandSeed? seed)
    {
        var s = seed ?? CommandSeed.New(time, Operations.AdministrativeLifetime);
        return new(runtime.Registry.Application, s.CommandId, s.NotAfter, actor);
    }

    static ControlActor Actor(ManagementActor actor, string? reason)
    {
        ArgumentNullException.ThrowIfNull(actor);
        return new ControlActor(actor.Subject, actor.Display, actor.Scheme, reason);
    }
}

/// <summary>A control's outcome could not be confirmed before its deadline; inspect the job's events (08 §7). Preview.</summary>
public sealed class ControlOutcomeUnknownException(Guid commandId, Exception? innerException = null)
    : RunwickException(RunwickErrorCodes.OutcomeUnknown, ErrorDisposition.OutcomeUnknown,
        $"The outcome of control command {commandId} is unknown.", innerException)
{
    public Guid CommandId { get; } = commandId;
}

/// <summary>A control command's identity and deadline: a new one, or the one an SP-020 client supplied and may resend.</summary>
internal readonly record struct CommandSeed(Guid CommandId, DateTimeOffset NotAfter)
{
    public static CommandSeed New(TimeProvider time, TimeSpan lifetime) => new(Guid.CreateVersion7(), Ms(time.GetUtcNow() + lifetime));

    public static DateTimeOffset Ms(DateTimeOffset t) => DateTimeOffset.FromUnixTimeMilliseconds(t.ToUnixTimeMilliseconds());
}

internal static class StoreCommandExtensions
{
    public static StoreCommand<T> With<TSeed, T>(this StoreCommand<TSeed> seed, T body) => new(seed.Application, seed.CommandId, seed.NotAfter, body);
}
