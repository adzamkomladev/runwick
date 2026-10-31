using Runwick.Hosting;
using Runwick.Protocol;

namespace Runwick.Management;

/// <summary>Outcome of one schedule control (13 §6). <see cref="JobId"/> is set for a manual trigger. Preview.</summary>
public sealed record ScheduleControlResult(
    string Key, bool Succeeded, string? Code, bool? Paused, bool? NeedsReview, long? DefinitionRevision, long? ControlRevision,
    JobId? JobId, bool Replayed);

/// <summary>Pause, resume, trigger and revalidate schedules (13 §6.3, §6.5, §6.8). Preview; HTTP is SP-020.</summary>
public interface IScheduleControl
{
    Task<ScheduleControlResult> PauseAsync(string key, long expectedControlRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<ScheduleControlResult> ResumeAsync(string key, long expectedControlRevision, ResumePolicy policy, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates one manual occurrence. <paramref name="triggerId"/> is the idempotency key: the same ID returns the same job.
    /// Works while the schedule is paused; never moves its cursor.
    /// </summary>
    Task<ScheduleControlResult> TriggerAsync(string key, long expectedDefinitionRevision, Guid triggerId, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);

    Task<ScheduleControlResult> RevalidateAsync(string key, long expectedDefinitionRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// <see cref="IScheduleControl"/> over <see cref="IScheduleStore"/>. Scheduler-family lifetime (30 s, 13 §9); an unknown
/// outcome resends the same command until it resolves or its NotAfter passes.
/// </summary>
internal sealed class ScheduleControlService(RunwickRuntime runtime, IScheduleStore? store, TimeProvider time) : IScheduleControl
{
    IScheduleStore Store => store ?? throw new InvalidOperationException("The configured store does not support schedules.");

    public Task<ScheduleControlResult> PauseAsync(string key, long expectedControlRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default) =>
        StateAsync(new ScheduleStateRequest(key, true, null, expectedControlRevision, Actor(actor, reason)), cancellationToken);

    public Task<ScheduleControlResult> ResumeAsync(string key, long expectedControlRevision, ResumePolicy policy, ManagementActor actor,
        string? reason = null, CancellationToken cancellationToken = default)
    {
        var wire = policy switch
        {
            ResumePolicy.RunOnce => ScheduleOperations.MisfireRunOnce,
            ResumePolicy.CatchUp => ScheduleOperations.MisfireCatchUp,
            _ => ScheduleOperations.ResumeSkipPaused,
        };
        return StateAsync(new ScheduleStateRequest(key, false, wire, expectedControlRevision, Actor(actor, reason)), cancellationToken);
    }

    public async Task<ScheduleControlResult> TriggerAsync(string key, long expectedDefinitionRevision, Guid triggerId, ManagementActor actor,
        string? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        var reply = await SendAsync(new TriggerScheduleRequest(key, expectedDefinitionRevision, triggerId, Guid.CreateVersion7(), Actor(actor, reason)),
            Store.TriggerScheduleAsync, cancellationToken).ConfigureAwait(false);
        return reply.Value is { } v
            ? new ScheduleControlResult(key, true, null, null, null, expectedDefinitionRevision, null,
                v.Occurrences[0].JobId is { } job ? new JobId(job) : null, reply.Replayed)
            : Failed(key, reply);
    }

    public Task<ScheduleControlResult> RevalidateAsync(string key, long expectedDefinitionRevision, ManagementActor actor, string? reason = null,
        CancellationToken cancellationToken = default) =>
        StateAsync(new RevalidateScheduleRequest(key, expectedDefinitionRevision, Actor(actor, reason)), Store.RevalidateScheduleAsync, key, cancellationToken);

    Task<ScheduleControlResult> StateAsync(ScheduleStateRequest request, CancellationToken ct) =>
        StateAsync(request, Store.SetScheduleStateAsync, request.Key, ct);

    async Task<ScheduleControlResult> StateAsync<TRequest>(TRequest request, Func<StoreCommand<TRequest>, CancellationToken, Task<StoreReply<ScheduleState>>> send,
        string key, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        var reply = await SendAsync(request, send, ct).ConfigureAwait(false);
        return reply.Value is { } v
            ? new ScheduleControlResult(key, true, null, v.Paused, v.NeedsReview, v.DefinitionRevision, v.ControlRevision, null, reply.Replayed)
            : Failed(key, reply);
    }

    static ScheduleControlResult Failed<T>(string key, StoreReply<T> reply) where T : class =>
        new(key, false, reply.Error?.Code, null, null, null, null, null, false);

    async Task<StoreReply<TResult>> SendAsync<TRequest, TResult>(TRequest body, Func<StoreCommand<TRequest>, CancellationToken, Task<StoreReply<TResult>>> send,
        CancellationToken ct) where TResult : class
    {
        await runtime.EnsureReadyAsync(ct).ConfigureAwait(false);
        var notAfter = time.GetUtcNow() + ScheduleOperations.SchedulerLifetime;
        var command = new StoreCommand<TRequest>(runtime.Registry.Application, Guid.CreateVersion7(),
            DateTimeOffset.FromUnixTimeMilliseconds(notAfter.ToUnixTimeMilliseconds()), body);
        var delay = TimeSpan.FromMilliseconds(100);
        while (true)
        {
            try
            {
                return await send(command, ct).ConfigureAwait(false);
            }
            catch (StoreOutcomeUnknownException ex) when (time.GetUtcNow() >= command.NotAfter)
            {
                throw new ControlOutcomeUnknownException(ex.CommandId, ex);
            }
            catch (StoreOutcomeUnknownException)
            {
                // Resend the same command: a committed effect replays from its receipt.
            }

            await Task.Delay(delay, time, ct).ConfigureAwait(false);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(1).Ticks));
        }
    }

    static ControlActor Actor(ManagementActor actor, string? reason)
    {
        ArgumentNullException.ThrowIfNull(actor);
        return new ControlActor(actor.Subject, actor.Display, actor.Scheme, reason);
    }
}
