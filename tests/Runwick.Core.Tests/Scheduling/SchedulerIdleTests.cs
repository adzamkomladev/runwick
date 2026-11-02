using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Protocol;
using Runwick.Scheduling;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// The scheduler's idle back-off (13 Changes 2026-11-02, idle caps) over fake time: 5 s without a healthy schedule
/// listener, 15 s with one, back to 5 s the moment the session is lost, and never past a due instant it knows of.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "PF-07")]
public sealed class SchedulerIdleTests
{
    static readonly TimeSpan Long = TimeSpan.FromMinutes(5);

    /// <summary>A schedule store with nothing due; it records each due read and offers a scripted schedule listener.</summary>
    sealed class IdleScheduleStore(TimeProvider time) : IScheduleStore, IWakeNotificationSource
    {
        public ScriptedListener Listener { get; } = new();

        public ConcurrentQueue<DateTimeOffset> Reads { get; } = new();

        public Task<DueSchedules> ReadDueSchedulesAsync(string application, int limit, CancellationToken ct)
        {
            Reads.Enqueue(time.GetUtcNow());
            return Task.FromResult(new DueSchedules(time.GetUtcNow(), []));
        }

        public Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct) =>
            channel == WakeChannels.Schedule ? Listener.RunAsync(onWake, onSubscribed, onLost, ct) : Task.CompletedTask;

        public Task<ScheduleSnapshot?> ReadScheduleAsync(string application, string key, CancellationToken ct) => throw new NotSupportedException();

        public Task<DateTimeOffset> StoreNowAsync(CancellationToken ct) => Task.FromResult(time.GetUtcNow());

        public Task<StoreReply<ScheduleRegistration>> RegisterScheduleAsync(StoreCommand<RegisterScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleMaterialization>> MaterializeOccurrencesAsync(StoreCommand<MaterializeRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleState>> SetScheduleStateAsync(StoreCommand<ScheduleStateRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleRegistration>> UpdateScheduleAsync(StoreCommand<UpdateScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleMaterialization>> TriggerScheduleAsync(StoreCommand<TriggerScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleMaterialization>> ResolveCatchUpAsync(StoreCommand<ResolveCatchUpRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleState>> RecordEvaluationFailureAsync(StoreCommand<EvaluationFailureRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleState>> RevalidateScheduleAsync(StoreCommand<RevalidateScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();
    }

    static int ReadsBetween(IdleScheduleStore store, DateTimeOffset from, DateTimeOffset to) => store.Reads.Count(r => r >= from && r < to);

    [Fact]
    [Trait("Case", "WAKE-03")]
    public async Task A_healthy_schedule_listener_lengthens_the_idle_back_off_and_a_lost_one_restores_it_at_once()
    {
        IdleScheduleStore? store = null;
        await using var e = EngineFixture.Create(
            configure: r => r.AddScheduler(),
            services: sc => sc.AddSingleton<IScheduleStore>(sp => store = new IdleScheduleStore(sp.GetRequiredService<TimeProvider>())));
        e.Step = TimeSpan.FromMilliseconds(20);
        var scheduler = e.Services.GetRequiredService<SchedulerService>();
        await e.StartAsync();
        Assert.NotNull(store);
        await e.RunUntilAsync(() => store.Listener.Running.IsCompleted, Long, "the schedule listener started");

        // Not subscribed: 5 s ± 10 %, so 60 s after the ramp is 11–13 reads.
        Assert.False(scheduler.ListenerHealthy);
        Assert.Equal(TimeSpan.FromSeconds(5), scheduler.IdleCap);
        await e.AdvanceAsync(TimeSpan.FromSeconds(20));
        var from = e.Time.GetUtcNow();
        await e.AdvanceAsync(TimeSpan.FromSeconds(60));
        Assert.InRange(ReadsBetween(store, from, e.Time.GetUtcNow()), 11, 13);

        // Healthy: 15 s ± 10 %, so 60 s after the ramp is 3–5 reads.
        store.Listener.Subscribe();
        Assert.True(scheduler.ListenerHealthy);
        Assert.Equal(TimeSpan.FromSeconds(15), scheduler.IdleCap);
        await e.AdvanceAsync(TimeSpan.FromSeconds(40));
        from = e.Time.GetUtcNow();
        await e.AdvanceAsync(TimeSpan.FromSeconds(60));
        Assert.InRange(ReadsBetween(store, from, e.Time.GetUtcNow()), 3, 5);

        // A known due instant bounds the long wait: it is read within one step of becoming due.
        var dueAt = e.Time.GetUtcNow() + TimeSpan.FromSeconds(20);
        scheduler.NoteNextScan(Guid.NewGuid(), dueAt, e.Time.GetUtcNow());
        await e.AdvanceAsync(TimeSpan.FromSeconds(21));
        Assert.Contains(store.Reads, r => r >= dueAt && r <= dueAt + TimeSpan.FromMilliseconds(100));
        Assert.Null(scheduler.UntilKnownDue());
        await e.AdvanceAsync(TimeSpan.FromSeconds(40));

        // Lost: the current wait ends at once (a read within a step), then the back-off returns to 5 s.
        from = e.Time.GetUtcNow();
        store.Listener.Lose();
        Assert.False(scheduler.ListenerHealthy);
        Assert.Equal(1, Interlocked.Read(ref scheduler.ListenerLosses));
        Assert.Equal(TimeSpan.FromSeconds(5), scheduler.IdleCap);
        await e.AdvanceAsync(TimeSpan.FromMilliseconds(100));
        Assert.True(ReadsBetween(store, from, e.Time.GetUtcNow()) >= 1, "a lost session ends the idle wait");
        await e.AdvanceAsync(TimeSpan.FromSeconds(20));
        from = e.Time.GetUtcNow();
        await e.AdvanceAsync(TimeSpan.FromSeconds(60));
        Assert.InRange(ReadsBetween(store, from, e.Time.GetUtcNow()), 11, 13);

        // Resubscribed: the long cap again.
        store.Listener.Subscribe();
        Assert.Equal(TimeSpan.FromSeconds(15), scheduler.IdleCap);
        await e.StopAsync();
    }
}
