using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Engine;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;

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

    /// <summary>Records due reads and offers a scripted schedule listener. Only the scheduler loop accesses the due rows.</summary>
    sealed class IdleScheduleStore(TimeProvider time) : IScheduleStore, IWakeNotificationSource
    {
        public ScriptedListener Listener { get; } = new();

        public ConcurrentQueue<DateTimeOffset> Reads { get; } = new();

        public List<ScheduleSnapshot> Due { get; } = [];

        public HashSet<Guid> Conflicts { get; } = [];

        public Task<DueSchedules> ReadDueSchedulesAsync(string application, int limit, CancellationToken ct, DueSchedulePageCursor? after = null)
        {
            var now = time.GetUtcNow();
            Reads.Enqueue(now);
            var page = Due.Where(s => s.ScanAfterAt <= now);
            if (after is { } cursor)
            {
                page = page.Where(s => s.ScanAfterAt <= cursor.ScanThrough && (s.ScanAfterAt > cursor.ScanAfterAt
                    || (s.ScanAfterAt == cursor.ScanAfterAt && s.ScheduleId.CompareTo(cursor.ScheduleId) > 0)));
            }

            return Task.FromResult(new DueSchedules(now, page.OrderBy(s => s.ScanAfterAt).ThenBy(s => s.ScheduleId).Take(limit).ToArray()));
        }

        public Task ListenAsync(string application, string channel, Action<string> onWake, Action onSubscribed, Action onLost, CancellationToken ct) =>
            channel == WakeChannels.Schedule ? Listener.RunAsync(onWake, onSubscribed, onLost, ct) : Task.CompletedTask;

        public Task<ScheduleSnapshot?> ReadScheduleAsync(string application, string key, CancellationToken ct) => throw new NotSupportedException();

        public Task<DateTimeOffset> StoreNowAsync(CancellationToken ct) => Task.FromResult(time.GetUtcNow());

        public Task<StoreReply<ScheduleRegistration>> RegisterScheduleAsync(StoreCommand<RegisterScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleMaterialization>> MaterializeOccurrencesAsync(StoreCommand<MaterializeRequest> command, CancellationToken ct)
        {
            var plan = command.Body;
            Assert.Single(plan.Occurrences);
            if (Conflicts.Contains(plan.ScheduleId))
            {
                return Task.FromResult(StoreReply<ScheduleMaterialization>.NoEffect(StoreStatus.VersionConflict,
                    new StoreError("revision_conflict"), time.GetUtcNow()));
            }

            Assert.Equal(1, Due.RemoveAll(s => s.ScheduleId == plan.ScheduleId));
            return Task.FromResult(StoreReply<ScheduleMaterialization>.Applied(new ScheduleMaterialization(plan.ScheduleId, 2, []), time.GetUtcNow()));
        }

        public Task<StoreReply<ScheduleState>> SetScheduleStateAsync(StoreCommand<ScheduleStateRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleRegistration>> UpdateScheduleAsync(StoreCommand<UpdateScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleMaterialization>> TriggerScheduleAsync(StoreCommand<TriggerScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleMaterialization>> ResolveCatchUpAsync(StoreCommand<ResolveCatchUpRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleState>> RecordEvaluationFailureAsync(StoreCommand<EvaluationFailureRequest> command, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoreReply<ScheduleState>> RevalidateScheduleAsync(StoreCommand<RevalidateScheduleRequest> command, CancellationToken ct) => throw new NotSupportedException();
    }

    static int ReadsBetween(IdleScheduleStore store, DateTimeOffset from, DateTimeOffset to) => store.Reads.Count(r => r >= from && r < to);

    static (EngineFixture Fixture, IdleScheduleStore Store, SchedulerService Scheduler) DueFixture(int count)
    {
        var e = EngineFixture.Create(
            configure: r => r.AddScheduler(),
            services: sc => sc.AddSingleton<IScheduleStore>(sp => new IdleScheduleStore(sp.GetRequiredService<TimeProvider>())));
        var store = (IdleScheduleStore)e.Services.GetRequiredService<IScheduleStore>();
        var now = e.Time.GetUtcNow();
        var declaration = new ScheduleDeclaration("once", new WorkJob("once")) { DueAt = now };
        var definition = ScheduleCompiler.Compile(declaration, e.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
        for (var i = 0; i < count; i++)
        {
            var id = new Guid(i, 0, 0, new byte[8]);
            store.Due.Add(new ScheduleSnapshot(id, $"once-{i}", 1, 1, 1, true, false, false, null,
                now, now, null, null, null, null, definition));
        }

        return (e, store, e.Services.GetRequiredService<SchedulerService>());
    }

    [Fact]
    [Trait("Case", "P-06s")]
    public async Task A_full_productive_due_page_is_followed_promptly_by_the_remaining_schedules()
    {
        var count = ScheduleBounds.DueReadLimit + 1;
        var (fixture, _, scheduler) = DueFixture(count);
        await using var e = fixture;
        await e.StartAsync();
        await e.RunUntilAsync(() => Interlocked.Read(ref scheduler.Materializations) >= ScheduleBounds.DueReadLimit,
            TimeSpan.FromSeconds(5), "the first full due page");
        // Allow time for the 33rd token to refill without advancing to the earliest possible jittered poll.
        await e.AdvanceAsync(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(10));

        Assert.Equal(count, Interlocked.Read(ref scheduler.Materializations));
        await e.StopAsync();
    }

    [Fact]
    [Trait("Case", "P-06s")]
    public async Task A_thousand_simultaneous_schedules_drain_within_the_default_allowance()
    {
        const int count = 1_000;
        var (fixture, _, scheduler) = DueFixture(count);
        await using var e = fixture;
        var start = e.Time.GetUtcNow();

        await e.StartAsync();
        await e.RunUntilAsync(() => Interlocked.Read(ref scheduler.Materializations) == count,
            TimeSpan.FromSeconds(13), "all one thousand due schedules", TimeSpan.FromMilliseconds(20));

        Assert.True(e.Time.GetUtcNow() - start >= TimeSpan.FromSeconds(9.68), "the default token bucket still limits the drain");
        await e.StopAsync();
    }

    [Fact]
    [Trait("Case", "P-06s")]
    public async Task An_unproductive_page_backs_off_and_then_reaches_later_schedules_at_the_same_instant()
    {
        var count = ScheduleBounds.DueReadLimit;
        var (fixture, store, scheduler) = DueFixture(count + 1);
        await using var e = fixture;
        store.Conflicts.UnionWith(store.Due.Take(count).Select(s => s.ScheduleId));

        await e.StartAsync();
        await e.RunUntilAsync(() => Interlocked.Read(ref scheduler.RevisionConflicts) >= count,
            TimeSpan.FromSeconds(5), "the full page conflicted");
        await e.AdvanceAsync(TimeSpan.FromMilliseconds(500));
        Assert.Single(store.Reads);
        Assert.Equal(0, Interlocked.Read(ref scheduler.Materializations));

        await e.RunUntilAsync(() => Interlocked.Read(ref scheduler.Materializations) == 1,
            TimeSpan.FromSeconds(3), "the later eligible schedule");
        Assert.Equal(2, store.Reads.Count);
        await e.RunUntilAsync(() => Interlocked.Read(ref scheduler.RevisionConflicts) > count,
            TimeSpan.FromSeconds(2), "the head of the next scan");
        await e.StopAsync();
    }

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
