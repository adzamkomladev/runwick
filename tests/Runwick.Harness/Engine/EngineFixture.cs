using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Runwick.Dispatching;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Worker;

namespace Runwick.Harness.Engine;

/// <summary>
/// The real worker engine over <see cref="FakeTimeProvider"/> and a <see cref="ScriptedStore"/> (07 §2.8). Time only
/// moves when the test advances it, in small steps with a real yield between them so engine continuations run.
/// </summary>
internal sealed class EngineFixture : IAsyncDisposable
{
    readonly ServiceProvider _provider;
    readonly IHostedService _hosted;
    bool _started;
    bool _stopped;

    sealed class ScriptedRegistration(ScriptedStore store) : IStoreProviderRegistration
    {
        public string Name => "scripted";

        public void Register(IServiceCollection services) => services.AddSingleton<IRunwickStore>(store);
    }

    EngineFixture(ServiceProvider provider, FakeTimeProvider time, ScriptedStore store)
    {
        _provider = provider;
        Time = time;
        Store = store;
        _hosted = provider.GetServices<IHostedService>().Single();
        Engine.Hooks.Event = (name, entry) => Events.Enqueue((name, entry, Engine.Clock.Now));
    }

    public FakeTimeProvider Time { get; }

    public ScriptedStore Store { get; }

    public HandlerScript Script => _provider.GetRequiredService<HandlerScript>();

    public WorkerEngine Engine => _provider.GetRequiredService<WorkerEngine>();

    public IServiceProvider Services => _provider;

    public ConcurrentQueue<(string Name, AttemptEntry? Entry, TimeSpan At)> Events { get; } = new();

    /// <summary>Default step for <see cref="AdvanceAsync"/> and <see cref="RunUntilAsync"/>.</summary>
    public TimeSpan Step { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Builds (not starts) an engine with default timing unless <paramref name="options"/> changes it.</summary>
    public static EngineFixture Create(int concurrency = 4, Action<JobBuilder<WorkJob>>? jobs = null, Action<WorkerOptions>? options = null,
        Action<RunwickBuilder>? configure = null, Action<IServiceCollection>? services = null)
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 10, 30, 12, 0, 0, TimeSpan.Zero));
        var store = new ScriptedStore(time);
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton<TimeProvider>(time);
        sc.AddSingleton<HandlerScript>();
        services?.Invoke(sc);
        sc.AddRunwick(r =>
        {
            r.Application("engine");
            r.AddStoreProvider(new ScriptedRegistration(store));
            var work = r.AddJob<WorkJob>().Named("test.work").ToQueue("default").WithMaxExecutions(3);
            jobs?.Invoke(work);
            r.AddHandler<WorkJob, ScriptedWorkHandler>();
            r.AddWorker(w => w.Pool("main", p => p.Queues("default").FixedConcurrency(concurrency)).Configure(o => options?.Invoke(o)));
            configure?.Invoke(r);
        });
        return new EngineFixture(sc.BuildServiceProvider(), time, store);
    }

    public async Task StartAsync()
    {
        await _hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _started = true;
        await AdvanceAsync(TimeSpan.Zero).ConfigureAwait(false);
    }

    /// <summary>Starts <see cref="IHostedService.StopAsync"/> and keeps time moving until it returns.</summary>
    public async Task StopAsync(TimeSpan? maxFake = null, CancellationToken hostToken = default)
    {
        if (!_started || _stopped)
        {
            return;
        }

        _stopped = true;
        var stop = _hosted.StopAsync(hostToken);
        await RunUntilAsync(() => stop.IsCompleted, maxFake ?? TimeSpan.FromSeconds(60), "StopAsync").ConfigureAwait(false);
        await stop.ConfigureAwait(false);
    }

    /// <summary>Adds a job through the real preparer (payload bytes and policy as dispatch would store them).</summary>
    public Guid Add(string key, IQueueJob? job = null)
    {
        var preparer = _provider.GetRequiredService<DispatchPreparer>();
        return Store.Add(preparer.Prepare(Store.StoreId, job ?? new WorkJob(key), null).Intent);
    }

    public Guid JobIdOf(string key) =>
        Store.Jobs.Single(j => System.Text.Encoding.UTF8.GetString(j.Intent.PayloadUtf8).Contains($"\"{key}\"", StringComparison.Ordinal)).Id;

    public int EventCount(string name) => Events.Count(e => e.Name == name);

    /// <summary>Lets pending continuations run without moving time.</summary>
    public static async Task SettleAsync(int rounds = 5)
    {
        for (var i = 0; i < rounds; i++)
        {
            await Task.Delay(2).ConfigureAwait(false);
        }
    }

    public async Task AdvanceAsync(TimeSpan by, TimeSpan? step = null)
    {
        var s = step ?? Step;
        var end = Time.GetUtcNow() + by;
        await SettleAsync().ConfigureAwait(false);
        while (Time.GetUtcNow() < end)
        {
            var next = end - Time.GetUtcNow();
            Time.Advance(next < s ? next : s);
            await SettleAsync(2).ConfigureAwait(false);
        }

        await SettleAsync().ConfigureAwait(false);
    }

    /// <summary>Advances fake time until <paramref name="condition"/> holds; fails after <paramref name="maxFake"/> of fake time.</summary>
    public async Task RunUntilAsync(Func<bool> condition, TimeSpan maxFake, string what, TimeSpan? step = null)
    {
        var s = step ?? Step;
        var end = Time.GetUtcNow() + maxFake;
        await SettleAsync().ConfigureAwait(false);
        while (!condition())
        {
            if (Time.GetUtcNow() >= end)
            {
                throw new TimeoutException($"'{what}' did not happen within {maxFake} of fake time");
            }

            Time.Advance(s);
            await SettleAsync(2).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_started && !_stopped)
            {
                using var cts = new CancellationTokenSource();
                await cts.CancelAsync().ConfigureAwait(false);
                _stopped = true;
                var stop = _hosted.StopAsync(cts.Token);
                for (var i = 0; i < 200 && !stop.IsCompleted; i++)
                {
                    Time.Advance(TimeSpan.FromSeconds(1));
                    await Task.Delay(5).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            await _provider.DisposeAsync().ConfigureAwait(false);
        }
    }
}
