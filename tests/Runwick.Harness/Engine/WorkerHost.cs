using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Harness.Timing;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Worker;

namespace Runwick.Harness.Engine;

/// <summary>A test job whose handler behaviour is chosen per job by <see cref="HandlerScript"/>.</summary>
public sealed record WorkJob(string Id) : IQueueJob;

/// <summary>Per-job handler behaviour and an invocation log shared by every attempt in one host (07 §2.8).</summary>
public sealed class HandlerScript
{
    readonly ConcurrentDictionary<string, Func<WorkJob, IJobContext, CancellationToken, Task>> _behaviours = new(StringComparer.Ordinal);

    public ConcurrentQueue<(string JobKey, Guid AttemptId, int Execution)> Invocations { get; } = new();

    public ConcurrentDictionary<string, int> Completed { get; } = new(StringComparer.Ordinal);

    /// <summary>Behaviour for jobs without an explicit entry; default: return at once.</summary>
    public Func<WorkJob, IJobContext, CancellationToken, Task> Default { get; set; } = (_, _, _) => Task.CompletedTask;

    public void On(string jobKey, Func<WorkJob, IJobContext, CancellationToken, Task> behaviour) => _behaviours[jobKey] = behaviour;

    public int InvocationsOf(string jobKey) => Invocations.Count(i => i.JobKey == jobKey);

    internal async Task RunAsync(WorkJob job, IJobContext context, CancellationToken ct)
    {
        Invocations.Enqueue((job.Id, context.AttemptId.Value, context.ExecutionNumber));
        await (_behaviours.TryGetValue(job.Id, out var b) ? b : Default)(job, context, ct).ConfigureAwait(false);
        Completed.AddOrUpdate(job.Id, 1, (_, n) => n + 1);
    }
}

internal sealed class ScriptedWorkHandler(HandlerScript script, IJobContext context, IServiceProvider services) : IJobHandler<WorkJob>
{
    public Task HandleAsync(WorkJob job, CancellationToken cancellationToken)
    {
        // Behaviours that need attempt-scoped services (e.g. IPostgresJobCompletion) read the scope from Items.
        context.Items[typeof(IServiceProvider)] = services;
        return script.RunAsync(job, context, cancellationToken);
    }
}

/// <summary>A second job type for mixed-type lanes; recorded in the same script under <c>ping:&lt;id&gt;</c>.</summary>
internal sealed class ScriptedPingHandler(HandlerScript script, IJobContext context) : IJobHandler<PingJob>
{
    public Task HandleAsync(PingJob job, CancellationToken cancellationToken) => script.RunAsync(new WorkJob("ping:" + job.Id), context, cancellationToken);
}

/// <summary>
/// The real worker in-process (07 §2.4 "local"): <c>AddRunwick(...).UsePostgres(...).AddWorker(...)</c> started through
/// the hosted service, with scaled test timing.
/// </summary>
internal sealed class WorkerHost : IAsyncDisposable
{
    readonly ServiceProvider _provider;
    readonly IHostedService _hosted;
    bool _started;
    bool _stopped;

    WorkerHost(ServiceProvider provider)
    {
        _provider = provider;
        _hosted = provider.GetServices<IHostedService>().Single();
    }

    public HandlerScript Script => _provider.GetRequiredService<HandlerScript>();

    public WorkerEngine Engine => _provider.GetRequiredService<WorkerEngine>();

    public IJobDispatcher Dispatcher => _provider.GetRequiredService<IJobDispatcher>();

    public IServiceProvider Services => _provider;

    public ConcurrentQueue<(string Name, Guid? AttemptId, TimeSpan At)> Events { get; } = new();

    /// <summary>
    /// Builds (not starts) a worker host. <paramref name="jobs"/> configures <see cref="WorkJob"/> (named
    /// <c>test.work</c> on queue <c>default</c>); <paramref name="options"/> runs after the scaled timing profile
    /// (<see cref="TestTiming.RealStoreDivisor"/> unless <paramref name="divisor"/> is given). <paramref name="worker"/>
    /// runs after pool <c>main</c> is declared, for further pools of the same group (CAP-01).
    /// </summary>
    public static WorkerHost Create(string connectionString, string application, IFaultInjector faults, int concurrency = 4,
        Action<JobBuilder<WorkJob>>? jobs = null, Action<WorkerOptions>? options = null, Action<RunwickBuilder>? configure = null,
        Action<IServiceCollection>? services = null, int? divisor = null, string queue = "default", Action<PoolBuilder>? pool = null,
        Action<WorkerBuilder>? worker = null, Action<IServiceCollection>? overrides = null)
    {
        var sc = new ServiceCollection();
        sc.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        sc.AddSingleton(faults);
        sc.AddSingleton<HandlerScript>();
        services?.Invoke(sc);
        sc.AddRunwick(r =>
        {
            r.Application(application);
            _ = TestClient.IsSqlite(connectionString) ? r.UseSqlite(connectionString) : r.UsePostgres(connectionString);
            RegisterWork(r, queue, jobs);
            r.AddHandler<WorkJob, ScriptedWorkHandler>();
            r.AddWorker(w =>
            {
                w.Pool("main", p =>
                {
                    // concurrency 0 leaves the pool's concurrency to the pool callback (an AutoConcurrency pool, 11 §2).
                    p.Queues(queue);
                    if (concurrency > 0)
                    {
                        p.FixedConcurrency(concurrency);
                    }

                    pool?.Invoke(p);
                }).Configure(o =>
                {
                    TestTiming.Scaled(o, divisor ?? TestTiming.RealStoreDivisor);

                    options?.Invoke(o);
                });
                worker?.Invoke(w);
            });
            configure?.Invoke(r);
        });
        overrides?.Invoke(sc);   // after AddRunwick: replaces store registrations (SQL-12 single-operation runs)
        var host = new WorkerHost(sc.BuildServiceProvider());
        host.Engine.Hooks.Event = (name, entry) => host.Events.Enqueue((name, entry?.AttemptId, host.Engine.Clock.Now));
        return host;
    }

    /// <summary>
    /// The contracts a <see cref="Create"/> host registers, without handler or worker: what a producer-only or
    /// control-only process (08 §9) registers to dispatch <see cref="WorkJob"/> onto <paramref name="queue"/>.
    /// </summary>
    public static void RegisterWork(RunwickBuilder r, string queue = "default", Action<JobBuilder<WorkJob>>? jobs = null)
    {
        TestJobs.Register(r);
        var work = r.AddJob<WorkJob>().Named("test.work").ToQueue(queue).WithMaxExecutions(3)
            .WithRetryDelay(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        jobs?.Invoke(work);
    }

    /// <summary>
    /// SQL-01 contention: a capacity-1 permit per resource (the job key up to its first '-') and a job-type rate of
    /// <see cref="ContendedRate"/> per second.
    /// </summary>
    public static void Contended(JobBuilder<WorkJob> jobs) =>
        jobs.PerResource(j => j.Id[..Math.Max(0, j.Id.IndexOf('-', StringComparison.Ordinal))], 1).RollingWindow(ContendedRate, TimeSpan.FromSeconds(1));

    public const int ContendedRate = 100;

    public async Task StartAsync()
    {
        await _hosted.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _started = true;
    }

    public async Task StopAsync(CancellationToken hostToken = default)
    {
        if (_started && !_stopped)
        {
            _stopped = true;
            await _hosted.StopAsync(hostToken).ConfigureAwait(false);
        }
    }

    public Task<DispatchReceipt> DispatchAsync(string key) => Dispatcher.DispatchAsync(new WorkJob(key));

    public int EventCount(string name) => Events.Count(e => e.Name == name);

    /// <summary>Polls until <paramref name="condition"/> holds; fails with <paramref name="what"/> after <paramref name="timeout"/>.</summary>
    public static async Task WaitUntil(Func<bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("timed out waiting for " + what);
            }

            await Task.Delay(20).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await StopAsync(cts.Token).ConfigureAwait(false);
        }
        finally
        {
            await _provider.DisposeAsync().ConfigureAwait(false);
        }
    }
}
