using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runwick.Hosting;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Registration;

namespace Runwick.Harness.Store;

/// <summary>One recorded store call (07 §2.2).</summary>
internal sealed record HistoryEntry(
    Guid CommandId, string Operation, string Status, string Effect, bool Replayed, DateTimeOffset? DecidedAt,
    long SentTicks, long ReceivedTicks, string? Code, Guid? JobId, Guid? AttemptId);

/// <summary>An in-process client: real registry, dispatcher, PostgreSQL store and SQL; no mocked client path.</summary>
internal sealed class TestClient(ServiceProvider provider, string application) : IAsyncDisposable
{
    /// <summary>Builds a client over <c>AddRunwick(...).UsePostgres(...)</c> with the given fault injector.</summary>
    internal static async Task<TestClient> CreateAsync(string connectionString, string application, IFaultInjector faults,
        Action<RunwickBuilder>? configure = null, TimeProvider? time = null, bool ensureReady = true)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSingleton(faults);
        if (time is not null)
        {
            services.AddSingleton(time);
        }

        services.AddRunwick(r =>
        {
            r.Application(application);
            if (RedisServer.IsRedis(connectionString))
            {
                r.UseRedis(RedisServer.ConfigurationOf(connectionString));
            }
            else if (IsSqlite(connectionString))
            {
                r.UseSqlite(connectionString);
            }
            else
            {
                r.UsePostgres(connectionString);
            }

            configure?.Invoke(r);
        });
        var client = new TestClient(services.BuildServiceProvider(), application);
        if (ensureReady)
        {
            await client.Runtime.EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);
        }

        return client;
    }

    /// <summary>A SQLite store is named by its file (20 §12: the harness store kind follows the connection string).</summary>
    internal static bool IsSqlite(string connectionString) => connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase);

    public ServiceProvider Services => provider;

    public string Application => application;

    public Guid WorkerInstanceId { get; } = Guid.CreateVersion7();

    public IJobDispatcher Dispatcher => provider.GetRequiredService<IJobDispatcher>();

    public IPreparedJobDispatcher Prepared => provider.GetRequiredService<IPreparedJobDispatcher>();

    internal PgStore Store => provider.GetRequiredService<PgStore>();

    /// <summary>The protocol store of either kind (PostgreSQL or SQLite).</summary>
    internal IRunwickStore Driver => provider.GetRequiredService<IRunwickStore>();

    internal JobRegistry Registry => provider.GetRequiredService<JobRegistry>();

    internal RunwickRuntime Runtime => provider.GetRequiredService<RunwickRuntime>();

    public ConcurrentQueue<HistoryEntry> History { get; } = new();

    public TimeSpan CommandLifetime { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<JobContract> Contracts =>
        [.. Registry.Compiled.SelectMany(j => j.Routes.Keys.Select(v => new JobContract(j.Name, v, Serialization.RunwickJsonProfile.ProfileId, Serialization.RunwickJsonProfile.ProfileVersion)))];

    internal StoreCommand<T> Command<T>(T body, Guid? commandId = null, DateTimeOffset? notAfter = null) =>
        new(application, commandId ?? Guid.CreateVersion7(), notAfter ?? DateTimeOffset.UtcNow + CommandLifetime, body);

    internal async Task<StoreReply<T>> Record<T>(StoreCommand<object> meta, string operation, Func<Task<StoreReply<T>>> call, Func<T?, (Guid?, Guid?)>? ids = null)
        where T : class
    {
        var sent = System.Diagnostics.Stopwatch.GetTimestamp();
        var reply = await call().ConfigureAwait(false);
        var (job, attempt) = ids?.Invoke(reply.Value) ?? (null, null);
        History.Enqueue(new HistoryEntry(meta.CommandId, operation, reply.Status.ToString(), reply.Effect.ToString(), reply.Replayed,
            reply.DecidedAt, sent, System.Diagnostics.Stopwatch.GetTimestamp(), reply.Error?.Code, job, attempt));
        return reply;
    }

    internal Task<StoreReply<ReservedJob>> ReserveAsync(string queue = "default", string priority = "medium", TimeSpan? lease = null,
        CandidateCursor? after = null, Guid? commandId = null, IReadOnlyList<JobContract>? contracts = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new ReserveRequest(WorkerInstanceId, queue, priority, contracts ?? Contracts, after, lease ?? TimeSpan.FromSeconds(30)), commandId, notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.Reserve,
            () => Driver.TryReserveAsync(cmd, CancellationToken.None), v => (v?.Position.JobId, v?.Lease.Handle.AttemptId));
    }

    internal async Task<BeginReply> BeginAsync(LeaseHandle lease, Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new BeginRequest(lease), commandId, notAfter);
        BeginReply? full = null;
        await Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.Begin, async () =>
        {
            full = await Driver.BeginExecutionAsync(cmd, CancellationToken.None).ConfigureAwait(false);
            return full.Reply;
        }, _ => (lease.JobId, lease.AttemptId)).ConfigureAwait(false);
        return full!;
    }

    internal Task<StoreReply<RenewedLease>> RenewAsync(LeaseHandle lease, TimeSpan? duration = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new RenewRequest(lease, duration ?? TimeSpan.FromSeconds(30)), notAfter: notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.Renew,
            () => Driver.RenewAsync(cmd, CancellationToken.None), _ => (lease.JobId, lease.AttemptId));
    }

    /// <summary>RenewMany/1 (PF-06) over the given leases; one history entry for the call.</summary>
    internal Task<StoreReply<RenewedLeases>> RenewManyAsync(IReadOnlyList<LeaseHandle> leases, TimeSpan? duration = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new RenewManyRequest(leases, duration ?? TimeSpan.FromSeconds(30)), notAfter: notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.RenewMany,
            () => ((ILeaseBatchStore)Driver).RenewManyAsync(cmd, CancellationToken.None));
    }

    /// <summary>ReserveAndBegin/1 (PF-06 batched claim); one history entry for the call.</summary>
    internal Task<StoreReply<ClaimedJobs>> ReserveAndBeginAsync(int maxJobs, string queue = "default", string priority = "medium",
        TimeSpan? lease = null, CandidateCursor? after = null, Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new ReserveAndBeginRequest(WorkerInstanceId, queue, priority, Contracts, after, lease ?? TimeSpan.FromSeconds(30), maxJobs),
            commandId, notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.ReserveAndBegin,
            () => ((IReserveAndBeginStore)Driver).ReserveAndBeginAsync(cmd, CancellationToken.None));
    }

    internal Task<StoreReply<FinishedAttempt>> FinishAsync(LeaseHandle lease, AttemptOutcome outcome, TimeSpan? elapsed = null,
        TimeSpan? requestedDelay = null, AttemptError? error = null, Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new FinishRequest(lease, outcome, elapsed ?? TimeSpan.FromMilliseconds(5), requestedDelay, error), commandId, notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.Finish,
            () => Driver.FinishAsync(cmd, CancellationToken.None), _ => (lease.JobId, lease.AttemptId));
    }

    /// <summary>One Finish/1 command as a batch item carries it (its own ID and NotAfter); not sent, not recorded.</summary>
    internal StoreCommand<FinishRequest> FinishCommand(LeaseHandle lease, AttemptOutcome outcome, TimeSpan? elapsed = null,
        TimeSpan? requestedDelay = null, AttemptError? error = null, Guid? commandId = null, DateTimeOffset? notAfter = null) =>
        Command(new FinishRequest(lease, outcome, elapsed ?? TimeSpan.FromMilliseconds(5), requestedDelay, error), commandId, notAfter);

    /// <summary>Finish/1 for a prepared command (the same ID and body a batch item carried): a per-item resolution.</summary>
    internal Task<StoreReply<FinishedAttempt>> FinishAsync(StoreCommand<FinishRequest> cmd) =>
        Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.Finish,
            () => Driver.FinishAsync(cmd, CancellationToken.None), _ => (cmd.Body.Lease.JobId, cmd.Body.Lease.AttemptId));

    /// <summary>FinishMany/1 (PF-06 batched finish) over prepared Finish commands; one history entry for the call.</summary>
    internal Task<StoreReply<FinishedMany>> FinishManyAsync(IReadOnlyList<StoreCommand<FinishRequest>> items, Guid? commandId = null,
        DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new FinishManyRequest(items), commandId, notAfter ?? (items.Count == 0 ? null : items.Max(i => i.NotAfter)));
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.FinishMany,
            () => ((IFinishManyStore)Driver).FinishManyAsync(cmd, CancellationToken.None));
    }

    internal Task<StoreReply<FinishedAttempt>> RecoverAsync(ExpiredLease expired, Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new RecoverRequest(expired.JobId, expired.AttemptId, expired.OwnershipToken), commandId, notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.Recover,
            () => Driver.RecoverAsync(cmd, CancellationToken.None), _ => (expired.JobId, expired.AttemptId));
    }

    internal Task<StoreReply<ReleasedReservation>> ReleaseAsync(LeaseHandle lease, Guid? commandId = null, DateTimeOffset? notAfter = null)
    {
        var cmd = Command(new ReleaseRequest(lease), commandId, notAfter);
        return Record(new StoreCommand<object>(application, cmd.CommandId, cmd.NotAfter, cmd.Body), Operations.ReleaseReservation,
            () => Driver.ReleaseReservationAsync(cmd, CancellationToken.None), _ => (lease.JobId, lease.AttemptId));
    }

    public async ValueTask DisposeAsync() => await provider.DisposeAsync().ConfigureAwait(false);
}
