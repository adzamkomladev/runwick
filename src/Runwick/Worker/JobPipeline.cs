using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Serialization;

namespace Runwick.Worker;

/// <summary>The scoped <see cref="IJobContext"/> slot, initialized before any middleware or handler is resolved (05 §10.1 step 4).</summary>
internal sealed class JobContextHolder
{
    public IJobContext? Context { get; set; }

    /// <summary>The attempt and engine behind this scope; used by provider transactional completion (08 §8.1).</summary>
    public AttemptEntry? Entry { get; set; }

    public WorkerEngine? Engine { get; set; }
}

internal sealed class JobContext(AttemptEntry entry, string application, object payload, IReadOnlyList<ResourceFence> fences) : IJobContext
{
    Dictionary<object, object?>? _items;

    public string Application => application;

    public JobId JobId => new(entry.Lease.JobId);

    public AttemptId AttemptId => new(entry.AttemptId);

    public int Generation => entry.Lease.Generation;

    public int ExecutionNumber => entry.Begun?.ExecutionNumber ?? 0;

    public int MaxExecutions => entry.Reserved.Policy.MaxExecutions;

    public string JobName => entry.Reserved.Contract.Name;

    public string Queue => entry.Reserved.Queue;

    public string Priority => entry.Reserved.Priority;

    public int OriginalPayloadVersion => entry.Reserved.Contract.PayloadVersion;

    public int CurrentPayloadVersion => entry.Job!.CurrentVersion;

    public string SerializerProfileId => entry.Reserved.Contract.SerializerProfileId;

    public int SerializerProfileVersion => entry.Reserved.Contract.SerializerProfileVersion;

    public DateTimeOffset CreatedAt => entry.Reserved.CreatedAt;

    public IReadOnlyDictionary<string, string> Metadata => entry.Reserved.BusinessMetadata;

    public JobTraceContext Trace => new(entry.Reserved.DiagnosticMetadata.GetValueOrDefault("traceparent"), entry.Reserved.DiagnosticMetadata.GetValueOrDefault("tracestate"));

    public IReadOnlyList<ResourceFence> Fences => fences;

    public JobScheduleInfo? Schedule => entry.Reserved.Schedule;

    public JobCancellationCause CancellationCauses => entry.Causes;

    public IDictionary<object, object?> Items => LazyInitializer.EnsureInitialized(ref _items);

    public TJob GetPayload<TJob>() where TJob : class, IQueueJob =>
        payload as TJob ?? throw new InvalidOperationException($"The current payload type of '{JobName}' is {payload.GetType().Name}, not {typeof(TJob).Name}.");
}

/// <summary>What the pipeline produced, frozen once with its monotonic elapsed time (05 §10.1 step 6).</summary>
internal sealed record PipelineObservation(JobExecutionResult? Result, Exception? Exception, string? Misuse, PayloadValidationException? DecodeFailure, TimeSpan Elapsed);

internal sealed class PipelineContractException(string code) : InvalidOperationException($"Middleware pipeline contract violated: {code}.")
{
    public string Code { get; } = code;
}

/// <summary>
/// Cached per-job invocation (05 §3.2: reflection happens at registration only): decode + upgrade, the terminal
/// handler call and the guarded middleware chain (05 §10.2).
/// </summary>
internal sealed class JobPipeline
{
    readonly CompiledJob _job;
    readonly Func<object, object, CancellationToken, Task> _handle;

    JobPipeline(CompiledJob job, Func<object, object, CancellationToken, Task> handle)
    {
        _job = job;
        _handle = handle;
    }

    public static JobPipeline Create(CompiledJob job)
    {
        var method = typeof(JobPipeline).GetMethod(nameof(Invoke), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(job.PayloadType);
        return new JobPipeline(job, method.CreateDelegate<Func<object, object, CancellationToken, Task>>());
    }

    static Task Invoke<TJob>(object handler, object payload, CancellationToken ct) where TJob : class, IQueueJob =>
        ((IJobHandler<TJob>)handler).HandleAsync((TJob)payload, ct);

    /// <summary>Decodes the stored bytes with the registered profile and runs the pure upgrade chain.</summary>
    public object Decode(ReservedJob reserved)
    {
        if (!_job.Routes.TryGetValue(reserved.Contract.PayloadVersion, out var route))
        {
            throw new InvalidOperationException($"No decode route for {reserved.Contract.Name} v{reserved.Contract.PayloadVersion}.");
        }

        var dto = RunwickJsonProfile.V1.Deserialize(reserved.PayloadUtf8, route.DtoType);
        try
        {
            return route.Upgrade(dto);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new PayloadValidationException("payload_upgrade_failed", $"Upgrade of {reserved.Contract.Name} v{route.Version} failed: {ex.Message}");
        }
    }

    /// <summary>Runs global → per-job middleware → handler inside <paramref name="scope"/> with once-only guarded <c>next</c>.</summary>
    public async Task<(JobExecutionResult? Result, Exception? Error, string? Misuse)> RunAsync(
        IServiceProvider scope, IJobContext context, object payload, CancellationToken token)
    {
        using var pipelineCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        var state = new PipelineState(pipelineCts);
        try
        {
            var result = await FrameAsync(0, scope, context, payload, state).ConfigureAwait(false);
            if (result is null)
            {
                state.Fault("null_result");
            }

            return (state.Misuse is null ? result : null, null, state.Misuse);
        }
        catch (Exception ex)
        {
            return (null, ex, state.Misuse);
        }
    }

    async Task<JobExecutionResult> FrameAsync(int index, IServiceProvider scope, IJobContext context, object payload, PipelineState state)
    {
        var token = state.Cts.Token;
        if (index == _job.MiddlewareChain.Count)
        {
            var handler = scope.GetRequiredService(_job.HandlerType!);
            await _handle(handler, payload, token).ConfigureAwait(false);
            return JobExecutionResult.Succeeded();
        }

        var middleware = (IJobMiddleware)scope.GetRequiredService(_job.MiddlewareChain[index]);
        var guard = new NextGuard(() => FrameAsync(index + 1, scope, context, payload, state), state);
        JobExecutionResult result;
        try
        {
            result = await middleware.InvokeAsync(context, guard.Next, token).ConfigureAwait(false);
        }
        finally
        {
            await guard.ReturnedAsync().ConfigureAwait(false);
        }

        return result;
    }

    sealed class PipelineState(CancellationTokenSource cts)
    {
        public CancellationTokenSource Cts => cts;

        public string? Misuse { get; private set; }

        public void Fault(string code)
        {
            Misuse ??= code;
            try
            {
                cts.Cancel();
            }
            catch (AggregateException)
            {
                // Downstream callback faults are observed by the join.
            }
        }
    }

    /// <summary>One guard per middleware frame (05 §10.2 table).</summary>
    sealed class NextGuard(Func<Task<JobExecutionResult>> downstream, PipelineState state)
    {
        readonly Lock _lock = new();
        Task<JobExecutionResult>? _task;
        bool _returned;

        public Task<JobExecutionResult> Next()
        {
            lock (_lock)
            {
                if (_returned)
                {
                    state.Fault("next_after_return");
                    return Task.FromException<JobExecutionResult>(new PipelineContractException("next_after_return"));
                }

                if (_task is not null)
                {
                    state.Fault("next_called_twice");
                    return Task.FromException<JobExecutionResult>(new PipelineContractException("next_called_twice"));
                }

                _task = downstream();
                return _task;
            }
        }

        /// <summary>The middleware returned: a still-running downstream is misuse; cancel it and join before disposal.</summary>
        public async Task ReturnedAsync()
        {
            Task<JobExecutionResult>? running;
            lock (_lock)
            {
                _returned = true;
                running = _task;
            }

            if (running is { IsCompleted: false })
            {
                state.Fault("returned_while_next_running");
                try
                {
                    await running.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Joined only; the misuse decides the outcome.
                }
            }
        }
    }
}

/// <summary>Outcome classification on the frozen observation, in the 05 §10.3 order.</summary>
internal static class OutcomeClassifier
{
    const int MaxDetailBytes = 16 * 1024;

    public static FinishRequest Classify(LeaseHandle lease, PipelineObservation o, JobCancellationCause causes, TimeSpan? executionTimeout)
    {
        FinishRequest Finish(AttemptOutcome outcome, AttemptError? error = null, TimeSpan? delay = null) =>
            new(lease, outcome, o.Elapsed, delay, error);

        // Row 2: elapsed ≥ timeout, whatever the pipeline returned.
        if (executionTimeout is { } timeout && o.Elapsed >= timeout)
        {
            return Finish(AttemptOutcome.TimedOut, new AttemptError("execution_timeout", "", $"Execution exceeded {timeout}.", null));
        }

        // Row 3: normal return or explicit success.
        if (o.Misuse is null && o.DecodeFailure is null && o.Exception is null && o.Result is { Kind: JobExecutionResultKind.Succeeded })
        {
            return Finish(AttemptOutcome.Succeeded);
        }

        // Row 4: pipeline misuse; negative requested delay; a result kind this release cannot honor.
        if (o.Misuse is not null || o.Exception is PipelineContractException)
        {
            var code = o.Misuse ?? ((PipelineContractException)o.Exception!).Code;
            return Finish(AttemptOutcome.PermanentFailure, new AttemptError("pipeline_contract_fault", "", $"Middleware contract violated: {code}.", null));
        }

        if (o.Result is { Kind: JobExecutionResultKind.Retry, RetryDelay: { } d } && d < TimeSpan.Zero)
        {
            return Finish(AttemptOutcome.PermanentFailure, new AttemptError("pipeline_contract_fault", "", "A requested retry delay cannot be negative.", null));
        }

        if (o.Result is { Kind: JobExecutionResultKind.Completed })
        {
            // Reached only when nothing was staged for this attempt (a staged attempt is observed by 08 §8.2 instead).
            return Finish(AttemptOutcome.PermanentFailure,
                new AttemptError("pipeline_contract_fault", "", "The pipeline returned a staged completion this attempt never staged.", null));
        }

        // Row 5: explicit Failed; deterministic payload/upgrade failure; application-classified permanent failure.
        if (o.Result is { Kind: JobExecutionResultKind.Failed } failed)
        {
            return Finish(AttemptOutcome.PermanentFailure, new AttemptError(failed.ReasonCode!, "", failed.Message ?? "", null));
        }

        if (o.DecodeFailure is { } decode)
        {
            return Finish(AttemptOutcome.PermanentFailure, new AttemptError(decode.Code, decode.GetType().Name, Bound(decode.Message), null));
        }

        if (o.Exception is PermanentJobFailureException permanent)
        {
            return Finish(AttemptOutcome.PermanentFailure, Error(permanent, permanent.Code));
        }

        // Row 6–7: user cancellation.
        if (o.Result is { Kind: JobExecutionResultKind.Cancelled } cancelled)
        {
            return Finish(AttemptOutcome.UserCancelled, new AttemptError(cancelled.ReasonCode!, "", "Cancelled by the pipeline.", null));
        }

        if (o.Exception is OperationCanceledException && causes.HasFlag(JobCancellationCause.UserRequested))
        {
            return Finish(AttemptOutcome.UserCancelled, Error(o.Exception, "cancellation_requested"));
        }

        // Row 8: unsuccessful exit under shutdown.
        if (causes.HasFlag(JobCancellationCause.Shutdown))
        {
            return Finish(AttemptOutcome.Interrupted, o.Exception is null ? new AttemptError("shutdown", "", "Interrupted by shutdown.", null) : Error(o.Exception, "shutdown"));
        }

        // Row 9: explicit retry.
        if (o.Result is { Kind: JobExecutionResultKind.Retry } retry)
        {
            return Finish(AttemptOutcome.RetryableFailure, new AttemptError(retry.ReasonCode!, "", "Retry requested.", null), retry.RetryDelay);
        }

        // Row 10: any other exception (or a malformed result).
        return Finish(AttemptOutcome.RetryableFailure, o.Exception is null
            ? new AttemptError("pipeline_contract_fault", "", "The pipeline returned no usable result.", null)
            : Error(o.Exception, "handler_exception"));
    }

    static AttemptError Error(Exception ex, string code) =>
        new(code, ex.GetType().FullName ?? ex.GetType().Name, Bound(ex.Message), Bound(ex.ToString()));

    /// <summary>Bounded detail: at most 16 KiB UTF-8 with an explicit truncation marker.</summary>
    static string Bound(string text)
    {
        if (System.Text.Encoding.UTF8.GetByteCount(text) <= MaxDetailBytes)
        {
            return text;
        }

        const string marker = "…[truncated]";
        var take = Math.Min(text.Length, MaxDetailBytes / 3);
        return text[..take] + marker;
    }
}
