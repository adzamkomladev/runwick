using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Runwick.Protocol;
using Runwick.Serialization;
using Runwick.Telemetry;
using T = Runwick.Telemetry.Telemetry;

namespace Runwick.Worker;

/// <summary>
/// The per-attempt lifecycle (05 §5.3, §9, §10, §16): Begin pre-checks, Begin resolution, invocation admission,
/// the runner on the thread pool, timeout/grace/cleanup supervision, Finish publication, drain release, retirement.
/// </summary>
internal sealed partial class WorkerEngine
{
    /// <summary>An Applied claim: the ticket moves once into a new entry; the lease is registered at once.</summary>
    /// <remarks>With <paramref name="begun"/> (ReserveAndBegin/1), the claim's Begin is already decided and the
    /// lifecycle settles that reply instead of sending Begin (05 §9.6).</remarks>
    internal void Admit(PoolCoordinator pool, CapacityTicket ticket, ReservedJob reserved, TimeSpan? deadline, TimeSpan sentAt,
        BeginReply? begun = null)
    {
        var job = Pipelines.ContainsKey(reserved.Contract.Name) ? Registry.ByName(reserved.Contract.Name) : null;
        var entry = new AttemptEntry(pool, ticket, reserved, job, deadline) { AdmittedAt = sentAt, PreBegun = begun is null ? null : (begun, sentAt) };
        Track(entry);
        if (deadline is null)
        {
            Lease.ScheduleNow(entry);
        }
        else
        {
            Lease.Schedule(entry, sentAt);
        }

        Emit("reserved", entry);
        _ = DriveAsync(entry);
    }

    /// <summary>Latches loss once, requests cancellation (LeaseLost) off the caller's thread and stops renewal.</summary>
    internal void LoseAuthority(AttemptEntry entry, LossCause cause)
    {
        if (entry.Latch(cause))
        {
            entry.AddCause(JobCancellationCause.LeaseLost, Clock.Now);
            _ = entry.CancelExecutionAsync();
            Lease.Unregister(entry);
            entry.Changed.Set();
            Emit("lost:" + cause, entry);
            entry.Span?.AddEvent(new ActivityEvent("runwick.authority_lost", tags: new ActivityTagsCollection { ["runwick.loss.reason"] = LossName(cause) }));
            RunwickLog.AuthorityLost(Logger, entry.Lease.JobId, entry.AttemptId, LossName(cause), entry.PipelineLive);
        }
    }

    /// <summary>A Renew reported a durable cancellation request (05 §11.3).</summary>
    internal void ObserveDurableCancellation(AttemptEntry entry)
    {
        entry.DurableCancellationObserved = true;
        if (entry.Phase is AttemptPhase.Running && entry.AddCause(JobCancellationCause.UserRequested, Clock.Now) | true)
        {
            _ = entry.CancelExecutionAsync();
        }

        entry.Changed.Set();
    }

    async Task DriveAsync(AttemptEntry entry)
    {
        await Task.Yield();
        StartProcessSpan(entry);
        try
        {
            if (entry.Job is null)
            {
                // The store returned a tuple this registry never advertised: configuration invariant fault (05 §10.1).
                CloseAdmission($"reserved unadvertised contract {entry.Reserved.Contract}");
                LoseAuthority(entry, LossCause.InvariantFault);
                return;
            }

            await LifecycleAsync(entry).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ControlStop.IsCancellationRequested)
        {
            entry.Latch(LossCause.ShutdownCutoff);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Runwick attempt {Entry} failed unexpectedly.", entry);
            LoseAuthority(entry, LossCause.InvariantFault);
        }
        finally
        {
            await RetireAsync(entry).ConfigureAwait(false);
            EndProcessSpan(entry);
        }
    }

    /// <summary>Retirement: no live user task, scope disposed or never created, commands resolved (05 §5.3).</summary>
    async Task RetireAsync(AttemptEntry entry)
    {
        if (entry.RunnerTask is { } runner)
        {
            await runner.ConfigureAwait(false);
        }

        Lease.Unregister(entry);
        if (entry.Retire())
        {
            Emit("retired", entry);

            // Healthy completed ticket durations feed the service estimate; loss or a pressure stop would teach an
            // inflated cost (capacity-defaults §4).
            if (entry.Begun is not null && entry.Loss is null && !PressureStopped)
            {
                Service.Completed(entry.Reserved.Contract.Name, Clock.Now - entry.AdmittedAt);
            }
        }

        Untrack(entry);
        entry.Dispose();
    }

    async Task LifecycleAsync(AttemptEntry entry)
    {
        bool? begun;
        if (entry.PreBegun is { } pre)
        {
            // ReserveAndBegin/1 (05 §9.6): storage already began the attempt in the claim's command, so there is no
            // reservation to release and no Begin to send. The reply is settled exactly as a Begin reply; the margin
            // and drain checks of invocation admission below still apply.
            entry.PreBegun = null;
            if (!entry.TryMove(AttemptPhase.Beginning))
            {
                return;
            }

            using (var span = T.Span("runwick.begin"))
            {
                begun = HandleBegin(entry, pre.Reply, pre.SentAt);
                span.Activity?.SetTag("runwick.result", entry.BeginResult);
            }
        }
        else
        {
            if (Draining)
            {
                await ReleaseAsync(entry).ConfigureAwait(false);
                return;
            }

            // §9.3: confirm authority (replayed claim) and margin before Begin.
            if (!await EnsureMarginAsync(entry).ConfigureAwait(false))
            {
                return;
            }

            if (Draining)
            {
                await ReleaseAsync(entry).ConfigureAwait(false);
                return;
            }

            if (!entry.TryMove(AttemptPhase.Beginning))
            {
                return;
            }

            using (var span = T.Span("runwick.begin"))
            {
                begun = await BeginAsync(entry).ConfigureAwait(false);
                span.Activity?.SetTag("runwick.result", entry.BeginResult ?? "stale");
            }
        }

        if (begun is null)
        {
            return;
        }

        // A replayed Begin never authorizes invocation alone: fresh ownership-checked Renew first (§9.4).
        if (begun.Value && !await EnsureMarginAsync(entry, forceFresh: true).ConfigureAwait(false))
        {
            return;
        }

        // §9.5 invocation admission.
        while (true)
        {
            if (entry.IsLost)
            {
                return;
            }

            if (entry.DurableCancellationObserved)
            {
                await Completion.SettleAsync(entry, new FinishRequest(entry.Lease, AttemptOutcome.UserCancelled, TimeSpan.Zero, null,
                    new AttemptError("cancellation_requested", "", "Cancellation was requested before invocation.", null))).ConfigureAwait(false);
                return;
            }

            if (PastDrainCutoff)
            {
                await Completion.SettleAsync(entry, new FinishRequest(entry.Lease, AttemptOutcome.Interrupted, TimeSpan.Zero, null,
                    new AttemptError("shutdown", "", "Begin resolved after the drain cutoff; not invoked.", null))).ConfigureAwait(false);
                return;
            }

            if (entry.Deadline is { } d && d - Clock.Now >= Options.MinimumInvocationMargin)
            {
                break;
            }

            if (!await EnsureMarginAsync(entry).ConfigureAwait(false))
            {
                return;
            }
        }

        if (!entry.TryLatchInvocation())
        {
            return;
        }

        entry.ExecutionStartedAt = Clock.Now;
        if (!entry.TryMove(AttemptPhase.Running))
        {
            return;
        }

        var observation = await ExecuteAsync(entry).ConfigureAwait(false);
        if (observation is null || entry.IsLost)
        {
            return;
        }

        var timeout = entry.Reserved.Policy.ExecutionTimeoutMs is { } ms ? TimeSpan.FromMilliseconds(ms) : (TimeSpan?)null;
        if (entry.Staged is { } staged)
        {
            await ObserveStagedAsync(entry, observation, staged, timeout).ConfigureAwait(false);
            return;
        }

        var finish = OutcomeClassifier.Classify(entry.Lease, observation, entry.Causes, timeout);
        RecordExecution(entry, observation, finish);
        if (!entry.IsLost)
        {
            await Completion.SettleAsync(entry, finish).ConfigureAwait(false);
        }
    }

    /// <summary>Renews until d − now ≥ MinimumInvocationMargin (or a fresh confirmation when forced); false when authority is gone.</summary>
    async Task<bool> EnsureMarginAsync(AttemptEntry entry, bool forceFresh = false)
    {
        var started = Clock.Now;
        var fresh = !forceFresh;
        for (var attempt = 0; ; attempt++)
        {
            if (entry.IsLost)
            {
                return false;
            }

            if (fresh && !entry.NeedsFreshRenew && entry.Deadline is { } d && d - Clock.Now >= Options.MinimumInvocationMargin)
            {
                return true;
            }

            if (entry.Deadline is null && Clock.Now - started >= Options.ClaimResolutionBudget)
            {
                // A replayed claim whose authority never got confirmed: do not Begin; durable expiry recovers it.
                LoseAuthority(entry, LossCause.SafetyDeadline);
                return false;
            }

            switch (await Lease.RenewOnceAsync(entry).ConfigureAwait(false))
            {
                case LeaseCoordinator.RenewResult.Fresh:
                    fresh = true;
                    continue;
                case LeaseCoordinator.RenewResult.Lost:
                    LoseAuthority(entry, LossCause.SafetyDeadline);
                    return false;
            }

            await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Begin through the arbiter (05 §9.4). Returns false for a fresh Applied, true for a replayed Applied, null when the
    /// entry closed, lost authority or stayed unresolved (no invocation ever).
    /// </summary>
    async Task<bool?> BeginAsync(AttemptEntry entry)
    {
        var command = Command(new BeginRequest(entry.Lease));
        var first = Clock.Now;
        var suspended = false;
        try
        {
            for (var attempt = 0; ; attempt++)
            {
                var now = Clock.Now;
                if (attempt > 0 && now - first >= Options.BeginResolutionBudget)
                {
                    LoseAuthority(entry, LossCause.BeginUnresolved);
                    Diagnose($"unresolved Begin {command.CommandId} of {entry}: never invoked; pool {entry.Pool.Settings.Name} suspended until it resolves");
                    ResolveBeginDetached(entry, command, suspended);
                    suspended = false;
                    return null;
                }

                if (attempt == 0 && entry.IsLost)
                {
                    return null;
                }

                BeginReply? reply = null;
                TimeSpan sentAt;
                using (await entry.Arbiter.AcquireAsync(attempt == 0 ? ControlPriority.Begin : ControlPriority.Resolution, ControlStop).ConfigureAwait(false))
                {
                    entry.CommandUnresolved = true;
                    sentAt = Clock.Now;
                    try
                    {
                        reply = await CallAsync(AdmissionIo, ControlPriority.Resolution,
                            Min(Options.ControlRequestTimeout, first + Options.BeginResolutionBudget - sentAt + Options.ControlRequestTimeout),
                            t => Store.BeginExecutionAsync(command, t)).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!ControlStop.IsCancellationRequested)
                    {
                        Logger.LogDebug(ex, "Runwick Begin {Command} unresolved; resubmitting the same command.", command.CommandId);
                    }
                }

                if (reply is null)
                {
                    if (!suspended)
                    {
                        suspended = true;
                        entry.Pool.Suspend();
                    }

                    await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
                    continue;
                }

                entry.CommandUnresolved = false;
                return HandleBegin(entry, reply, sentAt);
            }
        }
        finally
        {
            if (suspended)
            {
                entry.Pool.Resume();
            }
        }
    }

    bool? HandleBegin(AttemptEntry entry, BeginReply reply, TimeSpan sentAt)
    {
        var r = reply.Reply;
        switch (r.Status)
        {
            case StoreStatus.Applied when r.Value is { } begun:
                entry.Begun = begun;
                if (!r.Replayed && !entry.IsLost)
                {
                    entry.AdvanceDeadline(DeadlineFrom(sentAt, begun.Lease, r.DecidedAt));
                }

                Emit(r.Replayed ? "begunReplayed" : "begun", entry);
                entry.BeginResult = "authorized";
                RecordBegin(entry, begun);
                if (!r.Replayed)
                {
                    // One final admission outcome per cycle (capacity-defaults §5): the Begin, not its Reserve.
                    entry.Pool.Outcomes.Record(Clock.Now, blocked: false);
                }

                return r.Replayed;
            case StoreStatus.InvalidState when r.Effect == StoreEffect.Applied:
            case StoreStatus.BudgetExhausted:
                // Storage closed the unstarted attempt (cancelled before start / budget exhausted): no scope, no invocation.
                Emit("beginClosed", entry);
                entry.BeginResult = r.Status == StoreStatus.BudgetExhausted ? "exhausted" : "cancelled";
                return null;
            case StoreStatus.StaleLease:
                LoseAuthority(entry, LossCause.StaleLease);
                return null;
            case StoreStatus.InvalidState:
                LoseAuthority(entry, LossCause.InvalidState);
                return null;
            case StoreStatus.RateLimited when r.Effect == StoreEffect.Applied:
                // T20 (10 §4): storage closed the unstarted attempt and restored the waiting state until EligibleAt. No
                // scope, no middleware, no unstarted-guard increment; the ticket returns to the pool now (10 §5).
                Interlocked.Increment(ref RateDeferrals);
                entry.BeginResult = "deferred";
                T.AdmissionDeferred.Add(1, "rate");
                entry.Pool.Outcomes.Record(Clock.Now, blocked: true);
                Emit("beginRateDeferred", entry);
                return null;
            case StoreStatus.RateLimited:
                Diagnose($"Begin of {entry} returned RateLimited without a committed effect");
                LoseAuthority(entry, LossCause.InvariantFault);
                return null;
            case StoreStatus.AdmissionClosed or StoreStatus.UnsupportedVersion:
                CloseAdmission($"Begin returned {r.Status}");
                LoseAuthority(entry, LossCause.InvalidState);
                return null;
            case StoreStatus.CommandExpired:
                LoseAuthority(entry, LossCause.BeginUnresolved);
                return null;
            default:
                Diagnose($"Begin of {entry} returned {r.Status} {r.Error?.Code}");
                LoseAuthority(entry, LossCause.InvariantFault);
                return null;
        }
    }

    /// <summary>After the Begin budget: keep resolving the same command (pool suspended) until it resolves or control stops; never invoke.</summary>
    void ResolveBeginDetached(AttemptEntry entry, StoreCommand<BeginRequest> command, bool alreadySuspended)
    {
        if (!alreadySuspended)
        {
            entry.Pool.Suspend();
        }

        _ = Task.Run(async () =>
        {
            try
            {
                for (var attempt = 0; !ControlStop.IsCancellationRequested; attempt++)
                {
                    try
                    {
                        var reply = await CallAsync(AdmissionIo, ControlPriority.Resolution, Options.ControlRequestTimeout,
                            t => Store.BeginExecutionAsync(command, t)).ConfigureAwait(false);
                        Diagnose($"Begin {command.CommandId} of {entry} resolved late as {reply.Reply.Status}; not invoked");
                        return;
                    }
                    catch (Exception) when (!ControlStop.IsCancellationRequested)
                    {
                        await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                entry.Pool.Resume();
            }
        });
    }

    /// <summary>
    /// Runs the pipeline on the thread pool and supervises it (05 §10.4): execution timeout, cancellation grace,
    /// shutdown cutoffs, then cleanup with its budget. Returns the frozen observation, or null when no Finish may be sent.
    /// </summary>
    async Task<PipelineObservation?> ExecuteAsync(AttemptEntry entry)
    {
        var ended = new TaskCompletionSource<PipelineObservation?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleaned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = entry.ExecutionStartedAt!.Value;
        var timeout = entry.Reserved.Policy.ExecutionTimeoutMs is { } ms ? TimeSpan.FromMilliseconds(ms) : (TimeSpan?)null;
        entry.PipelineLive = true;

        // §10.1 step 1: thread pool, ExecutionContext flow suppressed, no token that could skip the runner's bookkeeping.
        using (ExecutionContext.SuppressFlow())
        {
            entry.RunnerTask = Task.Run(() => RunnerAsync(entry, ended, cleaned, start, timeout));
        }

        var timedOut = false;
        while (!ended.Task.IsCompleted)
        {
            var now = Clock.Now;
            var next = TimeSpan.MaxValue;
            if (timeout is { } t && !timedOut)
            {
                if (now >= start + t)
                {
                    timedOut = true;
                    entry.AddCause(JobCancellationCause.Timeout, now);
                    _ = entry.CancelExecutionAsync();
                    Emit("timeout", entry);
                }
                else
                {
                    next = Min(next, start + t);
                }
            }

            if (Draining)
            {
                if (now >= DrainCutoff && !entry.Causes.HasFlag(JobCancellationCause.Shutdown))
                {
                    entry.AddCause(JobCancellationCause.Shutdown, now);
                    _ = entry.CancelExecutionAsync();
                }
                else if (now < DrainCutoff)
                {
                    next = Min(next, DrainCutoff);
                }

                if (now >= CancellationCutoff)
                {
                    // Pipelines still live at the cancellation cutoff latch abandonment (05 §16 step 5).
                    LoseAuthority(entry, LossCause.GraceExpired);
                }
                else
                {
                    next = Min(next, CancellationCutoff);
                }
            }

            if (entry.FirstCancellationAt is { } first && !entry.IsLost)
            {
                var graceEnd = first + Options.CancellationGrace;
                if (now >= graceEnd)
                {
                    LoseAuthority(entry, LossCause.GraceExpired);
                    Diagnose($"cancellation grace expired with the pipeline still running: {entry}");
                }
                else
                {
                    next = Min(next, graceEnd);
                }
            }

            if (entry.IsLost)
            {
                // No timers remain: wait for user code to end (ticket and scope retained meanwhile).
                await ended.Task.ConfigureAwait(false);
                break;
            }

            var wait = next == TimeSpan.MaxValue ? TimeSpan.FromSeconds(1) : Min(TimeSpan.FromSeconds(1), next - now);
            using var cts = new CancellationTokenSource();
            var changed = entry.Changed.WaitAsync(Clock, wait, cts.Token);
            await Task.WhenAny(ended.Task, changed).ConfigureAwait(false);
            await cts.CancelAsync().ConfigureAwait(false);
            try
            {
                await changed.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        var observation = await ended.Task.ConfigureAwait(false);
        entry.PipelineLive = false;
        if (observation is null || entry.IsLost)
        {
            await cleaned.Task.ConfigureAwait(false);
            return null;
        }

        entry.TryMove(AttemptPhase.CleaningUp);
        var cleanupEnd = Clock.Now + Options.CleanupBudget;
        if (Draining)
        {
            cleanupEnd = Min(cleanupEnd, ControlStopCutoff);
        }

        using (var budget = new CancellationTokenSource())
        {
            var delay = Clock.Delay(cleanupEnd - Clock.Now, budget.Token);
            await Task.WhenAny(cleaned.Task, delay).ConfigureAwait(false);
            await budget.CancelAsync().ConfigureAwait(false);
        }

        if (!cleaned.Task.IsCompleted)
        {
            // Never a cleanup-induced retry and never a safe success: unconfirmed completion.
            LoseAuthority(entry, LossCause.CleanupExpired);
            Diagnose($"unconfirmed completion: scope disposal of {entry} exceeded {Options.CleanupBudget}");
            RunwickLog.CleanupBudgetExceeded(Logger, entry.Lease.JobId, entry.AttemptId, (long)Options.CleanupBudget.TotalMilliseconds);
            await cleaned.Task.ConfigureAwait(false);
            return null;
        }

        return entry.IsLost ? null : observation;
    }

    /// <summary>The runner body (05 §10.1 steps 2–7); it never throws.</summary>
    async Task RunnerAsync(AttemptEntry entry, TaskCompletionSource<PipelineObservation?> ended, TaskCompletionSource cleaned, TimeSpan start, TimeSpan? timeout)
    {
        AsyncServiceScope? scope = null;
        try
        {
            if (Hooks.BeforeRunnerStart is { } hook)
            {
                await hook(entry).ConfigureAwait(false);
            }

            // Step 2: rechecks. A latched loss skips decode, DI and handler; the Begin stays counted.
            if (entry.IsLost)
            {
                ended.TrySetResult(null);
                return;
            }

            if (timeout is { } t && Clock.Now - start >= t)
            {
                ended.TrySetResult(new PipelineObservation(null, null, null, null, Clock.Now - start));
                return;
            }

            var pipeline = Pipelines[entry.Job!.Name];
            object payload;
            try
            {
                payload = pipeline.Decode(entry.Reserved);
            }
            catch (PayloadValidationException ex)
            {
                ended.TrySetResult(new PipelineObservation(null, null, null, ex, Clock.Now - start));
                return;
            }
            catch (InvalidOperationException ex)
            {
                CloseAdmission("advertised tuple cannot be decoded: " + ex.Message);
                LoseAuthority(entry, LossCause.InvariantFault);
                ended.TrySetResult(null);
                return;
            }

            scope = ScopeFactory.CreateAsyncScope();
            Interlocked.Increment(ref ScopesCreated);
            JobPipelineResult result;
            try
            {
                var fences = entry.Reserved.Fences.Select(f => ScopeMap.TryGetValue(f.ScopeId, out var s)
                    ? new ResourceFence(s.Kind, s.Key, f.Value) : new ResourceFence("unknown", f.ScopeId.ToString("D"), f.Value)).ToList();
                var context = new JobContext(entry, Application, payload, fences);
                var holder = scope.Value.ServiceProvider.GetRequiredService<JobContextHolder>();
                holder.Context = context;
                holder.Entry = entry;
                holder.Engine = this;
                Interlocked.Increment(ref Invocations);
                Emit("handlerInvoked", entry);

                // The runner's flow is suppressed (no ambient activity or baggage); the handler sees its own execute span
                // as Activity.Current, parented explicitly to this attempt's process span (18 §2.2, OBS-05).
                using var execute = T.Span("runwick.execute", ActivityKind.Internal, entry.Span?.Context ?? default);
                var (value, error, misuse) = await pipeline.RunAsync(scope.Value.ServiceProvider, context, payload, entry.ExecutionToken).ConfigureAwait(false);
                result = new JobPipelineResult(value, error, misuse);
                if (execute.Activity is { } span)
                {
                    span.SetTag("runwick.outcome", error is null && misuse is null && value?.Kind is null or JobExecutionResultKind.Succeeded or JobExecutionResultKind.Completed
                        ? "succeeded" : "failed");
                    if (error is not null)
                    {
                        span.SetTag("error.type", T.ErrorType(error.GetType().FullName ?? error.GetType().Name)).SetStatus(ActivityStatusCode.Error);
                        if (T.RecordExceptionDetails)
                        {
                            span.SetTag("exception.message", T.Sanitize(error.Message));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result = new JobPipelineResult(null, ex, null);
            }

            ended.TrySetResult(new PipelineObservation(result.Value, result.Error, result.Misuse, null, Clock.Now - start));
        }
        catch (Exception ex)
        {
            ended.TrySetResult(new PipelineObservation(null, ex, null, null, Clock.Now - start));
        }
        finally
        {
            if (scope is { } s)
            {
                entry.CleanupLive = true;
                var cleanupStart = Stopwatch.GetTimestamp();
                using var cleanup = T.Span("runwick.attempt.cleanup", ActivityKind.Internal, entry.Span?.Context ?? default);
                try
                {
                    await s.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // A cleanup exception after a timely success is a diagnostic only; its message never reaches telemetry (18 §6).
                    entry.Diagnostic = "scope disposal failed: " + ex.GetType().Name;
                    Diagnose($"scope disposal of {entry} threw {ex.GetType().Name}");
                }

                entry.CleanupLive = false;
                var cleanupResult = entry.Loss == LossCause.CleanupExpired ? "budget_exceeded" : "completed";
                cleanup.Activity?.SetTag("runwick.result", cleanupResult);
                T.AttemptCleanupDuration.Record(Stopwatch.GetElapsedTime(cleanupStart).TotalSeconds, cleanupResult);
            }

            ended.TrySetResult(null);
            cleaned.TrySetResult();
        }
    }

    readonly record struct JobPipelineResult(JobExecutionResult? Value, Exception? Error, string? Misuse);

    /// <summary>
    /// Drain release of a reserved-not-begun attempt (05 §16 step 2): ownership-checked, the same command on every
    /// resubmission; unconfirmed at the drain cutoff latches <see cref="LossCause.ReleaseUnconfirmed"/>.
    /// </summary>
    async Task ReleaseAsync(AttemptEntry entry)
    {
        if (!entry.TryMove(AttemptPhase.Releasing))
        {
            return;
        }

        var command = Command(new ReleaseRequest(entry.Lease));
        for (var attempt = 0; ; attempt++)
        {
            var now = Clock.Now;
            if (now >= DrainCutoff)
            {
                LoseAuthority(entry, LossCause.ReleaseUnconfirmed);
                Diagnose($"ReleaseReservation {command.CommandId} of {entry} unconfirmed at the drain cutoff; expiry recovery applies");
                return;
            }

            StoreReply<ReleasedReservation>? reply = null;
            using (await entry.Arbiter.AcquireAsync(attempt == 0 ? ControlPriority.Settlement : ControlPriority.Resolution, ControlStop).ConfigureAwait(false))
            {
                entry.CommandUnresolved = true;
                try
                {
                    reply = await CallAsync(SettlementIo, 0, Min(Options.ControlRequestTimeout, DrainCutoff - now),
                        t => Store.ReleaseReservationAsync(command, t)).ConfigureAwait(false);
                }
                catch (Exception ex) when (!ControlStop.IsCancellationRequested)
                {
                    Logger.LogDebug(ex, "Runwick ReleaseReservation {Command} unresolved; resubmitting.", command.CommandId);
                }
            }

            if (reply is not null)
            {
                entry.CommandUnresolved = false;
                switch (reply.Status)
                {
                    case StoreStatus.Applied:
                        Emit("released", entry);
                        return;
                    case StoreStatus.StaleLease:
                        LoseAuthority(entry, LossCause.StaleLease);
                        return;
                    case StoreStatus.InvalidState:
                        LoseAuthority(entry, LossCause.InvalidState);
                        return;
                    case StoreStatus.CommandExpired:
                        LoseAuthority(entry, LossCause.ReleaseUnconfirmed);
                        return;
                }
            }

            await Clock.Delay(Backoff(attempt), ControlStop).ConfigureAwait(false);
        }
    }
}
