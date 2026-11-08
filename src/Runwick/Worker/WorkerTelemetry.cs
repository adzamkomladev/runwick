using System.Diagnostics;
using System.Diagnostics.Metrics;
using Runwick.Protocol;
using Runwick.Telemetry;
using T = Runwick.Telemetry.Telemetry;

namespace Runwick.Worker;

/// <summary>
/// Worker spans and measurements (18 §2–§3). Every call here runs after the state change it reports is decided and
/// outside lease, ticket and completion critical sections (18 §1, OBS-01).
/// </summary>
internal sealed partial class WorkerEngine
{
    internal static string Snake(string pascal)
    {
        var sb = new System.Text.StringBuilder(pascal.Length + 4);
        for (var i = 0; i < pascal.Length; i++)
        {
            if (char.IsUpper(pascal[i]) && i > 0)
            {
                sb.Append('_');
            }

            sb.Append(char.ToLowerInvariant(pascal[i]));
        }

        return sb.ToString();
    }

    static readonly Dictionary<AttemptOutcome, string> OutcomeNames = Enum.GetValues<AttemptOutcome>().ToDictionary(o => o, o => Snake(o.ToString()));
    static readonly Dictionary<LossCause, string> LossNames = Enum.GetValues<LossCause>().ToDictionary(o => o, o => Snake(o.ToString()));

    internal static string OutcomeName(AttemptOutcome outcome) => OutcomeNames[outcome];

    internal static string LossName(LossCause cause) => LossNames[cause];

    /// <summary>Starts the attempt's <c>process</c> span: a new trace linked to the stored dispatch context (18 §2.1–2.2).</summary>
    void StartProcessSpan(AttemptEntry entry)
    {
        if (!T.Source.HasListeners())
        {
            return;
        }

        // The worker loop's ambient context never becomes the parent: the process span is a root (18 §2.2).
        Activity.Current = null;
        var meta = entry.Reserved.DiagnosticMetadata;
        ActivityLink[]? links = null;
        if (meta.TryGetValue("traceparent", out var tp) && ActivityContext.TryParse(tp, meta.GetValueOrDefault("tracestate"), isRemote: true, out var linked))
        {
            links = [new ActivityLink(linked)];
        }

        var queue = entry.Reserved.Queue;
        var span = T.Source.StartActivity("process " + queue, ActivityKind.Consumer, default(ActivityContext), null, links);
        if (span is null)
        {
            return;
        }

        span.SetTag("runwick.job.id", entry.Lease.JobId.ToString("D"))
            .SetTag("runwick.job.generation", entry.Lease.Generation)
            .SetTag("runwick.attempt.id", entry.AttemptId.ToString("D"))
            .SetTag("messaging.system", "runwick")
            .SetTag("messaging.operation.type", "process")
            .SetTag("messaging.operation.name", "process")
            .SetTag("messaging.destination.name", queue)
            .SetTag("messaging.message.id", entry.Lease.JobId.ToString("D"));
        entry.Span = span;
    }

    /// <summary>Retirement: authority-loss count and the end of the <c>process</c> span.</summary>
    void EndProcessSpan(AttemptEntry entry)
    {
        if (entry.Loss is { } loss)
        {
            T.LeaseAuthorityLost.Add(1, LossName(loss));
        }

        if (entry.Span is { } span)
        {
            if (entry.Loss is not null && span.GetTagItem("runwick.settlement.result") is null)
            {
                span.SetTag("runwick.settlement.result", entry.Loss == LossCause.ShutdownCutoff ? "unresolved" : "lost");
                span.SetStatus(ActivityStatusCode.Error);
            }

            span.Dispose();
            entry.Span = null;
        }
    }

    /// <summary>An Applied Begin (18 §3.1): replayed receipts are not counted again (OBS-02, OBS-08).</summary>
    void RecordBegin(AttemptEntry entry, BegunExecution begun, bool replayed)
    {
        entry.Span?.SetTag("runwick.execution.number", begun.ExecutionNumber);
        if (replayed)
        {
            return;
        }

        var queue = T.Queue(entry.Reserved.Queue);
        T.ExecutionAuthorized.Add(1, queue, T.JobName(entry.Reserved.Contract.Name), entry.Pool.Settings.Name);
        T.ExecutionStartDelay.Record(Math.Max(0, (begun.AuthorizedAt - entry.Reserved.Cursor.EligibleAt).TotalSeconds), queue);
    }

    /// <summary>The classified outcome of one invocation (18 §3.1 <c>runwick.execution.duration</c>, log 1011).</summary>
    void RecordExecution(AttemptEntry entry, PipelineObservation observation, FinishRequest finish)
    {
        var outcome = OutcomeName(finish.Outcome);
        T.ExecutionDuration.Record(observation.Elapsed.TotalSeconds, T.Queue(entry.Reserved.Queue), T.JobName(entry.Reserved.Contract.Name), outcome);
        entry.Span?.SetTag("runwick.outcome", outcome);
        if (finish.Outcome == AttemptOutcome.Succeeded)
        {
            return;
        }

        var errorType = T.ErrorType(finish.Error is { ExceptionType.Length: > 0 } e ? e.ExceptionType : finish.Error?.Code ?? outcome);
        entry.Span?.SetTag("error.type", errorType).SetStatus(ActivityStatusCode.Error);
        var message = T.RecordExceptionDetails && finish.Error is { Message.Length: > 0 } m ? T.Sanitize(m.Message) : null;
        RunwickLog.HandlerFailed(Logger, entry.Lease.JobId, entry.AttemptId, entry.Reserved.Contract.Name, outcome, errorType, message);
    }

    /// <summary>One Renew result, recorded after the arbiter slot is released (18 §3.1).</summary>
    internal void RecordRenewal(AttemptEntry entry, LeaseCoordinator.RenewResult result, TimeSpan? marginAtReply)
    {
        switch (result)
        {
            case LeaseCoordinator.RenewResult.Fresh:
                T.LeaseRenewals.Add(1, "renewed");
                if (marginAtReply is { } m)
                {
                    T.LeaseMargin.Record(Math.Max(0, m.TotalSeconds), entry.Pool.Settings.Name);
                }

                break;
            case LeaseCoordinator.RenewResult.Lost:
                T.LeaseRenewals.Add(1, "stale");
                break;
            case LeaseCoordinator.RenewResult.Transient:
                T.LeaseRenewals.Add(1, "error");
                break;
        }
    }

    /// <summary>Rebuilds the local gauge snapshot (18 §3.4); called by the 1 s supervisor tick, never by a callback.</summary>
    internal void PublishGauges()
    {
        var now = Clock.Now;
        var open = AdmissionOpen;
        List<Measurement<double>> tickets = [], targets = [], oldest = [], admission = [];
        foreach (var pool in Pools)
        {
            var name = new KeyValuePair<string, object?>("runwick.pool", pool.Settings.Name);
            var mine = _entries.Values.Where(e => e.Pool == pool && e.Phase != AttemptPhase.Retired).ToList();
            foreach (var group in mine.GroupBy(TicketPhase))
            {
                tickets.Add(new(group.Count(), name, new("runwick.ticket.phase", group.Key)));
            }

            targets.Add(new(pool.Gate.Target, name));
            var unsettled = mine.Where(e => e.Phase == AttemptPhase.Finishing && e.FinishPublishedAt is not null).Select(e => now - e.FinishPublishedAt!.Value).ToList();
            if (unsettled.Count > 0)
            {
                oldest.Add(new(unsettled.Max().TotalSeconds, name));
            }

            admission.Add(new(open && !pool.Suspended ? 1 : 0, name));
        }

        _gauges = new WorkerGauges(tickets, targets, oldest, admission);
    }

    WorkerGauges? _gauges;

    static string TicketPhase(AttemptEntry e) => e.Phase switch
    {
        AttemptPhase.PendingClaim => "reserving",
        AttemptPhase.Reserved or AttemptPhase.Beginning or AttemptPhase.Releasing => "reserved",
        AttemptPhase.Running => "running",
        AttemptPhase.CleaningUp => "cleanup",
        AttemptPhase.Finishing => "settling",
        _ => e.PipelineLive ? "running" : e.CleanupLive ? "cleanup" : "settling",
    };
}
