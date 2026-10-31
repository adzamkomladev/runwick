using NodaTime;
using Runwick.Protocol;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Scheduling;

/// <summary>One evaluated occurrence: its UTC instant and whether a gap shift coalesced a normal match into it (12 §7.2).</summary>
internal readonly record struct DueOccurrence(DateTimeOffset At, bool Coalesced);

/// <summary>
/// <c>next(x)</c> of 13 §5 for one stored definition: the first occurrence strictly after x, or null when the schedule has
/// no further occurrence. Cron definitions run the bounded planner (12 §7–§8) under one shared budget per plan.
/// </summary>
internal static class ScheduleRecurrence
{
    public static Func<DateTimeOffset, DueOccurrence?> For(ScheduleDefinitionRecord definition, TzdbArtifact artifact, TimeProvider time)
    {
        if (definition.Kind == ScheduleOperations.OneTime)
        {
            var due = definition.DueAt!.Value;
            return x => due > x ? new DueOccurrence(due, false) : null;
        }

        // A stored definition this process cannot evaluate is local incompatibility, never a defect (12 §6.3, SCHED-11).
        ScheduleDefinition.RequireSupported(definition.EvaluatorProfile!, definition.TzdbVersion!, definition.TzdbSha256, artifact);
        var schedule = ScheduleDefinition.Compile(definition.CronText, definition.CronDialect!, definition.ZoneId, artifact,
            ParseGap(definition.GapPolicy), ParseFold(definition.FoldPolicy));
        var budget = new EvaluationBudget(time, PlannerLimits.Plan);
        return x =>
        {
            var next = RecurrencePlanner.Next(schedule, Instant.FromUnixTimeMilliseconds(x.ToUnixTimeMilliseconds()), PlannerLimits.Plan, budget);
            return new DueOccurrence(DateTimeOffset.FromUnixTimeMilliseconds(next.UtcEpochMilliseconds), next.Coalesced);
        };
    }

    public static GapPolicy ParseGap(string wire) => wire switch
    {
        "skip" => GapPolicy.Skip,
        "shift" => GapPolicy.Shift,
        _ => throw new ArgumentOutOfRangeException(nameof(wire), wire, "Unknown gap policy."),
    };

    public static FoldPolicy ParseFold(string wire) => wire switch
    {
        "earlier" => FoldPolicy.Earlier,
        "later" => FoldPolicy.Later,
        "both" => FoldPolicy.Both,
        _ => throw new ArgumentOutOfRangeException(nameof(wire), wire, "Unknown fold policy."),
    };
}

/// <summary>
/// The pure materialization planner of 13 §5. It turns one schedule snapshot and the frozen cutoff T into the
/// <see cref="MaterializeRequest"/> the store checks and applies, or null when nothing changes.
/// </summary>
internal static class SchedulePlanner
{
    public static MaterializeRequest? Plan(ScheduleSnapshot s, DateTimeOffset t, Func<DateTimeOffset, DueOccurrence?> next, Func<Guid> newJobId)
    {
        var d = s.Definition;
        var maxBatch = Math.Min(d.MaxBatch, ScheduleBounds.MaxBatch);
        var builder = new Builder(s, t, next, newJobId, maxBatch);

        if (s.Episode is { } episode)
        {
            return builder.CatchUp(episode, s.ResumePolicy is not null);
        }

        if (s.NextDueAt is not { } due)
        {
            return null;
        }

        if (due > t)
        {
            // Not due yet: only move the scan position to the due instant (keeps the due read from re-reading it).
            return s.ScanAfterAt == due ? null : builder.Cursor([], [], new ScheduleCursor(s.EvaluatedThrough, due, due, null), clears: false);
        }

        // A pending resume policy covers the occurrences through its cutoff (13 §5.4).
        if (s.ResumePolicy is { } pending && s.ResumeCutoff is { } resumeCutoff)
        {
            if (due <= resumeCutoff)
            {
                return pending switch
                {
                    ScheduleOperations.ResumeSkipPaused or ScheduleOperations.PendingEdit =>
                        builder.Skip(due, resumeCutoff, pending == ScheduleOperations.PendingEdit ? "skipped_by_edit" : "skipped_paused", clears: true),
                    ScheduleOperations.MisfireRunOnce => builder.RunOnce(due, resumeCutoff, clears: true),
                    _ => builder.OpenCatchUp(due, resumeCutoff, clears: true),
                };
            }

            return builder.Normal(due, clears: true);
        }

        if (due <= t - d.Grace)
        {
            return d.MisfirePolicy switch
            {
                ScheduleOperations.MisfireSkip => builder.Skip(due, t, "misfire_skip", clears: false),
                ScheduleOperations.MisfireCatchUp when d.Kind == ScheduleOperations.Cron => builder.OpenCatchUp(due, t, clears: false),
                _ => builder.RunOnce(due, t, clears: false),
            };
        }

        return builder.Normal(due, clears: false);
    }

    sealed class Builder(ScheduleSnapshot s, DateTimeOffset t, Func<DateTimeOffset, DueOccurrence?> next, Func<Guid> newJobId, int maxBatch)
    {
        public MaterializeRequest Cursor(IReadOnlyList<PlannedOccurrence> occurrences, IReadOnlyList<ScheduleAuditRange> ranges, ScheduleCursor after, bool clears) =>
            new(s.ScheduleId, s.DefinitionRevision, s.ControlRevision, s.CursorRevision, s.Episode?.Id, t, occurrences, after, ranges, clears);

        /// <summary>§5.1: every occurrence in [due, T], at most one batch.</summary>
        public MaterializeRequest Normal(DateTimeOffset due, bool clears)
        {
            var list = new List<PlannedOccurrence>();
            DueOccurrence? cursor = new DueOccurrence(due, false);
            while (cursor is { } c && c.At <= t && list.Count < maxBatch)
            {
                list.Add(new PlannedOccurrence(c.At, newJobId(), Coalesced: c.Coalesced));
                cursor = next(c.At);
            }

            var remaining = cursor is { } r && r.At <= t;
            var after = remaining
                ? new ScheduleCursor(list[^1].ScheduledAt, cursor!.Value.At, t, null)
                : new ScheduleCursor(t, cursor?.At, cursor?.At ?? t, null);
            return Cursor(list, [], after, clears);
        }

        /// <summary>§5.2 skip: no jobs, one audited range, next after the cutoff.</summary>
        public MaterializeRequest Skip(DateTimeOffset due, DateTimeOffset cutoff, string reason, bool clears)
        {
            var following = next(cutoff);
            return Cursor([], [new ScheduleAuditRange(s.EvaluatedThrough, cutoff, reason)],
                new ScheduleCursor(cutoff, following?.At, following?.At ?? cutoff, null), clears);
        }

        /// <summary>§5.2 run-once: one job keyed by the earliest missed instant, covering through the cutoff.</summary>
        public MaterializeRequest RunOnce(DateTimeOffset due, DateTimeOffset cutoff, bool clears)
        {
            var following = next(cutoff);
            return Cursor([new PlannedOccurrence(due, newJobId(), due, cutoff)],
                [new ScheduleAuditRange(s.EvaluatedThrough, cutoff, "run_once_covered")],
                new ScheduleCursor(cutoff, following?.At, following?.At ?? cutoff, null), clears);
        }

        /// <summary>§5.2 catch-up: open an episode with an immutable cutoff, or ask for review when the backlog is too old.</summary>
        public MaterializeRequest OpenCatchUp(DateTimeOffset due, DateTimeOffset cutoff, bool clears)
        {
            if (due < cutoff - s.Definition.MaxCatchUpAge)
            {
                return Cursor([], [], new ScheduleCursor(s.EvaluatedThrough ?? EarliestBefore(due), due, s.ScanAfterAt, null, "catch_up_too_old"), clears);
            }

            return CatchUp(new ScheduleEpisode(Guid.CreateVersion7(), cutoff, 0, s.Definition.MaxEpisodeJobs), clears, due);
        }

        /// <summary>§5.3: one chronological batch of an episode; the cutoff never moves.</summary>
        public MaterializeRequest CatchUp(ScheduleEpisode episode, bool clears, DateTimeOffset? first = null)
        {
            var list = new List<PlannedOccurrence>();
            DueOccurrence? cursor = (first ?? s.NextDueAt) is { } start ? new DueOccurrence(start, false) : null;
            while (cursor is { } c && c.At <= episode.Cutoff && list.Count < maxBatch && episode.Emitted + list.Count < episode.Limit)
            {
                list.Add(new PlannedOccurrence(c.At, newJobId(), Coalesced: c.Coalesced));
                cursor = next(c.At);
            }

            var emitted = episode.Emitted + list.Count;
            var through = list.Count > 0 ? list[^1].ScheduledAt : s.EvaluatedThrough;
            if (cursor is not { } rest || rest.At > episode.Cutoff)
            {
                // Episode complete; a full limit with nothing left completes too (SCHED-10).
                return Cursor(list, [], new ScheduleCursor(episode.Cutoff, cursor?.At, cursor?.At ?? t, null), clears);
            }

            var continued = episode with { Emitted = emitted };
            return emitted >= episode.Limit
                ? Cursor(list, [], new ScheduleCursor(through, rest.At, t, continued, "catch_up_limit"), clears)
                : Cursor(list, [], new ScheduleCursor(through, rest.At, t + ScheduleBounds.CatchUpPacing, continued), clears);
        }

        static DateTimeOffset EarliestBefore(DateTimeOffset due) => due.AddMilliseconds(-1);
    }
}
