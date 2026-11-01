using Runwick.Core.Tests.Support;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Core.Tests.Scheduling;

/// <summary>
/// The pure materialization planner of 13 §5 (SP-016): normal and partial batches, the three misfire policies, catch-up
/// episodes under a fixed cutoff and their limit, pending resume policies, one-time schedules and the review cases.
/// Every-minute UTC cron under the pinned artifact; the store's checks are covered by <c>ScheduleStoreTests</c>.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-016")]
public sealed class SchedulePlannerTests
{
    static readonly DateTimeOffset T = new(2026, 11, 1, 12, 10, 0, TimeSpan.Zero);

    static readonly Dispatching.DispatchPreparer Preparer = TestHost.Preparer(r => r.AddJob<PlainJob>());

    static ScheduleDefinitionRecord Definition(Action<ScheduleDeclaration>? configure = null)
    {
        var d = new ScheduleDeclaration("tick", new PlainJob("tick")) { Cron = "* * * * *" };
        configure?.Invoke(d);
        return ScheduleCompiler.Compile(d, Preparer, TzdbArtifact.Pinned);
    }

    /// <summary>A snapshot whose last evaluated instant is <paramref name="through"/>, next due one minute later.</summary>
    static ScheduleSnapshot Snapshot(ScheduleDefinitionRecord d, DateTimeOffset through, ScheduleEpisode? episode = null, string? resume = null,
        DateTimeOffset? resumeCutoff = null, DateTimeOffset? next = null) =>
        new(Guid.CreateVersion7(), "tick", 1, 1, 1, true, false, false, null, next ?? through.AddMinutes(1), through, through, episode, resume,
            resumeCutoff, d);

    static MaterializeRequest? Plan(ScheduleSnapshot s, DateTimeOffset? t = null) =>
        SchedulePlanner.Plan(s, t ?? T, ScheduleRecurrence.For(s.Definition, TzdbArtifact.Pinned, new ScheduleFixtures.FrozenTime()), Guid.CreateVersion7);

    static ScheduleSnapshot Apply(ScheduleSnapshot s, MaterializeRequest p) => s with
    {
        CursorRevision = s.CursorRevision + 1,
        EvaluatedThrough = p.After.EvaluatedThrough,
        NextDueAt = p.After.NextDueAt,
        ScanAfterAt = p.After.ScanAfterAt,
        Episode = p.After.Episode,
        NeedsReview = p.After.ReviewReason is not null,
        ReviewReason = p.After.ReviewReason,
        ResumePolicy = p.ClearsPendingPolicy ? null : s.ResumePolicy,
        ResumeCutoff = p.ClearsPendingPolicy ? null : s.ResumeCutoff,
    };

    [Fact]
    [Trait("Case", "SCHED-09")]
    public void Run_once_emits_the_earliest_missed_minute_covering_through_the_cutoff()
    {
        // T = 12:10Z, earliest missed 12:00Z, grace 60 s (13 §5.2, scheduler-evaluator §7 SCHED-09).
        var p = Plan(Snapshot(Definition(), T.AddMinutes(-11)))!;
        var o = Assert.Single(p.Occurrences);
        Assert.Equal((T.AddMinutes(-10), T.AddMinutes(-10), T), (o.ScheduledAt, o.CoveredFrom!.Value, o.CoveredThrough!.Value));
        Assert.Equal(new ScheduleAuditRange(T.AddMinutes(-11), T, "run_once_covered"), Assert.Single(p.AuditRanges));
        Assert.Equal((T, T.AddMinutes(1), T.AddMinutes(1)), (p.After.EvaluatedThrough!.Value, p.After.NextDueAt!.Value, p.After.ScanAfterAt));
        Assert.Null(p.After.Episode);
    }

    [Fact]
    [Trait("Case", "SCHED-09")]
    public void Skip_emits_nothing_and_audits_the_missed_range()
    {
        var p = Plan(Snapshot(Definition(d => d.Misfire = MisfirePolicy.Skip), T.AddMinutes(-11)))!;
        Assert.Empty(p.Occurrences);
        Assert.Equal(new ScheduleAuditRange(T.AddMinutes(-11), T, "misfire_skip"), Assert.Single(p.AuditRanges));
        Assert.Equal((T, T.AddMinutes(1)), (p.After.EvaluatedThrough!.Value, p.After.NextDueAt!.Value));
    }

    [Fact]
    [Trait("Case", "SCHED-09")]
    public void Catch_up_emits_all_eleven_and_closes_the_episode()
    {
        var p = Plan(Snapshot(Definition(d => d.Misfire = MisfirePolicy.CatchUp), T.AddMinutes(-11)))!;
        Assert.Equal(Enumerable.Range(0, 11).Select(i => T.AddMinutes(i - 10)), p.Occurrences.Select(o => o.ScheduledAt));
        Assert.Null(p.After.Episode);
        Assert.Equal((T, T.AddMinutes(1)), (p.After.EvaluatedThrough!.Value, p.After.NextDueAt!.Value));
        Assert.Empty(p.AuditRanges);
    }

    [Theory]
    [Trait("Case", "SCHED-09")]
    [InlineData(59_999, false)]
    [InlineData(60_000, true)]
    public void Grace_equality_counts_as_misfired(int lateMs, bool misfired)
    {
        // next_due_at ≤ T − grace is a misfire, equality included (13 §5.2).
        var due = T.AddMilliseconds(-lateMs);
        var s = Snapshot(Definition(), due.AddMinutes(-1), next: due);
        var p = Plan(s, T)!;
        Assert.Equal(misfired, p.AuditRanges.Count == 1);
        Assert.Equal(due, p.Occurrences[0].ScheduledAt);
        Assert.Equal(misfired ? 1 : 2, p.Occurrences.Count);
    }

    [Fact]
    [Trait("Case", "SCHED-09")]
    public void A_schedule_not_yet_due_only_moves_its_scan_position()
    {
        var s = Snapshot(Definition(), T) with { ScanAfterAt = T.AddSeconds(-5) };
        var p = Plan(s)!;
        Assert.Empty(p.Occurrences);
        Assert.Equal((T, T.AddMinutes(1), T.AddMinutes(1)), (p.After.EvaluatedThrough!.Value, p.After.NextDueAt!.Value, p.After.ScanAfterAt));
        Assert.Null(Plan(s with { ScanAfterAt = T.AddMinutes(1) }));
    }

    [Fact]
    [Trait("Case", "SCH-04")]
    public void A_normal_backlog_over_one_batch_is_partial_and_resumes_at_the_first_unemitted()
    {
        // A 40 s cron6 grace-free backlog: within grace, so §5.1 batches of 32 apply.
        var d = Definition(x => { x.Cron = "* * * * * *"; x.Dialect = CronDialect.Cron6; x.Grace = TimeSpan.FromMinutes(5); });
        var s = Snapshot(d, T.AddSeconds(-41), next: T.AddSeconds(-40));
        var p = Plan(s)!;
        Assert.Equal(32, p.Occurrences.Count);
        Assert.Equal((T.AddSeconds(-9), T.AddSeconds(-8), T), (p.After.EvaluatedThrough!.Value, p.After.NextDueAt!.Value, p.After.ScanAfterAt));
        var rest = Plan(Apply(s, p))!;
        Assert.Equal(9, rest.Occurrences.Count);
        Assert.Equal(T, rest.After.EvaluatedThrough);
    }

    [Fact]
    [Trait("Case", "SCH-04")]
    public void Catch_up_runs_32_32_6_under_one_cutoff_with_pacing()
    {
        var s = Snapshot(Definition(d => d.Misfire = MisfirePolicy.CatchUp), T.AddMinutes(-71));
        var sizes = new List<int>();
        DateTimeOffset? cutoff = null;
        var later = T;
        for (var pass = 0; pass < 5; pass++)
        {
            // Each later pass happens after the pacing delay; the episode cutoff never moves with T.
            var p = Plan(s, later)!;
            sizes.Add(p.Occurrences.Count);
            if (p.After.Episode is { } e)
            {
                cutoff ??= e.Cutoff;
                Assert.Equal(cutoff, e.Cutoff);
                Assert.Equal(p.Occurrences[^1].ScheduledAt, p.After.EvaluatedThrough);
                Assert.Equal(later + ScheduleBounds.CatchUpPacing, p.After.ScanAfterAt);
            }

            s = Apply(s, p);
            later = later.AddSeconds(1);
            if (s.Episode is null)
            {
                break;
            }
        }

        Assert.Equal([32, 32, 7], sizes);
        Assert.Equal(T, cutoff);
        Assert.Equal((T, T.AddMinutes(1)), (s.EvaluatedThrough!.Value, s.NextDueAt!.Value));
    }

    [Theory]
    [Trait("Case", "SCHED-10")]
    [InlineData(40, false)]
    [InlineData(41, true)]
    public void An_episode_of_exactly_the_limit_completes_and_one_more_needs_review(int missed, bool review)
    {
        var s = Snapshot(Definition(d => { d.Misfire = MisfirePolicy.CatchUp; d.MaxEpisodeJobs = 40; }), T.AddMinutes(-missed));
        var emitted = 0;
        for (var pass = 0; pass < 5 && !s.NeedsReview; pass++)
        {
            var p = Plan(s)!;
            emitted += p.Occurrences.Count;
            s = Apply(s, p);
            if (s.Episode is null)
            {
                break;
            }
        }

        Assert.Equal(40, emitted);
        Assert.Equal(review ? "catch_up_limit" : null, s.ReviewReason);
        Assert.Equal(review, s.Episode is not null);
        if (review)
        {
            // The first unprocessed occurrence is preserved for the operator (ResolveCatchUp).
            Assert.Equal(T, s.NextDueAt);
            Assert.Equal(T.AddMinutes(-1), s.EvaluatedThrough);
        }
    }

    [Fact]
    [Trait("Case", "SCHED-10")]
    public void A_backlog_older_than_the_catch_up_age_needs_review_with_the_cursor_kept()
    {
        var s = Snapshot(Definition(d => { d.Misfire = MisfirePolicy.CatchUp; d.MaxCatchUpAge = TimeSpan.FromMinutes(5); }), T.AddMinutes(-11));
        var p = Plan(s)!;
        Assert.Empty(p.Occurrences);
        Assert.Equal("catch_up_too_old", p.After.ReviewReason);
        Assert.Equal((s.EvaluatedThrough, s.NextDueAt), (p.After.EvaluatedThrough, p.After.NextDueAt));
        Assert.Null(p.After.Episode);
    }

    [Theory]
    [Trait("Case", "SCHED-15")]
    [InlineData(ScheduleOperations.ResumeSkipPaused, 0, "skipped_paused")]
    [InlineData(ScheduleOperations.PendingEdit, 0, "skipped_by_edit")]
    [InlineData(ScheduleOperations.MisfireRunOnce, 1, "run_once_covered")]
    [InlineData(ScheduleOperations.MisfireCatchUp, 6, null)]
    public void A_pending_resume_policy_covers_through_its_cutoff_then_normal_rules_apply(string policy, int jobs, string? range)
    {
        // Paused 12:00–12:05; resumed at 12:05 with a pending policy; planned at 12:10 (13 §5.4). The policy covers the
        // occurrences through 12:05 and is cleared; 12:06 … 12:10 follow in the next plan.
        var resumedAt = T.AddMinutes(-5);
        var s = Snapshot(Definition(), T.AddMinutes(-11), resume: policy, resumeCutoff: resumedAt);
        var p = Plan(s)!;
        Assert.True(p.ClearsPendingPolicy);
        Assert.Equal(jobs, p.Occurrences.Count);
        Assert.Equal(range, p.AuditRanges.SingleOrDefault()?.Reason);
        Assert.Equal(resumedAt, p.After.EvaluatedThrough);
        Assert.Equal(resumedAt.AddMinutes(1), p.After.NextDueAt);

        var next = Plan(Apply(s, p))!;
        Assert.False(next.ClearsPendingPolicy);
        Assert.Equal(T.AddMinutes(-4), Assert.Single(next.Occurrences).ScheduledAt);
        Assert.Equal(T, next.After.EvaluatedThrough);
    }

    [Fact]
    [Trait("Case", "SCHED-15")]
    public void A_one_time_schedule_runs_once_late_or_on_time_and_is_then_exhausted()
    {
        var due = T.AddMinutes(-3);
        var d = Definition(x => { x.Cron = null; x.DueAt = due; });
        var late = Plan(Snapshot(d, due.AddMilliseconds(-1), next: due))!;
        var o = Assert.Single(late.Occurrences);
        Assert.Equal((due, T), (o.ScheduledAt, o.CoveredThrough!.Value));
        Assert.Null(late.After.NextDueAt);

        // Catch-up on a one-time schedule behaves as run-once (13 §5.5).
        var catchUp = Definition(x => { x.Cron = null; x.DueAt = due; x.Misfire = MisfirePolicy.CatchUp; });
        Assert.Null(Plan(Snapshot(catchUp, due.AddMilliseconds(-1), next: due))!.After.Episode);

        var skipped = Plan(Snapshot(Definition(x => { x.Cron = null; x.DueAt = due; x.Misfire = MisfirePolicy.Skip; }), due.AddMilliseconds(-1), next: due))!;
        Assert.Empty(skipped.Occurrences);

        var onTime = Plan(Snapshot(d, due.AddMilliseconds(-1), next: due), due.AddSeconds(1))!;
        Assert.Equal(due, Assert.Single(onTime.Occurrences).ScheduledAt);
        Assert.Null(onTime.After.NextDueAt);
        Assert.Null(Plan(Apply(Snapshot(d, due.AddMilliseconds(-1), next: due), onTime)));
    }

    [Fact]
    [Trait("Case", "SCHED-13")]
    public void Plans_carry_the_revisions_they_were_made_against_and_never_rewind()
    {
        var s = Snapshot(Definition(), T.AddMinutes(-2)) with { DefinitionRevision = 3, ControlRevision = 4, CursorRevision = 5 };
        var p = Plan(s)!;
        Assert.Equal((s.ScheduleId, 3L, 4L, 5L, T), (p.ScheduleId, p.ExpectedDefinitionRevision, p.ExpectedControlRevision, p.ExpectedCursorRevision, p.Cutoff));
        Assert.All(p.Occurrences, o => Assert.True(o.ScheduledAt > s.EvaluatedThrough));
        Assert.True(p.After.EvaluatedThrough >= s.EvaluatedThrough);
        Assert.Equal(p.Occurrences.Count, p.Occurrences.Select(o => o.ProposedJobId).Distinct().Count());
    }
}
