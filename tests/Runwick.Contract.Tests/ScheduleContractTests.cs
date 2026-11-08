using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Engine;
using Runwick.Harness.Faults;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;

namespace Runwick.Contract.Tests;

/// <summary>The job every schedule contract case materializes.</summary>
public sealed record ScheduleTickJob(string Name) : IQueueJob;

/// <summary>Records what scheduled handlers saw in <c>IJobContext.Schedule</c>.</summary>
public sealed class ScheduleTickProbe
{
    public ConcurrentQueue<(string Name, JobScheduleInfo? Schedule)> Seen { get; } = new();
}

public sealed class ScheduleTickHandler(ScheduleTickProbe probe, IJobContext context) : IJobHandler<ScheduleTickJob>
{
    public Task HandleAsync(ScheduleTickJob job, CancellationToken cancellationToken)
    {
        probe.Seen.Enqueue((job.Name, context.Schedule));
        return Task.CompletedTask;
    }
}

/// <summary>
/// Schedules on both stores (13 §6, 20 §7): SCH-01 … SCH-05, SCHED-09/10/12/13/15, the template tuple (PGX-02 schedule half),
/// CMD-01/02/03 on the schedule controls, ISO-01, evaluation failure and revalidation, catch-up resolution, and the hosted
/// scheduler end to end. Ported from Runwick.Postgres.Tests ScheduleStoreTests; admin SQL is written once against
/// PostgreSQL names and mapped to the SQLite tables (<c>runwick.x</c> → <c>runwick_x</c>, instants as epoch ms).
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class ScheduleContractTests
{
    static readonly ControlActor Actor = new("user:ops", null, "host", "schedules");

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static void Register(RunwickBuilder r) => r.AddJob<ScheduleTickJob>().Named("test.tick").ToQueue("default");

    static string Q(DriverFixture f, string sql) => f.Kind == StoreKind.Sqlite ? sql.Replace("runwick.", "runwick_", StringComparison.Ordinal) : sql;

    /// <summary>An instant as the store's admin SQL binds it (epoch ms for SQLite and Redis).</summary>
    static object At(DriverFixture f, DateTimeOffset t) => f.Kind == StoreKind.Postgres ? t : t.ToUnixTimeMilliseconds();

    /// <summary>Redis answers the same fixed statements from its keys (<see cref="RedisAdmin"/>).</summary>
    static Task<long> Count(DriverFixture f, string sql, params (string, object)[] p) =>
        f.Kind == StoreKind.Redis ? RedisAdmin.CountAsync(f, sql, p) : f.CountAsync(Q(f, sql), p);

    static Task Exec(DriverFixture f, string sql, params (string, object)[] p) =>
        f.Kind == StoreKind.Redis ? RedisAdmin.ExecAsync(f, sql, p) : f.ExecAsync(Q(f, sql), p);

    /// <summary>Cases that arm store fault points or barriers (PostgreSQL and SQLite); Redis covers them with the write-prefix cases (REDIS-01).</summary>
    public static TheoryData<StoreKind> FaultKinds => [.. CoreContractTests.Kinds.Select(k => (StoreKind)k.Data).Where(k => k != StoreKind.Redis)];

    static Task<long> Jobs(DriverFixture f, string? application = null) =>
        Count(f, "SELECT count(*) FROM runwick.jobs WHERE application = @a", ("a", application ?? f.Application));

    static Task<long> Occurrences(DriverFixture f, string kind = "scheduled") =>
        Count(f, "SELECT count(*) FROM runwick.schedule_occurrences WHERE occurrence_kind = @kind", ("kind", kind));

    static IScheduleStore Store(TestClient c) => c.Services.GetRequiredService<IScheduleStore>();

    /// <summary>Activates capability <c>scheduler</c> with admin SQL (setup only: SQLite has no rate-control store yet).</summary>
    static Task ActivateAsync(DriverFixture f, string application) => f.Kind == StoreKind.Redis ? RedisAdmin.Activate(application, "scheduler") : Exec(f, f.Kind == StoreKind.Sqlite
        ? "UPDATE runwick.application_controls SET required_capabilities = json_set(required_capabilities, '$.scheduler', 1), revision = revision + 1 WHERE application = @a"
        : "UPDATE runwick.application_controls SET required_capabilities = required_capabilities || '{\"scheduler\":1}'::jsonb, revision = revision + 1 WHERE application = @a",
        ("a", application));

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name, bool activate = true)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        var c = await f.OpenClient(configure: Register);
        if (activate)
        {
            await ActivateAsync(f, c.Application);
        }

        return (f, c);
    }

    static ScheduleDefinitionRecord Definition(TestClient c, string key, Action<ScheduleDeclaration>? configure = null)
    {
        var declaration = new ScheduleDeclaration(key, new ScheduleTickJob(key)) { Cron = "* * * * *" };
        configure?.Invoke(declaration);
        return ScheduleCompiler.Compile(declaration, c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
    }

    static async Task<StoreReply<ScheduleRegistration>> RegisterAsync(TestClient c, string key, Action<ScheduleDeclaration>? configure = null)
    {
        var definition = Definition(c, key, configure);
        var cutoff = await Store(c).StoreNowAsync(Ct);
        var first = definition.Kind == ScheduleOperations.OneTime
            ? definition.DueAt
            : ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)?.At;
        return await Store(c).RegisterScheduleAsync(c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), Ct);
    }

    /// <summary>Moves the cursor back so exactly <paramref name="minutes"/> whole-minute occurrences are missed.</summary>
    static Task RewindAsync(DriverFixture f, string key, int minutes)
    {
        var now = DateTimeOffset.UtcNow;
        var minute = new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, TimeSpan.Zero);
        return Exec(f, """
            UPDATE runwick.schedules SET evaluated_through = @through, next_due_at = @next, scan_after_at = @scan WHERE schedule_key = @key
            """, ("through", At(f, minute.AddMinutes(-minutes))), ("next", At(f, minute.AddMinutes(1 - minutes))),
            ("scan", At(f, now.AddSeconds(-1))), ("key", key));
    }

    static async Task<ScheduleSnapshot> ReadAsync(TestClient c, string key) =>
        await Store(c).ReadScheduleAsync(c.Application, key, Ct) ?? throw new InvalidOperationException("no schedule");

    static async Task<(ScheduleSnapshot Snapshot, DateTimeOffset Cutoff, MaterializeRequest? Plan)> PlanAsync(TestClient c, string key)
    {
        var s = await ReadAsync(c, key);
        var t = await Store(c).StoreNowAsync(Ct);
        return (s, t, SchedulePlanner.Plan(s, t, ScheduleRecurrence.For(s.Definition, TzdbArtifact.Pinned, FrozenBudgetClock.Instance), Guid.CreateVersion7));
    }

    /// <summary>The planner's wall-clock budget is not under test here (as ScheduleStoreTests).</summary>
    sealed class FrozenBudgetClock : TimeProvider
    {
        public static readonly FrozenBudgetClock Instance = new();

        public override long GetTimestamp() => 0;
    }

    static Task<StoreReply<ScheduleMaterialization>> SendAsync(TestClient c, MaterializeRequest plan, Guid? id = null) =>
        Store(c).MaterializeOccurrencesAsync(c.Command(plan, id), Ct);

    static long MissedMinutes(ScheduleSnapshot s, DateTimeOffset t) =>
        (long)(new DateTimeOffset(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0, TimeSpan.Zero) - s.EvaluatedThrough!.Value).TotalMinutes;

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCHED-13")]
    public async Task Registration_is_gated_seeds_once_and_reports_drift(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "schreg", activate: false);
        await using var _ = f;
        var gated = await RegisterAsync(c, "nightly");
        Assert.Equal((StoreStatus.UnsupportedVersion, "scheduler_not_active"), (gated.Status, gated.Error!.Code));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.schedules"));

        await ActivateAsync(f, c.Application);
        Assert.Equal("created", (await RegisterAsync(c, "nightly")).Value!.Outcome);
        Assert.Equal("same", (await RegisterAsync(c, "nightly")).Value!.Outcome);
        var drift = await RegisterAsync(c, "nightly", d => d.Cron = "*/5 * * * *");
        Assert.Equal(("drift", 1L), (drift.Value!.Outcome, drift.Value.DefinitionRevision));

        var s = await ReadAsync(c, "nightly");
        Assert.True(s.NextDueAt > s.EvaluatedThrough, "the first occurrence is strictly after the registration cutoff");
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.schedule_definitions"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_registered'"));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCH-01")]
    public async Task Competing_materializations_of_one_plan_create_one_set_of_jobs(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sch01");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (s, t, a) = await PlanAsync(c, "tick");
        var b = SchedulePlanner.Plan(s, t, ScheduleRecurrence.For(s.Definition, TzdbArtifact.Pinned, TimeProvider.System), Guid.CreateVersion7)!;

        var replies = await Task.WhenAll(SendAsync(c, a!), SendAsync(c, b));
        Assert.Single(replies, r => r.Status == StoreStatus.Applied);
        Assert.Single(replies, r => (r.Status, r.Error?.Code) == (StoreStatus.VersionConflict, "revision_conflict"));
        var winner = replies.Single(r => r.Status == StoreStatus.Applied).Value!;
        Assert.Equal(winner.Occurrences.Count, await Jobs(f));
        Assert.Equal(winner.Occurrences.Count, await Occurrences(f));
        Assert.Equal(s.CursorRevision + 1, (await ReadAsync(c, "tick")).CursorRevision);
    }

    [Theory]
    [MemberData(nameof(FaultPoints))]
    [Trait("Case", "SCH-02")]
    public async Task A_fault_inside_materialization_leaves_nothing(StoreKind kind, string point)
    {
        var (f, c) = await OpenAsync(kind, "sch02");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 2);
        var (before, _, plan) = await PlanAsync(c, "tick");
        var id = Guid.CreateVersion7();
        f.ArmFault(Enum.Parse<FaultPoint>(point), FaultSpec.Throw(), FaultTarget.Command(id));

        await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(c, plan!, id));

        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await Occurrences(f));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", id)));
        var after = await ReadAsync(c, "tick");
        Assert.Equal((before.CursorRevision, before.EvaluatedThrough, before.NextDueAt), (after.CursorRevision, after.EvaluatedThrough, after.NextDueAt));
    }

    public static TheoryData<StoreKind, string> FaultPoints =>
        [.. FaultKinds.SelectMany(k => new[] { nameof(FaultPoint.AfterMutation), nameof(FaultPoint.BeforeCommit) }
            .Select(p => ((StoreKind)k.Data, p)))];

    [Theory]
    [MemberData(nameof(FaultKinds))]
    [Trait("Case", "SCH-03")]
    public async Task Pause_racing_materialization_has_one_serial_outcome(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sch03");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (s, _, plan) = await PlanAsync(c, "tick");

        // Materialize holds the lock (row lock or write lock); the pause waits on it and applies after.
        var id = Guid.CreateVersion7();
        var barrier = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Command(id));
        var materialize = SendAsync(c, plan!, id);
        await DriverFixture.WaitAtBarrier(barrier);
        var pause = Store(c).SetScheduleStateAsync(c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), Ct);
        DriverFixture.ReleaseBarrier(barrier);
        Assert.Equal(StoreStatus.Applied, (await materialize).Status);
        Assert.Equal(StoreStatus.Applied, (await pause).Status);
        Assert.Equal(plan!.Occurrences.Count, await Jobs(f));

        // The other order: a plan made before the pause is rejected, never applied on a paused row.
        await RewindAsync(f, "tick", 1);
        var stale = (await PlanAsync(c, "tick")).Plan;
        var rejected = await SendAsync(c, stale!);
        Assert.Equal((StoreStatus.InvalidState, "schedule_paused"), (rejected.Status, rejected.Error!.Code));
        Assert.Equal(plan.Occurrences.Count, await Jobs(f));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCH-04")]
    public async Task Catch_up_runs_in_batches_under_a_fixed_cutoff(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sch04");
        await using var _ = f;
        await RegisterAsync(c, "tick", d => d.Misfire = MisfirePolicy.CatchUp);
        await RewindAsync(f, "tick", 70);
        var (first, t0, _) = await PlanAsync(c, "tick");
        var missed = MissedMinutes(first, t0);

        var sizes = new List<int>();
        DateTimeOffset? cutoff = null;
        for (var pass = 0; pass < 10; pass++)
        {
            var (s, _, plan) = await PlanAsync(c, "tick");
            if (plan is null || plan.Occurrences.Count == 0 && s.Episode is null && sizes.Count > 0)
            {
                break;
            }

            Assert.Equal(StoreStatus.Applied, (await SendAsync(c, plan)).Status);
            sizes.Add(plan.Occurrences.Count);
            var after = await ReadAsync(c, "tick");
            if (after.Episode is not { } e)
            {
                break;
            }

            cutoff ??= e.Cutoff;
            Assert.Equal(cutoff, e.Cutoff);
            Assert.Equal(plan.Occurrences[^1].ScheduledAt, after.EvaluatedThrough);
        }

        Assert.Equal(new[] { 32, 32 }, sizes.Take(2));
        Assert.True(sizes.Sum() >= missed);
        Assert.Equal(sizes.Sum(), await Jobs(f));
        Assert.Null((await ReadAsync(c, "tick")).Episode);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCH-05")]
    public async Task Pruned_history_never_redispatches_below_the_high_water_mark(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sch05");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (_, _, plan) = await PlanAsync(c, "tick");
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, plan!)).Status);
        await Exec(f, "DELETE FROM runwick.schedule_occurrences");

        var after = await ReadAsync(c, "tick");
        var replay = plan! with
        {
            ExpectedCursorRevision = after.CursorRevision,
            Occurrences = [.. plan.Occurrences.Select(o => o with { ProposedJobId = Guid.CreateVersion7() })],
            After = plan.After with { EvaluatedThrough = after.EvaluatedThrough },
        };
        var rejected = await SendAsync(c, replay);
        Assert.Equal((StoreStatus.InvariantFault, "occurrence_below_high_water"), (rejected.Status, rejected.Error!.Code));
        Assert.Equal(plan.Occurrences.Count, await Jobs(f));
    }

    public static TheoryData<StoreKind, MisfirePolicy> Misfires =>
        [.. CoreContractTests.Kinds.SelectMany(k => new[] { MisfirePolicy.Skip, MisfirePolicy.RunOnce, MisfirePolicy.CatchUp }
            .Select(p => ((StoreKind)k.Data, p)))];

    [Theory]
    [MemberData(nameof(Misfires))]
    [Trait("Case", "SCHED-09")]
    public async Task Misfire_policies_cover_the_missed_range(StoreKind kind, MisfirePolicy policy)
    {
        var (f, c) = await OpenAsync(kind, "sched09");
        await using var _ = f;
        await RegisterAsync(c, "tick", d => d.Misfire = policy);
        await RewindAsync(f, "tick", 10);
        var (s, t, plan) = await PlanAsync(c, "tick");
        var missed = MissedMinutes(s, t);
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, plan!)).Status);

        Assert.Equal(policy switch { MisfirePolicy.Skip => 0, MisfirePolicy.RunOnce => 1, _ => missed }, await Jobs(f));
        var after = await ReadAsync(c, "tick");
        Assert.Null(after.Episode);
        Assert.Equal(t, after.EvaluatedThrough);
        Assert.True(after.NextDueAt > t && after.NextDueAt <= t.AddMinutes(1));
        if (policy == MisfirePolicy.RunOnce)
        {
            Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.schedule_occurrences WHERE occurrence_key = @k AND covered_through = @t",
                ("k", ScheduleOperations.OccurrenceKey(s.NextDueAt!.Value)), ("t", At(f, t))));
        }

        Assert.Equal(policy == MisfirePolicy.CatchUp ? 0 : 1, await Count(f, "SELECT count(*) FROM runwick.schedule_audit_ranges"));
    }

    public static TheoryData<StoreKind, int, bool> Limits =>
        [.. CoreContractTests.Kinds.SelectMany(k => new[] { ((StoreKind)k.Data, 0, false), ((StoreKind)k.Data, 1, true) })];

    [Theory]
    [MemberData(nameof(Limits))]
    [Trait("Case", "SCHED-10")]
    public async Task An_episode_of_exactly_the_limit_completes_and_one_more_needs_review(StoreKind kind, int extra, bool review)
    {
        var (f, c) = await OpenAsync(kind, "sched10");
        await using var _ = f;
        await RegisterAsync(c, "tick", d => d.Misfire = MisfirePolicy.CatchUp);
        await RewindAsync(f, "tick", 40 + extra);
        var (s0, t0, _) = await PlanAsync(c, "tick");
        var limit = (int)MissedMinutes(s0, t0) - extra;
        await Exec(f, "UPDATE runwick.schedule_definitions SET max_episode_jobs = @limit", ("limit", limit));

        for (var pass = 0; pass < 5; pass++)
        {
            var (s, _, plan) = await PlanAsync(c, "tick");
            if (s.NeedsReview || plan is null || (pass > 0 && s.Episode is null))
            {
                break;
            }

            Assert.Equal(StoreStatus.Applied, (await SendAsync(c, plan)).Status);
        }

        var after = await ReadAsync(c, "tick");
        Assert.Equal(limit, await Jobs(f));
        Assert.Equal(review, after.NeedsReview);
        Assert.Equal(review ? "catch_up_limit" : null, after.ReviewReason);
        Assert.Equal(review, after.Episode is not null);
        if (!review)
        {
            return;
        }

        // ResolveCatchUp (13 §6.6): skip closes the episode with one audit range and clears the review.
        var resolved = await Store(c).ResolveCatchUpAsync(c.Command(new ResolveCatchUpRequest("tick", after.Episode!.Id, "skip", 0,
            after.Episode.Cutoff.AddMinutes(1), Guid.CreateVersion7(), Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, resolved.Status);
        var closed = await ReadAsync(c, "tick");
        Assert.Equal((false, (ScheduleEpisode?)null, after.CursorRevision + 1), (closed.NeedsReview, closed.Episode, closed.CursorRevision));
        Assert.True(closed.EvaluatedThrough >= after.Episode.Cutoff);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.schedule_audit_ranges WHERE reason = 'catch_up_skipped'"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'ResolveCatchUp'"));
    }

    [Theory]
    [MemberData(nameof(FaultKinds))]
    [Trait("Case", "SCHED-12")]
    public async Task A_lost_materialization_reply_keeps_the_same_command(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sched12");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (_, _, plan) = await PlanAsync(c, "tick");
        var command = c.Command(plan!);
        f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(command.CommandId));

        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => Store(c).MaterializeOccurrencesAsync(command, Ct));
        var resent = await Store(c).MaterializeOccurrencesAsync(command, Ct);

        Assert.True(resent.Replayed);
        Assert.Equal(plan!.Occurrences.Select(o => (Guid?)o.ProposedJobId), resent.Value!.Occurrences.Select(o => o.JobId));
        Assert.Equal(plan.Occurrences.Count, await Jobs(f));
        var conflict = await Store(c).MaterializeOccurrencesAsync(c.Command(plan with { Cutoff = plan.Cutoff.AddMilliseconds(1) }, command.CommandId), Ct);
        Assert.Equal(StoreStatus.CommandConflict, conflict.Status);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCHED-13")]
    public async Task An_edit_cuts_over_forward_and_never_rewinds(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sched13");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, (await PlanAsync(c, "tick")).Plan!)).Status);
        await RewindAsync(f, "tick", 1);
        var (before, _, stale) = await PlanAsync(c, "tick");

        var edited = Definition(c, "tick", d => d.Cron = "*/5 * * * *");
        var cutoff = await Store(c).StoreNowAsync(Ct);
        var first = ScheduleRecurrence.For(edited, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)!.Value.At;
        var update = await Store(c).UpdateScheduleAsync(c.Command(new UpdateScheduleRequest("tick", edited, before.DefinitionRevision, cutoff, first, Actor)), Ct);
        Assert.Equal(("updated", 2L), (update.Value!.Outcome, update.Value.DefinitionRevision));

        var after = await ReadAsync(c, "tick");
        Assert.True(after.EvaluatedThrough >= before.EvaluatedThrough);
        Assert.Equal(first, after.NextDueAt);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.schedule_audit_ranges WHERE reason = 'skipped_by_edit'"));
        if (stale is { Occurrences.Count: > 0 })
        {
            Assert.Equal(StoreStatus.VersionConflict, (await SendAsync(c, stale)).Status);
        }

        var same = await Store(c).UpdateScheduleAsync(c.Command(new UpdateScheduleRequest("tick", edited, 2, cutoff, first, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'UpdateSchedule'"));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCHED-15")]
    public async Task Manual_triggers_are_idempotent_and_ignore_the_cursor(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "sched15");
        await using var _ = f;
        await RegisterAsync(c, "tick", d => d.Overlap = ScheduleOverlap.Serialize);
        var s = await ReadAsync(c, "tick");
        Assert.Equal(StoreStatus.Applied, (await Store(c).SetScheduleStateAsync(
            c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), Ct)).Status);

        var triggerId = Guid.CreateVersion7();
        var first = await Store(c).TriggerScheduleAsync(c.Command(new TriggerScheduleRequest("tick", 1, triggerId, Guid.CreateVersion7(), Actor)), Ct);
        var second = await Store(c).TriggerScheduleAsync(c.Command(new TriggerScheduleRequest("tick", 1, triggerId, Guid.CreateVersion7(), Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, first.Status);
        Assert.Equal(first.Value!.Occurrences[0].JobId, second.Value!.Occurrences[0].JobId);
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await Occurrences(f, "manual"));

        var after = await ReadAsync(c, "tick");
        Assert.Equal((s.CursorRevision, s.EvaluatedThrough, true), (after.CursorRevision, after.EvaluatedThrough, after.Paused));
        Assert.Equal(1, await Count(f, """
            SELECT count(*) FROM runwick.job_concurrency_requirements AS r
            JOIN runwick.concurrency_scopes AS s USING (application, scope_id)
            WHERE s.scope_kind = 'schedule' AND s.scope_key = @id AND s.capacity = 1
            """, ("id", s.ScheduleId.ToString("D"))));

        // Resume is refused while a catch-up episode is unresolved.
        await Exec(f, "UPDATE runwick.schedules SET episode_id = @e, episode_cutoff = @t, episode_emitted = 0, episode_limit = 10",
            ("e", Guid.NewGuid()), ("t", At(f, DateTimeOffset.UtcNow)));
        var resume = await Store(c).SetScheduleStateAsync(
            c.Command(new ScheduleStateRequest("tick", false, ScheduleOperations.ResumeSkipPaused, after.ControlRevision, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "episode_unresolved"), (resume.Status, resume.Error!.Code));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "PGX-02")]
    public async Task Scheduled_jobs_carry_the_template_profile_tuple(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "pgx02s");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, (await PlanAsync(c, "tick")).Plan!)).Status);

        Assert.Equal(0, await Count(f, """
            SELECT count(*) FROM runwick.jobs AS j
            JOIN runwick.schedule_occurrences AS o ON o.application = j.application AND o.job_id = j.job_id
            JOIN runwick.schedules AS s ON s.application = o.application AND s.schedule_id = o.schedule_id
            JOIN runwick.schedule_definitions AS d
              ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
            WHERE (j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version)
               <> (d.job_name, d.payload_version, d.serializer_profile_id, d.serializer_profile_version)
            """));
        Assert.Equal(await Jobs(f), await Count(f, "SELECT count(*) FROM runwick.job_generations WHERE created_reason = 'scheduled'"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_materialized'"));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CMD-01")]
    [Trait("Case", "CMD-02")]
    [Trait("Case", "CMD-03")]
    public async Task Schedule_controls_follow_the_receipt_protocol(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "schcmd");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        var s = await ReadAsync(c, "tick");

        // CMD-01: one command, concurrent and sequential sends, applies once.
        var pause = c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor));
        var replies = await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => Store(c).SetScheduleStateAsync(pause, Ct)));
        Assert.All(replies, r => Assert.Equal(StoreStatus.Applied, r.Status));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetScheduleState'"));

        // CMD-02: same ID, changed body.
        var changed = await Store(c).SetScheduleStateAsync(pause with { Body = pause.Body with { Actor = Actor with { Reason = "other" } } }, Ct);
        Assert.Equal(StoreStatus.CommandConflict, changed.Status);

        // CMD-03: lost reply, then the same command resolves to the committed result.
        var resume = c.Command(new ScheduleStateRequest("tick", false, ScheduleOperations.MisfireRunOnce, s.ControlRevision + 1, Actor));
        if (kind != StoreKind.Redis)
        {
            f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(resume.CommandId));
            await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => Store(c).SetScheduleStateAsync(resume, Ct));
        }
        else
        {
            // No store fault points on Redis (its lost-reply cases are REDIS-01): a plain first send, then the resend below replays.
            Assert.Equal(StoreStatus.Applied, (await Store(c).SetScheduleStateAsync(resume, Ct)).Status);
        }

        var resolved = await Store(c).SetScheduleStateAsync(resume, Ct);
        Assert.True(resolved.Replayed);
        Assert.False(resolved.Value!.Paused);
        Assert.Equal(ScheduleOperations.MisfireRunOnce, (await ReadAsync(c, "tick")).ResumePolicy);

        // Stale revision and no-change are rejections: no audit, no receipt.
        var stale = await Store(c).SetScheduleStateAsync(c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), Ct);
        Assert.Equal(StoreStatus.VersionConflict, stale.Status);
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetScheduleState'"));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCHED-11")]
    public async Task An_evaluation_failure_needs_review_until_revalidated(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "schrev");
        await using var _ = f;
        await RegisterAsync(c, "tick");
        var s = await ReadAsync(c, "tick");

        var failed = await Store(c).RecordEvaluationFailureAsync(c.Command(new EvaluationFailureRequest(s.ScheduleId, 1, "profile_missing", "x", Actor)), Ct);
        Assert.Equal((StoreStatus.Applied, true), (failed.Status, failed.Value!.NeedsReview));
        var again = await Store(c).RecordEvaluationFailureAsync(c.Command(new EvaluationFailureRequest(s.ScheduleId, 1, "profile_missing", null, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (again.Status, again.Error!.Code));
        Assert.Empty((await Store(c).ReadDueSchedulesAsync(c.Application, 32, Ct)).Schedules);

        var stale = await Store(c).RevalidateScheduleAsync(c.Command(new RevalidateScheduleRequest("tick", 2, Actor)), Ct);
        Assert.Equal(StoreStatus.VersionConflict, stale.Status);
        var revalidated = await Store(c).RevalidateScheduleAsync(c.Command(new RevalidateScheduleRequest("tick", 1, Actor)), Ct);
        Assert.Equal((StoreStatus.Applied, false), (revalidated.Status, revalidated.Value!.NeedsReview));
        var after = await ReadAsync(c, "tick");
        Assert.Equal((false, (string?)null, s.CursorRevision), (after.NeedsReview, after.ReviewReason, after.CursorRevision));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_needs_review'"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_revalidated'"));
        Assert.Equal(2, await Count(f, """
            SELECT count(*) FROM runwick.management_audit WHERE operation IN ('RecordScheduleEvaluationFailure', 'RevalidateSchedule')
            """));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "ISO-01")]
    public async Task The_same_schedule_key_in_two_applications_never_interacts(StoreKind kind)
    {
        var (f, a) = await OpenAsync(kind, "schiso");
        await using var _ = f;
        var b = await f.OpenClient(configure: Register, application: f.Application + "_b");
        await ActivateAsync(f, b.Application);
        await RegisterAsync(a, "tick");
        await RegisterAsync(b, "tick");
        await RewindAsync(f, "tick", 1);

        Assert.Equal(StoreStatus.Applied, (await SendAsync(a, (await PlanAsync(a, "tick")).Plan!)).Status);
        var sa = await ReadAsync(a, "tick");
        var sb = await ReadAsync(b, "tick");
        Assert.NotEqual(sa.ScheduleId, sb.ScheduleId);
        Assert.Equal((2L, 1L), (sa.CursorRevision, sb.CursorRevision));
        Assert.Equal(0, await Jobs(f, b.Application));
        Assert.True(await Jobs(f, a.Application) >= 1);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "SCHED-09")]
    public async Task The_hosted_scheduler_seeds_materializes_and_jobs_see_their_occurrence(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "schhost");
        await using var _ = f;
        var probe = new ScheduleTickProbe();
        await using var w = WorkerHost.Create(f.ClientConnectionString("sched"), f.Application, f.Faults,
            services: sc => sc.AddSingleton(probe),
            configure: r =>
            {
                Register(r);
                r.AddHandler<ScheduleTickJob, ScheduleTickHandler>();
                r.AddSchedule("tick", new ScheduleTickJob("tick")).Cron("* * * * *");
                r.AddScheduler(o =>
                {
                    o.PollInterval = TimeSpan.FromMilliseconds(200);
                    o.IdlePollInterval = TimeSpan.FromSeconds(1);
                });
            });
        await w.StartAsync();
        var seeded = await ReadAsync(c, "tick");
        Assert.Equal(1L, seeded.DefinitionRevision);
        Assert.True(seeded.NextDueAt > seeded.EvaluatedThrough);

        // Ten missed minutes (run-once default) become one job covering through the cutoff, and its handler sees it.
        await RewindAsync(f, "tick", 10);
        await WorkerHost.WaitUntil(() => !probe.Seen.IsEmpty, TimeSpan.FromSeconds(60), "the loop materializes and the job runs");
        var (name, info) = probe.Seen.First();
        Assert.Equal("tick", name);
        Assert.NotNull(info);
        Assert.Equal("tick", info.ScheduleName);
        Assert.True(info.CoveredUntil - info.ScheduledAt >= TimeSpan.FromMinutes(9), "run-once covers the missed range");
        Assert.True(Interlocked.Read(ref w.Services.GetRequiredService<SchedulerService>().Materializations) >= 1);
        await w.StopAsync(Ct);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_registered'"));
        Assert.Equal(await Occurrences(f), await Count(f, "SELECT count(*) FROM runwick.job_generations WHERE created_reason = 'scheduled'"));
    }
}
