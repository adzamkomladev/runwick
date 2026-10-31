using Runwick.Harness.Faults;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Dispatching;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;
using Runwick.Scheduling;
using Runwick.Scheduling.Evaluation;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>The job every schedule test materializes.</summary>
public sealed record TickJob(string Name) : IQueueJob;

/// <summary>
/// Schedules on the real store (13): SCH-01 … SCH-05, SCHED-09 … SCHED-13, SCHED-15, PGX-02 schedule half, CMD-01/02/03
/// for the schedule operations, ISO-01. Every-minute UTC cron; missed ranges are made by moving the cursor back.
/// </summary>
public sealed class ScheduleStoreTests
{
    static readonly ControlActor Actor = new("user:ops", null, "host", "schedules");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static void Register(RunwickBuilder r) => r.AddJob<TickJob>().Named("test.tick").ToQueue("default");

    static async Task<TestClient> OpenAsync(DriverFixture f, bool activate = true, string? application = null)
    {
        var c = await f.OpenClient(configure: Register, application: application);
        if (activate)
        {
            var reply = await c.Store.ActivateCapabilitiesAsync(
                c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Actor)), CancellationToken.None);
            Assert.Equal(StoreStatus.Applied, reply.Status);
        }

        return c;
    }

    static ScheduleDefinitionRecord Definition(TestClient c, string key, Action<ScheduleDeclaration>? configure = null)
    {
        var declaration = new ScheduleDeclaration(key, new TickJob(key)) { Cron = "* * * * *" };
        configure?.Invoke(declaration);
        return ScheduleCompiler.Compile(declaration, c.Services.GetRequiredService<DispatchPreparer>(), TzdbArtifact.Pinned);
    }

    static async Task<StoreReply<ScheduleRegistration>> RegisterAsync(TestClient c, string key, Action<ScheduleDeclaration>? configure = null)
    {
        var definition = Definition(c, key, configure);
        var cutoff = await c.Store.StoreNowAsync(CancellationToken.None);
        var first = definition.Kind == ScheduleOperations.OneTime
            ? definition.DueAt
            : ScheduleRecurrence.For(definition, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)?.At;
        return await c.Store.RegisterScheduleAsync(
            c.Command(new RegisterScheduleRequest(key, definition, cutoff, first)), CancellationToken.None);
    }

    /// <summary>Moves the cursor back so exactly <paramref name="minutes"/> whole-minute occurrences are missed.</summary>
    static Task RewindAsync(DriverFixture f, string key, int minutes) => f.ExecAsync("""
        UPDATE runwick.schedules
        SET evaluated_through = date_trunc('minute', clock_timestamp()) - make_interval(mins => @k),
            next_due_at = date_trunc('minute', clock_timestamp()) - make_interval(mins => @k - 1),
            scan_after_at = clock_timestamp() - interval '1 second'
        WHERE schedule_key = @key
        """, ("k", minutes), ("key", key));

    static async Task<ScheduleSnapshot> ReadAsync(TestClient c, string key) =>
        await c.Store.ReadScheduleAsync(c.Application, key, CancellationToken.None) ?? throw new InvalidOperationException("no schedule");

    static async Task<(ScheduleSnapshot Snapshot, DateTimeOffset Cutoff, MaterializeRequest? Plan)> PlanAsync(TestClient c, string key)
    {
        var s = await ReadAsync(c, key);
        var t = await c.Store.StoreNowAsync(CancellationToken.None);
        return (s, t, SchedulePlanner.Plan(s, t, ScheduleRecurrence.For(s.Definition, TzdbArtifact.Pinned, TimeProvider.System), Guid.CreateVersion7));
    }

    static Task<StoreReply<ScheduleMaterialization>> SendAsync(TestClient c, MaterializeRequest plan, Guid? id = null) =>
        c.Store.MaterializeOccurrencesAsync(c.Command(plan, id), CancellationToken.None);

    static long MissedMinutes(ScheduleSnapshot s, DateTimeOffset t) =>
        (long)(new DateTimeOffset(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0, TimeSpan.Zero) - s.EvaluatedThrough!.Value).TotalMinutes;

    static Task<long> Occurrences(DriverFixture f, string kind = "scheduled") =>
        f.CountAsync("SELECT count(*) FROM runwick.schedule_occurrences WHERE occurrence_kind = @kind", ("kind", kind));

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-13")]
    public async Task Registration_is_gated_seeds_once_and_reports_drift()
    {
        await using var f = await DriverFixture.StartAsync(Ns("schreg"));
        var c = await OpenAsync(f, activate: false);
        var gated = await RegisterAsync(c, "nightly");
        Assert.Equal((StoreStatus.UnsupportedVersion, "scheduler_not_active"), (gated.Status, gated.Error!.Code));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.schedules"));

        await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["scheduler"] = 1 }, Actor)), CancellationToken.None);
        var created = await RegisterAsync(c, "nightly");
        Assert.Equal("created", created.Value!.Outcome);
        Assert.Equal("same", (await RegisterAsync(c, "nightly")).Value!.Outcome);
        var drift = await RegisterAsync(c, "nightly", d => d.Cron = "*/5 * * * *");
        Assert.Equal(("drift", 1L), (drift.Value!.Outcome, drift.Value.DefinitionRevision));

        var s = await ReadAsync(c, "nightly");
        Assert.True(s.NextDueAt > s.EvaluatedThrough, "the first occurrence is strictly after the registration cutoff");
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.schedule_definitions"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'schedule_registered'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-01")]
    public async Task Competing_materializations_of_one_plan_create_one_set_of_jobs()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sch01"));
        var c = await OpenAsync(f);
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
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-02")]
    [InlineData(nameof(FaultPoint.AfterMutation))]
    [InlineData(nameof(FaultPoint.BeforeCommit))]
    public async Task A_fault_inside_materialization_leaves_nothing(string point)
    {
        await using var f = await DriverFixture.StartAsync(Ns("sch02"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 2);
        var (before, _, plan) = await PlanAsync(c, "tick");
        var id = Guid.CreateVersion7();
        f.ArmFault(Enum.Parse<FaultPoint>(point), FaultSpec.Throw(), FaultTarget.Command(id));

        await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(c, plan!, id));

        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await Occurrences(f));
        Assert.Equal(0, await Receipts(f, id));
        var after = await ReadAsync(c, "tick");
        Assert.Equal((before.CursorRevision, before.EvaluatedThrough, before.NextDueAt), (after.CursorRevision, after.EvaluatedThrough, after.NextDueAt));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-03")]
    public async Task Pause_racing_materialization_has_one_serial_outcome()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sch03"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (s, _, plan) = await PlanAsync(c, "tick");

        // Materialize holds the schedule row; the pause waits on it and applies after.
        var id = Guid.CreateVersion7();
        var barrier = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Command(id));
        var materialize = SendAsync(c, plan!, id);
        await DriverFixture.WaitAtBarrier(barrier);
        var pause = c.Store.SetScheduleStateAsync(c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), CancellationToken.None);
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

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-04")]
    public async Task Catch_up_runs_in_batches_under_a_fixed_cutoff()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sch04"));
        var c = await OpenAsync(f);
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

            var reply = await SendAsync(c, plan);
            Assert.Equal(StoreStatus.Applied, reply.Status);
            sizes.Add(plan.Occurrences.Count);
            var after = await ReadAsync(c, "tick");
            if (after.Episode is { } e)
            {
                cutoff ??= e.Cutoff;
                Assert.Equal(cutoff, e.Cutoff);
                Assert.Equal(plan.Occurrences[^1].ScheduledAt, after.EvaluatedThrough);
            }
            else
            {
                break;
            }
        }

        Assert.Equal(new[] { 32, 32 }, sizes.Take(2));
        Assert.True(sizes.Sum() >= missed);
        Assert.Equal(sizes.Sum(), await Jobs(f));
        Assert.Null((await ReadAsync(c, "tick")).Episode);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCH-05")]
    public async Task Pruned_history_never_redispatches_below_the_high_water_mark()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sch05"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (s, _, plan) = await PlanAsync(c, "tick");
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, plan!)).Status);
        await f.ExecAsync("DELETE FROM runwick.schedule_occurrences");

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

    [Theory]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-09")]
    [InlineData(MisfirePolicy.Skip)]
    [InlineData(MisfirePolicy.RunOnce)]
    [InlineData(MisfirePolicy.CatchUp)]
    public async Task Misfire_policies_cover_the_missed_range(MisfirePolicy policy)
    {
        await using var f = await DriverFixture.StartAsync(Ns("sched09"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick", d => d.Misfire = policy);
        await RewindAsync(f, "tick", 10);
        var (s, t, plan) = await PlanAsync(c, "tick");
        var missed = MissedMinutes(s, t);
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, plan!)).Status);

        var expected = policy switch { MisfirePolicy.Skip => 0, MisfirePolicy.RunOnce => 1, _ => missed };
        Assert.Equal(expected, await Jobs(f));
        var after = await ReadAsync(c, "tick");
        Assert.Null(after.Episode);
        Assert.Equal(t, after.EvaluatedThrough);
        Assert.True(after.NextDueAt > t && after.NextDueAt <= t.AddMinutes(1));
        if (policy == MisfirePolicy.RunOnce)
        {
            var covered = await TextAsync(f, "SELECT occurrence_key || '/' || (extract(epoch FROM covered_through) * 1000)::bigint FROM runwick.schedule_occurrences");
            Assert.Equal(ScheduleOperations.OccurrenceKey(s.NextDueAt!.Value) + "/" + t.ToUnixTimeMilliseconds(), Assert.Single(covered));
        }

        Assert.Equal(policy == MisfirePolicy.CatchUp ? 0 : 1, await f.CountAsync("SELECT count(*) FROM runwick.schedule_audit_ranges"));
    }

    [Theory]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-10")]
    [InlineData(0, false)]
    [InlineData(1, true)]
    public async Task An_episode_of_exactly_the_limit_completes_and_one_more_needs_review(int extra, bool review)
    {
        await using var f = await DriverFixture.StartAsync(Ns("sched10"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick", d => d.Misfire = MisfirePolicy.CatchUp);
        await RewindAsync(f, "tick", 40 + extra);
        var (s0, t0, _) = await PlanAsync(c, "tick");
        var limit = (int)MissedMinutes(s0, t0) - extra;
        await f.ExecAsync("UPDATE runwick.schedule_definitions SET max_episode_jobs = @limit", ("limit", limit));

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
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-12")]
    public async Task A_lost_materialization_reply_keeps_the_same_command()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sched12"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (_, _, plan) = await PlanAsync(c, "tick");
        var command = c.Command(plan!);
        f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(command.CommandId));

        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.Store.MaterializeOccurrencesAsync(command, CancellationToken.None));
        var resent = await c.Store.MaterializeOccurrencesAsync(command, CancellationToken.None);

        Assert.True(resent.Replayed);
        Assert.Equal(plan!.Occurrences.Select(o => (Guid?)o.ProposedJobId), resent.Value!.Occurrences.Select(o => o.JobId));
        Assert.Equal(plan.Occurrences.Count, await Jobs(f));
        var conflict = await c.Store.MaterializeOccurrencesAsync(c.Command(plan with { Cutoff = plan.Cutoff.AddMilliseconds(1) }, command.CommandId),
            CancellationToken.None);
        Assert.Equal(StoreStatus.CommandConflict, conflict.Status);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-13")]
    public async Task An_edit_cuts_over_forward_and_never_rewinds()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sched13"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        var (_, _, applied) = await PlanAsync(c, "tick");
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, applied!)).Status);
        await RewindAsync(f, "tick", 1);
        var (before, _, stale) = await PlanAsync(c, "tick");

        var edited = Definition(c, "tick", d => d.Cron = "*/5 * * * *");
        var cutoff = await c.Store.StoreNowAsync(CancellationToken.None);
        var first = ScheduleRecurrence.For(edited, TzdbArtifact.Pinned, TimeProvider.System)(cutoff)!.Value.At;
        var update = await c.Store.UpdateScheduleAsync(
            c.Command(new UpdateScheduleRequest("tick", edited, before.DefinitionRevision, cutoff, first, Actor)), CancellationToken.None);
        Assert.Equal(("updated", 2L), (update.Value!.Outcome, update.Value.DefinitionRevision));

        var after = await ReadAsync(c, "tick");
        Assert.True(after.EvaluatedThrough >= before.EvaluatedThrough);
        Assert.Equal(first, after.NextDueAt);
        if (stale is { Occurrences.Count: > 0 })
        {
            var rejected = await SendAsync(c, stale);
            Assert.Equal(StoreStatus.VersionConflict, rejected.Status);
        }

        var same = await c.Store.UpdateScheduleAsync(
            c.Command(new UpdateScheduleRequest("tick", edited, 2, cutoff, first, Actor)), CancellationToken.None);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'UpdateSchedule'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "SCHED-15")]
    public async Task Manual_triggers_are_idempotent_and_ignore_the_cursor()
    {
        await using var f = await DriverFixture.StartAsync(Ns("sched15"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick", d => d.Overlap = ScheduleOverlap.Serialize);
        var s = await ReadAsync(c, "tick");
        var paused = await c.Store.SetScheduleStateAsync(c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, paused.Status);

        var triggerId = Guid.CreateVersion7();
        var first = await c.Store.TriggerScheduleAsync(
            c.Command(new TriggerScheduleRequest("tick", 1, triggerId, Guid.CreateVersion7(), Actor)), CancellationToken.None);
        var second = await c.Store.TriggerScheduleAsync(
            c.Command(new TriggerScheduleRequest("tick", 1, triggerId, Guid.CreateVersion7(), Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, first.Status);
        Assert.Equal(first.Value!.Occurrences[0].JobId, second.Value!.Occurrences[0].JobId);
        Assert.Equal(1, await Jobs(f));
        Assert.Equal(1, await Occurrences(f, "manual"));

        var after = await ReadAsync(c, "tick");
        Assert.Equal((s.CursorRevision, s.EvaluatedThrough, true), (after.CursorRevision, after.EvaluatedThrough, after.Paused));
        Assert.Equal(1, await f.CountAsync("""
            SELECT count(*) FROM runwick.job_concurrency_requirements AS r
            JOIN runwick.concurrency_scopes AS s USING (application, scope_id)
            WHERE s.scope_kind = 'schedule' AND s.scope_key = @id AND s.capacity = 1
            """, ("id", s.ScheduleId.ToString("D"))));

        // Resume is refused while a catch-up episode is unresolved.
        await f.ExecAsync("""
            UPDATE runwick.schedules SET episode_id = gen_random_uuid(), episode_cutoff = clock_timestamp(), episode_emitted = 0, episode_limit = 10
            """);
        var resume = await c.Store.SetScheduleStateAsync(
            c.Command(new ScheduleStateRequest("tick", false, ScheduleOperations.ResumeSkipPaused, after.ControlRevision, Actor)), CancellationToken.None);
        Assert.Equal((StoreStatus.InvalidState, "episode_unresolved"), (resume.Status, resume.Error!.Code));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "PGX-02")]
    public async Task Scheduled_jobs_carry_the_template_profile_tuple()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pgx02s"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        await RewindAsync(f, "tick", 1);
        Assert.Equal(StoreStatus.Applied, (await SendAsync(c, (await PlanAsync(c, "tick")).Plan!)).Status);

        Assert.Equal(0, await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs AS j
            JOIN runwick.schedule_occurrences AS o ON o.application = j.application AND o.job_id = j.job_id
            JOIN runwick.schedules AS s ON s.application = o.application AND s.schedule_id = o.schedule_id
            JOIN runwick.schedule_definitions AS d
              ON d.application = s.application AND d.schedule_id = s.schedule_id AND d.revision = s.definition_revision
            WHERE (j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version)
               <> (d.job_name, d.payload_version, d.serializer_profile_id, d.serializer_profile_version)
            """));
        Assert.Equal(await Jobs(f), await f.CountAsync("SELECT count(*) FROM runwick.job_generations WHERE created_reason = 'scheduled'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "CMD-01")]
    public async Task Schedule_controls_follow_the_receipt_protocol()
    {
        await using var f = await DriverFixture.StartAsync(Ns("schcmd"));
        var c = await OpenAsync(f);
        await RegisterAsync(c, "tick");
        var s = await ReadAsync(c, "tick");

        // CMD-01: one command, concurrent and sequential sends, applies once.
        var pause = c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor));
        var replies = await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => c.Store.SetScheduleStateAsync(pause, CancellationToken.None)));
        Assert.All(replies, r => Assert.Equal(StoreStatus.Applied, r.Status));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetScheduleState'"));

        // CMD-02: same ID, changed body.
        var changed = await c.Store.SetScheduleStateAsync(pause with { Body = pause.Body with { Actor = Actor with { Reason = "other" } } },
            CancellationToken.None);
        Assert.Equal(StoreStatus.CommandConflict, changed.Status);

        // CMD-03: lost reply, then the same command resolves to the committed result.
        var resume = c.Command(new ScheduleStateRequest("tick", false, ScheduleOperations.MisfireRunOnce, s.ControlRevision + 1, Actor));
        f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(resume.CommandId));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.Store.SetScheduleStateAsync(resume, CancellationToken.None));
        var resolved = await c.Store.SetScheduleStateAsync(resume, CancellationToken.None);
        Assert.True(resolved.Replayed);
        Assert.False(resolved.Value!.Paused);
        var pending = await ReadAsync(c, "tick");
        Assert.Equal(ScheduleOperations.MisfireRunOnce, pending.ResumePolicy);

        // Stale revision and no-change are rejections: no audit, no receipt.
        var stale = await c.Store.SetScheduleStateAsync(c.Command(new ScheduleStateRequest("tick", true, null, s.ControlRevision, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.VersionConflict, stale.Status);
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetScheduleState'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-016")]
    [Trait("Case", "ISO-01")]
    public async Task The_same_schedule_key_in_two_applications_never_interacts()
    {
        await using var f = await DriverFixture.StartAsync(Ns("schiso"));
        var a = await OpenAsync(f);
        var b = await OpenAsync(f, application: f.Application + "_b");
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
}
