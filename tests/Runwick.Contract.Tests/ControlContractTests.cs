using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Contract.Tests;

/// <summary>
/// Revisioned controls, audit and the event cursor (08 §3, §5, §7; 20 §7) on both stores: the PostgreSQL ControlTests and
/// EventCursorTests cases as theories over <see cref="StoreKind"/>.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class ControlContractTests
{
    internal static readonly ControlActor Actor = new("user:42", "Ada", "host", "test");

    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    internal static async Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name, Action<RunwickBuilder>? jobs = null)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], kind);
        var c = await f.OpenClient(configure: jobs ?? (r => TestJobs.Register(r)));
        return (f, c);
    }

    /// <summary>Admin SQL written against the PostgreSQL schema; SQLite tables carry the <c>runwick_</c> prefix.</summary>
    internal static Task<long> Count(DriverFixture f, string sql, params (string Name, object Value)[] parameters) =>
        f.CountAsync(f.Kind == StoreKind.Sqlite ? sql.Replace("runwick.", "runwick_", StringComparison.Ordinal) : sql, parameters);

    internal static Task Exec(DriverFixture f, string sql, params (string Name, object Value)[] parameters) =>
        f.ExecAsync(f.Kind == StoreKind.Sqlite ? sql.Replace("runwick.", "runwick_", StringComparison.Ordinal) : sql, parameters);

    internal static IJobControlStore Controls(TestClient c) => c.Services.GetRequiredService<IJobControlStore>();

    internal static IManagementQueryStore Reads(TestClient c) => c.Services.GetRequiredService<IManagementQueryStore>();

    /// <summary>The job's position, through the management read (16 §6).</summary>
    internal static async Task<JobPosition> PositionAsync(TestClient c, Guid jobId)
    {
        var j = (await Reads(c).GetJobAsync(c.Application, jobId, Ct)).Value!.Job;
        return new JobPosition(jobId, j.Generation, j.Revision, j.State);
    }

    static Task<long> Audits(DriverFixture f, Guid commandId) =>
        Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", commandId));

    static Task<long> Receipts(DriverFixture f, Guid commandId) =>
        Count(f, "SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", commandId));

    static StoreCommand<CancelRequest> Cancel(TestClient c, JobPosition p, Guid? id = null, ControlActor? actor = null) =>
        c.Command(new CancelRequest(p.JobId, p.Generation, p.Revision, actor ?? Actor), id);

    static StoreCommand<RetryRequest> Retry(TestClient c, JobPosition p, Guid? id = null) =>
        c.Command(new RetryRequest(p.JobId, p.Generation, p.Revision, Actor), id);

    /// <summary>Reserves and begins the next job.</summary>
    internal static async Task<LeaseHandle> RunAsync(TestClient c, Guid? expectJob = null)
    {
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        if (expectJob is { } j)
        {
            Assert.Equal(j, reserved.Value!.Position.JobId);
        }

        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(reserved.Value!.Lease.Handle)).Reply.Status);
        return reserved.Value.Lease.Handle;
    }

    static async Task NoViolations(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "END-02")]
    public async Task Cancel_of_a_waiting_job_closes_it_releases_the_active_key_and_audits_once(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "end02");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"), cancellationToken: Ct)).JobId.Value;
        var id = Guid.CreateVersion7();

        var reply = await Controls(c).CancelAsync(Cancel(c, await PositionAsync(c, job), id), Ct);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal(("cancelled", "cancelled_while_waiting", JobStates.Cancelled), (reply.Value!.Effect, reply.Value.TerminalReason, reply.Value.Position.State));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'k1' AND bound_job_id IS NULL"));
        Assert.Equal(1, await Audits(f, id));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'job_cancelled' AND actor_id = 'user:42'"));

        var next = await c.Dispatcher.DispatchAsync(new KeyedJob("b", "k1"), cancellationToken: Ct);
        Assert.NotEqual(job, next.JobId.Value);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "END-03")]
    public async Task Cancel_of_an_owned_job_records_the_request_only_and_precedence_closes_it(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "end03");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("a", "r1"), cancellationToken: Ct)).JobId.Value;
        var lease = await RunAsync(c, job);

        var reply = await Controls(c).CancelAsync(Cancel(c, await PositionAsync(c, job)), Ct);
        Assert.Equal(("cancel_requested", JobStates.Running), (reply.Value!.Effect, reply.Value.Position.State));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.concurrency_permits"));
        Assert.True((await c.RenewAsync(lease)).Value!.CancellationRequested);

        var again = await Controls(c).CancelAsync(Cancel(c, await PositionAsync(c, job)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "cancel_already_requested"), (again.Status, again.Error!.Code));

        var finished = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure);
        Assert.Equal(("cancelled", "cancellation_requested"), (finished.Value!.Disposition, finished.Value.TerminalReason));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CTL-03")]
    public async Task A_stale_position_is_a_conflict_with_no_change_receipt_or_audit(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ctl03");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId.Value;
        var stale = await PositionAsync(c, job);
        await c.ReserveAsync();
        var id = Guid.CreateVersion7();

        var reply = await Controls(c).CancelAsync(Cancel(c, stale, id), Ct);
        Assert.Equal((StoreStatus.VersionConflict, "revision_conflict"), (reply.Status, reply.Error!.Code));
        Assert.Equal($"actual=1/{stale.Revision + 1}", reply.Error.Detail);
        Assert.Equal(0, await Receipts(f, id));
        Assert.Equal(0, await Audits(f, id));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND cancel_requested_at IS NULL", ("j", job)));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CTL-04")]
    public async Task One_receipt_and_one_audit_per_success_none_on_replay_or_rejection(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ctl04");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId.Value;
        var command = Cancel(c, await PositionAsync(c, job));

        var first = await Controls(c).CancelAsync(command, Ct);
        var replay = await Controls(c).CancelAsync(command, Ct);
        Assert.False(first.Replayed);
        Assert.True(replay.Replayed);
        Assert.Equal(first.Value, replay.Value);
        Assert.Equal(first.DecidedAt, replay.DecidedAt);
        Assert.Equal(1, await Receipts(f, command.CommandId));
        Assert.Equal(1, await Audits(f, command.CommandId));

        var changed = command with { Body = command.Body with { Actor = Actor with { Subject = "user:43" } } };
        Assert.Equal(StoreStatus.CommandConflict, (await Controls(c).CancelAsync(changed, Ct)).Status);

        var rejected = Cancel(c, await PositionAsync(c, job));
        Assert.Equal(StoreStatus.InvalidState, (await Controls(c).CancelAsync(rejected, Ct)).Status);
        Assert.Equal(0, await Receipts(f, rejected.CommandId));
        Assert.Equal(0, await Audits(f, rejected.CommandId));

        foreach (var (actor, field) in new[]
        {
            (Actor with { Subject = "" }, "actor.subject"),
            (Actor with { Subject = new string('x', 257) }, "actor.subject"),
            (Actor with { Scheme = "bad scheme" }, "actor.scheme"),
            (Actor with { Reason = new string('r', 513) }, "reason"),
        })
        {
            var invalid = await Controls(c).CancelAsync(Cancel(c, await PositionAsync(c, job), actor: actor), Ct);
            Assert.Equal((StoreStatus.InvalidRequest, field), (invalid.Status, invalid.Error!.Detail));
        }

        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Category", "Fault")]
    [Trait("Case", "MGMT-04")]
    public async Task Audit_effect_and_receipt_commit_together_or_not_at_all(StoreKind kind)
    {
        if (kind == StoreKind.Redis)
        {
            return; // Redis stores the whole decision first and finishes it on resend; RedisControlTests covers every write prefix.
        }

        var (f, c) = await OpenAsync(kind, "mgmt04");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId.Value;
        var command = Cancel(c, await PositionAsync(c, job));
        f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Throw(), FaultTarget.Command(command.CommandId));

        await Assert.ThrowsAnyAsync<Exception>(() => Controls(c).CancelAsync(command, Ct));
        Assert.Equal(0, await Receipts(f, command.CommandId));
        Assert.Equal(0, await Audits(f, command.CommandId));
        Assert.Equal(JobStates.Pending, (await PositionAsync(c, job)).State);

        Assert.Equal(StoreStatus.Applied, (await Controls(c).CancelAsync(command, Ct)).Status);
        Assert.Equal(1, await Audits(f, command.CommandId));

        // MGMT-09: the audit insert is the third write of a waiting cancel; if it fails nothing applies.
        var other = (await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct)).JobId.Value;
        var p = await PositionAsync(c, other);
        var audit = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = Operations.Cancel, Step = 3 }, times: 100);
        await Assert.ThrowsAnyAsync<Exception>(() => Controls(c).CancelAsync(Cancel(c, p), Ct));
        Assert.True(audit.Fired >= 1);
        Assert.Equal(p, await PositionAsync(c, other));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "UNI-04")]
    public async Task Manual_retry_creates_a_new_generation_and_reacquires_the_active_key(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "uni04");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"), cancellationToken: Ct)).JobId.Value;
        await c.FinishAsync(await RunAsync(c, job), AttemptOutcome.PermanentFailure);

        var retried = await Controls(c).ManualRetryAsync(Retry(c, await PositionAsync(c, job)), Ct);
        Assert.Equal(StoreStatus.Applied, retried.Status);
        Assert.Equal(("retried", 2, JobStates.Pending), (retried.Value!.Effect, retried.Value.Position.Generation, retried.Value.Position.State));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'k1' AND bound_job_id = @j", ("j", job)));
        Assert.Equal(1, await Count(f,
            "SELECT count(*) FROM runwick.job_generations WHERE job_id = @j AND generation = 2 AND created_reason = 'manual_retry' AND started_count = 0", ("j", job)));

        var second = await RunAsync(c, job);
        Assert.Equal(2, second.Generation);
        Assert.Equal(2, await Count(f, "SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "PGX-04")]
    public async Task Manual_retry_conflicts_when_another_job_holds_the_active_key(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "pgx04");
        await using var _ = f;
        var first = (await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"), cancellationToken: Ct)).JobId.Value;
        await Controls(c).CancelAsync(Cancel(c, await PositionAsync(c, first)), Ct);
        var holder = (await c.Dispatcher.DispatchAsync(new KeyedJob("b", "k1"), cancellationToken: Ct)).JobId.Value;

        var id = Guid.CreateVersion7();
        var reply = await Controls(c).ManualRetryAsync(Retry(c, await PositionAsync(c, first), id), Ct);
        Assert.Equal((StoreStatus.UniquenessConflict, holder), (reply.Status, reply.Error!.ConflictingJobId));
        Assert.Equal(0, await Audits(f, id));
        Assert.Equal(1, (await PositionAsync(c, first)).Generation);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "PGX-06")]
    public async Task Missing_payload_blocks_manual_retry(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "pgx06");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId.Value;
        await Controls(c).CancelAsync(Cancel(c, await PositionAsync(c, job)), Ct);

        // Harness pruning stands in for retention.
        await Exec(f, "UPDATE runwick.jobs SET payload_retained = false WHERE job_id = @j", ("j", job));
        await Exec(f, "DELETE FROM runwick.job_payloads WHERE job_id = @j", ("j", job));

        var reply = await Controls(c).ManualRetryAsync(Retry(c, await PositionAsync(c, job)), Ct);
        Assert.Equal((StoreStatus.HistoryUnavailable, "payload_unavailable"), (reply.Status, reply.Error!.Code));
        Assert.Equal(JobStates.Cancelled, (await PositionAsync(c, job)).State);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CTL-04")]
    public async Task Rerun_applies_only_to_succeeded_jobs(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "rerun");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId.Value;
        Assert.Equal(StoreStatus.InvalidState, (await Controls(c).RerunSucceededAsync(Retry(c, await PositionAsync(c, job)), Ct)).Status);

        await c.FinishAsync(await RunAsync(c, job), AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.InvalidState, (await Controls(c).ManualRetryAsync(Retry(c, await PositionAsync(c, job)), Ct)).Status);
        var rerun = await Controls(c).RerunSucceededAsync(Retry(c, await PositionAsync(c, job)), Ct);
        Assert.Equal(("rerun", 2), (rerun.Value!.Effect, rerun.Value.Position.Generation));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.job_generations WHERE job_id = @j AND created_reason = 'rerun_succeeded'", ("j", job)));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "ADM-04")]
    public async Task Public_pause_blocks_reserve_only_and_resume_restores_it(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "adm04");
        await using var _ = f;
        var owned = (await c.Dispatcher.DispatchAsync(new PingJob("owned"), cancellationToken: Ct)).JobId.Value;
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        var revision = await Count(f, "SELECT revision FROM runwick.queue_controls WHERE queue = 'default'");

        var paused = await Controls(c).SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, revision, Actor)), Ct);
        Assert.Equal((true, revision + 1), (paused.Value!.Paused, paused.Value.Revision));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_controls WHERE queue = 'default' AND updated_by = 'user:42'"));

        await c.Dispatcher.DispatchAsync(new PingJob("later"), cancellationToken: Ct);
        Assert.Equal(StoreStatus.QueuePaused, (await c.ReserveAsync()).Status);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        Assert.Equal(JobStates.Succeeded, (await PositionAsync(c, owned)).State);

        Assert.Equal("no_change", (await Controls(c).SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, revision + 1, Actor)), Ct)).Error!.Code);
        Assert.Equal(StoreStatus.VersionConflict, (await Controls(c).SetQueueStateAsync(c.Command(new QueueStateRequest("default", false, revision, Actor)), Ct)).Status);
        Assert.Equal(StoreStatus.NotFound, (await Controls(c).SetQueueStateAsync(c.Command(new QueueStateRequest("nowhere", false, 1, Actor)), Ct)).Status);

        Assert.Equal(StoreStatus.Applied, (await Controls(c).SetQueueStateAsync(c.Command(new QueueStateRequest("default", false, revision + 1, Actor)), Ct)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE target_kind = 'queue'"));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CTL-01")]
    public async Task A_capacity_decrease_drains_without_revoking_live_permits(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ctl01");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new SharedJob("a", "r"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new SharedJob("b", "r"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new SharedJob("c", "r"), cancellationToken: Ct);
        var l1 = await RunAsync(c);
        var l2 = await RunAsync(c);
        var revision = await Count(f, "SELECT policy_revision FROM runwick.concurrency_scopes WHERE scope_kind = 'resource' AND scope_key = 'r'");

        var edit = await Controls(c).EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "r", 1, revision, Actor)), Ct);
        Assert.Equal((1, revision + 1, 2), (edit.Value!.Capacity, edit.Value.PolicyRevision, edit.Value.LivePermits));
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.concurrency_permits"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE target_kind = 'scope' AND target_id = 'resource/r'"));

        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(l1)).Status);
        Assert.Equal(StoreStatus.CapacityBlocked, (await c.ReserveAsync()).Status);
        await c.FinishAsync(l1, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.CapacityBlocked, (await c.ReserveAsync()).Status);
        await c.FinishAsync(l2, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CTL-02")]
    public async Task Membership_cannot_be_edited_and_unknown_scopes_are_rejected(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ctl02");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new ResourceJob("a", "r1"), cancellationToken: Ct);

        var unknown = await Controls(c).EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "nobody", 3, 1, Actor)), Ct);
        Assert.Equal((StoreStatus.NotFound, "scope_not_registered"), (unknown.Status, unknown.Error!.Code));
        var badKind = await Controls(c).EditScopePolicyAsync(c.Command(new ScopePolicyRequest("tenant", "r1", 3, 1, Actor)), Ct);
        Assert.Equal(StoreStatus.InvalidRequest, badKind.Status);
        var same = await Controls(c).EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "r1", 1, 1, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.job_concurrency_requirements"));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "CTL-05")]
    public async Task Bulk_cancel_returns_per_item_results_each_with_its_own_receipt(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "ctl05");
        await using var _ = f;
        var a = (await c.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: Ct)).JobId;
        var b = (await c.Dispatcher.DispatchAsync(new PingJob("b"), cancellationToken: Ct)).JobId;
        var control = c.Services.GetRequiredService<IJobControl>();
        var pa = await PositionAsync(c, a.Value);

        var results = await control.CancelManyAsync(
            [new JobTarget(a, pa.Generation, pa.Revision), new JobTarget(b, 1, 99), new JobTarget(new JobId(Guid.NewGuid()), 1, 1)],
            new ManagementActor("user:42", null, "host"), cancellationToken: Ct);

        Assert.Equal([true, false, false], results.Select(r => r.Succeeded));
        Assert.Equal([null, "revision_conflict", "job_not_found"], results.Select(r => r.Code));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit"));
        Assert.Equal(JobStates.Pending, (await PositionAsync(c, b.Value)).State);
        await NoViolations(f);
    }
}

/// <summary>
/// The event cursor (08 §7, 20 §7.1). Not run in parallel with other classes: on PostgreSQL any open transaction in the
/// cluster holds the snapshot horizon, and a busy parallel run can hold it past the drain deadline.
/// </summary>
[Collection("EventCursor")]
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class EventCursorContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static IJobControlStore Controls(TestClient c) => ControlContractTests.Controls(c);

    static Task<(DriverFixture F, TestClient C)> OpenAsync(StoreKind kind, string name) => ControlContractTests.OpenAsync(kind, name);


    static async Task<List<JobEvent>> DrainAsync(TestClient c, EventCursor? from, int pageSize)
    {
        var all = new List<JobEvent>();
        var cursor = from;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (true)
        {
            var page = await Controls(c).EventsSinceAsync(c.Application, cursor, pageSize, Ct);
            Assert.True(page.Events.Count <= pageSize);
            all.AddRange(page.Events);
            if (page.Events.Count == 0)
            {
                // PostgreSQL's horizon is cluster-wide: another database's open transaction can hold it briefly (08 §7.2).
                if (page.Partial && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(50, Ct);
                    continue;
                }

                return all;
            }

            cursor = page.Next;
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "EVT-01")]
    public async Task Rereading_from_a_cursor_returns_the_same_ordered_rows(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "evt01");
        await using var _ = f;
        for (var i = 0; i < 10; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct);
        }

        var first = await DrainAsync(c, null, 3);
        var second = await DrainAsync(c, null, 4);
        Assert.Equal(10, first.Count(e => e.Kind == "dispatched"));
        Assert.Equal(first.Select(e => e.Cursor), second.Select(e => e.Cursor));
        Assert.Equal(first.OrderBy(e => e.TxId).ThenBy(e => e.EventId).Select(e => e.Cursor), first.Select(e => e.Cursor));
        Assert.All(first, e => Assert.Equal(1, e.SchemaVersion));
        if (kind is StoreKind.Sqlite or StoreKind.Redis)
        {
            Assert.All(first, e => Assert.Equal(0UL, e.TxId));
        }

        var tail = await DrainAsync(c, first[4].Cursor, 2);
        Assert.Equal(first.Skip(5).Select(e => e.EventId), tail.Select(e => e.EventId));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Controls(c).EventsSinceAsync(c.Application, null, 1001, Ct));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "EVT-02")]
    public async Task A_slow_writer_is_never_skipped_and_pages_stay_bounded(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "evt02");
        await using var _ = f;
        await c.Dispatcher.DispatchAsync(new PingJob("before"), cancellationToken: Ct);
        var start = (await DrainAsync(c, null, 100)).Last().Cursor;

        if (kind == StoreKind.Postgres)
        {
            // Writer A takes its transaction ID first and stays open; writer B commits after it.
            await using var slow = new NpgsqlConnection(f.AdminConnectionString);
            await slow.OpenAsync(Ct);
            await using var tx = await slow.BeginTransactionAsync(Ct);
            using (var insert = new NpgsqlCommand("""
                INSERT INTO runwick.queue_events (application, event_kind, occurred_at, details)
                VALUES (@a, 'queue_paused', clock_timestamp(), '{"slow":true}')
                """, slow, tx))
            {
                insert.Parameters.AddWithValue("a", c.Application);
                await insert.ExecuteNonQueryAsync(Ct);
            }

            await c.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct);
            var page = await Controls(c).EventsSinceAsync(c.Application, start, 100, Ct);
            Assert.Empty(page.Events);
            Assert.True(page.Partial);
            Assert.Equal(start, page.Next);
            await tx.CommitAsync(Ct);
        }
        else if (kind == StoreKind.Redis)
        {
            // Intents are published one at a time and a read never sees an unpublished one (22 §7.1): IDs follow publication order.
            var queue = (await ControlContractTests.Reads(c).ListQueuesAsync(c.Application, null, null, 10, Ct)).Value.Single();
            Assert.Equal(StoreStatus.Applied, (await Controls(c).SetQueueStateAsync(
                c.Command(new QueueStateRequest(queue.Name, true, queue.Revision, ControlContractTests.Actor)), Ct)).Status);
            await c.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct);
        }
        else
        {
            // One writer at a time (20 §7.1): an open writer's event is invisible until it commits, and every later
            // writer waits for it, so its ID can never fall behind a cursor.
            using var slow = new SqliteConnection($"Data Source={f.SqlitePath};Pooling=False");
            slow.Open();
            using var tx = slow.BeginTransaction(deferred: false);
            using (var insert = slow.CreateCommand())
            {
                insert.Transaction = tx;
                insert.CommandText = """
                    INSERT INTO runwick_queue_events (application, event_kind, occurred_at, details)
                    VALUES (@a, 'queue_paused', 0, '{"slow":true}')
                    """;
                insert.Parameters.AddWithValue("@a", c.Application);
                insert.ExecuteNonQuery();
            }

            var page = await Controls(c).EventsSinceAsync(c.Application, start, 100, Ct);
            Assert.Empty(page.Events);
            Assert.False(page.Partial);
            Assert.Equal(start, page.Next);
            tx.Commit();
            await c.Dispatcher.DispatchAsync(new PingJob("after"), cancellationToken: Ct);
        }

        var events = await DrainAsync(c, start, 1);
        Assert.Equal(["queue_paused", "dispatched"], events.Select(e => e.Kind));
    }
}

[CollectionDefinition("EventCursor", DisableParallelization = true)]
public sealed class EventCursorCollection;
