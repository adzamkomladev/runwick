using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>Revisioned controls and audit (08 §3, §5): END-02/03, CTL-01…05, MGMT-04 store half, UNI-04/PGX-04/PGX-06 sub-parts, ADM-04, CMD-* on controls.</summary>
public sealed class ControlTests
{
    static readonly ControlActor Actor = new("user:42", "Ada", "host", "test");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<JobPosition> PositionAsync(DriverFixture f, Guid jobId)
    {
        var row = (await TextAsync(f, "SELECT generation || '/' || revision || '/' || state FROM runwick.jobs WHERE job_id = @j", ("j", jobId))).Single().Split('/');
        return new JobPosition(jobId, int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(row[1], System.Globalization.CultureInfo.InvariantCulture), row[2]);
    }

    static Task<long> Audits(DriverFixture f, Guid commandId) =>
        f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", commandId));

    static StoreCommand<CancelRequest> Cancel(TestClient c, JobPosition p, Guid? id = null, ControlActor? actor = null) =>
        c.Command(new CancelRequest(p.JobId, p.Generation, p.Revision, actor ?? Actor), id);

    static StoreCommand<RetryRequest> Retry(TestClient c, JobPosition p, Guid? id = null) =>
        c.Command(new RetryRequest(p.JobId, p.Generation, p.Revision, Actor), id);

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "END-02")]
    public async Task Cancel_of_a_waiting_job_closes_it_releases_the_active_key_and_audits_once()
    {
        await using var f = await DriverFixture.StartAsync(Ns("end02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"))).JobId.Value;
        var id = Guid.CreateVersion7();

        var reply = await c.Store.CancelAsync(Cancel(c, await PositionAsync(f, job), id), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal(("cancelled", "cancelled_while_waiting", JobStates.Cancelled), (reply.Value!.Effect, reply.Value.TerminalReason, reply.Value.Position.State));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'k1' AND bound_job_id IS NULL"));
        Assert.Equal(1, await Audits(f, id));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'job_cancelled' AND actor_id = 'user:42'"));

        // The released key admits a new job with the same key.
        var next = await c.Dispatcher.DispatchAsync(new KeyedJob("b", "k1"));
        Assert.NotEqual(job, next.JobId.Value);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "END-03")]
    public async Task Cancel_of_an_owned_job_records_the_request_only_and_precedence_closes_it()
    {
        await using var f = await DriverFixture.StartAsync(Ns("end03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new ResourceJob("a", "r1"))).JobId.Value;
        var lease = await RunAsync(c, job);

        var reply = await c.Store.CancelAsync(Cancel(c, await PositionAsync(f, job)), CancellationToken.None);
        Assert.Equal(("cancel_requested", JobStates.Running), (reply.Value!.Effect, reply.Value.Position.State));

        // Nothing released: the permit and lease stay; a renewal reports the request.
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));
        var renewed = await c.RenewAsync(lease);
        Assert.True(renewed.Value!.CancellationRequested);

        // A second request is rejected without writing.
        var again = await c.Store.CancelAsync(Cancel(c, await PositionAsync(f, job)), CancellationToken.None);
        Assert.Equal((StoreStatus.InvalidState, "cancel_already_requested"), (again.Status, again.Error!.Code));

        // Unsuccessful outcome + durable request → cancellation_requested (03 §6.4).
        var finished = await c.FinishAsync(lease, AttemptOutcome.RetryableFailure);
        Assert.Equal(("cancelled", "cancellation_requested"), (finished.Value!.Disposition, finished.Value.TerminalReason));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-03")]
    public async Task A_stale_position_is_a_conflict_with_no_change_receipt_or_audit()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ctl03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        var stale = await PositionAsync(f, job);
        await c.ReserveAsync();
        var id = Guid.CreateVersion7();

        var reply = await c.Store.CancelAsync(Cancel(c, stale, id), CancellationToken.None);
        Assert.Equal((StoreStatus.VersionConflict, "revision_conflict"), (reply.Status, reply.Error!.Code));
        Assert.Equal($"actual=1/{stale.Revision + 1}", reply.Error.Detail);
        Assert.Equal(0, await Receipts(f, id));
        Assert.Equal(0, await Audits(f, id));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_id = @j AND cancel_requested_at IS NULL", ("j", job)));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-04")]
    public async Task One_receipt_and_one_audit_per_success_none_on_replay_or_rejection()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ctl04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        var command = Cancel(c, await PositionAsync(f, job));

        var first = await c.Store.CancelAsync(command, CancellationToken.None);
        var replay = await c.Store.CancelAsync(command, CancellationToken.None);
        Assert.False(first.Replayed);
        Assert.True(replay.Replayed);
        Assert.Equal(first.Value, replay.Value);
        Assert.Equal(first.DecidedAt, replay.DecidedAt);
        Assert.Equal(1, await Receipts(f, command.CommandId));
        Assert.Equal(1, await Audits(f, command.CommandId));

        // CMD-03: the same ID with a different body (actor) is a conflict.
        var changed = command with { Body = command.Body with { Actor = Actor with { Subject = "user:43" } } };
        Assert.Equal(StoreStatus.CommandConflict, (await c.Store.CancelAsync(changed, CancellationToken.None)).Status);

        // A rejected control (terminal job) writes neither.
        var rejected = Cancel(c, await PositionAsync(f, job));
        Assert.Equal(StoreStatus.InvalidState, (await c.Store.CancelAsync(rejected, CancellationToken.None)).Status);
        Assert.Equal(0, await Receipts(f, rejected.CommandId));
        Assert.Equal(0, await Audits(f, rejected.CommandId));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "MGMT-04")]
    public async Task Audit_effect_and_receipt_commit_together_or_not_at_all()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        var command = Cancel(c, await PositionAsync(f, job));
        f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Throw(), FaultTarget.Command(command.CommandId));

        await Assert.ThrowsAnyAsync<Exception>(() => c.Store.CancelAsync(command, CancellationToken.None));
        Assert.Equal(0, await Receipts(f, command.CommandId));
        Assert.Equal(0, await Audits(f, command.CommandId));
        Assert.Equal(JobStates.Pending, (await PositionAsync(f, job)).State);

        // The same command applies afterwards, exactly once.
        Assert.Equal(StoreStatus.Applied, (await c.Store.CancelAsync(command, CancellationToken.None)).Status);
        Assert.Equal(1, await Audits(f, command.CommandId));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "UNI-04")]
    public async Task Manual_retry_creates_a_new_generation_and_reacquires_the_active_key()
    {
        await using var f = await DriverFixture.StartAsync(Ns("uni04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"))).JobId.Value;
        var lease = await RunAsync(c, job);
        await c.FinishAsync(lease, AttemptOutcome.PermanentFailure);

        var retried = await c.Store.ManualRetryAsync(Retry(c, await PositionAsync(f, job)), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, retried.Status);
        Assert.Equal(("retried", 2, JobStates.Pending), (retried.Value!.Effect, retried.Value.Position.Generation, retried.Value.Position.State));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys WHERE key = 'k1' AND bound_job_id = @j", ("j", job)));
        Assert.Equal(1, await f.CountAsync(
            "SELECT count(*) FROM runwick.job_generations WHERE job_id = @j AND generation = 2 AND created_reason = 'manual_retry' AND started_count = 0", ("j", job)));

        // The new generation runs with a fresh budget; lifetime counters continue.
        var second = await RunAsync(c, job);
        Assert.Equal(2, second.Generation);
        Assert.Equal(2, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "PGX-04")]
    public async Task Manual_retry_conflicts_when_another_job_holds_the_active_key()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pgx04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var first = (await c.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"))).JobId.Value;
        await c.Store.CancelAsync(Cancel(c, await PositionAsync(f, first)), CancellationToken.None);
        var holder = (await c.Dispatcher.DispatchAsync(new KeyedJob("b", "k1"))).JobId.Value;

        var id = Guid.CreateVersion7();
        var reply = await c.Store.ManualRetryAsync(Retry(c, await PositionAsync(f, first), id), CancellationToken.None);
        Assert.Equal((StoreStatus.UniquenessConflict, holder), (reply.Status, reply.Error!.ConflictingJobId));
        Assert.Equal(0, await Audits(f, id));
        Assert.Equal(1, (await PositionAsync(f, first)).Generation);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "PGX-06")]
    public async Task Missing_payload_blocks_manual_retry()
    {
        await using var f = await DriverFixture.StartAsync(Ns("pgx06"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        await c.Store.CancelAsync(Cancel(c, await PositionAsync(f, job)), CancellationToken.None);

        // Harness pruning stands in for retention (SP-021).
        await f.ExecAsync("UPDATE runwick.jobs SET payload_retained = false WHERE job_id = @j", ("j", job));
        await f.ExecAsync("DELETE FROM runwick.job_payloads WHERE job_id = @j", ("j", job));

        var reply = await c.Store.ManualRetryAsync(Retry(c, await PositionAsync(f, job)), CancellationToken.None);
        Assert.Equal((StoreStatus.HistoryUnavailable, "payload_unavailable"), (reply.Status, reply.Error!.Code));
        Assert.Equal(JobStates.Cancelled, (await PositionAsync(f, job)).State);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-04")]
    public async Task Rerun_applies_only_to_succeeded_jobs()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rerun"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        var pending = await PositionAsync(f, job);
        Assert.Equal(StoreStatus.InvalidState, (await c.Store.RerunSucceededAsync(Retry(c, pending), CancellationToken.None)).Status);

        await c.FinishAsync(await RunAsync(c, job), AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.InvalidState, (await c.Store.ManualRetryAsync(Retry(c, await PositionAsync(f, job)), CancellationToken.None)).Status);
        var rerun = await c.Store.RerunSucceededAsync(Retry(c, await PositionAsync(f, job)), CancellationToken.None);
        Assert.Equal(("rerun", 2), (rerun.Value!.Effect, rerun.Value.Position.Generation));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_generations WHERE job_id = @j AND created_reason = 'rerun_succeeded'", ("j", job)));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "ADM-04")]
    public async Task Public_pause_blocks_reserve_only_and_resume_restores_it()
    {
        await using var f = await DriverFixture.StartAsync(Ns("adm04"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var owned = (await c.Dispatcher.DispatchAsync(new PingJob("owned"))).JobId.Value;
        var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
        var revision = await f.CountAsync("SELECT revision FROM runwick.queue_controls WHERE queue = 'default'");

        var paused = await c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, revision, Actor)), CancellationToken.None);
        Assert.Equal((true, revision + 1), (paused.Value!.Paused, paused.Value.Revision));
        Assert.Equal("user:42", (await TextAsync(f, "SELECT updated_by FROM runwick.queue_controls WHERE queue = 'default'")).Single());

        // Dispatch and the owned attempt continue; Reserve does not.
        await c.Dispatcher.DispatchAsync(new PingJob("later"));
        Assert.Equal(StoreStatus.QueuePaused, (await c.ReserveAsync()).Status);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        Assert.Equal(JobStates.Succeeded, (await PositionAsync(f, owned)).State);

        // Same value again: no change; stale revision: conflict.
        Assert.Equal("no_change", (await c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, revision + 1, Actor)), CancellationToken.None)).Error!.Code);
        Assert.Equal(StoreStatus.VersionConflict, (await c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", false, revision, Actor)), CancellationToken.None)).Status);

        Assert.Equal(StoreStatus.Applied, (await c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", false, revision + 1, Actor)), CancellationToken.None)).Status);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE target_kind = 'queue'"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-01")]
    public async Task A_capacity_decrease_drains_without_revoking_live_permits()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ctl01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new SharedJob("a", "r"));
        await c.Dispatcher.DispatchAsync(new SharedJob("b", "r"));
        await c.Dispatcher.DispatchAsync(new SharedJob("c", "r"));
        var l1 = await RunAsync(c);
        var l2 = await RunAsync(c);
        var revision = await f.CountAsync("SELECT policy_revision FROM runwick.concurrency_scopes WHERE scope_kind = 'resource' AND scope_key = 'r'");

        var edit = await c.Store.EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "r", 1, revision, Actor)), CancellationToken.None);
        Assert.Equal((1, revision + 1, 2), (edit.Value!.Capacity, edit.Value.PolicyRevision, edit.Value.LivePermits));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits"));

        // Both holders keep running; admission stays blocked until the live count is below the new capacity.
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(l1)).Status);
        Assert.Equal(StoreStatus.CapacityBlocked, (await c.ReserveAsync()).Status);
        await c.FinishAsync(l1, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.CapacityBlocked, (await c.ReserveAsync()).Status);
        await c.FinishAsync(l2, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, (await c.ReserveAsync()).Status);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-02")]
    public async Task Membership_cannot_be_edited_and_unknown_scopes_are_rejected()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ctl02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await c.Dispatcher.DispatchAsync(new ResourceJob("a", "r1"));

        // There is no request shape for membership; naming a scope no job registered is NotFound.
        var unknown = await c.Store.EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "nobody", 3, 1, Actor)), CancellationToken.None);
        Assert.Equal((StoreStatus.NotFound, "scope_not_registered"), (unknown.Status, unknown.Error!.Code));
        var badKind = await c.Store.EditScopePolicyAsync(c.Command(new ScopePolicyRequest("tenant", "r1", 3, 1, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.InvalidRequest, badKind.Status);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_concurrency_requirements"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-05")]
    public async Task Bulk_cancel_returns_per_item_results_each_with_its_own_receipt()
    {
        await using var f = await DriverFixture.StartAsync(Ns("ctl05"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var a = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId;
        var b = (await c.Dispatcher.DispatchAsync(new PingJob("b"))).JobId;
        var control = (IJobControl)c.Services.GetService(typeof(IJobControl))!;
        var pa = await PositionAsync(f, a.Value);

        var results = await control.CancelManyAsync(
            [new JobTarget(a, pa.Generation, pa.Revision), new JobTarget(b, 1, 99), new JobTarget(new JobId(Guid.NewGuid()), 1, 1)],
            new ManagementActor("user:42", null, "host"));

        Assert.Equal([true, false, false], results.Select(r => r.Succeeded));
        Assert.Equal([null, "revision_conflict", "job_not_found"], results.Select(r => r.Code));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit"));
        Assert.Equal(JobStates.Pending, (await PositionAsync(f, b.Value)).State);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CTL-04")]
    public async Task Actor_bounds_are_validated_before_submission()
    {
        await using var f = await DriverFixture.StartAsync(Ns("actor"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        var p = await PositionAsync(f, job);
        foreach (var (actor, field) in new[]
        {
            (Actor with { Subject = "" }, "actor.subject"),
            (Actor with { Subject = new string('x', 257) }, "actor.subject"),
            (Actor with { Scheme = "bad scheme" }, "actor.scheme"),
            (Actor with { Reason = new string('r', 513) }, "reason"),
        })
        {
            var reply = await c.Store.CancelAsync(Cancel(c, p, actor: actor), CancellationToken.None);
            Assert.Equal((StoreStatus.InvalidRequest, field), (reply.Status, reply.Error!.Detail));
        }

        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Cancel'"));
    }
}
