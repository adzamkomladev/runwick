using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>CMD-01 … CMD-05, OPS-03 and ISO-01 on the five control operations (08 §3, 15 §4).</summary>
public sealed class ControlProtocolTests
{
    static readonly ControlActor Actor = new("user:42", "Ada", "host", "test");

    static readonly ManagementActor Operator = new("user:42", "Ada", "host");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>The reply fields every control shares, so one assertion set covers all five operations.</summary>
    sealed record Sent(StoreStatus Status, bool Replayed, DateTimeOffset DecidedAt, string? Code, object? Value);

    delegate Task<Sent> Send(Guid commandId, DateTimeOffset notAfter, ControlActor actor);

    public static TheoryData<string> ControlOperations => new()
    {
        Operations.Cancel, Operations.ManualRetry, Operations.RerunSucceeded, Operations.SetQueueState, Operations.EditScopePolicy,
    };

    static async Task<Sent> Shape<T>(Task<StoreReply<T>> call) where T : class
    {
        var r = await call;
        return new Sent(r.Status, r.Replayed, r.DecidedAt, r.Error?.Code, r.Value);
    }

    static DateTimeOffset NotAfter(TimeSpan fromNow) =>
        DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + fromNow).ToUnixTimeMilliseconds());

    static async Task<JobPosition> PositionAsync(DriverFixture f, Guid jobId)
    {
        var row = (await TextAsync(f, "SELECT generation || '/' || revision || '/' || state FROM runwick.jobs WHERE job_id = @j", ("j", jobId))).Single().Split('/');
        return new JobPosition(jobId, int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(row[1], System.Globalization.CultureInfo.InvariantCulture), row[2]);
    }

    static Task<long> Audits(DriverFixture f, Guid commandId) =>
        f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", commandId));

    /// <summary>
    /// Puts a fresh target in the state <paramref name="operation"/> applies to and returns a sender for it. The job,
    /// queue or scope is chosen so the operation applies once; the second application of a different command would be
    /// rejected or <c>no_change</c>.
    /// </summary>
    static async Task<(Send Send, Guid? Job)> PrepareAsync(DriverFixture f, TestClient c, string operation)
    {
        switch (operation)
        {
            case Operations.Cancel:
            {
                var job = (await c.Dispatcher.DispatchAsync(new PingJob("cancel"))).JobId.Value;
                var p = await PositionAsync(f, job);
                Send send = (id, na, a) => Shape(c.Store.CancelAsync(c.Command(new CancelRequest(p.JobId, p.Generation, p.Revision, a), id, na), CancellationToken.None));
                return (send, job);
            }

            case Operations.ManualRetry:
            case Operations.RerunSucceeded:
            {
                var job = (await c.Dispatcher.DispatchAsync(new PingJob("again"))).JobId.Value;
                var outcome = operation == Operations.ManualRetry ? AttemptOutcome.PermanentFailure : AttemptOutcome.Succeeded;
                await c.FinishAsync(await RunAsync(c, job), outcome);
                var p = await PositionAsync(f, job);
                Send retry = (id, na, a) => Shape(c.Store.ManualRetryAsync(c.Command(new RetryRequest(p.JobId, p.Generation, p.Revision, a), id, na), CancellationToken.None));
                Send rerun = (id, na, a) => Shape(c.Store.RerunSucceededAsync(c.Command(new RetryRequest(p.JobId, p.Generation, p.Revision, a), id, na), CancellationToken.None));
                return (operation == Operations.ManualRetry ? retry : rerun, job);
            }

            case Operations.SetQueueState:
            {
                var revision = await f.CountAsync("SELECT revision FROM runwick.queue_controls WHERE application = @a AND queue = 'default'", ("a", c.Application));
                Send send = (id, na, a) => Shape(c.Store.SetQueueStateAsync(c.Command(new QueueStateRequest("default", true, revision, a), id, na), CancellationToken.None));
                return (send, null);
            }

            case Operations.EditScopePolicy:
            {
                await c.Dispatcher.DispatchAsync(new SharedJob("s", "r"));
                var revision = await f.CountAsync(
                    "SELECT policy_revision FROM runwick.concurrency_scopes WHERE application = @a AND scope_kind = 'resource' AND scope_key = 'r'", ("a", c.Application));
                Send send = (id, na, a) => Shape(c.Store.EditScopePolicyAsync(c.Command(new ScopePolicyRequest("resource", "r", 1, revision, a), id, na), CancellationToken.None));
                return (send, null);
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }
    }

    [Theory]
    [MemberData(nameof(ControlOperations))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CMD-01")]
    public async Task Same_control_concurrently_and_sequentially_applies_once(string operation)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd01c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (send, _) = await PrepareAsync(f, c, operation);
        var id = Guid.CreateVersion7();
        var notAfter = NotAfter(TimeSpan.FromMinutes(1));

        var both = await Task.WhenAll(send(id, notAfter, Actor), send(id, notAfter, Actor));
        Assert.All(both, r => Assert.Equal(StoreStatus.Applied, r.Status));
        Assert.Single(both, r => !r.Replayed);
        Assert.Single(both.Select(r => r.DecidedAt).Distinct());

        var replay = await send(id, notAfter, Actor);
        Assert.True(replay.Replayed);
        Assert.Equal(both[0].Value, replay.Value);
        Assert.Equal(1, await Receipts(f, id));
        Assert.Equal(1, await Audits(f, id));
    }

    [Theory]
    [MemberData(nameof(ControlOperations))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CMD-02")]
    public async Task Same_control_id_with_a_changed_body_or_deadline_conflicts(string operation)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd02c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (send, _) = await PrepareAsync(f, c, operation);
        var id = Guid.CreateVersion7();
        var notAfter = NotAfter(TimeSpan.FromMinutes(1));
        Assert.Equal(StoreStatus.Applied, (await send(id, notAfter, Actor)).Status);
        var pre = await f.ReadCommittedSnapshot();

        Assert.Equal(StoreStatus.CommandConflict, (await send(id, notAfter, Actor with { Subject = "user:43" })).Status);
        Assert.Equal(StoreStatus.CommandConflict, (await send(id, notAfter, Actor with { Reason = "other" })).Status);
        Assert.Equal(StoreStatus.CommandConflict, (await send(id, notAfter + TimeSpan.FromSeconds(1), Actor)).Status);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(1, await Audits(f, id));
    }

    [Theory]
    [MemberData(nameof(ControlOperations))]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CMD-03")]
    public async Task A_lost_control_reply_is_outcome_unknown_and_the_same_command_replays(string operation)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd03c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (send, _) = await PrepareAsync(f, c, operation);
        var id = Guid.CreateVersion7();
        var notAfter = NotAfter(TimeSpan.FromMinutes(1));
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));

        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => send(id, notAfter, Actor));
        Assert.Equal(1, drop.Fired);
        var replay = await send(id, notAfter, Actor);
        Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        Assert.Equal(1, await Receipts(f, id));
        Assert.Equal(1, await Audits(f, id));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CMD-03")]
    public async Task The_preview_service_resends_the_same_command_after_a_lost_reply()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd03s"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId;
        var p = await PositionAsync(f, job.Value);
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Op(Operations.Cancel));

        var control = (IJobControl)c.Services.GetService(typeof(IJobControl))!;
        var result = await control.CancelAsync(new JobTarget(job, p.Generation, p.Revision), Operator, "lost reply");

        Assert.Equal(1, drop.Fired);
        Assert.Equal((true, "cancelled", true), (result.Succeeded, result.Effect, result.Replayed));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'Cancel'"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit"));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CMD-04")]
    public async Task A_control_whose_deadline_passes_during_a_lock_wait_expires_without_effect()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd04c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("a"))).JobId.Value;
        var p = await PositionAsync(f, job);

        // An admin transaction holds the job row; Cancel waits on it past its NotAfter.
        await using var holder = new NpgsqlConnection(f.AdminConnectionString);
        await holder.OpenAsync();
        await using var tx = await holder.BeginTransactionAsync();
        using (var lockRow = new NpgsqlCommand("SELECT 1 FROM runwick.jobs WHERE job_id = @j FOR UPDATE", holder, tx))
        {
            lockRow.Parameters.AddWithValue("j", job);
            await lockRow.ExecuteNonQueryAsync();
        }

        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((await f.StoreNow() + TimeSpan.FromSeconds(2)).ToUnixTimeMilliseconds());
        var command = c.Command(new CancelRequest(p.JobId, p.Generation, p.Revision, Actor), notAfter: notAfter);
        var pending = c.Store.CancelAsync(command, CancellationToken.None);
        while (await f.StoreNow() <= notAfter)
        {
            await Task.Delay(100);
        }

        await Task.Delay(200);
        await tx.RollbackAsync();
        var expired = await WithTimeout(pending);
        Assert.Equal((StoreStatus.CommandExpired, StoreEffect.ObservedNoEffect), (expired.Status, expired.Effect));
        Assert.True(expired.DecidedAt >= notAfter);
        Assert.Equal(0, await Receipts(f, command.CommandId));
        Assert.Equal(0, await Audits(f, command.CommandId));
        Assert.Equal(JobStates.Pending, (await PositionAsync(f, job)).State);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "CMD-05")]
    public async Task A_pruned_control_receipt_replays_as_expired_and_never_reapplies()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd05c"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (send, job) = await PrepareAsync(f, c, Operations.ManualRetry);
        var id = Guid.CreateVersion7();
        var notAfter = NotAfter(TimeSpan.FromSeconds(10));
        Assert.Equal(StoreStatus.Applied, (await send(id, notAfter, Actor)).Status);
        while (await f.StoreNow() <= notAfter)
        {
            await Task.Delay(100);
        }

        Assert.True(await f.PruneReceipts("durable", DateTimeOffset.UtcNow + TimeSpan.FromDays(3650)) >= 1);
        Assert.Equal(0, await Receipts(f, id));
        var pre = await f.ReadCommittedSnapshot();

        var replay = await send(id, notAfter, Actor);
        Assert.Equal(StoreStatus.CommandExpired, replay.Status);
        Assert.Empty(pre.DiffTables(await f.ReadCommittedSnapshot()));
        Assert.Equal(2, (await PositionAsync(f, job!.Value)).Generation);
        Assert.Equal(1, await Audits(f, id));
    }

    [Theory]
    [MemberData(nameof(ControlOperations))]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "OPS-03")]
    public async Task Control_receipts_are_administrative_durable_class(string operation)
    {
        await using var f = await DriverFixture.StartAsync(Ns("ops03"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var (send, _) = await PrepareAsync(f, c, operation);

        // NotAfter beyond the 10 min cap is rejected before any effect.
        var tooLate = Guid.CreateVersion7();
        Assert.Equal(StoreStatus.InvalidRequest, (await send(tooLate, NotAfter(TimeSpan.FromMinutes(11)), Actor)).Status);
        Assert.Equal(0, await Receipts(f, tooLate));

        var id = Guid.CreateVersion7();
        Assert.Equal(StoreStatus.Applied, (await send(id, NotAfter(Operations.AdministrativeLifetime), Actor)).Status);
        Assert.Equal("durable", (await TextAsync(f, "SELECT retention_class FROM runwick.storage_commands WHERE command_id = @c", ("c", id))).Single());
        Assert.Equal(operation, (await TextAsync(f, "SELECT operation FROM runwick.storage_commands WHERE command_id = @c", ("c", id))).Single());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-011")]
    [Trait("Case", "ISO-01")]
    public async Task Controls_and_events_are_isolated_by_application()
    {
        await using var f = await DriverFixture.StartAsync(Ns("iso01c"));
        var a = await f.OpenClient(configure: r => TestJobs.Register(r));
        var b = await f.OpenClient(configure: r => TestJobs.Register(r), application: f.Application + "_b");
        var jobA = (await a.Dispatcher.DispatchAsync(new KeyedJob("a", "k1"))).JobId.Value;
        var jobB = (await b.Dispatcher.DispatchAsync(new KeyedJob("b", "k1"))).JobId.Value;
        var pa = await PositionAsync(f, jobA);

        // B cannot address A's job; the same queue name and key in B are untouched by A's controls.
        var foreign = await b.Store.CancelAsync(b.Command(new CancelRequest(pa.JobId, pa.Generation, pa.Revision, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.NotFound, foreign.Status);
        Assert.Equal(StoreStatus.Applied, (await a.Store.CancelAsync(a.Command(new CancelRequest(pa.JobId, pa.Generation, pa.Revision, Actor)), CancellationToken.None)).Status);
        var revision = await f.CountAsync("SELECT revision FROM runwick.queue_controls WHERE application = @a AND queue = 'default'", ("a", a.Application));
        Assert.Equal(StoreStatus.Applied,
            (await a.Store.SetQueueStateAsync(a.Command(new QueueStateRequest("default", true, revision, Actor)), CancellationToken.None)).Status);

        Assert.Equal(JobStates.Pending, (await PositionAsync(f, jobB)).State);
        Assert.Equal(StoreStatus.Applied, (await b.ReserveAsync()).Status);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE application = @a", ("a", b.Application)));

        var eventsB = await b.Store.EventsSinceAsync(b.Application, null, 100, CancellationToken.None);
        Assert.DoesNotContain(eventsB.Events, e => e.Kind is "job_cancelled" or "queue_paused");
        Assert.DoesNotContain(eventsB.Events, e => e.JobId == jobA);
    }
}
