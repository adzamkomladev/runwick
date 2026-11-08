using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Contract.Tests.ControlContractTests;

namespace Runwick.Contract.Tests;

public sealed record OtherQueueJob(string Id) : IQueueJob;

/// <summary>
/// Management reads, RecordDisclosure/1 and ManagementDispatch/1 (16 §2–§3, §6; 19 §6.2; 20 §7) on both stores, through the
/// transport-free management session: the store halves of the AspNetCore MGMT-03/09/13 cases.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class ManagementContractTests
{
    public static TheoryData<StoreKind> Kinds => CoreContractTests.Kinds;

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static readonly ManagementCaller Admin = new(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);

    static readonly ManagementCaller Scoped = new(new ManagementActor("scoped", null, "test"),
        [.. ManagementPermissions.QueueScoped.Select(p => new ManagementGrant(p, new HashSet<string>(StringComparer.Ordinal) { "default" }))]);

    static void Jobs(RunwickBuilder r)
    {
        TestJobs.Register(r);
        r.AddJob<OtherQueueJob>().Named("test.other").ToQueue("other");
    }

    static IManagementSession Session(TestClient c) => c.Services.GetRequiredService<IManagementSession>();

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-03")]
    public async Task Reads_page_by_keyset_and_a_queue_scoped_caller_sees_only_its_queues(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "mgmtread", Jobs);
        await using var _ = f;
        var resource = (await c.Dispatcher.DispatchAsync(new ResourceJob("r", "res"), cancellationToken: Ct)).JobId.Value;
        var jobs = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            jobs.Add((await c.Dispatcher.DispatchAsync(new PingJob("p" + i), cancellationToken: Ct)).JobId.Value);
        }

        var other = (await c.Dispatcher.DispatchAsync(new OtherQueueJob("o"), cancellationToken: Ct)).JobId.Value;
        var s = Session(c);

        // Keyset pages: newest first, no overlap, no total.
        var seen = new List<Guid>();
        string? cursor = null;
        do
        {
            var page = await s.ListJobsAsync(Admin, new JobListQuery(Limit: 2, Cursor: cursor), Ct);
            Assert.True(page.Items.Count <= 2);
            seen.AddRange(page.Items.Select(j => j.JobId));
            cursor = page.Next;
        }
        while (cursor is not null);
        Assert.Equal(5, seen.Distinct().Count());
        Assert.Contains(other, seen);

        var scoped = await s.ListJobsAsync(Scoped, new JobListQuery(Limit: 50), Ct);
        Assert.DoesNotContain(other, scoped.Items.Select(j => j.JobId));
        Assert.Equal(4, scoped.Items.Count);
        Assert.Equal(["default"], (await s.ListQueuesAsync(Scoped, cancellationToken: Ct)).Items.Select(q => q.Name));
        Assert.Equal(["default", "other"], (await s.ListQueuesAsync(Admin, cancellationToken: Ct)).Items.Select(q => q.Name));
        await Assert.ThrowsAnyAsync<RunwickException>(() => s.RevealPayloadAsync(Scoped, other, cancellationToken: Ct));

        // Filters, detail, scopes (with live permits), workers.
        Assert.Equal(3, (await s.ListJobsAsync(Admin, new JobListQuery(JobName: "test.ping"), Ct)).Items.Count);
        var running = await RunAsync(c, resource);
        var lease = await RunAsync(c, jobs[0]);
        await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMinutes(5),
            error: new AttemptError("boom", "System.Exception", "secret message", null));
        var detail = await s.GetJobAsync(Admin, jobs[0], Ct);
        Assert.Equal((JobStates.Retrying, 1), (detail.Job.State, detail.Job.ExecutionCount == 1 ? 1 : 0));
        Assert.Equal("initial", Assert.Single(detail.Generations).CreatedReason);
        var attempt = Assert.Single(detail.Attempts);
        Assert.Equal(("retryable_failure", "boom"), (attempt.Outcome, attempt.ErrorCode));
        Assert.Single((await s.ListJobsAsync(Admin, new JobListQuery(State: "retrying"), Ct)).Items);

        var scope = Assert.Single((await s.ListScopesAsync(Admin, "resource", cancellationToken: Ct)).Items);
        Assert.Equal(("res", 1, 1), (scope.Key, scope.Capacity, scope.LivePermits));
        await c.Driver.PublishPresenceAsync(c.Application, new PresenceRecord(c.WorkerInstanceId, 1, 1, "ready", Environment.ProcessId,
            "host", "build", ["default"], new Dictionary<string, int> { ["main"] = 1 }, c.Contracts, TimeSpan.FromSeconds(30)), Ct);
        var worker = Assert.Single((await s.ListWorkersAsync(Admin, cancellationToken: Ct)).Items);
        Assert.Equal((c.WorkerInstanceId, "host"), (worker.InstanceId, worker.HostName));
        Assert.Empty((await s.ListSchedulesAsync(Admin, cancellationToken: Ct)).Items);
        await c.FinishAsync(running, AttemptOutcome.Succeeded);

        // Reveal: one audit row each, and the stored values come back.
        var payload = await s.RevealPayloadAsync(Admin, jobs[1], "ticket", Ct);
        Assert.Equal("p1", payload.Payload!["id"]!.GetValue<string>());
        Assert.NotNull(payload.Headers!["business"]);
        var error = await s.RevealErrorAsync(Admin, attempt.AttemptId, cancellationToken: Ct);
        Assert.Equal(("boom", "secret message"), (error.Error!["code"]!.GetValue<string>(), error.Error["message"]!.GetValue<string>()));
        await Assert.ThrowsAsync<ManagementNotFoundException>(() => s.RevealPayloadAsync(Admin, Guid.NewGuid(), cancellationToken: Ct));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'RevealPayload' AND target_id = @t AND reason = 'ticket'",
            ("t", jobs[1].ToString("D"))));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'RevealError' AND target_kind = 'attempt'"));

        // An expired detail is detail_expired, with no audit row.
        var cancel = await Controls(c).CancelAsync(c.Command(new CancelRequest(jobs[2], 1, (await PositionAsync(c, jobs[2])).Revision, Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, cancel.Status);
        await Exec(f, "UPDATE runwick.jobs SET detail_expired_at = terminal_at WHERE job_id = @j", ("j", jobs[2]));
        await Assert.ThrowsAsync<ManagementDetailExpiredException>(() => s.RevealPayloadAsync(Admin, jobs[2], cancellationToken: Ct));
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation IN ('RevealPayload', 'RevealError')"));

        // The audit list: newest first, filtered by target.
        var audit = await s.ListAuditAsync(Admin, new AuditQuery(), Ct);
        Assert.Equal(["Cancel", "RevealError", "RevealPayload"], audit.Items.Select(a => a.Operation));
        Assert.Equal(jobs[2].ToString("D"), Assert.Single((await s.ListAuditAsync(Admin, new AuditQuery(TargetKind: "job", TargetId: jobs[2].ToString("D")), Ct)).Items).TargetId);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Category", "Fault")]
    [Trait("Case", "MGMT-09")]
    public async Task A_failing_disclosure_audit_reveals_nothing(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "mgmt09");
        await using var _ = f;
        var job = (await c.Dispatcher.DispatchAsync(new PingJob("payload-secret"), cancellationToken: Ct)).JobId.Value;

        if (kind == StoreKind.Redis)
        {
            return; // The audit row and the read are one intent on Redis; the failing-write form is RedisControlTests (REDIS-12).
        }

        var reveal = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = "RecordDisclosure", Step = 1 }, times: 100);
        await Assert.ThrowsAsync<StoreUnavailableException>(() => Session(c).RevealPayloadAsync(Admin, job, cancellationToken: Ct));
        Assert.True(reveal.Fired >= 1);
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.management_audit"));
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    [Trait("Case", "MGMT-13")]
    public async Task Dispatch_by_name_creates_one_job_with_one_audit_row_and_replays(StoreKind kind)
    {
        var (f, c) = await OpenAsync(kind, "mgmt13", Jobs);
        await using var _ = f;
        var s = Session(c);
        var id = Guid.NewGuid();
        ManagementDispatchRequest Ping(Guid commandId, string v) =>
            new(commandId, "test.ping", System.Text.Json.Nodes.JsonNode.Parse($$"""{"id":"{{v}}"}"""), Reason: "ops ticket");

        var first = await s.DispatchAsync(Admin, Ping(id, "a"), Ct);
        Assert.Equal(("created", false, "default", 1, id), (first.Disposition, first.Replayed, first.Queue, first.Generation, first.CommandId));
        Assert.Equal(ManagementSession.DispatchJobId(id), first.JobId);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c AND operation = 'Dispatch' AND target_kind = 'job' AND reason = 'ops ticket'", ("c", id)));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c AND operation = 'ManagementDispatch'", ("c", id)));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE command_id = @c AND event_kind = 'dispatched' AND actor_id = 'admin'", ("c", id)));

        var again = await s.DispatchAsync(Admin, Ping(id, "a"), Ct);
        Assert.Equal((true, first.JobId), (again.Replayed, again.JobId));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", id)));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.jobs"));
        var conflict = await Assert.ThrowsAnyAsync<RunwickException>(() => s.DispatchAsync(Admin, Ping(id, "b"), Ct));
        Assert.Equal("command_conflict", conflict.Code);

        // Two command IDs for one active key: the second reuses the job, and both are audited.
        ManagementDispatchRequest Keyed(string v) =>
            new(Guid.NewGuid(), "test.keyed", System.Text.Json.Nodes.JsonNode.Parse($$"""{"id":"{{v}}","key":"same"}"""));
        var k1 = await s.DispatchAsync(Admin, Keyed("k1"), Ct);
        var k2 = await s.DispatchAsync(Admin, Keyed("k2"), Ct);
        Assert.Equal(("created", "reused", k1.JobId), (k1.Disposition, k2.Disposition, k2.JobId));
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'Dispatch' AND details->>'jobName' = 'test.keyed'"));

        // A queue-scoped grant covers only its queue.
        await Assert.ThrowsAnyAsync<RunwickException>(() =>
            s.DispatchAsync(Scoped, new(Guid.NewGuid(), "test.other", System.Text.Json.Nodes.JsonNode.Parse("""{"id":"o"}""")), Ct));
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.jobs"));

        if (kind == StoreKind.Redis)
        {
            return; // Dispatch, event actor and audit row are one intent on Redis (RedisControlTests covers a failing write).
        }

        // The dispatch core writes (1), the event gets its actor (2), then the audit row (3): the audit insert raises.
        var audit = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = Operations.ManagementDispatch, Step = 3 }, times: 100);
        await Assert.ThrowsAnyAsync<Exception>(() => s.DispatchAsync(Admin, Ping(Guid.NewGuid(), "z"), Ct));
        Assert.True(audit.Fired >= 1);
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.jobs"));
        Assert.Equal(3, await Count(f, "SELECT count(*) FROM runwick.management_audit"));
    }
}
