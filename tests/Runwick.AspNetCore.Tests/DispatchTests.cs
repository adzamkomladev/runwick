using System.Diagnostics;
using System.Net;
using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>MGMT-13 (audited dispatch, 19 §6.2) and the events read (19 §6.1), added by SP-032.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-032")]
public sealed class DispatchTests
{
    static object Ping(Guid commandId, string id, string? queue = null, int? payloadVersion = null) =>
        new { commandId, jobName = "test.ping", payload = new { id }, queue, payloadVersion, reason = "ops ticket" };

    static Task<long> Count(DriverFixture f, string sql, Guid id) => f.CountAsync(sql, ("c", id));

    [Fact]
    [Trait("Case", "MGMT-13")]
    public async Task Dispatch_by_name_creates_one_job_with_one_audit_row_and_replays()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt13"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var id = Guid.NewGuid();

        var first = await h.PostAsync("jobs:dispatch", Users.Admin, Ping(id, "a"));
        Assert.Equal(HttpStatusCode.OK, first.Status);
        var job = first.Body!["jobId"]!.GetValue<Guid>();
        Assert.Equal(("created", false, "default", 1), (first.Body["disposition"]!.GetValue<string>(), first.Body["replayed"]!.GetValue<bool>(),
            first.Body["queue"]!.GetValue<string>(), first.Body["generation"]!.GetValue<int>()));
        Assert.Equal(id, first.Body["commandId"]!.GetValue<Guid>());
        Assert.Equal(ManagementSession.DispatchJobId(id), job);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c AND operation = 'Dispatch' AND target_kind = 'job' AND reason = 'ops ticket'", id));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c AND operation = 'ManagementDispatch'", id));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE command_id = @c AND event_kind = 'dispatched' AND actor_id = 'admin'", id));

        // The same command again replays: no second job, audit row or receipt. Another body under the same ID conflicts.
        var again = await h.PostAsync("jobs:dispatch", Users.Admin, Ping(id, "a"));
        Assert.Equal((HttpStatusCode.OK, true, job), (again.Status, again.Body!["replayed"]!.GetValue<bool>(), again.Body["jobId"]!.GetValue<Guid>()));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", id));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
        var conflict = await h.PostAsync("jobs:dispatch", Users.Admin, Ping(id, "b"));
        Assert.Equal((HttpStatusCode.Conflict, "command_conflict"), (conflict.Status, conflict.Body!["code"]!.GetValue<string>()));

        // Uniqueness: two command IDs for one active key → the second reuses the job, and both are audited.
        var k1 = await h.PostAsync("jobs:dispatch", Users.Admin, new { commandId = Guid.NewGuid(), jobName = "test.keyed", payload = new { id = "k1", key = "same" } });
        var k2 = await h.PostAsync("jobs:dispatch", Users.Admin, new { commandId = Guid.NewGuid(), jobName = "test.keyed", payload = new { id = "k2", key = "same" } });
        Assert.Equal(("created", "reused"), (k1.Body!["disposition"]!.GetValue<string>(), k2.Body!["disposition"]!.GetValue<string>()));
        Assert.Equal(k1.Body["jobId"]!.GetValue<Guid>(), k2.Body["jobId"]!.GetValue<Guid>());
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'Dispatch' AND details->>'jobName' = 'test.keyed'"));
    }

    [Fact]
    [Trait("Case", "MGMT-13")]
    public async Task Dispatch_rejections_permissions_and_body_limits()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt13r"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);

        // Unregistered name: 404 for everyone; an unbindable payload: 400 naming the path, never the value.
        var unknown = await h.PostAsync("jobs:dispatch", Users.Admin, new { commandId = Guid.NewGuid(), jobName = "no.such", payload = new { } });
        Assert.Equal((HttpStatusCode.NotFound, "job_not_registered"), (unknown.Status, unknown.Body!["code"]!.GetValue<string>()));
        var bad = await h.PostAsync("jobs:dispatch", Users.Admin, new { commandId = Guid.NewGuid(), jobName = "test.ping", payload = new { id = new { secret = "marker-7f3" } } });
        Assert.Equal((HttpStatusCode.BadRequest, "payload_invalid"), (bad.Status, bad.Body!["code"]!.GetValue<string>()));
        Assert.Contains("$.id", bad.Body["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.DoesNotContain("marker-7f3", bad.Body.ToJsonString(), StringComparison.Ordinal);
        var version = await h.PostAsync("jobs:dispatch", Users.Admin, Ping(Guid.NewGuid(), "v", payloadVersion: 9));
        Assert.Equal((HttpStatusCode.BadRequest, "payload_version_unsupported"), (version.Status, version.Body!["code"]!.GetValue<string>()));

        // Jobs.Dispatch is in no preset; a queue-scoped grant covers only its queue.
        Assert.DoesNotContain(ManagementRoles.Administrator, g => g.Permission == ManagementPermissions.JobsDispatch);
        h.Grants["preset"] = ManagementRoles.Administrator;
        Assert.Equal(HttpStatusCode.Forbidden, (await h.PostAsync("jobs:dispatch", "preset", Ping(Guid.NewGuid(), "p"))).Status);
        Assert.Equal(HttpStatusCode.Forbidden, (await h.PostAsync("jobs:dispatch", Users.Scoped,
            new { commandId = Guid.NewGuid(), jobName = "test.other", payload = new { id = "o" } })).Status);
        Assert.Equal(HttpStatusCode.Forbidden, (await h.PostAsync("jobs:dispatch", Users.Scoped, Ping(Guid.NewGuid(), "o", queue: "other"))).Status);
        Assert.Equal(HttpStatusCode.OK, (await h.PostAsync("jobs:dispatch", Users.Scoped, Ping(Guid.NewGuid(), "d"))).Status);

        // 64 KiB < body ≤ 1 MiB + 16 KiB is accepted on this route only; a payload over 1 MiB is payload_too_large.
        var big = new string('x', 200_000);
        Assert.Equal(HttpStatusCode.OK, (await h.PostAsync("jobs:dispatch", Users.Admin, Ping(Guid.NewGuid(), big))).Status);
        var other = await h.PostAsync($"jobs/{Guid.NewGuid()}/cancel", Users.Admin, new { commandId = Guid.NewGuid(), reason = big });
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, other.Status);
        var huge = await h.PostAsync("jobs:dispatch", Users.Admin, Ping(Guid.NewGuid(), new string('x', 1_048_576)));
        Assert.Equal((HttpStatusCode.RequestEntityTooLarge, "payload_too_large"), (huge.Status, huge.Body!["code"]!.GetValue<string>()));
        Assert.Equal(2, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "MGMT-13")]
    public async Task A_failing_audit_insert_rolls_the_dispatch_back()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt13a"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);

        // The dispatch core writes (1), the event gets its actor (2), then the audit row (3): the audit insert raises.
        var audit = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = Operations.ManagementDispatch, Step = 3 }, times: 100);
        var r = await h.PostAsync("jobs:dispatch", Users.Admin, Ping(Guid.NewGuid(), "a"));
        Assert.True(audit.Fired >= 1);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, r.Status);
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.jobs"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.management_audit"));
    }

    [Fact]
    [Trait("Case", "CLI-07")]
    public async Task Events_need_application_wide_read_and_show_actors_only_with_audit_read()
    {
        await using var f = await DriverFixture.StartAsync(Ns("events"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var id = Guid.NewGuid();
        Assert.Equal(HttpStatusCode.OK, (await h.PostAsync("jobs:dispatch", Users.Admin, Ping(id, "e"))).Status);

        Assert.Equal(HttpStatusCode.Forbidden, (await h.GetAsync("events", Users.Scoped)).Status);
        bool Has(JsonNode page) => page["items"]!.AsArray().Any(e => e!["commandId"]?.GetValue<Guid>() == id);
        // Events show only below the server-wide snapshot horizon (08 §7.2), so a transaction elsewhere on the shared
        // test server can hold the new event back briefly: wait until it is visible.
        var admin = await h.GetAsync("events?limit=1000", Users.Admin);
        for (var wait = Stopwatch.StartNew(); !Has(admin.Body!) && wait.Elapsed < TimeSpan.FromSeconds(15);)
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            admin = await h.GetAsync("events?limit=1000", Users.Admin);
        }

        var viewer = await h.GetAsync("events?limit=1000", Users.Viewer);
        JsonNode Dispatched(JsonNode page) => page["items"]!.AsArray().Single(e => e!["commandId"]?.GetValue<Guid>() == id)!;
        Assert.Equal("admin", Dispatched(admin.Body!)["actorId"]!.GetValue<string>());
        Assert.Null(Dispatched(viewer.Body!)["actorId"]);

        // The cursor is a plain position; an empty page returns the input cursor; a malformed one is invalid_cursor.
        var next = admin.Body!["next"]!.GetValue<string>();
        Assert.Matches(@"^\d+\.\d+$", next);
        var empty = await h.GetAsync("events?after=" + next, Users.Admin);
        Assert.Equal((0, next), (empty.Body!["items"]!.AsArray().Count, empty.Body["next"]!.GetValue<string>()));
        var bad = await h.GetAsync("events?after=nope", Users.Admin);
        Assert.Equal((HttpStatusCode.BadRequest, "invalid_cursor"), (bad.Status, bad.Body!["code"]!.GetValue<string>()));
    }
}
