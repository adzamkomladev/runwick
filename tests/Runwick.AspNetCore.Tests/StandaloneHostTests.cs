using System.Net;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;

namespace Runwick.AspNetCore.Tests;

/// <summary>MGMT-12 (SP-020 part): a management-only host serves every read and control and writes no worker presence.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-020")]
[Trait("Case", "MGMT-12")]
public sealed class StandaloneHostTests
{
    internal static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    internal static async Task<(int Generation, long Revision, string State)> PositionAsync(MgmtHost h, Guid job)
    {
        var (status, body, _) = await h.GetAsync($"jobs/{job}", Users.Admin);
        Assert.Equal(HttpStatusCode.OK, status);
        var j = body!["job"]!;
        return (j["generation"]!.GetValue<int>(), j["revision"]!.GetValue<long>(), j["state"]!.GetValue<string>());
    }

    [Fact]
    public async Task A_management_only_host_serves_every_read_and_control_without_presence()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt12"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = await h.DispatchAsync(new PingJob("a"));

        foreach (var path in new[] { "jobs", $"jobs/{job}", "queues", "workers?includeExpired=true", "scopes", "schedules", "audit", "settings" })
        {
            var (status, body, response) = await h.GetAsync(path, Users.Admin);
            Assert.True(status == HttpStatusCode.OK, $"{path}: {status} {body}");
            Assert.Equal("no-store, private", response.Headers.CacheControl!.ToString());
            Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        }

        var settings = (await h.GetAsync("settings", Users.Admin)).Body!;
        Assert.Equal(f.Application, settings["application"]!.GetValue<string>());
        Assert.Equal(f.Application, settings["application"]!.GetValue<string>());
        Assert.DoesNotContain("worker", settings["roles"]!.AsArray().Select(r => r!.GetValue<string>()));

        var revealed = await h.PostAsync($"jobs/{job}/payload:reveal", Users.Admin, new { reason = "support" });
        Assert.Equal(HttpStatusCode.OK, revealed.Status);
        Assert.Equal("a", revealed.Body!["payload"]!["id"]!.GetValue<string>());

        var p = await PositionAsync(h, job);
        var cancelled = await h.PostAsync($"jobs/{job}/cancel", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedGeneration = p.Generation, expectedRevision = p.Revision });
        Assert.Equal(HttpStatusCode.OK, cancelled.Status);
        p = await PositionAsync(h, job);
        var retried = await h.PostAsync($"jobs/{job}/retry", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedGeneration = p.Generation, expectedRevision = p.Revision });
        Assert.Equal(HttpStatusCode.OK, retried.Status);

        var queues = (await h.GetAsync("queues", Users.Admin)).Body!["items"]!.AsArray();
        var revision = queues.Single(q => q!["name"]!.GetValue<string>() == "default")!["revision"]!.GetValue<long>();
        var paused = await h.PostAsync("queues/default/pause", Users.Admin, new { commandId = Guid.NewGuid(), expectedRevision = revision });
        Assert.Equal(HttpStatusCode.OK, paused.Status);
        var resumed = await h.PostAsync("queues/default/resume", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedRevision = paused.Body!["revision"]!.GetValue<long>() });
        Assert.Equal(HttpStatusCode.OK, resumed.Status);

        var scope = (await h.GetAsync("scopes?kind=job-type", Users.Admin)).Body!["items"]!.AsArray()
            .Single(s => s!["key"]!.GetValue<string>() == "test.capped")!;
        var capacity = await h.PostAsync("scopes/job-type/test.capped/capacity", Users.Admin,
            new { commandId = Guid.NewGuid(), capacity = 2, expectedPolicyRevision = scope["policyRevision"]!.GetValue<long>() });
        Assert.Equal(HttpStatusCode.OK, capacity.Status);

        // The schedule is registered elsewhere (a scheduler-side client); this host only controls it.
        await f.RegisterScheduleAsync("nightly");
        var schedule = (await h.GetAsync("schedules", Users.Admin)).Body!["items"]![0]!;
        var control = schedule["controlRevision"]!.GetValue<long>();
        var definition = schedule["definitionRevision"]!.GetValue<long>();
        var pausedSchedule = await h.PostAsync("schedules/nightly/pause", Users.Admin, new { commandId = Guid.NewGuid(), expectedRevision = control });
        Assert.Equal(HttpStatusCode.OK, pausedSchedule.Status);
        Assert.Equal(HttpStatusCode.OK, (await h.PostAsync("schedules/nightly/resume", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedRevision = pausedSchedule.Body!["controlRevision"]!.GetValue<long>(), policy = "skipPaused" })).Status);
        var trigger = Guid.NewGuid();
        var triggered = await h.PostAsync("schedules/nightly/trigger", Users.Admin, new { commandId = trigger, expectedRevision = definition });
        Assert.Equal(HttpStatusCode.OK, triggered.Status);
        Assert.NotNull(triggered.Body!["jobId"]);
        var again = await h.PostAsync("schedules/nightly/trigger", Users.Admin, new { commandId = trigger, expectedRevision = definition });
        Assert.Equal((true, triggered.Body["jobId"]!.GetValue<Guid>()), (again.Body!["replayed"]!.GetValue<bool>(), again.Body["jobId"]!.GetValue<Guid>()));
        Assert.Equal(HttpStatusCode.NotFound, (await h.PostAsync("schedules/missing/pause", Users.Admin,
            new { commandId = Guid.NewGuid(), expectedRevision = 1 })).Status);

        var bulk = await h.PostAsync("jobs:cancel", Users.Admin, new { items = new[] { new { jobId = Guid.NewGuid(), commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 } } });
        Assert.Equal(HttpStatusCode.OK, bulk.Status);
        Assert.Equal("not_found", bulk.Body!["items"]![0]!["status"]!.GetValue<string>());

        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.worker_instances"));
        Assert.Equal(6, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE target_kind IN ('job', 'queue', 'scope')"));
        Assert.Equal(3, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE target_kind = 'schedule'"));
    }
}
