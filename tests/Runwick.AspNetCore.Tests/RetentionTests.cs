using System.Net;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>MGMT-11: retention over HTTP (17 §7).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-021")]
public sealed class RetentionTests
{
    static readonly CleanupLimits Limits = new(100, 1 << 20, TimeSpan.FromSeconds(5));

    static async Task PassAsync(TestClient c, string recordClass)
    {
        for (var r = await c.Store.RunCleanupBatchAsync(c.Application, recordClass, Limits, CancellationToken.None); !r.PassComplete;
             r = await c.Store.RunCleanupBatchAsync(c.Application, recordClass, Limits, CancellationToken.None))
        {
            Assert.Equal("done", r.Status);
        }
    }

    static object Policy(Guid commandId, long expectedRevision, int completed = 168, int failed = 720, int history = 2160) => new
    {
        commandId, expectedRevision, completedDetailHours = completed, failedDetailHours = failed, jobHistoryHours = history, auditHours = 2160,
        scalingEventsHours = 168, scheduleOccurrencesHours = 720, workerPresenceHours = 24, reason = "tighten",
    };

    [Fact]
    [Trait("Case", "MGMT-11")]
    public async Task Retention_over_http_expiry_purge_policy_and_permissions()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt11"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = await h.DispatchAsync(new PingJob("old"));
        await f.FailNextAsync("secret text");
        await f.ExecAsync("UPDATE runwick.jobs SET terminal_at = terminal_at - interval '31 days' WHERE job_id = @j", ("j", job));
        var c = await f.OpenClient("local", MgmtHost.Jobs);
        await PassAsync(c, RecordClasses.JobDetailFailed);

        // After detail expiry: identity, state, generations and error codes stay; the reveal is 410.
        var (status, body, _) = await h.GetAsync($"jobs/{job}", Users.Admin);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.NotNull(body!["job"]!["detailExpiredAt"]);
        Assert.Equal("failed", body["job"]!["state"]!.GetValue<string>());
        Assert.Single(body["generations"]!.AsArray());
        Assert.Equal("boom", body["attempts"]![0]!["errorCode"]!.GetValue<string>());
        var reveal = await h.PostAsync($"jobs/{job}/payload:reveal", Users.Admin, new { reason = "look" });
        Assert.Equal((HttpStatusCode.Gone, "detail_expired"), (reveal.Status, reveal.Body!["code"]!.GetValue<string>()));

        // After purge: 404, the same as an ID that never existed.
        await f.ExecAsync("UPDATE runwick.jobs SET terminal_at = terminal_at - interval '90 days' WHERE job_id = @j", ("j", job));
        await PassAsync(c, RecordClasses.JobHistory);
        Assert.Equal(HttpStatusCode.NotFound, (await h.GetAsync($"jobs/{job}", Users.Admin)).Status);

        // GET retention: policy and progress; Settings.Read required.
        var view = await h.GetAsync("retention", Users.Viewer);
        Assert.Equal(HttpStatusCode.OK, view.Status);
        Assert.Equal((720, 0L), (view.Body!["policy"]!["failedDetailHours"]!.GetValue<int>(), view.Body["revision"]!.GetValue<long>()));
        Assert.Equal(RecordClasses.All.Count, view.Body["classes"]!.AsArray().Count);
        Assert.Contains(view.Body["classes"]!.AsArray(), x => x!["recordClass"]!.GetValue<string>() == "job_history" && x["totalRows"]!.GetValue<long>() == 1);
        h.Grants[Users.Nobody] = [new ManagementGrant(ManagementPermissions.JobsRead)];
        Assert.Equal(HttpStatusCode.Forbidden, (await h.GetAsync("retention", Users.Nobody)).Status);

        // POST retention/policy: 403 without Retention.Manage, 400 on an invalid order, 200 once, 409 when stale.
        Assert.Equal(HttpStatusCode.Forbidden, (await h.PostAsync("retention/policy", Users.Viewer, Policy(Guid.NewGuid(), 0))).Status);
        var invalid = await h.PostAsync("retention/policy", Users.Admin, Policy(Guid.NewGuid(), 0, history: 100));
        Assert.Equal((HttpStatusCode.BadRequest, "invalid_retention_policy"), (invalid.Status, invalid.Body!["code"]!.GetValue<string>()));
        var applied = await h.PostAsync("retention/policy", Users.Admin, Policy(Guid.NewGuid(), 0, completed: 24));
        Assert.Equal((HttpStatusCode.OK, 1L), (applied.Status, applied.Body!["revision"]!.GetValue<long>()));
        var stale = await h.PostAsync("retention/policy", Users.Admin, Policy(Guid.NewGuid(), 0, completed: 48));
        Assert.Equal((HttpStatusCode.Conflict, 1L), (stale.Status, stale.Body!["actualRevision"]!.GetValue<long>()));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'EditRetentionPolicy'"));
        Assert.Equal(24, await f.CountAsync("SELECT completed_detail_hours FROM runwick.retention_policies"));
        Assert.Equal(1L, (await h.GetAsync("settings", Users.Viewer)).Body!["retentionPolicyRevision"]!.GetValue<long>());
    }
}
