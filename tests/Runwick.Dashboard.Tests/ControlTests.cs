using System.Net;
using System.Text.Json;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

/// <summary>MGMT-04, 05, 09, 10 (server side), 11 through the dashboard's control and page paths.</summary>
[Trait("Item", "SP-024")]
public sealed class ControlTests
{
    static Task<long> ReceiptsAsync(DriverFixture f, Guid id) =>
        f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", id));

    [Fact]
    [Trait("Case", "MGMT-04")]
    public async Task A_stale_page_gets_stale_revision_and_cancel_racing_finish_has_one_transition()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dmgmt04"));
        await using var h = await DashHost.StartAsync(f);
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync("/runwick");
        // Cancel racing Finish on owned work: one control effect at most, with one receipt and one audit row.
        var racer = await h.DispatchAsync(new PingJob("race"));
        var (client, lease) = await f.RunNextAsync();
        var owned = SecurityTests.Position(await SecurityTests.JobAsync(browser, racer));
        var cancelId = Guid.NewGuid();
        var cancel = browser.PostAsync($"/runwick/jobs/{racer}/cancel", owned, cancelId);
        var finish = client.FinishAsync(lease, AttemptOutcome.Succeeded);
        await Task.WhenAll(cancel, finish);
        var applied = (await cancel).StatusCode == HttpStatusCode.SeeOther ? 1 : 0;
        if (applied == 0)
        {
            var code = (await Browser.JsonAsync(await cancel)).Props().GetProperty("outcome").GetProperty("code").GetString();
            Assert.True(code is "stale_revision" or "invalid_state", code);
        }

        Assert.Equal(StoreStatus.Applied, (await finish).Status);
        Assert.Equal((applied, applied), (await h.AuditsAsync(cancelId), await ReceiptsAsync(f, cancelId)));
        Assert.Equal("succeeded", (await SecurityTests.JobAsync(browser, racer)).GetProperty("state").GetString());

        var job = await h.DispatchAsync(new PingJob("x"));
        await f.FailNextAsync("boom", "m");
        await browser.LoadAsync($"/runwick/jobs/{job}");
        var old = await SecurityTests.JobAsync(browser, job);

        // Someone else retries first; this page's retry carries the old revision.
        var other = h.Browser(Users.Admin);
        await other.LoadAsync("/runwick");
        Assert.Equal(HttpStatusCode.SeeOther, (await other.PostAsync($"/runwick/jobs/{job}/retry", SecurityTests.Position(old), Guid.NewGuid())).StatusCode);
        var actual = (await SecurityTests.JobAsync(browser, job)).GetProperty("revision").GetInt64();
        var id = Guid.NewGuid();
        var stale = await browser.PostAsync($"/runwick/jobs/{job}/retry", new { expectedGeneration = 2, expectedRevision = old.GetProperty("revision").GetInt64() }, id);
        DeliveryTests.AssertOutcome(stale, "stale_revision", id, $"/runwick/jobs/{job}");
        Assert.Equal(actual, (await Browser.JsonAsync(stale)).Props().GetProperty("outcome").GetProperty("actualRevision").GetInt64());
        Assert.Equal(0, await h.AuditsAsync(id));
    }

    [Fact]
    [Trait("Case", "MGMT-05")]
    public async Task Try_again_after_a_dropped_reply_is_one_transition_and_a_removed_grant_is_403()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dmgmt05"));
        await using var h = await DashHost.StartAsync(f);
        var job = await h.DispatchAsync(new PingJob("x"));
        var browser = h.Browser(Users.Operator);
        await browser.LoadAsync($"/runwick/jobs/{job}");
        var position = SecurityTests.Position(await SecurityTests.JobAsync(browser, job));
        var revision = (await SecurityTests.JobAsync(browser, job)).GetProperty("revision").GetInt64();

        var id = Guid.NewGuid();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        var first = await browser.PostAsync($"/runwick/jobs/{job}/cancel", position, id);
        Assert.Equal(1, drop.Fired);
        Assert.Equal(HttpStatusCode.SeeOther, first.StatusCode); // the session resent the command and the receipt answered
        var again = await browser.PostAsync($"/runwick/jobs/{job}/cancel", position, id); // the client's "Try again": same ID, same body
        Assert.Equal(HttpStatusCode.SeeOther, again.StatusCode);
        Assert.Equal((1L, 1L), (await h.AuditsAsync(id), await ReceiptsAsync(f, id)));
        Assert.Equal(revision + 1, (await SecurityTests.JobAsync(browser, job)).GetProperty("revision").GetInt64());

        // The grant is removed: the resend is refused before the store and discloses nothing of the receipt.
        h.Grants[Users.Operator] = ManagementRoles.Viewer;
        var denied = await browser.PostAsync($"/runwick/jobs/{job}/cancel", position, id);
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
        var body = await Browser.JsonAsync(denied);
        Assert.Equal(["code", "message"], body.EnumerateObject().Select(p => p.Name).Order());
        Assert.Equal(1, await h.AuditsAsync(id));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "MGMT-09")]
    public async Task A_failing_audit_insert_shows_store_unavailable_and_reveals_nothing()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dmgmt09"));
        await using var h = await DashHost.StartAsync(f);
        var job = await h.DispatchAsync(new PingJob("payload-secret"));
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync($"/runwick/jobs/{job}");
        var before = (await SecurityTests.JobAsync(browser, job)).GetRawText();

        // Cancel of a waiting job writes the job (1), its event (2), then the audit row (3): the audit insert raises.
        var audit = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = Operations.Cancel, Step = 3 }, times: 100);
        var id = Guid.NewGuid();
        var cancel = await browser.PostAsync($"/runwick/jobs/{job}/cancel", SecurityTests.Position(await SecurityTests.JobAsync(browser, job)), id);
        Assert.True(audit.Fired >= 1);
        DeliveryTests.AssertOutcome(cancel, "store_unavailable", id, $"/runwick/jobs/{job}");
        Assert.Equal(before, (await SecurityTests.JobAsync(browser, job)).GetRawText());

        var reveal = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = "RecordDisclosure", Step = 1 }, times: 100);
        var revealed = await browser.RevealAsync($"/runwick/jobs/{job}/payload:reveal");
        Assert.True(reveal.Fired >= 1);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, revealed.StatusCode);
        Assert.DoesNotContain("payload-secret", await revealed.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        Assert.Equal(0, await h.AuditsAsync());
    }

    [Fact]
    [Trait("Case", "MGMT-10")]
    public async Task Reads_over_the_rate_are_429_with_Retry_After()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dmgmt10r"));
        await using var h = await DashHost.StartAsync(f, limits: o => (o.ReadsPerMinute, o.MutationsPerMinute, o.RevealsPerMinute) = (3, 100, 100));
        var browser = h.Browser(Users.Admin);
        HttpResponseMessage? limited = null;
        for (var i = 0; i < 20 && limited is null; i++)
        {
            var r = await browser.VisitAsync("/runwick/queues");
            if (r.StatusCode == HttpStatusCode.TooManyRequests)
            {
                limited = r;
            }
        }

        Assert.NotNull(limited);
        Assert.True(int.Parse(limited.Header("Retry-After")!, System.Globalization.CultureInfo.InvariantCulture) >= 1);
        Assert.Equal("rate_limited", (await Browser.JsonAsync(limited)).GetProperty("code").GetString());
        Assert.Equal("no-store, private", limited.Header("Cache-Control"));

        // Another principal has its own bucket.
        Assert.Equal(HttpStatusCode.OK, (await h.Browser(Users.Viewer).LoadAsync("/runwick/queues")).Response.StatusCode);
    }

    [Fact]
    [Trait("Case", "MGMT-10")]
    public async Task Bulk_over_100_items_is_400_and_lists_keep_the_200_row_and_31_day_caps()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dmgmt10c"));
        await using var h = await DashHost.StartAsync(f);
        var browser = h.Browser(Users.Admin);
        await browser.LoadAsync("/runwick");
        var items = Enumerable.Range(0, 101).Select(_ => new { jobId = Guid.NewGuid(), commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 });
        var bulk = await browser.PostAsync("/runwick/jobs:cancel", new { items });
        Assert.Equal(HttpStatusCode.BadRequest, bulk.StatusCode);
        Assert.Equal(0, await h.AuditsAsync());

        for (var i = 0; i < 205; i++)
        {
            await h.DispatchAsync(new PingJob("j" + i));
        }

        await f.ScalingEventAsync(DateTimeOffset.UtcNow.AddMinutes(-1));
        var page = (await browser.VisitJsonAsync("/runwick/jobs?limit=500")).Props().GetProperty("jobs");
        Assert.Equal(200, page.GetProperty("items").GetArrayLength());
        Assert.NotNull(page.GetProperty("next").GetString());

        var from = Uri.EscapeDataString(DateTimeOffset.UtcNow.AddDays(-40).ToString("O"));
        var to = Uri.EscapeDataString(DateTimeOffset.UtcNow.ToString("O"));
        foreach (var (path, component, prop) in new[] { ("/runwick/jobs", "Jobs", "jobs"), ("/runwick/audit", "Audit", "audit"), ("/runwick/scaling", "Scaling", "events") })
        {
            var wide = await browser.VisitJsonAsync($"{path}?from={from}&to={to}");
            Assert.Equal(component, wide.GetProperty("component").GetString());
            Assert.True(wide.Props().TryGetProperty("notice", out var notice) && notice.GetString() == "invalid_request", path + " " + wide.Props().GetRawText());
            Assert.Equal(0, wide.Props().GetProperty(prop).GetProperty("items").GetArrayLength());
        }

        Assert.Equal(1, (await browser.VisitJsonAsync("/runwick/scaling")).Props().GetProperty("events").GetProperty("items").GetArrayLength());
    }

    static readonly CleanupLimits Limits = new(100, 1 << 20, TimeSpan.FromSeconds(5));

    static async Task PassAsync(TestClient c, string recordClass)
    {
        for (var r = await c.Store.RunCleanupBatchAsync(c.Application, recordClass, Limits, CancellationToken.None); !r.PassComplete;
             r = await c.Store.RunCleanupBatchAsync(c.Application, recordClass, Limits, CancellationToken.None))
        {
            Assert.Equal("done", r.Status);
        }
    }

    static object Policy(long expectedRevision, int completed) => new
    {
        expectedRevision, completedDetailHours = completed, failedDetailHours = 720, jobHistoryHours = 2160, auditHours = 2160,
        scalingEventsHours = 168, scheduleOccurrencesHours = 720, workerPresenceHours = 24, reason = "tighten",
    };

    [Fact]
    [Trait("Case", "MGMT-11")]
    public async Task Detail_expiry_purge_and_the_retention_page()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dmgmt11"));
        await using var h = await DashHost.StartAsync(f);
        var job = await h.DispatchAsync(new PingJob("old"));
        await f.FailNextAsync("boom", "secret text");
        await f.ExecAsync("UPDATE runwick.jobs SET terminal_at = terminal_at - interval '31 days' WHERE job_id = @j", ("j", job));
        var c = await f.OpenClient("local", DashHost.Jobs);
        await PassAsync(c, RecordClasses.JobDetailFailed);

        var browser = h.Browser(Users.Admin);
        var (response, _, page) = await browser.LoadAsync($"/runwick/jobs/{job}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(JsonValueKind.String, page.Props().GetProperty("job").GetProperty("job").GetProperty("detailExpiredAt").ValueKind);
        var reveal = await browser.RevealAsync($"/runwick/jobs/{job}/payload:reveal");
        Assert.Equal(HttpStatusCode.Gone, reveal.StatusCode);
        Assert.Equal("detail_expired", (await Browser.JsonAsync(reveal)).GetProperty("code").GetString());

        await f.ExecAsync("UPDATE runwick.jobs SET terminal_at = terminal_at - interval '90 days' WHERE job_id = @j", ("j", job));
        await PassAsync(c, RecordClasses.JobHistory);
        Assert.Equal(HttpStatusCode.NotFound, (await browser.VisitAsync($"/runwick/jobs/{job}")).StatusCode);

        var retention = (await browser.VisitJsonAsync("/runwick/retention")).Props().GetProperty("retention");
        Assert.Equal(720, retention.GetProperty("policy").GetProperty("failedDetailHours").GetInt32());
        Assert.Equal(0, retention.GetProperty("revision").GetInt64());
        Assert.Equal(RecordClasses.All.Count, retention.GetProperty("classes").GetArrayLength());

        Assert.Equal(HttpStatusCode.SeeOther, (await browser.PostAsync("/runwick/retention/policy", Policy(0, 24), Guid.NewGuid())).StatusCode);
        var id = Guid.NewGuid();
        var stale = await browser.PostAsync("/runwick/retention/policy", Policy(0, 48), id);
        // 17 §1 decision 28: the management code for a stale policy edit is stale_revision (the store's reply stays revision_conflict).
        DeliveryTests.AssertOutcome(stale, "stale_revision", id, "/runwick/retention");
        Assert.Equal(1, (await Browser.JsonAsync(stale)).Props().GetProperty("outcome").GetProperty("actualRevision").GetInt64());
        Assert.Equal(24, await f.CountAsync("SELECT completed_detail_hours FROM runwick.retention_policies"));

        var viewer = h.Browser(Users.Viewer);
        await viewer.LoadAsync("/runwick/retention");
        Assert.Equal(HttpStatusCode.Forbidden, (await viewer.PostAsync("/runwick/retention/policy", Policy(1, 12), Guid.NewGuid())).StatusCode);
    }
}
