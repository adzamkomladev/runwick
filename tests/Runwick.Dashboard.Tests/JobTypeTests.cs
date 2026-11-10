using System.Net;
using System.Text.Json;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using static Runwick.Dashboard.Tests.Support;

namespace Runwick.Dashboard.Tests;

/// <summary>The Jobs pages (21 §4.11): job types with exact figures, trigger a run, retry failed and cancel pending in bulk.</summary>
[Trait("Item", "SP-024")]
public sealed class JobTypeTests
{
    static JsonElement Outcome(JsonElement page) => page.Props().GetProperty("outcome");

    static int Applied(JsonElement outcome) =>
        outcome.GetProperty("items").EnumerateArray().Count(i => i.GetProperty("status").GetString() == "applied");

    [Fact]
    [Trait("Case", "DASH-JT")]
    public async Task Job_types_show_exact_figures_and_bulk_actions_act_on_every_matching_run_once()
    {
        await using var f = await DriverFixture.StartAsync(Ns("djt01"));
        await using var h = await DashHost.StartAsync(f);
        var admin = h.Browser(Users.Admin);
        await admin.LoadAsync("/runwick/jobs");
        for (var i = 0; i < 3; i++)
        {
            await h.DispatchAsync(new PingJob("p" + i));
        }

        await f.FailNextAsync("boom", "failed on purpose");

        var list = (await admin.VisitJsonAsync("/runwick/jobs")).Props();
        Assert.Contains(list.GetProperty("types").EnumerateArray(), t => t.GetProperty("name").GetString() == "test.ping");
        var group = list.GetProperty("stats").GetProperty("jobNames").EnumerateArray().Single(g => g.GetProperty("key").GetString() == "test.ping");
        Assert.Equal(1, group.GetProperty("states").GetProperty("failed").GetInt64());
        Assert.True(group.GetProperty("now").GetProperty("pending").GetInt64() == 2, group.GetRawText());

        var detail = (await admin.VisitJsonAsync("/runwick/jobs/test.ping")).Props();
        Assert.Equal("test.ping", detail.GetProperty("type").GetProperty("name").GetString());
        Assert.Equal(3, detail.GetProperty("runs").GetProperty("items").GetArrayLength());
        Assert.Equal(HttpStatusCode.NotFound, (await admin.VisitAsync("/runwick/jobs/no.such.type")).StatusCode);

        // Cancel pending: both pending runs, once; sending the request again finds nothing left to cancel.
        var cancelId = Guid.NewGuid();
        var cancelled = Outcome(await Browser.JsonAsync(await admin.PostAsync("/runwick/jobs/test.ping/cancel-pending", new { expectedCount = 2 }, cancelId)));
        Assert.True(Applied(cancelled) == 2, cancelled.GetRawText());
        var audits = await h.AuditsAsync();
        var replayed = Outcome(await Browser.JsonAsync(await admin.PostAsync("/runwick/jobs/test.ping/cancel-pending", new { expectedCount = 2 }, cancelId)));
        Assert.Equal(0, Applied(replayed)); // the cancelled runs are no longer pending, so nothing is listed or sent again
        Assert.Equal(audits, await h.AuditsAsync());

        // Retry failed: the one failed run goes back to pending.
        var retried = Outcome(await Browser.JsonAsync(await admin.PostAsync("/runwick/jobs/test.ping/retry-failed", new { expectedCount = 1, reason = "fixed" }, Guid.NewGuid())));
        Assert.True(Applied(retried) == 1, retried.GetRawText());
        var after = (await admin.VisitJsonAsync("/runwick/jobs/test.ping")).Props().GetProperty("stats");
        Assert.Equal(1, after.GetProperty("now").GetProperty("pending").GetInt64());
    }

    [Fact]
    [Trait("Case", "DASH-JT")]
    public async Task Trigger_dispatches_one_run_and_goes_to_it_and_needs_the_dispatch_grant()
    {
        await using var f = await DriverFixture.StartAsync(Ns("djt02"));
        await using var h = await DashHost.StartAsync(f);
        var admin = h.Browser(Users.Admin);
        await admin.LoadAsync("/runwick/jobs/test.ping");
        var sent = await admin.PostAsync("/runwick/jobs/test.ping/dispatch", new { payload = new { id = "from-dashboard" }, reason = "manual" }, Guid.NewGuid());
        Assert.True(sent.StatusCode == HttpStatusCode.SeeOther, $"{sent.StatusCode} {await sent.Content.ReadAsStringAsync()}");
        var location = sent.Headers.Location!.OriginalString;
        Assert.StartsWith("/runwick/runs/", location, StringComparison.Ordinal);
        var run = (await admin.VisitJsonAsync(location)).Props().GetProperty("job").GetProperty("job");
        Assert.Equal("test.ping", run.GetProperty("jobName").GetString());

        // Operators have no Jobs.Dispatch: refused before anything is written.
        var operatorBrowser = h.Browser(Users.Operator);
        await operatorBrowser.LoadAsync("/runwick/jobs/test.ping");
        var audits = await h.AuditsAsync();
        Assert.Equal(HttpStatusCode.Forbidden,
            (await operatorBrowser.PostAsync("/runwick/jobs/test.ping/dispatch", new { payload = new { id = "nope" } }, Guid.NewGuid())).StatusCode);
        Assert.Equal(audits, await h.AuditsAsync());
    }
}
