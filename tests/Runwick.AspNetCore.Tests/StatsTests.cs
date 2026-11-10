using System.Net;
using System.Text.Json.Nodes;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>
/// JST-06: the aggregate reads and bulk retry over HTTP (16 §2.4, §2.6, §2.7): camelCase field names as the dashboard reads them
/// (a null value is left out, as everywhere in the API), query parsing, and invalid input as 400.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "JST")]
public sealed class StatsTests
{
    static void Fields(JsonNode? node, params string[] names)
    {
        var o = Assert.IsType<JsonObject>(node);
        foreach (var name in names)
        {
            Assert.True(o.ContainsKey(name), $"missing {name} in {o.ToJsonString()}");
        }
    }

    [Fact]
    [Trait("Case", "JST-06")]
    public async Task Stats_job_types_and_bulk_retry_answer_in_the_dashboard_shape()
    {
        await using var f = await DriverFixture.StartAsync(Ns("jst06"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = (await h.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: TestContext.Current.CancellationToken)).JobId.Value;

        var (status, stats, _) = await h.GetAsync("jobs/stats?buckets=12&byQueue=true&byJobName=true&queue=default", Users.Admin);
        Assert.Equal(HttpStatusCode.OK, status);
        Fields(stats, "from", "to", "observedAt", "states", "retries", "duration", "buckets", "now", "oldestPendingEligibleAt", "queues", "jobNames", "errorCodes");
        Fields(stats!["duration"], "count");
        Fields(stats["now"], "pending", "scheduled", "running", "reserved");
        Assert.Equal(12, stats["buckets"]!.AsArray().Count);
        Fields(stats["buckets"]![0], "from", "created", "succeeded", "failed", "cancelled");
        var queue = stats["queues"]!.AsArray().Single()!;
        Fields(queue, "key", "states", "retries", "duration", "lastAt", "lastState", "lastJobId", "buckets", "now", "oldestPendingEligibleAt");
        Assert.Equal((job, 1L), (queue["lastJobId"]!.GetValue<Guid>(), stats["states"]!["pending"]!.GetValue<long>()));
        Assert.Equal("test.ping", stats["jobNames"]![0]!["key"]!.GetValue<string>());

        var (_, worker, _) = await h.GetAsync($"jobs/stats?worker={Guid.NewGuid()}&jobName=test.ping", Users.Admin);
        Assert.Equal(0L, worker!["states"]!["pending"]!.GetValue<long>());
        foreach (var bad in (string[])["buckets=0", "buckets=x", "byQueue=maybe", "worker=nope", "from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z"])
        {
            Assert.Equal(HttpStatusCode.BadRequest, (await h.GetAsync("jobs/stats?" + bad, Users.Admin)).Status);
        }

        var (_, types, _) = await h.GetAsync("jobtypes", Users.Admin);
        var ping = types!["items"]!.AsArray().Single(t => t!["name"]!.GetValue<string>() == "test.ping")!;
        Fields(ping, "name", "registered", "queue", "priority", "payloadVersion", "definition", "schedules", "seenInStore");
        Fields(ping["definition"], "payloadType", "maxExecutions", "retry", "rates", "resources", "unique");
        Assert.Equal(typeof(PingJob).FullName, ping["definition"]!["payloadType"]!.GetValue<string>());
        Assert.True(ping["seenInStore"]!.GetValue<bool>());

        var (_, schedules, _) = await h.GetAsync("schedules/stats", Users.Admin);
        Fields(schedules, "total", "paused", "needsReview", "occurrences", "misfires");

        var (retryStatus, retried, _) = await h.PostAsync("jobs:retry", Users.Admin, new
        {
            items = new[] { new { jobId = job, commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 } },
            reason = "bulk",
        });
        Assert.Equal(HttpStatusCode.OK, retryStatus);
        Assert.Equal("rejected", retried!["items"]![0]!["status"]!.GetValue<string>()); // a pending job is not retryable
        Assert.Equal(HttpStatusCode.BadRequest, (await h.PostAsync("jobs:retry", Users.Admin, new { items = Array.Empty<object>() })).Status);

        var (_, audit, _) = await h.GetAsync("audit/stats", Users.Admin);
        Fields(audit, "total", "operations", "actors");
        Assert.Null(audit!["denied"]);
        Assert.Equal(HttpStatusCode.Forbidden, (await h.GetAsync("audit/stats", Users.Scoped)).Status);
    }
}
