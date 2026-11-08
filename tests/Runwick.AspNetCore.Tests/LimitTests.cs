using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>MGMT-10: page and range caps, bulk size, per-principal rates, the instance limit and the principal map bound (16 §2, §4, §5).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-020")]
[Trait("Case", "MGMT-10")]
public sealed class LimitTests
{
    [Fact]
    public async Task Pages_ranges_and_bulk_are_bounded()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt10a"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        foreach (var chunk in Enumerable.Range(0, 205).Chunk(100))
        {
            await h.Dispatcher.DispatchManyAsync([.. chunk.Select(i => new BatchDispatchItem(new PingJob("p" + i)))],
                cancellationToken: TestContext.Current.CancellationToken);
        }

        var page = (await h.GetAsync("jobs?limit=500", Users.Admin)).Body!;
        Assert.Equal(200, page["items"]!.AsArray().Count);
        var rest = (await h.GetAsync("jobs?limit=500&cursor=" + page["next"]!.GetValue<string>(), Users.Admin)).Body!;
        Assert.Equal(5, rest["items"]!.AsArray().Count);
        Assert.Null(rest["next"]);
        Assert.Null(page["total"]);

        var from = Uri.EscapeDataString(DateTimeOffset.UtcNow.AddDays(-40).ToString("O"));
        Assert.Equal(HttpStatusCode.BadRequest, (await h.GetAsync($"jobs?from={from}", Users.Admin)).Status);

        var items = Enumerable.Range(0, 101).Select(_ => new { jobId = Guid.NewGuid(), commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = 1 });
        var bulk = await h.PostAsync("jobs:cancel", Users.Admin, new { items });
        Assert.Equal((HttpStatusCode.BadRequest, "invalid_request"), (bulk.Status, bulk.Body!["code"]!.GetValue<string>()));
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, (await h.PostAsync("jobs:cancel", Users.Admin, new { reason = new string('x', 70_000) })).Status);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await h.SendAsync(new HttpRequestMessage(HttpMethod.Post, h.Base + "jobs:cancel")
        {
            Headers = { { "X-Test-User", Users.Admin } },
            Content = new StringContent("items=1", System.Text.Encoding.UTF8, "application/x-www-form-urlencoded"),
        })).Status);
    }

    [Fact]
    public async Task Bursts_over_the_per_principal_rates_get_429_with_retry_after()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt10b"));
        await using var h = await MgmtHost.StartAsync(f, options: _ => { }); // the defaults: 120 reads, 30 mutations, 10 reveals per minute
        Users.Seed(h);

        async Task<int> AllowedAsync(Func<Task<(HttpStatusCode Status, System.Text.Json.Nodes.JsonNode? Body, HttpResponseMessage Response)>> send)
        {
            for (var n = 0; ; n++)
            {
                var (status, _, response) = await send();
                if (status == HttpStatusCode.TooManyRequests)
                {
                    Assert.True(int.Parse(response.Headers.GetValues("Retry-After").Single(), System.Globalization.CultureInfo.InvariantCulture) >= 1);
                    return n;
                }
            }
        }

        // Burst is a quarter of the per-minute rate: 30 reads, 7 mutations, 2 reveals.
        Assert.Equal(30, await AllowedAsync(() => h.GetAsync("settings", Users.Admin)));
        Assert.Equal(7, await AllowedAsync(() => h.PostAsync("queues/default/pause", Users.Admin, new { commandId = Guid.NewGuid(), expectedRevision = 99 })));
        Assert.Equal(2, await AllowedAsync(() => h.PostAsync($"jobs/{Guid.NewGuid()}/payload:reveal", Users.Admin)));

        // Another principal has its own buckets.
        Assert.Equal(HttpStatusCode.OK, (await h.GetAsync("settings", Users.Viewer)).Status);
    }

    [Fact]
    public async Task The_instance_limit_answers_busy_and_the_principal_map_stays_bounded()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt10c"));
        await using var h = await MgmtHost.StartAsync(f, options: o =>
        {
            (o.MaxActiveRequests, o.MaxWaitingRequests, o.MaxWait) = (1, 0, TimeSpan.FromMilliseconds(200));
            (o.ReadsPerMinute, o.MaxTrackedPrincipals) = (4, 3);
        });
        foreach (var u in new[] { "u1", "u2", "u3", "u4", "u5", "u6" })
        {
            h.Grants[u] = Management.ManagementRoles.Viewer;
        }

        await h.GetAsync("settings", "u6"); // completes the handshake before a read is held

        // One request held inside the store: the next one finds no slot and no waiting room.
        var hold = f.ArmFault(FaultPoint.BeforeGateLock, FaultSpec.Barrier, FaultTarget.Op("ManagementRead"));
        var held = h.GetAsync("jobs", "u1");
        await DriverFixture.WaitAtBarrier(hold);
        var busy = await h.GetAsync("queues", "u2");
        Assert.Equal((HttpStatusCode.ServiceUnavailable, "busy"), (busy.Status, busy.Body!["code"]!.GetValue<string>()));
        DriverFixture.ReleaseBarrier(hold);
        Assert.Equal(HttpStatusCode.OK, (await held).Status);

        // Burst 1 per principal; u6, u1 and u3 fill the map (u2 was refused before its rate); u4 and u5 share the restricted bucket.
        var limiter = h.App.Services.GetRequiredService<ManagementLimiter>();
        Assert.Equal(HttpStatusCode.OK, (await h.GetAsync("settings", "u3")).Status);
        Assert.Equal(HttpStatusCode.OK, (await h.GetAsync("settings", "u4")).Status);
        Assert.Equal(HttpStatusCode.TooManyRequests, (await h.GetAsync("settings", "u5")).Status);
        Assert.True(limiter.TrackedPrincipals <= 3, $"tracked {limiter.TrackedPrincipals}");
    }
}
