using System.Net;
using Runwick.Harness.Store;
using Runwick.Management;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>DSH-02: the management read behind the dashboard's Scaling page, over HTTP (21 §6).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-024")]
public sealed class ScalingEventsTests
{
    static Task InsertAsync(DriverFixture f, DateTimeOffset at, Guid instance, string pool, Guid? id = null) => f.ExecAsync("""
        INSERT INTO runwick.scaling_events (application, event_id, instance_id, pool_name, old_target, new_target, desired, candidate, reason, snapshot_at, created_at)
        VALUES (@a, @e, @i, @p, 1, 2, 2, NULL, 'scale_up', NULL, @t)
        """, ("a", f.Application), ("e", id ?? Guid.CreateVersion7()), ("i", instance), ("p", pool), ("t", at));

    static string Q(DateTimeOffset t) => Uri.EscapeDataString(t.ToString("O"));

    [Fact]
    [Trait("Case", "DSH-02")]
    public async Task Scaling_events_are_newest_first_in_keyset_pages_with_filters_caps_and_a_permission()
    {
        await using var f = await DriverFixture.StartAsync(Ns("dsh02"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var now = DateTimeOffset.UtcNow;
        var (i1, i2) = (Guid.NewGuid(), Guid.NewGuid());
        var tie = now.AddMinutes(-5);
        for (var k = 0; k < 5; k++)
        {
            await InsertAsync(f, now.AddMinutes(-10 - k), i1, "default");
        }

        await InsertAsync(f, tie, i2, "batch");
        await InsertAsync(f, tie, i2, "batch"); // same created_at: event_id breaks the tie
        await InsertAsync(f, now.AddDays(-8), i1, "default"); // outside the default 7-day range
        await InsertAsync(f, now.AddDays(-40), i1, "default");

        // Order and keyset pages: newest first, every row once, ties ordered by event ID.
        var seen = new List<(DateTimeOffset At, Guid Id)>();
        string? cursor = null;
        do
        {
            var (status, body, _) = await h.GetAsync("scaling-events?limit=2" + (cursor is null ? "" : "&cursor=" + cursor), Users.Admin);
            Assert.Equal(HttpStatusCode.OK, status);
            var items = body!["items"]!.AsArray();
            Assert.True(items.Count <= 2);
            seen.AddRange(items.Select(x => (x!["createdAt"]!.GetValue<DateTimeOffset>(), x["eventId"]!.GetValue<Guid>())));
            cursor = body["next"]?.GetValue<string>();
        }
        while (cursor is not null);

        Assert.Equal(7, seen.Count);
        Assert.Equal(seen.Count, seen.Distinct().Count());
        Assert.Equal(seen.OrderByDescending(x => x.At).ThenByDescending(x => x.Id), seen);

        // A wider range reaches the 8-day-old row; more than 31 days is refused.
        var (_, wide, _) = await h.GetAsync($"scaling-events?from={Q(now.AddDays(-30))}&to={Q(now)}", Users.Admin);
        Assert.Equal(8, wide!["items"]!.AsArray().Count);
        var (tooWide, error, _) = await h.GetAsync($"scaling-events?from={Q(now.AddDays(-32))}&to={Q(now)}", Users.Admin);
        Assert.Equal((HttpStatusCode.BadRequest, "invalid_request"), (tooWide, error!["code"]!.GetValue<string>()));

        // Filters.
        var (_, byInstance, _) = await h.GetAsync($"scaling-events?instance={i2}", Users.Admin);
        Assert.Equal(2, byInstance!["items"]!.AsArray().Count);
        Assert.All(byInstance["items"]!.AsArray(), x => Assert.Equal(i2, x!["instanceId"]!.GetValue<Guid>()));
        var (_, byPool, _) = await h.GetAsync("scaling-events?pool=default", Users.Admin);
        Assert.Equal(5, byPool!["items"]!.AsArray().Count);
        Assert.All(byPool["items"]!.AsArray(), x => Assert.Equal("default", x!["pool"]!.GetValue<string>()));
        var (badInstance, _, _) = await h.GetAsync("scaling-events?instance=nope", Users.Admin);
        Assert.Equal(HttpStatusCode.BadRequest, badInstance);

        // Capacity.Read is required.
        Assert.Equal(HttpStatusCode.OK, (await h.GetAsync("scaling-events", Users.Viewer)).Status);
        h.Grants[Users.Nobody] = [new ManagementGrant(ManagementPermissions.JobsRead)];
        Assert.Equal(HttpStatusCode.Forbidden, (await h.GetAsync("scaling-events", Users.Nobody)).Status);
    }
}
