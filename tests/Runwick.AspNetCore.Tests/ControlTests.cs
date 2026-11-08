using System.Net;
using System.Text.Json.Nodes;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.Protocol;
using static Runwick.AspNetCore.Tests.StandaloneHostTests;

namespace Runwick.AspNetCore.Tests;

/// <summary>MGMT-03, 04, 05, 09: queue scope, revisioned controls, client command IDs, audit failure (16 §2–§4).</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-020")]
public sealed class ControlTests
{
    static object Control(Guid commandId, (int Generation, long Revision, string State) p, string? reason = null) =>
        new { commandId, expectedGeneration = p.Generation, expectedRevision = p.Revision, reason };

    static Task<long> Audits(DriverFixture f, Guid? commandId = null) => commandId is { } id
        ? f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE command_id = @c", ("c", id))
        : f.CountAsync("SELECT count(*) FROM runwick.management_audit");

    static Task<long> Receipts(DriverFixture f, Guid commandId) =>
        f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE command_id = @c", ("c", commandId));

    static async Task<List<string>> PagedQueuesAsync(MgmtHost h, string user)
    {
        var queues = new List<string>();
        string? cursor = null;
        do
        {
            var (status, body, _) = await h.GetAsync("jobs?limit=2" + (cursor is null ? "" : "&cursor=" + cursor), user);
            Assert.Equal(HttpStatusCode.OK, status);
            queues.AddRange(body!["items"]!.AsArray().Select(j => j!["queue"]!.GetValue<string>()));
            cursor = body["next"]?.GetValue<string>();
        }
        while (cursor is not null);
        return queues;
    }

    [Fact]
    [Trait("Case", "MGMT-03")]
    public async Task A_queue_scoped_caller_never_sees_or_touches_another_queue()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt03"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        for (var i = 0; i < 3; i++)
        {
            await h.DispatchAsync(new PingJob("d" + i));
        }

        var hidden = await h.DispatchAsync(new OtherJob("secret"));
        await h.DispatchAsync(new OtherJob("o1"));
        var attempt = await f.FailNextAsync("db password in message"); // a default-queue attempt, for the error reveal below
        var before = await PositionAsync(h, hidden);

        // Another queue's job: 404 on read, reveal and every control; no audit, no change.
        Assert.Equal(HttpStatusCode.NotFound, (await h.GetAsync($"jobs/{hidden}", Users.Scoped)).Status);
        var notFound = await h.PostAsync($"jobs/{hidden}/payload:reveal", Users.Scoped);
        Assert.Equal((HttpStatusCode.NotFound, "not_found"), (notFound.Status, notFound.Body!["code"]!.GetValue<string>()));
        var missing = await h.PostAsync($"jobs/{Guid.NewGuid()}/payload:reveal", Users.Scoped);
        Assert.Equal(notFound.Body.ToJsonString(), missing.Body!.ToJsonString()); // same body either way
        foreach (var action in new[] { "cancel", "retry", "rerun" })
        {
            Assert.Equal(HttpStatusCode.NotFound, (await h.PostAsync($"jobs/{hidden}/{action}", Users.Scoped, Control(Guid.NewGuid(), before))).Status);
        }

        Assert.Equal(HttpStatusCode.NotFound, (await h.PostAsync("queues/other/pause", Users.Scoped, new { commandId = Guid.NewGuid(), expectedRevision = 1 })).Status);
        var bulk = await h.PostAsync("jobs:cancel", Users.Scoped, new
        {
            items = new[] { new { jobId = hidden, commandId = Guid.NewGuid(), expectedGeneration = 1, expectedRevision = before.Revision } },
        });
        Assert.Equal("not_found", bulk.Body!["items"]![0]!["status"]!.GetValue<string>());
        Assert.Equal(0, await Audits(f));
        Assert.Equal(before, await PositionAsync(h, hidden));

        // Lists and every cursor page hold only the allowed queue; the admin pages see both.
        Assert.Equal(["default", "default", "default"], await PagedQueuesAsync(h, Users.Scoped));
        Assert.Equal(5, (await PagedQueuesAsync(h, Users.Admin)).Count);
        Assert.Empty((await h.GetAsync("jobs?queue=other", Users.Scoped)).Body!["items"]!.AsArray());
        Assert.Equal(["default"], (await h.GetAsync("queues", Users.Scoped)).Body!["items"]!.AsArray().Select(q => q!["name"]!.GetValue<string>()));

        // A forged actor or grant in a body is ignored: the audit row names the authenticated caller.
        var mine = (await h.GetAsync("jobs?queue=default&state=pending&limit=1", Users.Scoped)).Body!["items"]![0]!["jobId"]!.GetValue<Guid>();
        var p = await PositionAsync(h, mine);
        var forged = await h.PostAsync($"jobs/{mine}/cancel", Users.Scoped, new
        {
            commandId = Guid.NewGuid(), expectedGeneration = p.Generation, expectedRevision = p.Revision,
            actor = new { subject = "root", scheme = "forged" }, grants = new[] { "Audit.Read" },
        });
        Assert.Equal(HttpStatusCode.OK, forged.Status);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE actor_subject = 'scoped' AND actor_scheme = 'Test'"));
        Assert.Equal(HttpStatusCode.Forbidden, (await h.GetAsync("audit", Users.Scoped)).Status);

        // The default-queue error reveal works for the scoped caller and is audited; the error never appears in GetJob.
        var job = (await h.GetAsync("jobs?state=failed", Users.Scoped)).Body!["items"]![0]!["jobId"]!.GetValue<Guid>();
        var detail = await h.GetAsync($"jobs/{job}", Users.Scoped);
        Assert.DoesNotContain("db password", detail.Body!.ToJsonString(), StringComparison.Ordinal);
        Assert.Equal("boom", detail.Body!["attempts"]![0]!["errorCode"]!.GetValue<string>());
        var error = await h.PostAsync($"attempts/{attempt}/error:reveal", Users.Scoped, new { reason = "incident 7" });
        Assert.Equal(HttpStatusCode.OK, error.Status);
        Assert.Contains("db password", error.Body!["error"]!.ToJsonString(), StringComparison.Ordinal);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'RevealError' AND target_kind = 'attempt' AND reason = 'incident 7'"));

        // Cursors: tampered, other filters, other scope and expired are all invalid_cursor.
        var page = (await h.GetAsync("jobs?limit=1", Users.Admin)).Body!["next"]!.GetValue<string>();
        var tampered = page[..^3] + (page[^3] == 'A' ? 'B' : 'A') + page[^2..];
        var stateCursor = (await h.GetAsync("jobs?limit=1&state=pending", Users.Admin)).Body!["next"]!.GetValue<string>();
        var scopedCursor = (await h.GetAsync("jobs?limit=1", Users.Scoped)).Body!["next"]!.GetValue<string>();
        foreach (var (user, path) in new[]
        {
            (Users.Admin, "jobs?limit=1&cursor=" + tampered), (Users.Admin, "jobs?limit=1&cursor=" + stateCursor),
            (Users.Scoped, "jobs?limit=1&cursor=" + page), (Users.Admin, "jobs?limit=1&cursor=" + scopedCursor),
            (Users.Admin, "jobs?limit=1&cursor=" + ExpiredCursor(h, page)),
        })
        {
            var (status, body, _) = await h.GetAsync(path, user);
            Assert.Equal((HttpStatusCode.BadRequest, "invalid_cursor"), (status, body!["code"]!.GetValue<string>()));
        }
    }

    /// <summary>The same cursor content resealed with an expiry in the past (the host's own protector).</summary>
    static string ExpiredCursor(MgmtHost h, string cursor)
    {
        var protector = (IManagementCursorProtector)h.App.Services.GetService(typeof(IManagementCursorProtector))!;
        var payload = JsonNode.Parse(protector.Unprotect(System.Buffers.Text.Base64Url.DecodeFromChars(cursor))!)!.AsObject();
        payload["e"] = DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds();
        return System.Buffers.Text.Base64Url.EncodeToString(protector.Protect(System.Text.Encoding.UTF8.GetBytes(payload.ToJsonString())));
    }

    [Fact]
    [Trait("Case", "MGMT-04")]
    public async Task Stale_positions_are_409_and_races_end_with_one_transition()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt04"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = await h.DispatchAsync(new PingJob("a"));
        var p = await PositionAsync(h, job);

        var staleRevision = await h.PostAsync($"jobs/{job}/cancel", Users.Admin, Control(Guid.NewGuid(), p with { Revision = p.Revision + 5 }));
        Assert.Equal((HttpStatusCode.Conflict, "stale_revision", p.Revision),
            (staleRevision.Status, staleRevision.Body!["code"]!.GetValue<string>(), staleRevision.Body["actualRevision"]!.GetValue<long>()));
        var staleGeneration = await h.PostAsync($"jobs/{job}/cancel", Users.Admin, Control(Guid.NewGuid(), p with { Generation = 2 }));
        Assert.Equal((HttpStatusCode.Conflict, "stale_generation"), (staleGeneration.Status, staleGeneration.Body!["code"]!.GetValue<string>()));
        Assert.Equal(0, await Audits(f));

        // Cancel racing Finish on owned work: whichever commits first, one control effect at most, with one receipt and one audit row.
        var (client, lease) = await f.RunNextAsync();
        var owned = await PositionAsync(h, job);
        var cancelId = Guid.NewGuid();
        var cancel = h.PostAsync($"jobs/{job}/cancel", Users.Admin, Control(cancelId, owned));
        var finish = client.FinishAsync(lease, AttemptOutcome.Succeeded);
        await Task.WhenAll(cancel, finish);
        var applied = (await cancel).Status is HttpStatusCode.Accepted ? 1 : 0;
        Assert.True(applied == 1 || (await cancel).Body!["code"]!.GetValue<string>() is "stale_revision" or "invalid_state", (await cancel).Body!.ToJsonString());
        Assert.Equal(StoreStatus.Applied, (await finish).Status);
        Assert.Equal((applied, applied), (await Audits(f, cancelId), await Receipts(f, cancelId)));
        Assert.Equal("succeeded", (await PositionAsync(h, job)).State);

        // Retry racing a uniqueness holder: the cancelled keyed job and a new dispatch with the same key.
        var keyed = await h.DispatchAsync(new KeyedJob("k-a", "k1"));
        var kp = await PositionAsync(h, keyed);
        Assert.Equal(HttpStatusCode.OK, (await h.PostAsync($"jobs/{keyed}/cancel", Users.Admin, Control(Guid.NewGuid(), kp))).Status);
        kp = await PositionAsync(h, keyed);
        var retryId = Guid.NewGuid();
        var retry = h.PostAsync($"jobs/{keyed}/retry", Users.Admin, Control(retryId, kp));
        var holder = h.DispatchAsync(new KeyedJob("k-b", "k1"));
        await Task.WhenAll(retry, holder);
        var retried = (await retry).Status == HttpStatusCode.OK ? 1 : 0;
        Assert.True(retried == 1 || (await retry).Body!["code"]!.GetValue<string>() == "unique_conflict", (await retry).Body!.ToJsonString());
        Assert.Equal((retried, retried), (await Audits(f, retryId), await Receipts(f, retryId)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE job_name = 'test.keyed' AND state = 'pending'"));
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Case", "MGMT-05")]
    public async Task A_client_command_id_replays_conflicts_and_resolves_a_dropped_reply()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt05"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        h.Grants["op"] = ManagementRoles.Operator;
        var job = await h.DispatchAsync(new PingJob("a"));
        var p = await PositionAsync(h, job);
        var id = Guid.NewGuid();

        var first = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "dup"));
        var second = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "dup"));
        Assert.Equal((HttpStatusCode.OK, false), (first.Status, first.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal((HttpStatusCode.OK, true), (second.Status, second.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal(first.Body["revision"]!.GetValue<long>(), second.Body["revision"]!.GetValue<long>());
        Assert.Equal(1, await Audits(f, id));

        var other = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "a different reason"));
        Assert.Equal((HttpStatusCode.Conflict, "command_conflict"), (other.Status, other.Body!["code"]!.GetValue<string>()));

        // The grant is removed: the replay is refused before the store, and discloses nothing of the receipt.
        h.Grants["op"] = ManagementRoles.Viewer;
        var denied = await h.PostAsync($"jobs/{job}/cancel", "op", Control(id, p, "dup"));
        Assert.Equal(HttpStatusCode.Forbidden, denied.Status);
        Assert.Equal(["code", "message"], denied.Body!.AsObject().Select(x => x.Key).Order());
        Assert.Equal(1, await Audits(f, id));

        // A dropped reply after commit: the session resends the same command and the receipt answers; one transition, one audit row.
        h.Grants["op"] = ManagementRoles.Operator;
        var retryJob = await h.DispatchAsync(new PingJob("b"));
        var rp = await PositionAsync(h, retryJob);
        var dropId = Guid.NewGuid();
        var drop = f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(dropId));
        var resolved = await h.PostAsync($"jobs/{retryJob}/cancel", "op", Control(dropId, rp));
        Assert.Equal(1, drop.Fired);
        Assert.Equal((HttpStatusCode.OK, true), (resolved.Status, resolved.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal((1L, 1L), (await Audits(f, dropId), await Receipts(f, dropId)));
        Assert.Equal(rp.Revision + 1, (await PositionAsync(h, retryJob)).Revision);

        // The client may also resend after an unknown outcome: the same ID and body replays.
        var resent = await h.PostAsync($"jobs/{retryJob}/cancel", "op", Control(dropId, rp));
        Assert.Equal((HttpStatusCode.OK, true), (resent.Status, resent.Body!["replayed"]!.GetValue<bool>()));
        Assert.Equal(1, await Audits(f, dropId));
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Case", "MGMT-09")]
    public async Task A_failing_audit_insert_applies_nothing_and_reveals_nothing()
    {
        await using var f = await DriverFixture.StartAsync(Ns("mgmt09"));
        await using var h = await MgmtHost.StartAsync(f);
        Users.Seed(h);
        var job = await h.DispatchAsync(new PingJob("payload-secret"));
        var p = await PositionAsync(h, job);

        // Cancel of a waiting job writes the job (1), its event (2), then the audit row (3): the audit insert raises.
        var audit = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = Operations.Cancel, Step = 3 }, times: 100);
        var cancel = await h.PostAsync($"jobs/{job}/cancel", Users.Admin, Control(Guid.NewGuid(), p));
        Assert.True(audit.Fired >= 1);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, cancel.Status);
        Assert.Equal(p, await PositionAsync(h, job));

        // The disclosure audit row is the reveal's first write: if it fails, no payload leaves the store.
        var reveal = f.ArmFault(FaultPoint.AfterMutation, FaultSpec.Throw(), new FaultTarget { Operation = "RecordDisclosure", Step = 1 }, times: 100);
        var revealed = await h.PostAsync($"jobs/{job}/payload:reveal", Users.Admin);
        Assert.True(reveal.Fired >= 1);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, revealed.Status);
        Assert.DoesNotContain("payload-secret", revealed.Body!.ToJsonString(), StringComparison.Ordinal);
        Assert.Equal(0, await Audits(f));
    }
}
