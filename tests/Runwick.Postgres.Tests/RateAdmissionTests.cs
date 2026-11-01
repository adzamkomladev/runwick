using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.Registration;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>A job-type rate scope of 2 per 3 s.</summary>
public sealed record RatedJob(string Id) : IQueueJob;

/// <summary>Two rate scopes: job type (10 per 3 s) and per resource (1 per 3 s).</summary>
public sealed record RatedPairJob(string Id, string Resource) : IQueueJob;

/// <summary>Rate admission (10): RATE-01 … RATE-04, CAP-05, CMD-06 rate half, TIME-06 rate sub-part, PGX-03 rate half.</summary>
public sealed class RateAdmissionTests
{
    static readonly TimeSpan Window = TimeSpan.FromSeconds(3);

    static readonly ControlActor Actor = new("user:ops", null, "host", "rates");

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static void Register(RunwickBuilder r)
    {
        r.AddJob<RatedJob>().Named("test.rated").ToQueue("default").RollingWindow(2, Window);
        r.AddJob<RatedPairJob>().Named("test.rated-pair").ToQueue("default").RollingWindow(10, Window)
            .RollingWindowPerResource(j => j.Resource, 1, Window);
    }

    static async Task<TestClient> OpenAsync(DriverFixture f, bool activate = true)
    {
        var c = await f.OpenClient(configure: Register);
        if (activate)
        {
            var reply = await c.Store.ActivateCapabilitiesAsync(
                c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None);
            Assert.Equal(StoreStatus.Applied, reply.Status);
        }

        return c;
    }

    static Task<long> Grants(DriverFixture f, string? kind = null) => f.CountAsync("""
        SELECT count(*) FROM runwick.rate_grants AS g JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
        WHERE @kind = '' OR s.scope_kind = @kind
        """, ("kind", kind ?? ""));

    static async Task<LeaseHandle> ReserveAsync(TestClient c)
    {
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        return reserved.Value!.Lease.Handle;
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "PGX-03")]
    public async Task Rate_scopes_are_refused_until_the_application_activates_rate_admission()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rateact"));
        var c = await OpenAsync(f, activate: false);

        await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.Dispatcher.DispatchAsync(new RatedJob("early")));
        Assert.Equal(0, await Jobs(f));

        var command = c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor));
        var first = await c.Store.ActivateCapabilitiesAsync(command, CancellationToken.None);
        Assert.Equal(1, first.Value!.RequiredCapabilities["rate_admission"]);
        Assert.True((await c.Store.ActivateCapabilitiesAsync(command, CancellationToken.None)).Replayed);
        var again = await c.Store.ActivateCapabilitiesAsync(c.Command(command.Body), CancellationToken.None);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (again.Status, again.Error!.Code));
        var unknown = await c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["teleport"] = 1 }, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.InvalidRequest, unknown.Status);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'ActivateCapabilities'"));

        await c.Dispatcher.DispatchAsync(new RatedJob("after"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_rate_requirements"));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-01")]
    public async Task Competing_begins_get_at_most_n_grants_per_rolling_window()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate01"));
        var c = await OpenAsync(f);
        for (var i = 0; i < 5; i++)
        {
            await c.Dispatcher.DispatchAsync(new RatedJob("r" + i));
        }

        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 5; i++)
        {
            leases.Add(await ReserveAsync(c));
        }

        var replies = await Task.WhenAll(leases.Select(l => c.BeginAsync(l)));
        Assert.Equal(2, replies.Count(r => r.Reply.Status == StoreStatus.Applied));
        var deferred = replies.Where(r => r.Reply.Status == StoreStatus.RateLimited).ToList();
        Assert.Equal(3, deferred.Count);
        Assert.All(deferred, r =>
        {
            Assert.Equal(StoreEffect.Applied, r.Reply.Effect);
            Assert.NotNull(r.Deferred);
            Assert.Equal(JobStates.Pending, r.Deferred!.Position.State);
            Assert.Single(r.Deferred.BlockingScopes);
        });
        Assert.Equal(2, await Grants(f));

        // Eligibility is no earlier than the oldest counted grant leaving the window.
        var oldest = (await TextAsync(f, "SELECT (extract(epoch FROM min(authorized_at)) * 1000)::bigint::text FROM runwick.rate_grants")).Single();
        var oldestMs = long.Parse(oldest, System.Globalization.CultureInfo.InvariantCulture);
        Assert.All(deferred, r => Assert.True(r.Deferred!.EligibleAt.ToUnixTimeMilliseconds() >= oldestMs + (long)Window.TotalMilliseconds));
        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAsync()).Status);

        // After the window the next two Begins are admitted, the third deferred again.
        while (await f.StoreNow() < DateTimeOffset.FromUnixTimeMilliseconds(oldestMs) + Window + TimeSpan.FromMilliseconds(400))
        {
            await Task.Delay(100);
        }

        var next = new List<BeginReply>();
        for (var i = 0; i < 3; i++)
        {
            var reserved = await c.ReserveAsync();
            if (reserved.Status != StoreStatus.Applied)
            {
                break;
            }

            next.Add(await c.BeginAsync(reserved.Value!.Lease.Handle));
        }

        Assert.True(next.Count(r => r.Reply.Status == StoreStatus.Applied) <= 2);
        Assert.Equal(0, await f.CountAsync("""
            SELECT count(*) FROM runwick.rate_grants AS a
            WHERE (SELECT count(*) FROM runwick.rate_grants AS b
                   WHERE b.rate_scope_id = a.rate_scope_id AND b.authorized_at > a.authorized_at - interval '3 seconds'
                     AND b.authorized_at <= a.authorized_at) > 2
            """));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-02")]
    public async Task One_full_scope_defers_without_a_partial_charge_and_releases_permits()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate02"));
        var c = await OpenAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedPairJob("a", "r"));
        var second = (await c.Dispatcher.DispatchAsync(new RatedPairJob("b", "r"))).JobId.Value;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(await ReserveAsync(c))).Reply.Status);
        Assert.Equal((1L, 1L), (await Grants(f, "job-type"), await Grants(f, "resource")));

        var lease = await ReserveAsync(c);
        Assert.Equal(second, lease.JobId);
        var reply = await c.BeginAsync(lease);
        Assert.Equal(StoreStatus.RateLimited, reply.Reply.Status);

        // Nothing charged in the scope that had room; the attempt closed admission_deferred, permits gone, budgets unchanged.
        Assert.Equal((1L, 1L), (await Grants(f, "job-type"), await Grants(f, "resource")));
        Assert.Equal("admission_deferred", (await TextAsync(f, "SELECT outcome FROM runwick.job_attempts WHERE attempt_id = @a", ("a", lease.AttemptId))).Single());
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = @j", ("j", second)));
        Assert.Equal("0/0/0/pending", (await TextAsync(f, """
            SELECT g.started_count || '/' || g.consecutive_unstarted_count || '/' || j.execution_count || '/' || j.state
            FROM runwick.jobs AS j JOIN runwick.job_generations AS g USING (application, job_id, generation)
            WHERE j.job_id = @j
            """, ("j", second))).Single());
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_admission_deferred'"));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "CMD-06")]
    public async Task A_replayed_deferral_returns_the_same_decision_without_a_second_close()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd06r"));
        var c = await OpenAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedPairJob("a", "r"));
        await c.Dispatcher.DispatchAsync(new RatedPairJob("b", "r"));
        await c.BeginAsync(await ReserveAsync(c));
        var lease = await ReserveAsync(c);
        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30)).ToUnixTimeMilliseconds());

        var first = await c.BeginAsync(lease, id, notAfter);
        var replay = await c.BeginAsync(lease, id, notAfter);
        Assert.Equal((StoreStatus.RateLimited, false), (first.Reply.Status, first.Reply.Replayed));
        Assert.Equal((StoreStatus.RateLimited, true), (replay.Reply.Status, replay.Reply.Replayed));
        Assert.Equal((first.Deferred!.Position, first.Deferred.EligibleAt), (replay.Deferred!.Position, replay.Deferred.EligibleAt));
        Assert.Equal(first.Deferred.BlockingScopes, replay.Deferred.BlockingScopes);
        Assert.Equal(first.Reply.DecidedAt, replay.Reply.DecidedAt);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE outcome = 'admission_deferred'"));
        Assert.Equal(2, await Grants(f));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-03")]
    public async Task A_failed_execution_keeps_its_grant_and_the_retry_needs_a_new_one()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate03"));
        var c = await OpenAsync(f);
        var job = (await c.Dispatcher.DispatchAsync(new RatedPairJob("a", "r"))).JobId.Value;
        var lease = await ReserveAsync(c);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(1));
        Assert.Equal(2, await Grants(f));

        await WorkerHost.WaitUntil(() => c.ReserveAsync().GetAwaiter().GetResult() is { Status: StoreStatus.Applied } r
            && (lease = r.Value!.Lease.Handle) is not null, TimeSpan.FromSeconds(10), "retry reservable");
        var retry = await c.BeginAsync(lease);
        var firstGrant = DateTimeOffset.FromUnixTimeMilliseconds(
            await f.CountAsync("SELECT (extract(epoch FROM min(authorized_at)) * 1000)::bigint FROM runwick.rate_grants"));
        if (retry.Reply.DecidedAt >= firstGrant + Window)
        {
            // A loaded host let a full window pass before the retry's Begin: admission with a new grant is the
            // correct outcome then, charging both scopes again (the failed execution kept its two grants).
            Assert.Equal(StoreStatus.Applied, retry.Reply.Status);
            Assert.Equal(4, await Grants(f));
            return;
        }

        Assert.Equal(StoreStatus.RateLimited, retry.Reply.Status);
        Assert.Equal(JobStates.Retrying, retry.Deferred!.Position.State);
        Assert.Equal(2, await Grants(f));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-04")]
    public async Task Window_edits_freeze_admission_and_logical_time_never_goes_backward()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate04"));
        var c = await OpenAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedJob("seed"));
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(await ReserveAsync(c))).Reply.Status);
        var revision = await f.CountAsync("SELECT policy_revision FROM runwick.rate_scopes WHERE scope_kind = 'job-type'");

        // Identical policy: no_change, no freeze. Enlarged window: freeze for one full new window.
        var same = await c.Store.EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", 2, 3_000, revision, Actor)), CancellationToken.None);
        Assert.Equal("no_change", same.Error!.Code);
        var edit = await c.Store.EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", 5, 6_000, revision, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, edit.Status);
        Assert.Equal((5, revision + 1, 1), (edit.Value!.Limit, edit.Value.PolicyRevision, edit.Value.RetainedGrants));
        var stale = await c.Store.EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", 6, 6_000, revision, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.VersionConflict, stale.Status);

        await c.Dispatcher.DispatchAsync(new RatedJob("frozen"));
        var frozen = await c.BeginAsync(await ReserveAsync(c));
        Assert.Equal(StoreStatus.RateLimited, frozen.Reply.Status);
        Assert.True(frozen.Deferred!.EligibleAt >= edit.Value.NotBefore);

        // Backward database time: a scope whose last decision is ahead of the clock charges at that later instant.
        var ahead = (await f.StoreNow()) + TimeSpan.FromMinutes(5);
        await f.ExecAsync("UPDATE runwick.rate_scopes SET last_decision_at = @t, not_before = NULL", ("t", ahead));
        await WorkerHost.WaitUntil(() => c.ReserveAsync().GetAwaiter().GetResult() is { Status: StoreStatus.Applied } r
            && c.BeginAsync(r.Value!.Lease.Handle).GetAwaiter().GetResult().Reply.Status == StoreStatus.Applied, TimeSpan.FromSeconds(10), "admitted");
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.rate_grants WHERE authorized_at >= @t", ("t", ahead)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.rate_scopes WHERE last_decision_at >= @t", ("t", ahead)));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "CAP-05")]
    public async Task Rate_policy_bounds_are_validated_at_registration_and_on_edit()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap05"));
        await Assert.ThrowsAnyAsync<Exception>(() => f.OpenClient(configure: r => r.AddJob<RatedJob>().Named("bad").RollingWindow(0, Window)));
        await Assert.ThrowsAnyAsync<Exception>(() => f.OpenClient(configure: r => r.AddJob<RatedJob>().Named("bad").RollingWindow(1, TimeSpan.FromMilliseconds(1500.5))));

        var c = await OpenAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedJob("a"));
        foreach (var (limit, window, field) in new[] { (0, 3_000L, "limit"), (100_001, 3_000L, "limit"), (1, 999L, "windowMs"), (1, 86_400_001L, "windowMs") })
        {
            var reply = await c.Store.EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", limit, window, 1, Actor)), CancellationToken.None);
            Assert.Equal((StoreStatus.InvalidRequest, field), (reply.Status, reply.Error!.Detail));
        }

        var missing = await c.Store.EditRatePolicyAsync(c.Command(new RatePolicyRequest("resource", "nobody", 1, 3_000, 1, Actor)), CancellationToken.None);
        Assert.Equal((StoreStatus.NotFound, "scope_not_registered"), (missing.Status, missing.Error!.Code));

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "TIME-06")]
    public async Task The_worker_retires_a_deferred_begin_and_runs_the_job_once_admitted()
    {
        await using var f = await DriverFixture.StartAsync(Ns("time06r"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 4,
            jobs: j => j.RollingWindow(1, TimeSpan.FromSeconds(2)));
        await w.StartAsync();
        var store = w.Services.GetRequiredService<IRateControlStore>();
        var activated = await store.ActivateCapabilitiesAsync(new StoreCommand<ActivateCapabilitiesRequest>(f.Application, Guid.CreateVersion7(),
            DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromMinutes(1)).ToUnixTimeMilliseconds()),
            new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), CancellationToken.None);
        Assert.Equal(StoreStatus.Applied, activated.Status);

        var jobs = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            jobs.Add((await w.DispatchAsync("w" + i)).JobId.Value);
        }

        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 3, TimeSpan.FromSeconds(60), "three rated jobs");
        Assert.True(Interlocked.Read(ref w.Engine.RateDeferrals) >= 1);
        Assert.All(jobs, j => Assert.Equal(1, w.Script.Invocations.Count(x => x.JobKey == "w" + jobs.IndexOf(j))));

        // Consecutive grants are at least one window apart; deferrals never touched the unstarted guard.
        var times = await TextAsync(f, "SELECT (extract(epoch FROM authorized_at) * 1000)::bigint::text FROM runwick.rate_grants ORDER BY authorized_at");
        var ms = times.Select(t => long.Parse(t, System.Globalization.CultureInfo.InvariantCulture)).ToList();
        Assert.All(ms.Zip(ms.Skip(1)), p => Assert.True(p.Second - p.First >= 2_000, $"grants {p.First} and {p.Second} share a window"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.job_generations WHERE consecutive_unstarted_count > 0"));
        await w.StopAsync();

        Assert.Empty(await f.HistoryViolations());
    }
}
