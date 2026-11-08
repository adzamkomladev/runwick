using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Registration;

namespace Runwick.Contract.Tests;

/// <summary>A job-type rate scope of 2 per window.</summary>
public sealed record RatedJob(string Id) : IQueueJob;

/// <summary>Two rate scopes: job type (10 per window) and per resource (1 per window).</summary>
public sealed record RatedPairJob(string Id, string Resource) : IQueueJob;

/// <summary>
/// Rate admission (10), scaler support (11) and the telemetry sample (18 §4) on both stores (20 §7, §12), ported from the
/// PostgreSQL RateAdmissionTests, ScalingTests, ScalerCaseTests (CAP-06) and TelemetrySampleTests. Admin SQL is written once in
/// portable form against <c>runwick.x</c> and runs on SQLite as <c>runwick_x</c>.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class RateScalingContractTests
{
    static readonly TimeSpan Window = TimeSpan.FromSeconds(10);
    static readonly long WindowMs = (long)Window.TotalMilliseconds;
    static readonly ControlActor Actor = new("user:ops", null, "host", "rates");
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    // ------------------------------------------------------------------ portable admin SQL

    static string Sql(DriverFixture f, string sql) => f.Kind == StoreKind.Sqlite ? sql.Replace("runwick.", "runwick_", StringComparison.Ordinal) : sql;

    static object At(DriverFixture f, DateTimeOffset t) => f.Kind == StoreKind.Sqlite ? t.ToUnixTimeMilliseconds() : t;

    /// <summary>An instant column as epoch milliseconds.</summary>
    static string Ms(DriverFixture f, string column) => f.Kind == StoreKind.Sqlite ? column : $"(extract(epoch FROM {column}) * 1000)::bigint";

    static Task<long> Count(DriverFixture f, string sql, params (string, object)[] p) => f.CountAsync(Sql(f, sql), p);

    static async Task Exec(DriverFixture f, string sql, params (string Name, object Value)[] p)
    {
        if (f.Kind == StoreKind.Postgres)
        {
            await f.ExecAsync(sql, p);
            return;
        }

        using var conn = new SqliteConnection(f.AdminConnectionString + ";Default Timeout=30");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = Sql(f, sql);
        foreach (var (name, value) in p)
        {
            cmd.Parameters.AddWithValue("@" + name, value);
        }

        cmd.ExecuteNonQuery();
    }

    static async Task<DateTimeOffset> StoreNow(DriverFixture f) => f.Kind == StoreKind.Postgres ? await f.StoreNow() : DateTimeOffset.UtcNow;

    static async Task NoViolations(DriverFixture f)
    {
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
        if (f.Kind == StoreKind.Postgres)
        {
            Assert.Empty(await f.HistoryViolations());
        }
    }

    // ------------------------------------------------------------------ rate admission (10)

    static IRateControlStore Rates(TestClient c) => (IRateControlStore)c.Driver;

    static IScalingStore Scaling(TestClient c) => (IScalingStore)c.Driver;

    static void Register(RunwickBuilder r)
    {
        r.AddJob<RatedJob>().Named("test.rated").ToQueue("default").RollingWindow(2, Window);
        r.AddJob<RatedPairJob>().Named("test.rated-pair").ToQueue("default").RollingWindow(10, Window)
            .RollingWindowPerResource(j => j.Resource, 1, Window);
    }

    static async Task<TestClient> OpenRatedAsync(DriverFixture f, bool activate = true)
    {
        var c = await f.OpenClient(configure: Register);
        if (activate)
        {
            var reply = await Rates(c).ActivateCapabilitiesAsync(
                c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), Ct);
            Assert.Equal(StoreStatus.Applied, reply.Status);
        }

        return c;
    }

    static Task<long> Grants(DriverFixture f, string? kind = null) => Count(f, """
        SELECT count(*) FROM runwick.rate_grants AS g JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
        WHERE @kind = '' OR s.scope_kind = @kind
        """, ("kind", kind ?? ""));

    static async Task<LeaseHandle> ReserveAsync(TestClient c)
    {
        var reserved = await c.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        return reserved.Value!.Lease.Handle;
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "PGX-03")]
    public async Task Rate_scopes_are_refused_until_the_application_activates_rate_admission(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rateact"), kind);
        var c = await OpenRatedAsync(f, activate: false);

        await Assert.ThrowsAsync<StoreIncompatibleException>(() => c.Dispatcher.DispatchAsync(new RatedJob("early"), cancellationToken: Ct));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.jobs"));

        var command = c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor));
        var first = await Rates(c).ActivateCapabilitiesAsync(command, Ct);
        Assert.Equal(1, first.Value!.RequiredCapabilities["rate_admission"]);
        Assert.True((await Rates(c).ActivateCapabilitiesAsync(command, Ct)).Replayed);
        var again = await Rates(c).ActivateCapabilitiesAsync(c.Command(command.Body), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (again.Status, again.Error!.Code));
        var unknown = await Rates(c).ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["teleport"] = 1 }, Actor)), Ct);
        Assert.Equal(StoreStatus.InvalidRequest, unknown.Status);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'ActivateCapabilities'"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'capabilities_activated'"));

        await c.Dispatcher.DispatchAsync(new RatedJob("after"), cancellationToken: Ct);
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.job_rate_requirements"));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "RATE-01")]
    public async Task Competing_begins_get_at_most_n_grants_per_rolling_window(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate01"), kind);
        var c = await OpenRatedAsync(f);
        for (var i = 0; i < 5; i++)
        {
            await c.Dispatcher.DispatchAsync(new RatedJob("r" + i), cancellationToken: Ct);
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
        var oldestMs = await Count(f, $"SELECT min({Ms(f, "authorized_at")}) FROM runwick.rate_grants");
        Assert.All(deferred, r => Assert.True(r.Deferred!.EligibleAt.ToUnixTimeMilliseconds() >= oldestMs + WindowMs));
        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAsync()).Status);

        // After the window the next two Begins are admitted, the third deferred again.
        while (await StoreNow(f) < DateTimeOffset.FromUnixTimeMilliseconds(oldestMs) + Window + TimeSpan.FromMilliseconds(400))
        {
            await Task.Delay(100, Ct);
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
        Assert.Equal(0, await Count(f, $"""
            SELECT count(*) FROM runwick.rate_grants AS a
            WHERE (SELECT count(*) FROM runwick.rate_grants AS b
                   WHERE b.rate_scope_id = a.rate_scope_id AND {Ms(f, "b.authorized_at")} > {Ms(f, "a.authorized_at")} - {WindowMs}
                     AND b.authorized_at <= a.authorized_at) > 2
            """));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "RATE-02")]
    public async Task One_full_scope_defers_without_a_partial_charge_and_releases_permits(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate02"), kind);
        var c = await OpenRatedAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedPairJob("a", "r"), cancellationToken: Ct);
        var second = (await c.Dispatcher.DispatchAsync(new RatedPairJob("b", "r"), cancellationToken: Ct)).JobId.Value;
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(await ReserveAsync(c))).Reply.Status);
        Assert.Equal((1L, 1L), (await Grants(f, "job-type"), await Grants(f, "resource")));

        var lease = await ReserveAsync(c);
        Assert.Equal(second, lease.JobId);
        var reply = await c.BeginAsync(lease);
        Assert.Equal(StoreStatus.RateLimited, reply.Reply.Status);

        // Nothing charged in the scope that had room; the attempt closed admission_deferred, permits gone, budgets unchanged.
        Assert.Equal((1L, 1L), (await Grants(f, "job-type"), await Grants(f, "resource")));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.job_attempts WHERE attempt_id = @a AND outcome = 'admission_deferred'",
            ("a", lease.AttemptId)));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = @j", ("j", second)));
        Assert.Equal(1, await Count(f, """
            SELECT count(*) FROM runwick.jobs AS j JOIN runwick.job_generations AS g USING (application, job_id, generation)
            WHERE j.job_id = @j AND g.started_count = 0 AND g.consecutive_unstarted_count = 0 AND j.execution_count = 0 AND j.state = 'pending'
            """, ("j", second)));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_admission_deferred'"));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CMD-06")]
    public async Task A_replayed_deferral_returns_the_same_decision_without_a_second_close(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cmd06r"), kind);
        var c = await OpenRatedAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedPairJob("a", "r"), cancellationToken: Ct);
        await c.Dispatcher.DispatchAsync(new RatedPairJob("b", "r"), cancellationToken: Ct);
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
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.job_attempts WHERE outcome = 'admission_deferred'"));
        Assert.Equal(2, await Grants(f));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "RATE-03")]
    public async Task A_failed_execution_keeps_its_grant_and_the_retry_needs_a_new_one(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate03"), kind);
        var c = await OpenRatedAsync(f);
        var job = (await c.Dispatcher.DispatchAsync(new RatedPairJob("a", "r"), cancellationToken: Ct)).JobId.Value;
        var lease = await ReserveAsync(c);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(lease)).Reply.Status);
        await c.FinishAsync(lease, AttemptOutcome.RetryableFailure, requestedDelay: TimeSpan.FromMilliseconds(1));
        Assert.Equal(2, await Grants(f));

        await WorkerHost.WaitUntil(() => c.ReserveAsync().GetAwaiter().GetResult() is { Status: StoreStatus.Applied } r
            && (lease = r.Value!.Lease.Handle) is not null, TimeSpan.FromSeconds(10), "retry reservable");
        var retry = await c.BeginAsync(lease);
        var firstGrant = DateTimeOffset.FromUnixTimeMilliseconds(await Count(f, $"SELECT min({Ms(f, "authorized_at")}) FROM runwick.rate_grants"));
        if (retry.Reply.DecidedAt >= firstGrant + Window)
        {
            // A loaded host let a full window pass before the retry's Begin: admission with new grants is correct then.
            Assert.Equal(StoreStatus.Applied, retry.Reply.Status);
            Assert.Equal(4, await Grants(f));
            return;
        }

        Assert.Equal(StoreStatus.RateLimited, retry.Reply.Status);
        Assert.Equal(JobStates.Retrying, retry.Deferred!.Position.State);
        Assert.Equal(2, await Grants(f));
        Assert.Equal(1, await Count(f, "SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "RATE-04")]
    public async Task Window_edits_freeze_admission_and_logical_time_never_goes_backward(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("rate04"), kind);
        var c = await OpenRatedAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedJob("seed"), cancellationToken: Ct);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(await ReserveAsync(c))).Reply.Status);
        var revision = await Count(f, "SELECT policy_revision FROM runwick.rate_scopes WHERE scope_kind = 'job-type'");

        // Identical policy: no_change, no freeze. Enlarged window: freeze for one full new window.
        var same = await Rates(c).EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", 2, WindowMs, revision, Actor)), Ct);
        Assert.Equal("no_change", same.Error!.Code);
        var edit = await Rates(c).EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", 5, 2 * WindowMs, revision, Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, edit.Status);
        Assert.Equal((5, revision + 1, 1), (edit.Value!.Limit, edit.Value.PolicyRevision, edit.Value.RetainedGrants));
        var stale = await Rates(c).EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", 6, 2 * WindowMs, revision, Actor)), Ct);
        Assert.Equal((StoreStatus.VersionConflict, revision + 1), (stale.Status, stale.Error!.ActualRevision));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'EditRatePolicy' AND target_kind = 'rate_scope'"));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'rate_policy_edited'"));

        await c.Dispatcher.DispatchAsync(new RatedJob("frozen"), cancellationToken: Ct);
        var frozen = await c.BeginAsync(await ReserveAsync(c));
        Assert.Equal(StoreStatus.RateLimited, frozen.Reply.Status);
        Assert.True(frozen.Deferred!.EligibleAt >= edit.Value.NotBefore);

        // Backward store time: a scope whose last decision is ahead of the clock charges at that later instant.
        var ahead = DateTimeOffset.FromUnixTimeMilliseconds((await StoreNow(f) + TimeSpan.FromMinutes(5)).ToUnixTimeMilliseconds());
        await Exec(f, "UPDATE runwick.rate_scopes SET last_decision_at = @t, not_before = NULL", ("t", At(f, ahead)));
        await WorkerHost.WaitUntil(() => c.ReserveAsync().GetAwaiter().GetResult() is { Status: StoreStatus.Applied } r
            && c.BeginAsync(r.Value!.Lease.Handle).GetAwaiter().GetResult().Reply.Status == StoreStatus.Applied, 2 * Window + TimeSpan.FromSeconds(10), "admitted");
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.rate_grants WHERE authorized_at >= @t", ("t", At(f, ahead))));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.rate_scopes WHERE last_decision_at >= @t", ("t", At(f, ahead))));
        await NoViolations(f);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-05")]
    public async Task Rate_policy_bounds_are_validated_on_edit(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap05"), kind);
        var c = await OpenRatedAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedJob("a"), cancellationToken: Ct);
        foreach (var (limit, window, field) in new[] { (0, 3_000L, "limit"), (100_001, 3_000L, "limit"), (1, 999L, "windowMs"), (1, 86_400_001L, "windowMs") })
        {
            var reply = await Rates(c).EditRatePolicyAsync(c.Command(new RatePolicyRequest("job-type", "test.rated", limit, window, 1, Actor)), Ct);
            Assert.Equal((StoreStatus.InvalidRequest, field), (reply.Status, reply.Error!.Detail));
        }

        var missing = await Rates(c).EditRatePolicyAsync(c.Command(new RatePolicyRequest("resource", "nobody", 1, 3_000, 1, Actor)), Ct);
        Assert.Equal((StoreStatus.NotFound, "scope_not_registered"), (missing.Status, missing.Error!.Code));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'EditRatePolicy'"));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "TIME-06")]
    public async Task The_worker_retires_a_deferred_begin_and_runs_the_job_once_admitted(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("time06r"), kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 4,
            jobs: j => j.RollingWindow(1, TimeSpan.FromSeconds(2)));
        await w.StartAsync();
        var store = w.Services.GetRequiredService<IRateControlStore>();
        var activated = await store.ActivateCapabilitiesAsync(new StoreCommand<ActivateCapabilitiesRequest>(f.Application, Guid.CreateVersion7(),
            DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromMinutes(1)).ToUnixTimeMilliseconds()),
            new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = 1 }, Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, activated.Status);

        for (var i = 0; i < 3; i++)
        {
            await w.DispatchAsync("w" + i);
        }

        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 3, TimeSpan.FromSeconds(60), "three rated jobs");
        Assert.True(Interlocked.Read(ref w.Engine.RateDeferrals) >= 1);
        Assert.All(Enumerable.Range(0, 3), i => Assert.Equal(1, w.Script.Invocations.Count(x => x.JobKey == "w" + i)));

        // Consecutive grants are at least one window apart; deferrals never touched the unstarted guard.
        Assert.Equal(0, await Count(f, $"""
            SELECT count(*) FROM runwick.rate_grants AS a JOIN runwick.rate_grants AS b
              ON b.rate_scope_id = a.rate_scope_id AND b.attempt_id <> a.attempt_id
             AND {Ms(f, "b.authorized_at")} >= {Ms(f, "a.authorized_at")} AND {Ms(f, "b.authorized_at")} - {Ms(f, "a.authorized_at")} < 2000
            """));
        Assert.Equal(3, await Count(f, "SELECT count(*) FROM runwick.rate_grants"));
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.job_generations WHERE consecutive_unstarted_count > 0"));
        await w.StopAsync(Ct);
        await NoViolations(f);
    }

    // ------------------------------------------------------------------ scaler support (11)

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-07")]
    [Trait("Case", "CMD-01")]
    public async Task Pool_overrides_are_revisioned_replayable_and_audited(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("ovr"), kind);
        var c = await f.OpenClient();
        var s = Scaling(c);
        var instance = Guid.CreateVersion7();

        var id = Guid.CreateVersion7();
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromMinutes(1)).ToUnixTimeMilliseconds());
        var create = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 0, Actor), id, notAfter), Ct);
        Assert.Equal(StoreStatus.Applied, create.Status);
        Assert.Equal((3, 1L), (create.Value!.Target, create.Value.Revision));

        var replay = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 0, Actor), id, notAfter), Ct);
        Assert.True(replay.Replayed);
        Assert.Equal(create.Value, replay.Value);

        var same = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 3, null, 1, Actor)), Ct);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));
        var stale = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 5, null, 0, Actor)), Ct);
        Assert.Equal((StoreStatus.VersionConflict, 1L), (stale.Status, stale.Error!.ActualRevision));

        var expires = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(10);
        var timed = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", 5, expires, 1, Actor)), Ct);
        Assert.Equal((5, 2L), (timed.Value!.Target, timed.Value.Revision));
        var cleared = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", null, null, 2, Actor)), Ct);
        Assert.Equal((null, 3L), (cleared.Value!.Target, cleared.Value.Revision));

        foreach (var (target, exp, field) in new (int?, DateTimeOffset?, string)[]
        {
            (10_001, null, "target"), (-1, null, "target"), (null, expires, "expiresAt"),
            (1, DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1), "expiresAt"), (1, DateTimeOffset.UtcNow + TimeSpan.FromDays(8), "expiresAt"),
        })
        {
            var bad = await s.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(instance, "main", target, exp, 3, Actor)), Ct);
            Assert.Equal((StoreStatus.InvalidRequest, field), (bad.Status, bad.Error!.Detail));
        }

        // Applied changes only: one event and one audit row each; rejections and the replay write neither.
        Assert.Equal(3, await Count(f, "SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'pool_override_set'"));
        Assert.Equal(3, await Count(f, "SELECT count(*) FROM runwick.management_audit WHERE operation = 'SetPoolOverride' AND target_kind = 'pool'"));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-08")]
    [Trait("Case", "WRK-02")]
    [Trait("Case", "CAP-12")]
    public async Task The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("scl"), kind);
        var release = new TaskCompletionSource();
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0,
            pool: p => p.AutoConcurrency(1, 1));
        w.Script.Default = (_, _, _) => release.Task;
        foreach (var key in new[] { "a", "b", "c", "d" })
        {
            await w.DispatchAsync(key);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 1, Wait, "one job running");

        var scaling = (IScalingStore)w.Engine.Store;
        var estimate = await scaling.EstimateDemandAsync(f.Application, w.Engine.WorkerInstanceId, ["default"], w.Engine.Contracts, Ct);
        Assert.Equal(1, estimate.LiveWorkers);
        Assert.NotEqual(0, estimate.MembershipDigest);
        Assert.False(estimate.Truncated);
        var row = Assert.Single(estimate.Backlog);
        Assert.Equal(("default", "test.work", 3L, 1), (row.Queue, row.Contract.Name, row.Due, row.CapableInstances));
        Assert.Empty(estimate.Overrides);

        // Override to zero: refill stops, the running handler is not cancelled and finishes normally.
        var c = await f.OpenClient();
        var set = await scaling.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(w.Engine.WorkerInstanceId, "main", 0, null, 0, Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, set.Status);
        var pool = w.Engine.Pools.Single();
        await WorkerHost.WaitUntil(() => pool.Gate.Target == 0, Wait, "override applied");
        Assert.Contains(pool.LastDecision!.Reason, new[] { "override", "override_active" });
        Assert.Equal(0, (await scaling.EstimateDemandAsync(f.Application, w.Engine.WorkerInstanceId, ["default"], w.Engine.Contracts, Ct)).Overrides["main"]);

        release.SetResult();
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 1, Wait, "the running job completes");
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        Assert.Single(w.Script.Invocations);
        Assert.Equal(0, await Count(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'cancelled'"));
        Assert.Equal(3, await Count(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'pending'"));

        // History holds the applied change only, never unchanged samples.
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.scaling_events WHERE old_target = 1 AND new_target = 0 AND reason = 'override'"));

        // Clearing the override resumes inside [min, max]; the backlog drains.
        var clear = await scaling.SetPoolOverrideAsync(c.Command(new PoolOverrideRequest(w.Engine.WorkerInstanceId, "main", null, null, 1, Actor)), Ct);
        Assert.Equal(StoreStatus.Applied, clear.Status);
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 4, Wait, "backlog drains after the override is removed");
        Assert.Equal(2, await Count(f, "SELECT count(*) FROM runwick.scaling_events"));
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-07")]
    public async Task Unchanged_targets_are_never_recorded_and_unknown_demand_is_not_zero(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("hist"), kind);
        var c = await f.OpenClient();
        var s = Scaling(c);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => s.RecordScalingEventAsync(f.Application,
            new ScalingEventRecord(Guid.CreateVersion7(), "main", 2, 2, 2, 2, "within_band", null, null), Ct));
        Assert.True(await s.RecordScalingEventAsync(f.Application,
            new ScalingEventRecord(Guid.CreateVersion7(), "main", 2, 3, 4, 4, "grow", DateTimeOffset.UtcNow, 7), Ct));
        Assert.Equal(1, await Count(f, "SELECT count(*) FROM runwick.scaling_events WHERE membership_digest = 7 AND desired = 4"));

        // No live worker and no backlog: a known, empty estimate — distinct from a failed one, which throws.
        var empty = await s.EstimateDemandAsync(f.Application, Guid.CreateVersion7(), ["default"],
            [new JobContract("test.work", 1, Serialization.RunwickJsonProfile.ProfileId, Serialization.RunwickJsonProfile.ProfileVersion)], Ct);
        Assert.Equal((0, 0L), (empty.LiveWorkers, empty.MembershipDigest));
        Assert.Empty(empty.Backlog);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "CAP-06")]
    public async Task Each_lane_counts_at_most_2000_due_rows_and_a_full_lane_sets_truncated(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("trunc"), kind);
        var c = await f.OpenClient(configure: r => WorkerHost.RegisterWork(r));
        async Task DispatchAsync(int count, string priority, DateTimeOffset? notBefore = null, string tag = "")
        {
            var options = new DispatchOptions { Priority = priority, NotBefore = notBefore };
            for (var done = 0; done < count; done += 100)
            {
                await c.Dispatcher.DispatchManyAsync([.. Enumerable.Range(done, Math.Min(100, count - done))
                    .Select(i => new BatchDispatchItem(new WorkJob($"{priority}-{notBefore is null}-{tag}{i}"), options))], cancellationToken: Ct);
            }
        }

        Task<DemandEstimate> EstimateAsync() => Scaling(c).EstimateDemandAsync(f.Application, Guid.CreateVersion7(), ["default"], c.Contracts, Ct);

        await DispatchAsync(1_999, "high");
        await DispatchAsync(3, "low");
        await DispatchAsync(5, "medium", DateTimeOffset.UtcNow + TimeSpan.FromHours(1));
        var below = await EstimateAsync();
        Assert.False(below.Truncated);
        Assert.Equal(2_002, Assert.Single(below.Backlog).Due);

        await DispatchAsync(1, "high");
        var full = await EstimateAsync();
        Assert.True(full.Truncated);
        Assert.Equal(2_003, Assert.Single(full.Backlog).Due);

        await DispatchAsync(1, "high", null, "more");
        var over = await EstimateAsync();
        Assert.True(over.Truncated);
        Assert.Equal(2_003, Assert.Single(over.Backlog).Due);
        Assert.Equal(2_001, await Count(f, "SELECT count(*) FROM runwick.jobs WHERE priority = 'high'"));
    }

    // ------------------------------------------------------------------ telemetry sample (18 §4)

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "OBS-06")]
    public async Task Sample_counts_due_jobs_per_queue_and_reports_the_oldest(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync(Ns("tel"), kind);
        var client = await f.OpenClient(configure: r => TestJobs.Register(r));
        var store = (ITelemetrySampleStore)client.Driver;

        var empty = await store.SampleAsync(client.Application, ["default", "other"], Ct);
        Assert.Equal([("default", 0L, (DateTimeOffset?)null), ("other", 0L, null)], empty.Queues.Select(q => (q.Queue, q.Due, q.OldestDueAt)).Order());
        Assert.Equal(0, empty.NeedsReview);
        Assert.Null(empty.OldestOverdueAt);

        for (var i = 0; i < 3; i++)
        {
            await client.Dispatcher.DispatchAsync(new PingJob("s" + i), cancellationToken: Ct);
        }

        var sample = await store.SampleAsync(client.Application, ["default"], Ct);
        var q = Assert.Single(sample.Queues);
        Assert.Equal(3, q.Due);
        Assert.NotNull(q.OldestDueAt);
        Assert.True(q.OldestDueAt <= sample.ObservedAt);

        var other = await store.SampleAsync(client.Application + "_x", ["default"], Ct);
        Assert.Equal(0, Assert.Single(other.Queues).Due);
    }
}
