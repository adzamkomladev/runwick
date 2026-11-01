using System.Text.Json.Nodes;
using Npgsql;
using Runwick.Harness.Faults;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Registration;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>Shared setup for the SP-013 owed cases (15 §6, I-1 RATE-05 … RATE-10, UPG-02, UPG-06, PGX-01).</summary>
internal static class RateSupport
{
    public static readonly ControlActor Actor = new("user:ops", null, "host", "rates");

    public static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    public static void Register(RunwickBuilder r)
    {
        TestJobs.Register(r);
        RateJobs.Register(r);
    }

    public static async Task<TestClient> OpenAsync(DriverFixture f, bool activate = true)
    {
        var c = await f.OpenClient(configure: Register);
        if (activate)
        {
            Assert.Equal(StoreStatus.Applied, (await ActivateAsync(c, 1)).Status);
        }

        return c;
    }

    public static Task<StoreReply<ActivatedCapabilities>> ActivateAsync(TestClient c, int version, Guid? commandId = null) =>
        c.Store.ActivateCapabilitiesAsync(
            c.Command(new ActivateCapabilitiesRequest(new Dictionary<string, int> { ["rate_admission"] = version }, Actor), commandId),
            CancellationToken.None);

    public static Task<long> Grants(DriverFixture f, string? kind = null) => f.CountAsync("""
        SELECT count(*) FROM runwick.rate_grants AS g JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
        WHERE @kind = '' OR s.scope_kind = @kind
        """, ("kind", kind ?? ""));

    public static async Task<LeaseHandle> ReserveAsync(TestClient c, TimeSpan? lease = null)
    {
        var reserved = await c.ReserveAsync(lease: lease);
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        return reserved.Value!.Lease.Handle;
    }

    public static async Task<long> PeerDeadlocks(PeerClient peer) => (await peer.SendAsync("deadlocks"))["count"]!.GetValue<long>();

    public static async Task NoInvariantViolations(DriverFixture f)
    {
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
        Assert.Empty(await f.HistoryViolations());
    }

    /// <summary>Waits until a backend of this database waits on a heavyweight lock (the arranged blocking happened).</summary>
    public static async Task WaitForLockWaiter(DriverFixture f, string applicationPrefix = "", string? blockedBy = null)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (await f.CountAsync("""
            SELECT count(*) FROM pg_stat_activity AS w
            WHERE w.datname = current_database() AND w.wait_event_type = 'Lock' AND w.application_name LIKE @p || '%'
              AND (@b = '' OR EXISTS (SELECT 1 FROM pg_stat_activity AS b
                                      WHERE b.pid = ANY (pg_blocking_pids(w.pid)) AND b.application_name LIKE @b || '%'))
            """, ("p", applicationPrefix), ("b", blockedBy ?? "")) == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, $"no backend '{applicationPrefix}*' waits on a lock held by '{blockedBy}*'");
            await Task.Delay(20);
        }
    }

    /// <summary>The application names of the backends blocking the lock waiter(s) whose name starts with the prefix.</summary>
    public static Task<List<string>> BlockersOf(DriverFixture f, string applicationPrefix) => TextAsync(f, """
        SELECT DISTINCT b.application_name FROM pg_stat_activity AS w
        JOIN pg_stat_activity AS b ON b.pid = ANY (pg_blocking_pids(w.pid))
        WHERE w.datname = current_database() AND w.wait_event_type = 'Lock' AND w.application_name LIKE @p || '%'
        """, ("p", applicationPrefix));

    public static async Task WaitUntilAllEligible(DriverFixture f)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (await f.CountAsync("""
            SELECT count(*) FROM runwick.jobs WHERE state IN ('pending', 'retrying') AND eligible_at > clock_timestamp()
            """) > 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "deferred jobs never became eligible");
            await Task.Delay(100);
        }
    }
}

/// <summary>
/// capacity-control §6 rate rows with peer processes and barriers (15 §6, I-1): RATE-05 … RATE-09, and the
/// UPG-02 / UPG-06 subset (an old binary after activation).
/// </summary>
public sealed class RateAdmissionReplicaTests
{
    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-05")]
    public async Task Three_replica_processes_never_grant_more_than_n_per_rolling_window()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("rate05"));
        var c = await RateSupport.OpenAsync(f);
        var peers = new List<PeerClient>();
        foreach (var id in new[] { "peer-1", "peer-2", "peer-3" })
        {
            var p = await f.StartPeer(id);
            await p.OpenRatedAsync(f.ClientConnectionString(id), f.Application);
            peers.Add(p);
        }

        const int Jobs = 9;
        for (var i = 0; i < Jobs; i++)
        {
            await c.Dispatcher.DispatchAsync(new RatedReplicaJob("j" + i, "shared"));
        }

        var applied = 0;
        for (var round = 0; applied < Jobs; round++)
        {
            Assert.True(round < 10, $"only {applied} of {Jobs} jobs were admitted after {round} rounds");
            await RateSupport.WaitUntilAllEligible(f);

            // Every replica reserves what it can, then all Begins race for the shared scopes at once.
            var leases = (await Task.WhenAll(peers.Select(async p =>
            {
                var mine = new List<(PeerClient Peer, JsonNode Lease)>();
                for (var k = 0; k < Jobs; k++)
                {
                    var r = await p.SendAsync("reserve");
                    if (r["status"]!.GetValue<string>() != "Applied")
                    {
                        break;
                    }

                    mine.Add((p, r["lease"]!));
                }

                return mine;
            }))).SelectMany(x => x).ToList();
            var roundStart = await f.StoreNow();
            var replies = await Task.WhenAll(leases.Select(l => l.Peer.SendAsync("begin", new JsonObject { ["lease"] = l.Lease.DeepClone() })));
            var statuses = replies.Select(r => r["status"]!.GetValue<string>()).ToList();
            Assert.All(statuses, s => Assert.Contains(s, new[] { "Applied", "RateLimited" }));
            Assert.All(replies.Where(r => r["status"]!.GetValue<string>() == "RateLimited"), r => Assert.Equal("Applied", r["effect"]!.GetValue<string>()));
            var admitted = statuses.Count(s => s == "Applied");

            // The bound is per rolling window, not per round: on a loaded host the round's Begins can span more than one
            // window, and then a second window's grants are correct. The store's grant times decide.
            Assert.Equal(0, await WindowsOverLimit(f));
            var spreadMs = (await TextAsync(f, """
                SELECT coalesce((extract(epoch FROM max(g.authorized_at) - min(g.authorized_at)) * 1000)::bigint, 0)::text
                FROM runwick.rate_grants AS g JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
                WHERE s.scope_kind = 'job-type' AND g.authorized_at >= @r
                """, ("r", roundStart))).Single();
            var oneWindow = long.Parse(spreadMs, System.Globalization.CultureInfo.InvariantCulture) < (long)RateJobs.ReplicaWindow.TotalMilliseconds;
            if (oneWindow)
            {
                Assert.True(admitted <= RateJobs.ReplicaLimit, $"round {round}: {admitted} grants within {spreadMs} ms for limit {RateJobs.ReplicaLimit}");
            }

            if (round == 0 && oneWindow)
            {
                // The empty window takes exactly n of the nine contenders: the last grant goes to exactly one replica.
                Assert.Equal((Jobs, RateJobs.ReplicaLimit), (leases.Count, admitted));
            }

            applied += admitted;
        }

        // Exactly one grant per admitted execution in each scope (no partial multi-scope charge) …
        Assert.Equal((9L, 9L), (await RateSupport.Grants(f, "job-type"), await RateSupport.Grants(f, "resource")));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT attempt_id FROM runwick.rate_grants GROUP BY attempt_id HAVING count(*) <> 2) AS x"));
        Assert.Equal(Jobs, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE execution_count = 1 AND state = 'running'"));

        // … and no rolling window of either scope ever holds more than n.
        Assert.Equal(0, await WindowsOverLimit(f));
        await RateSupport.NoInvariantViolations(f);
    }

    static Task<long> WindowsOverLimit(DriverFixture f) => f.CountAsync("""
        SELECT count(*) FROM runwick.rate_grants AS a JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
        WHERE s.scope_kind = 'job-type'
          AND (SELECT count(*) FROM runwick.rate_grants AS b
               WHERE b.rate_scope_id = a.rate_scope_id AND b.authorized_at > a.authorized_at - s.window_ms * interval '1 millisecond'
                 AND b.authorized_at <= a.authorized_at) > s.limit_count
        """);

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-06")]
    public async Task A_rate_rejection_after_reservation_releases_exact_permits_and_sets_eligibility()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("rate06"));
        var c = await RateSupport.OpenAsync(f);
        var first = (await c.Dispatcher.DispatchAsync(new RatedPermitJob("a", "r-a"))).JobId.Value;
        var second = (await c.Dispatcher.DispatchAsync(new RatedPermitJob("b", "r-b"))).JobId.Value;
        var running = await RateSupport.ReserveAsync(c);
        Assert.Equal(first, running.JobId);
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(running)).Reply.Status);
        var grantMs = await f.CountAsync("SELECT (extract(epoch FROM authorized_at) * 1000)::bigint FROM runwick.rate_grants");

        var lease = await RateSupport.ReserveAsync(c);
        Assert.Equal(second, lease.JobId);
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = @j", ("j", second)));
        var reply = await c.BeginAsync(lease);
        Assert.Equal((StoreStatus.RateLimited, StoreEffect.Applied), (reply.Reply.Status, reply.Reply.Effect));

        // Exactly the deferred attempt's permit is gone; the running job keeps its own.
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = @j", ("j", second)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.concurrency_permits WHERE job_id = @j", ("j", first)));
        Assert.Equal("admission_deferred", (await TextAsync(f, "SELECT outcome FROM runwick.job_attempts WHERE attempt_id = @a", ("a", lease.AttemptId))).Single());

        // Budgets and the abandonment guard are untouched; the job is pending with eligibility at the deferral.
        Assert.Equal("0/0/0/1/pending", (await TextAsync(f, """
            SELECT g.started_count || '/' || g.consecutive_unstarted_count || '/' || j.execution_count || '/' || j.reservation_count || '/' || j.state
            FROM runwick.jobs AS j JOIN runwick.job_generations AS g USING (application, job_id, generation)
            WHERE j.job_id = @j
            """, ("j", second))).Single());
        var eligibleMs = await f.CountAsync("SELECT (extract(epoch FROM eligible_at) * 1000)::bigint FROM runwick.jobs WHERE job_id = @j", ("j", second));
        Assert.Equal(reply.Deferred!.EligibleAt.ToUnixTimeMilliseconds(), eligibleMs);
        Assert.True(eligibleMs >= grantMs + 60_000, $"eligible {eligibleMs} before the grant {grantMs} leaves the window");
        Assert.Equal(1L, await RateSupport.Grants(f));
        Assert.Equal(StoreStatus.NoWork, (await c.ReserveAsync()).Status);
        await RateSupport.NoInvariantViolations(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-07")]
    public async Task A_lost_begin_reply_replays_without_a_second_grant_or_start()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("rate07"));
        var c = await RateSupport.OpenAsync(f);
        await c.Dispatcher.DispatchAsync(new RatedOrderJob("a", "k1", "k2"));
        await c.Dispatcher.DispatchAsync(new RatedOrderJob("b", "k1", "k2"));
        var notAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30)).ToUnixTimeMilliseconds());

        // (a) Applied Begin, reply lost after commit: the same command replays the authorization.
        var lease = await RateSupport.ReserveAsync(c);
        var id = Guid.CreateVersion7();
        f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.BeginAsync(lease, id, notAfter));
        Assert.Equal(2L, await RateSupport.Grants(f));
        var replay = await c.BeginAsync(lease, id, notAfter);
        Assert.Equal((StoreStatus.Applied, true, 1), (replay.Reply.Status, replay.Reply.Replayed, replay.Reply.Value!.ExecutionNumber));
        Assert.Equal(2L, await RateSupport.Grants(f));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", lease.JobId)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_started'"));

        // (b) Deferred Begin, reply lost after commit: the replay is the stored deferral, closed once.
        var deferredLease = await RateSupport.ReserveAsync(c);
        var id2 = Guid.CreateVersion7();
        f.ArmFault(FaultPoint.AfterCommitBeforeReply, FaultSpec.DropReply, FaultTarget.Command(id2));
        await Assert.ThrowsAsync<StoreOutcomeUnknownException>(() => c.BeginAsync(deferredLease, id2, notAfter));
        var deferred = await c.BeginAsync(deferredLease, id2, notAfter);
        Assert.Equal((StoreStatus.RateLimited, StoreEffect.Applied, true), (deferred.Reply.Status, deferred.Reply.Effect, deferred.Reply.Replayed));
        Assert.Equal(2, deferred.Deferred!.BlockingScopes.Count);
        Assert.Equal(deferred.Deferred.EligibleAt.ToUnixTimeMilliseconds(),
            await f.CountAsync("SELECT (extract(epoch FROM eligible_at) * 1000)::bigint FROM runwick.jobs WHERE job_id = @j", ("j", deferredLease.JobId)));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE outcome = 'admission_deferred'"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.queue_events WHERE event_kind = 'attempt_admission_deferred'"));
        Assert.Equal(2L, await RateSupport.Grants(f));
        await RateSupport.NoInvariantViolations(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-08")]
    public async Task A_crash_after_begin_keeps_the_grant_and_the_retry_needs_a_new_one()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("rate08"));
        var c = await RateSupport.OpenAsync(f);
        var peer = await f.StartPeer("peer-1");
        await peer.OpenRatedAsync(f.ClientConnectionString("peer-1"), f.Application);
        var job = (await c.Dispatcher.DispatchAsync(new RatedCrashJob("c"))).JobId.Value;

        // The peer's Begin commits, then the process is SIGKILLed before the reply.
        var reserved = await peer.SendAsync("reserve", new JsonObject { ["leaseMs"] = 2000 });
        Assert.Equal("Applied", reserved["status"]!.GetValue<string>());
        await peer.SendAsync("armFault", new JsonObject { ["point"] = nameof(FaultPoint.AfterCommitBeforeReply), ["action"] = "KillProcess", ["operation"] = Operations.Begin });
        var begin = peer.SendInBackground("begin", new JsonObject { ["lease"] = reserved["lease"]!.DeepClone() });
        await peer.WaitForEventAsync("atBarrier", timeout: Wait);
        peer.Kill();
        await Record.ExceptionAsync(() => begin);
        Assert.Equal(1L, await RateSupport.Grants(f));
        var firstGrant = DateTimeOffset.FromUnixTimeMilliseconds(
            await f.CountAsync("SELECT (extract(epoch FROM authorized_at) * 1000)::bigint FROM runwick.rate_grants"));

        // Recovery closes the crashed attempt and refunds nothing.
        ExpiredLease? expired = null;
        var deadline = DateTime.UtcNow + Wait;
        while (expired is null)
        {
            Assert.True(DateTime.UtcNow < deadline, "the crashed peer's lease never expired");
            await Task.Delay(100);
            expired = (await c.Store.ListExpiredLeasesAsync(c.Application, 10, CancellationToken.None)).SingleOrDefault(e => e.JobId == job);
        }

        var recovered = await c.RecoverAsync(expired);
        Assert.Equal(StoreStatus.Applied, recovered.Status);
        Assert.Equal(1L, await RateSupport.Grants(f));
        Assert.Equal(1, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));

        // The retry's Begin needs a grant of its own: deferred inside the window, admitted after it.
        LeaseHandle? retry = null;
        await Runwick.Harness.Engine.WorkerHost.WaitUntil(() => c.ReserveAsync().GetAwaiter().GetResult() is { Status: StoreStatus.Applied } r
            && (retry = r.Value!.Lease.Handle) is not null, TimeSpan.FromSeconds(10), "retry reservable");
        var second = await c.BeginAsync(retry!);
        if (second.Reply.Status == StoreStatus.RateLimited)
        {
            Assert.True(second.Reply.DecidedAt < firstGrant + RateJobs.CrashWindow);
            Assert.Equal(JobStates.Retrying, second.Deferred!.Position.State);
            Assert.Equal(1L, await RateSupport.Grants(f));
            await RateSupport.WaitUntilAllEligible(f);
            second = await c.BeginAsync(await RateSupport.ReserveAsync(c));
        }

        // A loaded host may reach the first Begin only after the window; then Applied is the correct first answer.
        Assert.Equal(StoreStatus.Applied, second.Reply.Status);
        Assert.Equal(2L, await RateSupport.Grants(f));
        Assert.Equal(2, await f.CountAsync("SELECT count(DISTINCT attempt_id) FROM runwick.rate_grants"));
        Assert.True(second.Reply.DecidedAt >= firstGrant + RateJobs.CrashWindow);
        Assert.Equal(2, await f.CountAsync("SELECT execution_count FROM runwick.jobs WHERE job_id = @j", ("j", job)));
        await RateSupport.NoInvariantViolations(f);
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-09")]
    public async Task Opposite_order_rate_scopes_lock_in_uuid_order_and_never_deadlock()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("rate09"));
        var c = await RateSupport.OpenAsync(f);
        var peer = await f.StartPeer("peer-1");
        await peer.OpenRatedAsync(f.ClientConnectionString("peer-1"), f.Application);
        var before = Deadlocks(c) + await RateSupport.PeerDeadlocks(peer);

        for (var i = 0; i < 12; i++)
        {
            // X declares (a, b), Y declares (b, a); fresh keys per round, limit 1 each.
            string a = $"a{i}", b = $"b{i}";
            await c.Dispatcher.DispatchAsync(new RatedOrderJob("x" + i, a, b));
            await c.Dispatcher.DispatchAsync(new RatedOrderJob("y" + i, b, a));
            var localLease = await RateSupport.ReserveAsync(c);
            var peerReserved = await peer.SendAsync("reserve");
            Assert.Equal("Applied", peerReserved["status"]!.GetValue<string>());
            var peerLease = PeerJson.Lease(peerReserved["lease"]!);
            var scopes = await TextAsync(f, """
                SELECT rate_scope_id::text FROM runwick.rate_scopes WHERE scope_key IN (@a, @b) ORDER BY rate_scope_id
                """, ("a", a), ("b", b));
            var high = Guid.Parse(scopes[1]);

            // An admin transaction holds the higher-UUID scope, so the first Begin holds only the lower one while it waits.
            await using var admin = new NpgsqlConnection(f.AdminConnectionString);
            await admin.OpenAsync();
            var holdTx = await admin.BeginTransactionAsync();
            using (var hold = new NpgsqlCommand("SELECT 1 FROM runwick.rate_scopes WHERE rate_scope_id = @id FOR UPDATE", admin, holdTx))
            {
                hold.Parameters.AddWithValue("id", high);
                await hold.ExecuteScalarAsync();
            }

            var localFirst = i % 2 == 0;
            string firstName = localFirst ? "rw:local" : "rw:peer-1", secondName = localFirst ? "rw:peer-1" : "rw:local";
            Task<string> firstTask, secondTask;
            FaultHandle? localBarrier = null;
            if (localFirst)
            {
                var id = Guid.CreateVersion7();
                localBarrier = f.ArmFault(FaultPoint.AfterLocksBeforeFreshTime, FaultSpec.Barrier, FaultTarget.Command(id));
                firstTask = c.BeginAsync(localLease, id).ContinueWith(t => t.Result.Reply.Status.ToString(), TaskScheduler.Default);
            }
            else
            {
                await peer.SendAsync("armFault", new JsonObject { ["point"] = nameof(FaultPoint.AfterLocksBeforeFreshTime), ["action"] = "Barrier", ["operation"] = Operations.Begin });
                firstTask = peer.SendInBackground("begin", new JsonObject { ["lease"] = PeerJson.Lease(peerLease) })
                    .ContinueWith(t => t.Result["status"]!.GetValue<string>(), TaskScheduler.Default);
            }

            await RateSupport.WaitForLockWaiter(f, firstName, "rw:harness:admin");

            // The opposite-order Begin queues behind the first one on the lower UUID, never on the admin-held higher one.
            secondTask = localFirst
                ? peer.SendInBackground("begin", new JsonObject { ["lease"] = PeerJson.Lease(peerLease) })
                    .ContinueWith(t => t.Result["status"]!.GetValue<string>(), TaskScheduler.Default)
                : c.BeginAsync(localLease).ContinueWith(t => t.Result.Reply.Status.ToString(), TaskScheduler.Default);
            await RateSupport.WaitForLockWaiter(f, secondName, firstName);
            Assert.All(await RateSupport.BlockersOf(f, secondName), name => Assert.StartsWith(firstName, name, StringComparison.Ordinal));

            await holdTx.CommitAsync();
            if (localFirst)
            {
                await DriverFixture.WaitAtBarrier(localBarrier!, Wait);
            }
            else
            {
                await peer.WaitForEventAsync("atBarrier", timeout: Wait);
            }

            await Task.Delay(100);
            Assert.False(secondTask.IsCompleted, "the second Begin did not wait for the first one's scope locks");
            if (localFirst)
            {
                DriverFixture.ReleaseBarrier(localBarrier!);
            }
            else
            {
                await peer.SendAsync("releaseBarrier", new JsonObject { ["point"] = nameof(FaultPoint.AfterLocksBeforeFreshTime) });
            }

            Assert.Equal(("Applied", "RateLimited"), (await WithTimeout(firstTask), await WithTimeout(secondTask)));

            // All or none: the first attempt holds one grant in each scope; the second charged nothing.
            var winner = localFirst ? localLease.AttemptId : peerLease.AttemptId;
            Assert.Equal(2, await f.CountAsync("""
                SELECT count(*) FROM runwick.rate_grants AS g JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
                WHERE s.scope_key IN (@a, @b) AND g.attempt_id = @w
                """, ("a", a), ("b", b), ("w", winner)));
            Assert.Equal(2, await f.CountAsync("""
                SELECT count(*) FROM runwick.rate_grants AS g JOIN runwick.rate_scopes AS s USING (application, rate_scope_id)
                WHERE s.scope_key IN (@a, @b)
                """, ("a", a), ("b", b)));
        }

        Assert.Equal(before, Deadlocks(c) + await RateSupport.PeerDeadlocks(peer));
        Assert.Equal(12, await f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE outcome = 'admission_deferred'"));
        await RateSupport.NoInvariantViolations(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "UPG-02")]
    public async Task After_activation_an_old_binary_cannot_begin_and_keeps_only_settlement_paths()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("upg02"));
        var c = await RateSupport.OpenAsync(f, activate: false);
        var old = await f.StartPeer("old-1");
        await old.OpenOldBinaryAsync(f.ClientConnectionString("old-1"), f.Application, "rate_admission");

        // Before activation the old binary is a compatible writer: it dispatches, reserves and begins.
        await old.SendAsync("dispatch", new JsonObject { ["jobKey"] = "p0" });
        var r0 = await old.SendAsync("reserve");
        var l0 = r0["lease"]!;
        Assert.Equal("Applied", (await old.SendAsync("begin", new JsonObject { ["lease"] = l0.DeepClone() }))["status"]!.GetValue<string>());
        await c.Dispatcher.DispatchAsync(new PingJob("p1"));
        var r1 = await old.SendAsync("reserve");
        Assert.Equal("Applied", r1["status"]!.GetValue<string>());
        var l1 = r1["lease"]!;

        // Activation holds the application row; the old binary's Begin waits on the gate and then observes the requirement.
        var h = f.ArmFault(FaultPoint.BeforeCommit, FaultSpec.Barrier, FaultTarget.Op(Operations.ActivateCapabilities));
        var activation = RateSupport.ActivateAsync(c, 1);
        await DriverFixture.WaitAtBarrier(h, Wait);
        var oldBegin = old.SendInBackground("begin", new JsonObject { ["lease"] = l1.DeepClone() });
        await RateSupport.WaitForLockWaiter(f, "rw:old-1");
        Assert.False(oldBegin.IsCompleted);
        DriverFixture.ReleaseBarrier(h);
        Assert.Equal(StoreStatus.Applied, (await WithTimeout(activation)).Status);
        var refused = await WithTimeout(oldBegin);
        Assert.Equal(("UnsupportedVersion", "capability_unsupported", "ObservedNoEffect"),
            (refused["status"]!.GetValue<string>(), refused["code"]!.GetValue<string>(), refused["effect"]!.GetValue<string>()));
        Assert.Equal("reserved/0", (await TextAsync(f, "SELECT state || '/' || execution_count FROM runwick.jobs WHERE job_id = @j",
            ("j", PeerJson.Lease(l1).JobId))).Single());

        // A rate-limited job: the old binary's Begin is refused at the gate with nothing charged; the new binary's is admitted.
        await c.Dispatcher.DispatchAsync(new RatedCrashJob("rated"));
        var rated = await RateSupport.ReserveAsync(c);
        var oldRated = await old.SendAsync("begin", new JsonObject { ["lease"] = PeerJson.Lease(rated) });
        Assert.Equal(("UnsupportedVersion", "capability_unsupported"), (oldRated["status"]!.GetValue<string>(), oldRated["code"]!.GetValue<string>()));
        Assert.Equal(0L, await RateSupport.Grants(f));
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(rated)).Reply.Status);
        Assert.Equal(1L, await RateSupport.Grants(f));

        // Every other creation path is closed to it; settlement of what it already owns stays open.
        Assert.Equal("capability_unsupported", (await old.SendAsync("reserve"))["code"]!.GetValue<string>());
        var jobsBefore = await Jobs(f);
        await Assert.ThrowsAsync<PeerCommandException>(() => old.SendAsync("dispatch", new JsonObject { ["jobKey"] = "late" }));
        Assert.Equal(jobsBefore, await Jobs(f));
        Assert.Equal("Applied", (await old.SendAsync("renew", new JsonObject { ["lease"] = l0.DeepClone() }))["status"]!.GetValue<string>());
        Assert.Equal("Applied", (await old.SendAsync("finish", new JsonObject { ["lease"] = l0.DeepClone() }))["status"]!.GetValue<string>());
        Assert.Equal("Applied", (await old.SendAsync("release", new JsonObject { ["lease"] = l1.DeepClone() }))["status"]!.GetValue<string>());
        await RateSupport.NoInvariantViolations(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "UPG-06")]
    public async Task Rollback_after_activation_is_rejected()
    {
        await using var f = await DriverFixture.StartAsync(RateSupport.Ns("upg06"));
        var c = await RateSupport.OpenAsync(f);
        static string Required(List<string> rows) => rows.Single();
        var required = Required(await TextAsync(f, "SELECT required_capabilities::text FROM runwick.application_controls"));
        var revision = await f.CountAsync("SELECT revision FROM runwick.application_controls");

        // Deactivation: a capability is never lowered.
        var lower = await RateSupport.ActivateAsync(c, 0);
        Assert.Equal((StoreStatus.InvalidRequest, StoreEffect.ObservedNoEffect), (lower.Status, lower.Effect));
        var same = await RateSupport.ActivateAsync(c, 1);
        Assert.Equal((StoreStatus.InvalidState, "no_change"), (same.Status, same.Error!.Code));

        // Binary rollback: the old binary cannot even complete its handshake (RegisterQueue is a creation path).
        var old = await f.StartPeer("old-1");
        var ex = await Assert.ThrowsAsync<PeerCommandException>(() => old.OpenOldBinaryAsync(f.ClientConnectionString("old-1"), f.Application, "rate_admission"));
        Assert.Contains("capability_unsupported", ex.Error, StringComparison.Ordinal);

        Assert.Equal(required, Required(await TextAsync(f, "SELECT required_capabilities::text FROM runwick.application_controls")));
        Assert.Contains("\"rate_admission\": 1", required, StringComparison.Ordinal);
        Assert.Equal(revision, await f.CountAsync("SELECT revision FROM runwick.application_controls"));
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM runwick.management_audit WHERE operation = 'ActivateCapabilities'"));

        // The current binary still runs rated work.
        await c.Dispatcher.DispatchAsync(new RatedCrashJob("after"));
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(await RateSupport.ReserveAsync(c))).Reply.Status);
    }
}

/// <summary>RATE-10: backward store time on the libfaketime server (07 I-9) never refills a window early.</summary>
public sealed class RateAdmissionClockTests
{
    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "SP-013")]
    [Trait("Case", "RATE-10")]
    [Trait("RequiresDocker", "true")]
    public async Task A_backward_store_clock_never_reverses_rate_time_or_refills_early()
    {
        if (Environment.GetEnvironmentVariable("RUNWICK_TEST_PG") is { Length: > 0 })
        {
            Assert.Skip("requires-docker");
        }

        await using var db = await TestDatabase.StartFakeTimeAsync();
        await Runwick.Postgres.Migrations.RunwickPostgresMigrations.MigrateAsync(db.For("rw:harness:migrate", pooling: false));
        var app = "clockrate";
        await using var c = await TestClient.CreateAsync(db.For("rw:local"), app, new HarnessFaultInjector(),
            r => r.AddJob<RatedCrashJob>().Named("test.rate.crash").ToQueue("default").RollingWindow(1, TimeSpan.FromSeconds(20)));
        Assert.Equal(StoreStatus.Applied, (await RateSupport.ActivateAsync(c, 1)).Status);
        await using var admin = new NpgsqlConnection(db.For("rw:harness:admin"));
        await admin.OpenAsync();
        async Task<string> Scalar(string sql)
        {
            using var cmd = new NpgsqlCommand(sql, admin);
            return Convert.ToString(await cmd.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture)!;
        }

        try
        {
            await c.Dispatcher.DispatchAsync(new RatedCrashJob("a"));
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(await RateSupport.ReserveAsync(c))).Reply.Status);
            var grant = long.Parse(await Scalar("SELECT (extract(epoch FROM authorized_at) * 1000)::bigint FROM runwick.rate_grants"), System.Globalization.CultureInfo.InvariantCulture);

            // The store clock steps 30 s back: by wall time the window looks empty, by logical time it is not.
            // (libfaketime applies the step to running backends at different moments; a statement caught across it can
            // trip a session timeout, which is a not-applied reply, so those calls retry.)
            await db.StepClockAsync(TimeSpan.FromSeconds(-30));
            await c.Dispatcher.DispatchAsync(new RatedCrashJob("b"));
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            LeaseHandle? lease = null;
            while (lease is null)
            {
                Assert.True(DateTime.UtcNow < deadline, "b never reservable");
                try
                {
                    var r = await c.ReserveAsync();
                    lease = r.Status == StoreStatus.Applied ? r.Value!.Lease.Handle : null;
                }
                catch (StoreNotAppliedException)
                {
                }

                await Task.Delay(lease is null ? 100 : 0);
            }

            var beginId = Guid.CreateVersion7();
            var beginNotAfter = DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30)).ToUnixTimeMilliseconds());
            BeginReply? reply = null;
            for (var attempt = 0; reply is null; attempt++)
            {
                try
                {
                    reply = await c.BeginAsync(lease, beginId, beginNotAfter);
                }
                catch (StoreNotAppliedException) when (attempt < 5)
                {
                    await Task.Delay(200);
                }
            }

            Assert.Equal(StoreStatus.RateLimited, reply.Reply.Status);
            Assert.Equal("1", await Scalar("SELECT count(*) FROM runwick.rate_grants"));
            var last = long.Parse(await Scalar("SELECT (extract(epoch FROM last_decision_at) * 1000)::bigint FROM runwick.rate_scopes"), System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(last >= grant, "logical rate time went backward");

            // The deferral is measured from the stepped clock: no earlier than the remaining logical window.
            var remaining = grant + 20_000 - last;
            Assert.True(reply.Deferred!.EligibleAt >= reply.Reply.DecidedAt + TimeSpan.FromMilliseconds(Math.Max(250, remaining)),
                $"eligible {reply.Deferred.EligibleAt:O} decided {reply.Reply.DecidedAt:O} remaining {remaining} ms");
        }
        finally
        {
            await db.StepClockAsync(TimeSpan.Zero);
        }
    }
}
