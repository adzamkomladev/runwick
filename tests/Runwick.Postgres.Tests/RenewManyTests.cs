using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// RenewMany/1 (performance plan PF-06, 03/04/05 "Changes (2026-11-02, PF-06)"): per-lease fencing, per-lease outcomes
/// that never affect another lease, whole-command refusals that touch nothing, and renewal waves sent as one call.
/// </summary>
public sealed class RenewManyTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(180);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task<string> ExpiryAsync(DriverFixture f, Guid job) =>
        (await TextAsync(f, "SELECT lease_expires_at::text FROM runwick.jobs WHERE job_id = @j", ("j", job))).Single();

    static async Task<List<string>> PermitExpiriesAsync(DriverFixture f, Guid job) =>
        await TextAsync(f, "SELECT lease_expires_at::text FROM runwick.concurrency_permits WHERE job_id = @j ORDER BY scope_id", ("j", job));

    static async Task<List<LeaseHandle>> ReserveResourceJobsAsync(TestClient c, int n, string prefix, TimeSpan? lease = null)
    {
        var leases = new List<LeaseHandle>();
        for (var i = 0; i < n; i++)
        {
            await c.Dispatcher.DispatchAsync(new ResourceJob(prefix + i, prefix + "-r" + i));
        }

        for (var i = 0; i < n; i++)
        {
            var reserved = await c.ReserveAsync(lease: lease);
            Assert.Equal(StoreStatus.Applied, reserved.Status);
            leases.Add(reserved.Value!.Lease.Handle);
        }

        return leases;
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RENEWMANY-01")]
    public async Task Each_lease_is_fenced_by_its_own_row()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rm01"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var leases = await ReserveResourceJobsAsync(c, 4, "f");
        Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(leases[1])).Reply.Status);

        // Wrong token, wrong worker, wrong generation, unknown job: each refused exactly as Renew/1 refuses it.
        var wrongToken = leases[2] with { OwnershipToken = Guid.NewGuid() };
        var wrongGeneration = leases[3] with { Generation = leases[3].Generation + 1 };
        var wrongWorker = leases[0] with { WorkerInstanceId = Guid.NewGuid() };
        var unknown = leases[0] with { JobId = Guid.NewGuid() };
        var before = new Dictionary<Guid, (string Job, List<string> Permits)>();
        foreach (var l in leases)
        {
            before[l.JobId] = (await ExpiryAsync(f, l.JobId), await PermitExpiriesAsync(f, l.JobId));
        }

        var reply = await c.RenewManyAsync([leases[1], wrongToken, wrongGeneration, unknown], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal(4, o.Count);
        Assert.Equal((StoreStatus.Applied, (string?)null), (o[0].Status, o[0].Code));
        Assert.Equal((StoreStatus.StaleLease, "ownership_mismatch"), (o[1].Status, o[1].Code));
        Assert.Equal((StoreStatus.StaleLease, "ownership_mismatch"), (o[2].Status, o[2].Code));
        Assert.Equal((StoreStatus.NotFound, "job_not_found"), (o[3].Status, o[3].Code));
        Assert.All(o.Skip(1), x => Assert.Null(x.Value));

        // The renewed lease: the job and its permit carry the new expiry; the refused leases' rows are untouched.
        var renewed = o[0].Value!.Lease.ExpiresAt;
        Assert.True(renewed > reply.DecidedAt + TimeSpan.FromSeconds(59));
        var after = await ExpiryAsync(f, leases[1].JobId);
        Assert.NotEqual(before[leases[1].JobId].Job, after);
        Assert.All(await PermitExpiriesAsync(f, leases[1].JobId), p => Assert.Equal(after, p));
        foreach (var l in new[] { leases[0], leases[2], leases[3] })
        {
            Assert.Equal(before[l.JobId].Job, await ExpiryAsync(f, l.JobId));
            Assert.Equal(before[l.JobId].Permits, await PermitExpiriesAsync(f, l.JobId));
        }

        // A wrong worker instance on an otherwise valid lease is also a per-lease refusal.
        var worker = await c.RenewManyAsync([wrongWorker, leases[2]], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, worker.Status);
        Assert.Equal(StoreStatus.StaleLease, worker.Value!.Outcomes[0].Status);
        Assert.Equal(StoreStatus.Applied, worker.Value.Outcomes[1].Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.RenewManyAsync([leases[0], leases[0]])).Status);

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "RENEWMANY-02")]
    public async Task A_refused_lease_never_affects_the_others()
    {
        await using var f = await DriverFixture.StartAsync(Ns("rm02"));
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var healthy = await ReserveResourceJobsAsync(c, 3, "h");
        var expiring = (await ReserveResourceJobsAsync(c, 1, "x", TimeSpan.FromSeconds(1)))[0];
        var broken = (await ReserveResourceJobsAsync(c, 1, "b"))[0];
        var finished = (await ReserveResourceJobsAsync(c, 1, "d"))[0];
        await c.BeginAsync(healthy[0]);
        await c.BeginAsync(finished);
        Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(finished, AttemptOutcome.Succeeded)).Status);

        // A missing permit (the invariant Renew/1 reports as permit_set_mismatch), a finished attempt and a lapsed
        // lease sit between healthy leases in the same command.
        await f.ExecAsync("DELETE FROM runwick.concurrency_permits WHERE job_id = @j", ("j", broken.JobId));
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        var brokenBefore = await ExpiryAsync(f, broken.JobId);
        var expiringBefore = await ExpiryAsync(f, expiring.JobId);

        var reply = await c.RenewManyAsync([healthy[0], broken, healthy[1], finished, expiring, healthy[2]], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, reply.Status);
        var o = reply.Value!.Outcomes;
        Assert.Equal(StoreStatus.Applied, o[0].Status);
        Assert.Equal((StoreStatus.InvariantFault, "permit_set_mismatch"), (o[1].Status, o[1].Code));
        Assert.Equal(StoreStatus.Applied, o[2].Status);
        Assert.Contains(o[3].Status, new[] { StoreStatus.InvalidState, StoreStatus.StaleLease });
        Assert.Equal((StoreStatus.StaleLease, "lease_expired"), (o[4].Status, o[4].Code));
        Assert.Equal(StoreStatus.Applied, o[5].Status);

        foreach (var (lease, outcome) in new[] { (healthy[0], o[0]), (healthy[1], o[2]), (healthy[2], o[5]) })
        {
            var expiry = await ExpiryAsync(f, lease.JobId);
            Assert.All(await PermitExpiriesAsync(f, lease.JobId), p => Assert.Equal(expiry, p));
            Assert.True(outcome.Value!.Lease.ExpiresAt > reply.DecidedAt + TimeSpan.FromSeconds(59));
        }

        Assert.Equal(brokenBefore, await ExpiryAsync(f, broken.JobId));
        Assert.Equal(expiringBefore, await ExpiryAsync(f, expiring.JobId));

        // Every lease refused: Applied with no effect.
        var none = await c.RenewManyAsync([broken, expiring], TimeSpan.FromSeconds(60));
        Assert.Equal(StoreStatus.Applied, none.Status);
        Assert.DoesNotContain(none.Value!.Outcomes, x => x.Status == StoreStatus.Applied);

        // A whole-command refusal (expired NotAfter) applies nothing to any lease.
        var healthyBefore = await ExpiryAsync(f, healthy[1].JobId);
        var expired = await c.RenewManyAsync([healthy[1], healthy[2]], TimeSpan.FromSeconds(60), notAfter: DateTimeOffset.UtcNow.AddSeconds(-1));
        Assert.Equal(StoreStatus.CommandExpired, expired.Status);
        Assert.Equal(healthyBefore, await ExpiryAsync(f, healthy[1].JobId));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands WHERE operation = 'RenewMany'"));

        // The healthy leases still settle normally afterwards.
        foreach (var l in healthy.Skip(1))
        {
            Assert.Equal(StoreStatus.Applied, (await c.BeginAsync(l)).Reply.Status);
        }

        foreach (var l in healthy)
        {
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(l, AttemptOutcome.Succeeded)).Status);
        }

        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Fault")]
    [Trait("Item", "PF-06")]
    [Trait("Case", "CAP-10")]
    public async Task Renewal_waves_go_out_as_one_call_per_wave()
    {
        // CAP-10's shape: 16 long handlers started together, so renewals fall due together; RenewMany turns each wave
        // into one call. Every attempt keeps renewing, nothing is lost, and the renewal lane bound still holds.
        await using var f = await DriverFixture.StartAsync(Ns("rmwave"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, 16);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        for (var i = 0; i < 16; i++)
        {
            w.Script.On("wave" + i, (_, _, _) => release.Task);
            await w.DispatchAsync("wave" + i);
        }

        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 16, Wait, "16 long handlers");
        var maxRenewal = 0;
        using var stop = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                maxRenewal = Math.Max(maxRenewal, w.Engine.RenewalIo.Held);
                await Task.Delay(5);
            }
        });

        await WorkerHost.WaitUntil(() => w.Engine.Entries.All(e => e.Renewals >= 2), Wait, "two renewals per attempt");
        await stop.CancelAsync();
        await sampler;
        var renewals = w.Engine.Entries.Sum(e => e.Renewals);
        var waves = Interlocked.Read(ref w.Engine.Lease.RenewWavesSent);
        TestContext.Current.TestOutputHelper?.WriteLine($"renewals {renewals}, waves {waves}, renews sent {w.Engine.Lease.RenewsSent}");
        Assert.True(waves >= 1, "no renewal wave was batched");
        Assert.InRange(maxRenewal, 1, 4);
        Assert.DoesNotContain(w.Events, e => e.Name.StartsWith("lost:", StringComparison.Ordinal));

        release.SetResult();
        await CapacityFaultTests.WaitCount(f, "SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'", 16, "all 16 succeeded");
        await w.StopAsync();
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM (SELECT job_id FROM runwick.job_attempts GROUP BY job_id HAVING count(*) > 1) x"));
        Assert.Empty(await f.HistoryViolations());
    }
}
