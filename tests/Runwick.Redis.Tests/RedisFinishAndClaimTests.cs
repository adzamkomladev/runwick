using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Protocol;
using Runwick.ReferenceModel;
using Runwick.Redis.Store;

namespace Runwick.Redis.Tests;

/// <summary>
/// FinishAndClaim (05 §12.7, perf plan F): one call carries the Finish commands of finishing attempts and the ReserveAndBegin command that fills
/// their slots. Every command keeps its own ID, receipt and rules; the call is one intent, so both halves apply or neither does.
/// </summary>
[Trait("Category", "Contract")]
[Trait("Item", "PF2-F")]
public sealed class RedisFinishAndClaimTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    static async Task<(DriverFixture F, TestClient C)> OpenAsync(string name)
    {
        var f = await DriverFixture.StartAsync(name + "_" + Guid.NewGuid().ToString("N")[..8], StoreKind.Redis);
        return (f, await f.OpenClient(configure: r => TestJobs.Register(r)));
    }

    /// <summary>Dispatches the jobs and claims them all with one ReserveAndBegin; returns the begun leases in dispatch order.</summary>
    static async Task<LeaseHandle[]> RunningAsync(TestClient c, params IQueueJob[] jobs)
    {
        await PendingAsync(c, jobs);
        var claimed = await c.ReserveAndBeginAsync(jobs.Length);
        Assert.Equal(StoreStatus.Applied, claimed.Status);
        Assert.All(claimed.Value!.Outcomes, o => Assert.Equal(StoreStatus.Applied, o.Status));
        return [.. claimed.Value.Outcomes.Select(o => o.Begun!.Lease.Handle)];
    }

    static async Task PendingAsync(TestClient c, params IQueueJob[] jobs)
    {
        foreach (var job in jobs)
        {
            await c.Dispatcher.DispatchAsync((dynamic)job, cancellationToken: Ct);
            await Task.Delay(2, Ct);
        }
    }

    static async Task<StoreSnapshot> ModelAsync(DriverFixture f)
    {
        var model = (await f.ReadCommittedSnapshot()).Model;
        Assert.Empty(SnapshotInvariants.Violations(model));
        return model;
    }

    [Fact]
    [Trait("Case", "FC-01")]
    public async Task One_call_decides_the_finishes_and_the_claim_each_under_its_own_receipt()
    {
        var (f, c) = await OpenAsync("fc01");
        await using var _ = f;
        var running = await RunningAsync(c, new PingJob("a"), new PingJob("b"));
        await PendingAsync(c, new PingJob("c"), new PingJob("d"));

        var finishes = running.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var reply = await c.FinishAndClaimAsync(finishes, c.ClaimCommand(2));

        Assert.Equal((StoreStatus.Applied, StoreEffect.Applied, false), (reply.Status, reply.Effect, reply.Replayed));
        var value = reply.Value!;
        Assert.All(value.Finished!.Outcomes, o => Assert.Equal((StoreStatus.Applied, false), (o.Status, o.Replayed)));
        Assert.Equal(StoreStatus.Applied, value.Claim.Status);
        Assert.Equal(2, value.Claim.Value!.Outcomes.Count);
        Assert.All(value.Claim.Value.Outcomes, o => Assert.True(o.Status == StoreStatus.Applied && o.Begun is not null));

        var model = await ModelAsync(f);
        Assert.Equal(2, model.Jobs.Count(j => j.State == "succeeded"));
        Assert.Equal(2, model.Jobs.Count(j => j.State == "running"));
        Assert.Equal(2, model.Receipts.Count(r => r.Operation == "Finish"));
        Assert.Equal(2, model.Receipts.Count(r => r.Operation == "ReserveAndBegin")); // the first claim and this one
    }

    [Fact]
    [Trait("Case", "FC-02")]
    public async Task A_resent_call_replays_the_claim_alone_and_each_finish_replays_from_its_own_receipt()
    {
        var (f, c) = await OpenAsync("fc02");
        await using var _ = f;
        var running = await RunningAsync(c, new PingJob("a"), new PingJob("b"));
        await PendingAsync(c, new PingJob("c"), new PingJob("d"));
        var finishes = running.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var claim = c.ClaimCommand(2);
        var first = await c.FinishAndClaimAsync(finishes, claim);

        var again = await c.FinishAndClaimAsync(finishes, claim);
        Assert.Equal((StoreStatus.Applied, true), (again.Status, again.Replayed));
        Assert.Null(again.Value!.Finished); // the claim receipt holds only the claim
        Assert.Equal(first.Value!.Claim.Value!.Outcomes.Select(o => o.JobId), again.Value.Claim.Value!.Outcomes.Select(o => o.JobId));
        Assert.True(again.Value.Claim.Replayed);

        foreach (var finish in finishes)
        {
            var replay = await c.FinishAsync(finish);
            Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        }

        // The same claim command sent alone is the same command: it replays too.
        var alone = await c.ReserveAndBeginAsync(2, commandId: claim.CommandId, notAfter: claim.NotAfter);
        Assert.Equal((StoreStatus.Applied, true), (alone.Status, alone.Replayed));

        var model = await ModelAsync(f);
        Assert.Equal(2, model.Jobs.Count(j => j.State == "running"));
        Assert.Equal(4, model.Jobs.Count);
        Assert.Equal(2, model.Receipts.Count(r => r.Operation == "Finish"));
    }

    [Fact]
    [Trait("Case", "FC-03")]
    public async Task With_no_work_the_finishes_still_apply_and_no_claim_receipt_is_left()
    {
        var (f, c) = await OpenAsync("fc03");
        await using var _ = f;
        var running = await RunningAsync(c, new PingJob("a"));
        var finishes = running.Select(l => c.FinishCommand(l, AttemptOutcome.Succeeded)).ToList();
        var claim = c.ClaimCommand(1);

        var reply = await c.FinishAndClaimAsync(finishes, claim);
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal(StoreStatus.Applied, reply.Value!.Finished!.Outcomes[0].Status);
        Assert.Equal(StoreStatus.NoWork, reply.Value.Claim.Status);
        var model = await ModelAsync(f);
        Assert.Equal("succeeded", Assert.Single(model.Jobs).State);
        Assert.Equal(1, model.Receipts.Count(r => r.Operation == "ReserveAndBegin")); // only the setup claim

        // A resend finds the finish replayed and, with work now waiting, claims afresh: a no-effect claim leaves nothing to replay.
        await PendingAsync(c, new PingJob("b"));
        var resent = await c.FinishAndClaimAsync(finishes, claim);
        Assert.Equal((StoreStatus.Applied, false), (resent.Status, resent.Replayed));
        Assert.True(resent.Value!.Finished!.Outcomes[0].Replayed);
        Assert.Equal(StoreStatus.Applied, resent.Value.Claim.Status);
    }

    [Fact]
    [Trait("Case", "FC-04")]
    public async Task A_finish_with_a_stale_lease_is_refused_alone_and_the_claim_still_applies()
    {
        var (f, c) = await OpenAsync("fc04");
        await using var _ = f;
        var running = await RunningAsync(c, new PingJob("a"), new PingJob("b"), new PingJob("c"));
        await PendingAsync(c, new PingJob("d"));
        var finishes = new[]
        {
            c.FinishCommand(running[0], AttemptOutcome.Succeeded),
            c.FinishCommand(running[1] with { OwnershipToken = Guid.NewGuid() }, AttemptOutcome.Succeeded),
            c.FinishCommand(running[2], AttemptOutcome.Succeeded),
        };

        var reply = await c.FinishAndClaimAsync(finishes, c.ClaimCommand(1));
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal([StoreStatus.Applied, StoreStatus.StaleLease, StoreStatus.Applied], reply.Value!.Finished!.Outcomes.Select(o => o.Status));
        Assert.Equal(StoreStatus.Applied, reply.Value.Claim.Status);
        var model = await ModelAsync(f);
        Assert.Equal(2, model.Jobs.Count(j => j.State == "succeeded"));
        Assert.Equal(2, model.Jobs.Count(j => j.State == "running")); // the stale one and the claimed one
    }

    [Fact]
    [Trait("Case", "FC-05")]
    public async Task The_claim_sees_the_permit_that_a_finish_in_the_same_call_frees()
    {
        var (f, c) = await OpenAsync("fc05");
        await using var _ = f;
        var running = await RunningAsync(c, new ResourceJob("a", "r"));
        await PendingAsync(c, new ResourceJob("b", "r"));
        Assert.Equal(StoreStatus.CapacityBlocked, (await c.ReserveAndBeginAsync(1)).Status); // the resource has one permit and "a" holds it

        var reply = await c.FinishAndClaimAsync([c.FinishCommand(running[0], AttemptOutcome.Succeeded)], c.ClaimCommand(1));
        Assert.Equal(StoreStatus.Applied, reply.Status);
        Assert.Equal(StoreStatus.Applied, reply.Value!.Finished!.Outcomes[0].Status);
        Assert.Equal(StoreStatus.Applied, reply.Value.Claim.Status);
        var model = await ModelAsync(f);
        Assert.Equal(["running", "succeeded"], model.Jobs.Select(j => j.State).Order());
    }

    [Fact]
    [Trait("Case", "FC-06")]
    public async Task A_call_that_is_not_valid_is_refused_before_any_write()
    {
        var (f, c) = await OpenAsync("fc06");
        await using var _ = f;
        var running = await RunningAsync(c, new PingJob("a"));
        await PendingAsync(c, new PingJob("b"));
        var finish = c.FinishCommand(running[0], AttemptOutcome.Succeeded);

        // The same command ID twice, a finish for a lease twice and no finishes at all are invalid; nothing is decided.
        var claim = c.ClaimCommand(1);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishAndClaimAsync([finish, finish], claim)).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishAndClaimAsync([], claim)).Status);
        Assert.Equal(StoreStatus.InvalidRequest, (await c.FinishAndClaimAsync([finish], c.ClaimCommand(1, commandId: finish.CommandId))).Status);
        var model = await ModelAsync(f);
        Assert.Equal(["pending", "running"], model.Jobs.Select(j => j.State).Order());
    }

    [Fact]
    [Trait("Case", "FC-07")]
    public async Task Every_write_prefix_of_the_call_leaves_both_halves_applied_or_neither_and_the_resend_finishes_the_job()
    {
        var (f, c) = await OpenAsync("fc07");
        await using var _ = f;
        var store = c.Services.GetRequiredService<RedisStore>();
        var lease = (await RunningAsync(c, new PingJob("p0")))[0];
        var interrupted = 0;
        const int rounds = 40;
        for (var n = 1; n <= rounds; n++)
        {
            await PendingAsync(c, new PingJob("p" + n));
            var finish = c.FinishCommand(lease, AttemptOutcome.Succeeded);
            var claim = c.ClaimCommand(1);
            store.Runner.Client.FailAfter = n;
            try
            {
                await c.FinishAndClaimAsync([finish], claim);
            }
            catch (StoreOutcomeUnknownException)
            {
                interrupted++;
            }
            finally
            {
                store.Runner.Client.FailAfter = 0;
            }

            // The resend recovers a pending intent first. Whatever the first call did, the job finishes once and the next one starts once.
            var again = await c.FinishAndClaimAsync([finish], claim);
            Assert.Equal(StoreStatus.Applied, again.Status);
            Assert.Equal(StoreStatus.Applied, again.Value!.Claim.Status);
            var begun = Assert.Single(again.Value.Claim.Value!.Outcomes).Begun!;
            lease = begun.Lease.Handle;
            var replay = await c.FinishAsync(finish);
            Assert.Equal((StoreStatus.Applied, true), (replay.Status, replay.Replayed));
        }

        Assert.True(interrupted > 5, $"only {interrupted} interrupted calls");
        var model = await ModelAsync(f);
        Assert.Equal(rounds + 1, model.Jobs.Count);
        Assert.Equal(rounds, model.Jobs.Count(j => j.State == "succeeded"));
        Assert.Equal(1, model.Jobs.Count(j => j.State == "running"));
        Assert.Equal(rounds, model.Receipts.Count(r => r.Operation == "Finish"));
        Assert.Equal(rounds + 1, model.Receipts.Count(r => r.Operation == "ReserveAndBegin"));
    }
}
