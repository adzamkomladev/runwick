using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>The part of WV-10 (02 §13) that needs a caller transaction: a rolled-back staged dispatch. The other parts run on every store.</summary>
[Trait("Item", "SP-028")]
public sealed class WireVectorTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    [Trait("Case", "WV-10")]
    public async Task A_rolled_back_staged_dispatch_leaves_no_job_binding_or_receipt()
    {
        await using var f = await DriverFixture.StartAsync("wv10pg_" + Guid.NewGuid().ToString("N")[..8]);
        await using var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var options = new DispatchOptions { Uniqueness = new DispatchUniqueness("user:42", "w", TimeSpan.FromHours(1)) };
        var prepared = c.Prepared.Prepare(new PingJob("staged"), options);

        await using (var conn = await OpenBusinessAsync(f))
        {
            await using var tx = await conn.BeginTransactionAsync(Ct);
            var staged = await Staging(c).StageAsync(prepared, tx, Ct);
            Assert.NotNull(staged);
            await tx.RollbackAsync(Ct);
        }

        Assert.Equal(0, await Jobs(f));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.dispatch_keys"));
        Assert.Equal(0, await Receipts(f, prepared.OperationId.Value));

        // The same prepared operation then dispatches normally: nothing of the rolled-back attempt is left to collide with.
        var receipt = await c.Prepared.DispatchAsync(prepared, Ct);
        Assert.Equal(DispatchDisposition.Created, receipt.Disposition);
        Assert.Equal(1, await Jobs(f));
    }
}
