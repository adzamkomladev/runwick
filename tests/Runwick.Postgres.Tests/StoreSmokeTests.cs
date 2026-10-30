using Runwick.Harness.Store;
using Runwick.Harness.Jobs;
using Runwick.Protocol;
using Runwick.ReferenceModel;

namespace Runwick.Postgres.Tests;

[Trait("Category", "Contract")]
[Trait("Item", "SP-005")]
public sealed class StoreSmokeTests
{
    [Fact]
    [Trait("Case", "INIT-01")]
    public async Task Dispatch_reserve_begin_finish_round_trip()
    {
        await using var f = await DriverFixture.StartAsync("smoke_" + Guid.NewGuid().ToString("N")[..8]);
        await using var client = await f.OpenClient(configure: r => TestJobs.Register(r));

        var receipt = await client.Dispatcher.DispatchAsync(new PingJob("a"), cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(DispatchDisposition.Created, receipt.Disposition);

        var reserved = await client.ReserveAsync();
        Assert.Equal(StoreStatus.Applied, reserved.Status);
        Assert.Equal(receipt.JobId.Value, reserved.Value!.Position.JobId);

        var begun = await client.BeginAsync(reserved.Value.Lease.Handle);
        Assert.Equal(StoreStatus.Applied, begun.Reply.Status);
        Assert.Equal(1, begun.Reply.Value!.ExecutionNumber);

        var renewed = await client.RenewAsync(reserved.Value.Lease.Handle);
        Assert.Equal(StoreStatus.Applied, renewed.Status);

        var finished = await client.FinishAsync(reserved.Value.Lease.Handle, AttemptOutcome.Succeeded);
        Assert.Equal(StoreStatus.Applied, finished.Status);
        Assert.Equal("succeeded", finished.Value!.Disposition);

        var snapshot = await f.ReadCommittedSnapshot();
        Assert.Empty(SnapshotInvariants.Violations(snapshot.Model));
        Assert.Equal("succeeded", snapshot.Model.Job(receipt.JobId.Value).State);

        var none = await client.ReserveAsync();
        Assert.Equal(StoreStatus.NoWork, none.Status);
    }
}
