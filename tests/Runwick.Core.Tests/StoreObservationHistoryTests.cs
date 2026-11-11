using Runwick.Management;

namespace Runwick.Core.Tests;

public sealed class StoreObservationHistoryTests
{
    [Fact]
    public void Checks_record_only_state_changes_and_drop_the_oldest()
    {
        var history = new StoreObservationHistory();
        var start = DateTimeOffset.UnixEpoch;
        history.RecordCheck(start, StoreCheckStatus.Reachable);
        history.RecordCheck(start.AddSeconds(10), StoreCheckStatus.Reachable);
        history.RecordCheck(start.AddSeconds(20), StoreCheckStatus.Incompatible);
        history.RecordCheck(start.AddSeconds(30), StoreCheckStatus.Reachable);
        var (checks, _) = history.Snapshot(null);
        Assert.Equal([0, 20, 30], checks.Select(c => (c.ObservedAt - start).TotalSeconds));
        Assert.Equal([StoreCheckStatus.Reachable, StoreCheckStatus.Incompatible, StoreCheckStatus.Reachable], checks.Select(c => c.Status));
        Assert.All(checks, check => Assert.Equal(StoreCheckSource.HostCompatibilityCheck, check.Source));

        for (var i = 1; i <= StoreObservationHistory.Capacity; i++)
        {
            history.RecordCheck(start.AddMinutes(i), i % 2 == 0 ? StoreCheckStatus.Reachable : StoreCheckStatus.Unavailable);
        }

        var (recent, _) = history.Snapshot(null);
        Assert.Equal(StoreObservationHistory.Capacity, recent.Count);
        Assert.Equal(start.AddMinutes(1), recent[0].ObservedAt);
        Assert.Equal(3, checks.Count);
        Assert.Empty(new StoreObservationHistory().Snapshot(null).Checks);
    }

    [Fact]
    public void Gauge_history_keeps_gaps_and_never_crosses_logical_store_identity()
    {
        var history = new StoreObservationHistory();
        var store = Guid.NewGuid();
        var next = Guid.NewGuid();
        var start = DateTimeOffset.UnixEpoch;
        history.SelectStore(store);
        for (var i = 0; i < StoreObservationHistory.Capacity; i++)
        {
            history.RecordSample(store, start.AddSeconds(i * 10), StoreTrendMetric.PostgresLockWaits, StoreSnapshotScope.Store, i);
        }

        history.RecordGap(start.AddSeconds(StoreObservationHistory.Capacity * 10));
        var trend = Assert.IsType<StoreTrend>(history.Snapshot(store).Trend);
        Assert.Equal(StoreObservationHistory.Capacity, trend.Samples.Count);
        Assert.Equal(1L, trend.Samples[0].Value);
        Assert.Null(trend.Samples[^1].Value);
        Assert.Null(history.Snapshot(null).Trend);
        Assert.Null(history.Snapshot(next).Trend);

        history.SelectStore(next);
        history.RecordSample(store, start, StoreTrendMetric.PostgresLockWaits, StoreSnapshotScope.Store, 42);
        Assert.Null(history.Snapshot(next).Trend);
        history.RecordSample(next, start, StoreTrendMetric.PostgresLockWaits, StoreSnapshotScope.Store, 0);
        Assert.Equal(0L, Assert.Single(history.Snapshot(next).Trend!.Samples).Value);
    }
}
