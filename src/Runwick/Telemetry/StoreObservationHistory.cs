using Runwick.Management;

namespace Runwick;

/// <summary>Management-host history. The caller serializes writes and copies snapshots under the descriptor cache lock.</summary>
internal sealed class StoreObservationHistory
{
    internal const int Capacity = 12;
    readonly Queue<StoreCheckObservation> _checks = new(Capacity);
    readonly Queue<StoreTrendSample> _samples = new(Capacity);
    StoreCheckStatus? _lastStatus;
    Guid? _storeId;
    (StoreTrendMetric Metric, StoreSnapshotScope Scope)? _series;

    public void RecordCheck(DateTimeOffset at, StoreCheckStatus status)
    {
        if (_lastStatus != status)
        {
            Add(_checks, new(StoreCheckSource.HostCompatibilityCheck, at, status));
            _lastStatus = status;
        }
    }

    public void SelectStore(Guid storeId)
    {
        if (_storeId != storeId)
        {
            _storeId = storeId;
            _series = null;
            _samples.Clear();
        }
    }

    public void RecordSample(Guid storeId, DateTimeOffset at, StoreTrendMetric metric, StoreSnapshotScope scope, long? value)
    {
        if (_storeId != storeId)
        {
            return;
        }

        if (_series != (metric, scope))
        {
            _series = (metric, scope);
            _samples.Clear();
        }

        Add(_samples, new(at, value));
    }

    public void RecordGap(DateTimeOffset at)
    {
        if (_series is not null)
        {
            Add(_samples, new(at, null));
        }
    }

    public (IReadOnlyList<StoreCheckObservation> Checks, StoreTrend? Trend) Snapshot(Guid? trendStoreId) =>
        (_checks.ToArray(), trendStoreId is not null && trendStoreId == _storeId && _series is { } series
            ? new(series.Metric, series.Scope, _samples.ToArray()) : null);

    static void Add<T>(Queue<T> queue, T item)
    {
        if (queue.Count == Capacity)
        {
            queue.Dequeue();
        }

        queue.Enqueue(item);
    }
}
