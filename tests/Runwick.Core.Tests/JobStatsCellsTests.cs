using Runwick.Management;
using Runwick.Protocol;

namespace Runwick.Core.Tests;

/// <summary>JST-07: the store-independent half of GetJobStats (16 §2.4): slices, percentiles and grouping of per-job rows.</summary>
[Trait("Category", "Core")]
[Trait("Item", "JST")]
public sealed class JobStatsCellsTests
{
    static readonly DateTimeOffset T0 = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000);

    [Fact]
    [Trait("Case", "JST-07")]
    public void Slices_are_half_open_and_their_starts_bound_them()
    {
        var f = new JobStatsFilter(T0, T0.AddMilliseconds(1_000), 3, null, null, null, null, false);
        Assert.Equal((0, 0, 1, 2), (f.Slice(T0), f.Slice(T0.AddMilliseconds(333)), f.Slice(T0.AddMilliseconds(334)), f.Slice(T0.AddMilliseconds(999))));
        Assert.Equal([T0, T0.AddMilliseconds(334), T0.AddMilliseconds(667)], Enumerable.Range(0, 3).Select(f.SliceStart));
        Assert.True(f.Contains(T0) && !f.Contains(T0.AddMilliseconds(1_000)));
        Assert.True(new JobStatsFilter(T0, T0.AddDays(1), 24, "a", null, null, ["a", "b"], false).Allows("a"));
        Assert.False(new JobStatsFilter(T0, T0.AddDays(1), 24, null, null, null, ["b"], false).Allows("a"));
    }

    [Fact]
    [Trait("Case", "JST-07")]
    public void Percentiles_interpolate_as_percentile_cont()
    {
        Assert.Equal(2.5, JobStatsCells.Percentile([1, 2, 3, 4], 0.5));
        Assert.Equal(3.85, JobStatsCells.Percentile([1, 2, 3, 4], 0.95), 10);
        Assert.Equal(7, JobStatsCells.Percentile([7], 0.95));
    }

    [Fact]
    [Trait("Case", "JST-07")]
    public void Rows_group_into_cells_and_codes_rank_by_count_then_name()
    {
        var f = new JobStatsFilter(T0, T0.AddMilliseconds(100), 2, null, null, null, null, true);
        Guid a = Guid.CreateVersion7(), b = Guid.CreateVersion7();
        var cells = JobStatsCells.From(f,
            [("q", "n", "succeeded", T0, 3L, a), ("q", "n", "pending", T0.AddMilliseconds(60), 0L, b)],
            [("q", "n", "succeeded", T0.AddMilliseconds(10), 10.0), ("q", "n", "cancelled", T0.AddMilliseconds(70), null)],
            [("q", "n", "pending", T0.AddMilliseconds(60)), ("q", "n", "scheduled", T0.AddDays(1))],
            ["z", "y", "z"]);
        Assert.Equal(2L, cells.Created.Single(c => c.State == "succeeded").Retries);
        Assert.Equal([0, 1], cells.Created.Select(c => c.Slice).Order());
        Assert.Equal([0, 1], cells.Ended.Select(c => c.Slice).Order());
        var overall = cells.Durations.Single(d => d.Queue is null && d.JobName is null && d.Slice is null);
        Assert.Equal((1L, 10.0, 10.0), (overall.Count, overall.MedianMs, overall.MaxMs));
        Assert.Contains(cells.Durations, d => d.Queue == "q" && d.Slice == 0);
        Assert.Equal((b, "pending"), (cells.Last.Single().JobId, cells.Last.Single().State));
        Assert.Equal(T0.AddMilliseconds(60), cells.Live.Single(l => l.Kind == "pending").OldestEligibleAt);
        Assert.Null(cells.Live.Single(l => l.Kind == "scheduled").OldestEligibleAt);
        Assert.Equal([new JobErrorCount("z", 2), new JobErrorCount("y", 1)], cells.ErrorCodes);
        Assert.Equal(["a", "b"], JobStatsCells.TopCodes([new("b", 1), new("a", 1)]).Select(c => c.Code));
    }
}
