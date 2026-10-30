using Runwick.Execution;

namespace Runwick.Core.Tests;

/// <summary>
/// TIME-05 (evaluator half): exponential-v1 with equal jitter and the requested-delay rules
/// (06 F-1). The persistence half is in Postgres.Tests.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-008")]
[Trait("Case", "TIME-05")]
public sealed class RetryEvaluatorTests
{
    static readonly TimeSpan Initial = TimeSpan.FromSeconds(5);
    static readonly TimeSpan Max = TimeSpan.FromSeconds(60);

    static (long Low, long High) Range(int n, TimeSpan initial, TimeSpan max) =>
        ((long)RetryEvaluator.Delay(n, initial, max, 0).TotalMilliseconds, (long)RetryEvaluator.Delay(n, initial, max, Span(n, initial, max) - 1).TotalMilliseconds);

    static ulong Span(int n, TimeSpan initial, TimeSpan max)
    {
        var b = RetryEvaluator.NominalMilliseconds(n, initial, max);
        return (ulong)(b - (b + 1) / 2 + 1);
    }

    [Theory]
    [InlineData(1, 2_500, 5_000)]
    [InlineData(2, 5_000, 10_000)]
    [InlineData(4, 20_000, 40_000)]
    [InlineData(5, 30_000, 60_000)]
    [InlineData(64, 30_000, 60_000)]
    [InlineData(int.MaxValue, 30_000, 60_000)]
    public void Delay_lies_in_the_equal_jitter_band(int n, long low, long high)
    {
        Assert.Equal((low, high), Range(n, Initial, Max));
        for (var i = 0; i < 2_000; i++)
        {
            var ms = (long)RetryEvaluator.Delay(n, Initial, Max, (ulong)Random.Shared.NextInt64() * 3UL).TotalMilliseconds;
            Assert.InRange(ms, low, high);
        }
    }

    [Fact]
    public void Nominal_saturates_at_the_maximum_without_overflow()
    {
        Assert.Equal(60_000, RetryEvaluator.NominalMilliseconds(5, Initial, Max));
        Assert.Equal(60_000, RetryEvaluator.NominalMilliseconds(64, Initial, Max));
        var huge = TimeSpan.FromMilliseconds(long.MaxValue / TimeSpan.TicksPerMillisecond / 2);
        var hugeMs = (long)huge.TotalMilliseconds;
        Assert.Equal(3L << 39, RetryEvaluator.NominalMilliseconds(40, TimeSpan.FromMilliseconds(3), huge));
        foreach (var n in new[] { 61, 62, 63, int.MaxValue })
        {
            Assert.Equal(hugeMs, RetryEvaluator.NominalMilliseconds(n, TimeSpan.FromMilliseconds(3), huge));
        }
    }

    [Fact]
    public void Sub_second_initial_rounds_up()
    {
        Assert.Equal((3L, 5L), Range(1, TimeSpan.FromMilliseconds(5), Max));
        Assert.Equal((1L, 1L), Range(1, TimeSpan.FromTicks(1), Max));
    }

    [Fact]
    public void Same_entropy_gives_the_same_delay()
    {
        const ulong entropy = 0x9E37_79B9_7F4A_7C15;
        Assert.Equal(RetryEvaluator.Delay(3, Initial, Max, entropy), RetryEvaluator.Delay(3, Initial, Max, entropy));
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1.5, 2)]
    [InlineData(7_000, 7_000)]
    [InlineData(90_000, 60_000)]
    [InlineData(120_000, 60_000)]
    public void Requested_delay_is_rounded_up_floored_and_capped_without_jitter(double requestedMs, long expectedMs)
    {
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), RetryEvaluator.Requested(TimeSpan.FromMilliseconds(requestedMs), Max));
        }
    }

    [Fact]
    public void Negative_requested_delay_is_a_contract_fault() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => RetryEvaluator.Requested(TimeSpan.FromMilliseconds(-1), Max));
}
