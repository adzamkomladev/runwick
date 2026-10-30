namespace Runwick.Execution;

/// <summary>
/// The <c>exponential-v1</c> evaluator with equal jitter. Pure: the caller supplies the entropy,
/// so a store can draw once inside its transaction and persist the choice in the receipt.
/// </summary>
internal static class RetryEvaluator
{
    public const string Name = "exponential-v1";
    public const int Version = 1;
    public const string EqualJitter = "equal";

    /// <summary>Nominal delay b = min(max, initial · 2^(n−1)) in whole milliseconds, saturating.</summary>
    public static long NominalMilliseconds(int n, TimeSpan initial, TimeSpan maximum)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(n, 1);
        var initialMs = CeilMilliseconds(initial);
        var maxMs = CeilMilliseconds(maximum);
        var shift = n - 1;
        if (shift >= 62 || initialMs > (long.MaxValue >> shift))
        {
            return maxMs;
        }

        return Math.Min(maxMs, initialMs << shift);
    }

    /// <summary>
    /// Equal jitter: an integer delay uniformly drawn from ceil(b/2) through b inclusive.
    /// <paramref name="entropy"/> is a uniformly distributed 64-bit value.
    /// </summary>
    public static TimeSpan Delay(int n, TimeSpan initial, TimeSpan maximum, ulong entropy)
    {
        var b = NominalMilliseconds(n, initial, maximum);
        var low = (b + 1) / 2;
        var span = (ulong)(b - low + 1);
        var pick = low + (long)(entropy % span);
        return TimeSpan.FromMilliseconds(pick);
    }

    /// <summary>
    /// A middleware-requested delay replaces the exponential draw: negative is a contract fault,
    /// rounded up to the millisecond quantum, zero becomes 1 ms, capped at the maximum, no jitter.
    /// </summary>
    public static TimeSpan Requested(TimeSpan requested, TimeSpan maximum)
    {
        if (requested < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(requested), "A requested retry delay cannot be negative.");
        }

        var ms = Math.Max(1, CeilMilliseconds(requested));
        return TimeSpan.FromMilliseconds(Math.Min(ms, CeilMilliseconds(maximum)));
    }

    static long CeilMilliseconds(TimeSpan value) =>
        (value.Ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;
}
