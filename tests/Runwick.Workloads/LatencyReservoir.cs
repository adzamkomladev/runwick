namespace Runwick.Workloads;

/// <summary>Uniform reservoir sample (Algorithm R) with exact count/min/max.</summary>
public sealed class LatencyReservoir(int capacity = 100_000, int seed = 17)
{
    readonly double[] _samples = new double[capacity];
    readonly Random _random = new(seed);
    readonly Lock _gate = new();
    long _count;
    double _sum;
    double _min = double.MaxValue;
    double _max;

    public long Count { get { lock (_gate) { return _count; } } }

    public void Add(double milliseconds)
    {
        lock (_gate)
        {
            _count++;
            _sum += milliseconds;
            _min = Math.Min(_min, milliseconds);
            _max = Math.Max(_max, milliseconds);
            if (_count <= _samples.Length)
            {
                _samples[_count - 1] = milliseconds;
            }
            else
            {
                var slot = _random.NextInt64(_count);
                if (slot < _samples.Length)
                {
                    _samples[slot] = milliseconds;
                }
            }
        }
    }

    public double Mean { get { lock (_gate) { return _count == 0 ? 0 : _sum / _count; } } }

    public double Quantile(double q)
    {
        lock (_gate)
        {
            var n = (int)Math.Min(_count, _samples.Length);
            if (n == 0)
            {
                return 0;
            }

            var sorted = _samples.AsSpan(0, n).ToArray();
            Array.Sort(sorted);
            return sorted[Math.Clamp((int)Math.Ceiling(q * n) - 1, 0, n - 1)];
        }
    }

    public Summary Summarize()
    {
        lock (_gate)
        {
            var n = (int)Math.Min(_count, _samples.Length);
            var sorted = _samples.AsSpan(0, n).ToArray();
            Array.Sort(sorted);
            double P(double q) => n == 0 ? 0 : sorted[Math.Clamp((int)Math.Ceiling(q * n) - 1, 0, n - 1)];
            return new Summary(_count, n == 0 ? 0 : _min, _max, P(0.50), P(0.95), P(0.99), P(0.999));
        }
    }

    public sealed record Summary(long Count, double MinMs, double MaxMs, double P50Ms, double P95Ms, double P99Ms, double P999Ms);
}
