namespace Runwick.Registration;

/// <summary>One configuration layer's value for a scalar; repeated equal sets are idempotent.</summary>
internal sealed class LayerValue<T>
{
    readonly List<T> _conflicts = [];

    public bool IsSet { get; private set; }

    public T Value { get; private set; } = default!;

    public bool HasConflict => _conflicts.Count > 0;

    public IReadOnlyList<T> Conflicts => _conflicts;

    public void Set(T value)
    {
        if (IsSet && !EqualityComparer<T>.Default.Equals(Value, value))
        {
            _conflicts.Add(value);
            return;
        }

        IsSet = true;
        Value = value;
    }
}

/// <summary>Policy values a layer may set (application defaults and fluent share the shape).</summary>
internal sealed class PolicyLayer
{
    public LayerValue<string> Queue { get; } = new();
    public LayerValue<string> Priority { get; } = new();
    public LayerValue<int> MaxExecutions { get; } = new();
    /// <summary>Null value means "explicitly no timeout".</summary>
    public LayerValue<TimeSpan?> ExecutionTimeout { get; } = new();
    public LayerValue<(TimeSpan Initial, TimeSpan Max)> RetryDelay { get; } = new();
    public LayerValue<int> MaxUnstartedAbandonments { get; } = new();
    public LayerValue<(TimeSpan Initial, TimeSpan Max)> UnstartedDelay { get; } = new();
}
