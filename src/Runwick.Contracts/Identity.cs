namespace Runwick;

/// <summary>Logical job identity. Stable across retries and generations.</summary>
public readonly record struct JobId(Guid Value)
{
    public override string ToString() => Value.ToString("D");
}

/// <summary>Identity of one reservation (attempt) of a job.</summary>
public readonly record struct AttemptId(Guid Value)
{
    public override string ToString() => Value.ToString("D");
}

/// <summary>Identity of one dispatch command; reusing it replays the same command.</summary>
public readonly record struct DispatchOperationId(Guid Value)
{
    public override string ToString() => Value.ToString("D");
}
