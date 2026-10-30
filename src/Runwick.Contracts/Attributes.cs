namespace Runwick;

/// <summary>The stable, case-sensitive durable name of a job contract (for example <c>email.welcome</c>).</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class JobNameAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

/// <summary>The current payload version of a job contract. Defaults to 1 when absent.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class PayloadVersionAttribute(int version) : Attribute
{
    public int Version { get; } = version;
}

/// <summary>
/// The default queue of a job contract. If another library also defines <c>QueueAttribute</c>
/// (Hangfire does), alias it: <c>using JobQueue = Runwick.QueueAttribute;</c> and write <c>[JobQueue("emails")]</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class QueueAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

/// <summary>The default priority lane: <c>low</c>, <c>medium</c> or <c>high</c>.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class PriorityAttribute(string priority) : Attribute
{
    public string Priority { get; } = priority;
}

/// <summary>The execution budget: how many committed Begin authorizations a generation may consume.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class MaxExecutionsAttribute(int maxExecutions) : Attribute
{
    public int MaxExecutions { get; } = maxExecutions;
}

/// <summary>
/// Applies a worker-registered middleware by alias. Producer contract assemblies never reference
/// middleware implementation types; workers resolve the alias registered with <c>AddMiddleware&lt;T&gt;(alias)</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class UseJobMiddlewareAttribute(string alias) : Attribute
{
    public string Alias { get; } = alias;

    /// <summary>Lower values run first; ties are ordered by ordinal alias.</summary>
    public int Order { get; set; }
}
