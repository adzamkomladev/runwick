using System.Collections.Concurrent;
using Microsoft.AspNetCore.Authorization;

namespace Runwick.Dashboard.Spike;

/// <summary>One dashboard mount. Nothing here is shared with the host or with another mount.</summary>
public sealed class DashboardMountOptions
{
    /// <summary>The page title.</summary>
    public string Title { get; set; } = "Runwick";

    /// <summary>Who may read pages. Required: a mount without it is not mapped.</summary>
    public AuthorizationPolicy? ViewPolicy { get; set; }

    /// <summary>Who may run commands and reveal sensitive detail. Required.</summary>
    public AuthorizationPolicy? OperatePolicy { get; set; }

    /// <summary>The (stand-in) management service. Required.</summary>
    public IDashboardSpikeData? Data { get; set; }

    /// <summary>The client build; defaults to the embedded one.</summary>
    public DashboardAssets Assets { get; set; } = DashboardAssets.Embedded;
}

/// <summary>A job as the dashboard may show it: already filtered, no payload.</summary>
public sealed record SpikeJob(string Id, string Name, string State, long Revision, int Retries);

/// <summary>What a retry command reported.</summary>
public enum RetryOutcome
{
    /// <summary>Applied.</summary>
    Done,

    /// <summary>The job changed since the caller read it.</summary>
    Conflict,

    /// <summary>The result is not known (for example a timeout); the command id resolves it later.</summary>
    Unknown,

    /// <summary>No such job.</summary>
    NotFound,
}

/// <summary>The stand-in for the SP-020 management services the real dashboard will call.</summary>
public interface IDashboardSpikeData
{
    /// <summary>Lists jobs.</summary>
    IReadOnlyList<SpikeJob> Jobs();

    /// <summary>Finds a job.</summary>
    SpikeJob? Find(string id);

    /// <summary>The sensitive payload, shown only on an explicit reveal.</summary>
    string? Payload(string id);

    /// <summary>Retries a job once per command id.</summary>
    RetryOutcome Retry(string id, long revision, string commandId);
}

/// <summary>In-memory data for the spike's tests and sample host.</summary>
public sealed class InMemoryDashboardSpikeData : IDashboardSpikeData
{
    private readonly ConcurrentDictionary<string, SpikeJob> _jobs = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _payloads = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, RetryOutcome> _commands = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();

    /// <summary>The outcome every retry reports instead of applying it; null applies retries.</summary>
    public RetryOutcome? ForcedOutcome { get; set; }

    /// <summary>How many retries were applied.</summary>
    public int Applied { get; private set; }

    /// <summary>Adds a job.</summary>
    public InMemoryDashboardSpikeData Add(string id, string name, string payload)
    {
        _jobs[id] = new SpikeJob(id, name, "Failed", 1, 0);
        _payloads[id] = payload;
        return this;
    }

    /// <inheritdoc />
    public IReadOnlyList<SpikeJob> Jobs() => [.. _jobs.Values.OrderBy(j => j.Id, StringComparer.Ordinal)];

    /// <inheritdoc />
    public SpikeJob? Find(string id) => _jobs.GetValueOrDefault(id);

    /// <inheritdoc />
    public string? Payload(string id) => _payloads.GetValueOrDefault(id);

    /// <inheritdoc />
    public RetryOutcome Retry(string id, long revision, string commandId)
    {
        lock (_gate)
        {
            if (_commands.TryGetValue(commandId, out var earlier))
            {
                return earlier;
            }

            var outcome = ForcedOutcome ?? Apply(id, revision);
            _commands[commandId] = outcome;
            return outcome;
        }
    }

    private RetryOutcome Apply(string id, long revision)
    {
        if (!_jobs.TryGetValue(id, out var job))
        {
            return RetryOutcome.NotFound;
        }

        if (job.Revision != revision)
        {
            return RetryOutcome.Conflict;
        }

        _jobs[id] = job with { State = "Queued", Revision = revision + 1, Retries = job.Retries + 1 };
        Applied++;
        return RetryOutcome.Done;
    }
}
