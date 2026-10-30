using System.Text.Json.Nodes;
using Runwick.Harness.Engine;
using Runwick.Protocol;

namespace Runwick.Harness.Peer;

/// <summary>
/// Worker commands (07 §2.4): <c>startWorker</c> hosts the real worker engine in this process (Fast timing);
/// <c>stopWorker</c> begins its staged shutdown without waiting, so the fixture can kill the process mid-phase.
/// Handler behaviour by job key prefix: <c>block</c> ignores cancellation and never returns; <c>hold</c> returns when the
/// fixture sends <c>releaseHeld</c>; anything else returns at once.
/// </summary>
internal static class PeerWorker
{
    static WorkerHost? _host;
    static Task? _stopping;
    static readonly TaskCompletionSource Held = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static async Task<JsonObject> HandleAsync(string cmd, JsonObject m, IFaultInjector faults)
    {
        switch (cmd)
        {
            case "startWorker":
                if (_host is not null)
                {
                    throw new InvalidOperationException("worker already started");
                }

                _host = WorkerHost.Create(m["cs"]!.GetValue<string>(), m["app"]!.GetValue<string>(), faults, m["concurrency"]?.GetValue<int>() ?? 4);
                _host.Script.Default = (job, _, _) =>
                    job.Id.StartsWith("block", StringComparison.Ordinal) ? Task.Delay(Timeout.InfiniteTimeSpan, CancellationToken.None)
                    : job.Id.StartsWith("hold", StringComparison.Ordinal) ? Held.Task
                    : Task.CompletedTask;
                await _host.StartAsync().ConfigureAwait(false);
                return new JsonObject { ["workerInstanceId"] = _host.Engine.WorkerInstanceId.ToString("D") };
            case "releaseHeld":
                Held.TrySetResult();
                return [];
            case "stopWorker":
                var host = _host ?? throw new InvalidOperationException("send 'startWorker' first");
                _stopping = Task.Run(() => host.StopAsync());
                return new JsonObject { ["stopSignalAtMs"] = (long)(host.Engine.StopSignalAt ?? TimeSpan.Zero).TotalMilliseconds };
            default:
                throw new InvalidOperationException($"unknown worker command '{cmd}'");
        }
    }
}
