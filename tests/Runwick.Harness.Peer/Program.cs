// Peer process controlled over JSON Lines on stdin/stdout (spec 07 §2.4).
using System.Text.Json.Nodes;
using Runwick;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Protocol;

var identity = args.Length > 0 ? args[0] : "peer";
var output = new PeerOutput(Console.Out);
var faults = new PeerFaultInjector(output);
TestClient? client = null;

while (await Console.In.ReadLineAsync().ConfigureAwait(false) is { } line)
{
    if (string.IsNullOrWhiteSpace(line))
    {
        continue;
    }

    var msg = JsonNode.Parse(line)!.AsObject();
    var id = msg["id"]!.GetValue<long>();
    var cmd = msg["cmd"]!.GetValue<string>();

    // Store commands run concurrently so a command blocked at a barrier does not block the control channel.
    _ = Task.Run(async () =>
    {
        try
        {
            var result = await RunAsync(cmd, msg).ConfigureAwait(false);
            output.Write(new JsonObject { ["type"] = "reply", ["id"] = id, ["ok"] = true, ["result"] = result });
        }
        catch (Exception ex)
        {
            output.Write(new JsonObject { ["type"] = "reply", ["id"] = id, ["ok"] = false, ["error"] = $"{ex.GetType().Name}: {ex.Message}" });
        }
    });
}

if (client is not null)
{
    await client.DisposeAsync().ConfigureAwait(false);
}

return 0;

async Task<JsonObject> RunAsync(string cmd, JsonObject m)
{
    switch (cmd)
    {
        case "open":
            var max = m["maxExecutions"]?.GetValue<int>() ?? 3;
            client = await TestClient.CreateAsync(m["cs"]!.GetValue<string>(), m["app"]!.GetValue<string>(), faults,
                r => TestJobs.Register(r, max)).ConfigureAwait(false);
            return new JsonObject { ["identity"] = identity, ["pid"] = Environment.ProcessId, ["workerInstanceId"] = client.WorkerInstanceId.ToString("D") };
        case "dispatch":
            var kind = m["job"]?.GetValue<string>() ?? "ping";
            var jobId = m["jobKey"]?.GetValue<string>() ?? Guid.NewGuid().ToString("N");
            IQueueJob job = kind switch
            {
                "keyed" => new KeyedJob(jobId, m["key"]!.GetValue<string>()),
                "resource" => new ResourceJob(jobId, m["key"]!.GetValue<string>()),
                _ => new PingJob(jobId),
            };
            var receipt = await Dispatch(job).ConfigureAwait(false);
            return new JsonObject { ["jobId"] = receipt.JobId.Value.ToString("D"), ["disposition"] = receipt.Disposition.ToString() };
        case "reserve":
            var reserved = await C().ReserveAsync(m["queue"]?.GetValue<string>() ?? "default", m["priority"]?.GetValue<string>() ?? "medium",
                TimeSpan.FromMilliseconds(m["leaseMs"]?.GetValue<long>() ?? 30_000)).ConfigureAwait(false);
            var r = PeerJson.Reply(reserved);
            if (reserved.Value is { } v)
            {
                r["lease"] = PeerJson.Lease(v.Lease.Handle);
                r["expiresAt"] = v.Lease.ExpiresAt.ToUnixTimeMilliseconds();
            }

            return r;
        case "begin":
            var begun = await C().BeginAsync(PeerJson.Lease(m["lease"]!)).ConfigureAwait(false);
            var b = PeerJson.Reply(begun.Reply);
            b["executionNumber"] = begun.Reply.Value?.ExecutionNumber;
            return b;
        case "renew":
            var renewed = await C().RenewAsync(PeerJson.Lease(m["lease"]!), TimeSpan.FromMilliseconds(m["leaseMs"]?.GetValue<long>() ?? 30_000)).ConfigureAwait(false);
            var rr = PeerJson.Reply(renewed);
            rr["expiresAt"] = renewed.Value?.Lease.ExpiresAt.ToUnixTimeMilliseconds();
            return rr;
        case "finish":
            var finished = await C().FinishAsync(PeerJson.Lease(m["lease"]!), Enum.Parse<AttemptOutcome>(m["outcome"]?.GetValue<string>() ?? "Succeeded")).ConfigureAwait(false);
            var fr = PeerJson.Reply(finished);
            fr["disposition"] = finished.Value?.Disposition;
            return fr;
        case "release":
            return PeerJson.Reply(await C().ReleaseAsync(PeerJson.Lease(m["lease"]!)).ConfigureAwait(false));
        case "recover":
            var expired = new ExpiredLease(Guid.Parse(m["jobId"]!.GetValue<string>()), Guid.Parse(m["attemptId"]!.GetValue<string>()),
                Guid.Parse(m["ownershipToken"]!.GetValue<string>()), "", default);
            return PeerJson.Reply(await C().RecoverAsync(expired).ConfigureAwait(false));
        case "armFault":
            faults.Arm(m);
            return [];
        case "releaseBarrier":
            faults.Release(Enum.Parse<FaultPoint>(m["point"]!.GetValue<string>()));
            return [];
        case "startWorker":
        case "stopWorker":
            return await PeerWorker.HandleAsync(cmd, m, () => C()).ConfigureAwait(false);
        default:
            throw new InvalidOperationException($"unknown command '{cmd}'");
    }
}

TestClient C() => client ?? throw new InvalidOperationException("send 'open' first");

Task<DispatchReceipt> Dispatch(IQueueJob job) => job switch
{
    KeyedJob k => C().Dispatcher.DispatchAsync(k),
    ResourceJob r => C().Dispatcher.DispatchAsync(r),
    PingJob p => C().Dispatcher.DispatchAsync(p),
    _ => throw new ArgumentOutOfRangeException(nameof(job)),
};
