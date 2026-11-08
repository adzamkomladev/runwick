// Peer process controlled over JSON Lines on stdin/stdout (spec 07 §2.4).
using System.Text.Json.Nodes;
using Runwick;
using Runwick.Harness.Engine;
using Runwick.Harness.Jobs;
using Runwick.Harness.Peer;
using Runwick.Harness.Store;
using Runwick.Protocol;

Runwick.Postgres.Store.PgRunner.HarnessMarker = true;   // a test binary: may use fault-hooks stores (PF-05)
var identity = args.Length > 0 ? args[0] : "peer";

// `--store sqlite --path <file>` (20 §12): commands without "cs" use that file; a "cs" in a command still selects the store.
string? storeCs = null;
for (var i = 1; i + 1 < args.Length; i += 2)
{
    if (args[i] == "--path")
    {
        storeCs = $"Data Source={args[i + 1]}";
    }
    else if (args[i] != "--store" || args[i + 1] != "sqlite")
    {
        throw new ArgumentException($"unknown peer option {args[i]} {args[i + 1]}");
    }
}

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
    if (msg["cs"] is null && storeCs is not null)
    {
        msg["cs"] = storeCs;
    }

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
                r =>
                {
                    // "work": the WorkJob contracts of a worker peer (optionally contended, SQL-01), for a producer-only process.
                    if (m["work"]?.GetValue<bool>() == true)
                    {
                        WorkerHost.RegisterWork(r, jobs: m["contended"]?.GetValue<bool>() == true ? WorkerHost.Contended : null);
                    }
                    else
                    {
                        TestJobs.Register(r, max);
                    }

                    // Concurrent peer commands queue behind the 2-call admission limit; under suite load 1 s is too short.
                    r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20));
                }).ConfigureAwait(false);
            return new JsonObject { ["identity"] = identity, ["pid"] = Environment.ProcessId, ["workerInstanceId"] = client.WorkerInstanceId.ToString("D") };
        case "dispatch":
            var kind = m["job"]?.GetValue<string>() ?? "ping";
            var jobId = m["jobKey"]?.GetValue<string>() ?? Guid.NewGuid().ToString("N");
            IQueueJob job = kind switch
            {
                "keyed" => new KeyedJob(jobId, m["key"]!.GetValue<string>()),
                "resource" => new ResourceJob(jobId, m["key"]!.GetValue<string>()),
                "capped" => new CappedJob(jobId),
                "pair" => new PairJob(jobId, m["key"]!.GetValue<string>(), m["key2"]!.GetValue<string>()),
                "work" => new WorkJob(jobId),
                _ => new PingJob(jobId),
            };
            return ReceiptJson(await Dispatch(job).ConfigureAwait(false));
        case "dispatchWork":
            // SQL-01 producer: WorkJob "<prefix><i % resources>-<i>" for i in [0, count), four calls in flight.
            var prefix = m["prefix"]!.GetValue<string>();
            var resources = m["resources"]!.GetValue<int>();
            await Parallel.ForAsync(0, m["count"]!.GetValue<int>(), new ParallelOptions { MaxDegreeOfParallelism = 4 },
                async (i, ct) => await C().Dispatcher.DispatchAsync(new WorkJob($"{prefix}{i % resources}-{i}"), cancellationToken: ct).ConfigureAwait(false))
                .ConfigureAwait(false);
            return [];
        case "holdWriter":
            // SQL-02/07: this process takes the file's write lock (BEGIN IMMEDIATE) and keeps it for "ms".
            using (var holder = new Microsoft.Data.Sqlite.SqliteConnection(m["cs"]!.GetValue<string>() + ";Pooling=False"))
            {
                holder.Open();
                using var begin = holder.CreateCommand();
                begin.CommandText = "BEGIN IMMEDIATE";
                begin.ExecuteNonQuery();
                output.Write(new JsonObject { ["type"] = "writerHeld" });
                await Task.Delay(m["ms"]!.GetValue<int>()).ConfigureAwait(false);
                using var commit = holder.CreateCommand();
                commit.CommandText = "COMMIT";
                commit.ExecuteNonQuery();
            }

            return [];
        case "dispatchPrepared":
            // The same PreparedDispatch (operation ID, body, NotAfter) imported from the local process's envelope.
            var imported = C().Prepared.ImportPrepared(Convert.FromBase64String(m["envelope"]!.GetValue<string>()));
            try
            {
                return ReceiptJson(await C().Prepared.DispatchAsync(imported).ConfigureAwait(false));
            }
            catch (RunwickException ex)
            {
                return new JsonObject { ["error"] = ex.Code, ["disposition"] = ex.Disposition.ToString() };
            }
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
            // "commandId"/"notAfterMs" let the fixture resend the same command after a crash (SQL-04).
            var finished = await C().FinishAsync(PeerJson.Lease(m["lease"]!), Enum.Parse<AttemptOutcome>(m["outcome"]?.GetValue<string>() ?? "Succeeded"),
                commandId: m["commandId"]?.GetValue<string>() is { } fc ? Guid.Parse(fc) : null,
                notAfter: m["notAfterMs"]?.GetValue<long>() is { } na ? DateTimeOffset.FromUnixTimeMilliseconds(na) : null).ConfigureAwait(false);
            var fr = PeerJson.Reply(finished);
            fr["disposition"] = finished.Value?.Disposition;
            return fr;
        case "release":
            return PeerJson.Reply(await C().ReleaseAsync(PeerJson.Lease(m["lease"]!)).ConfigureAwait(false));
        case "recover":
            var expired = new ExpiredLease(Guid.Parse(m["jobId"]!.GetValue<string>()), Guid.Parse(m["attemptId"]!.GetValue<string>()),
                Guid.Parse(m["ownershipToken"]!.GetValue<string>()), "", default);
            return PeerJson.Reply(await C().RecoverAsync(expired).ConfigureAwait(false));
        case "deadlocks":
            // 40P01 rollbacks this process's runner has observed: deadlock-freedom evidence (07 I-18).
            return new JsonObject { ["count"] = ((Runwick.Postgres.Store.PgRunner)C().Services.GetService(typeof(Runwick.Postgres.Store.PgRunner))!).Deadlocks };
        case "armFault":
            faults.Arm(m);
            return [];
        case "releaseBarrier":
            faults.Release(Enum.Parse<FaultPoint>(m["point"]!.GetValue<string>()));
            return [];
        case "openRated":
            // SP-013: as 'open', plus the rated contracts (RateJobs) so this process can reserve and begin them.
            var ratedMax = m["maxExecutions"]?.GetValue<int>() ?? 3;
            client = await TestClient.CreateAsync(m["cs"]!.GetValue<string>(), m["app"]!.GetValue<string>(), faults,
                r =>
                {
                    TestJobs.Register(r, ratedMax);
                    RateJobs.Register(r);
                    r.Producer(o => o.AdmissionWait = TimeSpan.FromSeconds(20));
                }).ConfigureAwait(false);
            return new JsonObject { ["identity"] = identity, ["pid"] = Environment.ProcessId, ["workerInstanceId"] = client.WorkerInstanceId.ToString("D") };
        case "openOldBinary":
            // SP-013 UPG-02/UPG-06: an older binary, simulated by removing capabilities from this process's known set
            // before 'open' (the gate and V-24 read only this set). Process-wide, so it is a peer-only command.
            var known = (Dictionary<string, int>)Runwick.Hosting.RunwickRuntime.KnownCapabilities;
            foreach (var name in m["without"]!.AsArray())
            {
                known.Remove(name!.GetValue<string>());
            }

            return await RunAsync("open", m).ConfigureAwait(false);
        case "startWorker":
        case "releaseHeld":
        case "stopWorker":
        case "workerStats":
        case "workerDispatch":
        case "workerPause":
        case "workerResume":
        case "workerCancel":
        case "workerObserve":
            return await PeerWorker.HandleAsync(cmd, m, faults).ConfigureAwait(false);
        default:
            throw new InvalidOperationException($"unknown command '{cmd}'");
    }
}

static JsonObject ReceiptJson(DispatchReceipt receipt) => new()
{
    ["jobId"] = receipt.JobId.Value.ToString("D"),
    ["disposition"] = receipt.Disposition.ToString(),
    ["match"] = receipt.DuplicateMatch.ToString(),
};

TestClient C() => client ?? throw new InvalidOperationException("send 'open' first");

Task<DispatchReceipt> Dispatch(IQueueJob job) => job switch
{
    KeyedJob k => C().Dispatcher.DispatchAsync(k),
    ResourceJob r => C().Dispatcher.DispatchAsync(r),
    CappedJob cj => C().Dispatcher.DispatchAsync(cj),
    PairJob pj => C().Dispatcher.DispatchAsync(pj),
    PingJob p => C().Dispatcher.DispatchAsync(p),
    WorkJob w => C().Dispatcher.DispatchAsync(w),
    _ => throw new ArgumentOutOfRangeException(nameof(job)),
};
