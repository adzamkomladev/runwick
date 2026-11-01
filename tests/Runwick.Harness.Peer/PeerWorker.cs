using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick.Harness.Engine;
using Runwick.Management;
using Runwick.Postgres;
using Runwick.Postgres.Store;
using Runwick.Protocol;

namespace Runwick.Harness.Peer;

/// <summary>
/// Worker commands (07 §2.4): <c>startWorker</c> hosts the real worker engine in this process (scaled timing);
/// <c>stopWorker</c> begins its staged shutdown without waiting (or, with <c>wait</c>, waits for it), so the fixture can
/// kill the process mid-phase. Handler behaviour by job key prefix: <c>block</c> ignores cancellation and never returns;
/// <c>hold</c> returns when the fixture sends <c>releaseHeld</c>; <c>long</c> runs until its cancellation token fires;
/// anything else returns at once. With <c>bcs</c> (a business connection string) every other job writes a
/// <c>public.ledger</c> row and stages its completion in that transaction (08 §8, MODE-05).
/// The role commands (<c>workerDispatch</c>, <c>workerPause</c>, <c>workerResume</c>, <c>workerCancel</c>,
/// <c>workerObserve</c>) use the worker's own provider, so one process can hold every role (MODE-01, 15 §2).
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

                var business = m["bcs"]?.GetValue<string>();
                _host = WorkerHost.Create(m["cs"]!.GetValue<string>(), m["app"]!.GetValue<string>(), faults, m["concurrency"]?.GetValue<int>() ?? 4,
                    queue: m["queue"]?.GetValue<string>() ?? "default");
                _host.Script.Default = (job, ctx, ct) =>
                    job.Id.StartsWith("block", StringComparison.Ordinal) ? Task.Delay(Timeout.InfiniteTimeSpan, CancellationToken.None)
                    : job.Id.StartsWith("hold", StringComparison.Ordinal) ? Held.Task
                    : job.Id.StartsWith("long", StringComparison.Ordinal) ? Task.Delay(Timeout.InfiniteTimeSpan, ct)
                    : business is not null ? StagedAsync(business, ctx, ct)
                    : Task.CompletedTask;
                await _host.StartAsync().ConfigureAwait(false);
                return new JsonObject { ["workerInstanceId"] = _host.Engine.WorkerInstanceId.ToString("D") };
            case "releaseHeld":
                Held.TrySetResult();
                return [];
            case "stopWorker":
                var host = _host ?? throw new InvalidOperationException("send 'startWorker' first");
                _stopping = Task.Run(() => host.StopAsync());
                if (m["wait"]?.GetValue<bool>() == true)
                {
                    await _stopping.ConfigureAwait(false);
                }

                return new JsonObject { ["stopSignalAtMs"] = (long)(host.Engine.StopSignalAt ?? TimeSpan.Zero).TotalMilliseconds };
            case "workerStats":
                var h = Host();
                var completed = new JsonObject();
                foreach (var (key, count) in h.Script.Completed)
                {
                    completed[key] = count;
                }

                var invoked = new JsonObject();
                foreach (var group in h.Script.Invocations.GroupBy(i => i.JobKey, StringComparer.Ordinal))
                {
                    invoked[group.Key] = group.Count();
                }

                return new JsonObject
                {
                    ["workerInstanceId"] = h.Engine.WorkerInstanceId.ToString("D"),
                    ["completed"] = completed,
                    ["invoked"] = invoked,
                    ["finishesSent"] = Interlocked.Read(ref h.Engine.FinishesSent),
                    ["stagedObserved"] = Interlocked.Read(ref h.Engine.StagedCompletionsObserved),
                };
            case "workerDispatch":
                var options = m["queue"]?.GetValue<string>() is { } q ? new DispatchOptions { Queue = q } : null;
                var receipt = await Host().Dispatcher.DispatchAsync(new WorkJob(m["jobKey"]!.GetValue<string>()), options).ConfigureAwait(false);
                return new JsonObject { ["jobId"] = receipt.JobId.Value.ToString("D") };
            case "workerPause":
            case "workerResume":
                var queues = Host().Services.GetRequiredService<IQueueControl>();
                var queue = m["queue"]?.GetValue<string>() ?? "default";
                var revision = m["revision"]!.GetValue<long>();
                var qr = cmd == "workerPause"
                    ? await queues.PauseAsync(queue, revision, Actor(m), m["reason"]?.GetValue<string>()).ConfigureAwait(false)
                    : await queues.ResumeAsync(queue, revision, Actor(m), m["reason"]?.GetValue<string>()).ConfigureAwait(false);
                return new JsonObject { ["succeeded"] = qr.Succeeded, ["code"] = qr.Code, ["revision"] = qr.Revision };
            case "workerCancel":
                var target = new JobTarget(new JobId(Guid.Parse(m["jobId"]!.GetValue<string>())), m["generation"]!.GetValue<int>(),
                    m["revision"]!.GetValue<long>());
                var cr = await Host().Services.GetRequiredService<IJobControl>().CancelAsync(target, Actor(m), m["reason"]?.GetValue<string>())
                    .ConfigureAwait(false);
                return new JsonObject { ["succeeded"] = cr.Succeeded, ["code"] = cr.Code, ["effect"] = cr.Effect };
            case "workerObserve":
                var seen = await ObserveAsync(Host(), m["kind"]!.GetValue<string>(), m["jobId"]?.GetValue<string>() is { } j ? Guid.Parse(j) : null,
                    TimeSpan.FromSeconds(m["timeoutSeconds"]?.GetValue<int>() ?? 90)).ConfigureAwait(false);
                return new JsonObject { ["actorId"] = seen.ActorId, ["kind"] = seen.Kind };
            default:
                throw new InvalidOperationException($"unknown worker command '{cmd}'");
        }
    }

    static WorkerHost Host() => _host ?? throw new InvalidOperationException("send 'startWorker' first");

    static ManagementActor Actor(JsonObject m) =>
        new(m["actor"]?.GetValue<string>() ?? "user:ops", m["actorName"]?.GetValue<string>() ?? "Ops", "host");

    /// <summary>Reads the cursor from the start until an event of <paramref name="kind"/> (for <paramref name="job"/>, if given) is visible.</summary>
    static async Task<JobEvent> ObserveAsync(WorkerHost host, string kind, Guid? job, TimeSpan timeout)
    {
        var store = host.Services.GetRequiredService<PgStore>();
        var deadline = DateTime.UtcNow + timeout;
        EventCursor? cursor = null;
        while (DateTime.UtcNow < deadline)
        {
            var page = await store.EventsSinceAsync(host.Engine.Application, cursor, 100, CancellationToken.None).ConfigureAwait(false);
            var hit = page.Events.FirstOrDefault(e => e.Kind == kind && (job is null || e.JobId == job));
            if (hit is not null)
            {
                return hit;
            }

            cursor = page.Next;
            if (page.Events.Count == 0)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }
        }

        throw new TimeoutException($"event {kind} not observed through the cursor");
    }

    /// <summary>A business row and the staged completion commit together (08 §8); the engine then sends no Finish.</summary>
    static async Task StagedAsync(string businessConnectionString, IJobContext ctx, CancellationToken ct)
    {
        var conn = new NpgsqlConnection(businessConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
            var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);
            await using (tx.ConfigureAwait(false))
            {
                using (var cmd = new NpgsqlCommand("INSERT INTO public.ledger VALUES (@j, @a)", conn, tx))
                {
                    cmd.Parameters.AddWithValue("j", ctx.JobId.Value);
                    cmd.Parameters.AddWithValue("a", ctx.AttemptId.Value);
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                var completion = ((IServiceProvider)ctx.Items[typeof(IServiceProvider)]!).GetRequiredService<IPostgresJobCompletion>();
                await completion.CompleteInTransactionAsync(tx, ct).ConfigureAwait(false);
                await tx.CommitAsync(ct).ConfigureAwait(false);
            }
        }
    }
}
