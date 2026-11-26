using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runwick.Hosting;
using Runwick.Protocol;
using Runwick.Registration;
using Runwick.Serialization;
using Runwick.Worker;

namespace Runwick.Workloads;

/// <summary>
/// <c>stage2 --part worker</c>: drain an identical seeded backlog (<c>--n</c> ready jobs, one queue, no-op handler) either through the public worker
/// (<c>--arm public</c>) or through a minimal direct driver (<c>--arm direct</c>) that calls the same provider methods (ReserveAndBegin, FinishMany or Finish)
/// with the same slots, payloads, leases and no-op handler. Both arms go through the same <see cref="TimingStore"/> decoration, which records claim and
/// finish batch sizes and call durations. Timing runs from the first claim to the last confirmed finish; startup is a separate clock.
/// Direct driver knobs: <c>--claim-size c</c> (jobs per claim, up to 32), <c>--claims-in-flight f</c>, <c>--finish-size b</c>, <c>--flush-ms t</c>, <c>--buffered true|false</c>.
/// Public knobs: <c>--claim-ahead k</c>, <c>--claims-in-flight f</c>, <c>--buffered true|false</c>. Both: <c>--slots</c>, <c>--n</c>, <c>--lease-seconds</c> (default 300), <c>--usage</c>
/// (public records resource usage; the direct driver sends none, so the matched pair runs without it).
/// </summary>
internal static class Stage2Worker
{
    public static async Task<int> RunAsync(Args a)
    {
        var store = a.OptStr("store") ?? "postgres";
        var arm = a.OptStr("arm") ?? "public";
        var n = a.Int("n", 20000);
        var slots = a.Int("slots", 16);
        var payload = a.Int("payload", 1024);
        var lease = TimeSpan.FromSeconds(a.Double("lease-seconds", 300));
        var timing = new Metrics();
        var claimAhead = a.Int("claim-ahead", 0);
        var inFlight = a.Int("claims-in-flight", 1);
        bool? buffered = a.OptStr("buffered") is { } b ? bool.Parse(b) : null;
        await using var setup = await Stage2Mode.BuildAsync(a, store, o =>
        {
            o.LeaseDuration = lease;
            o.RecordResourceUsage = a.Has("usage");
            o.ClaimAhead = claimAhead;
            o.ClaimsInFlight = inFlight;
            if (buffered is { } v)
            {
                o.BufferedFinish = v;
            }
        }, slots, withWorker: arm == "public", timing).ConfigureAwait(false);
        var sp = setup.Provider;
        var dispatcher = sp.GetRequiredService<IJobDispatcher>();
        var runtime = sp.GetRequiredService<RunwickRuntime>();
        var registry = sp.GetRequiredService<JobRegistry>();
        var driver = sp.GetRequiredService<IRunwickStore>();
        await runtime.EnsureReadyAsync(CancellationToken.None).ConfigureAwait(false);

        // Seed: identical jobs, 100 per call, four callers; the receipts must account for every job before the drain starts.
        BenchMode.Phase("seed-start");
        long seq = 0, accepted = 0;
        var chunks = (n + 99) / 100;
        var next = -1;
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (var c = Interlocked.Increment(ref next); c < chunks; c = Interlocked.Increment(ref next))
            {
                var items = Enumerable.Range(0, Math.Min(100, n - c * 100)).Select(_ => new BatchDispatchItem(LoadJob.Create(Interlocked.Increment(ref seq), payload))).ToList();
                var receipt = await dispatcher.DispatchManyAsync(items).ConfigureAwait(false);
                Interlocked.Add(ref accepted, receipt.Items.Count(x => x.Receipt is not null));
            }
        }))).ConfigureAwait(false);
        BenchMode.Phase("seed-end");
        if (accepted != n)
        {
            Console.Error.WriteLine($"seed accepted {accepted} of {n}");
            return 1;
        }

        var cgroup = a.OptStr("container") is { } cn ? Stage2Mode.CgroupCpuFile(cn) : null;
        double StoreCpu() => cgroup is null ? 0 : Stage2Mode.ReadCgroupUsec(cgroup) / 1000.0;
        var proc = Process.GetCurrentProcess();
        double ProcCpu() { proc.Refresh(); return proc.TotalProcessorTime.TotalMilliseconds; }
        var before = setup.Counters is null ? null : await setup.Counters().ConfigureAwait(false);
        var clock = Stopwatch.StartNew();
        long firstClaim = -1, lastFinish = 0, confirmed = 0, handlerRuns = 0;
        var handlerAt = new ConcurrentDictionary<Guid, long>();
        var returnToConfirm = new LatencyReservoir();
        var idle = new IdleSampler();
        var swStart = Stopwatch.GetTimestamp();
        var cpu0 = ProcCpu();
        var s0 = StoreCpu();
        BenchMode.Phase("timed-start");
        JsonObject extra;
        if (arm == "public")
        {
            var engine = sp.GetRequiredService<WorkerEngine>();
            engine.Hooks.Event = (name, entry) =>
            {
                var now = clock.ElapsedTicks;
                if (name is "reserved" or "started" or "handlerInvoked")
                {
                    Interlocked.CompareExchange(ref firstClaim, now, -1);
                }

                if (name == "handlerInvoked" && entry is not null)
                {
                    Interlocked.Increment(ref handlerRuns);
                    handlerAt[entry.Reserved.Position.JobId] = now;
                }
                else if (name is "finished" or "finishReplayed")
                {
                    Interlocked.Exchange(ref lastFinish, now);
                    Interlocked.Increment(ref confirmed);
                    if (entry is not null && handlerAt.TryRemove(entry.Reserved.Position.JobId, out var h))
                    {
                        returnToConfirm.Add((now - h) * 1000.0 / Stopwatch.Frequency);
                    }
                }
            };
            var hosted = sp.GetServices<IHostedService>().ToList();
            foreach (var s in hosted) { await s.StartAsync(CancellationToken.None).ConfigureAwait(false); }
            while (Interlocked.Read(ref confirmed) < n)
            {
                await Task.Delay(20).ConfigureAwait(false);
            }

            extra = new JsonObject { ["handlerRuns"] = Interlocked.Read(ref handlerRuns) };
            foreach (var s in Enumerable.Reverse(hosted)) { await s.StopAsync(CancellationToken.None).ConfigureAwait(false); }
        }
        else
        {
            extra = await DirectAsync(a, driver, registry, n, slots, lease, buffered ?? driver.BuffersFinishByDefault, inFlight,
                () => Interlocked.CompareExchange(ref firstClaim, clock.ElapsedTicks, -1),
                () => { Interlocked.Exchange(ref lastFinish, clock.ElapsedTicks); return Interlocked.Increment(ref confirmed); },
                (id, ms) => returnToConfirm.Add(ms), idle, clock).ConfigureAwait(false);
        }

        BenchMode.Phase("timed-end");
        var wallSeconds = (Interlocked.Read(ref lastFinish) - Interlocked.Read(ref firstClaim)) / (double)Stopwatch.Frequency;
        var rc = returnToConfirm.Summarize();
        var counters = timing.CountersJson();
        var after = setup.Counters is null ? null : await setup.Counters().ConfigureAwait(false);
        var result = new JsonObject
        {
            ["mode"] = "stage2-worker", ["store"] = store, ["arm"] = arm, ["slots"] = slots, ["jobs"] = n, ["confirmed"] = Interlocked.Read(ref confirmed),
            ["buffered"] = buffered ?? driver.BuffersFinishByDefault, ["claimAhead"] = claimAhead, ["claimsInFlightSetting"] = inFlight,
            ["claimSize"] = a.OptInt("claim-size"), ["finishSize"] = a.OptInt("finish-size"), ["flushMs"] = a.OptInt("flush-ms"), ["usage"] = a.Has("usage"),
            ["startupMsToFirstClaim"] = Math.Round((Interlocked.Read(ref firstClaim) - 0) * 1000.0 / Stopwatch.Frequency, 1),
            ["drainSeconds"] = Math.Round(wallSeconds, 3), ["jobsPerSec"] = Math.Round(n / wallSeconds, 1),
            ["handlerReturnToConfirmMs"] = new JsonObject { ["p50"] = Math.Round(rc.P50Ms, 2), ["p95"] = Math.Round(rc.P95Ms, 2), ["p99"] = Math.Round(rc.P99Ms, 2) },
            ["clientCpuMsPerJob"] = Math.Round((ProcCpu() - cpu0) / n, 4), ["storeCpuMsPerJob"] = Math.Round((StoreCpu() - s0) / n, 4),
            ["storeCallLatencies"] = timing.LatenciesJson(), ["storeCallCounts"] = counters, ["idleWithBacklog"] = idle.ToJson(),
            ["direct"] = extra,
        };
        _ = swStart;
        if (before is not null && after is not null)
        {
            var d = new JsonObject();
            foreach (var (k, v) in after)
            {
                var x = (double?)v - ((double?)before[k] ?? 0);
                if (x is { } y && y != 0 && (k.StartsWith("calls:", StringComparison.Ordinal) || k is "commandCalls" or "commandUsec" or "cpuSeconds" or "transactions" or "rowsWritten" or "walBytes" || k.StartsWith("function", StringComparison.Ordinal)))
                {
                    d[k] = Math.Round(y / n, 4);
                }
            }

            result["storeCountersPerJob"] = d;
        }

        var json = result.ToJsonString(Json.Indented);
        if (a.OptStr("out") is { } file) { await File.WriteAllTextAsync(file, json).ConfigureAwait(false); } else { Console.WriteLine(json); }
        return Interlocked.Read(ref confirmed) == n ? 0 : 1;
    }

    /// <summary>Counts how long a free slot and unclaimed backlog existed together (sampled every 2 ms).</summary>
    sealed class IdleSampler
    {
        long _samples, _idle;
        public void Sample(bool idleWithBacklog) { Interlocked.Increment(ref _samples); if (idleWithBacklog) { Interlocked.Increment(ref _idle); } }
        public JsonObject ToJson() => new() { ["samples"] = _samples, ["idleSlotWithBacklogShare"] = _samples == 0 ? null : Math.Round((double)_idle / _samples, 4) };
    }

    static async Task<JsonObject> DirectAsync(Args a, IRunwickStore driver, JobRegistry registry, int n, int slots, TimeSpan lease, bool buffered, int claimers,
        Action firstClaim, Func<long> confirm, Action<Guid, double> settle, IdleSampler idle, Stopwatch clock)
    {
        var claimStore = (IReserveAndBeginStore)driver;
        var finishStore = (IFinishManyStore)driver;
        var claimSize = Math.Clamp(a.Int("claim-size", slots), 1, IReserveAndBeginStore.MaxJobs);
        var finishSize = Math.Clamp(a.Int("finish-size", 16), 1, IFinishManyStore.MaxItems);
        var flush = TimeSpan.FromMilliseconds(a.Double("flush-ms", 5));
        var worker = Guid.CreateVersion7();
        var application = registry.Application;
        var contracts = registry.Capabilities.Select(c => new JobContract(c.Name, c.PayloadVersion, c.SerializerProfileId, c.SerializerProfileVersion)).ToList();
        StoreCommand<T> Cmd<T>(T body) => new(application, Guid.CreateVersion7(), DateTimeOffset.FromUnixTimeMilliseconds(Fingerprints.FloorEpochMs(DateTimeOffset.UtcNow.AddSeconds(30))), body);
        var tickets = new SemaphoreSlim(slots, slots);
        long claimed = 0, free = slots, failures = 0, settledCount = 0;
        var finishes = Channel.CreateUnbounded<(StoreCommand<FinishRequest> Command, long ReturnedAt)>();
        var stop = new CancellationTokenSource();
        using var sampler = new Timer(_ => idle.Sample(Volatile.Read(ref free) > 0 && Interlocked.Read(ref claimed) < n), null, 2, 2);

        async Task Settle(List<(StoreCommand<FinishRequest> Command, long ReturnedAt)> items)
        {
            var reply = await finishStore.FinishManyAsync(Cmd(new FinishManyRequest([.. items.Select(i => i.Command)])) with
            { NotAfter = items.Max(i => i.Command.NotAfter) }, CancellationToken.None).ConfigureAwait(false);
            var now = clock.ElapsedTicks;
            for (var i = 0; i < items.Count; i++)
            {
                if (reply.Value is { } v && v.Outcomes[i].Status == StoreStatus.Applied)
                {
                    confirm();
                    Interlocked.Increment(ref settledCount);
                    settle(Guid.Empty, (now - items[i].ReturnedAt) * 1000.0 / Stopwatch.Frequency);
                    
                }
                else
                {
                    Interlocked.Increment(ref failures);
                }
            }
        }

        // Finish flusher: a batch goes out when it reaches finishSize or the oldest item has waited flush.
        var flusher = Task.Run(async () =>
        {
            var batch = new List<(StoreCommand<FinishRequest>, long)>();
            while (await finishes.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                var deadline = Stopwatch.GetTimestamp() + (long)(flush.TotalSeconds * Stopwatch.Frequency);
                while (batch.Count < finishSize)
                {
                    if (finishes.Reader.TryRead(out var item)) { batch.Add(item); continue; }
                    if (Stopwatch.GetTimestamp() >= deadline) { break; }
                    await Task.Delay(1).ConfigureAwait(false);
                }

                if (batch.Count > 0) { await Settle(batch).ConfigureAwait(false); batch.Clear(); }
            }
        });

        async Task Claimer()
        {
            while (Interlocked.Read(ref claimed) < n)
            {
                await tickets.WaitAsync().ConfigureAwait(false);
                var got = 1;
                while (got < claimSize && tickets.Wait(0)) { got++; }
                Interlocked.Add(ref free, -got);
                firstClaim();
                var reply = await claimStore.ReserveAndBeginAsync(Cmd(new ReserveAndBeginRequest(worker, LoadRegistration.Queue, "medium", contracts, null, lease, got)), CancellationToken.None).ConfigureAwait(false);
                var jobs = reply is { Status: StoreStatus.Applied, Value: { } v } ? v.Outcomes.Where(o => o is { Status: StoreStatus.Applied, Begun: not null, Reserved: not null }).ToList() : [];
                Interlocked.Add(ref claimed, jobs.Count);
                if (jobs.Count < got)
                {
                    Interlocked.Add(ref free, got - jobs.Count);
                    tickets.Release(got - jobs.Count);
                    if (jobs.Count == 0) { await Task.Delay(1).ConfigureAwait(false); }
                }

                var finishCommands = jobs.Select(o => Cmd(new FinishRequest(o.Begun!.Lease.Handle, AttemptOutcome.Succeeded, TimeSpan.Zero, null, null))).ToList();
                if (buffered)
                {
                    // Buffered: the slot goes back at once and the finish waits for the flusher.
                    foreach (var finish in finishCommands)
                    {
                        Interlocked.Increment(ref free);
                        tickets.Release();
                        await finishes.Writer.WriteAsync((finish, clock.ElapsedTicks)).ConfigureAwait(false);
                    }
                }
                else if (finishCommands.Count > 0)
                {
                    // Not buffered (the Redis default): jobs that finish together, as a claim's no-op jobs do, settle in FinishMany groups; a lone one by Finish.
                    // The slots stay held until the store confirms.
                    _ = Task.Run(async () =>
                    {
                        var returned = clock.ElapsedTicks;
                        for (var i = 0; i < finishCommands.Count; i += finishSize)
                        {
                            var group = finishCommands.Skip(i).Take(finishSize).ToList();
                            if (group.Count == 1)
                            {
                                var r = await driver.FinishAsync(group[0], CancellationToken.None).ConfigureAwait(false);
                                if (r.Status == StoreStatus.Applied) { confirm(); Interlocked.Increment(ref settledCount); settle(Guid.Empty, (clock.ElapsedTicks - returned) * 1000.0 / Stopwatch.Frequency); }
                                else { Interlocked.Increment(ref failures); }
                            }
                            else
                            {
                                await Settle(group.Select(g => (g, returned)).ToList()).ConfigureAwait(false);
                            }
                        }

                        Interlocked.Add(ref free, finishCommands.Count);
                        tickets.Release(finishCommands.Count);
                    });
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, claimers).Select(_ => Task.Run(Claimer))).ConfigureAwait(false);
        while (Interlocked.Read(ref settledCount) < n && Interlocked.Read(ref failures) == 0) { await Task.Delay(5).ConfigureAwait(false); }
        finishes.Writer.Complete();
        await flusher.ConfigureAwait(false);
        return new JsonObject { ["claimSizeUsed"] = claimSize, ["finishSizeUsed"] = finishSize, ["flushMsUsed"] = flush.TotalMilliseconds, ["claimers"] = claimers, ["finishFailures"] = failures };
    }
}
