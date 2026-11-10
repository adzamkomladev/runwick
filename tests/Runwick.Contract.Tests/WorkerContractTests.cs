using Microsoft.Extensions.DependencyInjection;
using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.Management;
using Runwick.ReferenceModel;

namespace Runwick.Contract.Tests;

/// <summary>The real worker engine over each store kind (20 §12): batched claim, renewal and finish included.</summary>
[Trait("Category", "Contract")]
[Trait("Item", "SP-019")]
public sealed class WorkerContractTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "RUN-01")]
    public async Task A_worker_runs_dispatched_jobs_to_success_with_one_retry(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("wrk_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 8);
        var failedOnce = 0;
        w.Script.On("flaky", (_, _, _) => Interlocked.Exchange(ref failedOnce, 1) == 0 ? throw new InvalidOperationException("first try") : Task.CompletedTask);
        await w.StartAsync();
        await w.DispatchAsync("flaky");
        for (var i = 0; i < 40; i++)
        {
            await w.DispatchAsync("j" + i);
        }

        try
        {
            await WorkerHost.WaitUntil(() => f.ReadCommittedSnapshot().GetAwaiter().GetResult().Model.Jobs.Count(j => j.State == "succeeded") == 41, Wait, "41 succeeded");
        }
        catch (TimeoutException)
        {
            var s = await f.ReadCommittedSnapshot();
            Assert.Fail($"states: {string.Join(", ", s.Model.Jobs.GroupBy(j => j.State).Select(g => g.Key + "=" + g.Count()))}; "
                + $"events {string.Join(",", w.Events.Select(e => e.Item1).Distinct().Take(30))}; invocations {w.Script.Invocations.Count}; receipts {string.Join(", ", s.Model.Receipts.GroupBy(r => r.Operation).Select(g => g.Key + "=" + g.Count()))}");
        }

        await w.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, w.Script.InvocationsOf("flaky"));
        Assert.Equal(42, w.Script.Invocations.Count);
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));
    }

    /// <summary>
    /// USG-03 (16 §3 AttemptResourceUsage): a handler that burns about 300 ms of CPU and allocates about 64 MB records at least
    /// that much against its attempt; one that does almost nothing records far less CPU; with the option off nothing is recorded.
    /// </summary>
    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "USG-03")]
    public async Task A_cpu_burning_allocating_handler_records_plausible_usage(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("usg_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 1);
        w.Script.On("burn", (_, _, _) =>
        {
            var until = System.Diagnostics.Stopwatch.GetTimestamp() + System.Diagnostics.Stopwatch.Frequency * 3 / 10;
            long keep = 0;
            for (var i = 0; i < 64; i++)
            {
                keep += new byte[1 << 20].Length; // 64 x 1 MiB
            }

            var x = 0.0;
            while (System.Diagnostics.Stopwatch.GetTimestamp() < until)
            {
                x += Math.Sqrt(keep + x);
            }

            return x > 0 ? Task.CompletedTask : Task.FromException(new InvalidOperationException());
        });
        await w.StartAsync();
        var burn = (await w.DispatchAsync("burn")).JobId.Value;
        var idle = (await w.DispatchAsync("idle")).JobId.Value;
        var session = (await f.OpenClient()).Services.GetRequiredService<IManagementSession>();
        var admin = new ManagementCaller(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);
        async Task<AttemptResourceUsage?> UsageOf(Guid job) =>
            (await session.GetJobAsync(admin, job, CancellationToken.None))?.Attempts.SingleOrDefault(a => a.FinishedAt is not null)?.Usage;
        await WorkerHost.WaitUntil(() => UsageOf(burn).GetAwaiter().GetResult() is not null && UsageOf(idle).GetAwaiter().GetResult() is not null, Wait, "both recorded");
        await w.StopAsync(TestContext.Current.CancellationToken);

        var b = (await UsageOf(burn))!;
        var i0 = (await UsageOf(idle))!;
        Assert.InRange(b.ProcessCpuMicroseconds, 250_000, 30_000_000);
        Assert.True(b.ProcessAllocatedBytes >= 64L << 20, $"allocated {b.ProcessAllocatedBytes}");
        Assert.True(b.WorkingSetPeakBytes >= Math.Max(b.WorkingSetStartBytes, b.WorkingSetEndBytes));
        Assert.True(b.WorkingSetStartBytes > 0);
        Assert.Equal(0, b.OverlappingAttempts);
        Assert.True(i0.ProcessCpuMicroseconds < b.ProcessCpuMicroseconds / 2, $"idle cpu {i0.ProcessCpuMicroseconds} vs burn {b.ProcessCpuMicroseconds}");
        Assert.True(i0.ProcessAllocatedBytes < 32L << 20, $"idle allocated {i0.ProcessAllocatedBytes}");
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "USG-04")]
    public async Task With_RecordResourceUsage_off_an_attempt_reads_as_not_recorded(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("usg_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, options: o => o.RecordResourceUsage = false);
        await w.StartAsync();
        var job = (await w.DispatchAsync("j")).JobId.Value;
        var session = (await f.OpenClient()).Services.GetRequiredService<IManagementSession>();
        var admin = new ManagementCaller(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);
        JobDetail? detail = null;
        await WorkerHost.WaitUntil(() => (detail = session.GetJobAsync(admin, job, CancellationToken.None).GetAwaiter().GetResult())?.Job.State == "succeeded", Wait, "succeeded");
        await w.StopAsync(TestContext.Current.CancellationToken);
        Assert.Null(Assert.Single(detail!.Attempts).Usage);
    }

    [Theory]
    [MemberData(nameof(CoreContractTests.Kinds), MemberType = typeof(CoreContractTests))]
    [Trait("Case", "WDT-03")]
    public async Task A_running_worker_publishes_its_detail_with_the_attempts_it_finished(StoreKind kind)
    {
        await using var f = await DriverFixture.StartAsync("wdt_" + Guid.NewGuid().ToString("N")[..8], kind);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults);
        await w.StartAsync();
        var job = (await w.DispatchAsync("j")).JobId.Value.ToString("D");
        var session = (await f.OpenClient()).Services.GetRequiredService<IManagementSession>();
        var admin = new ManagementCaller(new ManagementActor("admin", null, "test"), [.. ManagementPermissions.All.Select(p => new ManagementGrant(p))]);
        System.Text.Json.Nodes.JsonNode? detail = null;
        await WorkerHost.WaitUntil(() => (detail = session.ListWorkersAsync(admin).GetAwaiter().GetResult().Items.SingleOrDefault()?.Detail)?["recent"]?.AsArray()
            .Any(r => r!["jobId"]!.GetValue<string>() == job) == true, Wait, "the finished attempt is published");
        await w.StopAsync(TestContext.Current.CancellationToken);

        var recent = detail!["recent"]!.AsArray().First(r => r!["jobId"]!.GetValue<string>() == job)!;
        Assert.Equal("succeeded", recent["outcome"]!.GetValue<string>());
        Assert.False(string.IsNullOrEmpty(recent["queue"]!.GetValue<string>()));
        Assert.Equal(2, detail["v"]!.GetValue<int>());
        Assert.Contains(recent["jobName"]!.GetValue<string>(), detail["handlers"]!.AsArray().Select(h => h!.GetValue<string>()));
        Assert.Equal(Environment.ProcessorCount, detail["host"]!["cpus"]!.GetValue<int>());
        Assert.True(detail["host"]!["memoryTotalBytes"]!.GetValue<long>() > 0);
        Assert.NotEmpty(detail["pools"]!.AsArray());
        Assert.True(detail["settings"]!["leaseDurationMs"]!.GetValue<long>() > 0);
        Assert.True(detail["settings"]!["recordResourceUsage"]!.GetValue<bool>());
        Assert.NotNull(detail["runningCount"]);
    }
}
