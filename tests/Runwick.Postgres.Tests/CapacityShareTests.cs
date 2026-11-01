using Runwick.Harness.Engine;
using Runwick.Harness.Store;
using Runwick.ReferenceModel;
using Runwick.Registration;
using Runwick.Worker;
using static Runwick.Postgres.Tests.DispatchSupport;

namespace Runwick.Postgres.Tests;

/// <summary>
/// SP-012 real-store halves of CAP-01 (fixed-mode remainder), CAP-02 and CAP-03 (capacity-defaults-and-budgets §1–§2,
/// §7; 15 §5). The Pure halves are <c>CapacityCaseTests</c>. Selection is serial per pool (one Reserve outstanding), so
/// the admission order is the store order of the reservations (<c>reserved_at</c>, then the time-ordered attempt ID).
/// </summary>
[Trait("Item", "SP-012")]
public sealed class CapacityShareTests
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(90);

    static string Ns(string name) => name + "_" + Guid.NewGuid().ToString("N")[..8];

    static async Task AssertInvariants(DriverFixture f) =>
        Assert.Empty(SnapshotInvariants.Violations((await f.ReadCommittedSnapshot()).Model));

    static async Task DispatchManyAsync(WorkerHost w, IEnumerable<string> keys, DispatchOptions? options = null)
    {
        foreach (var chunk in keys.Chunk(100))
        {
            await w.Dispatcher.DispatchManyAsync([.. chunk.Select(k => new BatchDispatchItem(new WorkJob(k), options))]);
        }
    }

    static async Task SampleAsync(TimeSpan window, Action sample)
    {
        var end = DateTime.UtcNow + window;
        while (DateTime.UtcNow < end)
        {
            sample();
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// The first letter of <paramref name="column"/> (queue or priority) of every reservation of <paramref name="w"/>, in
    /// admission order, with whether it was reserved before <paramref name="before"/> (store time).
    /// </summary>
    static async Task<List<(char Key, bool Early)>> Admissions(DriverFixture f, WorkerHost w, string column, string before = "'infinity'::timestamptz")
    {
        var rows = await TextAsync(f, $"""
            SELECT left(j.{column}, 1) || CASE WHEN a.reserved_at < {before} THEN '1' ELSE '0' END
            FROM runwick.job_attempts AS a
            JOIN runwick.jobs AS j ON j.application = a.application AND j.job_id = a.job_id
            WHERE a.worker_instance_id = @i
            ORDER BY a.reserved_at, a.attempt_id
            """, ("i", w.Engine.WorkerInstanceId));
        return [.. rows.Select(r => (r[0], r[1] == '1'))];
    }

    static async Task<List<char>> Order(DriverFixture f, WorkerHost w, string column) =>
        [.. (await Admissions(f, w, column)).Select(a => a.Key)];

    // Admissions are serial per pool, one Reserve round trip each; on the shared host that is 0.3–1 s under load.
    // A reservation whose lease margin could not be confirmed before Begin on the loaded host is given up without an
    // invocation (05 §9.3, lost:SafetyDeadline) and its job stays reserved until expiry recovery, then runs once. Such a
    // job is not begun; the admission bounds are on invocations and gate tickets, which these tests sample directly.
    static async Task<long> NotBegunAsync(DriverFixture f, WorkerHost w)
    {
        var lost = w.Events.Where(e => e.Name.StartsWith("lost:", StringComparison.Ordinal)).Select(e => e.Name).ToList();
        var pending = await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'pending'");
        var reserved = await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'reserved'");
        Assert.True(reserved <= lost.Count, $"{reserved} reserved jobs but only {lost.Count} given-up reservations ({string.Join(", ", lost)})");
        return pending + reserved;
    }

    // One success per job, and no second execution while the handlers were hung (before @released). After the release a
    // lease that expired under starved renewals may be recovered and run again (at-least-once), which is not CAP-01.
    const string OneExecutionViolations = """
        SELECT count(*) FROM (
            SELECT job_id FROM runwick.job_attempts GROUP BY job_id
            HAVING count(*) FILTER (WHERE execution_number IS NOT NULL AND authorized_at < @released) > 1
                OR count(*) FILTER (WHERE outcome = 'succeeded') <> 1) x
        """;

    static Task WaitAdmissions(DriverFixture f, WorkerHost w, int n, string what) =>
        WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.job_attempts WHERE worker_instance_id = @i",
            ("i", w.Engine.WorkerInstanceId)).Result >= n, TimeSpan.FromMinutes(5), what);

    // ------------------------------------------------------------------ CAP-01 (fixed-mode remainder)

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-01")]
    public async Task Invalid_fixed_pools_fail_startup_before_any_store_write()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap01inv"));
        var invalid = new (string What, int Concurrency, Action<WorkerBuilder>? Worker, string Code)[]
        {
            ("FixedConcurrency(0)", 0, null, RunwickErrorCodes.InvalidPool),
            ("maxima 8 + 8 + 1 = 17 > 16", 8, w => w.Pool("b", p => p.Queues("other").FixedConcurrency(8)).Pool("c", p => p.Queues("other").FixedConcurrency(1)),
                RunwickErrorCodes.PoolBudgetExceeded),
        };
        foreach (var (what, concurrency, worker, code) in invalid)
        {
            var ex = Assert.Throws<RunwickConfigurationException>(() => WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults,
                concurrency: concurrency, configure: r => r.Queue("other"), worker: worker,
                pool: concurrency == 0 ? p => p.FixedConcurrency(0) : null));
            Assert.True(ex.Errors.Any(e => e.Code == code), $"{what}: {string.Join(", ", ex.Errors.Select(e => e.Code))}");
        }

        // Nothing reached the store: no presence, no command, no registration.
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.worker_instances"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.storage_commands"));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM runwick.application_controls"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-01")]
    public async Task Fixed_pools_keep_their_own_maxima_and_the_group_limit_under_hung_handlers()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap01grp"));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 8,
            configure: r => r.Queue("other"), worker: b => b.Pool("side", p => p.Queues("other").FixedConcurrency(8)));
        w.Script.Default = (job, _, _) => job.Id.StartsWith("quick", StringComparison.Ordinal) ? Task.CompletedTask : release.Task;
        var main = w.Engine.Pools.Single(p => p.Settings.Name == "main");
        var side = w.Engine.Pools.Single(p => p.Settings.Name == "side");
        Assert.Equal((8, 8, 16), (main.Gate.Target, side.Gate.Target, w.Engine.Settings.GroupMax));

        // Pool main fills with hung handlers; pool side still admits: one pool's held tickets are not another's.
        await DispatchManyAsync(w, Enumerable.Range(0, 12).Select(i => "hung-d" + i));
        await w.StartAsync();
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 8, Wait, "pool main full");
        await DispatchManyAsync(w, Enumerable.Range(0, 4).Select(i => "quick" + i), new DispatchOptions { Queue = "other" });
        await WorkerHost.WaitUntil(() => w.Script.Completed.Count == 4, Wait, "pool side admits while main is hung");

        // Both pools full: 16 hung handlers = the group maximum; no replacement attempt is admitted.
        await DispatchManyAsync(w, Enumerable.Range(0, 12).Select(i => "hung-o" + i), new DispatchOptions { Queue = "other" });
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 20, Wait, "pool side full");
        var (maxMain, maxSide, maxEntries) = (0, 0, 0);
        await SampleAsync(TimeSpan.FromSeconds(10), () =>
        {
            maxMain = Math.Max(maxMain, main.Gate.Outstanding);
            maxSide = Math.Max(maxSide, side.Gate.Outstanding);
            maxEntries = Math.Max(maxEntries, w.Engine.Entries.Count);
        });
        Assert.Equal((8, 8, 16), (maxMain, maxSide, maxEntries));
        Assert.Equal(20, w.Script.Invocations.Count);
        Assert.Equal(8, await NotBegunAsync(f, w));
        Assert.Equal(16, await f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'running'"));

        var released = await f.StoreNow();
        release.SetResult();
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 28, Wait, "all 28 succeeded");
        await w.StopAsync();
        Assert.Equal(0, await f.CountAsync(OneExecutionViolations, ("released", released)));
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-01")]
    public async Task A_pool_with_no_mode_is_fixed_four_against_the_real_store()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap01def"));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 0);
        w.Script.Default = (_, _, _) => release.Task;
        await DispatchManyAsync(w, Enumerable.Range(0, 7).Select(i => "hung" + i));
        await w.StartAsync();
        var pool = w.Engine.Pools.Single();
        Assert.Equal(WorkerSettings.DefaultPoolTarget, pool.Gate.Target);
        await WorkerHost.WaitUntil(() => w.Script.Invocations.Count == 4, Wait, "four hung handlers");
        var max = 0;
        await SampleAsync(TimeSpan.FromSeconds(5), () => max = Math.Max(max, pool.Gate.Outstanding));
        Assert.Equal(4, max);
        Assert.Equal(4, w.Script.Invocations.Count);
        Assert.Equal(3, await NotBegunAsync(f, w));
        release.SetResult();
        await WorkerHost.WaitUntil(() => f.CountAsync("SELECT count(*) FROM runwick.jobs WHERE state = 'succeeded'").Result == 7, Wait, "all succeeded");
        await AssertInvariants(f);
    }

    // ------------------------------------------------------------------ CAP-02

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-02")]
    public async Task Queue_and_lane_shares_hold_against_the_real_store_and_a_returning_queue_banks_no_credit()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap02"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 4,
            configure: r => r.Queue("other"), pool: p => p.Queue("other", 3));
        await DispatchManyAsync(w, Enumerable.Range(0, 60).Select(i => "d" + i));
        await DispatchManyAsync(w, Enumerable.Range(0, 200).Select(i => "o" + i), new DispatchOptions { Queue = "other" });
        await w.StartAsync();

        // Queues weighted 1:3, both continuously claimable: 10 of every 40 admissions go to "default", never two in a row.
        await WaitAdmissions(f, w, 40, "40 admissions");
        var first = (await Order(f, w, "queue")).Take(40).ToList();
        Assert.InRange(first.Count(k => k == 'd'), 9, 11);
        Assert.DoesNotContain(first.Zip(first.Skip(1)), p => p.First == 'd' && p.Second == 'd');

        // "default" leaves the eligible set (paused) while "other" keeps running, then returns: a returning member
        // starts at zero (capacity-defaults §2), so it gets no burst for the admissions it missed.
        await f.SetQueuePaused(f.Application, "default", true);
        var pausedAt = (await Order(f, w, "queue")).Count;
        await WaitAdmissions(f, w, pausedAt + 40, "40 admissions while paused");
        var during = (await Order(f, w, "queue")).Skip(pausedAt + 8).Take(30).ToList(); // the first may come from a hint that predates the pause
        Assert.All(during, k => Assert.Equal('o', k));
        await f.SetQueuePaused(f.Application, "default", false);
        var resumedAt = (await Order(f, w, "queue")).Count;
        await WaitAdmissions(f, w, resumedAt + 48, "48 admissions after resume");
        var after = (await Order(f, w, "queue")).Skip(resumedAt + 8).Take(40).ToList();
        Assert.InRange(after.Count(k => k == 'd'), 9, 11);
        Assert.DoesNotContain(after.Zip(after.Skip(1)), p => p.First == 'd' && p.Second == 'd');
        await w.StopAsync();
        await AssertInvariants(f);
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-02")]
    public async Task Lane_shares_are_six_three_one_against_the_real_store()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap02l"));
        await using var w = WorkerHost.Create(f.ClientConnectionString("worker"), f.Application, f.Faults, concurrency: 4);
        foreach (var (p, count) in new[] { ("high", 120), ("medium", 60), ("low", 30) })
        {
            await DispatchManyAsync(w, Enumerable.Range(0, count).Select(i => p[0] + i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                new DispatchOptions { Priority = p });
        }

        await w.StartAsync();
        await WaitAdmissions(f, w, 100, "100 admissions");
        await w.StopAsync();

        // Only admissions before any lower lane could be aged count (its oldest job younger than 60 s, capacity-defaults
        // §2): there weights alone divide the opportunities, 6:3:1 per cycle of 10. A slow host shortens the window.
        var early = (await Admissions(f, w, "priority",
            "(SELECT min(eligible_at) FROM runwick.jobs WHERE priority <> 'high') + interval '59 seconds'")).TakeWhile(a => a.Early).ToList();
        var n = Math.Min(100, early.Count / 10 * 10);
        Assert.True(n >= 30, $"only {early.Count} admissions before the aging threshold");
        var picks = early.Take(n).Select(a => a.Key).ToList();
        Assert.InRange(picks.Count(k => k == 'h'), (6 * n / 10) - 1, (6 * n / 10) + 1);
        Assert.InRange(picks.Count(k => k == 'm'), (3 * n / 10) - 1, (3 * n / 10) + 1);
        Assert.InRange(picks.Count(k => k == 'l'), (n / 10) - 1, (n / 10) + 1);
        await AssertInvariants(f);
    }

    // ------------------------------------------------------------------ CAP-03

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Case", "CAP-03")]
    public async Task Strict_starves_an_aged_lane_and_strict_with_aging_takes_it_every_ninth_admission()
    {
        await using var f = await DriverFixture.StartAsync(Ns("cap03"));
        var c = await f.OpenClient(configure: r => WorkerHost.RegisterWork(r));
        foreach (var chunk in Enumerable.Range(0, 100).Select(i => "h" + i).Chunk(100))
        {
            await c.Dispatcher.DispatchManyAsync([.. chunk.Select(k => new BatchDispatchItem(new WorkJob(k), new DispatchOptions { Priority = "high" }))]);
        }

        await c.Dispatcher.DispatchManyAsync([.. Enumerable.Range(0, 10).Select(i => new BatchDispatchItem(new WorkJob("l" + i), new DispatchOptions { Priority = "low" }))]);

        // Setup only: the low lane's eligibility is moved 61 s back in store time, so its age is ≥ the 60 s threshold.
        await f.ExecAsync("UPDATE runwick.jobs SET eligible_at = runwick.ms_floor(eligible_at - interval '61 seconds') WHERE priority = 'low'");

        // Strict: the continuously claimable high lane starves the aged low lane (documented behaviour).
        await using (var strict = WorkerHost.Create(f.ClientConnectionString("strict"), f.Application, f.Faults, concurrency: 4,
            pool: p => p.LaneSelection(LaneSelectionMode.Strict)))
        {
            await strict.StartAsync();
            await WaitAdmissions(f, strict, 30, "30 strict admissions");
            await strict.StopAsync();
            Assert.All(await Order(f, strict, "priority"), k => Assert.Equal('h', k));
        }

        // Strict with aging: after 8 admissions in the queue the aged lower lane gets the next one, then the counter
        // resets, so every ninth admission is low (store-time age from the probe; threshold equality is the Pure half).
        await using var aging = WorkerHost.Create(f.ClientConnectionString("aging"), f.Application, f.Faults, concurrency: 4,
            pool: p => p.LaneSelection(LaneSelectionMode.StrictWithAging));
        await aging.StartAsync();
        await WaitAdmissions(f, aging, 45, "45 admissions with aging");
        await aging.StopAsync();
        var picks = (await Order(f, aging, "priority")).Take(45).ToList();
        Assert.Equal(5, picks.Count(k => k == 'l'));
        Assert.All(picks.Where((_, i) => i % 9 == 8), k => Assert.Equal('l', k));
        await AssertInvariants(f);
    }
}
