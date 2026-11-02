using System.Collections.Concurrent;
using Npgsql;
using Runwick.Harness.Jobs;
using Runwick.Harness.Store;
using Runwick.Postgres.Store;
using Runwick.Protocol;

namespace Runwick.Postgres.Tests;

/// <summary>
/// Performance plan PF-03 (04 §9.2, "Changes (2026-11-01, PF-03)"): automatic statement preparation on the
/// driver-owned data sources, planned per bind; the 0A000 stale-plan retry after a schema change under load; and the
/// warm control pool.
/// </summary>
public sealed class PreparedStatementTests
{
    static async Task<List<int>> BackendsAsync(DriverFixture f, string applicationName)
    {
        var conn = new NpgsqlConnection(f.AdminConnectionString);
        await using (conn.ConfigureAwait(false))
        {
            await conn.OpenAsync();
            using var cmd = new NpgsqlCommand(
                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND application_name = @app ORDER BY pid", conn);
            cmd.Parameters.AddWithValue("app", applicationName);
            var pids = new List<int>();
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                pids.Add(r.GetInt32(0));
            }

            return pids;
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-03")]
    [Trait("Case", "PGX-11")]
    public async Task Driver_connections_prepare_statements_and_plan_each_bind()
    {
        await using var f = await DriverFixture.StartAsync("prep_" + Guid.NewGuid().ToString("N")[..8]);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        for (var i = 0; i < 3; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("p" + i));
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            await c.BeginAsync(lease);
            Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(lease)).Status);
            Assert.Equal(StoreStatus.Applied, (await c.FinishAsync(lease, AttemptOutcome.Succeeded)).Status);
        }

        var sources = (PgDataSources)c.Services.GetService(typeof(PgDataSources))!;

        // The control pool served only Renew and Finish: its connection has prepared their statements.
        var conn = await sources.For(PgLane.Renewal).OpenConnectionAsync();
        await using (conn)
        {
            using var mode = new NpgsqlCommand("SELECT current_setting('plan_cache_mode')", conn);
            Assert.Equal("force_custom_plan", (string?)await mode.ExecuteScalarAsync());
            using var prepared = new NpgsqlCommand("SELECT count(*) FROM pg_prepared_statements", conn);
            Assert.True((long)(await prepared.ExecuteScalarAsync())! > 0, "the control connection prepared nothing");
        }

        var work = await sources.For(PgLane.Producer).OpenConnectionAsync();
        await using (work)
        {
            using var mode = new NpgsqlCommand("SELECT current_setting('plan_cache_mode')", work);
            Assert.Equal("force_custom_plan", (string?)await mode.ExecuteScalarAsync());
        }
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-03")]
    [Trait("Case", "DEP-08")]
    public async Task A_schema_change_under_load_is_a_transient_retry_never_a_failure()
    {
        await using var f = await DriverFixture.StartAsync("prepmig_" + Guid.NewGuid().ToString("N")[..8]);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        var runner = (PgRunner)c.Services.GetService(typeof(PgRunner))!;

        // Warm: every statement of the job path is used more than twice, so each pooled connection has prepared it.
        for (var i = 0; i < 4; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("w" + i));
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            await c.BeginAsync(lease);
            await c.RenewAsync(lease);
            await c.FinishAsync(lease, AttemptOutcome.Succeeded);
        }

        using var stop = new CancellationTokenSource();
        var failures = new ConcurrentQueue<string>();
        var cycles = 0;
        async Task Loop(int n)
        {
            var i = 0;
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await c.Dispatcher.DispatchAsync(new PingJob($"l{n}-{i++}"));
                    var reserved = await c.ReserveAsync();
                    if (reserved.Status != StoreStatus.Applied)
                    {
                        if (reserved.Status != StoreStatus.NoWork)
                        {
                            failures.Enqueue("Reserve " + reserved.Status + " " + reserved.Error?.Code);
                        }

                        continue;
                    }

                    var lease = reserved.Value!.Lease.Handle;
                    var begun = (await c.BeginAsync(lease)).Reply;
                    var renewed = await c.RenewAsync(lease);
                    var finished = await c.FinishAsync(lease, AttemptOutcome.Succeeded);
                    foreach (var (op, status, code) in new[]
                    {
                        ("Begin", begun.Status, begun.Error?.Code), ("Renew", renewed.Status, renewed.Error?.Code),
                        ("Finish", finished.Status, finished.Error?.Code),
                    })
                    {
                        if (status != StoreStatus.Applied)
                        {
                            failures.Enqueue($"{op} {status} {code}");
                        }
                    }

                    Interlocked.Increment(ref cycles);
                }
                catch (Exception ex)
                {
                    failures.Enqueue(ex.GetType().Name + ": " + ex.Message);
                }
            }
        }

        var load = Enumerable.Range(0, 3).Select(n => Task.Run(() => Loop(n))).ToArray();
        await Task.Delay(500);

        // The "migration": a column the gate statements return changes type (binary-compatible, no rewrite), so every
        // statement prepared over it must not reuse its cached plan (0A000).
        await f.ExecAsync("""ALTER TABLE runwick.store_metadata ALTER COLUMN migration_mode TYPE varchar(16) COLLATE "C" """);
        var at = cycles;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (Volatile.Read(ref cycles) < at + 6 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }

        await stop.CancelAsync();
        await Task.WhenAll(load);

        Assert.Empty(failures);
        Assert.True(cycles >= at + 6, $"the load made no progress after the schema change ({cycles - at} cycles)");
        Assert.True(runner.StalePlans >= 1, "no 0A000 was met: the test did not exercise the stale-plan path");
        Assert.Empty(await f.HistoryViolations());
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-03")]
    [Trait("Case", "PGX-12")]
    public async Task The_control_pool_keeps_two_warm_connections_after_idle()
    {
        await using var f = await DriverFixture.StartAsync("warm_" + Guid.NewGuid().ToString("N")[..8]);

        // A short idle lifetime stands in for the 300 s default: an idle connection beyond the minimum is closed
        // within a few seconds.
        var cs = new NpgsqlConnectionStringBuilder(f.ClientConnectionString("warm"))
        {
            ConnectionIdleLifetime = 2,
            ConnectionPruningInterval = 1,
        }.ConnectionString;
        await using var c = await TestClient.CreateAsync(cs, f.Application, f.Faults, r => TestJobs.Register(r));

        var leases = new List<LeaseHandle>();
        for (var i = 0; i < 4; i++)
        {
            await c.Dispatcher.DispatchAsync(new PingJob("k" + i));
            var lease = (await c.ReserveAsync()).Value!.Lease.Handle;
            await c.BeginAsync(lease);
            leases.Add(lease);
        }

        // Concurrent renewals open up to the lane width of control connections.
        foreach (var r in await Task.WhenAll(leases.Select(l => c.RenewAsync(l))))
        {
            Assert.Equal(StoreStatus.Applied, r.Status);
        }

        var opened = await BackendsAsync(f, "rw:warm:control");
        Assert.NotEmpty(opened);

        await Task.Delay(TimeSpan.FromSeconds(8));
        var idle = await BackendsAsync(f, "rw:warm:control");
        Assert.Equal(Math.Min(opened.Count, PgDataSources.ControlWarm), idle.Count);
        Assert.Subset(opened.ToHashSet(), idle.ToHashSet());

        // The next Renew after idle uses a warm connection: no physical open on the control pool.
        Assert.Equal(StoreStatus.Applied, (await c.RenewAsync(leases[0])).Status);
        Assert.Equal(idle, await BackendsAsync(f, "rw:warm:control"));
    }

    [Fact]
    [Trait("Category", "Contract")]
    [Trait("Item", "PF-08")]
    [Trait("Case", "POOL-01")]
    public async Task Pool_returns_send_no_reset_and_still_roll_back_an_open_transaction()
    {
        // 04 Changes 2026-11-02, PF-08: the driver pools skip the reset sequence; nothing a transaction set or wrote
        // survives the return of its connection.
        await using var f = await DriverFixture.StartAsync("noreset_" + Guid.NewGuid().ToString("N")[..8]);
        var c = await f.OpenClient(configure: r => TestJobs.Register(r));
        await f.ExecAsync("CREATE TABLE public.left_open (id integer PRIMARY KEY)");
        await f.ExecAsync("GRANT ALL ON public.left_open TO PUBLIC");
        var sources = (PgDataSources)c.Services.GetService(typeof(PgDataSources))!;
        int pid;
        var conn = await sources.For(PgLane.Renewal).OpenConnectionAsync();
        await using (conn)
        {
            using var who = new NpgsqlCommand("SELECT pg_backend_pid()", conn);
            pid = (int)(await who.ExecuteScalarAsync())!;
            var tx = await conn.BeginTransactionAsync();
            using var set = new NpgsqlCommand("SELECT set_config('lock_timeout', '1234ms', true); INSERT INTO public.left_open VALUES (1)", conn, tx);
            await set.ExecuteNonQueryAsync();

            // Returned to the pool with the transaction still open.
        }

        // The driver rolled the transaction back on return: the backend is idle (not idle in transaction), the row is gone
        // and the setting was transaction-local.
        Assert.Equal(1, await f.CountAsync("SELECT count(*) FROM pg_stat_activity WHERE pid = @pid AND state = 'idle'", ("pid", pid)));
        Assert.Equal(0, await f.CountAsync("SELECT count(*) FROM public.left_open"));
        for (var i = 0; i < 4; i++)
        {
            var again = await sources.For(PgLane.Renewal).OpenConnectionAsync();
            await using (again)
            {
                using var setting = new NpgsqlCommand("SELECT current_setting('lock_timeout')", again);
                Assert.NotEqual("1234ms", (string?)await setting.ExecuteScalarAsync());
            }
        }
    }
}
