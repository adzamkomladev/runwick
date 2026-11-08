using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Runwick;
using Runwick.Outbox.Postgres;
using Runwick.Outbox.Sqlite;
using Runwick.Postgres.Migrations;

/// <summary>One scenario of docs/scenarios.md. <c>Run</c> returns a one-line detail or throws with the reason it failed.</summary>
sealed record Scenario(string Id, string Title, Func<Task<string>> Run);

static class Catalogue
{
    static readonly StoreKind[] Stores = [StoreKind.Pg, StoreKind.Sqlite, StoreKind.Redis];

    public static IReadOnlyList<Scenario> All { get; } = Build();

    static string Id(int n) => $"SCN-{n:00}";

    static List<Scenario> Build()
    {
        var list = new List<Scenario>();
        var n = 0;
        foreach (var s in Stores)
        {
            for (var m = 1; m <= 5; m++)
            {
                var (store, mode) = (s, m);
                list.Add(new Scenario(Id(++n), $"Deployment mode {mode} on {store}", () => Mode(store, mode)));
            }
        }

        list.Add(new Scenario(Id(++n), "Management API, Basic login with antiforgery (PostgreSQL)", Auth.BasicAsync));
        list.Add(new Scenario(Id(++n), "Management API, bearer token for machine clients, roles and queue scope (PostgreSQL)", Auth.BearerAsync));
        list.Add(new Scenario(Id(++n), "Dashboard, cookie login (PostgreSQL)", Auth.DashboardAsync));
        foreach (var s in Stores)
        {
            var store = s;
            list.Add(new Scenario(Id(++n), $"Schedule fires, two scheduler replicas, separate worker on {store}", () => Schedule(store)));
        }

        foreach (var s in Stores)
        {
            var store = s;
            list.Add(new Scenario(Id(++n), $"Capacity: queue limit across two workers, then autoscaler on {store}", () => Capacity(store)));
        }

        foreach (var s in Stores)
        {
            var store = s;
            list.Add(new Scenario(Id(++n), $"Drain, refused startup, reopen on {store}", () => Drain(store)));
        }

        foreach (var s in Stores)
        {
            var store = s;
            list.Add(new Scenario(Id(++n), $"Store outage and recovery on {store}", () => Outage(store)));
        }

        foreach (var s in Stores)
        {
            var store = s;
            list.Add(new Scenario(Id(++n), $"Outbox: {(store == StoreKind.Pg ? "PostgreSQL source" : "SQLite source")} to {store} destination", () => Outbox(store)));
        }

        return list;
    }

    static NodeOptions Opts(Scratch sc, string name, Role role, string? sink = null) =>
        new(sc.Kind, sc.Conn, sc.App, sink ?? sc.Sink(), name, role);

    // ---- SCN-01..15: the five deployment modes (spec 00 §5) ----
    static async Task<string> Mode(StoreKind store, int mode)
    {
        await using var sc = await Scratch.CreateAsync(store, "mode");
        (string Name, Role Role, int Jobs)[] plan = mode switch
        {
            1 => [("app1", Role.Both, 30)],
            2 => [("w1", Role.Worker, 0), ("app1", Role.Producer, 30)],
            3 => [("r1", Role.Both, 10), ("r2", Role.Both, 10), ("r3", Role.Both, 10)],
            4 => [("w1", Role.Worker, 0), ("w2", Role.Worker, 0), ("web1", Role.Producer, 15), ("web2", Role.Producer, 15)],
            _ => [("w1", Role.Worker, 0), ("w2", Role.Worker, 0), ("r1", Role.Both, 15), ("r2", Role.Both, 15)],
        };
        var children = new List<Child>();
        try
        {
            foreach (var (name, role, jobs) in plan)
            {
                var c = Child.Node(Opts(sc, name, role) with { Jobs = jobs, Ms = 40 });
                children.Add(c);
                await c.ReadyAsync();
            }

            var total = plan.Sum(p => p.Jobs);
            await Wait.UntilAsync(() => Wait.Count(sc.Sink()) >= total, 90, $"{total} jobs executed");
            var by = Wait.ByProcess(sc.Sink());
            var producerOnly = plan.Where(p => p.Role == Role.Producer).Select(p => p.Name);
            if (by.Keys.Intersect(producerOnly).Any())
            {
                throw new InvalidOperationException("a producer-only process ran a job");
            }

            await Task.Delay(1500);
            if (Wait.Count(sc.Sink()) != total)
            {
                throw new InvalidOperationException($"expected exactly {total} distinct jobs, found {Wait.Count(sc.Sink())}; by process {string.Join(",", by)}");
            }

            return $"{total} jobs, ran in: {string.Join(", ", by.OrderBy(k => k.Key).Select(k => $"{k.Key}={k.Value}"))}";
        }
        finally
        {
            foreach (var c in children)
            {
                await c.StopAsync();
                await c.DisposeAsync();
            }
        }
    }

    // ---- SCN-19..21: schedules ----
    static async Task<string> Schedule(StoreKind store)
    {
        await using var sc = await Scratch.CreateAsync(store, "sched");
        await ActivateSchedulerAsync(sc);
        var nodes = new[]
        {
            Opts(sc, "worker", Role.Worker), Opts(sc, "sched1", Role.Scheduler) with { Tick = true }, Opts(sc, "sched2", Role.Scheduler) with { Tick = true },
        };
        var children = new List<Child>();
        try
        {
            foreach (var o in nodes)
            {
                var c = Child.Node(o);
                children.Add(c);
                await c.ReadyAsync();
            }

            const int window = 14;
            await Task.Delay(TimeSpan.FromSeconds(window));
            await children[1].StopAsync();
            await children[2].StopAsync();
            await Task.Delay(1500); // let the last materialized job run
            var ran = Wait.Count(sc.Sink());
            var by = Wait.ByProcess(sc.Sink());
            // One "every 2 s" schedule over 14 s is about 7 jobs. Two schedulers must not double it.
            if (ran < 3 || ran > window / 2 + 3)
            {
                throw new InvalidOperationException($"{ran} schedule jobs in {window} s (expected 3 to {window / 2 + 3})");
            }

            return $"{ran} jobs in {window} s from 2 scheduler replicas, all on {string.Join(",", by.Keys)}";
        }
        finally
        {
            foreach (var c in children)
            {
                await c.StopAsync();
                await c.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// A schedule cannot be registered until the application raises capability "scheduler" (spec 13 §6.1). A fresh application is
    /// registered by its first start, so start one producer once, then make the operator call (CLI: <c>store activate-capability</c>).
    /// </summary>
    static async Task ActivateSchedulerAsync(Scratch sc)
    {
        using (var host = NodeHost.Create(Opts(sc, "activate", Role.Producer)))
        {
            await host.StartAsync();
            await host.StopAsync();
        }

        await sc.ActivateSchedulerAsync();
    }

    // ---- SCN-22..24: capacity ----
    static async Task<string> Capacity(StoreKind store)
    {
        await using var sc = await Scratch.CreateAsync(store, "cap");
        // Part 1: a queue limit of 3 holds across two worker processes that could each run 4.
        var w1 = Child.Node(Opts(sc, "w1", Role.Worker) with { Cap = 3, Concurrency = 4, Ms = 400 });
        var w2 = Child.Node(Opts(sc, "w2", Role.Worker) with { Cap = 3, Concurrency = 4, Ms = 400 });
        string part1;
        try
        {
            await w1.ReadyAsync();
            await w2.ReadyAsync();
            await using var p = Child.Node(Opts(sc, "p1", Role.Producer) with { Cap = 3, Jobs = 24, Ms = 400 });
            await p.ExitAsync();
            await Wait.UntilAsync(() => Wait.Count(sc.Sink()) >= 24, 90, "24 capped jobs");
            var peak = Wait.PeakOverlap(sc.Sink());
            if (peak is > 3 or < 2)
            {
                throw new InvalidOperationException($"queue limit 3: peak {peak} jobs at once (expected 2 to 3)");
            }

            part1 = $"limit 3 held, peak {peak}";
        }
        finally
        {
            await w1.StopAsync();
            await w2.StopAsync();
            await w1.DisposeAsync();
            await w2.DisposeAsync();
        }

        // Part 2: one worker with AutoConcurrency(1, 6) grows beyond 1 under a backlog and never beyond 6.
        var sink2 = sc.Sink("sink2");
        var auto = Child.Node(Opts(sc, "auto", Role.Worker, sink2) with { Auto = (1, 6), Ms = 500 });
        try
        {
            await auto.ReadyAsync();
            await using var p = Child.Node(Opts(sc, "p2", Role.Producer, sink2) with { Jobs = 80, Ms = 500 });
            await p.ExitAsync();
            await Wait.UntilAsync(() => Wait.Count(sink2) >= 80, 150, "80 autoscaled jobs");
            var peak = Wait.PeakOverlap(sink2);
            if (peak is < 2 or > 6)
            {
                throw new InvalidOperationException($"autoscaler 1..6: peak {peak} jobs at once (expected 2 to 6)");
            }

            return $"{part1}; autoscaler 1..6 peaked at {peak}";
        }
        finally
        {
            await auto.StopAsync();
            await auto.DisposeAsync();
        }
    }

    // ---- SCN-25..27: drain, refused startup, reopen (docs/runbooks/upgrade.md steps 2 and 5) ----
    static async Task<string> Drain(StoreKind store)
    {
        await using var sc = await Scratch.CreateAsync(store, "drain");
        await using var worker = Child.Node(Opts(sc, "w1", Role.Worker));
        await worker.ReadyAsync();
        var po = Opts(sc, "api", Role.Producer) with { Ms = 40 };
        using var api = NodeHost.Create(po);
        await api.StartAsync();
        await NodeHost.DispatchAsync(api, po, 5, "a");
        await Wait.UntilAsync(() => Wait.Count(sc.Sink()) >= 5, 60, "5 jobs before drain");

        await sc.SetAdmissionAsync("drain");
        try
        {
            await NodeHost.DispatchAsync(api, po, 1, "refused");
            throw new InvalidOperationException("dispatch was accepted while drained");
        }
        catch (AdmissionClosedException)
        {
        }

        // A new process started while drained must say why and exit 75, not crash.
        await using (var late = Child.Node(Opts(sc, "late", Role.Worker)))
        {
            var code = await late.ExitAsync(60);
            if (code != 75 || !late.Output.Contains("STARTUP REFUSED", StringComparison.Ordinal) || late.Output.Contains("Unhandled", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"drained startup: exit {code}, output: {late.Output}");
            }
        }

        await sc.SetAdmissionAsync("open");
        await NodeHost.DispatchAsync(api, po, 5, "b");
        await Wait.UntilAsync(() => Wait.Count(sc.Sink()) >= 10, 60, "the original worker to run work again after reopen");
        await using var started = Child.Node(Opts(sc, "after", Role.Worker));
        await started.ReadyAsync();
        await started.StopAsync();
        return "dispatch refused while drained, new process exited 75 with a clear message, 5 more jobs ran after reopen, new process started";
    }

    // ---- SCN-28..30: store outage ----
    static async Task<string> Outage(StoreKind store)
    {
        await using var sc = await Scratch.CreateAsync(store, "outage");
        TcpGate? gate = null;
        SqliteConnection? locker = null;
        var conn = sc.Conn;
        try
        {
            if (store == StoreKind.Pg)
            {
                var b = new NpgsqlConnectionStringBuilder(sc.Conn);
                gate = new TcpGate(b.Host!, b.Port);
                b.Host = "127.0.0.1";
                b.Port = gate.Port;
                conn = b.ConnectionString;
            }
            else if (store == StoreKind.Redis)
            {
                var (host, rest) = (Scratch.RedisBase.Split(':')[0], Scratch.RedisBase.Split(':')[1].Split(',')[0]);
                gate = new TcpGate(host, int.Parse(rest));
                conn = $"127.0.0.1:{gate.Port},defaultDatabase={sc.Conn.Split('=')[^1]}";
            }

            var o = new NodeOptions(store, conn, sc.App, sc.Sink(), "w1", Role.Worker);
            await using var worker = Child.Node(o);
            await worker.ReadyAsync();
            var po = o with { Name = "api", Role = Role.Producer, Ms = 40 };
            using var api = NodeHost.Create(po);
            await api.StartAsync();
            await NodeHost.DispatchAsync(api, po, 5, "before");
            await Wait.UntilAsync(() => Wait.Count(sc.Sink()) >= 5, 60, "5 jobs before the outage");

            // Outage: PostgreSQL/Redis, every connection cut and refused; SQLite, the file is held by a writer.
            if (gate is not null)
            {
                gate.Cut();
            }
            else
            {
                locker = new SqliteConnection(sc.Conn + ";Pooling=False");
                locker.Open();
                using var begin = locker.CreateCommand();
                begin.CommandText = "BEGIN IMMEDIATE";
                begin.ExecuteNonQuery();
            }

            var started = DateTime.UtcNow;
            string during;
            try
            {
                await NodeHost.DispatchAsync(api, po, 1, "during").WaitAsync(TimeSpan.FromSeconds(30));
                during = "accepted";
            }
            catch (TimeoutException)
            {
                throw new InvalidOperationException("a dispatch hung for 30 s during the outage");
            }
            catch (RunwickException ex)
            {
                during = $"refused as {ex.GetType().Name}";
            }

            await Task.Delay(TimeSpan.FromSeconds(Math.Max(0, 6 - (DateTime.UtcNow - started).TotalSeconds)));
            if (worker.HasExited)
            {
                throw new InvalidOperationException($"the worker process exited during the outage: {worker.Output}");
            }

            gate?.Restore();
            if (locker is not null)
            {
                using var end = locker.CreateCommand();
                end.CommandText = "ROLLBACK";
                end.ExecuteNonQuery();
            }

            // Recovery: the same producer and worker carry on without a restart.
            var recovered = false;
            for (var i = 0; i < 40 && !recovered; i++)
            {
                try
                {
                    await NodeHost.DispatchAsync(api, po, 1, "after");
                    recovered = true;
                }
                catch (RunwickException)
                {
                    await Task.Delay(500);
                }
            }

            if (!recovered)
            {
                throw new InvalidOperationException("dispatch never recovered after the outage ended");
            }

            await Wait.UntilAsync(() => Wait.Count(sc.Sink()) >= 6, 60, "the worker to run work again after the outage");
            return $"dispatch during the outage: {during}; worker kept running; {Wait.Count(sc.Sink())} jobs ran in total";
        }
        finally
        {
            locker?.Dispose();
            if (gate is not null)
            {
                await gate.DisposeAsync();
            }
        }
    }

    // ---- SCN-31..33: outbox relay ----
    static async Task<string> Outbox(StoreKind destination)
    {
        await using var dest = await Scratch.CreateAsync(destination, "obx");
        string sourceConn;
        Func<Task> cleanup;
        var bad = Guid.NewGuid().ToString("N")[..6];
        if (destination == StoreKind.Pg)
        {
            sourceConn = await Scratch.NewPgDatabaseAsync("src", migrate: false);
            await PostgresOutboxSchema.InstallAsync(sourceConn);
            await Exec(sourceConn, true, "CREATE TABLE business_rows (id TEXT PRIMARY KEY)");
            cleanup = () => Scratch.DropPgDatabaseAsync(sourceConn);
        }
        else
        {
            sourceConn = $"Data Source={Path.Combine(dest.Dir, "outbox-source.db")};Pooling=False";
            await SqliteOutboxSchema.InstallAsync(sourceConn);
            await Exec(sourceConn, false, "CREATE TABLE business_rows (id TEXT PRIMARY KEY)");
            cleanup = () => Task.CompletedTask;
        }

        var o = Opts(dest, "relay", Role.Worker);
        using var host = NodeHost.Create(o, s =>
        {
            if (destination == StoreKind.Pg)
            {
                s.AddRunwickPostgresOutbox(sourceConn);
            }
            else
            {
                s.AddRunwickSqliteOutbox(sourceConn);
            }
        });
        try
        {
            Directory.CreateDirectory(o.Sink);
            await host.StartAsync();
            for (var i = 0; i < 10; i++)
            {
                await Stage(host, destination == StoreKind.Pg, sourceConn, $"row-{i}", commit: true);
            }

            await Stage(host, destination == StoreKind.Pg, sourceConn, "rolled-back-" + bad, commit: false);
            await Wait.UntilAsync(() => Wait.Count(dest.Sink()) >= 10, 90, "10 staged jobs delivered and run");
            await Task.Delay(2000);
            var ran = Wait.Count(dest.Sink());
            if (ran != 10)
            {
                throw new InvalidOperationException($"expected exactly 10 jobs (the rolled-back one must not run), found {ran}");
            }

            return "10 jobs staged with their business rows reached the destination exactly once; the rolled-back one never ran";
        }
        finally
        {
            await host.StopAsync();
            await cleanup();
        }
    }

    static async Task Stage(Microsoft.Extensions.Hosting.IHost host, bool pg, string source, string id, bool commit)
    {
        var job = new Work(id, 40);
        if (pg)
        {
            await using var c = new NpgsqlConnection(source);
            await c.OpenAsync();
            await using var tx = await c.BeginTransactionAsync();
            await using (var cmd = new NpgsqlCommand("INSERT INTO business_rows VALUES (@id)", c, tx))
            {
                cmd.Parameters.AddWithValue("id", id);
                await cmd.ExecuteNonQueryAsync();
            }

            await host.Services.GetRequiredService<IPostgresOutbox>().StageAsync(job, tx);
            await (commit ? tx.CommitAsync() : tx.RollbackAsync());
        }
        else
        {
            await using var c = new SqliteConnection(source);
            await c.OpenAsync();
            await using (var pragma = c.CreateCommand())
            {
                // The staging transaction must be on a connection with the store's durability profile (spec 20 §8).
                pragma.CommandText = "PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL;";
                await pragma.ExecuteNonQueryAsync();
            }

            await using var tx = (SqliteTransaction)await c.BeginTransactionAsync();
            await using (var cmd = c.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO business_rows VALUES ($id)";
                cmd.Parameters.AddWithValue("$id", id);
                await cmd.ExecuteNonQueryAsync();
            }

            await host.Services.GetRequiredService<ISqliteOutbox>().StageAsync(job, tx);
            await (commit ? tx.CommitAsync() : tx.RollbackAsync());
        }
    }

    static async Task Exec(string conn, bool pg, string sql)
    {
        if (pg)
        {
            await using var c = new NpgsqlConnection(conn);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, c);
            await cmd.ExecuteNonQueryAsync();
        }
        else
        {
            await using var c = new SqliteConnection(conn);
            await c.OpenAsync();
            await using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
