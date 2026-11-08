using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runwick;

/// <summary>The one job the scenarios run. Its handler leaves a file in the sink directory, so any process and any store can be checked the same way.</summary>
sealed record Work(string Id, int Ms) : IQueueJob;

sealed record NodeName(string Value, string Sink);

sealed class WorkHandler(NodeName node, IJobContext context) : IJobHandler<Work>
{
    public async Task HandleAsync(Work job, CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow.Ticks;
        await Task.Delay(job.Ms, cancellationToken);
        // One file per job: "<process> <start ticks> <end ticks>". A retry overwrites it, so counts are of distinct jobs.
        await File.WriteAllTextAsync(Path.Combine(node.Sink, context.JobId.Value.ToString("N")), $"{node.Value} {start} {DateTime.UtcNow.Ticks} {job.Id} {context.ExecutionNumber}", cancellationToken);
    }
}

[Flags]
enum Role { None = 0, Producer = 1, Worker = 2, Scheduler = 4, Both = Producer | Worker }

enum StoreKind { Pg, Sqlite, Redis }

/// <summary>What one process looks like. The same options build a child process (`node` command) or an in-process host.</summary>
sealed record NodeOptions(StoreKind Store, string Conn, string App, string Sink, string Name, Role Role)
{
    public int Jobs { get; init; }

    public int Ms { get; init; } = 50;

    public int Cap { get; init; }

    public int Concurrency { get; init; } = 4;

    public (int Min, int Max)? Auto { get; init; }

    public bool Tick { get; init; }

    public string Prefix => Name + "-";

    public static NodeOptions Parse(string[] a)
    {
        string V(string k, string? d = null) => a.Contains(k) ? a[Array.IndexOf(a, k) + 1] : d ?? throw new ArgumentException($"missing {k}");
        var auto = V("--auto", "").Split(':');
        return new NodeOptions(Enum.Parse<StoreKind>(V("--store"), true), V("--conn"), V("--app"), V("--sink"), V("--name"), Enum.Parse<Role>(V("--role"), true))
        {
            Jobs = int.Parse(V("--jobs", "0")), Ms = int.Parse(V("--ms", "50")), Cap = int.Parse(V("--cap", "0")), Concurrency = int.Parse(V("--conc", "4")),
            Auto = auto.Length == 2 ? (int.Parse(auto[0]), int.Parse(auto[1])) : null, Tick = a.Contains("--tick"),
        };
    }

    public IEnumerable<string> ToArgs()
    {
        yield return "node";
        foreach (var x in new[] { "--store", Store.ToString(), "--conn", Conn, "--app", App, "--sink", Sink, "--name", Name, "--role", Role.ToString(),
            "--jobs", Jobs.ToString(), "--ms", Ms.ToString(), "--cap", Cap.ToString(), "--conc", Concurrency.ToString() })
        {
            yield return x;
        }

        if (Auto is { } au)
        {
            yield return "--auto";
            yield return $"{au.Min}:{au.Max}";
        }

        if (Tick)
        {
            yield return "--tick";
        }
    }
}

static class NodeHost
{
    /// <summary>The Runwick registration every process shares; only the role-dependent parts differ (spec 00 §5: only AddRunwick differs).</summary>
    public static void Configure(RunwickBuilder r, NodeOptions o)
    {
        r.Application(o.App);
        switch (o.Store)
        {
            case StoreKind.Pg: r.UsePostgres(o.Conn); break;
            case StoreKind.Sqlite: r.UseSqlite(o.Conn); break;
            default: r.UseRedis(o.Conn); break;
        }

        r.Queue("work", q =>
        {
            if (o.Cap > 0)
            {
                q.MaxConcurrent(o.Cap);
            }
        });
        r.AddJob<Work>().Named("scn.work").ToQueue("work");
        r.AddHandler<Work, WorkHandler>();
        if (o.Role.HasFlag(Role.Worker))
        {
            r.AddWorker(w => w.Pool("main", p =>
            {
                p.Queues("work");
                _ = o.Auto is { } a ? p.AutoConcurrency(a.Min, a.Max) : p.FixedConcurrency(o.Concurrency);
            }));
        }

        if (o.Role.HasFlag(Role.Scheduler))
        {
            r.AddScheduler(s => (s.PollInterval, s.IdlePollInterval) = (TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1)));
            if (o.Tick)
            {
                r.AddSchedule("tick", new Work("tick", 20)).Cron("*/2 * * * * *", ScheduleCronFormat.SixField);
            }
        }
    }

    public static IHost Create(NodeOptions o, Action<IServiceCollection>? more = null)
    {
        var b = Host.CreateApplicationBuilder();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.Services.AddSingleton(new NodeName(o.Name, o.Sink));
        b.Services.AddRunwick(r => Configure(r, o));
        more?.Invoke(b.Services);
        return b.Build();
    }

    public static async Task DispatchAsync(IHost host, NodeOptions o, int count, string? prefix = null)
    {
        var d = host.Services.GetRequiredService<IJobDispatcher>();
        for (var i = 0; i < count; i++)
        {
            await d.DispatchAsync(new Work((prefix ?? o.Prefix) + i, o.Ms));
        }
    }

    /// <summary>`node`: one process. Exit 0 normal, 75 (EX_TEMPFAIL) when startup was refused for a reason that goes away (drained application, store down).</summary>
    public static async Task<int> RunNodeAsync(string[] args)
    {
        var o = NodeOptions.Parse(args);
        Directory.CreateDirectory(o.Sink);
        using var host = Create(o);
        try
        {
            await host.StartAsync();
        }
        catch (Exception ex) when (ex is AdmissionClosedException or StoreUnavailableException)
        {
            Console.Error.WriteLine($"STARTUP REFUSED (retryable, exit 75): {ex.Message}");
            return 75;
        }

        Console.WriteLine("ready");
        if (o.Role.HasFlag(Role.Producer) && o.Jobs > 0)
        {
            await DispatchAsync(host, o, o.Jobs);
            Console.WriteLine($"dispatched {o.Jobs}");
        }

        if (o.Role != Role.Producer)
        {
            // Run until the orchestrator closes our stdin.
            await Task.Run(() => { while (Console.In.ReadLine() is not null) { } });
        }

        await host.StopAsync();
        return 0;
    }
}
