using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;

namespace Runwick.Workloads;

// G-2 workload job contracts (15 §11). Every G-2 child process registers all of them with the same policies (the
// policy arguments are passed identically to every child of one run), and registers handlers only for the subset
// named by --handlers, which is how P-05 builds heterogeneous capability sets.

/// <summary>Plain work: P-04 (queue and lane chosen per dispatch) and P-05 unconstrained (queue <c>pu</c>).</summary>
public sealed record G2Work(string Id, string Pad) : IQueueJob;

/// <summary>P-05 constrained, type A: queue <c>pc</c> ceiling plus job-type ceiling.</summary>
public sealed record G2A(string Id, string Pad) : IQueueJob;

/// <summary>P-05 constrained, type B: queue <c>pc</c> ceiling plus job-type ceiling.</summary>
public sealed record G2B(string Id, string Pad) : IQueueJob;

/// <summary>P-05 constrained, type C: queue <c>pc</c> ceiling plus per-resource ceiling (one hot key).</summary>
public sealed record G2C(string Id, string Key, string Pad) : IQueueJob;

/// <summary>P-06 rate half: one rate scope (job type).</summary>
public sealed record G2Rated1(string Id, string Pad) : IQueueJob;

/// <summary>P-06 rate half: eight rate scopes (job type plus seven resource scopes).</summary>
public sealed record G2Rated8(string Id, string Pad) : IQueueJob;

/// <summary>P-06 scheduler half: one scheduled occurrence.</summary>
public sealed record G2Tick(string Schedule) : IQueueJob;

/// <summary>P-09: a 1 KiB outbox intent.</summary>
public sealed record G2Out(string Id, string Pad) : IQueueJob;

/// <summary>Handler behaviour of this process: an asynchronous delay, or a CPU-bound spin of the same length (P-04 d).</summary>
public sealed record G2Behaviour(TimeSpan Duration, bool Spin);

internal sealed class G2Handler<TJob>(G2Behaviour behaviour) : IJobHandler<TJob> where TJob : class, IQueueJob
{
    public Task HandleAsync(TJob job, CancellationToken cancellationToken)
    {
        if (behaviour.Duration <= TimeSpan.Zero)
        {
            return Task.CompletedTask;
        }

        if (!behaviour.Spin)
        {
            return Task.Delay(behaviour.Duration, cancellationToken);
        }

        // CPU-bound: burn the thread for the whole duration (no await, no sleep).
        var started = Stopwatch.GetTimestamp();
        var x = 0UL;
        while (Stopwatch.GetElapsedTime(started) < behaviour.Duration)
        {
            for (var i = 0; i < 1000; i++)
            {
                x = (x * 6364136223846793005UL) + 1442695040888963407UL;
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        GC.KeepAlive(x);
        return Task.CompletedTask;
    }
}

internal static class G2Jobs
{
    public const string App = "wl";

    /// <summary>Queues every G-2 contract can be dispatched to.</summary>
    public static readonly string[] Queues = ["fa", "fb", "fl", "pu", "pc", "pr", "ps", "po"];

    public static string Pad(int payloadBytes) => new('x', Math.Max(0, payloadBytes - 48));

    /// <summary>Registers every G-2 contract; policy values come from the run's common arguments.</summary>
    public static void Register(RunwickBuilder r, Args a)
    {
        foreach (var q in Queues)
        {
            if (q == "pc")
            {
                r.Queue(q, b => b.MaxConcurrent(a.Int("queue-ceiling", 24)));
            }
            else
            {
                r.Queue(q);
            }
        }

        var typeCeiling = a.Int("type-ceiling", 8);
        var limit = a.Int("rate-limit", 100);
        var window = TimeSpan.FromMilliseconds(a.Int("rate-window-ms", 1000));
        r.AddJob<G2Work>().Named("g2.work").ToQueue("pu").WithMaxExecutions(3);
        r.AddJob<G2A>().Named("g2.a").ToQueue("pc").WithMaxExecutions(3).MaxConcurrent(typeCeiling);
        r.AddJob<G2B>().Named("g2.b").ToQueue("pc").WithMaxExecutions(3).MaxConcurrent(typeCeiling);
        r.AddJob<G2C>().Named("g2.c").ToQueue("pc").WithMaxExecutions(3).PerResource(j => j.Key, a.Int("resource-ceiling", 2));
        r.AddJob<G2Rated1>().Named("g2.rated1").ToQueue("pr").WithMaxExecutions(3).RollingWindow(limit, window);
        var rated8 = r.AddJob<G2Rated8>().Named("g2.rated8").ToQueue("pr").WithMaxExecutions(3).RollingWindow(limit, window);
        for (var k = 1; k <= 7; k++)
        {
            var key = "rs" + k.ToString(CultureInfo.InvariantCulture);
            rated8.RollingWindowPerResource(_ => key, limit, window);
        }

        r.AddJob<G2Tick>().Named("g2.tick").ToQueue("ps").WithMaxExecutions(3);
        r.AddJob<G2Out>().Named("g2.out").ToQueue("po").WithMaxExecutions(3);
    }

    /// <summary>Handlers for the comma list <c>--handlers</c> (work, a, b, c, rated1, rated8, tick, out); default <c>work</c>.</summary>
    public static void AddHandlers(RunwickBuilder r, Args a)
    {
        r.Services.AddSingleton(new G2Behaviour(TimeSpan.FromMilliseconds(a.Int("handler-ms", 0)), a.Has("spin")));
        foreach (var h in (a.OptStr("handlers") ?? "work").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            switch (h)
            {
                case "work": r.AddHandler<G2Work, G2Handler<G2Work>>(); break;
                case "a": r.AddHandler<G2A, G2Handler<G2A>>(); break;
                case "b": r.AddHandler<G2B, G2Handler<G2B>>(); break;
                case "c": r.AddHandler<G2C, G2Handler<G2C>>(); break;
                case "rated1": r.AddHandler<G2Rated1, G2Handler<G2Rated1>>(); break;
                case "rated8": r.AddHandler<G2Rated8, G2Handler<G2Rated8>>(); break;
                case "tick": r.AddHandler<G2Tick, G2Handler<G2Tick>>(); break;
                case "out": r.AddHandler<G2Out, G2Handler<G2Out>>(); break;
                default: throw new ArgumentException($"unknown handler '{h}'");
            }
        }
    }

    /// <summary>The worker pool: <c>--queues q:w,...</c> (default <c>pu:1</c>), <c>--lane-mode weighted|strict|strict-aging</c>.</summary>
    public static void Pool(PoolBuilder p, Args a, int concurrency)
    {
        foreach (var part in (a.OptStr("queues") ?? "pu:1").Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var qw = part.Split(':');
            p.Queue(qw[0], qw.Length > 1 ? int.Parse(qw[1], CultureInfo.InvariantCulture) : 1);
        }

        p.FixedConcurrency(concurrency).LaneWeights(6, 3, 1).LaneSelection((a.OptStr("lane-mode") ?? "weighted") switch
        {
            "strict" => LaneSelectionMode.Strict,
            "strict-aging" => LaneSelectionMode.StrictWithAging,
            _ => LaneSelectionMode.Weighted,
        });
    }

    /// <summary>Job factory for producers: <c>work</c>, <c>mix</c> (A/B/C with a hot C key), <c>rated1</c>, <c>rated8</c>, <c>out</c>.</summary>
    public static Func<long, IQueueJob> Factory(string kind, Args a)
    {
        var pad = Pad(a.Int("payload", 1024));
        var hot = a.Double("hot-fraction", 0.5);
        var keys = a.Int("resource-keys", 64);
        var random = new Random(a.Int("seed", 20261030) + 1);
        string Id(long n) => n.ToString(CultureInfo.InvariantCulture);
        return kind switch
        {
            "work" => n => new G2Work(Id(n), pad),
            "rated1" => n => new G2Rated1(Id(n), pad),
            "rated8" => n => new G2Rated8(Id(n), pad),
            "out" => n => new G2Out(Id(n), pad),
            "mix" => n =>
            {
                lock (random)
                {
                    return (n % 3) switch
                    {
                        0 => new G2A(Id(n), pad),
                        1 => new G2B(Id(n), pad),
                        _ => new G2C(Id(n), random.NextDouble() < hot ? "hot" : "k" + random.Next(keys).ToString(CultureInfo.InvariantCulture), pad),
                    };
                }
            },
            _ => throw new ArgumentException($"unknown job kind '{kind}'"),
        };
    }
}
