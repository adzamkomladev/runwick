using System.Text.RegularExpressions;
using Runwick.Core.Tests.Support;
using Runwick.Outbox;
using Runwick.Registration;
using Runwick.Worker;

namespace Runwick.Core.Tests;

/// <summary>
/// G-6 "defaults match code" (SP-030): the numbers in the defaults tables of docs/spec/06, 17 §4.4, 20 §10 and 22 §10
/// equal the defaults the code exposes. Each check names a table row and gives the code's value in the row's unit;
/// the row's leading numbers must equal it, so editing either side alone fails here.
/// </summary>
[Trait("Category", "Pure")]
[Trait("Item", "SP-030")]
public sealed partial class DefaultsMatchCodeTests
{
    /// <summary>
    /// Defaults that cannot be checked mechanically (no option or constant exposes them, or they are not plain numbers).
    /// Kept as a list so a reviewer sees what the test does not cover.
    /// </summary>
    public static readonly string[] NotCheckedMechanically =
    [
        "06 §1.1 Duration quantum, Retry evaluator: profile constants inside the evaluator, covered by RetryEvaluatorTests",
        "06 §1.2 Host ShutdownTimeout, Liveness stall bound: reference values, not set by Runwick",
        "06 §1.4 receipt retention, command lifetime: encoded in store SQL/Lua per provider, covered by contract tests",
        "06 §1.5 queue weight range, SWRR modes, prefetch, per-pool claim concurrency: behaviour, not options",
        "06 §1.6 pressure gates: thresholds inline in the pressure controller, covered by WorkerEngineTests/CAP cases",
        "06 §1.7 connection counts (control 8/2, work 14, lanes, background): PostgreSQL data-source strings built in the store",
        "06 §1.9 identity, payload and record bounds: profile constants, covered by SerializationCaseTests and wire vectors",
        "06 §1.10 schedule evaluator limits: profile constants, covered by the scheduling tests",
        "06 §1.11 rate bounds: RateBounds constants are internal to the compiler, covered by RATE cases",
        "06 §1.13 autoscaler: thresholds inline in the scaler, covered by ScalerTests",
        "06 §1.14 scheduler: due read, plan, misfire grace, catch-up limits and NotAfter values are inline; only polling and local allowance are checked",
        "17 §4.4 FailureBackoff: inline in the retention loop",
        "20 §10 fixed values (reader busy_timeout, writer threads, journal_size_limit, migration lock): fixed inside the store, covered by SQLite tests",
        "22 §10 intent/page/batch limits, ConnectTimeout, migration lock: fixed inside the Redis store, covered by Redis tests",
    ];


    static string[][] Rows(string file)
    {
        var path = Path.Combine(Repo.Root, "docs", "spec", file);
        return [.. File.ReadAllLines(path)
            .Where(l => l.StartsWith('|') && !l.StartsWith("|---", StringComparison.Ordinal))
            .Select(l => l.Trim().Trim('|').Split('|').Select(c => c.Trim().Trim('`').Trim()).ToArray())];
    }

    [GeneratedRegex(@"\d[\d,]*(?:\.\d+)?")]
    private static partial Regex Number();

    static double[] DocNumbers(string file, string row, int column)
    {
        var matches = Rows(file).Where(r => r.Length > column && r[0] == row).ToList();
        Assert.True(matches.Count == 1, $"{file}: expected exactly one table row named '{row}', found {matches.Count}");
        return [.. Number().Matches(matches[0][column]).Select(m => double.Parse(m.Value.Replace(",", ""), System.Globalization.CultureInfo.InvariantCulture))];
    }

    static void Check(List<string> failures, string file, string row, params double[] code)
    {
        var doc = DocNumbers(file, row, 1);
        if (doc.Length < code.Length || !doc.Take(code.Length).SequenceEqual(code))
        {
            failures.Add($"{file} '{row}': doc says [{string.Join(", ", doc.Take(code.Length))}], code has [{string.Join(", ", code)}]");
        }
    }

    static double Ms(TimeSpan t) => t.TotalMilliseconds;

    static double Sec(TimeSpan t) => t.TotalSeconds;

    [Fact]
    public void Worker_timing_and_pool_defaults_in_06_equal_the_code()
    {
        const string f = "06-defaults.md";
        var failures = new List<string>();
        var o = new WorkerOptions();

        // 1.1 generation policy: the library defaults of a job nobody configured.
        var job = TestHost.CompileApp(b => b.AddJob<PlainJob>()).Get<PlainJob>();
        Check(failures, f, "MaxExecutions", job.MaxExecutions.Value);
        Check(failures, f, "Retry InitialDelay", Ms(job.RetryDelay.Value.InitialDelay));
        Check(failures, f, "Retry MaximumDelay", Ms(job.RetryDelay.Value.MaxDelay));
        Check(failures, f, "MaxConsecutiveUnstartedAbandonments", job.MaxUnstartedAbandonments.Value);
        Check(failures, f, "Unstarted InitialDelay", Ms(job.UnstartedRecoveryDelay.Value.InitialDelay));
        Check(failures, f, "Unstarted MaximumDelay", Ms(job.UnstartedRecoveryDelay.Value.MaxDelay));
        Assert.Null(job.ExecutionTimeout.Value); // 'ExecutionTimeout | none'

        // 1.2 lease and control timing.
        Check(failures, f, "LeaseDuration", Ms(o.LeaseDuration));
        Check(failures, f, "LeaseSafetyMargin", Ms(o.LeaseSafetyMargin));
        Check(failures, f, "RenewalInterval (nominal)", Ms(o.RenewalInterval));
        Check(failures, f, "RenewalIntervalMin / Max", Ms(o.RenewalInterval - o.RenewalJitter), Ms(o.RenewalInterval + o.RenewalJitter));
        Check(failures, f, "LeaseWatchdogInterval", Ms(o.LeaseWatchdogInterval));
        Check(failures, f, "SchedulerAllowance", Ms(o.SchedulerAllowance));
        Check(failures, f, "ControlRequestTimeout", Ms(o.ControlRequestTimeout));
        Check(failures, f, "ControlRetryBackoff initial / max", Ms(o.ControlRetryInitialBackoff), Ms(o.ControlRetryMaxBackoff));
        Check(failures, f, "MinimumInvocationMargin", Ms(o.MinimumInvocationMargin));
        Check(failures, f, "ClaimResolutionBudget", Ms(o.ClaimResolutionBudget));
        Check(failures, f, "BeginResolutionBudget", Ms(o.BeginResolutionBudget));
        Check(failures, f, "CompletionResolutionBudget", Ms(o.CompletionResolutionBudget));
        Check(failures, f, "CancellationGrace", Ms(o.CancellationGrace));
        Check(failures, f, "CleanupBudget", Ms(o.CleanupBudget));
        Check(failures, f, "OwnershipCommandLifetime", Ms(o.OwnershipCommandLifetime));
        Check(failures, f, "RecoveryScanInterval", Ms(o.RecoveryScanInterval), Ms(o.RecoveryScanInterval - o.RecoveryScanJitter), Ms(o.RecoveryScanInterval + o.RecoveryScanJitter));
        Check(failures, f, "RecoveryBatch", WorkerSettings.RecoveryBatch);
        Check(failures, f, "ShutdownBudget", Ms(o.ShutdownBudget));
        Check(failures, f, "ShutdownDrain", Ms(o.ShutdownDrain));
        Check(failures, f, "ShutdownCancellation", Ms(o.ShutdownCancellation));
        Check(failures, f, "ShutdownFinalization", Ms(o.ShutdownFinalization));

        // 1.3 derived values, computed from the option defaults by the documented formulas.
        var usable = Ms(o.LeaseDuration - o.LeaseSafetyMargin);
        var stop = usable - Ms(o.RenewalInterval + o.RenewalJitter) - Ms(o.ControlRequestTimeout) - Ms(o.SchedulerAllowance);
        Check(failures, f, "usableAuthority", usable);
        Check(failures, f, "StopMargin", stop);
        Check(failures, f, "ResumeMargin", stop + Ms(o.ControlRequestTimeout));
        Check(failures, f, "Renewal cap offset", 2 * Ms(o.ControlRequestTimeout) + Ms(o.ControlRetryMaxBackoff) + Ms(o.SchedulerAllowance));
        Check(failures, f, "Renewal check sum", Ms(o.RenewalInterval + o.RenewalJitter) + 2 * Ms(o.ControlRequestTimeout) + Ms(o.ControlRetryMaxBackoff) + Ms(o.SchedulerAllowance));

        // 1.5 capacity, fairness, probing.
        Check(failures, f, "Default pool mode", WorkerSettings.DefaultPoolTarget);
        Check(failures, f, "Worker/store group maximum", WorkerSettings.GroupMaximum);
        var weights = new PoolBuilder("p").Weights;
        Check(failures, f, "Lane weights high : medium : low", weights.High, weights.Medium, weights.Low);
        Check(failures, f, "AgingEvery", WorkerSettings.AgingEvery);
        Check(failures, f, "AgingThreshold", Ms(WorkerSettings.AgingThreshold));
        Check(failures, f, "BlockedCandidatesPerLaneVisit", WorkerSettings.BlockedCandidatesPerLaneVisit);
        Check(failures, f, "ReservesPerRound", WorkerSettings.ReservesPerRound);
        Check(failures, f, "Readiness maximum hint age", Ms(WorkerSettings.HintMaxAge));
        Check(failures, f, "Idle probe backoff", Ms(WorkerSettings.IdleBackoffBase), Ms(WorkerSettings.IdleBackoffCap), Ms(WorkerSettings.IdleBackoffCapListening));
        Check(failures, f, "Blocked lane/queue cooldown", Ms(WorkerSettings.CooldownBase), Ms(WorkerSettings.CooldownCap));
        Check(failures, f, "Notification probe spacing", Ms(WorkerSettings.ProbeSpacing));

        // 1.7 the classes the options expose.
        var p = new ProducerOptions();
        Check(failures, f, "Producer/admin mutation class", p.MaxConcurrentCalls);
        Check(failures, f, "Settlement lane", WorkerSettings.SettlementLane);
        Check(failures, f, "Local admission waiters", p.MaxWaitingOperations);
        Check(failures, f, "Local admission wait cap", Ms(p.AdmissionWait));
        Check(failures, f, "Dispatch / admin command lifetime", p.CommandLifetime.TotalMinutes);

        // 1.8 presence.
        Check(failures, f, "Presence heartbeat", Ms(WorkerSettings.PresenceHeartbeat));
        Check(failures, f, "Presence expiry", Ms(WorkerSettings.PresenceExpiry));

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void Outbox_scheduler_retention_and_store_defaults_equal_the_code()
    {
        var failures = new List<string>();

        // 06 §1.12 outbox relay.
        var ob = new OutboxRelayOptions();
        Check(failures, "06-defaults.md", "MaxInFlight / claim batch", ob.MaxInFlight);
        Check(failures, "06-defaults.md", "Source lease / renewal", Sec(ob.LeaseDuration), Sec(ob.RenewalInterval));
        Check(failures, "06-defaults.md", "Provider call timeout", Sec(ob.ProviderCallTimeout));
        Check(failures, "06-defaults.md", "Idle poll", Sec(ob.IdlePoll));
        Check(failures, "06-defaults.md", "Retry", Sec(ob.RetryInitial), Sec(ob.RetryMax));
        Check(failures, "06-defaults.md", "Transport NotAfter", Sec(ob.TransportLifetime));

        // 06 §1.14 scheduler: polling and the local allowance.
        var sch = new SchedulerOptions();
        Check(failures, "06-defaults.md", "Polling", Sec(sch.PollInterval), Sec(sch.IdlePollInterval));
        Check(failures, "06-defaults.md", "Local allowance", sch.JobsPerSecond, sch.Burst);

        // 17 §4.4 retention.
        var r = new RetentionOptions();
        Assert.True(r.Enabled); // doc: 'Enabled | true'
        Check(failures, "17-retention.md", "BatchRows", r.BatchRows);
        Check(failures, "17-retention.md", "BatchDetailBytes", r.BatchDetailBytes / 1048576.0);
        Check(failures, "17-retention.md", "BatchTarget", Ms(r.BatchTarget));
        Check(failures, "17-retention.md", "BatchPause", Sec(r.BatchPause));
        Check(failures, "17-retention.md", "IdlePoll", Sec(r.IdlePoll));

        // 20 §10 SQLite and 22 §10 Redis.
        var sq = new SqliteOptions();
        Check(failures, "20-sqlite.md", "WriterBusyTimeout", Sec(sq.WriterBusyTimeout));
        Check(failures, "20-sqlite.md", "ReaderThreads", sq.ReaderThreads);
        Check(failures, "22-redis.md", "ProfileRecheckInterval", Sec(new RedisOptions().ProfileRecheckInterval));

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void Unchecked_list_is_documented()
    {
        Assert.NotEmpty(NotCheckedMechanically);
    }
}
